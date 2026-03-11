"""State management for resumable ingestion

Tracks per-source progress, handles partial failures, enables
pick-up-where-left-off when API limits hit
"""

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


@dataclass
class SourceState:
    #Per-source ingestion tracking."""
    initial_start_date: str  # earliest date to backfill from
    initial_end_date: str  # target end date for initial load
    last_processed_date: Optional[str] = None  # resume point
    status: str = "pending"  # pending|in_progress|complete
    last_run_ts: Optional[str] = None
    error_count: int = 0
    last_error: Optional[str] = None


@dataclass
class IngestionState:
    #Global ingestion state across all sources
    initial_ingestion_complete: bool = False
    last_successful_run: Optional[str] = None
    sources: Dict[str, SourceState] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "initial_ingestion_complete": self.initial_ingestion_complete,
            "last_successful_run": self.last_successful_run,
            "sources": {k: asdict(v) for k, v in self.sources.items()},
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IngestionState":
        sources = {}
        for name, src_data in data.get("sources", {}).items():
            sources[name] = SourceState(**src_data)
        return cls(
            initial_ingestion_complete=data.get("initial_ingestion_complete", False),
            last_successful_run=data.get("last_successful_run"),
            sources=sources,
        )


class StateManager:
    """Handles state persistence to S3 or local file

    State stored as JSON; supports both S3 (prod) and local file (dev)
    """

    STATE_FILE_NAME = "ingestion_state.json"

    def __init__(
            self,
            bucket: Optional[str] = None,
            prefix: str = "state",
            region: str = "us-east-1",
            local_path: Optional[str] = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.region = region
        self.local_path = local_path  # if set, use local file instead of S3
        self._s3 = boto3.client("s3", region_name=region) if bucket else None

    def _s3_key(self) -> str:
        return f"{self.prefix}/{self.STATE_FILE_NAME}"

    def _local_file(self) -> str:
        return os.path.join(self.local_path or ".", self.STATE_FILE_NAME)

    def load(self) -> IngestionState:
        #Load state from S3 or local file. Returns empty state if not found
        if self.local_path:
            return self._load_local()
        return self._load_s3()

    def _load_s3(self) -> IngestionState:
        if not self._s3 or not self.bucket:
            logger.warning("No S3 configured; returning empty state")
            return IngestionState()
        try:
            resp = self._s3.get_object(Bucket=self.bucket, Key=self._s3_key())
            data = json.loads(resp["Body"].read().decode("utf-8"))
            logger.info("Loaded state from s3://%s/%s", self.bucket, self._s3_key())
            return IngestionState.from_dict(data)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.info("No existing state in S3; starting fresh")
                return IngestionState()
            raise

    def _load_local(self) -> IngestionState:
        path = self._local_file()
        if not os.path.exists(path):
            logger.info("No local state file; starting fresh")
            return IngestionState()
        with open(path, "r") as f:
            data = json.load(f)
        logger.info("Loaded state from %s", path)
        return IngestionState.from_dict(data)

    def save(self, state: IngestionState) -> None:
        #Persist state to S3 or local file
        if self.local_path:
            self._save_local(state)
        else:
            self._save_s3(state)

    def _save_s3(self, state: IngestionState) -> None:
        if not self._s3 or not self.bucket:
            logger.warning("No S3 configured; skipping state save")
            return
        body = json.dumps(state.to_dict(), indent=2)
        self._s3.put_object(Bucket=self.bucket, Key=self._s3_key(), Body=body)
        logger.info("Saved state to s3://%s/%s", self.bucket, self._s3_key())

    def _save_local(self, state: IngestionState) -> None:
        path = self._local_file()
        with open(path, "w") as f:
            json.dump(state.to_dict(), f, indent=2)
        logger.info("Saved state to %s", path)


def generate_date_batches(
        start_date: str,
        end_date: str,
        batch_days: int = 30,
) -> list[tuple[str, str]]:
    """Chunk date range into batches for API-friendly iteration

    start_date: YYYY-MM-DD format
    end_date: YYYY-MM-DD format
    batch_days: days per batch (default 30)

    returns list of (batch_start, batch_end) tuples
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    batches = []
    current = start
    while current < end:
        batch_end = min(current + timedelta(days=batch_days - 1), end)
        batches.append((current.strftime("%Y-%m-%d"), batch_end.strftime("%Y-%m-%d")))
        current = batch_end + timedelta(days=1)
    return batches
