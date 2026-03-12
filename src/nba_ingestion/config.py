"""Configuration module for nba_ingestion.

Centralizes the reading of environ vars and config settings.

Reports syntax errors and missing environment variables during import (compile time),
rather than when vars first used at runtime.  To add additional config
parameters, define them here and access from other modules.
"""

import os
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Settings:
    """Config dataclass for DataUpdater.
    - S3/AWS settings
    - Ingestion mode settings
    - Source credentials
    - Rate limiting
    """
    bronze_bucket: str
    bronze_prefix: str = "bronze/nba"
    silver_bucket: str = ""
    silver_prefix: str = "silver/nba"
    gold_bucket: str = ""
    gold_prefix: str = "gold/nba"
    aws_region: str = "us-east-1"

    #ingestion control
    dry_run: bool = False
    initial_ingestion_date: str = ""  #YYYY-MM-DD; backfill target date
    initial_start_date: str = "2015-10-01"  #earliest date to fetch
    state_local_path: str = ""  #if set, use local file for state

    #bball-index credentials
    bball_email: str = ""
    bball_password: str = ""

    #pbpstats settings
    pbp_stats_provider: str = "data_nba"         #data_nba | stats_nba
    pbp_stats_source: str = "web"                #web | file
    pbp_stats_data_directory: str = ""           #optional local cache dir
    pbp_rate_limit_sleep: float = 0.5

    #nba_api rate limiting
    nba_api_rate_limit_sleep: float = 0.6

    #batch sizing for initial ingestion
    batch_days: int = 30


def load_settings() -> Settings:
    """Load settings from environment variables.

    Required: BRONZE_BUCKET
    Optional: everything else with sensible defaults
    """

    bronze_bucket = os.environ.get("BRONZE_BUCKET")
    if not bronze_bucket:
        raise ValueError("BRONZE_BUCKET environment variable must be set")

    #parse rate limit sleeps
    def parse_float(key: str, default: float) -> float:
        raw = os.environ.get(key, str(default))
        try:
            return float(raw)
        except ValueError:
            return default

    #parse batch days
    def parse_int(key: str, default: int) -> int:
        raw = os.environ.get(key, str(default))
        try:
            return int(raw)
        except ValueError:
            return default

    #default initial_ingestion_date to today if not set
    today = datetime.utcnow().strftime("%Y-%m-%d")

    return Settings(
        bronze_bucket=bronze_bucket,
        bronze_prefix=os.environ.get("BRONZE_PREFIX", "bronze/nba"),
        silver_bucket=os.environ.get("SILVER_BUCKET", bronze_bucket),
        silver_prefix=os.environ.get("SILVER_PREFIX", "silver/nba"),
        gold_bucket=os.environ.get("GOLD_BUCKET", bronze_bucket),
        gold_prefix=os.environ.get("GOLD_PREFIX", "gold/nba"),
        aws_region=os.environ.get("AWS_REGION", "us-east-1"),
        dry_run=os.environ.get("DRY_RUN", "false").lower() == "true",
        initial_ingestion_date=os.environ.get("INITIAL_INGESTION_DATE", today),
        initial_start_date=os.environ.get("INITIAL_START_DATE", "2015-10-01"),
        state_local_path=os.environ.get("STATE_LOCAL_PATH", ""),
        bball_email=os.environ.get("BBALL_EMAIL", ""),
        bball_password=os.environ.get("BBALL_PSWRD", ""),
        pbp_stats_provider=os.environ.get("PBP_STATS_PROVIDER", "data_nba"),
        pbp_stats_source=os.environ.get("PBP_STATS_SOURCE", "web"),
        pbp_stats_data_directory=os.environ.get("PBP_STATS_DATA_DIRECTORY", ""),
        pbp_rate_limit_sleep=parse_float("PBP_STATS_RATE_LIMIT_SLEEP", 0.5),
        nba_api_rate_limit_sleep=parse_float("NBA_API_RATE_LIMIT_SLEEP", 0.6),
        batch_days=parse_int("BATCH_DAYS", 30),
    )
