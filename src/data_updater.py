#!/usr/bin/env python3
"""DataUpdater service for NBA data ingestion.

This module provides a production‑oriented implementation of a data
ingestion service that pulls data from multiple public basketball APIs
and scrapers and writes Parquet files to an S3 data lake.  It is
designed to run inside AWS Fargate, ECS or a simple EC2 host and can
be scheduled via AWS EventBridge or Step Functions.  The logic has
been modularised into component classes to make syntax errors (e.g.,
missing imports) obvious at import time and runtime errors easier to
trace back to their source modules.  Adding new data sources or
transformations should not require editing this file.

## Key components

* **nba_api_client.NBAApiClient** – Wraps calls to the official NBA
  statistics API using the `nba_api` Python library.  Retrieves game
  metadata and other statistics as Pandas DataFrames.
* **pbpstats_client.PBPStatsClient** – Encapsulates calls to the
  `pbpstats` package, which provides play‑by‑play statistics such as
  possessions, seconds per possession, assisted/unassisted shots and
  shot quality.  Rate limiting and caching strategies should be
  implemented within this class.
* **bball_index_scraper.BballIndexScraper** – Handles authentication
  and scraping of player profiles from bball‑index.com.  Credentials
  are supplied via environment variables (`BBALL_EMAIL` and
  `BBALL_PSWRD`) and passed into the scraper at runtime.  If either
  credential is missing, the scraper step is skipped.
* **config.load_settings** – Reads environment variables at import
  time and raises an exception if required configuration values are
  missing.  Storing configuration in a dataclass ensures that
  misconfigured environments fail fast during startup rather than
  causing subtle runtime errors later.

The service writes raw (Bronze) datasets to S3, then triggers
downstream transformations (Silver and Gold tables) via AWS Glue or
Spark jobs (not implemented here).  See README.md for details on
deployment and scheduling.
"""

import logging
from datetime import datetime
from typing import List

import boto3
import pandas as pd

from nba_ingestion.config import load_settings, Settings
from nba_ingestion.nba_api_client import NBAApiClient
from nba_ingestion.pbpstats_client import PBPStatsClient
from nba_ingestion.bball_index_scraper import BballIndexScraper


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataUpdater:
    """Class encapsulating the orchestration of data ingestion.

    The DataUpdater composes several client classes to fetch data
    from `nba_api`, `pbpstats` and `bball-index`.  It writes each
    dataset to S3 as a Parquet file using the naming convention
    `<bronze_prefix>/<dataset_name>/<dataset_name>_<timestamp>.parquet`.

    Attributes:
        settings: Loaded configuration settings (see config.py).
        s3_client: boto3 S3 client for uploading Parquet files.
        nba_client: Instance of NBAApiClient for fetching games.
        pbp_client: Instance of PBPStatsClient for fetching possession stats.
        bball_scraper: Optional instance of BballIndexScraper for player profiles.
    """
    SOURCE_NBA_API = "nba_api"
    SOURCE_PBPSTATS = "pbpstats"
    SOURCE_BBALL_INDEX = "bball_index"

    def __init__(self) -> None:
        self.settings: Settings = load_settings()
        self.s3 = boto3.client("s3", region_name=self.settings.aws_region)

        #init state manager
        self.state_manager = StateManager(
            bucket=self.settings.bronze_bucket,
            prefix=f"{self.settings.bronze_prefix}/state",
            region=self.settings.aws_region,
            local_path=self.settings.state_local_path or None,
        )

        #init clients
        self.nba_client = NBAApiClient(
            rate_limit_sleep=self.settings.nba_api_rate_limit_sleep,
        )
        self.pbp_client = PBPStatsClient(
            rate_limit_sleep=self.settings.pbp_rate_limit_sleep,
            data_provider=self.settings.pbp_stats_provider,
            source=self.settings.pbp_stats_source,
            data_dir=self.settings.pbp_stats_data_directory or None,
        )

        #bball-index scraper (optional)
        if self.settings.bball_email and self.settings.bball_password:
            self.bball_scraper: Optional[BballIndexScraper] = BballIndexScraper(
                self.settings.bball_email,
                self.settings.bball_password,
            )
        else:
            self.bball_scraper = None
            logger.info("BBALL_EMAIL/BBALL_PSWRD not set; skipping bball-index")

        logger.info(
            "DataUpdater init: bucket=%s prefix=%s dry_run=%s",
            self.settings.bronze_bucket,
            self.settings.bronze_prefix,
            self.settings.dry_run,
        )

    def run(self) -> None:
        """Main entry point for the DataUpdater service.

        Coordinates calls to data sources and writes results to S3.  In
        production, this method could be invoked by an AWS Step Functions
        task or EventBridge schedule.  Surround API calls with try/except
        blocks to gracefully handle failures and continue with other data.
        """
        logger.info("Starting data ingestion run at %s", datetime.utcnow().isoformat())

        # Fetch league games and write to S3
        games_df = self.nba_client.fetch_games()
        logger.info("Fetched %d games from nba_api", len(games_df))
        self._write_parquet(games_df, "games")

        # Extract game IDs for downstream play-by-play fetches
        game_ids: List[str] = []
        if not games_df.empty and "game_id" in games_df.columns:
            game_ids = games_df["game_id"].astype(str).tolist()

        # Fetch possession stats via pbpstats
        try:
            possession_df = self.pbp_client.fetch_possession_stats(game_ids)
            logger.info(
                "Fetched possession stats rows=%d across requested_games=%d",
                len(possession_df),
                len(game_ids),
            )
            self._write_parquet(possession_df, "pbpstats_possessions")
        except Exception as exc:
            logger.error("Error fetching pbpstats data: %s", exc, exc_info=True)

        # Fetch player profiles from Bball-Index if credentials are available
        if self.bball_scraper:
            try:
                self.bball_scraper.authenticate()
                profiles_df = self.bball_scraper.fetch_all_profiles()
                logger.info(
                    "Fetched %d player profiles from Bball-Index", len(profiles_df)
                )
                self._write_parquet(profiles_df, "bball_index_profiles")
            except Exception as exc:
                logger.error(
                    "Error fetching Bball-Index profiles: %s", exc, exc_info=True
                )

        logger.info("Data ingestion run completed")

    def _write_parquet(self, df: pd.DataFrame, dataset_name: str) -> None:
        """Write a Pandas DataFrame to S3 in Parquet format.

        :param df: DataFrame to write
        :param dataset_name: Logical name of the dataset (used in the file prefix)
        """
        if df.empty:
            logger.warning(
                "Received empty DataFrame for dataset '%s' – skipping write",
                dataset_name,
            )
            return

        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key = f"{self.settings.bronze_prefix}/{dataset_name}/{dataset_name}_{now}.parquet"
        logger.info(
            "Writing %s records to s3://%s/%s",
            len(df),
            self.settings.bronze_bucket,
            key,
        )

        if self.settings.dry_run:
            logger.info("Dry run enabled – not uploading Parquet file")
            return

        # Save DataFrame to a temporary local file
        tmp_path = f"/tmp/{dataset_name}_{now}.parquet"
        df.to_parquet(tmp_path, index=False)

        # Upload to S3
        self.s3_client.upload_file(
            tmp_path, self.settings.bronze_bucket, key
        )
        logger.info(            "Uploaded %s to s3://%s/%s", tmp_path, self.settings.bronze_bucket,
            key,
        )


if __name__ == "__main__":
    updater = DataUpdater()
    updater.run()