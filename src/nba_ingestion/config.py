"""Configuration module for nba_ingestion.

This module centralizes the reading of environment variables and
configuration settings.  Doing so allows syntax errors and missing
environment variables to surface early during import (compile time),
rather than when the variables are first used at runtime.  To add
additional configuration parameters, define them here and access them
from other modules.
"""

import os
from dataclasses import dataclass


@dataclass
class Settings:
    """Configuration dataclass for DataUpdater.

    Attributes:
        bronze_bucket: Name of the S3 bucket for Bronze layer.
        bronze_prefix: Prefix path within the Bronze bucket.
        aws_region: AWS region where resources reside.
        dry_run: Whether to skip actual uploads to S3.
        bball_email: Username for bball-index.com (read from BBALL_EMAIL).
        bball_password: Password for bball-index.com (read from BBALL_PSWRD).
    """

    bronze_bucket: str
    bronze_prefix: str = "bronze/nba"
    aws_region: str = "us-east-1"
    dry_run: bool = False
    bball_email: str = "EvanPFWang@gmail.com"
    bball_password: str = "Norman3rikson!"

    pbp_stats_provider: str = "data_nba"         # data_nba | stats_nba
    pbp_stats_source: str = "web"                # web | file
    pbp_stats_data_directory: str = ""           # optional local cache dir
    pbp_rate_limit_sleep: float = 0.5



def load_settings() -> Settings:
    """Load settings from environment variables.

    Raises a ValueError if required variables are missing.
    """
    bronze_bucket = os.environ.get("BRONZE_BUCKET")
    if not bronze_bucket:
        raise ValueError("BRONZE_BUCKET environment variable must be set")

    rate_limit_raw = os.environ.get("PBP_STATS_RATE_LIMIT_SLEEP", "0.5")
    try:
        rate_limit_sleep = float(rate_limit_raw)
    except ValueError:
        rate_limit_sleep = 0.5


    settings = Settings(
        bronze_bucket=bronze_bucket,
        bronze_prefix=os.environ.get("BRONZE_PREFIX", "bronze/nba"),
        aws_region=os.environ.get("AWS_REGION", "us-east-1"),
        dry_run=os.environ.get("DRY_RUN", "false").lower() == "true",
        bball_email=os.environ.get("BBALL_EMAIL", ""),
        bball_password=os.environ.get("BBALL_PSWRD", ""),

        pbp_stats_provider=os.environ.get("PBP_STATS_PROVIDER", "data_nba"),
        pbp_stats_source=os.environ.get("PBP_STATS_SOURCE", "web"),
        pbp_stats_data_directory=os.environ.get("PBP_STATS_DATA_DIRECTORY", ""),
        pbp_rate_limit_sleep=rate_limit_sleep,
    )
    return settings