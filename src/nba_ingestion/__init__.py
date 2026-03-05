"""Package initialization for nba_ingestion.

This package groups together the various modules used by the DataUpdater
service.  Splitting functionality into separate modules makes it easier to
identify issues at build time (module import errors), syntax errors
(detected when compiling individual files) and runtime errors (raised
from within specific client implementations).  Use `data_updater.py` as
the entry point for orchestrating ingestion tasks.
"""

__all__ = [
    "config",
    "nba_api_client",
    "pbpstats_client",
    "bball_index_scraper",
]