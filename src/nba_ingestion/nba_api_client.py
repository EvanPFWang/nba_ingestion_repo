"""Client wrapper for interacting with the nba_api library.

This module encapsulates calls to the official NBA statistics API via
`nba_api`.  By isolating these calls into a class, syntax errors in
third‑party imports and runtime errors during API requests are easier
to diagnose.  The methods return Pandas DataFrames so that upstream
modules can combine and transform the data.
"""

import logging
from typing import Optional

import pandas as pd

try:
    from nba_api.stats.endpoints import leaguegamefinder  # type: ignore
except ImportError:
    leaguegamefinder = None  # type: ignore


logger = logging.getLogger(__name__)


class NBAApiClient:
    """Encapsulate nba_api calls for games, teams and players."""

    def __init__(self, season: Optional[str] = None) -> None:
        self.season = season or "2025-26"

    def fetch_games(self) -> pd.DataFrame:
        """Fetch game metadata for the configured season.

        Returns an empty DataFrame if the nba_api dependency is missing or
        the request fails.  Column names are normalized to lower case.
        """
        if leaguegamefinder is None:
            logger.warning("nba_api not available – returning empty games DataFrame")
            return pd.DataFrame()

        try:
            finder = leaguegamefinder.LeagueGameFinder(season_nullable=self.season)
            games = finder.get_data_frames()[0]
        except Exception as exc:
            logger.error("Failed to fetch games via nba_api: %s", exc)
            return pd.DataFrame()

        games.rename(columns={c: c.lower() for c in games.columns}, inplace=True)
        return games