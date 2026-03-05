"""Client wrapper for interacting with the nba_api library.

Encapsulates calls to nba-api by isolating calls into a class,
with alerts for syntax errors in third‑party imports and runtime errors during API requests are easier
to diagnose.

Supports date-range queries for backfill mode and rate limiting to avoid bans.

return Pandas DataFrames so upstream modules can combine/transform data.
"""

import logging
import time
from datetime import datetime
from typing import List, Optional

import pandas as pd

try:
    from nba_api.stats.endpoints import (
        leaguegamefinder,
        boxscoretraditionalv2,
        playergamelog,
    )
    from nba_api.stats.static import teams, players
except ImportError:
    leaguegamefinder = None
    boxscoretraditionalv2 = None
    playergamelog = None
    teams = None
    players = None

logger = logging.getLogger(__name__)


class NBAApiClient:
    """Encapsulate nba_api calls for games, box scores  teams and players."""

    def __init__(self, season: Optional[str] = None,
                 rate_limit_sleep: float = 0.6,) -> None:
        self.season = season or self._current_season()
        self.rate_limit_sleep = rate_limit_sleep

    @staticmethod
    def _current_season() -> str:
        #derive current NBA season string (ex. '2025-26')
        now = datetime.utcnow()
        year = now.year if now.month >= 10 else now.year - 1
        return f"{year}-{str(year + 1)[-2:]}"

    def _sleep(self) -> None:
        #rate limit between API calls
        if self.rate_limit_sleep > 0:
            time.sleep(self.rate_limit_sleep)
    def fetch_games(
        self,   date_from: Optional[str] = None,date_to: Optional[str] = None,
        season: Optional[str] = None,) -> pd.DataFrame:
        """Fetch game metadata, optionally filtered by date range

        Args:
            date_from: MM/DD/YYYY format or None
            date_to: MM/DD/YYYY format or None
            season: override season string

        Returns:
            DataFrame with game_id, game_date, matchup, etc.
        """
        if leaguegamefinder is None:
            logger.warning("nba_api not available; returning empty DataFrame")
            return pd.DataFrame()

        season = season or self.season
        try:
            finder = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                date_from_nullable=date_from,
                date_to_nullable=date_to,
            )
            games = finder.get_data_frames()[0]
            self._sleep()
        except Exception as exc:
            logger.error("Failed to fetch games: %s", exc)
            return pd.DataFrame()

        games.rename(columns={c: c.lower() for c in games.columns}, inplace=True)
        return games

    def fetch_games_for_date_range(
        self,
        start_date: str,
        end_date: str,
        seasons: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Fetch games across date range, handling multi-season spans.

        Args:
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            seasons: list of season strings to query; auto-derived if None

        Returns:
            Combined DataFrame of all games
        """
        #convert YYYY-MM-DD to MM/DD/YYYY for nba_api
        def fmt(d: str) -> str:
            dt = datetime.strptime(d, "%Y-%m-%d")
            return dt.strftime("%m/%d/%Y")

        date_from = fmt(start_date)
        date_to = fmt(end_date)

        if seasons is None:
            seasons = self._seasons_for_range(start_date, end_date)

        all_games = []
        for season in seasons:
            logger.info("Fetching games for season=%s from %s to %s", season, start_date, end_date)
            df = self.fetch_games(date_from=date_from, date_to=date_to, season=season)
            if not df.empty:
                all_games.append(df)

        if not all_games:
            return pd.DataFrame()
        combined = pd.concat(all_games, ignore_index=True)
        #dedupe by game_id
        if "game_id" in combined.columns:
            combined = combined.drop_duplicates(subset=["game_id"])
        return combined

    @staticmethod
    def _seasons_for_range(start_date: str, end_date: str) -> List[str]:
        #Determine NBA seasons that overlap w/ date range
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        seasons = []
        #nba season runs Oct-Jun; season '2024-25' covers Oct 2024 - Jun 2025
        year = start.year if start.month >= 10 else start.year - 1
        end_year = end.year if end.month >= 10 else end.year - 1
        for y in range(year, end_year + 1):
            seasons.append(f"{y}-{str(y + 1)[-2:]}")
        return seasons

    def fetch_box_scores(self, game_ids: List[str]) -> pd.DataFrame:
        """Fetch traditional box scores for list of game IDs.

        Returns:
            DataFrame with player stats per game
        """
        if boxscoretraditionalv2 is None:
            logger.warning("nba_api not available; returning empty DataFrame")
            return pd.DataFrame()

        all_rows = []
        for idx, game_id in enumerate(game_ids):
            try:
                box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
                player_stats = box.get_data_frames()[0]
                player_stats["game_id"] = game_id
                all_rows.append(player_stats)
            except Exception as exc:
                logger.warning("Failed box score for %s: %s", game_id, exc)
            self._sleep()

        if not all_rows:
            return pd.DataFrame()
        combined = pd.concat(all_rows, ignore_index=True)
        combined.rename(columns={c: c.lower() for c in combined.columns}, inplace=True)
        return combined

    #return static team and player info
    def get_all_teams(self) -> pd.DataFrame:
        if teams is None:
            return pd.DataFrame()
        return pd.DataFrame(teams.get_teams())

    def get_all_players(self) -> pd.DataFrame:
        if players is None:
            return pd.DataFrame()
        return pd.DataFrame(players.get_players())
