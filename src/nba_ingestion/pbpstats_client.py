"""Uses pbpstats' documented Client/Game/Possessions flow
to fetch possession-level data for a list of game IDs and flatten into
a Pandas DataFrame.
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    import pbpstats  # noqa: F401
    from pbpstats.client import Client
except ImportError:
    pbpstats = None  # type: ignore
    Client = None  # type: ignore

logger = logging.getLogger(__name__)


class PBPStatsClient:
    """Encapsulate pbpstats API calls."""

    def __init__(
        self,
        rate_limit_sleep: float = 0.5,
        data_provider: str = "data_nba",
        source: str = "web",
        data_dir: Optional[str] = None,
    ) -> None:
        self.rate_limit_sleep = rate_limit_sleep
        self.data_provider = data_provider
        self.source = source
        self.data_dir = data_dir or os.environ.get("PBP_STATS_DATA_DIRECTORY")

    def _build_settings(self) -> Dict[str, Any]:
        settings: Dict[str, Any] = {
            "Possessions": {
                "source": self.source,
                "data_provider": self.data_provider,
            }
        }
        if self.data_dir:
            settings["dir"] = self.data_dir
        return settings

    @staticmethod
    def _safe_get(obj: Any, attr: str, default: Any = None) -> Any:
        try:
            return getattr(obj, attr)
        except Exception:
            return default

    @staticmethod
    def _normalize_game_ids(game_ids: List[str]) -> List[str]:
        seen = set()
        normalized: List[str] = []
        for game_id in game_ids:
            if game_id is None:
                continue
            gid = str(game_id).strip()
            if not gid or gid in seen:
                continue
            seen.add(gid)
            normalized.append(gid)
        return normalized

    def _flatten_possession(self, game_id: str, possession: Any, fallback_idx: int) -> Dict[str, Any]:
        events = self._safe_get(possession, "events", []) or []
        first_event = events[0] if events else None
        last_event = events[-1] if events else None

        prev_event = self._safe_get(possession, "previous_possession_ending_event")
        prev_event_type = type(prev_event).__name__ if prev_event is not None else None

        team_ids: List[Any] = []
        try:
            team_ids = possession.get_team_ids() or []
        except Exception:
            team_ids = []

        return {
            "game_id": str(game_id),
            "possession_number": self._safe_get(possession, "number", fallback_idx + 1),
            "period": self._safe_get(first_event, "period"),
            "start_time": self._safe_get(possession, "start_time"),
            "end_time": self._safe_get(possession, "end_time"),
            "offense_team_id": self._safe_get(possession, "offense_team_id"),
            "team_ids": ",".join(str(team_id) for team_id in team_ids) if team_ids else None,
            "start_score_margin": self._safe_get(possession, "start_score_margin"),
            "possession_start_type": self._safe_get(possession, "possession_start_type"),
            "possession_has_timeout": self._safe_get(possession, "possession_has_timeout"),
            "previous_possession_has_timeout": self._safe_get(possession, "previous_possession_has_timeout"),
            "previous_possession_ending_event_type": prev_event_type,
            "previous_possession_end_shooter_player_id": self._safe_get(
                possession, "previous_possession_end_shooter_player_id"
            ),
            "previous_possession_end_rebound_player_id": self._safe_get(
                possession, "previous_possession_end_rebound_player_id"
            ),
            "previous_possession_end_steal_player_id": self._safe_get(
                possession, "previous_possession_end_steal_player_id"
            ),
            "previous_possession_end_turnover_player_id": self._safe_get(
                possession, "previous_possession_end_turnover_player_id"
            ),
            "event_count": len(events),
            "start_score": self._safe_get(first_event, "score"),
            "end_score": self._safe_get(last_event, "score"),
        }

    def fetch_possession_stats(self, game_ids: List[str]) -> pd.DataFrame:
        """Fetch possession-level rows for a list of game IDs.

        Returns one row per possession.
        """
        if Client is None:
            logger.warning("pbpstats not available – returning empty DataFrame")
            return pd.DataFrame()

        game_ids = self._normalize_game_ids(game_ids)
        if not game_ids:
            logger.info("No game IDs provided to pbpstats – returning empty DataFrame")
            return pd.DataFrame()

        logger.info(
            "Fetching possession stats for %d games using provider=%s source=%s",
            len(game_ids),
            self.data_provider,
            self.source,
        )

        client = Client(self._build_settings())
        rows: List[Dict[str, Any]] = []

        for idx, game_id in enumerate(game_ids):
            try:
                game = client.Game(game_id)
                possessions_resource = self._safe_get(game, "possessions")
                possession_items = self._safe_get(possessions_resource, "items", []) or []

                if not possession_items:
                    logger.warning("No possession items returned for game %s", game_id)
                    continue

                for p_idx, possession in enumerate(possession_items):
                    rows.append(self._flatten_possession(game_id, possession, p_idx))

            except Exception as exc:
                logger.warning("Failed to fetch pbpstats possessions for game %s: %s", game_id, exc)

            if self.rate_limit_sleep > 0 and idx < len(game_ids) - 1:
                time.sleep(self.rate_limit_sleep)

        df = pd.DataFrame(rows)

        if df.empty:
            logger.warning("pbpstats returned no possession rows")
            return df

        numeric_columns = [
            "possession_number",
            "period",
            "offense_team_id",
            "start_score_margin",
            "previous_possession_end_shooter_player_id",
            "previous_possession_end_rebound_player_id",
            "previous_possession_end_steal_player_id",
            "previous_possession_end_turnover_player_id",
            "event_count",
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df