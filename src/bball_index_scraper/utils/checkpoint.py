"""Checkpoint management for bball_index_scraper.

Provides SQLite-based progress tracking for resumable crawls:
    Track player-season combinations
    Skip already-completed work
    Record failures for retry
"""

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manage crawl progress checkpoints in SQLite

    Usage:
    ```
        with CheckpointManager(db_path) as cp:
            if not cp.is_completed(player_id, season):
                # do work
                cp.mark_completed(player_id, season, ...)
    ```
    """

    def __init__(self, db_path: Path):
        # init checkpoint manager and db_path: Path to SQLite database file
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def connect(self):
        """Open database connection and create tables."""
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        logger.debug("Checkpoint DB connected: %s", self.db_path)

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.commit()
            self.conn.close()
            self.conn = None

    def _create_tables(self):
        """Create checkpoint tables."""
        cursor = self.conn.cursor()

        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS crawl_progress
                       (
                           id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           player_id
                           TEXT
                           NOT
                           NULL,
                           player_slug
                           TEXT,
                           player_name
                           TEXT,
                           season
                           TEXT
                           NOT
                           NULL,
                           status
                           TEXT
                           NOT
                           NULL
                           DEFAULT
                           'pending',
                           url
                           TEXT,
                           extraction_source
                           TEXT,
                           stat_count
                           INTEGER
                           DEFAULT
                           0,
                           error
                           TEXT,
                           attempts
                           INTEGER
                           DEFAULT
                           0,
                           last_attempt
                           TEXT,
                           completed_at
                           TEXT,
                           created_at
                           TEXT
                           DEFAULT
                           CURRENT_TIMESTAMP,
                           UNIQUE
                       (
                           player_id,
                           season
                       )
                           )
                       """)

        cursor.execute("""
                       CREATE INDEX IF NOT EXISTS idx_progress_status
                           ON crawl_progress(status)
                       """)

        cursor.execute("""
                       CREATE INDEX IF NOT EXISTS idx_progress_player
                           ON crawl_progress(player_id)
                       """)

        cursor.execute("""
                       CREATE INDEX IF NOT EXISTS idx_progress_season
                           ON crawl_progress(season)
                       """)

        self.conn.commit()

    def is_completed(self, player_id: str, season: str) -> bool:
        """Check if player-season already successfully scraped

        player_id: Player ID
        season: Season string

       ret True if already completed successfully.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
                       SELECT 1
                       FROM crawl_progress
                       WHERE player_id = ?
                         AND season = ?
                         AND status = 'success'
                       """, (player_id, season))
        return cursor.fetchone() is not None

    def is_pending(self, player_id: str, season: str) -> bool:
        """Check if player-season is pending (not started or failed)

        player_id: Player ID
        season: Season string

        ret True if should be (re)processed.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
                       SELECT status, attempts
                       FROM crawl_progress
                       WHERE player_id = ?
                         AND season = ?
                       """, (player_id, season))

        row = cursor.fetchone()
        if row is None:
            return True  # never attempted

        status = row["status"]
        attempts = row["attempts"]

        # allow retry if failed but under max attempts
        if status == "failed" and attempts < 3:
            return True

        return status != "success"

    def mark_started(
            self,
            player_id: str,
            season: str,
            player_slug: str = None,
            player_name: str = None,
            url: str = None,
    ):
        """Mark player-season as in progress

        player_id: Player ID
        season: Season string
        player_slug: Player URL slug
        player_name: Player name
        url: Page URL
        """
        cursor = self.conn.cursor()
        now = datetime.utcnow().isoformat()

        cursor.execute("""
                       INSERT INTO crawl_progress
                       (player_id, player_slug, player_name, season, status, url, last_attempt, attempts)
                       VALUES (?, ?, ?, ?, 'in_progress', ?, ?, 1) ON CONFLICT(player_id, season) DO
                       UPDATE SET
                           status = 'in_progress',
                           player_slug = COALESCE (excluded.player_slug, player_slug),
                           player_name = COALESCE (excluded.player_name, player_name),
                           url = excluded.url,
                           last_attempt = excluded.last_attempt,
                           attempts = attempts + 1
                       """, (player_id, player_slug, player_name, season, url, now))

        self.conn.commit()

    def mark_completed(
            self,
            player_id: str,
            season: str,
            extraction_source: str = None,
            stat_count: int = 0,
    ):
        """Mark player-season as successfully completed.


        player_id: Player ID
        season: Season string
        extraction_source: Source used for extraction
        stat_count: Number of stats extracted
        """
        cursor = self.conn.cursor()
        now = datetime.utcnow().isoformat()

        cursor.execute("""
                       UPDATE crawl_progress
                       SET status            = 'success',
                           extraction_source = ?,
                           stat_count        = ?,
                           completed_at      = ?,
                           error             = NULL
                       WHERE player_id = ?
                         AND season = ?
                       """, (extraction_source, stat_count, now, player_id, season))

        self.conn.commit()

    def mark_failed(
            self,
            player_id: str,
            season: str,
            error: str,
    ):
        """Mark player-season as failed.

        player_id: Player ID
        season: Season string
        error: Error message
        """
        cursor = self.conn.cursor()
        now = datetime.utcnow().isoformat()

        cursor.execute("""
                       UPDATE crawl_progress
                       SET status       = 'failed',
                           error        = ?,
                           last_attempt = ?
                       WHERE player_id = ?
                         AND season = ?
                       """, (error, now, player_id, season))

        self.conn.commit()

    def mark_skipped(
            self,
            player_id: str,
            season: str,
            reason: str = "unavailable",
    ):
        """Mark player-season as skipped (not available).

        player_id: Player ID
        season: Season string
        reason: Skip reason
        """
        cursor = self.conn.cursor()
        now = datetime.utcnow().isoformat()

        cursor.execute("""
                       INSERT INTO crawl_progress
                           (player_id, season, status, error, last_attempt)
                       VALUES (?, ?, 'skipped', ?, ?) ON CONFLICT(player_id, season) DO
                       UPDATE SET
                           status = 'skipped',
                           error = excluded.error,
                           last_attempt = excluded.last_attempt
                       """, (player_id, season, reason, now))

        self.conn.commit()

    def get_completed_keys(self) -> Set[str]:
        """Get all completed player-season keys

        ret set of "player_id_season" keys
        """
        cursor = self.conn.cursor()
        cursor.execute("""
                       SELECT player_id, season
                       FROM crawl_progress
                       WHERE status = 'success'
                       """)

        return {f"{row['player_id']}_{row['season']}" for row in cursor.fetchall()}

    def get_pending_items(self, max_items: int = None) -> List[Tuple[str, str, str]]:
        """Get pending items to process

        max_items: Maximum items to return

        return list of (player_id, season, url) tuples
        """
        cursor = self.conn.cursor()

        query = """
                SELECT player_id, season, url \
                FROM crawl_progress
                WHERE status IN ('pending', 'failed') \
                  AND attempts < 3
                ORDER BY created_at ASC \
                """

        if max_items:
            query += f" LIMIT {max_items}"

        cursor.execute(query)
        return [(row["player_id"], row["season"], row["url"]) for row in cursor.fetchall()]

    def get_stats(self) -> dict:
        # Get checkpoint statistics + returns dict with counts by status.
        cursor = self.conn.cursor()
        cursor.execute("""
                       SELECT status, COUNT(*) as count
                       FROM crawl_progress
                       GROUP BY status
                       """)

        stats = {row["status"]: row["count"] for row in cursor.fetchall()}
        stats["total"] = sum(stats.values())
        return stats

    def reset_failed(self, max_attempts: int = 3):
        """Reset failed items for retry.

        max_attempts: Only reset items with fewer attempts
        """
        cursor = self.conn.cursor()
        cursor.execute("""
                       UPDATE crawl_progress
                       SET status = 'pending',
                           error  = NULL
                       WHERE status = 'failed'
                         AND attempts < ?
                       """, (max_attempts,))

        affected = cursor.rowcount
        self.conn.commit()

        logger.info("Reset %d failed items for retry", affected)
        return affected


@contextmanager
def checkpoint_session(db_path: Path) -> Generator[CheckpointManager, None, None]:
    """Context manager for checkpoint database session.

    db_path: Path to checkpoint database

    yield CheckpointManager instance
    """
    manager = CheckpointManager(db_path)
    manager.connect()
    try:
        yield manager
    finally:
        manager.close()
