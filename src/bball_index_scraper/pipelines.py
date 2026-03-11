"""Scrapy pipelines for bball_index_scraper.

Includes:
- NormalizationPipeline: standardize values, handle missing data
- CheckpointPipeline: track progress in SQLite for resumability
- JsonlExportPipeline: export items to JSONL files
- SqliteDataPipeline: store normalized data in SQLite
"""

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from scrapy import Spider
from scrapy.exceptions import DropItem

from bball_index_scraper.items import PlayerStatItem, PlayerProfileItem
from bball_index_scraper.utils.normalization import normalize_stat_value, normalize_grade


logger = logging.getLogger(__name__)


class NormalizationPipeline:
    """Normalize item values according to configuration.
    
    Handles:
    - Numeric missing sentinel (default: "00000")
    - Non-numeric null handling
    - Grade standardization
    - Percentile conversion
    """
    
    def __init__(self, missing_numeric_sentinel: str, missing_string_value: Any):
        self.missing_numeric_sentinel = missing_numeric_sentinel
        self.missing_string_value = missing_string_value
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            missing_numeric_sentinel=crawler.settings.get("MISSING_NUMERIC_SENTINEL", "00000"),
            missing_string_value=crawler.settings.get("MISSING_STRING_VALUE", None),
        )
    
    def process_item(self, item, spider: Spider):
        #Normalize item values
        
        if isinstance(item, PlayerStatItem):
            return self._normalize_stat_item(item)
        elif isinstance(item, PlayerProfileItem):
            return self._normalize_profile_item(item)
        
        return item
    
    def _normalize_stat_item(self, item: PlayerStatItem) -> PlayerStatItem:
        #Normalize a single stat item
        
        #store raw value before normalization
        if "raw_data" not in item or not item.get("raw_data"):
            item["raw_data"] = {
                "value": item.get("value"),
                "percentile": item.get("percentile"),
                "grade": item.get("grade"),
            }
        
        #normalize value
        raw_value = item.get("value")
        if raw_value is None or raw_value == "" or raw_value == "-":
            item["value"] = self.missing_numeric_sentinel
            item["value_numeric"] = None
        else:
            item["value_numeric"] = normalize_stat_value(raw_value)
        
        #normalize percentile
        raw_pct = item.get("percentile")
        if raw_pct is None or raw_pct == "" or raw_pct == "-":
            item["percentile"] = self.missing_string_value
            item["percentile_numeric"] = None
        else:
            pct_str = str(raw_pct).rstrip("%")
            try:
                item["percentile_numeric"] = float(pct_str) / 100.0
            except ValueError:
                item["percentile_numeric"] = None
        
        #normalize grade
        raw_grade = item.get("grade")
        item["grade"] = normalize_grade(raw_grade) if raw_grade else self.missing_string_value
        
        #set completeness flag
        item["is_complete"] = (
            item.get("statistic_name") is not None and
            item.get("value") != self.missing_numeric_sentinel
        )
        
        #add extraction timestamp if missing
        if "extracted_at" not in item or not item.get("extracted_at"):
            item["extracted_at"] = datetime.utcnow().isoformat()
        
        return item
    
    def _normalize_profile_item(self, item: PlayerProfileItem) -> PlayerProfileItem:
        #Normalize a profile item with nested stats
        
        if "stats" in item and item["stats"]:
            normalized_stats = []
            for stat in item["stats"]:
                #create temp stat item for normalization
                stat_item = PlayerStatItem()
                stat_item.update(stat)
                normalized = self._normalize_stat_item(stat_item)
                normalized_stats.append(dict(normalized))
            item["stats"] = normalized_stats
        
        if "extracted_at" not in item or not item.get("extracted_at"):
            item["extracted_at"] = datetime.utcnow().isoformat()
        
        return item


class CheckpointPipeline:
    """Track crawl progress in SQLite for resumability.
    
    Records:
    - Player-season combinations attempted
    - Success/failure status
    - Retry counts
    - Extraction source used
    """
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
    
    @classmethod
    def from_crawler(cls, crawler):
        db_path = crawler.settings.get("CHECKPOINT_DB_PATH")
        return cls(db_path)
    
    def open_spider(self, spider: Spider):
        #Initialize checkpoint database
        self.conn = sqlite3.connect(str(self.db_path))
        self._create_tables()
        logger.info("Checkpoint database initialized: %s", self.db_path)
    
    def close_spider(self, spider: Spider):
        #Close database connection
        if self.conn:
            self.conn.commit()
            self.conn.close()
    
    def _create_tables(self):
        #Create checkpoint tables if not exist
        cursor = self.conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crawl_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id TEXT NOT NULL,
                player_slug TEXT NOT NULL,
                season TEXT NOT NULL,
                status TEXT NOT NULL,
                url TEXT,
                extraction_source TEXT,
                stat_count INTEGER DEFAULT 0,
                error TEXT,
                attempts INTEGER DEFAULT 0,
                last_attempt TEXT,
                completed_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(player_id, season)
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_progress_status 
            ON crawl_progress(status)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_progress_season 
            ON crawl_progress(season)
        """)
        
        self.conn.commit()
    
    def process_item(self, item, spider: Spider):
        """Record successful extraction in checkpoint"""
        
        player_id = item.get("player_id", "")
        season = item.get("season", "")
        
        if not player_id or not season:
            return item
        
        stat_count = 1 if isinstance(item, PlayerStatItem) else len(item.get("stats", []))
        
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO crawl_progress 
            (player_id, player_slug, season, status, url, extraction_source, stat_count, completed_at)
            VALUES (?, ?, ?, 'success', ?, ?, ?, ?)
            ON CONFLICT(player_id, season) DO UPDATE SET
                status = 'success',
                extraction_source = excluded.extraction_source,
                stat_count = stat_count + excluded.stat_count,
                completed_at = excluded.completed_at
        """, (
            player_id,
            item.get("player_slug", ""),
            season,
            item.get("page_url", ""),
            item.get("extraction_source", ""),
            stat_count,
            datetime.utcnow().isoformat(),
        ))
        
        self.conn.commit()
        return item
    
    def record_failure(self, player_id: str, player_slug: str, season: str, 
                       url: str, error: str):
        """Record failed extraction attempt"""
        if not self.conn:
            return
        
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO crawl_progress 
            (player_id, player_slug, season, status, url, error, attempts, last_attempt)
            VALUES (?, ?, ?, 'failed', ?, ?, 1, ?)
            ON CONFLICT(player_id, season) DO UPDATE SET
                status = 'failed',
                error = excluded.error,
                attempts = attempts + 1,
                last_attempt = excluded.last_attempt
        """, (
            player_id,
            player_slug,
            season,
            url,
            error,
            datetime.utcnow().isoformat(),
        ))
        
        self.conn.commit()
    
    def is_completed(self, player_id: str, season: str) -> bool:
        """Check if player-season already successfully scraped"""
        if not self.conn:
            return False
        
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT status FROM crawl_progress 
            WHERE player_id = ? AND season = ? AND status = 'success'
        """, (player_id, season))
        
        return cursor.fetchone() is not None


class JsonlExportPipeline:
    """Export items to JSONL files.
    
    Creates timestamped JSONL files in exports directory.
    """
    
    def __init__(self, exports_dir: Path):
        self.exports_dir = exports_dir
        self.file = None
        self.item_count = 0
    
    @classmethod
    def from_crawler(cls, crawler):
        exports_dir = crawler.settings.get("EXPORTS_DIR")
        return cls(exports_dir)
    
    def open_spider(self, spider: Spider):
        #Open JSONL export file"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"player_stats_{timestamp}.jsonl"
        filepath = self.exports_dir / filename
        
        self.file = open(filepath, "w", encoding="utf-8")
        logger.info("Opened JSONL export: %s", filepath)
    
    def close_spider(self, spider: Spider):
        """Close JSONL file"""
        if self.file:
            self.file.close()
            logger.info("Closed JSONL export with %d items", self.item_count)
    
    def process_item(self, item, spider: Spider):
        """Write item to JSONL file"""
        if self.file:
            line = json.dumps(dict(item), ensure_ascii=False, default=str)
            self.file.write(line + "\n")
            self.item_count += 1
        
        return item


class SqliteDataPipeline:
    """Store normalized data in SQLite database.
    
    Creates denormalized tables for:
    - Player stats (one row per stat)
    - Player profiles (one row per player-season)
    """
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
    
    @classmethod
    def from_crawler(cls, crawler):
        db_path = crawler.settings.get("DATA_DB_PATH")
        return cls(db_path)
    
    def open_spider(self, spider: Spider):
        #Initialize data database"""
        self.conn = sqlite3.connect(str(self.db_path))
        self._create_tables()
        logger.info("Data database initialized: %s", self.db_path)
    
    def close_spider(self, spider: Spider):
        #Close database connection"""
        if self.conn:
            self.conn.commit()
            self.conn.close()
    
    def _create_tables(self):
        #Create data tables"""
        cursor = self.conn.cursor()
        
        #player stats table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id TEXT NOT NULL,
                player_name TEXT,
                player_slug TEXT,
                season TEXT NOT NULL,
                statistic_name TEXT NOT NULL,
                statistic_category TEXT,
                value TEXT,
                value_numeric REAL,
                percentile TEXT,
                percentile_numeric REAL,
                grade TEXT,
                extraction_source TEXT,
                extracted_at TEXT,
                page_url TEXT,
                is_complete INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(player_id, season, statistic_name)
            )
        """)
        
        #indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_stats_player 
            ON player_stats(player_id)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_stats_season 
            ON player_stats(season)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_stats_category 
            ON player_stats(statistic_category)
        """)
        
        self.conn.commit()
    
    def process_item(self, item, spider: Spider):
        #Insert item into database"""
        
        if isinstance(item, PlayerStatItem):
            self._insert_stat(item)
        elif isinstance(item, PlayerProfileItem):
            self._insert_profile(item)
        
        return item
    
    def _insert_stat(self, item: PlayerStatItem):
        #Insert single stat row"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            INSERT INTO player_stats 
            (player_id, player_name, player_slug, season, statistic_name,
             statistic_category, value, value_numeric, percentile, 
             percentile_numeric, grade, extraction_source, extracted_at,
             page_url, is_complete)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(player_id, season, statistic_name) DO UPDATE SET
                value = excluded.value,
                value_numeric = excluded.value_numeric,
                percentile = excluded.percentile,
                percentile_numeric = excluded.percentile_numeric,
                grade = excluded.grade,
                extraction_source = excluded.extraction_source,
                extracted_at = excluded.extracted_at
        """, (
            item.get("player_id"),
            item.get("player_name"),
            item.get("player_slug"),
            item.get("season"),
            item.get("statistic_name"),
            item.get("statistic_category"),
            item.get("value"),
            item.get("value_numeric"),
            item.get("percentile"),
            item.get("percentile_numeric"),
            item.get("grade"),
            item.get("extraction_source"),
            item.get("extracted_at"),
            item.get("page_url"),
            1 if item.get("is_complete") else 0,
        ))
        
        self.conn.commit()
    
    def _insert_profile(self, item: PlayerProfileItem):
        #Insert profile with nested stats"""
        stats = item.get("stats", [])
        for stat in stats:
            stat_item = PlayerStatItem()
            stat_item["player_id"] = item.get("player_id")
            stat_item["player_name"] = item.get("player_name")
            stat_item["player_slug"] = item.get("player_slug")
            stat_item["season"] = item.get("season")
            stat_item["extraction_source"] = item.get("extraction_source")
            stat_item["extracted_at"] = item.get("extracted_at")
            stat_item["page_url"] = item.get("page_url")
            stat_item.update(stat)
            self._insert_stat(stat_item)
