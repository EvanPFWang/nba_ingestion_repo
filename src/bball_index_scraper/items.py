"""Scrapy Items for BBall Index player stats.

Defines structured items for player profile data with:
- Player identification
- Season context
- Stat categories and values
- Extraction metadata
"""

import scrapy
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
from datetime import datetime


class PlayerStatItem(scrapy.Item):
    #Single stat row from a player profile

    #player identification
    player_id = scrapy.Field()
    player_name = scrapy.Field()
    player_slug = scrapy.Field()
    player_url = scrapy.Field()

    #season context
    season = scrapy.Field()
    season_key = scrapy.Field()  #"0".."13" mapping

    #stat data
    statistic_name = scrapy.Field()
    statistic_category = scrapy.Field()
    value = scrapy.Field()
    value_numeric = scrapy.Field()
    percentile = scrapy.Field()
    percentile_numeric = scrapy.Field()
    grade = scrapy.Field()

    #extraction metadata
    extraction_source = scrapy.Field()  #next_data, apollo_state, xhr_json, dom_tables
    extracted_at = scrapy.Field()
    page_url = scrapy.Field()

    #quality flags
    is_complete = scrapy.Field()
    raw_data = scrapy.Field()  #original data before normalization


class PlayerProfileItem(scrapy.Item):
    #Complete player profile with all stats for a season

    #player identification
    player_id = scrapy.Field()
    player_name = scrapy.Field()
    player_slug = scrapy.Field()
    player_url = scrapy.Field()

    #season context
    season = scrapy.Field()
    season_key = scrapy.Field()

    #all stats grouped by category
    stats = scrapy.Field()  #List[Dict] of stat rows

    #profile metadata
    team = scrapy.Field()
    position = scrapy.Field()
    height = scrapy.Field()
    weight = scrapy.Field()

    #extraction metadata
    extraction_source = scrapy.Field()
    extracted_at = scrapy.Field()
    page_url = scrapy.Field()
    raw_payload = scrapy.Field()  #original structured data


@dataclass
class ExtractionResult:
    #Result from extraction attempt."""
    success: bool
    source: str  #extraction source type
    data: Optional[Dict[str, Any]] = None
    stats: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CrawlProgress:
    #Crawl progress tracking
    player_id: str
    player_slug: str
    season: str
    status: str  #pending, success, failed, skipped
    url: str
    extraction_source: Optional[str] = None
    stat_count: int = 0
    error: Optional[str] = None
    attempts: int = 0
    last_attempt: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        #convert datetime to ISO string
        if d["last_attempt"]:
            d["last_attempt"] = d["last_attempt"].isoformat()
        if d["completed_at"]:
            d["completed_at"] = d["completed_at"].isoformat()
        return d
