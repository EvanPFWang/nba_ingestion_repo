"""Player profiles spider for BBall Index / Fanspo

Production-grade spider with:
- Playwright browser rendering
- Multi-source extraction (NEXT_DATA, Apollo, XHR, DOM)
- SQLite checkpointing for resumability
- Conservative pacing with jitter
- Auth session reuse
"""

import json
import logging
import random
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import urljoin

import scrapy
from scrapy import signals
from scrapy.http import Request, Response
from scrapy_playwright.page import PageMethod

from bball_index_scraper.items import PlayerStatItem, PlayerProfileItem
from bball_index_scraper.utils.config import get_settings, validate_auth_state
from bball_index_scraper.utils.url_utils import (
    build_player_url,
    parse_player_url,
    get_all_seasons,
    build_url_key,
    PLAYER_PROFILES_BASE,
)
from bball_index_scraper.utils.extraction import (
    ExtractionStrategy,
    ExtractionSource,
    detect_best_extraction_source,
)
from bball_index_scraper.utils.checkpoint import CheckpointManager
from bball_index_scraper.utils.network_capture import NetworkCapture

logger = logging.getLogger(__name__)


class PlayerProfilesSpider(scrapy.Spider):
    """Spider for scraping BBall Index player profile pages.

    Supports:
    - Input file of player URLs
    - Direct season URL enumeration
    - Fallback season dropdown navigation
    - SQLite checkpointing for resume

    Usage:
        ```
        scrapy crawl player_profiles -a input_file=players.txt
        scrapy crawl player_profiles -a player_id=203932 -a player_slug=aaron-gordon
        ```
    """

    name = "player_profiles"
    allowed_domains = ["fanspo.com"]

    custom_settings = {
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 30000,
    }

    def __init__(
            self,
            input_file: str = None,
            player_id: str = None,
            player_slug: str = None,
            seasons: str = None,  #comma-separated list
            resume: bool = True,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.input_file = input_file
        self.player_id = player_id
        self.player_slug = player_slug
        self.seasons = seasons.split(",") if seasons else None
        self.resume = resume

        self.settings_obj = get_settings()
        self.checkpoint: Optional[CheckpointManager] = None
        self.completed_keys = set()

        #network capture for XHR interception
        self.network_capture = NetworkCapture()

        #validate auth state
        if not validate_auth_state(self.settings_obj):
            logger.warning(
                "Auth state file not found. Run scripts/save_auth_state.py first."
            )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        """Initialize checkpoint on spider open."""
        checkpoint_path = self.settings.get("CHECKPOINT_DB_PATH")
        if checkpoint_path:
            self.checkpoint = CheckpointManager(checkpoint_path)
            self.checkpoint.connect()

            if self.resume:
                self.completed_keys = self.checkpoint.get_completed_keys()
                logger.info("Loaded %d completed keys from checkpoint", len(self.completed_keys))

    def spider_closed(self, spider, reason):
        #"""Close checkpoint on spider close."""
        if self.checkpoint:
            stats = self.checkpoint.get_stats()
            logger.info("Checkpoint stats: %s", stats)
            self.checkpoint.close()

    def start_requests(self) -> Generator[Request, None, None]:
        """Generate initial requests.

        Supports:
        - Input file of URLs
        - Single player with all seasons
        - Default: sample URLs for testing
        """
        urls_to_crawl = []

        if self.input_file:
            urls_to_crawl = self._load_urls_from_file(self.input_file)

        elif self.player_id and self.player_slug:
            #generate URLs for all seasons
            seasons = self.seasons or [s for _, s in get_all_seasons()]
            for season in seasons:
                url = build_player_url(season, self.player_slug, self.player_id)
                urls_to_crawl.append({
                    "url": url,
                    "player_id": self.player_id,
                    "player_slug": self.player_slug,
                    "season": season,
                })

        else:
            #sample URLs for testing
            logger.info("No input specified. Using sample URLs for testing.")
            urls_to_crawl = [
                {
                    "url": f"{PLAYER_PROFILES_BASE}/2024-2025/lebron-james/2544",
                    "player_id": "2544",
                    "player_slug": "lebron-james",
                    "season": "2024-2025",
                },
                {
                    "url": f"{PLAYER_PROFILES_BASE}/2024-2025/stephen-curry/201939",
                    "player_id": "201939",
                    "player_slug": "stephen-curry",
                    "season": "2024-2025",
                },
            ]

        #filter already completed
        for item in urls_to_crawl:
            url = item["url"] if isinstance(item, dict) else item
            player_id = item.get("player_id") if isinstance(item, dict) else None
            season = item.get("season") if isinstance(item, dict) else None

            #check checkpoint
            if player_id and season:
                key = build_url_key(player_id, season)
                if key in self.completed_keys:
                    logger.debug("Skipping completed: %s", url)
                    continue

            yield self._make_request(item)

    def _load_urls_from_file(self, filepath: str) -> List[Dict]:
        """Load player URLs from file.

        Supports formats:
        - Plain URLs (one per line)
        - JSON array of {url, player_id, player_slug, season}
        - JSONL format

        filepath: Path to input file

        returns List of URL dicts"""
        path = Path(filepath)
        if not path.exists():
            logger.error("Input file not found: %s", filepath)
            return []

        content = path.read_text()

        #try JSON array
        try:
            data = json.loads(content)
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            pass

        #try JSONL
        urls = []
        for line in content.strip().split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            try:
                item = json.loads(line)
                urls.append(item)
            except json.JSONDecodeError:
                #plain URL
                parsed = parse_player_url(line)
                if parsed:
                    urls.append({
                        "url": line,
                        "player_id": parsed.player_id,
                        "player_slug": parsed.player_slug,
                        "season": parsed.season,
                    })
                else:
                    urls.append({"url": line})

        return urls

    def _make_request(self, item: Dict) -> Request:
        """Create Playwright request for player profile.

        item: Dict with url and metadata

        returns Scrapy Request with Playwright handling"""
        url = item["url"] if isinstance(item, dict) else item

        meta = {
            "playwright": True,
            "playwright_include_page": True,
            "playwright_context": "default",
            "playwright_page_methods": [
                #wait for content
                PageMethod(
                    "wait_for_selector",
                    "table, [class*='stat'], [class*='player'], #__NEXT_DATA__",
                    timeout=15000,
                ),
                #light scroll for dynamic loading
                PageMethod("evaluate", "window.scrollBy(0, 300)"),
                PageMethod("wait_for_timeout", 500),
            ],
            "errback": self.errback,
        }

        #add item metadata
        if isinstance(item, dict):
            meta["player_id"] = item.get("player_id")
            meta["player_slug"] = item.get("player_slug")
            meta["season"] = item.get("season")

        return Request(
            url=url,
            callback=self.parse_player_profile,
            meta=meta,
            dont_filter=True,
        )

    async def parse_player_profile(self, response: Response) -> Generator:
        """Parse player profile page.

        Tries extraction sources in priority order:
        1. __NEXT_DATA__
        2. Apollo state
        3. XHR JSON
        4. DOM tables

        response: Scrapy Response with Playwright page

        Yields:
            PlayerStatItem for each stat found"""
        url = response.url
        page = response.meta.get("playwright_page")

        player_id = response.meta.get("player_id")
        player_slug = response.meta.get("player_slug")
        season = response.meta.get("season")

        #try to parse from URL if not in meta
        if not all([player_id, player_slug, season]):
            parsed = parse_player_url(url)
            if parsed:
                player_id = player_id or parsed.player_id
                player_slug = player_slug or parsed.player_slug
                season = season or parsed.season

        logger.info("Processing: %s (player=%s, season=%s)", url, player_id, season)

        #mark as started in checkpoint
        if self.checkpoint and player_id and season:
            self.checkpoint.mark_started(
                player_id=player_id,
                season=season,
                player_slug=player_slug,
                url=url,
            )

        try:
            #get page content
            content = response.text

            #capture network responses if available
            network_responses = []
            if hasattr(self, "network_capture"):
                network_responses = self.network_capture.get_json_responses()

            #run extraction
            strategy = ExtractionStrategy(content, network_responses)
            result = strategy.extract()

            if not result.success:
                logger.warning("No stats found for %s - trying DOM fallback", url)
                result = strategy._extract_from_source(ExtractionSource.DOM_TABLES)

            if result.success and result.stats:
                #yield stats
                stat_count = 0
                for stat in result.stats:
                    item = PlayerStatItem()
                    item["player_id"] = player_id or result.player_id
                    item["player_name"] = result.player_name
                    item["player_slug"] = player_slug
                    item["player_url"] = url
                    item["season"] = season
                    item["extraction_source"] = result.source.value
                    item["page_url"] = url
                    item["extracted_at"] = datetime.utcnow().isoformat()

                    #copy stat fields
                    item["statistic_name"] = stat.get("statistic_name")
                    item["statistic_category"] = stat.get("statistic_category", "General")
                    item["value"] = stat.get("value")
                    item["percentile"] = stat.get("percentile")
                    item["grade"] = stat.get("grade")

                    stat_count += 1
                    yield item

                logger.info(
                    "Extracted %d stats from %s [source=%s]",
                    stat_count,
                    url,
                    result.source.value,
                )

                #mark completed
                if self.checkpoint and player_id and season:
                    self.checkpoint.mark_completed(
                        player_id=player_id,
                        season=season,
                        extraction_source=result.source.value,
                        stat_count=stat_count,
                    )

            else:
                #no data found
                logger.warning("No stats extracted from %s", url)
                if self.checkpoint and player_id and season:
                    self.checkpoint.mark_failed(
                        player_id=player_id,
                        season=season,
                        error="No stats found",
                    )

        except Exception as e:
            logger.error("Error processing %s: %s", url, e, exc_info=True)
            if self.checkpoint and player_id and season:
                self.checkpoint.mark_failed(
                    player_id=player_id,
                    season=season,
                    error=str(e),
                )

        finally:
            #close page to prevent leaks
            if page:
                await page.close()

    async def errback(self, failure):
        """Handle request failures.

        failure: Twisted Failure object
        """
        request = failure.request
        url = request.url

        player_id = request.meta.get("player_id")
        season = request.meta.get("season")

        logger.error("Request failed: %s - %s", url, failure.value)

        if self.checkpoint and player_id and season:
            self.checkpoint.mark_failed(
                player_id=player_id,
                season=season,
                error=str(failure.value),
            )

        #close page if present
        page = request.meta.get("playwright_page")
        if page:
            try:
                await page.close()
            except Exception:
                pass
