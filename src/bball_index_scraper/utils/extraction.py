"""Extraction utilities for bball_index_scraper.

Implements extraction priority:
1. __NEXT_DATA__ script tag (Next.js server-side data)
2. Apollo Client cache state
3. Intercepted XHR/fetch JSON responses
4. DOM table scraping (fallback)

Prefers structured payloads over DOM scraping.
"""

import json
import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class ExtractionSource(Enum):
    # Extraction source types
    NEXT_DATA = "next_data"
    APOLLO_STATE = "apollo_state"
    XHR_JSON = "xhr_json"
    DOM_TABLES = "dom_tables"
    UNKNOWN = "unknown"


@dataclass
class ExtractionResult:
    # Result from extraction attempt
    success: bool
    source: ExtractionSource
    player_id: Optional[str] = None
    player_name: Optional[str] = None
    season: Optional[str] = None
    stats: List[Dict[str, Any]] = None
    raw_data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.stats is None:
            self.stats = []


class ExtractionStrategy:
    """Multi-source extraction strategy.

    Tries extraction sources in priority order until one succeeds.
    """

    PRIORITY = [ExtractionSource.NEXT_DATA,
                ExtractionSource.APOLLO_STATE,
                ExtractionSource.XHR_JSON,
                ExtractionSource.DOM_TABLES, ]

    def __init__(self, page_content: str, network_responses: Optional[List[Dict]] = None):
        """Initialize extraction strategy.

        page_content: HTML page content
        network_responses: Intercepted network JSON responses
        """
        self.page_content = page_content
        self.soup = BeautifulSoup(page_content, "html.parser")
        self.network_responses = network_responses or []

    def extract(self) -> ExtractionResult:
        """Try extraction from all sources in priority order.

        returns ExtractionResult from first successful source.
        """
        for source in self.PRIORITY:
            try:
                result = self._extract_from_source(source)
                if result.success and result.stats:
                    logger.info(
                        "Extraction succeeded from %s: %d stats found",
                        source.value,
                        len(result.stats),
                    )
                    return result
            except Exception as e:
                logger.warning("Extraction from %s failed: %s", source.value, e)
                continue

        # all sources failed
        return ExtractionResult(
            success=False,
            source=ExtractionSource.UNKNOWN,
            error="All extraction sources failed",
        )

    def _extract_from_source(self, source: ExtractionSource) -> ExtractionResult:
        # Extract from specific source
        if source == ExtractionSource.NEXT_DATA:
            return extract_next_data(self.page_content)
        elif source == ExtractionSource.APOLLO_STATE:
            return extract_apollo_state(self.page_content)
        elif source == ExtractionSource.XHR_JSON:
            return extract_from_xhr(self.network_responses)
        elif source == ExtractionSource.DOM_TABLES:
            return extract_from_dom(self.page_content)
        else:
            return ExtractionResult(success=False, source=source, error="Unknown source")

    def detect_best_source(self) -> Tuple[ExtractionSource, Optional[Dict]]:
        """Detect which source likely has the richest data.

        Returns Tuple of (best_source, sample_data).
        """
        results = {}

        # check __NEXT_DATA__
        next_data = self._find_next_data()
        if next_data:
            results[ExtractionSource.NEXT_DATA] = {
                "has_data": True,
                "keys": list(next_data.keys()) if isinstance(next_data, dict) else [],
                "stats_found": self._count_stats_in_data(next_data), }

        # check Apollo state
        apollo_data = self._find_apollo_state()
        if apollo_data:
            results[ExtractionSource.APOLLO_STATE] = {
                "has_data": True,
                "keys": list(apollo_data.keys()) if isinstance(apollo_data, dict) else [],
                "stats_found": self._count_stats_in_data(apollo_data), }

        # check XHR responses
        if self.network_responses:
            for resp in self.network_responses:
                stats = self._count_stats_in_data(resp)
                if stats > 0:
                    results[ExtractionSource.XHR_JSON] = {
                        "has_data": True,
                        "stats_found": stats, }
                    break

        # check DOM tables
        tables = self.soup.select("table")
        if tables:
            results[ExtractionSource.DOM_TABLES] = {"has_data": True,
                                                    "table_count": len(tables),
                                                    "stats_found": self._count_table_rows(), }

        # find best source by stats count
        best_source = ExtractionSource.UNKNOWN
        best_count = 0
        best_data = None

        for source, info in results.items():
            count = info.get("stats_found", 0)
            if count > best_count:
                best_count = count
                best_source = source
                best_data = info
        return best_source, best_data

    def _find_next_data(self) -> Optional[Dict]:
        # Find and parse __NEXT_DATA__ script tag
        script = self.soup.find("script", id="__NEXT_DATA__")
        if script and script.string:
            try:
                return json.loads(script.string)
            except json.JSONDecodeError:
                pass
        return None

    def _find_apollo_state(self) -> Optional[Dict]:
        # Find Apollo Client state in page
        # look for __APOLLO_STATE__ in scripts
        for script in self.soup.find_all("script"):
            if script.string and "__APOLLO_STATE__" in script.string:
                match = re.search(r"__APOLLO_STATE__\s*=\s*(\{.+?\});", script.string, re.DOTALL)
                if match:
                    try:
                        return json.loads(match.group(1))
                    except json.JSONDecodeError:
                        pass
        return None

    def _count_stats_in_data(self, data: Any, depth: int = 0) -> int:
        # Recursively count stat-like entries in data structure
        if depth > 10:  return 0
        count = 0

        if isinstance(data, dict):
            # look for stat-like keys
            stat_keys = ["value", "percentile", "grade", "stat", "score"]
            if any(k.lower() in [sk.lower() for sk in data.keys()] for k in stat_keys): count += 1
            for v in data.values(): count += self._count_stats_in_data(v, depth + 1)

        elif isinstance(data, list):
            for item in data[:100]: count += self._count_stats_in_data(item, depth + 1)
            # limit to first 100

        return count

    def _count_table_rows(self) -> int:
        # Count data rows in tables
        count = 0
        for table in self.soup.select("table"):
            rows = table.select("tbody tr")
            count += len(rows)
        return count


def extract_next_data(page_content: str) -> ExtractionResult:
    """Extract stats from __NEXT_DATA__ script tag.

    page_content: HTML page content

    Returns returns ExtractionResult with parsed stats"""
    soup = BeautifulSoup(page_content, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")

    if not script or not script.string:
        return ExtractionResult(
            success=False,
            source=ExtractionSource.NEXT_DATA,
            error="No __NEXT_DATA__ script found",
        )

    try:
        data = json.loads(script.string)
    except json.JSONDecodeError as e:
        return ExtractionResult(
            success=False,
            source=ExtractionSource.NEXT_DATA,
            error=f"JSON decode error: {e}",
        )

    # navigate to page props
    props = data.get("props", {}).get("pageProps", {})

    # extract player info
    player_id = None
    player_name = None
    season = None
    stats = []

    # look for player data in various locations
    player_data = (props.get("player") or props.get("playerData")
                   or props.get("data", {}).get("player") or {})

    if player_data:
        player_id = str(player_data.get("id", player_data.get("playerId", "")))
        player_name = player_data.get("name", player_data.get("playerName", ""))

    # look for stats
    stats_data = (props.get("stats") or props.get("playerStats") or
                  player_data.get("stats") or player_data.get("statistics") or []
                  )

    # parse stats into standard format
    if isinstance(stats_data, list):
        for stat in stats_data:
            parsed = _parse_stat_entry(stat)
            if parsed:
                stats.append(parsed)
    elif isinstance(stats_data, dict):
        # stats might be grouped by category
        for category, category_stats in stats_data.items():
            if isinstance(category_stats, list):
                for stat in category_stats:
                    parsed = _parse_stat_entry(stat, category)
                    if parsed:
                        stats.append(parsed)

    # look for season
    season = (props.get("season") or player_data.get("season") or data.get("query", {}).get("season"))

    return ExtractionResult(
        success=len(stats) > 0,
        source=ExtractionSource.NEXT_DATA,
        player_id=player_id,
        player_name=player_name,
        season=season,
        stats=stats,
        raw_data=props,
    )


def extract_apollo_state(page_content: str) -> ExtractionResult:
    """Extract stats from Apollo Client cache state.

    page_content: HTML page content

    Returns ExtractionResult with parsed stats.
    """
    soup = BeautifulSoup(page_content, "html.parser")

    apollo_data = None

    for script in soup.find_all("script"):
        if script.string and "__APOLLO_STATE__" in script.string:
            match = re.search(r"__APOLLO_STATE__\s*=\s*(\{.+?\});", script.string, re.DOTALL)
            if match:
                try:
                    apollo_data = json.loads(match.group(1))
                    break
                except json.JSONDecodeError:
                    continue

    if not apollo_data:
        return ExtractionResult(success=False,
                                source=ExtractionSource.APOLLO_STATE, error="No Apollo state found", )

    # parse Apollo cache entries
    stats = []
    player_id = None
    player_name = None

    for key, value in apollo_data.items():
        if not isinstance(value, dict):
            continue

        # look for player entries
        if "Player:" in key or "__typename" in value and value.get("__typename") == "Player":
            player_id = value.get("id") or key.split(":")[-1]
            player_name = value.get("name")

        # look for stat entries
        if "Stat:" in key or (value.get("__typename", "").lower().find("stat") >= 0):
            parsed = _parse_stat_entry(value)
            if parsed:
                stats.append(parsed)

    return ExtractionResult(
        success=len(stats) > 0,
        source=ExtractionSource.APOLLO_STATE,
        player_id=player_id,
        player_name=player_name,
        stats=stats,
        raw_data=apollo_data,
    )


def extract_from_xhr(network_responses: List[Dict]) -> ExtractionResult:
    """Extract stats from intercepted XHR/fetch responses.

    network_responses: List of captured JSON responses

    returns ExtractionResult with parsed stats.
    """
    if not network_responses:
        return ExtractionResult(
            success=False,
            source=ExtractionSource.XHR_JSON,
            error="No network responses captured",
        )

    stats = []
    player_id = None
    player_name = None
    best_response = None

    for response in network_responses:
        if not isinstance(response, dict):
            continue

        # look for player stats in response
        stats_data = (response.get("stats") or response.get("playerStats") or
                      response.get("data", {}).get("stats") or response.get("data", {}).get("player", {}).get(
                    "stats") or [])

        if isinstance(stats_data, list) and len(stats_data) > len(stats):
            stats = []
            for stat in stats_data:
                parsed = _parse_stat_entry(stat)
                if parsed:
                    stats.append(parsed)

            if stats:
                best_response = response
                player_id = response.get("playerId") or response.get("data", {}).get("player", {}).get("id")
                player_name = response.get("playerName") or response.get("data", {}).get("player", {}).get("name")

    return ExtractionResult(
        success=len(stats) > 0,
        source=ExtractionSource.XHR_JSON,
        player_id=str(player_id) if player_id else None,
        player_name=player_name,
        stats=stats,
        raw_data=best_response,
    )


def extract_from_dom(page_content: str) -> ExtractionResult:
    """Extract stats from DOM tables (fallback).
    page_content: HTML page content

    Returns extractionResult with parsed stats.
    """
    soup = BeautifulSoup(page_content, "html.parser")

    stats = []
    current_category = "General"

    # find stat sections/tables
    sections = soup.select(".stat-section, .stats-container, section.player-stats, "
                           "[class*='category'], .stat-table-wrapper, table")

    for section in sections:
        # extract category from headers
        header = section.find_previous_sibling(["h2", "h3", "h4"])
        if header:
            cat_text = header.get_text(strip=True)
            if cat_text and cat_text.upper() not in ["STATISTIC", "VALUE", "PERCENTILE", "GRADE"]:
                current_category = cat_text

        # extract inner header
        inner_header = section.select_one("h2, h3, h4, .category-title")
        if inner_header:
            cat_text = inner_header.get_text(strip=True)
            if cat_text and cat_text.upper() not in ["STATISTIC", "VALUE", "PERCENTILE", "GRADE"]:
                current_category = cat_text

        # parse table rows
        if section.name == "table":
            table = section
        else:
            table = section.select_one("table")

        if table:
            rows = table.select("tbody tr, tr.stat-row")
            if not rows:
                rows = table.select("tr")[1:]  # skip header

            for row in rows:
                stat = _parse_dom_row(row, current_category)
                if stat:
                    stats.append(stat)

    # extract player info from page
    player_name = None
    player_header = soup.select_one("h1.player-name, h1.entry-title, .player-header h1")
    if player_header:
        player_name = player_header.get_text(strip=True)

    return ExtractionResult(success=len(stats) > 0, source=ExtractionSource.DOM_TABLES,
                            player_name=player_name, stats=stats, )


def _parse_stat_entry(stat: Dict, category: str = None) -> Optional[Dict]:
    """Parse a stat entry from structured data.

    stat: Raw stat dict
    category: Stat category override

    returns normalized stat dict or None.
    """
    if not isinstance(stat, dict):
        return None

    # extract fields with various naming conventions
    stat_name = (stat.get("name") or stat.get("statName") or
                 stat.get("stat_name") or stat.get("label") or stat.get("metric"))

    if not stat_name:
        return None

    value = (stat.get("value") or stat.get("stat_value") or stat.get("score"))

    percentile = (stat.get("percentile") or stat.get("pct") or stat.get("rank"))

    grade = (stat.get("grade") or stat.get("rating"))

    stat_category = (category or stat.get("category") or
                     stat.get("statCategory") or stat.get("type") or "General")

    return {
        "statistic_name": stat_name, "value": value,
        "percentile": percentile, "grade": grade, "statistic_category": stat_category, }


def _parse_dom_row(row, category: str) -> Optional[Dict]:
    """Parse a DOM table row into stat dict

    row: BeautifulSoup row element
    category: Stat category

    returns stat dict or None
    """
    cells = row.select("td")

    if len(cells) < 2:
        return None

    def get_cell(idx: int) -> Optional[str]:
        if idx < len(cells):
            text = cells[idx].get_text(strip=True)
            if text and text not in ["-", "—", "N/A", ""]:
                return text
        return None

    stat_name = get_cell(0)

    # skip header-like rows
    if not stat_name:
        return None
    if stat_name.upper() in ["STATISTIC", "STAT", "NAME", "METRIC"]:
        return None

    return {
        "statistic_name": stat_name,
        "value": get_cell(1),
        "percentile": get_cell(2),
        "grade": get_cell(3),
        "statistic_category": category,
    }


def detect_best_extraction_source(page_content: str, network_responses: Optional[List[Dict]] = None) -> Tuple[
    ExtractionSource, str]:
    """Detect which extraction source is best for this page.

    page_content: HTML page content
    network_responses: Intercepted network responses

    Returns Tuple of (best_source, reason).
    """
    strategy = ExtractionStrategy(page_content, network_responses)
    best_source, info = strategy.detect_best_source()

    if best_source == ExtractionSource.NEXT_DATA:
        return best_source, f"Found __NEXT_DATA__ with {info.get('stats_found', 0)} stats"
    elif best_source == ExtractionSource.APOLLO_STATE:
        return best_source, f"Found Apollo state with {info.get('stats_found', 0)} stats"
    elif best_source == ExtractionSource.XHR_JSON:
        return best_source, f"Found XHR JSON with {info.get('stats_found', 0)} stats"
    elif best_source == ExtractionSource.DOM_TABLES:
        return best_source, f"Found {info.get('table_count', 0)} tables with {info.get('stats_found', 0)} rows"
    else:
        return ExtractionSource.UNKNOWN, "No suitable extraction source found"
