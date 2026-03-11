"""URL utilities for bball_index_scraper.

Handles:
- Building player profile URLs
- Parsing player info from URLs
- Season string construction
"""

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

#base URLs
FANSPO_BASE = "https://fanspo.com"
BBALL_INDEX_BASE = "https://fanspo.com/bball-index"
PLAYER_PROFILES_BASE = f"{BBALL_INDEX_BASE}/player-profiles"


@dataclass
class PlayerUrlInfo:
    #Parsed info from a player profile URL
    season: str
    player_slug: str
    player_id: str
    full_url: str


def build_season_string(start_year: int) -> str:
    """Build season string from start year.

    start_year: e.g., 2024

    returns Season string e.g., "2024-2025"
    """
    return f"{start_year}-{start_year + 1}"


def build_season_key(start_year: int, base_year: int = 2013) -> str:
    """Build season key (0-13) from start year.

    start_year: e.g., 2024
    base_year: First season year (default 2013)

    returns Season key e.g., "11" for 2024-2025
    """
    return str(start_year - base_year)


def get_all_seasons(start_year: int = 2013, end_year: int = 2026) -> List[Tuple[str, str]]:
    """Get all season strings and keys.

    start_year: First season start year
    end_year: Last season start year (inclusive)

    returns List of (season_key, season_string) tuples"""
    seasons = []
    for year in range(start_year, end_year + 1):
        key = build_season_key(year, start_year)
        season_str = build_season_string(year)
        seasons.append((key, season_str))
    return seasons


def build_player_url(
        season: str,
        player_slug: str,
        player_id: str,
) -> str:
    """Build full player profile URL.

    season: e.g., "2024-2025"
    player_slug: e.g., "aaron-gordon"
    player_id: e.g., "203932"

    returns Full URL e.g., "https://fanspo.com/bball-index/player-profiles/2024-2025/aaron-gordon/203932"
    """
    return f"{PLAYER_PROFILES_BASE}/{season}/{player_slug}/{player_id}"


def parse_player_url(url: str) -> Optional[PlayerUrlInfo]:
    """Parse player info from profile URL.

    url: Player profile URL

    returns PlayerUrlInfo or None if unparseable"""
    #pattern: /player-profiles/{season}/{slug}/{id}
    pattern = r"/player-profiles/(\d{4}-\d{4})/([^/]+)/(\d+)"

    match = re.search(pattern, url)
    if not match:
        return None

    return PlayerUrlInfo(
        season=match.group(1),
        player_slug=match.group(2),
        player_id=match.group(3),
        full_url=url,
    )


def normalize_url(url: str) -> str:
    """Normalize URL to canonical form.

    url: URL to normalize

    returns Normalized URL"""
    #ensure scheme
    if not url.startswith("http"):
        url = f"https://{url}"

    #parse and reconstruct
    parsed = urlparse(url)

    #remove trailing slash
    path = parsed.path.rstrip("/")

    #reconstruct
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def extract_player_id_from_url(url: str) -> Optional[str]:
    """Extract player ID from URL.

    url: Player profile URL

    returns Player ID or None"""
    info = parse_player_url(url)
    return info.player_id if info else None


def extract_player_slug_from_url(url: str) -> Optional[str]:
    """Extract player slug from URL.

    url: Player profile URL

    returns Player slug or None"""
    info = parse_player_url(url)
    return info.player_slug if info else None


def extract_season_from_url(url: str) -> Optional[str]:
    """Extract season from URL.

    url: Player profile URL

    returns Season string or None"""
    info = parse_player_url(url)
    return info.season if info else None


def build_player_list_url(season: Optional[str] = None, page: int = 1) -> str:
    """Build player list URL.

    season: Season filter (optional)
    page: Page number

    returns Player list URL"""
    base = f"{BBALL_INDEX_BASE}/players"

    params = []
    if season:
        params.append(f"season={season}")
    if page > 1:
        params.append(f"page={page}")

    if params:
        return f"{base}?{'&'.join(params)}"
    return base


def build_bball_index_iframe_url() -> str:
    """Build the BBall Index embed/iframe URL.

    Since BBall Index is an iframe embed within Fanspo,
    this returns the main Fanspo bball-index URL.

    returns Fanspo BBall Index base URL"""
    return BBALL_INDEX_BASE


def is_player_profile_url(url: str) -> bool:
    """Check if URL is a player profile URL.

    url: URL to check

    returns True if this is a player profile URL"""
    return "/player-profiles/" in url and parse_player_url(url) is not None


def build_url_key(player_id: str, season: str) -> str:
    """Build unique key for player-season combination.

    player_id: Player ID
    season: Season string

    returns Unique key for checkpointing"""
    return f"{player_id}_{season}"
