"""Scraper for Bball‑Index player profiles.

Authenticates via session, scrapes player profile data with rate limiting
Credentials from BBALL_EMAIL and BBALL_PSWRD env vars

This module provides a `BballIndexScraper` class that handles
authentication and scraping of player profile data from
`www.bball-index.com`.  Credentials must be supplied via environment
variables (`BBALL_EMAIL` and `BBALL_PSWRD`) or passed explicitly when
instantiating the class.  The scraper uses an authenticated
`requests.Session` to persist login cookies.

**Disclaimer**: Scraping websites may violate their terms of service.
Ensure that you have the right to access and scrape the site, and
respect any rate limits imposed by the service.
"""
import logging
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)

@dataclass
class BballIndexConfig:
    """Configuration for BballIndexScraper

    rate_limit_sleep(flt): base delay (sec) between succ HTTP
        requests since rdm jitter is added auto to avoid hammering
        server at fixed intervals.  0 out to disable sleeping entirely.
    retry_attempts(int): # of retries for each failed network request
        before giving up. Retries use exponential backoff with jitter.

    backoff_base(flt): Base for exp backoff when retries occur, delay on the *n*‑th retry is roughly ``backoff_base ** n``
        sec plus jitter.
    backoff_max(flt): max sec of backoff delay and prevents
        backoff from growing boundlessly
    jitter_factor(flt): rdm jitter factor added to backoff delays to avoid
        sync with other clients
    max_pages(int): max # of player list pages to fetch when
        enumerating slugs.  This prevents runaway scraping if the
        pagination structure changes.
    max_profiles(int): Hard limit on the number of player profiles to fetch
        in BballIndexScraper.fetch_all_profiles and good for
        partial backfills or debugging.
    """

    rate_limit_sleep: float = 1.0
    retry_attempts: int = 3
    backoff_base: float = 2.0
    backoff_max: float = 60.0
    jitter_factor: float = 0.1
    max_pages: int = 10
    max_profiles: int = 500

class BballIndexScraper:
    #Authenticate w/ bball-index.com and fetch player profiles

    """
    Scrape player profs from bball-index.com w/ requests, logging in
    via persistent requests.Session and fetches prof list pages & individ
    player profs.

    Applies rate limits + exponential backoff on network errors to min. risk
    of server throttling

    email: str, optional
    password: str, optional
    config: `BballIndexConfig`, optional
        Fine‑tuning for rate limiting and retry behaviour.
    """
    LOGIN_URL = "https://www.bball-index.com/wp-login.php"
    PROFILE_BASE_URL = "https://www.bball-index.com/player-profiles/"
    PLAYERS_LIST_URL = "https://www.bball-index.com/players/"

    def __init__(self,email: str,password: str,
                 config: Optional[BballIndexConfig] = None,) -> None:
        self.email = email or os.environ.get("BBALL_EMAIL") or os.environ.get("BBALL_USER")
        self.password = password or os.environ.get("BBALL_PSWRD")
        if not self.email or not self.password:
            raise ValueError(
                "Email and password must be provided either as arguments or via "
                "BBALL_EMAIL/BBALL_USER and BBALL_PSWRD environment variables"
            )
        self.config = config or BballIndexConfig()
        self.session: Optional[requests.Session] = None

    def authenticate(self) -> None:
        """Login to bball-index; assigns session on success."""
        session = requests.Session()
        payload = { "log": self.email, "pwd": self.password,
            "redirect_to": "https://www.bball-index.com/player-profiles/",
            "testcookie": "1",}

        logger.info("Authenticating to bball-index as %s", self.email)
        resp = session.post(self.LOGIN_URL, data=payload, timeout=30)
        if resp.status_code != 200 or "Invalid username" in resp.text:
            raise RuntimeError("Failed to authenticate with bball-index")
        self.session = session
        logger.info("Authentication successful")

    def _ensure_authenticated(self) -> None:
        if self.session is None:
            raise RuntimeError("Not authenticated; call authenticate() first")

    def _sleep(self) -> None:
        if self.rate_limit_sleep > 0:
            time.sleep(self.rate_limit_sleep)

    def fetch_player_slugs(self, max_pages: int = 10) -> List[str]:
        """Scrape player slugs from players list page(s).

        Returns list of URL slugs for individual player profiles.
        """
        self._ensure_authenticated()
        slugs: List[str] = []

        for page in range(1, max_pages + 1):
            url = f"{self.PLAYERS_LIST_URL}page/{page}/" if page > 1 else self.PLAYERS_LIST_URL
            logger.info("Fetching player list page %d", page)
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code != 200:
                    logger.warning("Page %d returned HTTP %d", page, resp.status_code)
                    break
                soup = BeautifulSoup(resp.text, "html.parser")
                # find player links (adjust selector based on actual HTML)
                links = soup.select("a[href*='/player-profiles/']")
                if not links:
                    break
                for link in links:
                    href = link.get("href", "")
                    if "/player-profiles/" in href:
                        slug = href.rstrip("/").split("/")[-1]
                        if slug and slug not in slugs:
                            slugs.append(slug)
                self._sleep()
            except Exception as exc:
                logger.error("Error fetching player list page %d: %s", page, exc)
                break

        logger.info("Found %d player slugs", len(slugs))
        return slugs


    def fetch_player_profile(self, player_slug: str) -> Dict[str, Optional[str]]:
        """Fetch a single player profile by slug.

        Returns dict of scraped stats
        struct of the return value should be tailored to actual HTML struct
         of bball-index’s player profile pages.
        """
        self._ensure_authenticated()
        url = f"{self.PROFILE_BASE_URL}{player_slug}/"
        logger.info("Fetching Bball Index profile for %s", player_slug)
        resp = self.session.get(url)
        if resp.status_code != 200:
            logger.error("Failed to fetch profile %s: HTTP %d", player_slug, resp.status_code)
            return {}
        soup = BeautifulSoup(resp.text, "html.parser")
        #TODO: parse the HTML to extract stats.  Example below is a placeholder.
        profile_data = {}
        #Example: extract player name
        header = soup.find("h1", class_="entry-title")
        profile_data["name"] = header.get_text(strip=True) if header else None
        # Add more parsing logic here
        return profile_data

    def fetch_all_profiles(self) -> pd.DataFrame:
        """Iterate over a list of players and return a DataFrame of profiles.

        For demonstration, this method returns an empty DataFrame.  In
        production, you might first scrape a list of player slugs from
        the site or another data source, then call `fetch_player_profile`
        for each slug and aggregate the results into a DataFrame.
        """
        logger.info("Fetching all player profiles (placeholder)")
        return pd.DataFrame()



    _____

    class BballIndexScraper:
        """Authenticate with bball-index.com and fetch player profiles."""

        def fetch_player_profile(self, player_slug: str) -> Dict[str, Any]:
            """Fetch single player profile by slug.

            Returns dict with scraped data; empty dict on failure.
            """
            self._ensure_authenticated()
            url = f"{self.PROFILE_BASE_URL}{player_slug}/"
            logger.debug("Fetching profile: %s", player_slug)

            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code != 200:
                    logger.warning("Profile %s returned HTTP %d", player_slug, resp.status_code)
                    return {}
            except Exception as exc:
                logger.error("Error fetching profile %s: %s", player_slug, exc)
                return {}

            soup = BeautifulSoup(resp.text, "html.parser")
            profile: Dict[str, Any] = {"slug": player_slug}

            # extract player name
            header = soup.find("h1", class_="entry-title")
            profile["name"] = header.get_text(strip=True) if header else None

            # extract stats tables (adjust selectors based on actual HTML)
            tables = soup.find_all("table")
            for i, table in enumerate(tables[:3]):  # limit to first 3 tables
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all(["th", "td"])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True).lower().replace(" ", "_")
                        val = cells[1].get_text(strip=True)
                        if key:
                            profile[f"{key}"] = val

            return profile

        def fetch_all_profiles(
                self,
                player_slugs: Optional[List[str]] = None,
                max_players: int = 500,
        ) -> pd.DataFrame:
            """Fetch profiles for all players (or provided list).

            Args:
                player_slugs: if None, scrapes player list first
                max_players: limit to avoid runaway scraping

            Returns:
                DataFrame with player profile data
            """
            self._ensure_authenticated()

            if player_slugs is None:
                player_slugs = self.fetch_player_slugs()

            player_slugs = player_slugs[:max_players]
            logger.info("Fetching %d player profiles", len(player_slugs))

            profiles: List[Dict[str, Any]] = []
            for idx, slug in enumerate(player_slugs):
                profile = self.fetch_player_profile(slug)
                if profile:
                    profiles.append(profile)
                if idx % 50 == 0 and idx > 0:
                    logger.info("Progress: %d/%d profiles", idx, len(player_slugs))
                self._sleep()

            logger.info("Fetched %d profiles total", len(profiles))
            return pd.DataFrame(profiles)