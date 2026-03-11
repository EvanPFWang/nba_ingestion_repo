"""Configuration utilities for bball_index_scraper

Handles:
- Environment variable loading
- Settings access
- Credential management
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class ScraperSettings:
    #Centralized scraper configuration

    #bright data proxy
    brightdata_host: str = "brd.superproxy.io"
    brightdata_port: str = "22225"
    brightdata_user: str = ""
    brightdata_pass: str = ""

    #fanspo credentials
    fanspo_email: str = ""
    fanspo_password: str = ""

    #paths
    project_root: Path = Path(__file__).parent.parent.parent.parent
    data_dir: Path = None
    auth_state_file: Path = None
    checkpoint_db_path: Path = None
    exports_dir: Path = None

    #scraping behavior
    concurrent_requests: int = 3
    download_delay: float = 2.0
    headless: bool = True

    #normalization
    missing_numeric_sentinel: str = "00000"

    #seasons
    season_start_year: int = 2013
    season_end_year: int = 2026

    def __post_init__(self):
        if self.data_dir is None:
            self.data_dir = self.project_root / "data"
        if self.auth_state_file is None:
            self.auth_state_file = self.data_dir / "auth" / "fanspo_auth_state.json"
        if self.checkpoint_db_path is None:
            self.checkpoint_db_path = self.data_dir / "checkpoints" / "crawl_checkpoint.db"
        if self.exports_dir is None:
            self.exports_dir = self.data_dir / "exports"

        #ensure directories exist
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.auth_state_file.parent.mkdir(parents=True, exist_ok=True)
        self.checkpoint_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.exports_dir.mkdir(parents=True, exist_ok=True)


def load_env(env_file: Optional[Path] = None) -> None:
    """Load environment variables from .env file
    env_file: Path to .env file. If None, looks in project root"""
    if env_file is None:
        project_root = Path(__file__).parent.parent.parent.parent
        env_file = project_root / ".env"

    if not env_file.exists():
        return

    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value:
                    os.environ.setdefault(key, value)


def get_settings() -> ScraperSettings:
    """Load settings from environment variables

    returns ScraperSettings with values from environment"""
    load_env()

    return ScraperSettings(
        brightdata_host=os.environ.get("BRIGHTDATA_HOST", "brd.superproxy.io"),
        brightdata_port=os.environ.get("BRIGHTDATA_PORT", "22225"),
        brightdata_user=os.environ.get("BRIGHTDATA_USER", ""),
        brightdata_pass=os.environ.get("BRIGHTDATA_PASS", ""),
        fanspo_email=os.environ.get("BBALL_USER", os.environ.get("FANSPO_EMAIL", "")),
        fanspo_password=os.environ.get("BBALL_PSWRD", os.environ.get("FANSPO_PASSWORD", "")),
        concurrent_requests=int(os.environ.get("CONCURRENT_REQUESTS", "3")),
        download_delay=float(os.environ.get("DOWNLOAD_DELAY", "2.0")),
        headless=os.environ.get("PLAYWRIGHT_HEADLESS", "true").lower() == "true",
        missing_numeric_sentinel=os.environ.get("MISSING_NUMERIC_SENTINEL", "00000"),
        season_start_year=int(os.environ.get("SEASON_START_YEAR", "2013")),
        season_end_year=int(os.environ.get("SEASON_END_YEAR", "2026")),
    )


def get_proxy_url(settings: Optional[ScraperSettings] = None) -> Optional[str]:
    """Build Bright Data proxy URL from settings

    returns Proxy URL string or None if no credentials"""
    if settings is None:
        settings = get_settings()

    if not settings.brightdata_user or not settings.brightdata_pass:
        return None

    return (
        f"http://{settings.brightdata_user}:{settings.brightdata_pass}"
        f"@{settings.brightdata_host}:{settings.brightdata_port}"
    )


def validate_auth_state(settings: Optional[ScraperSettings] = None) -> bool:
    """Check if auth state file exists and is valid

    returns True if auth state file exists"""
    if settings is None:
        settings = get_settings()

    return settings.auth_state_file.exists()
