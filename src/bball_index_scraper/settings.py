"""Scrapy settings for bball_index_scraper

Production-grade configuration for Playwright-backed crawling with:
- Bright Data proxy integration
- Conservative concurrency
- Resumable checkpointing
- Authenticated session reuse
"""

import os
from pathlib import Path

#project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CHECKPOINTS_DIR = DATA_DIR / "checkpoints"
EXPORTS_DIR = DATA_DIR / "exports"
AUTH_DIR = DATA_DIR / "auth"

#ensure directories exist
for d in [DATA_DIR, CHECKPOINTS_DIR, EXPORTS_DIR, AUTH_DIR]:
    d.mkdir(parents=True, exist_ok=True)

BOT_NAME = "bball_index_scraper"
SPIDER_MODULES = ["bball_index_scraper.spiders"]
NEWSPIDER_MODULE = "bball_index_scraper.spiders"

#obey robots.txt respectfully
ROBOTSTXT_OBEY = False  #authenticated session; manual compliance

#conservative concurrency
CONCURRENT_REQUESTS = 3
CONCURRENT_REQUESTS_PER_DOMAIN = 2
DOWNLOAD_DELAY = 2.0  #base delay; jitter added in spider

#disable cookies in Scrapy (Playwright handles them via storage_state)
COOKIES_ENABLED = False

#telnet disabled for security
TELNETCONSOLE_ENABLED = False

#default headers
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

#playwright download handler
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

#playwright settings
PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": os.environ.get("PLAYWRIGHT_HEADLESS", "true").lower() == "true",
    "args": [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-dev-shm-usage",
    ],
}

#bright data proxy configuration (loaded from environment)
BRIGHTDATA_HOST = os.environ.get("BRIGHTDATA_HOST", "brd.superproxy.io")
BRIGHTDATA_PORT = os.environ.get("BRIGHTDATA_PORT", "22225")
BRIGHTDATA_USER = os.environ.get("BRIGHTDATA_USER", "")
BRIGHTDATA_PASS = os.environ.get("BRIGHTDATA_PASS", "")

#construct proxy URL if credentials present
if BRIGHTDATA_USER and BRIGHTDATA_PASS:
    PLAYWRIGHT_CONTEXTS = {
        "default": {
            "proxy": {
                "server": f"http://{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}",
                "username": BRIGHTDATA_USER,
                "password": BRIGHTDATA_PASS,
            },
            "ignore_https_errors": True,
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }
    }
else:
    #no proxy; direct connection
    PLAYWRIGHT_CONTEXTS = {
        "default": {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }
    }

#storage state for authenticated sessions
AUTH_STATE_FILE = AUTH_DIR / "fanspo_auth_state.json"
if AUTH_STATE_FILE.exists():
    PLAYWRIGHT_CONTEXTS["default"]["storage_state"] = str(AUTH_STATE_FILE)

#middlewares
DOWNLOADER_MIDDLEWARES = {
    "bball_index_scraper.middlewares.PlaywrightRetryMiddleware": 550,
    "bball_index_scraper.middlewares.JitteredDelayMiddleware": 560,
}

#pipelines
ITEM_PIPELINES = {
    "bball_index_scraper.pipelines.NormalizationPipeline": 100,
    "bball_index_scraper.pipelines.CheckpointPipeline": 200,
    "bball_index_scraper.pipelines.JsonlExportPipeline": 300,
    "bball_index_scraper.pipelines.SqliteDataPipeline": 400,
}

#extensions
EXTENSIONS = {
    "bball_index_scraper.extensions.ProgressExtension": 500,
}

#retry settings
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

#exponential backoff delays (seconds)
RETRY_BACKOFF_BASE = 5
RETRY_BACKOFF_MULTIPLIER = 4  #5, 20, 80 pattern

#logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

#feed exports (backup; pipelines handle primary export)
FEEDS = {
    str(EXPORTS_DIR / "player_stats_%(time)s.jsonl"): {
        "format": "jsonlines",
        "encoding": "utf-8",
        "overwrite": False,
    },
}

#checkpoint database
CHECKPOINT_DB_PATH = CHECKPOINTS_DIR / "crawl_checkpoint.db"

#data sink database
DATA_DB_PATH = EXPORTS_DIR / "player_stats.db"

#normalization settings
MISSING_NUMERIC_SENTINEL = os.environ.get("MISSING_NUMERIC_SENTINEL", "00000")
MISSING_STRING_VALUE = None  #null for non-numeric missing

#extraction source priority
EXTRACTION_PRIORITY = [
    "next_data",      #__NEXT_DATA__ script tag
    "apollo_state",   #Apollo Client cache
    "xhr_json",       #intercepted XHR/fetch JSON
    "dom_tables",     #rendered HTML tables (fallback)
]

#season range
SEASON_START_YEAR = 2013
SEASON_END_YEAR = 2026

#page wait settings
PAGE_WAIT_SELECTOR = "table, [class*='stat'], [class*='player']"
PAGE_WAIT_TIMEOUT = 15000  #ms

#request fingerprinting
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
