# BBall Index Scraper

Production-grade Scrapy + Playwright scraper for Fanspo/BBall Index player profile pages.

## Architecture Summary

- **Framework**: Scrapy for crawl orchestration, scheduling, retries, pipelines
- **Browser**: Playwright Chromium for JS-rendered pages
- **Proxy**: Bright Data integration via Playwright context
- **Auth**: Storage state JSON reused across runs
- **Checkpoint**: SQLite for resumable crawls
- **Export**: JSONL + normalized SQLite
- **Extraction Priority**:
  1. `__NEXT_DATA__` (Next.js server-side state)
  2. Apollo Client cache
  3. Intercepted XHR/fetch JSON
  4. DOM tables (fallback)

## Assumptions and Validation

- BBall Index is embedded as iframe within Fanspo (URL never changes in iframe)
- Pages use Next.js with possible `__NEXT_DATA__` script tag
- Extraction strategy auto-detects richest structured source
- Auth state persists via cookies/localStorage in storage_state.json
- Conservative concurrency (3 requests) with jittered delays

## Project Structure

```
bball_index_scraper/
├── scrapy.cfg                 # Scrapy project config
├── requirements.txt           # Python dependencies
├── settings.py                # Scrapy settings (Playwright, proxy, pipelines)
├── items.py                   # Scrapy Item definitions
├── middlewares.py             # Retry, jitter, auth middlewares
├── pipelines.py               # Normalization, checkpoint, export pipelines
├── extensions.py              # Progress tracking extension
├── spiders/
│   ├── __init__.py
│   └── player_profiles.py     # Main spider
└── utils/
    ├── __init__.py
    ├── config.py              # Settings and env loading
    ├── normalization.py       # Value/grade normalization
    ├── url_utils.py           # URL building/parsing
    ├── extraction.py          # Multi-source extraction
    ├── checkpoint.py          # SQLite progress tracking
    ├── network_capture.py     # XHR/fetch interception
    └── logging_utils.py       # Logging setup

scripts/
├── save_auth_state.py         # One-time auth bootstrap
├── inspect_player_payload.py  # Payload inspection/smoke test
└── run_spider.py              # Spider runner with options
```

## Run Order

### 1. Install Dependencies

```bash
cd src/bball_index_scraper
pip install -r requirements.txt
```

### 2. Install Playwright Browsers

```bash
playwright install chromium
```

### 3. Configure Environment

Create `.env` in project root:

```bash
# Bright Data proxy (optional)
BRIGHTDATA_HOST=brd.superproxy.io
BRIGHTDATA_PORT=22225
BRIGHTDATA_USER=your_username
BRIGHTDATA_PASS=your_password

# Fanspo credentials
BBALL_USER=your_email@example.com
BBALL_PSWRD=your_password

# Options
PLAYWRIGHT_HEADLESS=true
CONCURRENT_REQUESTS=3
DOWNLOAD_DELAY=2.0
MISSING_NUMERIC_SENTINEL=00000
```

### 4. Generate Auth State (One-Time)

```bash
python scripts/save_auth_state.py
```

This opens a browser for manual login. Auth state saved to `data/auth/fanspo_auth_state.json`.

### 5. Run Payload Inspection (Smoke Test)

```bash
python scripts/inspect_player_payload.py --url "https://fanspo.com/bball-index/player-profiles/2024-2025/lebron-james/2544"
```

Outputs which extraction source has the richest data.

### 6. Run Spider

```bash
# Run with sample URLs
cd src/bball_index_scraper
scrapy crawl player_profiles

# Run with input file
scrapy crawl player_profiles -a input_file=players.txt

# Run single player for all seasons
scrapy crawl player_profiles -a player_id=2544 -a player_slug=lebron-james

# Run single player for specific season
scrapy crawl player_profiles -a player_id=2544 -a player_slug=lebron-james -a seasons=2024-2025
```

Or use the convenience script:

```bash
python scripts/run_spider.py --player-id 2544 --player-slug lebron-james
```

### 7. Resume a Stopped Crawl

```bash
# Spider automatically resumes by default
scrapy crawl player_profiles

# Or with script
python scripts/run_spider.py --resume

# Reset failed items and retry
python scripts/run_spider.py --reset-failed --resume
```

## Output Files

- `data/exports/player_stats_YYYYMMDD_HHMMSS.jsonl` - JSONL export
- `data/exports/player_stats.db` - SQLite data sink
- `data/checkpoints/crawl_checkpoint.db` - Progress tracking

## Common Failure Modes

### Expired Auth State

**Symptom**: Redirects to login page, "auth_expired" in logs

**Fix**:
```bash
python scripts/save_auth_state.py
# Re-login manually and save
```

### Proxy Auth Errors

**Symptom**: 407 Proxy Authentication Required, connection refused

**Fix**:
- Verify BRIGHTDATA_USER/BRIGHTDATA_PASS in .env
- Check Bright Data account/zone status
- Remove proxy config to test direct connection

### No Data in __NEXT_DATA__

**Symptom**: Extraction falls back to DOM, fewer stats

**Fix**:
- Run `scripts/inspect_player_payload.py --save-raw`
- Check `data/inspection/next_data.json` for structure
- Adjust extraction.py `_parse_stat_entry` field names

### Page Methods Not Closing

**Symptom**: Memory growth, "context closed" errors

**Fix**:
- Ensure `await page.close()` in spider errback
- Check `playwright_include_page` meta handling
- Reduce CONCURRENT_REQUESTS

### Context/Page Leaks

**Symptom**: Browser instances accumulate, eventual crash

**Fix**:
- Add explicit cleanup in spider `spider_closed` signal
- Check middleware exception handlers
- Set `PLAYWRIGHT_MAX_PAGES_PER_CONTEXT` in settings

### Season Navigation Mismatch

**Symptom**: Wrong season data extracted, URL/data mismatch

**Fix**:
- Verify URL pattern: `/player-profiles/{season}/{slug}/{id}`
- Check season dropdown interaction if direct URL fails
- Parse season from response URL, not request

## Extending

### Adding New Extraction Sources

1. Add method to `utils/extraction.py`
2. Add to `ExtractionSource` enum
3. Add to `ExtractionStrategy.PRIORITY` list

### Adding New Output Formats

1. Create new pipeline in `pipelines.py`
2. Add to `ITEM_PIPELINES` in `settings.py`

### Adding Player Discovery

1. Create new spider for player list pages
2. Output player URLs to input file
3. Run main spider with input file

## License

MIT
