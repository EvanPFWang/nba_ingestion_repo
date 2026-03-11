#!/usr/bin/env python3
"""Inspect player profile payload to determine extraction strategy.

Loads a player profile page and analyzes:
- __NEXT_DATA__ script tag content
- Apollo Client state
- Intercepted XHR/fetch JSON responses
- DOM table structure

Reports which source contains the richest structured data.

Usage:

    ```python scripts/inspect_player_payload.py
    python scripts/inspect_player_payload.py --url "https://fanspo.com/bball-index/player-profiles/2024-2025/lebron-james/2544"
    ```"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

#add project to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from playwright.async_api import async_playwright

from bball_index_scraper.utils.config import get_settings, validate_auth_state
from bball_index_scraper.utils.extraction import (
    ExtractionStrategy,
    ExtractionSource,
    detect_best_extraction_source,
    extract_next_data,
    extract_apollo_state,
    extract_from_dom,
)
from bball_index_scraper.utils.network_capture import NetworkCapture

DEFAULT_URL = "https://fanspo.com/bball-index/player-profiles/2024-2025/lebron-james/2544"


async def inspect_payload(url: str, save_raw: bool = False):
    """Inspect player profile payload.

    
    url: Player profile URL to inspect
    save_raw: If True, save raw payloads to files
    """
    print("=" * 70)
    print("Player Profile Payload Inspector")
    print("=" * 70)
    print()
    print(f"URL: {url}")
    print()

    settings = get_settings()

    #check auth state
    if validate_auth_state(settings):
        print("  Auth state file found")
        auth_state_path = str(settings.auth_state_file)
    else:
        print("  Auth state file not found - may not see all data")
        auth_state_path = None

    print()

    #network capture for XHR
    network_capture = NetworkCapture()
    captured_responses = []

    async with async_playwright() as p:
        #launch browser
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )

        #create context with auth state
        context_options = {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }

        if auth_state_path:
            context_options["storage_state"] = auth_state_path

        context = await browser.new_context(**context_options)
        page = await context.new_page()

        #setup response capture
        async def on_response(response):
            url = response.url
            content_type = response.headers.get("content-type", "")

            if "json" in content_type.lower():
                try:
                    body = await response.text()
                    data = json.loads(body)
                    captured_responses.append({
                        "url": url,
                        "status": response.status,
                        "data": data,
                    })
                except Exception:
                    pass

        page.on("response", on_response)

        #navigate to page
        print("Loading page...")
        await page.goto(url, wait_until="networkidle")

        #small scroll to trigger lazy loading
        await page.evaluate("window.scrollBy(0, 300)")
        await page.wait_for_timeout(1000)

        #get page content
        content = await page.content()

        await browser.close()

    print()
    print("=" * 70)
    print("EXTRACTION SOURCE ANALYSIS")
    print("=" * 70)
    print()

    #analyze each source
    results = {}

    #1. __NEXT_DATA__
    print("1. __NEXT_DATA__ (Next.js server-side data)")
    print("-" * 50)
    next_result = extract_next_data(content)
    results["next_data"] = next_result

    if next_result.success:
        print(f"     Found: {len(next_result.stats)} stats")
        print(f"   Player: {next_result.player_name}")
        print(f"   Season: {next_result.season}")
        if next_result.stats:
            print(f"   Sample stat: {next_result.stats[0]}")
    else:
        print(f"   x Not found: {next_result.error}")
    print()

    #2. Apollo State
    print("2. Apollo Client State")
    print("-" * 50)
    apollo_result = extract_apollo_state(content)
    results["apollo_state"] = apollo_result

    if apollo_result.success:
        print(f"     Found: {len(apollo_result.stats)} stats")
        print(f"   Player: {apollo_result.player_name}")
    else:
        print(f"   x Not found: {apollo_result.error}")
    print()

    #3. XHR/Fetch JSON
    print("3. XHR/Fetch JSON Responses")
    print("-" * 50)
    print(f"   Captured {len(captured_responses)} JSON responses")

    stats_responses = []
    for resp in captured_responses:
        data = resp["data"]
        strategy = ExtractionStrategy("", [data])
        if strategy._count_stats_in_data(data) > 0:
            stats_responses.append(resp)

    if stats_responses:
        print(f"     {len(stats_responses)} responses contain stats-like data")
        for resp in stats_responses[:3]:
            print(f"      - {resp['url'][:60]}...")
    else:
        print("   x No stats-like data in captured responses")
    print()

    #4. DOM Tables
    print("4. DOM Tables (fallback)")
    print("-" * 50)
    dom_result = extract_from_dom(content)
    results["dom_tables"] = dom_result

    if dom_result.success:
        print(f"     Found: {len(dom_result.stats)} stat rows")
        if dom_result.stats:
            categories = set(s.get("statistic_category", "General") for s in dom_result.stats)
            print(f"   Categories: {', '.join(categories)}")
    else:
        print("   x No stats found in DOM tables")
    print()

    #summary
    print("=" * 70)
    print("CONCLUSION")
    print("=" * 70)
    print()

    #find best source
    best_source = None
    best_count = 0

    for source_name, result in results.items():
        if result.success and len(result.stats) > best_count:
            best_source = source_name
            best_count = len(result.stats)

    if best_source:
        print(f"  BEST SOURCE: {best_source.upper()}")
        print(f"  Stats found: {best_count}")
        print()
        print("Extraction strategy should prioritize this source.")
    else:
        print("x NO SUITABLE SOURCE FOUND")
        print("  Page may require different approach or authentication.")

    print()

    #save raw data if requested
    if save_raw:
        output_dir = Path("data/inspection")
        output_dir.mkdir(parents=True, exist_ok=True)

        #save HTML
        with open(output_dir / "page_content.html", "w") as f:
            f.write(content)
        print(f"Saved: {output_dir}/page_content.html")

        #save __NEXT_DATA__
        if next_result.raw_data:
            with open(output_dir / "next_data.json", "w") as f:
                json.dump(next_result.raw_data, f, indent=2)
            print(f"Saved: {output_dir}/next_data.json")

        #save captured responses
        with open(output_dir / "xhr_responses.json", "w") as f:
            json.dump(captured_responses, f, indent=2, default=str)
        print(f"Saved: {output_dir}/xhr_responses.json")

        print()

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Inspect player profile payload")
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="Player profile URL to inspect",
    )
    parser.add_argument(
        "--save-raw",
        action="store_true",
        help="Save raw payloads to files",
    )

    args = parser.parse_args()

    asyncio.run(inspect_payload(args.url, args.save_raw))


if __name__ == "__main__":
    main()
