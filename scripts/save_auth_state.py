#!/usr/bin/env python3
"""Save authenticated browser state for Fanspo

Opens a headed browser session for manual login, then saves
the storage state (cookies, localStorage) for reuse in crawls

Usage:
    python scripts/save_auth_state.py

After running:
    1. Browser window opens
    2. Log in to fanspo.com manually
    3. Navigate to verify you're logged in
    4. Press Enter in terminal
    5. Auth state saved to data/auth/fanspo_auth_state.json
"""

import asyncio
import os
import sys
from pathlib import Path

#add project to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from playwright.async_api import async_playwright


async def save_auth_state():
    #Launch browser for manual login and save auth state

    #determine paths
    project_root = Path(__file__).parent.parent
    auth_dir = project_root / "data" / "auth"
    auth_dir.mkdir(parents=True, exist_ok=True)
    auth_state_file = auth_dir / "fanspo_auth_state.json"

    print("=" * 60)
    print("Fanspo Auth State Saver")
    print("=" * 60)
    print()
    print("This script will:")
    print("1. Open a browser window")
    print("2. Navigate to fanspo.com")
    print("3. Wait for you to log in manually")
    print("4. Save the auth state for reuse")
    print()
    print(f"Auth state will be saved to: {auth_state_file}")
    print()

    async with async_playwright() as p:
        #launch headed browser
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )

        #create context
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )

        #create page
        page = await context.new_page()

        #navigate to fanspo login
        print("Navigating to fanspo.com...")
        await page.goto("https://fanspo.com/login")

        print()
        print("=" * 60)
        print("MANUAL LOGIN REQUIRED")
        print("=" * 60)
        print()
        print("1. Log in using your credentials in the browser window")
        print("2. Wait for the page to load after login")
        print("3. Navigate to any bball-index page to verify login")
        print()

        #wait for user input
        input("Press ENTER when you have logged in and verified... ")

        #verify login by checking for logout button or user menu
        try:
            logged_in = await page.query_selector(
                ".logout, .user-menu, [href*='logout'], .account"
            )
            if logged_in:
                print("✓ Login detected!")
            else:
                print("ERROR Could not detect login state - saving anyway")
        except Exception:
            print("ERROR Could not verify login state - saving anyway")

        #save storage state
        await context.storage_state(path=str(auth_state_file))

        print()
        print("=" * 60)
        print(f"  Auth state saved to: {auth_state_file}")
        print("=" * 60)
        print()
        print("You can now close the browser or press ENTER to close automatically.")

        input()

        await browser.close()

    print("Done!")


def main():
    #main entry point
    asyncio.run(save_auth_state())


if __name__ == "__main__":
    main()
