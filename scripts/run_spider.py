"""Run the BBall Index spider with common options.

Provides a convenient CLI for running the spider with:
- Resume support
- Input file handling
- Single player/season targeting
- Checkpoint management

Usage:
    ```#Run with input file
    python scripts/run_spider.py --input players.txt

    #Run single player for all seasons
    python scripts/run_spider.py --player-id 2544 --player-slug lebron-james

    #Run single player for specific season
    python scripts/run_spider.py --player-id 2544 --player-slug lebron-james --seasons 2024-2025

    #Resume failed crawl
    python scripts/run_spider.py --resume

    #Reset failed items and retry
    python scripts/run_spider.py --reset-failed --resume
    ```"""

import argparse
import os
import sys
from pathlib import Path
from subprocess import run

#add project to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bball_index_scraper.utils.config import get_settings, validate_auth_state
from bball_index_scraper.utils.checkpoint import CheckpointManager


def check_prerequisites():
    #Check that prerequisites are in place
    settings = get_settings()

    print("Checking prerequisites...")
    print()

    #check auth state
    if validate_auth_state(settings):
        print("✓ Auth state file found")
    else:
        print("✗ Auth state file not found")
        print("  Run: python scripts/save_auth_state.py")
        return False

    #check directories
    for name, path in [
        ("Data dir", settings.data_dir),
        ("Auth dir", settings.auth_state_file.parent),
        ("Checkpoint dir", settings.checkpoint_db_path.parent),
        ("Exports dir", settings.exports_dir),
    ]:
        if path.exists():
            print(f"✓ {name}: {path}")
        else:
            path.mkdir(parents=True, exist_ok=True)
            print(f"✓ {name}: {path} (created)")

    print()
    return True


def show_checkpoint_stats(settings):
    #Show checkpoint statistics
    checkpoint_path = settings.checkpoint_db_path

    if not checkpoint_path.exists():
        print("No checkpoint database yet.")
        return

    with CheckpointManager(checkpoint_path) as cp:
        stats = cp.get_stats()

    print("Checkpoint Statistics:")
    print(f"  Total: {stats.get('total', 0)}")
    print(f"  Success: {stats.get('success', 0)}")
    print(f"  Failed: {stats.get('failed', 0)}")
    print(f"  Skipped: {stats.get('skipped', 0)}")
    print(f"  Pending: {stats.get('pending', 0)}")
    print()


def reset_failed_items(settings):
    #Reset failed items for retry
    checkpoint_path = settings.checkpoint_db_path

    if not checkpoint_path.exists():
        print("No checkpoint database.")
        return

    with CheckpointManager(checkpoint_path) as cp:
        count = cp.reset_failed()

    print(f"Reset {count} failed items for retry.")
    print()


def run_spider(args):
    #Run the Scrapy spider

    #build scrapy command
    cmd = [
        "scrapy", "crawl", "player_profiles",
    ]

    #add spider arguments
    if args.input:
        cmd.extend(["-a", f"input_file={args.input}"])

    if args.player_id:
        cmd.extend(["-a", f"player_id={args.player_id}"])

    if args.player_slug:
        cmd.extend(["-a", f"player_slug={args.player_slug}"])

    if args.seasons:
        cmd.extend(["-a", f"seasons={args.seasons}"])

    if not args.no_resume:
        cmd.extend(["-a", "resume=true"])

    #add logging
    if args.verbose:
        cmd.extend(["-s", "LOG_LEVEL=DEBUG"])

    #set working directory
    project_dir = Path(__file__).parent.parent / "src" / "bball_index_scraper"

    print("Running spider...")
    print(f"Command: {' '.join(cmd)}")
    print()

    #run scrapy
    result = run(cmd, cwd=project_dir)

    return result.returncode


def main():
    #Main entry point
    parser = argparse.ArgumentParser(
        description="Run BBall Index spider",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    #Run with input file
    python scripts/run_spider.py --input players.txt

    #Run single player
    python scripts/run_spider.py --player-id 2544 --player-slug lebron-james

    #Resume previous crawl
    python scripts/run_spider.py --resume
        """,
    )

    parser.add_argument(
        "--input", "-i",
        help="Input file with player URLs",
    )
    parser.add_argument(
        "--player-id",
        help="Single player ID to crawl",
    )
    parser.add_argument(
        "--player-slug",
        help="Single player slug (e.g., lebron-james)",
    )
    parser.add_argument(
        "--seasons",
        help="Comma-separated seasons (e.g., 2024-2025,2023-2024)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Don't skip already-completed items",
    )
    parser.add_argument(
        "--reset-failed",
        action="store_true",
        help="Reset failed items for retry before running",
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Show checkpoint stats and exit",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    settings = get_settings()

    #show stats only
    if args.stats_only:
        show_checkpoint_stats(settings)
        return 0

    #check prerequisites
    if not check_prerequisites():
        return 1

    #show current stats
    show_checkpoint_stats(settings)

    #reset failed if requested
    if args.reset_failed:
        reset_failed_items(settings)

    #run spider
    return run_spider(args)


if __name__ == "__main__":
    sys.exit(main())
