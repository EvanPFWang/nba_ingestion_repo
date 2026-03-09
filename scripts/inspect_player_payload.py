#!/usr/bin/env python3
"""Inspect player profile payload to determine extraction strategy

Loads a player profile page and analyzes:
__NEXT_DATA__ script tag content
Apollo Client state
Intercepted XHR/fetch JSON responses
DOM table structure

Reports which source contains the richest structured data.

Usage:
    python scripts/inspect_player_payload.py --url "https://fanspo.com/bball-index/player-profiles/2024-2025/lebron-james/2544"
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

#add project to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from playwright.async_api import async_playwright
