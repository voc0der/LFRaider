#!/usr/bin/env python3
"""Scrape WCL guild rankings pages and write scores.json.

Usage:
    python3 tools/scrape_scores.py

Reads guild IDs from data/fetch_state.json, scrapes each guild's rankings
page for zone 1047 and 1048 via headless Chromium, and writes data/scores.json
in the same format as fetch_wcl_scores.py. No API credentials needed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
STATE_FILE = REPO_ROOT / "data" / "fetch_state.json"
OUTPUT_FILE = REPO_ROOT / "data" / "scores.json"

MAX_GUILDS = 1000
ZONE_IDS = [1047, 1048]
REALM = "Dreamscythe"
CHROMIUM_EXECUTABLE = (
    shutil.which("chromium") or shutil.which("chromium-browser") or None
)
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)
DELAY_MIN = 0.3
DELAY_MAX = 1.2
AJAX_TIMEOUT = 8.0
SCORE_POLICY_VERSION = 4
SCORE_POLICY = (
    "Mean of WCL per-encounter rank percentiles across zones 1047 and 1048, "
    "sourced from guild rankings pages."
)


def load_guild_ids() -> list[int]:
    with open(STATE_FILE) as f:
        state = json.load(f)
    guilds = state.get("guilds", [])
    return [g["id"] for g in guilds if isinstance(g, dict)][:MAX_GUILDS]


async def scrape_guild_zone(browser, guild_id: int, zone_id: int) -> list[tuple[str, float]]:
    """Return [(name, avg_score), ...] for one guild+zone page."""
    context = await browser.new_context(user_agent=UA)
    page = await context.new_page()

    ajax_done = asyncio.Event()

    async def on_response(resp):
        if "guild-rankings-for-zone" in resp.url:
            ajax_done.set()

    page.on("response", on_response)

    url = f"https://fresh.warcraftlogs.com/guild/rankings/{guild_id}/{zone_id}?recent=true"
    try:
        await page.goto(url, wait_until="domcontentloaded")
    except Exception:
        await context.close()
        return []

    try:
        await asyncio.wait_for(ajax_done.wait(), timeout=AJAX_TIMEOUT)
    except asyncio.TimeoutError:
        await context.close()
        return []

    results: list[tuple[str, float]] = []
    rows = await page.query_selector_all(".character-metric-table tbody tr")
    for row in rows:
        cells = await row.query_selector_all("td")
        if len(cells) < 2:
            continue
        name_el = await cells[0].query_selector("a")
        if not name_el:
            continue
        name = (await name_el.inner_text()).strip()
        avg_text = (await cells[1].inner_text()).strip()
        try:
            avg = float(avg_text)
        except ValueError:
            continue
        if name and 0 <= avg <= 100:
            results.append((name, avg))

    await context.close()
    return results


async def scrape_guild(browser, guild_id: int) -> dict[str, float]:
    """Scrape all configured zones for a guild and return {name: avg_score}."""
    zone_scores: dict[str, list[float]] = {}
    for zone_id in ZONE_IDS:
        rows = await scrape_guild_zone(browser, guild_id, zone_id)
        for name, avg in rows:
            zone_scores.setdefault(name, []).append(avg)
    return {name: sum(avgs) / len(avgs) for name, avgs in zone_scores.items()}


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-guilds", type=int, default=MAX_GUILDS, metavar="N")
    parser.add_argument("--state-file", type=Path, default=STATE_FILE)
    parser.add_argument("--output", type=Path, default=OUTPUT_FILE)
    parser.add_argument(
        "--failed-guilds", type=Path, default=None, metavar="PATH",
        help="write guild IDs that returned 0 results to this JSON file",
    )
    parser.add_argument(
        "--retry-from", type=Path, default=None, metavar="PATH",
        help="scrape only guild IDs in this JSON file; pre-loads existing --output as base scores",
    )
    args = parser.parse_args()

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("playwright not installed.  Run:  pip install playwright")
        sys.exit(1)

    if args.retry_from:
        with open(args.retry_from) as f:
            retry_data = json.load(f)
        guild_ids = retry_data.get("guild_ids", [])[:args.max_guilds]
        print(f"Retry mode: {len(guild_ids)} guild IDs from {args.retry_from.name}")
    else:
        guild_ids = load_guild_ids()[:args.max_guilds]
        print(f"Loaded {len(guild_ids)} guilds from {args.state_file.name}")
    print(f"Scraping zones {ZONE_IDS} for each guild (recent=true) ...")
    print()

    all_scores: dict[str, float] = {}

    if args.retry_from and args.output.exists():
        with open(args.output) as f:
            existing = json.load(f)
        for char in existing.get("characters", []):
            all_scores[char["name"]] = char["score"]
        print(f"Pre-loaded {len(all_scores)} existing scores from {args.output.name}")

    t_start = time.perf_counter()
    done = 0
    non_empty = 0
    failed_guild_ids: list[int] = []

    async with async_playwright() as pw:
        launch_kwargs: dict = {"headless": True}
        if CHROMIUM_EXECUTABLE:
            launch_kwargs["executable_path"] = CHROMIUM_EXECUTABLE
        browser = await pw.chromium.launch(**launch_kwargs)

        for guild_id in guild_ids:
            if done > 0:
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

            guild_scores = await scrape_guild(browser, guild_id)
            for name, score in guild_scores.items():
                if name not in all_scores or score > all_scores[name]:
                    all_scores[name] = score

            done += 1
            if guild_scores:
                non_empty += 1
            else:
                failed_guild_ids.append(guild_id)
            elapsed = time.perf_counter() - t_start
            rate = done / elapsed
            remaining = (len(guild_ids) - done) / rate if rate > 0 else 0
            print(
                f"  [{done}/{len(guild_ids)}] guild {guild_id}: "
                f"{len(guild_scores)} chars  "
                f"total={len(all_scores)}  "
                f"eta={remaining:.0f}s",
                flush=True,
            )

        await browser.close()

    wall = time.perf_counter() - t_start
    print()
    print(f"Done in {wall:.0f}s. {len(all_scores)} unique characters from {non_empty}/{done} guilds.")
    if failed_guild_ids:
        print(f"{len(failed_guild_ids)} guilds returned 0 results: {failed_guild_ids}")

    if args.failed_guilds is not None and failed_guild_ids:
        with open(args.failed_guilds, "w") as f:
            json.dump({"guild_ids": failed_guild_ids}, f, indent=2)
        print(f"Wrote {len(failed_guild_ids)} failed guild IDs to {args.failed_guilds}")

    characters = [
        {"name": name, "realm": REALM, "score": round(score, 1)}
        for name, score in sorted(all_scores.items())
    ]

    output = {
        "characters": characters,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "metric": "dps",
        "scorePolicy": SCORE_POLICY,
        "scorePolicyVersion": SCORE_POLICY_VERSION,
        "source": "warcraftlogs-scrape",
        "zoneIDs": ZONE_IDS,
    }

    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
