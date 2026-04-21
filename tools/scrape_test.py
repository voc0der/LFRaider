#!/usr/bin/env python3
"""Scrape test: measure guild rankings page fetch time via headless Chromium.

Usage:
    pip install playwright
    python tools/scrape_test.py [--guilds N] [--concurrency N] [--guild-id ID]

The page at /guild/rankings/{id}/latest?recent=true loads character score data
via an AJAX sub-request. A real browser is required because Cloudflare gates
the data endpoint behind a JS challenge on first visit. Each guild takes ~1s
when it has data; guilds with no recent zone rankings return immediately empty.

Typical result for Dreamscythe guilds: ~1s per guild that has data, 250 guilds
in ~5 minutes at concurrency=1, or proportionally faster with higher concurrency.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_URL = "https://fresh.warcraftlogs.com"
GUILD_URL = BASE_URL + "/guild/rankings/{guild_id}/latest?recent=true"
CHROMIUM_EXECUTABLE = "/usr/bin/chromium"
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)
DELAY_MIN = 0.5   # seconds between requests (randomized)
DELAY_MAX = 1.5


def load_guild_ids(n: int) -> list[int]:
    state_path = REPO_ROOT / "data" / "fetch_state.json"
    if state_path.exists():
        with open(state_path) as f:
            state = json.load(f)
        guilds = state.get("guilds", [])
        return [g["id"] for g in guilds[:n] if isinstance(g, dict)]
    raise FileNotFoundError(f"fetch_state.json not found at {state_path}")


async def scrape_guild(browser, guild_id: int, ajax_timeout_ms: int = 8_000) -> dict:
    """Fetch one guild page in a fresh browser context+tab, wait for AJAX, return rows.

    Each guild gets its own context so the Cloudflare JS challenge fires fresh
    and the page's jQuery AJAX call is guaranteed to trigger.
    """
    context = await browser.new_context(user_agent=UA)
    page = await context.new_page()

    ajax_done = asyncio.Event()
    ajax_body: list[bytes] = []

    async def on_response(resp):
        if "guild-rankings-for-zone" in resp.url:
            body = await resp.body()
            ajax_body.append(body)
            ajax_done.set()

    page.on("response", on_response)

    t0 = time.perf_counter()
    await page.goto(GUILD_URL.format(guild_id=guild_id), wait_until="domcontentloaded")

    # Wait for the AJAX sub-request to complete.
    try:
        await asyncio.wait_for(ajax_done.wait(), timeout=ajax_timeout_ms / 1000)
    except asyncio.TimeoutError:
        pass  # guild has no rankings data for this zone

    elapsed = time.perf_counter() - t0

    rows_data: list[list[str]] = []
    if ajax_body:
        row_els = await page.query_selector_all(".character-metric-table tbody tr")
        for row in row_els:
            cells = await row.query_selector_all("td")
            texts = [await c.inner_text() for c in cells]
            rows_data.append(texts)

    await context.close()
    return {
        "guild_id": guild_id,
        "elapsed": round(elapsed, 3),
        "row_count": len(rows_data),
        "ajax_bytes": len(ajax_body[0]) if ajax_body else 0,
        "rows": rows_data,
    }


async def run(playwright, guild_ids: list[int], concurrency: int) -> list[dict]:
    launch_kwargs: dict = {"headless": True}
    if CHROMIUM_EXECUTABLE:
        launch_kwargs["executable_path"] = CHROMIUM_EXECUTABLE

    browser = await playwright.chromium.launch(**launch_kwargs)

    semaphore = asyncio.Semaphore(concurrency)
    results = []

    async def fetch(gid: int, delay: float) -> dict:
        async with semaphore:
            if delay > 0:
                await asyncio.sleep(delay)
            result = await scrape_guild(browser, gid)
            status = f"{result['row_count']} rows" if result["row_count"] else "empty"
            print(f"  guild {gid}: {result['elapsed']:.2f}s  {status}  ({result['ajax_bytes']} bytes)")
            return result

    tasks = []
    for i, gid in enumerate(guild_ids):
        # stagger start times to avoid simultaneous requests
        base_delay = i * random.uniform(DELAY_MIN, DELAY_MAX) / concurrency
        tasks.append(fetch(gid, base_delay))

    results = await asyncio.gather(*tasks)
    await browser.close()
    return list(results)


async def main(args: argparse.Namespace) -> None:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("playwright not installed.  Run:  pip install playwright")
        raise SystemExit(1)

    guild_ids = [args.guild_id] if args.guild_id else load_guild_ids(args.guilds)
    print(f"Scraping {len(guild_ids)} guild(s) @ concurrency={args.concurrency}  "
          f"delay={DELAY_MIN}-{DELAY_MAX}s")
    print()

    t_total = time.perf_counter()
    async with async_playwright() as pw:
        results = await run(pw, guild_ids, args.concurrency)
    wall = time.perf_counter() - t_total

    elapsed_times = [r["elapsed"] for r in results]
    non_empty = [r for r in results if r["row_count"] > 0]
    total_rows = sum(r["row_count"] for r in results)

    print()
    print("─" * 54)
    print(f"Guilds scraped:    {len(results)}")
    print(f"With data:         {len(non_empty)}")
    print(f"Total char rows:   {total_rows}")
    print(f"Wall time:         {wall:.1f}s")
    if elapsed_times:
        avg = statistics.mean(elapsed_times)
        print(f"Per-guild time:    min={min(elapsed_times):.2f}s  avg={avg:.2f}s  max={max(elapsed_times):.2f}s")
        est = avg * 250 / max(args.concurrency, 1)
        print(f"Estimate 250 guilds (concurrency={args.concurrency}): {est:.0f}s ({est/60:.1f}min)")

    if non_empty:
        sample = non_empty[0]
        print(f"\nSample rows from guild {sample['guild_id']}:")
        print(f"  {'Name':<16} {'Avg':>5}  {'Per-boss percentiles...'}")
        for row in sample["rows"][:5]:
            if row:
                name = row[0]
                rest = "  ".join(f"{v:>5}" for v in row[1:])
                print(f"  {name:<16} {rest}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guilds", type=int, default=5, metavar="N",
                        help="number of guilds to test from fetch_state.json (default: 5)")
    parser.add_argument("--concurrency", type=int, default=1, metavar="N",
                        help="parallel browser tabs (default: 1)")
    parser.add_argument("--guild-id", type=int, default=None, metavar="ID",
                        help="test a single specific guild ID instead")
    asyncio.run(main(parser.parse_args()))
