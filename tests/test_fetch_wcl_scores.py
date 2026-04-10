#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "tools" / "fetch_wcl_scores.py"

spec = importlib.util.spec_from_file_location("fetch_wcl_scores", MODULE_PATH)
fetch_wcl_scores = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(fetch_wcl_scores)


class FetchWclScoresTests(unittest.TestCase):
    def test_make_score_entry_prefers_wcl_percentile(self) -> None:
        entry = fetch_wcl_scores.make_score_entry(
            {
                "name": "Voidless",
                "serverName": "Dreamscythe",
                "rankPercent": 94.8,
                "amount": 1234.5,
                "itemLevel": 126,
            },
            "Dreamscythe",
            None,
        )

        self.assertEqual(entry, ("Voidless", "Dreamscythe", 94.8, 126.0))

    def test_extract_zone_rankings_uses_rank_percent_and_best_rank_item_level(self) -> None:
        encounter_raw = fetch_wcl_scores.extract_zone_rankings(
            {
                "rankings": [
                    {
                        "encounter": {"id": 50652, "name": "Attumen"},
                        "rankPercent": 94.4,
                        "bestRank": {"ilvl": 126},
                    }
                ]
            },
            1047,
            "Dreamscythe",
            "Voidless",
        )

        self.assertEqual(
            encounter_raw,
            {
                "1047:50652": [("Voidless", "Dreamscythe", 94.4, 126.0)],
            },
        )

    def test_server_characters_page_limit_clamps_to_api_cap(self) -> None:
        args = SimpleNamespace(page_size=1000, effective_page_size=1000)

        self.assertEqual(fetch_wcl_scores.server_characters_page_limit(args), 100)

    def test_scores_from_state_averages_stored_percentiles_and_keeps_best_duplicate(self) -> None:
        state = fetch_wcl_scores.new_state()
        state["encounterEntries"] = {
            "1047:649": [
                ["Voidless", "Dreamscythe", 95.0, 126.0],
                ["Voidless", "Dreamscythe", 93.0, 125.0],
            ],
            "1047:650": [
                ["Voidless", "Dreamscythe", 94.0, 127.0],
            ],
        }

        characters = fetch_wcl_scores.scores_from_state(state, [1047], "dps")

        self.assertEqual(
            characters,
            [
                {
                    "name": "Voidless",
                    "realm": "Dreamscythe",
                    "score": 94.5,
                    "encounters": 2,
                    "itemScore": 127.0,
                }
            ],
        )

    def test_collect_realm_scores_passes_page_size_and_uses_api_percentile(self) -> None:
        calls: list[dict[str, object]] = []
        original_graphql = fetch_wcl_scores.graphql

        def fake_graphql(_url: str, _token: str, _query: str, variables: dict[str, object]) -> dict[str, object]:
            calls.append(variables)
            return {
                "worldData": {
                    "zone": {
                        "name": "The Burning Crusade",
                        "encounters": [
                            {
                                "id": 649,
                                "name": "High King Maulgar",
                                "characterRankings": {
                                    "hasMorePages": False,
                                    "rankings": [
                                        {
                                            "name": "Voidless",
                                            "serverName": "Dreamscythe",
                                            "rankPercent": 95.0,
                                            "amount": 1888.7,
                                        }
                                    ],
                                },
                            }
                        ],
                    }
                },
                "rateLimitData": {"limitPerHour": 10000, "pointsSpentThisHour": 1},
            }

        fetch_wcl_scores.graphql = fake_graphql
        try:
            args = SimpleNamespace(
                graphql_url="https://example.invalid/graphql",
                max_pages=20,
                page_size=1000,
                metric="dps",
                partition=None,
                sleep_seconds=0,
            )
            with redirect_stdout(io.StringIO()):
                scores = fetch_wcl_scores.collect_realm_scores(
                    args,
                    "token",
                    "us",
                    {"name": "Dreamscythe", "slug": "dreamscythe", "region": "us"},
                    1047,
                )
        finally:
            fetch_wcl_scores.graphql = original_graphql

        self.assertEqual(calls[0]["pageSize"], 1000)
        self.assertEqual(scores[("Dreamscythe", "Voidless")]["encounters"]["1047:649"], 95.0)

    def test_collect_realm_scores_falls_back_to_api_default_when_explicit_sizes_fail(self) -> None:
        calls: list[dict[str, object]] = []
        original_graphql = fetch_wcl_scores.graphql

        def fake_graphql(_url: str, _token: str, _query: str, variables: dict[str, object]) -> dict[str, object]:
            calls.append(dict(variables))
            if "pageSize" in variables:
                raise RuntimeError("Unknown argument 'size' on field 'characterRankings'.")
            return {
                "worldData": {
                    "zone": {
                        "name": "The Burning Crusade",
                        "encounters": [
                            {
                                "id": 649,
                                "name": "High King Maulgar",
                                "characterRankings": {
                                    "hasMorePages": False,
                                    "rankings": [
                                        {
                                            "name": "Voidless",
                                            "serverName": "Dreamscythe",
                                            "rankPercent": 95.0,
                                        }
                                    ],
                                },
                            }
                        ],
                    }
                },
                "rateLimitData": {"limitPerHour": 10000, "pointsSpentThisHour": 1},
            }

        fetch_wcl_scores.graphql = fake_graphql
        try:
            args = SimpleNamespace(
                graphql_url="https://example.invalid/graphql",
                max_pages=20,
                page_size=1000,
                effective_page_size=1000,
                metric="dps",
                partition=None,
                sleep_seconds=0,
            )
            with redirect_stdout(io.StringIO()):
                scores = fetch_wcl_scores.collect_realm_scores(
                    args,
                    "token",
                    "us",
                    {"name": "Dreamscythe", "slug": "dreamscythe", "region": "us"},
                    1047,
                )
        finally:
            fetch_wcl_scores.graphql = original_graphql

        attempted_sizes = [call.get("pageSize") for call in calls]
        self.assertEqual(attempted_sizes, [1000, 500, 200, 100, None])
        self.assertIsNone(args.effective_page_size)
        self.assertEqual(scores[("Dreamscythe", "Voidless")]["encounters"]["1047:649"], 95.0)

    def test_collect_realm_scores_retries_when_rankings_payload_reports_invalid_size(self) -> None:
        calls: list[dict[str, object]] = []
        original_graphql = fetch_wcl_scores.graphql

        def fake_graphql(_url: str, _token: str, _query: str, variables: dict[str, object]) -> dict[str, object]:
            calls.append(dict(variables))
            if "pageSize" in variables:
                rankings_payload: object = {"error": "Invalid difficulty setting or size specified."}
            else:
                rankings_payload = {
                    "hasMorePages": False,
                    "rankings": [
                        {
                            "name": "Voidless",
                            "serverName": "Dreamscythe",
                            "rankPercent": 95.0,
                        }
                    ],
                }

            return {
                "worldData": {
                    "zone": {
                        "id": 1047,
                        "name": "Karazhan",
                        "encounters": [
                            {
                                "id": 50652,
                                "name": "Attumen",
                                "characterRankings": rankings_payload,
                            }
                        ],
                    }
                },
                "rateLimitData": {"limitPerHour": 10000, "pointsSpentThisHour": 1},
            }

        fetch_wcl_scores.graphql = fake_graphql
        try:
            args = SimpleNamespace(
                graphql_url="https://example.invalid/graphql",
                max_pages=20,
                page_size=1000,
                effective_page_size=1000,
                metric="dps",
                partition=None,
                sleep_seconds=0,
            )
            with redirect_stdout(io.StringIO()):
                scores = fetch_wcl_scores.collect_realm_scores(
                    args,
                    "token",
                    "us",
                    {"name": "Dreamscythe", "slug": "dreamscythe", "region": "us"},
                    1047,
                )
        finally:
            fetch_wcl_scores.graphql = original_graphql

        attempted_sizes = [call.get("pageSize") for call in calls]
        self.assertEqual(attempted_sizes, [1000, 500, 200, 100, None])
        self.assertIsNone(args.effective_page_size)
        self.assertEqual(scores[("Dreamscythe", "Voidless")]["encounters"]["1047:50652"], 95.0)

    def test_incremental_state_resets_when_score_policy_changes(self) -> None:
        original_fetch_realm_character_chunk = fetch_wcl_scores.fetch_realm_character_chunk

        def fake_fetch_realm_character_chunk(_args, _token, _region, _realm, _zone_ids, _start_page):
            return {"1047:649": [("Vocoder", "Dreamscythe", 74.7, 126.0)]}, True

        fetch_wcl_scores.fetch_realm_character_chunk = fake_fetch_realm_character_chunk
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                state_file = Path(tmpdir) / "state.json"
                state_file.write_text(
                    '{"complete": false, "cycle": 1, "progress": {}, '
                    '"encounterEntries": {"old": [["Bad", "Dreamscythe", "Rogue", 1, null]]}}\n',
                    encoding="utf-8",
                )
                args = SimpleNamespace(state_file=state_file, max_pages=20, pages_per_chunk=20)

                with redirect_stdout(io.StringIO()):
                    complete = fetch_wcl_scores.run_incremental(
                        args,
                        [1047],
                        "us",
                        [{"name": "Dreamscythe", "slug": "dreamscythe", "region": "us"}],
                        "token",
                    )

                state = fetch_wcl_scores.load_state(state_file)
        finally:
            fetch_wcl_scores.fetch_realm_character_chunk = original_fetch_realm_character_chunk

        self.assertTrue(complete)
        self.assertEqual(state["scorePolicyVersion"], fetch_wcl_scores.SCORE_POLICY_VERSION)
        self.assertNotIn("old", state["encounterEntries"])
        self.assertEqual(state["encounterEntries"]["1047:649"][0], ["Vocoder", "Dreamscythe", 74.7, 126.0])

    def test_incremental_fetches_only_one_realm_chunk_per_run(self) -> None:
        original_fetch_realm_character_chunk = fetch_wcl_scores.fetch_realm_character_chunk
        calls: list[str] = []

        def fake_fetch_realm_character_chunk(_args, _token, _region, realm, _zone_ids, _start_page):
            calls.append(realm["name"])
            return {"1047:649": [("Vocoder", realm["name"], 74.7, 126.0)]}, True

        fetch_wcl_scores.fetch_realm_character_chunk = fake_fetch_realm_character_chunk
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                state_file = Path(tmpdir) / "state.json"
                args = SimpleNamespace(state_file=state_file, max_pages=20, pages_per_chunk=1)

                with redirect_stdout(io.StringIO()):
                    complete = fetch_wcl_scores.run_incremental(
                        args,
                        [1047],
                        "us",
                        [
                            {"name": "Dreamscythe", "slug": "dreamscythe", "region": "us"},
                            {"name": "Nightslayer", "slug": "nightslayer", "region": "us"},
                        ],
                        "token",
                    )

                state = fetch_wcl_scores.load_state(state_file)
        finally:
            fetch_wcl_scores.fetch_realm_character_chunk = original_fetch_realm_character_chunk

        self.assertFalse(complete)
        self.assertEqual(calls, ["Dreamscythe"])
        self.assertTrue(state["progress"]["us/dreamscythe"]["done"])
        self.assertFalse(state["progress"].get("us/nightslayer", {}).get("done", False))


if __name__ == "__main__":
    unittest.main()
