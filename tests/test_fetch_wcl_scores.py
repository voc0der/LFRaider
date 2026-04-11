#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import io
import os
import sys
import tempfile
import urllib.error
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


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

    def test_request_json_retries_http_503_before_succeeding(self) -> None:
        original_urlopen = fetch_wcl_scores.urllib.request.urlopen
        original_sleep = fetch_wcl_scores.time.sleep
        attempts: list[int] = []
        sleeps: list[float] = []

        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, _exc_type, _exc, _tb) -> bool:
                return False

            def read(self) -> bytes:
                return b'{"ok": true}'

        def fake_urlopen(_request, timeout: int):
            attempts.append(timeout)
            if len(attempts) == 1:
                raise urllib.error.HTTPError(
                    "https://example.invalid/graphql",
                    503,
                    "Service Unavailable",
                    None,
                    io.BytesIO(b"temporary outage"),
                )
            return FakeResponse()

        fetch_wcl_scores.urllib.request.urlopen = fake_urlopen
        fetch_wcl_scores.time.sleep = sleeps.append
        try:
            output = io.StringIO()
            with redirect_stdout(output):
                payload = fetch_wcl_scores.request_json(
                    "https://example.invalid/graphql",
                    {"query": "{}"},
                    {"Content-Type": "application/json"},
                )
        finally:
            fetch_wcl_scores.urllib.request.urlopen = original_urlopen
            fetch_wcl_scores.time.sleep = original_sleep

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(len(attempts), 2)
        self.assertEqual(sleeps, [1.0])
        self.assertIn("HTTP 503", output.getvalue())

    def test_request_json_retries_timeout_before_succeeding(self) -> None:
        original_urlopen = fetch_wcl_scores.urllib.request.urlopen
        original_sleep = fetch_wcl_scores.time.sleep
        attempts = 0
        sleeps: list[float] = []

        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, _exc_type, _exc, _tb) -> bool:
                return False

            def read(self) -> bytes:
                return b'{"ok": true}'

        def fake_urlopen(_request, timeout: int):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise TimeoutError("The read operation timed out")
            return FakeResponse()

        fetch_wcl_scores.urllib.request.urlopen = fake_urlopen
        fetch_wcl_scores.time.sleep = sleeps.append
        try:
            output = io.StringIO()
            with redirect_stdout(output):
                payload = fetch_wcl_scores.request_json(
                    "https://example.invalid/graphql",
                    {"query": "{}"},
                    {"Content-Type": "application/json"},
                )
        finally:
            fetch_wcl_scores.urllib.request.urlopen = original_urlopen
            fetch_wcl_scores.time.sleep = original_sleep

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(attempts, 2)
        self.assertEqual(sleeps, [1.0])
        self.assertIn("read operation timed out", output.getvalue())

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
            return fetch_wcl_scores.RealmChunkResult(
                {"1047:649": [("Vocoder", "Dreamscythe", 74.7, 126.0)]},
                True,
                21,
            )

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
            return fetch_wcl_scores.RealmChunkResult(
                {"1047:649": [("Vocoder", realm["name"], 74.7, 126.0)]},
                True,
                2,
            )

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

    def test_fetch_realm_character_chunk_keeps_partial_pages_when_rate_limited(self) -> None:
        original_fetch_server_characters_page = fetch_wcl_scores.fetch_server_characters_page
        calls: list[int] = []

        def fake_fetch_server_characters_page(_args, _token, _realm_region, _realm_slug, realm_name, _zone_ids, page):
            calls.append(page)
            if page == 3:
                raise fetch_wcl_scores.RateLimitExceededError("quota exceeded")
            return (
                {"1047:649": [(f"Vocoder-{page}", realm_name, 70.0 + page, 126.0)]},
                100,
                True,
                20,
                154693,
                float(page * 1000),
                3600,
                3500 - page,
            )

        fetch_wcl_scores.fetch_server_characters_page = fake_fetch_server_characters_page
        try:
            args = SimpleNamespace(max_pages=20, pages_per_chunk=5, sleep_seconds=0)

            with redirect_stdout(io.StringIO()):
                result = fetch_wcl_scores.fetch_realm_character_chunk(
                    args,
                    "token",
                    "us",
                    {"name": "Dreamscythe", "slug": "dreamscythe", "region": "us"},
                    [1047],
                    1,
                )
        finally:
            fetch_wcl_scores.fetch_server_characters_page = original_fetch_server_characters_page

        self.assertEqual(calls, [1, 2, 3])
        self.assertTrue(result.rate_limited)
        self.assertFalse(result.exhausted)
        self.assertEqual(result.next_page, 3)
        self.assertEqual(
            result.encounter_raw["1047:649"],
            [
                ("Vocoder-1", "Dreamscythe", 71.0, 126.0),
                ("Vocoder-2", "Dreamscythe", 72.0, 126.0),
            ],
        )

    def test_fetch_realm_character_chunk_does_not_exhaust_when_rate_limited_before_any_progress(self) -> None:
        original_fetch_server_characters_page = fetch_wcl_scores.fetch_server_characters_page
        calls: list[int] = []

        def fake_fetch_server_characters_page(_args, _token, _realm_region, _realm_slug, _realm_name, _zone_ids, page):
            calls.append(page)
            raise fetch_wcl_scores.RateLimitExceededError("quota exceeded")

        fetch_wcl_scores.fetch_server_characters_page = fake_fetch_server_characters_page
        try:
            args = SimpleNamespace(max_pages=20, pages_per_chunk=20, sleep_seconds=0)

            with redirect_stdout(io.StringIO()):
                result = fetch_wcl_scores.fetch_realm_character_chunk(
                    args,
                    "token",
                    "us",
                    {"name": "Dreamscythe", "slug": "dreamscythe", "region": "us"},
                    [1047],
                    1,
                )
        finally:
            fetch_wcl_scores.fetch_server_characters_page = original_fetch_server_characters_page

        self.assertEqual(calls, [1])
        self.assertTrue(result.rate_limited)
        self.assertFalse(result.exhausted)
        self.assertEqual(result.next_page, 1)
        self.assertEqual(result.encounter_raw, {})

    def test_fetch_realm_character_chunk_keeps_partial_pages_when_transient_failure_interrupts(self) -> None:
        original_fetch_server_characters_page = fetch_wcl_scores.fetch_server_characters_page
        calls: list[int] = []

        def fake_fetch_server_characters_page(_args, _token, _realm_region, _realm_slug, realm_name, _zone_ids, page):
            calls.append(page)
            if page == 3:
                raise fetch_wcl_scores.TransientRequestError("HTTP 503: Service Unavailable")
            return (
                {"1047:649": [(f"Vocoder-{page}", realm_name, 70.0 + page, 126.0)]},
                100,
                True,
                20,
                154693,
                float(page * 1000),
                3600,
                3500 - page,
            )

        fetch_wcl_scores.fetch_server_characters_page = fake_fetch_server_characters_page
        try:
            args = SimpleNamespace(max_pages=20, pages_per_chunk=5, sleep_seconds=0)

            with redirect_stdout(io.StringIO()):
                result = fetch_wcl_scores.fetch_realm_character_chunk(
                    args,
                    "token",
                    "us",
                    {"name": "Dreamscythe", "slug": "dreamscythe", "region": "us"},
                    [1047],
                    1,
                )
        finally:
            fetch_wcl_scores.fetch_server_characters_page = original_fetch_server_characters_page

        self.assertEqual(calls, [1, 2, 3])
        self.assertFalse(result.rate_limited)
        self.assertTrue(result.interrupted)
        self.assertFalse(result.exhausted)
        self.assertEqual(result.next_page, 3)
        self.assertEqual(
            result.encounter_raw["1047:649"],
            [
                ("Vocoder-1", "Dreamscythe", 71.0, 126.0),
                ("Vocoder-2", "Dreamscythe", 72.0, 126.0),
            ],
        )

    def test_incremental_saves_partial_progress_when_rate_limited(self) -> None:
        original_fetch_realm_character_chunk = fetch_wcl_scores.fetch_realm_character_chunk

        def fake_fetch_realm_character_chunk(_args, _token, _region, _realm, _zone_ids, _start_page):
            return fetch_wcl_scores.RealmChunkResult(
                {"1047:649": [("Vocoder", "Dreamscythe", 74.7, 126.0)]},
                False,
                5,
                True,
            )

        fetch_wcl_scores.fetch_realm_character_chunk = fake_fetch_realm_character_chunk
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                state_file = Path(tmpdir) / "state.json"
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

        self.assertFalse(complete)
        self.assertEqual(state["progress"]["us/dreamscythe"]["nextPage"], 5)
        self.assertFalse(state["progress"]["us/dreamscythe"]["done"])
        self.assertEqual(state["encounterEntries"]["1047:649"][0], ["Vocoder", "Dreamscythe", 74.7, 126.0])

    def test_incremental_keeps_realm_pending_when_rate_limited_before_any_progress(self) -> None:
        original_fetch_realm_character_chunk = fetch_wcl_scores.fetch_realm_character_chunk

        def fake_fetch_realm_character_chunk(_args, _token, _region, _realm, _zone_ids, _start_page):
            return fetch_wcl_scores.RealmChunkResult({}, False, 1, True)

        fetch_wcl_scores.fetch_realm_character_chunk = fake_fetch_realm_character_chunk
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                state_file = Path(tmpdir) / "state.json"
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

        self.assertFalse(complete)
        self.assertEqual(state["progress"]["us/dreamscythe"]["nextPage"], 1)
        self.assertFalse(state["progress"]["us/dreamscythe"]["done"])
        self.assertEqual(state["encounterEntries"], {})

    def test_main_chunk_mode_soft_fails_when_token_request_is_transient(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            realms_file = tmp_path / "realms.json"
            realms_file.write_text(
                '{"region": "us", "realms": [{"name": "Dreamscythe", "slug": "dreamscythe", "region": "us"}]}\n',
                encoding="utf-8",
            )
            output_file = tmp_path / "scores.json"
            output_file.write_text('{"generatedAt":"unchanged"}\n', encoding="utf-8")
            state_file = tmp_path / "state.json"
            output = io.StringIO()

            with patch.object(
                fetch_wcl_scores,
                "get_access_token",
                side_effect=fetch_wcl_scores.TransientRequestError(
                    "https://example.invalid/oauth/token returned HTTP 503: temporary outage"
                ),
            ), patch.object(
                sys,
                "argv",
                [
                    "fetch_wcl_scores.py",
                    "--realms",
                    str(realms_file),
                    "--output",
                    str(output_file),
                    "--state-file",
                    str(state_file),
                    "--zone-id",
                    "1047",
                    "--distribution-approved",
                ],
            ), patch.dict(
                os.environ,
                {"WCL_CLIENT_ID": "client-id", "WCL_CLIENT_SECRET": "client-secret"},
                clear=False,
            ), redirect_stdout(output):
                result = fetch_wcl_scores.main()
            output_text = output.getvalue()
            output_contents = output_file.read_text(encoding="utf-8")
            state_exists = state_file.exists()

        self.assertEqual(result, 0)
        self.assertFalse(state_exists)
        self.assertEqual(output_contents, '{"generatedAt":"unchanged"}\n')
        self.assertIn("Transient upstream failure while requesting OAuth token", output_text)
        self.assertIn("Cycle incomplete", output_text)


if __name__ == "__main__":
    unittest.main()
