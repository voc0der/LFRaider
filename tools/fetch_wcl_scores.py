#!/usr/bin/env python3
"""Fetch Warcraft Logs rankings and collapse them into LFRaider scores.

This script is intentionally permission-gated. RPGLogs' API terms restrict
building databases, permanent copies, and in-game addon redistribution unless
you have permission from the content owner. Set LFR_WCL_DISTRIBUTION_APPROVED
only after that permission exists.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TOKEN_URL = "https://www.warcraftlogs.com/oauth/token"
GRAPHQL_URL = "https://www.warcraftlogs.com/api/v2/client"
TERMS_URL = "https://www.archon.gg/wow/articles/help/rpg-logs-api-terms-of-service"
API_MAX_PAGE = 20
DEFAULT_PAGE_SIZE = 1000
SCORE_POLICY_VERSION = 2
SCORE_POLICY = "Mean of WCL per-encounter rank percentiles across the configured zone IDs."

RANKINGS_QUERY = """
query LFRaiderRankings(
  $zoneID: Int!
  $serverRegion: String!
  $serverSlug: String!
  $page: Int!
  $pageSize: Int
  $metric: CharacterRankingMetricType
  $partition: Int
) {
  worldData {
    zone(id: $zoneID) {
      id
      name
      encounters {
        id
        name
        characterRankings(
          serverRegion: $serverRegion
          serverSlug: $serverSlug
          page: $page
          size: $pageSize
          metric: $metric
          partition: $partition
        )
      }
    }
  }
  rateLimitData {
    limitPerHour
    pointsSpentThisHour
    pointsResetIn
  }
}
"""


def require_distribution_permission(args: argparse.Namespace) -> None:
    approved = args.distribution_approved or os.getenv("LFR_WCL_DISTRIBUTION_APPROVED") == "true"
    if approved:
        return

    raise SystemExit(
        "Refusing to build a redistributable Warcraft Logs data dump without explicit approval.\n"
        f"Read {TERMS_URL}, get permission for the addon use case, then set "
        "LFR_WCL_DISTRIBUTION_APPROVED=true or pass --distribution-approved."
    )


def env_int(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    return int(value)


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def clamp_page_size(value: int) -> int:
    if value <= 0:
        raise ValueError("page size must be a positive integer")
    return value


def env_str(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def parse_zone_ids(value: str | None) -> list[int]:
    if value is None:
        return []

    zone_ids: list[int] = []
    seen: set[int] = set()
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue

        try:
            zone_id = int(part)
        except ValueError as exc:
            raise ValueError(f"invalid zone ID {part!r}") from exc
        if zone_id <= 0:
            raise ValueError("zone IDs must be positive integers")
        if zone_id in seen:
            continue

        seen.add(zone_id)
        zone_ids.append(zone_id)

    return zone_ids


def request_json(url: str, body: dict[str, Any] | bytes, headers: dict[str, str], auth: tuple[str, str] | None = None) -> dict[str, Any]:
    if isinstance(body, dict):
        data = json.dumps(body).encode("utf-8")
    else:
        data = body

    request = urllib.request.Request(url, data=data, headers=headers, method="POST")
    if auth:
        import base64

        token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{url} returned HTTP {exc.code}: {detail}") from exc


def get_access_token(client_id: str, client_secret: str, token_url: str) -> str:
    body = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode("ascii")
    response = request_json(
        token_url,
        body,
        {"Content-Type": "application/x-www-form-urlencoded"},
        auth=(client_id, client_secret),
    )
    token = response.get("access_token")
    if not token:
        raise RuntimeError(f"OAuth token response did not include access_token: {response!r}")
    return str(token)


def graphql(graphql_url: str, token: str, query: str, variables: dict[str, Any]) -> dict[str, Any]:
    response = request_json(
        graphql_url,
        {"query": query, "variables": variables},
        {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    if response.get("errors"):
        raise RuntimeError(json.dumps(response["errors"], indent=2))
    return response.get("data") or {}


def decode_json_payload(payload: Any) -> Any:
    if not isinstance(payload, str):
        return payload

    text = payload.strip()
    if not text:
        return payload

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return payload


def payload_shape(payload: Any) -> str:
    payload = decode_json_payload(payload)
    if isinstance(payload, dict):
        if payload.get("error"):
            return f"error({payload['error']})"
        keys = ",".join(sorted(str(key) for key in payload.keys())[:6])
        return f"dict({keys})"
    if isinstance(payload, list):
        return f"list({len(payload)})"
    return type(payload).__name__


def payload_error(payload: Any) -> str | None:
    payload = decode_json_payload(payload)
    if isinstance(payload, dict) and payload.get("error"):
        return str(payload["error"])
    return None


def ranking_entries(payload: Any) -> list[dict[str, Any]]:
    payload = decode_json_payload(payload)

    if isinstance(payload, list):
        return [entry for entry in payload if isinstance(entry, dict)]

    if not isinstance(payload, dict):
        return []

    for key in ("rankings", "entries", "data"):
        entries = payload.get(key)
        if isinstance(entries, list):
            return [entry for entry in entries if isinstance(entry, dict)]

    return []


def payload_has_more(payload: Any) -> bool:
    payload = decode_json_payload(payload)

    if not isinstance(payload, dict):
        return False

    return bool(payload.get("hasMorePages") or payload.get("has_more_pages"))


def payload_count(payload: Any) -> int | None:
    payload = decode_json_payload(payload)

    if not isinstance(payload, dict):
        return None

    value = payload.get("total") or payload.get("totalCount") or payload.get("outOf")
    if value is None:
        return None

    return int(value)


def percentile_from_ranking(ranking: dict[str, Any], total_count: int | None = None, fallback_rank: int | None = None) -> float | None:
    for key in ("rankPercent", "percentile", "historicalPercent", "bracketPercent", "percent"):
        value = ranking.get(key)
        if value is not None:
            return float(value)

    rank = ranking.get("rank") or fallback_rank
    out_of = ranking.get("outOf") or ranking.get("total") or ranking.get("totalCount") or total_count
    if rank is None or out_of is None:
        return None

    rank = float(rank)
    out_of = float(out_of)
    if rank <= 0 or out_of <= 0:
        return None

    if out_of == 1:
        return 100.0

    return max(0.0, min(100.0, (1.0 - ((rank - 1.0) / (out_of - 1.0))) * 100.0))


def item_score_from_ranking(ranking: dict[str, Any]) -> float | None:
    for key in ("itemScore", "itemLevel", "ilvl", "gearScore"):
        value = ranking.get(key)
        if value is not None:
            return float(value)
    return None


def name_from_ranking(ranking: dict[str, Any]) -> str | None:
    for key in ("name", "characterName"):
        value = ranking.get(key)
        if value:
            return str(value)

    character = ranking.get("character")
    if isinstance(character, dict) and character.get("name"):
        return str(character["name"])

    return None


def realm_from_ranking(ranking: dict[str, Any], fallback: str) -> str:
    for key in ("serverName", "realmName"):
        value = ranking.get(key)
        if value:
            return str(value)

    server = ranking.get("server")
    if isinstance(server, dict) and server.get("name"):
        return str(server["name"])

    return fallback


def load_realms(path: Path) -> tuple[str, list[dict[str, str]]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    region = str(document.get("region") or "us").strip().lower()
    realms = document.get("realms")
    if not isinstance(realms, list) or not realms:
        raise ValueError(f"{path} must contain a non-empty realms[] list")

    normalized_realms = []
    for realm in realms:
        if not isinstance(realm, dict):
            raise ValueError(f"realm entries in {path} must be objects")

        realm_copy = dict(realm)
        realm_copy["region"] = str(realm_copy.get("region") or region).strip().lower()
        normalized_realms.append(realm_copy)

    return region, normalized_realms


def normalize_score(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def make_score_entry(ranking: dict[str, Any], realm_name: str, total_rankings: int | None) -> tuple[str, str, float, float | None] | None:
    name = name_from_ranking(ranking)
    if not name:
        return None

    percentile = percentile_from_ranking(ranking, total_rankings)
    if percentile is None:
        return None

    resolved_realm = realm_from_ranking(ranking, realm_name)
    item_score = item_score_from_ranking(ranking)
    return (name, resolved_realm, normalize_score(percentile), item_score)


def remember_character_score(
    by_character: dict[tuple[str, str], dict[str, Any]],
    encounter_key: str,
    name: str,
    realm: str,
    percentile: float,
    item_score: float | None,
) -> None:
    key = (realm, name)
    character = by_character.setdefault(key, {"encounters": {}, "itemScores": []})
    character["encounters"][encounter_key] = max(character["encounters"].get(encounter_key, 0), percentile)
    if item_score and item_score > 0:
        character["itemScores"].append(item_score)


def collect_realm_scores(args: argparse.Namespace, token: str, default_region: str, realm: dict[str, str], zone_id: int) -> dict[tuple[str, str], dict[str, Any]]:
    realm_name = realm["name"]
    region = realm.get("region") or default_region
    realm_slug = realm.get("slug") or realm_name.lower().replace(" ", "")

    by_character: dict[tuple[str, str], dict[str, Any]] = {}
    zone_name = "unknown"

    for page in range(1, args.max_pages + 1):
        variables = {
            "zoneID": zone_id,
            "serverRegion": region,
            "serverSlug": realm_slug,
            "page": page,
            "pageSize": args.page_size,
            "metric": args.metric,
            "partition": args.partition,
        }
        data = graphql(args.graphql_url, token, RANKINGS_QUERY, variables)
        world_data = data.get("worldData") or {}
        zone = world_data.get("zone") or {}
        zone_name = zone.get("name") or zone_name
        encounters = zone.get("encounters") or []
        if not isinstance(encounters, list):
            raise RuntimeError(f"unexpected encounters payload for {realm_name}: {encounters!r}")

        any_more = False
        any_entries = False
        first_payload_shape = "no encounters"
        first_ranking_shape = "no rankings"
        page_rankings = 0
        usable_rankings = 0
        missing_name = 0
        missing_percentile = 0

        for encounter in encounters:
            if not isinstance(encounter, dict):
                continue

            encounter_id = int(encounter.get("id") or 0)
            encounter_key = f"{zone_id}:{encounter_id}"
            rankings_payload = encounter.get("characterRankings")
            if first_payload_shape == "no encounters":
                first_payload_shape = payload_shape(rankings_payload)
            rankings_error = payload_error(rankings_payload)
            if rankings_error:
                raise RuntimeError(
                    f"Warcraft Logs characterRankings failed for zone {zone_id} "
                    f"{zone_name}, realm {realm_name}, encounter {encounter_id}: {rankings_error}"
                )
            any_more = any_more or payload_has_more(rankings_payload)
            total_rankings = payload_count(rankings_payload)

            entries = ranking_entries(rankings_payload)
            for ranking in entries:
                page_rankings += 1
                if first_ranking_shape == "no rankings":
                    first_ranking_shape = payload_shape(ranking)
                if not name_from_ranking(ranking):
                    missing_name += 1
                    continue
                score_entry = make_score_entry(ranking, realm_name, total_rankings)
                if not score_entry:
                    missing_percentile += 1
                    continue
                usable_rankings += 1
                name, resolved_realm, percentile, item_score = score_entry
                remember_character_score(by_character, encounter_key, name, resolved_realm, percentile, item_score)
                any_entries = True

        rate_limit = data.get("rateLimitData") or {}
        spent = rate_limit.get("pointsSpentThisHour")
        limit = rate_limit.get("limitPerHour")
        print(
            f"zone {zone_id} {zone_name} {region}/{realm_name} page {page}: "
            f"{len(by_character)} characters, {len(encounters)} encounters, "
            f"{page_rankings} rankings, {usable_rankings} usable, "
            f"missing name {missing_name}, missing percentile {missing_percentile}, "
            f"payload {first_payload_shape}, first ranking {first_ranking_shape}, rate {spent}/{limit}"
        )

        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

        if not any_more or not any_entries:
            break

    return by_character


def new_state(cycle: int = 1) -> dict[str, Any]:
    return {
        "cycle": cycle,
        "complete": False,
        "progress": {},
        "encounterEntries": {},
        "scorePolicyVersion": SCORE_POLICY_VERSION,
    }


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return new_state()
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def scores_from_state(state: dict[str, Any], zone_ids: list[int], metric: str) -> list[dict[str, Any]]:
    """Rebuild the character score list from accumulated encounter entries in state.

    Each encounter's entries are a list of [name, realm, percentile, itemScore|null].
    """
    by_character: dict[tuple[str, str], dict[str, Any]] = {}

    for enc_key, raw_entries in state.get("encounterEntries", {}).items():
        if not raw_entries:
            continue
        for e in raw_entries:
            if len(e) < 4:
                continue
            name, realm = str(e[0]), str(e[1])
            percentile = normalize_score(float(e[2]))
            item_score = float(e[3]) if e[3] is not None else None
            remember_character_score(by_character, enc_key, name, realm, percentile, item_score)

    characters = []
    for (realm, name), char in sorted(by_character.items()):
        if not char["encounters"]:
            continue
        score = statistics.fmean(char["encounters"].values())
        entry: dict[str, Any] = {
            "name": name,
            "realm": realm,
            "score": round(score, 1),
            "encounters": len(char["encounters"]),
        }
        if char["itemScores"]:
            entry["itemScore"] = round(max(char["itemScores"]), 1)
        characters.append(entry)
    return characters


def fetch_chunk(
    args: argparse.Namespace,
    token: str,
    default_region: str,
    realm: dict[str, str],
    zone_id: int,
    start_page: int,
) -> tuple[dict[str, list[tuple[str, str, float, float | None]]], bool]:
    """Fetch one chunk of pages for a single realm+zone. Returns (encounter_raw, exhausted)."""
    realm_name = realm["name"]
    region = realm.get("region") or default_region
    realm_slug = realm.get("slug") or realm_name.lower().replace(" ", "")
    # Entries: (name, realm, percentile, item_score)
    encounter_raw: dict[str, list[tuple[str, str, float, float | None]]] = {}
    zone_name = "unknown"
    exhausted = False

    if start_page > args.max_pages:
        print(
            f"zone {zone_id} {region}/{realm_name}: "
            f"start page {start_page} exceeds max page {args.max_pages}; marking exhausted"
        )
        return encounter_raw, True

    end_page = min(start_page + args.pages_per_chunk - 1, args.max_pages)
    for page in range(start_page, end_page + 1):
        variables = {
            "zoneID": zone_id,
            "serverRegion": region,
            "serverSlug": realm_slug,
            "page": page,
            "pageSize": args.page_size,
            "metric": args.metric,
            "partition": args.partition,
        }
        data = graphql(args.graphql_url, token, RANKINGS_QUERY, variables)
        world_data = data.get("worldData") or {}
        zone = world_data.get("zone") or {}
        zone_name = zone.get("name") or zone_name
        encounters = zone.get("encounters") or []
        if not isinstance(encounters, list):
            raise RuntimeError(f"unexpected encounters payload for {realm_name}: {encounters!r}")

        any_more = False
        any_entries = False
        first_payload_shape = "no encounters"
        first_ranking_shape = "no rankings"
        page_rankings = 0
        usable_rankings = 0
        missing_name = 0
        missing_percentile = 0

        for encounter in encounters:
            if not isinstance(encounter, dict):
                continue
            encounter_id = int(encounter.get("id") or 0)
            encounter_key = f"{zone_id}:{encounter_id}"
            rankings_payload = encounter.get("characterRankings")
            if first_payload_shape == "no encounters":
                first_payload_shape = payload_shape(rankings_payload)
            rankings_error = payload_error(rankings_payload)
            if rankings_error:
                raise RuntimeError(
                    f"Warcraft Logs characterRankings failed for zone {zone_id} "
                    f"{zone_name}, realm {realm_name}, encounter {encounter_id}: {rankings_error}"
                )
            any_more = any_more or payload_has_more(rankings_payload)
            total_rankings = payload_count(rankings_payload)
            entries = ranking_entries(rankings_payload)
            raw_list = encounter_raw.setdefault(encounter_key, [])
            for ranking in entries:
                page_rankings += 1
                if first_ranking_shape == "no rankings":
                    first_ranking_shape = payload_shape(ranking)
                if not name_from_ranking(ranking):
                    missing_name += 1
                    continue
                score_entry = make_score_entry(ranking, realm_name, total_rankings)
                if not score_entry:
                    missing_percentile += 1
                    continue
                usable_rankings += 1
                raw_list.append(score_entry)
                any_entries = True

        rate_limit = data.get("rateLimitData") or {}
        spent = rate_limit.get("pointsSpentThisHour")
        limit = rate_limit.get("limitPerHour")
        print(
            f"zone {zone_id} {zone_name} {region}/{realm_name} page {page}: "
            f"{len(encounter_raw)} encounters, {page_rankings} rankings, "
            f"{usable_rankings} usable, missing name {missing_name}, "
            f"missing percentile {missing_percentile}, "
            f"payload {first_payload_shape}, first ranking {first_ranking_shape}, rate {spent}/{limit}"
        )

        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

        if not any_more or not any_entries:
            exhausted = True
            break

    if not exhausted and end_page >= args.max_pages:
        exhausted = True

    return encounter_raw, exhausted


def run_incremental(args: argparse.Namespace, zone_ids: list[int], region: str, realms: list[dict[str, str]], token: str) -> bool:
    """Fetch next chunk for all realm+zone combos. Returns True when the full cycle is complete."""
    state = load_state(args.state_file)

    if state.get("scorePolicyVersion") != SCORE_POLICY_VERSION:
        print("Fetch state score policy changed — resetting accumulated WCL state.")
        state = new_state(int(state.get("cycle", 1) or 1))

    if state.get("complete"):
        print("Cycle complete — resetting state for new cycle.")
        state = new_state(int(state.get("cycle", 1) or 1) + 1)

    progress: dict[str, Any] = state.setdefault("progress", {})
    state["scorePolicyVersion"] = SCORE_POLICY_VERSION
    # encounterEntries: enc_key -> list of [name, realm, percentile, itemScore|null]
    enc_entries: dict[str, list[list[Any]]] = state.setdefault("encounterEntries", {})

    all_done = True
    for zone_id in zone_ids:
        for realm in realms:
            realm_name = realm["name"]
            realm_region = realm.get("region") or region
            realm_slug = realm.get("slug") or realm_name.lower().replace(" ", "")
            combo_key = f"{realm_region}/{realm_slug}/{zone_id}"

            combo = progress.setdefault(combo_key, {"nextPage": 1, "done": False})
            if combo["done"]:
                continue

            all_done = False
            start_page = combo["nextPage"]
            encounter_raw, exhausted = fetch_chunk(args, token, region, realm, zone_id, start_page)

            for enc_key, raw_list in encounter_raw.items():
                stored = enc_entries.setdefault(enc_key, [])
                # Index existing entries by (realm, name) to avoid duplicates across chunks.
                existing: dict[tuple[str, str], list[Any]] = {(e[1], e[0]): e for e in stored if len(e) >= 4}
                for name, resolved_realm, percentile, item_score in raw_list:
                    existing_entry = existing.get((resolved_realm, name))
                    if existing_entry:
                        existing_entry[2] = max(float(existing_entry[2]), percentile)
                        if item_score and item_score > 0:
                            old_item_score = float(existing_entry[3]) if existing_entry[3] is not None else 0.0
                            existing_entry[3] = max(old_item_score, item_score)
                    else:
                        new_entry = [name, resolved_realm, percentile, item_score]
                        stored.append(new_entry)
                        existing[(resolved_realm, name)] = new_entry

            combo["nextPage"] = start_page + args.pages_per_chunk
            if exhausted:
                combo["done"] = True
                print(f"combo {combo_key} exhausted")

    all_combos_done = all(c.get("done", False) for c in progress.values()) and bool(progress)
    if all_done and not all_combos_done:
        all_combos_done = True
    state["complete"] = all_combos_done

    total_chars = len({(e[1], e[0]) for entries in enc_entries.values() for e in entries})
    print(f"state: {sum(1 for c in progress.values() if c.get('done'))} / {len(progress)} combos done, ~{total_chars} characters accumulated")
    save_state(args.state_file, state)
    return all_combos_done


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--realms", default="data/realms.json", type=Path)
    parser.add_argument("--output", default="data/scores.json", type=Path)
    parser.add_argument("--state-file", default=None, type=Path,
                        help="Path to incremental fetch state file. When set, runs in chunked mode.")
    parser.add_argument("--pages-per-chunk", default=env_int("WCL_PAGES_PER_CHUNK") or 20, type=int,
                        help="Pages to fetch per realm+zone per run in chunked mode.")
    parser.add_argument("--zone-id", default=env_int("WCL_ZONE_ID"), type=int)
    parser.add_argument("--zone-ids", default=env_str("WCL_ZONE_IDS"), help="Comma-separated Warcraft Logs zone IDs. Overrides --zone-id when set.")
    parser.add_argument("--metric", default=env_str("WCL_METRIC", "dps"))
    parser.add_argument("--partition", default=env_int("WCL_PARTITION"), type=int)
    parser.add_argument("--max-pages", default=env_int("WCL_MAX_PAGES") or 200, type=int)
    parser.add_argument("--page-size", default=env_int("WCL_PAGE_SIZE") or DEFAULT_PAGE_SIZE, type=int,
                        help="Ranking entries to request per WCL page. Larger pages reduce truncation under the 20-page API limit.")
    parser.add_argument("--sleep-seconds", default=env_float("WCL_SLEEP_SECONDS", 1.0), type=float)
    parser.add_argument("--token-url", default=env_str("WCL_TOKEN_URL", TOKEN_URL))
    parser.add_argument("--graphql-url", default=env_str("WCL_GRAPHQL_URL", GRAPHQL_URL))
    parser.add_argument("--distribution-approved", action="store_true")
    args = parser.parse_args()

    if args.max_pages > API_MAX_PAGE:
        print(f"clamping --max-pages from {args.max_pages} to Warcraft Logs API max page {API_MAX_PAGE}")
        args.max_pages = API_MAX_PAGE
    try:
        args.page_size = clamp_page_size(args.page_size)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    try:
        zone_ids = parse_zone_ids(args.zone_ids)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if not zone_ids and args.zone_id:
        zone_ids = [args.zone_id]
    if not zone_ids:
        raise SystemExit("WCL_ZONE_IDS or WCL_ZONE_ID is required")

    require_distribution_permission(args)

    client_id = os.getenv("WCL_CLIENT_ID")
    client_secret = os.getenv("WCL_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise SystemExit("WCL_CLIENT_ID and WCL_CLIENT_SECRET are required")

    region, realms = load_realms(args.realms)
    token = get_access_token(client_id, client_secret, args.token_url)

    # ── Incremental / chunked mode ────────────────────────────────────────────
    if args.state_file:
        cycle_complete = run_incremental(args, zone_ids, region, realms, token)

        state = load_state(args.state_file)
        characters = scores_from_state(state, zone_ids, args.metric)
        if not characters:
            print("No characters accumulated yet — skipping scores.json update.")
            return 0

        document: dict[str, Any] = {
            "generatedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "source": "warcraftlogs-api-v2",
            "scorePolicy": SCORE_POLICY,
            "scorePolicyVersion": SCORE_POLICY_VERSION,
            "zoneIDs": zone_ids,
            "metric": args.metric,
            "pageSize": args.page_size,
            "characters": characters,
        }
        if len(zone_ids) == 1:
            document["zoneID"] = zone_ids[0]
        args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote {args.output} with {len(characters)} characters")

        if cycle_complete:
            print("CYCLE_COMPLETE")  # workflow can detect this to trigger a full release
        return 0

    # ── Full / one-shot mode (original behaviour) ─────────────────────────────
    combined: dict[tuple[str, str], dict[str, Any]] = {}
    for zone_id in zone_ids:
        for realm in realms:
            realm_scores = collect_realm_scores(args, token, region, realm, zone_id)
            for key, character in realm_scores.items():
                target = combined.setdefault(key, {"encounters": {}, "itemScores": []})
                target["encounters"].update(character["encounters"])
                target["itemScores"].extend(character["itemScores"])

    characters = []
    for (realm, name), character in sorted(combined.items()):
        encounter_scores = character["encounters"]
        if not encounter_scores:
            continue
        score = statistics.fmean(encounter_scores.values())
        entry: dict[str, Any] = {
            "name": name,
            "realm": realm,
            "score": round(score, 1),
            "encounters": len(encounter_scores),
        }
        if character["itemScores"]:
            entry["itemScore"] = round(max(character["itemScores"]), 1)
        characters.append(entry)

    if not characters:
        raise SystemExit(
            "Warcraft Logs returned 0 characters for the configured realms and zone IDs. "
            "Refusing to replace the bundled dataset with an empty dump."
        )

    document = {
        "generatedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "warcraftlogs-api-v2",
        "scorePolicy": SCORE_POLICY,
        "scorePolicyVersion": SCORE_POLICY_VERSION,
        "zoneIDs": zone_ids,
        "metric": args.metric,
        "pageSize": args.page_size,
        "characters": characters,
    }
    if len(zone_ids) == 1:
        document["zoneID"] = zone_ids[0]

    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {args.output} with {len(characters)} characters")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
