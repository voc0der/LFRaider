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
import socket
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NamedTuple


TOKEN_URL = "https://www.warcraftlogs.com/oauth/token"
GRAPHQL_URL = "https://www.warcraftlogs.com/api/v2/client"
TERMS_URL = "https://www.archon.gg/wow/articles/help/rpg-logs-api-terms-of-service"
API_MAX_PAGE = 10_000
DEFAULT_PAGE_SIZE = 1000
DEFAULT_CHARACTER_QUERY_BATCH_SIZE = 25
DEFAULT_RESUME_OVERLAP_PAGES = 2
DEFAULT_REALM_CHUNKS_PER_RUN = 0
SERVER_CHARACTERS_MAX_LIMIT = 100
PAGE_SIZE_FALLBACKS: tuple[int | None, ...] = (500, 200, 100, None)
REQUEST_TIMEOUT_SECONDS = 60
REQUEST_MAX_ATTEMPTS = 4
RETRYABLE_HTTP_STATUS_CODES: frozenset[int] = frozenset({408, 500, 502, 503, 504})
SCORE_POLICY_VERSION = 3
SCORE_POLICY = "Mean of WCL per-encounter rank percentiles across the configured zone IDs."


class RateLimitExceededError(RuntimeError):
    """Raised when Warcraft Logs rejects a request for quota reasons."""


class TransientRequestError(RuntimeError):
    """Raised when a retryable upstream request keeps failing after retries."""


class RealmChunkResult(NamedTuple):
    """The incremental fetch result for one realm's current chunk."""

    encounter_raw: dict[str, list[tuple[str, str, float, float | None]]]
    exhausted: bool
    next_page: int
    rate_limited: bool = False
    interrupted: bool = False


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

SERVER_CHARACTERS_QUERY = """
query LFRaiderServerCharacterNames(
  $serverRegion: String!
  $serverSlug: String!
  $page: Int!
  $limit: Int!
) {
  worldData {
    server(region: $serverRegion, slug: $serverSlug) {
      characters(page: $page, limit: $limit) {
        data {
          name
          hidden
        }
        has_more_pages
        total
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


def active_page_size(args: argparse.Namespace) -> int | None:
    return getattr(args, "effective_page_size", getattr(args, "page_size", None))


def format_page_size(value: int | None) -> str:
    if value is None:
        return "API default"
    return str(value)


def server_characters_page_limit(args: argparse.Namespace) -> int:
    requested_size = active_page_size(args) or DEFAULT_PAGE_SIZE
    return min(requested_size, SERVER_CHARACTERS_MAX_LIMIT)


def resume_overlap_pages(args: argparse.Namespace) -> int:
    return max(0, int(getattr(args, "resume_overlap_pages", DEFAULT_RESUME_OVERLAP_PAGES)))


def realm_chunks_per_run_limit(args: argparse.Namespace) -> int | None:
    value = int(getattr(args, "realm_chunks_per_run", DEFAULT_REALM_CHUNKS_PER_RUN) or 0)
    if value <= 0:
        return None
    return value


def build_server_characters_query(metric: str | None, include_partition: bool, zone_ids: list[int]) -> str:
    variables = [
        "  $serverRegion: String!",
        "  $serverSlug: String!",
        "  $page: Int!",
        "  $limit: Int!",
    ]
    if metric:
        variables.append("  $metric: CharacterPageRankingMetricType")
    if include_partition:
        variables.append("  $partition: Int")

    zone_fields: list[str] = []
    for zone_id in zone_ids:
        arguments = [f"zoneID: {zone_id}", "timeframe: Historical"]
        if metric:
            arguments.append("metric: $metric")
        if include_partition:
            arguments.append("partition: $partition")
        zone_fields.append(f"          zone_{zone_id}: zoneRankings({', '.join(arguments)})")

    query_lines = [
        "query LFRaiderServerCharacters(",
        *variables,
        ") {",
        "  worldData {",
        "    server(region: $serverRegion, slug: $serverSlug) {",
        "      name",
        "      characters(page: $page, limit: $limit) {",
        "        data {",
        "          name",
        "          hidden",
        *zone_fields,
        "        }",
        "        current_page",
        "        has_more_pages",
        "        last_page",
        "        total",
        "      }",
        "    }",
        "  }",
        "  rateLimitData {",
        "    limitPerHour",
        "    pointsSpentThisHour",
        "    pointsResetIn",
        "  }",
        "}",
    ]
    return "\n".join(query_lines) + "\n"


def page_size_candidates(requested_size: int | None) -> list[int | None]:
    candidates: list[int | None] = [requested_size]
    for candidate in PAGE_SIZE_FALLBACKS:
        if candidate is not None and requested_size is not None and candidate >= requested_size:
            continue
        candidates.append(candidate)

    ordered: list[int | None] = []
    for candidate in candidates:
        if candidate not in ordered:
            ordered.append(candidate)
    return ordered


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


def request_retry_delay_seconds(attempt: int) -> float:
    return float(2 ** (attempt - 1))


def retryable_request_text(text: str) -> bool:
    lowered = text.lower()
    return any(
        marker in lowered
        for marker in (
            "timed out",
            "timeout",
            "temporary failure",
            "temporarily unavailable",
            "connection reset",
            "connection aborted",
            "connection refused",
            "remote end closed connection",
        )
    )


def is_retryable_request_exception(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in RETRYABLE_HTTP_STATUS_CODES

    if isinstance(exc, urllib.error.URLError):
        reason = exc.reason
        if isinstance(reason, BaseException):
            return is_retryable_request_exception(reason)
        return retryable_request_text(str(reason))

    if isinstance(exc, (TimeoutError, socket.timeout, ConnectionError)):
        return True

    return retryable_request_text(str(exc))


def log_request_retry(url: str, summary: str, attempt: int) -> None:
    wait_seconds = request_retry_delay_seconds(attempt)
    print(
        f"request to {url} failed ({summary}); "
        f"retrying in {wait_seconds:.1f}s "
        f"(attempt {attempt + 1}/{REQUEST_MAX_ATTEMPTS})"
    )
    time.sleep(wait_seconds)


def request_json(url: str, body: dict[str, Any] | bytes, headers: dict[str, str], auth: tuple[str, str] | None = None) -> dict[str, Any]:
    if isinstance(body, dict):
        data = json.dumps(body).encode("utf-8")
    else:
        data = body

    for attempt in range(1, REQUEST_MAX_ATTEMPTS + 1):
        request = urllib.request.Request(url, data=data, headers=headers, method="POST")
        if auth:
            import base64

            token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode("utf-8")).decode("ascii")
            request.add_header("Authorization", f"Basic {token}")

        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            try:
                detail = exc.read().decode("utf-8", errors="replace")
            finally:
                exc.close()
            if exc.code == 429:
                raise RateLimitExceededError(detail) from exc

            message = f"{url} returned HTTP {exc.code}: {detail}"
            if exc.code in RETRYABLE_HTTP_STATUS_CODES:
                if attempt < REQUEST_MAX_ATTEMPTS:
                    log_request_retry(url, f"HTTP {exc.code}", attempt)
                    continue
                raise TransientRequestError(message) from exc
            raise RuntimeError(message) from exc
        except urllib.error.URLError as exc:
            message = f"{url} request failed: {exc.reason}"
            if is_retryable_request_exception(exc):
                if attempt < REQUEST_MAX_ATTEMPTS:
                    log_request_retry(url, str(exc.reason), attempt)
                    continue
                raise TransientRequestError(message) from exc
            raise RuntimeError(message) from exc
        except (TimeoutError, socket.timeout, ConnectionError) as exc:
            message = f"{url} request failed: {exc}"
            if attempt < REQUEST_MAX_ATTEMPTS:
                log_request_retry(url, str(exc), attempt)
                continue
            raise TransientRequestError(message) from exc

    raise AssertionError("request_json exhausted retry loop without returning or raising")


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


def pagination_items(payload: Any) -> list[dict[str, Any]]:
    payload = decode_json_payload(payload)
    if not isinstance(payload, dict):
        return []

    items = payload.get("data") or payload.get("items") or payload.get("characters")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def pagination_has_more(payload: Any) -> bool:
    payload = decode_json_payload(payload)
    if not isinstance(payload, dict):
        return False
    return bool(payload.get("has_more_pages") or payload.get("hasMorePages"))


def pagination_last_page(payload: Any, current_page: int) -> int:
    payload = decode_json_payload(payload)
    if not isinstance(payload, dict):
        return current_page

    value = payload.get("last_page") or payload.get("lastPage")
    if value is None:
        return current_page
    return int(value)


def chunked(items: list[dict[str, str]], size: int) -> list[list[dict[str, str]]]:
    if size <= 0:
        raise ValueError("batch size must be positive")
    return [items[index:index + size] for index in range(0, len(items), size)]


def extract_zone_rankings(
    zone_payload: Any,
    zone_id: int,
    realm_name: str,
    character_name: str,
) -> dict[str, list[tuple[str, str, float, float | None]]]:
    zone_payload = decode_json_payload(zone_payload)
    if not isinstance(zone_payload, dict):
        return {}

    rankings = zone_payload.get("rankings")
    if not isinstance(rankings, list):
        return {}

    encounter_raw: dict[str, list[tuple[str, str, float, float | None]]] = {}
    for ranking in rankings:
        if not isinstance(ranking, dict):
            continue
        encounter = ranking.get("encounter")
        if not isinstance(encounter, dict):
            continue
        encounter_id = int(encounter.get("id") or 0)
        if encounter_id <= 0:
            continue

        percentile = ranking.get("rankPercent")
        if percentile is None:
            continue

        item_score = None
        best_rank = ranking.get("bestRank")
        if isinstance(best_rank, dict) and best_rank.get("ilvl") is not None:
            item_score = float(best_rank["ilvl"])

        encounter_key = f"{zone_id}:{encounter_id}"
        encounter_raw.setdefault(encounter_key, []).append(
            (character_name, realm_name, normalize_score(float(percentile)), item_score)
        )

    return encounter_raw


def merge_encounter_raw(
    target: dict[str, list[tuple[str, str, float, float | None]]],
    incoming: dict[str, list[tuple[str, str, float, float | None]]],
) -> None:
    for encounter_key, raw_list in incoming.items():
        if not raw_list:
            continue
        target.setdefault(encounter_key, []).extend(raw_list)


def merge_state_entries(
    enc_entries: dict[str, list[list[Any]]],
    encounter_raw: dict[str, list[tuple[str, str, float, float | None]]],
) -> None:
    for enc_key, raw_list in encounter_raw.items():
        stored = enc_entries.setdefault(enc_key, [])
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


def build_character_batch_query(
    zone_ids: list[int],
    characters: list[dict[str, str]],
    partition: int | None = None,
) -> tuple[str, dict[str, dict[str, str]]]:
    alias_map: dict[str, dict[str, str]] = {}
    selections: list[str] = []

    for index, character in enumerate(characters):
        alias = f"character_{index}"
        alias_map[alias] = character
        zone_fields: list[str] = []
        for zone_id in zone_ids:
            arguments = [f"zoneID: {zone_id}", "timeframe: Historical"]
            if partition is not None:
                arguments.append(f"partition: {partition}")
            zone_fields.append(f"zone_{zone_id}: zoneRankings({', '.join(arguments)})")

        selections.append(
            "\n".join(
                [
                    f"{alias}: character(",
                    f"  name: {json.dumps(character['name'])}",
                    f"  serverRegion: {json.dumps(character['region'])}",
                    f"  serverSlug: {json.dumps(character['slug'])}",
                    ") {",
                    "  name",
                    "  hidden",
                    *[f"  {field}" for field in zone_fields],
                    "}",
                ]
            )
        )

    query = "query LFRaiderCharacterBatch {\n  characterData {\n"
    for selection in selections:
        for line in selection.splitlines():
            query += f"    {line}\n"
    query += "  }\n}\n"
    return query, alias_map


def fetch_server_characters_page(
    args: argparse.Namespace,
    token: str,
    realm_region: str,
    realm_slug: str,
    realm_name: str,
    zone_ids: list[int],
    page: int,
) -> tuple[dict[str, list[tuple[str, str, float, float | None]]], int, bool, int, int | None, float | None, int | None, int | None]:
    variables: dict[str, Any] = {
        "serverRegion": realm_region,
        "serverSlug": realm_slug,
        "page": page,
        "limit": server_characters_page_limit(args),
    }
    if args.metric:
        variables["metric"] = args.metric
    if args.partition is not None:
        variables["partition"] = args.partition

    data = graphql(
        args.graphql_url,
        token,
        build_server_characters_query(args.metric, args.partition is not None, zone_ids),
        variables,
    )
    world_data = data.get("worldData") or {}
    server = world_data.get("server") or {}
    if not isinstance(server, dict):
        raise RuntimeError(f"Warcraft Logs did not return server data for {realm_region}/{realm_slug}")

    characters_payload = server.get("characters") or {}
    items = pagination_items(characters_payload)
    encounter_raw: dict[str, list[tuple[str, str, float, float | None]]] = {}
    ranked_characters = 0
    for item in items:
        if not isinstance(item, dict) or not item.get("name") or item.get("hidden"):
            continue
        character_entries: dict[str, list[tuple[str, str, float, float | None]]] = {}
        character_name = str(item["name"])
        for zone_id in zone_ids:
            merge_encounter_raw(
                character_entries,
                extract_zone_rankings(item.get(f"zone_{zone_id}"), zone_id, realm_name, character_name),
            )
        if character_entries:
            ranked_characters += 1
            merge_encounter_raw(encounter_raw, character_entries)

    last_page = pagination_last_page(characters_payload, page)
    total = None
    if isinstance(characters_payload, dict) and characters_payload.get("total") is not None:
        total = int(characters_payload["total"])
    rate_limit = data.get("rateLimitData") or {}
    spent = float(rate_limit["pointsSpentThisHour"]) if rate_limit.get("pointsSpentThisHour") is not None else None
    limit = int(rate_limit["limitPerHour"]) if rate_limit.get("limitPerHour") is not None else None
    reset_in = int(rate_limit["pointsResetIn"]) if rate_limit.get("pointsResetIn") is not None else None
    return (
        encounter_raw,
        len(items),
        pagination_has_more(characters_payload),
        last_page,
        total,
        spent,
        limit,
        reset_in,
    )


def fetch_character_batch_entries(
    args: argparse.Namespace,
    token: str,
    zone_ids: list[int],
    realm_name: str,
    characters: list[dict[str, str]],
) -> tuple[dict[str, list[tuple[str, str, float, float | None]]], int, float | None, int | None]:
    if not characters:
        return {}, 0, None, None

    query, alias_map = build_character_batch_query(zone_ids, characters, args.partition)
    data = graphql(args.graphql_url, token, query, {})
    character_data = data.get("characterData") or {}
    if not isinstance(character_data, dict):
        raise RuntimeError(f"Warcraft Logs characterData payload was not an object: {character_data!r}")

    rate_limit = data.get("rateLimitData") or {}
    spent = float(rate_limit["pointsSpentThisHour"]) if rate_limit.get("pointsSpentThisHour") is not None else None
    limit = int(rate_limit["limitPerHour"]) if rate_limit.get("limitPerHour") is not None else None

    encounter_raw: dict[str, list[tuple[str, str, float, float | None]]] = {}
    ranked_characters = 0
    for alias, character in alias_map.items():
        payload = character_data.get(alias)
        if not isinstance(payload, dict) or payload.get("hidden"):
            continue

        character_name = str(payload.get("name") or character["name"])
        character_entries: dict[str, list[tuple[str, str, float, float | None]]] = {}
        for zone_id in zone_ids:
            merge_encounter_raw(
                character_entries,
                extract_zone_rankings(payload.get(f"zone_{zone_id}"), zone_id, realm_name, character_name),
            )

        if character_entries:
            ranked_characters += 1
            merge_encounter_raw(encounter_raw, character_entries)

    return encounter_raw, ranked_characters, spent, limit


def fetch_realm_character_chunk(
    args: argparse.Namespace,
    token: str,
    default_region: str,
    realm: dict[str, str],
    zone_ids: list[int],
    start_page: int,
) -> RealmChunkResult:
    realm_name = realm["name"]
    realm_region = realm.get("region") or default_region
    realm_slug = realm.get("slug") or realm_name.lower().replace(" ", "")
    combo_key = f"{realm_region}/{realm_slug}"

    encounter_raw: dict[str, list[tuple[str, str, float, float | None]]] = {}
    exhausted = False
    next_page = start_page
    rate_limited = False
    interrupted = False

    if start_page > args.max_pages:
        print(
            f"realm {realm_region}/{realm_name}: "
            f"start page {start_page} exceeds max page {args.max_pages}; marking exhausted"
        )
        return RealmChunkResult(encounter_raw, True, start_page)

    end_page = min(start_page + args.pages_per_chunk - 1, args.max_pages)
    for page in range(start_page, end_page + 1):
        try:
            (
                page_entries,
                page_character_count,
                has_more_pages,
                last_page,
                total,
                spent,
                limit,
                reset_in,
            ) = fetch_server_characters_page(
                args,
                token,
                realm_region,
                realm_slug,
                realm_name,
                zone_ids,
                page,
            )
        except RateLimitExceededError as exc:
            print(f"rate limited while fetching {combo_key}: {exc}")
            rate_limited = True
            interrupted = True
            break
        except TransientRequestError as exc:
            print(f"transient upstream failure while fetching {combo_key}: {exc}")
            interrupted = True
            break
        ranked_characters = len({(realm_name, name) for rows in page_entries.values() for name, _, _, _ in rows})
        merge_encounter_raw(encounter_raw, page_entries)
        encounter_rows = sum(len(rows) for rows in page_entries.values())
        next_page = page + 1
        print(
            f"realm {realm_region}/{realm_name} page {page}: "
            f"{page_character_count} characters, {ranked_characters} with rankings, "
            f"{encounter_rows} encounter rows, total {total}, rate {spent}/{limit}, reset {reset_in}s"
        )

        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

        if not has_more_pages or page >= last_page:
            exhausted = True
            break

    return RealmChunkResult(encounter_raw, exhausted, next_page, rate_limited, interrupted)


def discover_realm_characters_chunk(
    args: argparse.Namespace,
    token: str,
    region: str,
    realm: dict[str, str],
    zone_ids: list[int],
    zone_next_pages: dict[str, int],
) -> tuple[dict[str, int], list[str], bool, bool]:
    """
    Page through encounter rankings (server-filtered) to discover character names.
    Returns (updated_zone_pages, new_names, all_zones_exhausted, rate_limited).
    """
    realm_name = realm["name"]
    realm_region = realm.get("region") or region
    realm_slug = realm.get("slug") or realm_name.lower().replace(" ", "")

    discovered: list[str] = []
    next_pages = dict(zone_next_pages)
    zones_done: set[str] = set()

    for zone_id in zone_ids:
        zone_key = str(zone_id)
        start_page = next_pages.get(zone_key, 1)
        end_page = start_page + args.pages_per_chunk - 1

        for page in range(start_page, end_page + 1):
            try:
                data = graphql_rankings_page(args, token, zone_id, realm_region, realm_name, realm_slug, page)
            except RateLimitExceededError as exc:
                print(f"rate limited during discovery of {realm_region}/{realm_slug} zone {zone_id}: {exc}")
                return next_pages, discovered, False, True
            except TransientRequestError as exc:
                print(f"transient failure during discovery of {realm_region}/{realm_slug}: {exc}")
                return next_pages, discovered, False, True

            world_data = data.get("worldData") or {}
            zone_data = world_data.get("zone") or {}
            encounters = zone_data.get("encounters") or []
            rate_limit = data.get("rateLimitData") or {}
            spent = rate_limit.get("pointsSpentThisHour")
            limit = rate_limit.get("limitPerHour")
            reset_in = rate_limit.get("pointsResetIn")

            any_more = False
            page_names: list[str] = []
            for encounter in encounters:
                rankings_payload = encounter.get("characterRankings")
                any_more = any_more or payload_has_more(rankings_payload)
                for ranking in ranking_entries(rankings_payload):
                    name = name_from_ranking(ranking)
                    if name:
                        page_names.append(name)

            discovered.extend(page_names)
            next_pages[zone_key] = page + 1
            print(
                f"discovery {realm_region}/{realm_name} zone {zone_id} page {page}: "
                f"{len(page_names)} names, rate {spent}/{limit}, reset {reset_in}s"
            )

            if not any_more:
                zones_done.add(zone_key)
                break

    all_exhausted = len(zones_done) == len(zone_ids)
    return next_pages, discovered, all_exhausted, False


def score_characters_chunk(
    args: argparse.Namespace,
    token: str,
    zone_ids: list[int],
    realm_name: str,
    char_dicts: list[dict[str, str]],
    offset: int,
) -> tuple[dict[str, list[tuple[str, str, float, float | None]]], int, bool]:
    """
    Batch-query zoneRankings for a chunk of discovered characters starting at offset.
    Returns (encounter_raw, new_offset, rate_limited).
    """
    encounter_raw: dict[str, list[tuple[str, str, float, float | None]]] = {}
    batch_size = args.character_query_batch_size
    chunk = char_dicts[offset: offset + args.pages_per_chunk * batch_size]
    processed = 0

    for batch in chunked(chunk, batch_size):
        try:
            batch_entries, ranked, pts_spent, pts_limit = fetch_character_batch_entries(args, token, zone_ids, realm_name, batch)
        except RateLimitExceededError as exc:
            print(f"rate limited during scoring of {realm_name}: {exc}")
            return encounter_raw, offset + processed, True
        except TransientRequestError as exc:
            print(f"transient failure during scoring of {realm_name}: {exc}")
            return encounter_raw, offset + processed, True

        merge_encounter_raw(encounter_raw, batch_entries)
        processed += len(batch)
        total = len(char_dicts)
        rate_info = f" [pts: {pts_spent}/{pts_limit}]" if pts_spent is not None else ""
        print(
            f"scored {realm_name}: {offset + processed}/{total} characters "
            f"({ranked} with rankings in this batch){rate_info}"
        )

    return encounter_raw, offset + processed, False


def supplement_realm_characters_chunk(
    args: argparse.Namespace,
    token: str,
    region: str,
    realm: dict[str, str],
    start_page: int,
) -> tuple[list[str], bool, int, bool]:
    """
    Page through WCL's server character registry to find names missed by encounter rankings.
    The registry uses a character-level index that includes characters whose fight-log entries
    lack a server ID association, which the encounter-rankings discovery misses.
    Returns (names, all_exhausted, next_page, rate_limited).
    """
    realm_name = realm["name"]
    realm_region = realm.get("region") or region
    realm_slug = realm.get("slug") or realm_name.lower().replace(" ", "")

    names: list[str] = []
    next_page = start_page
    end_page = start_page + args.pages_per_chunk - 1

    for page in range(start_page, end_page + 1):
        try:
            data = graphql(args.graphql_url, token, SERVER_CHARACTERS_QUERY, {
                "serverRegion": realm_region,
                "serverSlug": realm_slug,
                "page": page,
                "limit": SERVER_CHARACTERS_MAX_LIMIT,
            })
        except RateLimitExceededError as exc:
            print(f"rate limited during server supplement for {realm_region}/{realm_slug}: {exc}")
            return names, False, next_page, True
        except TransientRequestError as exc:
            print(f"transient failure during server supplement for {realm_region}/{realm_slug}: {exc}")
            return names, False, next_page, True

        world_data = data.get("worldData") or {}
        server_data = world_data.get("server") or {}
        characters_payload = server_data.get("characters") or {}
        items = pagination_items(characters_payload)
        rate_limit = data.get("rateLimitData") or {}
        spent = rate_limit.get("pointsSpentThisHour")
        limit_per_hour = rate_limit.get("limitPerHour")
        total = characters_payload.get("total") if isinstance(characters_payload, dict) else None

        page_names: list[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if not name or item.get("hidden"):
                continue
            page_names.append(str(name))

        names.extend(page_names)
        next_page = page + 1
        print(
            f"server supplement {realm_region}/{realm_name} page {page}: "
            f"{len(page_names)} names, total {total}, rate {spent}/{limit_per_hour}"
        )

        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

        if not pagination_has_more(characters_payload):
            return names, True, next_page, False

    return names, False, next_page, False


def run_hybrid_incremental(
    args: argparse.Namespace,
    zone_ids: list[int],
    region: str,
    realms: list[dict[str, str]],
    tokens: list[str],
) -> bool:
    """
    Three-phase incremental fetch:
      1. Discovery        — page through encounter rankings (server-filtered) to collect character names.
      2. Server supplement — page through WCL's server character registry to catch names whose fight-log
                             entries lack a server ID, which the encounter-rankings discovery misses.
      3. Scoring          — batch-query zoneRankings for all discovered characters.
    Returns True when the full cycle is complete.
    """
    token_idx = 0
    token = tokens[token_idx]

    def next_token() -> bool:
        nonlocal token_idx, token
        token_idx += 1
        if token_idx < len(tokens):
            token = tokens[token_idx]
            print(f"API key {token_idx} exhausted, rotating to key {token_idx + 1}/{len(tokens)}")
            return True
        return False

    state = load_state(args.state_file)
    if state.get("scorePolicyVersion") != SCORE_POLICY_VERSION:
        print("Score policy version mismatch — resetting hybrid state.")
        state = new_state(int(state.get("cycle", 1) or 1))

    if state.get("complete"):
        print("Cycle complete — resetting state for new cycle.")
        state = new_state(int(state.get("cycle", 1) or 1) + 1)

    progress: dict[str, Any] = state.setdefault("progress", {})
    state["scorePolicyVersion"] = SCORE_POLICY_VERSION
    enc_entries: dict[str, list[list[Any]]] = state.setdefault("encounterEntries", {})

    for realm in realms:
        realm_name = realm["name"]
        realm_region = realm.get("region") or region
        realm_slug = realm.get("slug") or realm_name.lower().replace(" ", "")
        combo_key = f"{realm_region}/{realm_slug}"

        combo = progress.setdefault(combo_key, {
            "phase": "discovery",
            "discoveryPages": {},
            "discoveredNames": [],
            "scoringOffset": 0,
            "done": False,
        })

        if combo.get("done"):
            continue

        phase = combo.get("phase", "discovery")

        if phase == "discovery":
            next_pages, names, all_exhausted, rate_limited = discover_realm_characters_chunk(
                args, token, region, realm, zone_ids, combo.setdefault("discoveryPages", {})
            )
            combo["discoveryPages"] = next_pages

            existing = set(combo.setdefault("discoveredNames", []))
            for name in names:
                if name not in existing:
                    combo["discoveredNames"].append(name)
                    existing.add(name)

            if all_exhausted:
                combo["phase"] = "server_supplement"
                combo.setdefault("supplementPage", 1)
                print(
                    f"{combo_key}: discovery complete — "
                    f"{len(combo['discoveredNames'])} names from encounter rankings, "
                    f"starting server supplement"
                )

            if rate_limited:
                if not next_token():
                    save_state(args.state_file, state)
                    return False

        if combo.get("phase") == "server_supplement":
            supp_page = combo.get("supplementPage", 1)
            while True:
                new_names, supp_exhausted, next_supp_page, rate_limited = supplement_realm_characters_chunk(
                    args, token, region, realm, supp_page
                )
                combo["supplementPage"] = next_supp_page

                existing = set(combo["discoveredNames"])
                added = 0
                for name in new_names:
                    if name not in existing:
                        combo["discoveredNames"].append(name)
                        existing.add(name)
                        added += 1

                if added:
                    print(f"{combo_key}: server supplement added {added} characters missed by encounter rankings")

                if supp_exhausted:
                    combo["phase"] = "scoring"
                    print(
                        f"{combo_key}: server supplement complete — "
                        f"{len(combo['discoveredNames'])} unique characters to score"
                    )

                if not rate_limited:
                    break
                if not next_token():
                    save_state(args.state_file, state)
                    return False
                supp_page = combo.get("supplementPage", 1)

        if combo.get("phase") == "scoring":
            all_chars = combo["discoveredNames"]
            char_dicts = [
                {"name": name, "region": realm_region, "slug": realm_slug}
                for name in all_chars
            ]

            while True:
                offset = combo.get("scoringOffset", 0)
                batch_raw, new_offset, rate_limited = score_characters_chunk(
                    args, token, zone_ids, realm_name, char_dicts, offset
                )
                merge_state_entries(enc_entries, batch_raw)
                combo["scoringOffset"] = new_offset

                if new_offset >= len(all_chars):
                    combo["done"] = True
                    print(f"{combo_key}: scoring complete")

                if not rate_limited:
                    break
                if not next_token():
                    save_state(args.state_file, state)
                    return False

    all_done = all(c.get("done", False) for c in progress.values()) and bool(progress)
    state["complete"] = all_done

    total_chars = len({(e[1], e[0]) for entries in enc_entries.values() for e in entries})
    print(
        f"state: {sum(1 for c in progress.values() if c.get('done'))} / {len(progress)} realms done, "
        f"~{total_chars} characters accumulated"
    )
    save_state(args.state_file, state)
    return all_done


def build_rankings_variables(
    zone_id: int,
    region: str,
    realm_slug: str,
    page: int,
    page_size: int | None,
    metric: str | None,
    partition: int | None,
) -> dict[str, Any]:
    variables: dict[str, Any] = {
        "zoneID": zone_id,
        "serverRegion": region,
        "serverSlug": realm_slug,
        "page": page,
        "metric": metric,
        "partition": partition,
    }
    if page_size is not None:
        variables["pageSize"] = page_size
    return variables


def should_retry_with_smaller_page_size(message: str, attempted_size: int | None) -> bool:
    if attempted_size is None:
        return False

    text = message.lower()
    if any(
        marker in text
        for marker in (
            "http 413",
            "http 429",
            "http 500",
            "http 502",
            "http 503",
            "http 504",
            "timed out",
            "timeout",
            "bad gateway",
            "gateway timeout",
            "service unavailable",
            "payload too large",
        )
    ):
        return True

    mentions_size = any(marker in text for marker in ("size", "page size", "pagesize", "$pagesize"))
    mentions_validation = any(
        marker in text
        for marker in (
            "unknown argument",
            "invalid",
            "invalid value",
            "expected type",
            "must be",
            "cannot be",
            "too large",
        )
    )
    return mentions_size and mentions_validation


def retryable_rankings_payload_error(data: dict[str, Any], attempted_size: int | None) -> str | None:
    world_data = data.get("worldData") or {}
    if not isinstance(world_data, dict):
        return None

    zone = world_data.get("zone") or {}
    if not isinstance(zone, dict):
        return None

    encounters = zone.get("encounters") or []
    if not isinstance(encounters, list):
        return None

    for encounter in encounters:
        if not isinstance(encounter, dict):
            continue
        rankings_error = payload_error(encounter.get("characterRankings"))
        if rankings_error and should_retry_with_smaller_page_size(rankings_error, attempted_size):
            encounter_id = int(encounter.get("id") or 0)
            zone_name = zone.get("name") or "unknown"
            return (
                f"Warcraft Logs characterRankings failed for zone {zone.get('id') or 'unknown'} "
                f"{zone_name}, encounter {encounter_id}: {rankings_error}"
            )

    return None


def graphql_rankings_page(
    args: argparse.Namespace,
    token: str,
    zone_id: int,
    region: str,
    realm_name: str,
    realm_slug: str,
    page: int,
) -> dict[str, Any]:
    requested_page_size = active_page_size(args)
    candidates = page_size_candidates(requested_page_size)
    last_error: RuntimeError | None = None

    for index, candidate in enumerate(candidates):
        variables = build_rankings_variables(zone_id, region, realm_slug, page, candidate, args.metric, args.partition)
        try:
            data = graphql(args.graphql_url, token, RANKINGS_QUERY, variables)
        except RuntimeError as exc:
            last_error = exc
            if not should_retry_with_smaller_page_size(str(exc), candidate):
                raise
            if index + 1 >= len(candidates):
                raise
            next_candidate = candidates[index + 1]
            print(
                f"zone {zone_id} {region}/{realm_name} page {page}: "
                f"page size {format_page_size(candidate)} failed ({exc}); "
                f"retrying with {format_page_size(next_candidate)}"
            )
            continue

        embedded_error = retryable_rankings_payload_error(data, candidate)
        if embedded_error:
            last_error = RuntimeError(embedded_error)
            if index + 1 >= len(candidates):
                return data
            next_candidate = candidates[index + 1]
            print(
                f"zone {zone_id} {region}/{realm_name} page {page}: "
                f"page size {format_page_size(candidate)} failed ({embedded_error}); "
                f"retrying with {format_page_size(next_candidate)}"
            )
            continue
        if candidate != requested_page_size:
            print(
                f"zone {zone_id} {region}/{realm_name}: "
                f"using page size {format_page_size(candidate)} after "
                f"{format_page_size(requested_page_size)} failed"
            )
        args.effective_page_size = candidate
        return data

    assert last_error is not None
    raise last_error


def collect_realm_scores(args: argparse.Namespace, token: str, default_region: str, realm: dict[str, str], zone_id: int) -> dict[tuple[str, str], dict[str, Any]]:
    realm_name = realm["name"]
    region = realm.get("region") or default_region
    realm_slug = realm.get("slug") or realm_name.lower().replace(" ", "")

    by_character: dict[tuple[str, str], dict[str, Any]] = {}
    zone_name = "unknown"

    for page in range(1, args.max_pages + 1):
        data = graphql_rankings_page(args, token, zone_id, region, realm_name, realm_slug, page)
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
            f"payload {first_payload_shape}, first ranking {first_ranking_shape}, "
            f"page size {format_page_size(active_page_size(args))}, rate {spent}/{limit}"
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
        data = graphql_rankings_page(args, token, zone_id, region, realm_name, realm_slug, page)
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
            f"payload {first_payload_shape}, first ranking {first_ranking_shape}, "
            f"page size {format_page_size(active_page_size(args))}, rate {spent}/{limit}"
        )

        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

        if not any_more or not any_entries:
            exhausted = True
            break

    return encounter_raw, exhausted


def run_incremental(args: argparse.Namespace, zone_ids: list[int], region: str, realms: list[dict[str, str]], tokens: list[str]) -> bool:
    """Fetch incremental chunks for pending realms. Returns True when the full cycle is complete."""
    token_idx = 0
    token = tokens[token_idx]

    def next_token() -> bool:
        nonlocal token_idx, token
        token_idx += 1
        if token_idx < len(tokens):
            token = tokens[token_idx]
            print(f"API key {token_idx} exhausted, rotating to key {token_idx + 1}/{len(tokens)}")
            return True
        return False

    state = load_state(args.state_file)

    if state.get("scorePolicyVersion") != SCORE_POLICY_VERSION:
        print("Fetch state score policy changed — resetting accumulated WCL state.")
        state = new_state(int(state.get("cycle", 1) or 1))

    if state.get("complete"):
        print("Cycle complete — resetting state for new cycle.")
        state = new_state(int(state.get("cycle", 1) or 1) + 1)

    progress: dict[str, Any] = state.setdefault("progress", {})
    state["scorePolicyVersion"] = SCORE_POLICY_VERSION
    overlap_pages = resume_overlap_pages(args)
    realm_chunk_limit = realm_chunks_per_run_limit(args)
    # encounterEntries: enc_key -> list of [name, realm, percentile, itemScore|null]
    enc_entries: dict[str, list[list[Any]]] = state.setdefault("encounterEntries", {})
    realm_progress: list[tuple[dict[str, str], str, dict[str, Any]]] = []
    for realm in realms:
        realm_name = realm["name"]
        realm_region = realm.get("region") or region
        realm_slug = realm.get("slug") or realm_name.lower().replace(" ", "")
        combo_key = f"{realm_region}/{realm_slug}"
        combo = progress.setdefault(combo_key, {"nextPage": 1, "done": False})
        realm_progress.append((realm, combo_key, combo))

    all_done = True
    made_progress = False
    processed_realm_chunks = 0
    for realm, combo_key, combo in realm_progress:
        if combo["done"]:
            continue

        all_done = False
        if realm_chunk_limit is not None and processed_realm_chunks >= realm_chunk_limit:
            break

        processed_realm_chunks += 1
        frontier_page = int(combo["nextPage"])
        start_page = max(1, frontier_page - overlap_pages)
        if start_page < frontier_page:
            print(
                f"rewinding {combo_key} from page {frontier_page} to {start_page} "
                "to recheck live pagination drift"
            )
        chunk = fetch_realm_character_chunk(args, token, region, realm, zone_ids, start_page)
        if chunk.rate_limited:
            next_token()
        chunk_interrupted = chunk.rate_limited or chunk.interrupted
        merge_state_entries(enc_entries, chunk.encounter_raw)

        if chunk.next_page > frontier_page:
            combo["nextPage"] = chunk.next_page
            made_progress = True
        if chunk.exhausted:
            combo["done"] = True
            print(f"combo {combo_key} exhausted")
            made_progress = True
        if chunk_interrupted and chunk.next_page > start_page:
            pages_fetched = chunk.next_page - start_page
            pages_advanced = max(0, chunk.next_page - frontier_page)
            overlap_note = ""
            if start_page < frontier_page:
                overlap_note = (
                    f", rechecked {frontier_page - start_page} overlap page(s)"
                    if pages_advanced > 0
                    else f", rechecked overlap pages up to {frontier_page - 1}"
                )
            print(
                f"saved partial progress for {combo_key}: "
                f"{pages_fetched} page(s){overlap_note}, "
                f"resume from page {combo['nextPage']} next run"
            )
        if chunk_interrupted:
            break

        if chunk.next_page <= frontier_page and not chunk.exhausted:
            print(f"No progress for {combo_key}; stopping incremental run early.")
            break

    all_combos_done = all(combo.get("done", False) for _, _, combo in realm_progress) and bool(realm_progress)
    if all_done and not all_combos_done:
        all_combos_done = True
    if not made_progress and not all_done:
        print("No progress this run; keeping fetch state as-is.")
    state["complete"] = all_combos_done

    total_chars = len({(e[1], e[0]) for entries in enc_entries.values() for e in entries})
    print(f"state: {sum(1 for c in progress.values() if c.get('done'))} / {len(progress)} combos done, ~{total_chars} characters accumulated")
    save_state(args.state_file, state)
    return all_combos_done


def _load_api_credentials() -> list[tuple[str, str]]:
    """Load WCL API credential pairs from env vars.

    Always reads WCL_CLIENT_ID / WCL_CLIENT_SECRET as the first key, then
    WCL_CLIENT_ID_2 / WCL_CLIENT_SECRET_2, WCL_CLIENT_ID_3 / WCL_CLIENT_SECRET_3, …
    stopping at the first missing numbered pair.
    """
    pairs: list[tuple[str, str]] = []
    base_id = os.getenv("WCL_CLIENT_ID")
    base_secret = os.getenv("WCL_CLIENT_SECRET")
    if base_id and base_secret:
        pairs.append((base_id, base_secret))
    n = 2
    while True:
        cid = os.getenv(f"WCL_CLIENT_ID_{n}")
        csecret = os.getenv(f"WCL_CLIENT_SECRET_{n}")
        if not cid or not csecret:
            break
        pairs.append((cid, csecret))
        n += 1
    return pairs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--realms", default="data/realms.json", type=Path)
    parser.add_argument("--output", default="data/scores.json", type=Path)
    parser.add_argument("--state-file", default=None, type=Path,
                        help="Path to incremental fetch state file. When set, runs in chunked mode.")
    parser.add_argument("--pages-per-chunk", default=env_int("WCL_PAGES_PER_CHUNK") or 20, type=int,
                        help="Pages to fetch for one realm per run in chunked mode.")
    parser.add_argument(
        "--resume-overlap-pages",
        default=env_int("WCL_RESUME_OVERLAP_PAGES")
        if env_int("WCL_RESUME_OVERLAP_PAGES") is not None
        else DEFAULT_RESUME_OVERLAP_PAGES,
        type=int,
        help="Pages to rewind and re-fetch before the saved frontier in chunked mode.",
    )
    parser.add_argument(
        "--realm-chunks-per-run",
        default=env_int("WCL_REALM_CHUNKS_PER_RUN")
        if env_int("WCL_REALM_CHUNKS_PER_RUN") is not None
        else DEFAULT_REALM_CHUNKS_PER_RUN,
        type=int,
        help="Realm chunks to process per incremental run. Use 0 to keep going until all pending realms are done or the run is interrupted.",
    )
    parser.add_argument("--zone-id", default=env_int("WCL_ZONE_ID"), type=int)
    parser.add_argument("--zone-ids", default=env_str("WCL_ZONE_IDS"), help="Comma-separated Warcraft Logs zone IDs. Overrides --zone-id when set.")
    parser.add_argument("--metric", default=env_str("WCL_METRIC", "dps"))
    parser.add_argument("--partition", default=env_int("WCL_PARTITION"), type=int)
    parser.add_argument("--max-pages", default=env_int("WCL_MAX_PAGES") or 200, type=int)
    parser.add_argument("--page-size", default=env_int("WCL_PAGE_SIZE") or DEFAULT_PAGE_SIZE, type=int,
                        help=f"Characters to request per server pagination page (Warcraft Logs caps this at {SERVER_CHARACTERS_MAX_LIMIT}).")
    parser.add_argument("--character-query-batch-size", default=env_int("WCL_CHARACTER_QUERY_BATCH_SIZE") or DEFAULT_CHARACTER_QUERY_BATCH_SIZE, type=int,
                        help="Number of character zone ranking lookups to batch into one GraphQL query.")
    parser.add_argument("--sleep-seconds", default=env_float("WCL_SLEEP_SECONDS", 1.0), type=float)
    parser.add_argument("--token-url", default=env_str("WCL_TOKEN_URL", TOKEN_URL))
    parser.add_argument("--graphql-url", default=env_str("WCL_GRAPHQL_URL", GRAPHQL_URL))
    parser.add_argument("--distribution-approved", action="store_true")
    parser.add_argument("--hybrid", action="store_true",
                        help="Use two-phase hybrid fetch: encounter rankings for discovery, "
                             "then zoneRankings batch queries for correct server percentiles.")
    args = parser.parse_args()


    if args.pages_per_chunk <= 0:
        raise SystemExit("--pages-per-chunk must be a positive integer")
    if args.resume_overlap_pages < 0:
        raise SystemExit("--resume-overlap-pages must be zero or greater")
    if args.realm_chunks_per_run < 0:
        raise SystemExit("--realm-chunks-per-run must be zero or greater")
    try:
        args.page_size = clamp_page_size(args.page_size)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    try:
        args.character_query_batch_size = clamp_page_size(args.character_query_batch_size)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    args.effective_page_size = args.page_size

    try:
        zone_ids = parse_zone_ids(args.zone_ids)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if not zone_ids and args.zone_id:
        zone_ids = [args.zone_id]
    if not zone_ids:
        raise SystemExit("WCL_ZONE_IDS or WCL_ZONE_ID is required")

    require_distribution_permission(args)

    credentials = _load_api_credentials()
    if not credentials:
        raise SystemExit("WCL_CLIENT_ID and WCL_CLIENT_SECRET are required")

    region, realms = load_realms(args.realms)
    tokens: list[str] = []
    for idx, (cid, csecret) in enumerate(credentials, start=1):
        try:
            tokens.append(get_access_token(cid, csecret, args.token_url))
        except TransientRequestError as exc:
            if args.state_file:
                key_suffix = f" (key {idx})" if len(credentials) > 1 else ""
                print(f"Transient upstream failure while requesting OAuth token{key_suffix}: {exc}")
                if not tokens:
                    print("Cycle incomplete — leaving existing scores.json unchanged.")
                    return 0
            else:
                raise SystemExit(str(exc)) from exc
    if not tokens:
        print("Cycle incomplete — leaving existing scores.json unchanged.")
        return 0
    print(f"Loaded {len(tokens)} API key(s)")

    # ── Incremental / chunked mode ────────────────────────────────────────────
    if args.state_file:
        if args.hybrid:
            cycle_complete = run_hybrid_incremental(args, zone_ids, region, realms, tokens)
        else:
            cycle_complete = run_incremental(args, zone_ids, region, realms, tokens)
        if not cycle_complete:
            print("Cycle incomplete — leaving existing scores.json unchanged.")
            return 0

        state = load_state(args.state_file)
        characters = scores_from_state(state, zone_ids, args.metric)
        if not characters:
            print("Cycle complete, but no characters accumulated — leaving existing scores.json unchanged.")
            return 0

        document: dict[str, Any] = {
            "generatedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "source": "warcraftlogs-api-v2",
            "scorePolicy": SCORE_POLICY,
            "scorePolicyVersion": SCORE_POLICY_VERSION,
            "zoneIDs": zone_ids,
            "metric": args.metric,
            "characters": characters,
        }
        if active_page_size(args) is not None:
            document["pageSize"] = active_page_size(args)
        if len(zone_ids) == 1:
            document["zoneID"] = zone_ids[0]
        args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote {args.output} with {len(characters)} characters")

        if cycle_complete:
            print("CYCLE_COMPLETE")  # workflow can detect this to trigger a full release
        return 0

    # ── Full / one-shot mode ─────────────────────────────────────────────────
    token = tokens[0]
    full_state = new_state()
    enc_entries: dict[str, list[list[Any]]] = full_state["encounterEntries"]
    for realm in realms:
        start_page = 1
        while start_page <= args.max_pages:
            chunk = fetch_realm_character_chunk(args, token, region, realm, zone_ids, start_page)
            chunk_interrupted = chunk.rate_limited or chunk.interrupted
            merge_state_entries(enc_entries, chunk.encounter_raw)
            if chunk_interrupted:
                realm_name = realm["name"]
                realm_region = realm.get("region") or region
                realm_slug = realm.get("slug") or realm_name.lower().replace(" ", "")
                failure_label = "Rate limited" if chunk.rate_limited else "Transient upstream failure"
                if chunk.next_page <= start_page:
                    raise SystemExit(
                        f"{failure_label} before any pages could be fetched for {realm_region}/{realm_slug}. "
                        "Retry later or use --state-file chunked mode."
                    )
                raise SystemExit(
                    f"{failure_label} after fetching through page {chunk.next_page - 1} for {realm_region}/{realm_slug}. "
                    "Retry later or use --state-file chunked mode."
                )
            if chunk.exhausted:
                break
            if chunk.next_page <= start_page:
                raise SystemExit(
                    f"Fetcher made no progress for {realm.get('region') or region}/{realm.get('slug') or realm['name'].lower().replace(' ', '')}."
                )
            start_page = chunk.next_page

    characters = scores_from_state(full_state, zone_ids, args.metric)

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
        "characters": characters,
    }
    if active_page_size(args) is not None:
        document["pageSize"] = active_page_size(args)
    if len(zone_ids) == 1:
        document["zoneID"] = zone_ids[0]

    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {args.output} with {len(characters)} characters")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
