#!/usr/bin/env python3
"""Fetch Warcraft Logs guild rankings and score member rosters for LFRaider.

This script is intentionally permission-gated. RPGLogs' API terms restrict
building databases, permanent copies, and in-game addon redistribution unless
you have permission from the content owner. Set LFR_WCL_DISTRIBUTION_APPROVED
only after that permission exists.

Collection strategy:
  Phase 1 — guild list: page through zone guild rankings for each configured
             server until all guilds are collected (or --max-guilds is reached).
  Phase 2 — scoring: for each guild, fetch member zone rankings with
             timeframe: Recent and collapse into per-character scores.
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
from typing import Any


TOKEN_URL = "https://www.warcraftlogs.com/oauth/token"
GRAPHQL_URL = "https://www.warcraftlogs.com/api/v2/client"
TERMS_URL = "https://www.archon.gg/wow/articles/help/rpg-logs-api-terms-of-service"
REQUEST_TIMEOUT_SECONDS = 60
REQUEST_MAX_ATTEMPTS = 4
RETRYABLE_HTTP_STATUS_CODES: frozenset[int] = frozenset({408, 500, 502, 503, 504})
SCORE_POLICY_VERSION = 4
SCORE_POLICY = (
    "Mean of WCL per-encounter rank percentiles across the configured zone IDs, "
    "sourced from top guild rosters."
)


class RateLimitExceededError(RuntimeError):
    pass


class TransientRequestError(RuntimeError):
    pass


# ── GraphQL queries ───────────────────────────────────────────────────────────

GUILD_LIST_QUERY = """
query LFRaiderGuildList(
  $serverRegion: String!
  $serverSlug: String!
  $page: Int!
) {
  guildData {
    guilds(
      serverRegion: $serverRegion
      serverSlug: $serverSlug
      page: $page
    ) {
      data {
        id
        name
        server {
          name
          slug
          region {
            slug
          }
        }
      }
      has_more_pages
      current_page
      last_page
    }
  }
  rateLimitData {
    limitPerHour
    pointsSpentThisHour
    pointsResetIn
  }
}
"""


def build_guild_members_query(zone_ids: list[int], metric: str | None, partition: int | None) -> str:
    zone_fields: list[str] = []
    for zone_id in zone_ids:
        arguments = [f"zoneID: {zone_id}", "timeframe: Historical"]
        if metric:
            arguments.append(f"metric: {metric}")
        if partition is not None:
            arguments.append(f"partition: {partition}")
        zone_fields.append(f"          zone_{zone_id}: zoneRankings({', '.join(arguments)})")

    lines = [
        "query LFRaiderGuildMembers($guildID: Int!, $page: Int!) {",
        "  guildData {",
        "    guild(id: $guildID) {",
        "      name",
        "      members(page: $page) {",
        "        data {",
        "          name",
        "          hidden",
        *zone_fields,
        "        }",
        "        has_more_pages",
        "        current_page",
        "        last_page",
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
    return "\n".join(lines) + "\n"


# ── Infrastructure ────────────────────────────────────────────────────────────

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


def request_json(
    url: str,
    body: dict[str, Any] | bytes,
    headers: dict[str, str],
    auth: tuple[str, str] | None = None,
) -> dict[str, Any]:
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


def graphql_request(graphql_url: str, token: str, query: str, variables: dict[str, Any]) -> dict[str, Any]:
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


def _load_api_credentials() -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    cid = os.getenv("WCL_CLIENT_ID")
    csecret = os.getenv("WCL_CLIENT_SECRET")
    if cid and csecret:
        pairs.append((cid, csecret))
    n = 2
    while True:
        cid = os.getenv(f"WCL_CLIENT_ID_{n}")
        csecret = os.getenv(f"WCL_CLIENT_SECRET_{n}")
        if not cid or not csecret:
            break
        pairs.append((cid, csecret))
        n += 1
    return pairs


def load_realms(path: Path) -> tuple[str, list[dict[str, str]]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    region = str(document.get("region") or "us").strip().lower()
    realms = document.get("realms")
    if not isinstance(realms, list) or not realms:
        raise ValueError(f"{path} must contain a non-empty realms[] list")
    normalized: list[dict[str, str]] = []
    for realm in realms:
        if not isinstance(realm, dict):
            raise ValueError(f"realm entries in {path} must be objects")
        copy = dict(realm)
        copy["region"] = str(copy.get("region") or region).strip().lower()
        normalized.append(copy)
    return region, normalized


# ── Score helpers ─────────────────────────────────────────────────────────────

def normalize_score(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


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


def extract_zone_rankings(
    zone_payload: Any,
    zone_id: int,
    fallback_realm: str,
    character_name: str,
) -> dict[str, list[tuple[str, str, float]]]:
    zone_payload = decode_json_payload(zone_payload)
    if not isinstance(zone_payload, dict):
        return {}

    rankings = zone_payload.get("rankings")
    if not isinstance(rankings, list):
        return {}

    encounter_raw: dict[str, list[tuple[str, str, float]]] = {}
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

        server = ranking.get("server")
        realm = (
            str(server.get("name"))
            if isinstance(server, dict) and server.get("name")
            else fallback_realm
        )

        encounter_key = f"{zone_id}:{encounter_id}"
        encounter_raw.setdefault(encounter_key, []).append(
            (character_name, realm, normalize_score(float(percentile)))
        )
    return encounter_raw


def merge_encounter_raw(
    target: dict[str, list[tuple[str, str, float]]],
    incoming: dict[str, list[tuple[str, str, float]]],
) -> None:
    for key, raw_list in incoming.items():
        if raw_list:
            target.setdefault(key, []).extend(raw_list)


def merge_state_entries(
    enc_entries: dict[str, list[list[Any]]],
    encounter_raw: dict[str, list[tuple[str, str, float]]],
) -> None:
    for enc_key, raw_list in encounter_raw.items():
        stored = enc_entries.setdefault(enc_key, [])
        existing: dict[tuple[str, str], list[Any]] = {
            (e[1], e[0]): e for e in stored if len(e) >= 3
        }
        for name, realm, percentile in raw_list:
            entry = existing.get((realm, name))
            if entry:
                entry[2] = max(float(entry[2]), percentile)
            else:
                new_entry: list[Any] = [name, realm, percentile]
                stored.append(new_entry)
                existing[(realm, name)] = new_entry


def scores_from_state(state: dict[str, Any]) -> list[dict[str, Any]]:
    by_character: dict[tuple[str, str], dict[str, Any]] = {}
    for enc_key, raw_entries in state.get("encounterEntries", {}).items():
        if not raw_entries:
            continue
        for e in raw_entries:
            if len(e) < 3:
                continue
            name, realm = str(e[0]), str(e[1])
            percentile = normalize_score(float(e[2]))
            key = (realm, name)
            char = by_character.setdefault(key, {"encounters": {}})
            char["encounters"][enc_key] = max(char["encounters"].get(enc_key, 0.0), percentile)

    characters: list[dict[str, Any]] = []
    for (realm, name), char in sorted(by_character.items()):
        if not char["encounters"]:
            continue
        score = statistics.fmean(char["encounters"].values())
        characters.append({
            "name": name,
            "realm": realm,
            "score": round(score, 1),
            "encounters": len(char["encounters"]),
        })
    return characters


# ── State management ──────────────────────────────────────────────────────────

def new_state(cycle: int = 1) -> dict[str, Any]:
    return {
        "cycle": cycle,
        "complete": False,
        "scorePolicyVersion": SCORE_POLICY_VERSION,
        "phase": "guild_list",
        "guildListState": {},
        "guilds": [],
        "guildsDone": [],
        "encounterEntries": {},
    }


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return new_state()
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


# ── Rate limit ────────────────────────────────────────────────────────────────

def extract_rate_limit(data: dict[str, Any]) -> tuple[float | None, int | None, int | None]:
    rate_limit = data.get("rateLimitData") or {}
    spent = float(rate_limit["pointsSpentThisHour"]) if rate_limit.get("pointsSpentThisHour") is not None else None
    limit = int(rate_limit["limitPerHour"]) if rate_limit.get("limitPerHour") is not None else None
    reset_in = int(rate_limit["pointsResetIn"]) if rate_limit.get("pointsResetIn") is not None else None
    return spent, limit, reset_in


def make_token_rotator(tokens: list[str]) -> Any:
    state = {"idx": 0, "available_at": [0.0] * len(tokens)}

    def current() -> str:
        return tokens[state["idx"]]

    def rotate(reset_in: int | None = None) -> bool:
        idx = state["idx"]
        if reset_in is not None:
            state["available_at"][idx] = time.time() + reset_in
        idx += 1
        if idx < len(tokens):
            state["idx"] = idx
            print(f"API key {idx} exhausted, rotating to key {idx + 1}/{len(tokens)}")
            return True
        known = [(at, i) for i, at in enumerate(state["available_at"]) if at > 0]
        if not known:
            return False
        earliest_at, earliest_idx = min(known)
        wait = earliest_at - time.time()
        if wait > 180:
            return False
        if wait > 0:
            print(f"All API keys exhausted; sleeping {wait:.0f}s until key {earliest_idx + 1} resets")
            time.sleep(wait)
        state["idx"] = earliest_idx
        print(f"Resuming with API key {earliest_idx + 1}")
        return True

    return current, rotate


# ── Data collection ───────────────────────────────────────────────────────────

def fetch_guild_list_page(
    graphql_url: str,
    token: str,
    realm_region: str,
    realm_slug: str,
    page: int,
) -> tuple[list[dict[str, Any]], bool, float | None, int | None, int | None]:
    data = graphql_request(
        graphql_url,
        token,
        GUILD_LIST_QUERY,
        {"serverRegion": realm_region, "serverSlug": realm_slug, "page": page},
    )
    guild_data = data.get("guildData") or {}
    guilds_payload = guild_data.get("guilds") or {}
    raw_guilds = guilds_payload.get("data") or []
    has_more = bool(guilds_payload.get("has_more_pages"))
    spent, limit, reset_in = extract_rate_limit(data)

    guilds: list[dict[str, Any]] = []
    for g in raw_guilds:
        if not isinstance(g, dict) or not g.get("id"):
            continue
        server = g.get("server") or {}
        region_data = server.get("region") or {}
        guilds.append({
            "id": int(g["id"]),
            "name": str(g.get("name") or ""),
            "realm": str(server.get("name") or ""),
            "region": str(region_data.get("slug") or realm_region),
            "slug": str(server.get("slug") or realm_slug),
        })
    return guilds, has_more, spent, limit, reset_in


def fetch_guild_members_page(
    graphql_url: str,
    token: str,
    guild: dict[str, Any],
    zone_ids: list[int],
    metric: str | None,
    partition: int | None,
    page: int,
) -> tuple[dict[str, list[tuple[str, str, float]]], bool, float | None, int | None, int | None]:
    members_query = build_guild_members_query(zone_ids, metric, partition)
    data = graphql_request(
        graphql_url,
        token,
        members_query,
        {"guildID": guild["id"], "page": page},
    )
    guild_data = data.get("guildData") or {}
    guild_obj = guild_data.get("guild") or {}
    members_payload = guild_obj.get("members") or {}
    members = members_payload.get("data") or []
    has_more = bool(members_payload.get("has_more_pages"))
    spent, limit, reset_in = extract_rate_limit(data)

    fallback_realm = guild.get("realm") or ""
    encounter_raw: dict[str, list[tuple[str, str, float, float | None]]] = {}
    for member in members:
        if not isinstance(member, dict) or not member.get("name") or member.get("hidden"):
            continue
        char_name = str(member["name"])
        for zone_id in zone_ids:
            zone_payload = decode_json_payload(member.get(f"zone_{zone_id}"))
            char_entries = extract_zone_rankings(zone_payload, zone_id, fallback_realm, char_name)
            merge_encounter_raw(encounter_raw, char_entries)

    return encounter_raw, has_more, spent, limit, reset_in


# ── Orchestration ─────────────────────────────────────────────────────────────

def run_guild_collection(
    args: argparse.Namespace,
    zone_ids: list[int],
    region: str,
    realms: list[dict[str, str]],
    tokens: list[str],
) -> bool:
    current_token, rotate_token = make_token_rotator(tokens)

    state = load_state(args.state_file)
    if state.get("scorePolicyVersion") != SCORE_POLICY_VERSION:
        print("Score policy version changed — resetting state.")
        state = new_state(int(state.get("cycle", 1) or 1))
    if state.get("complete"):
        print("Previous cycle complete — starting new cycle.")
        state = new_state(int(state.get("cycle", 1) or 1) + 1)
    if getattr(args, "guild_list_only", False) and state.get("phase") != "guild_list":
        print("--guild-list-only: re-collecting guild list.")
        cycle = int(state.get("cycle", 1) or 1)
        state = new_state(cycle)
        state["phase"] = "guild_list"

    enc_entries: dict[str, list[list[Any]]] = state.setdefault("encounterEntries", {})
    guilds: list[dict[str, Any]] = state.setdefault("guilds", [])
    guilds_done: list[int] = state.setdefault("guildsDone", [])
    done_ids: set[int] = set(guilds_done)

    # ── Phase 1: collect guild list ───────────────────────────────────────────
    if state.get("phase") == "guild_list":
        known_ids: set[int] = {g["id"] for g in guilds}
        max_guilds = args.max_guilds or 0
        guild_list_state: dict[str, Any] = state.setdefault("guildListState", {})

        for realm in realms:
            realm_region = realm.get("region") or region
            realm_slug = realm.get("slug") or realm["name"].lower().replace(" ", "")
            realm_key = f"{realm_region}/{realm_slug}"
            realm_state = guild_list_state.setdefault(realm_key, {"page": 1, "done": False})
            if realm_state.get("done"):
                continue

            page = int(realm_state.get("page") or 1)
            while True:
                if max_guilds and len(guilds) >= max_guilds:
                    realm_state["done"] = True
                    break

                last_reset_in: int | None = None
                try:
                    page_guilds, has_more, spent, limit, last_reset_in = fetch_guild_list_page(
                        args.graphql_url, current_token(), realm_region, realm_slug, page
                    )
                except RateLimitExceededError as exc:
                    print(f"rate limited fetching guild list {realm_key} page {page}: {exc}")
                    realm_state["page"] = page
                    save_state(args.state_file, state)
                    if not rotate_token(last_reset_in):
                        return False
                    continue
                except TransientRequestError as exc:
                    print(f"transient failure fetching guild list {realm_key}: {exc}")
                    realm_state["page"] = page
                    save_state(args.state_file, state)
                    return False

                added = 0
                for g in page_guilds:
                    if g["id"] not in known_ids:
                        if not max_guilds or len(guilds) < max_guilds:
                            guilds.append(g)
                            known_ids.add(g["id"])
                            added += 1

                print(
                    f"guild list {realm_key} page {page}: "
                    f"{len(page_guilds)} guilds, {added} new, total {len(guilds)}, "
                    f"rate {spent}/{limit}, reset {last_reset_in}s"
                )
                page += 1
                realm_state["page"] = page

                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)

                if not has_more or (max_guilds and len(guilds) >= max_guilds):
                    realm_state["done"] = True
                    break

        all_realms_done = all(
            guild_list_state.get(f"{r.get('region') or region}/{r.get('slug') or r['name'].lower().replace(' ', '')}", {}).get("done")
            for r in realms
        ) or (max_guilds and len(guilds) >= max_guilds)

        if not all_realms_done:
            save_state(args.state_file, state)
            return False

        state["phase"] = "scoring"
        print(f"Guild list complete: {len(guilds)} guilds to score")
        save_state(args.state_file, state)
        if getattr(args, "guild_list_only", False):
            print("GUILD_LIST_COMPLETE")
            return True

    # ── Phase 2: score each guild's members ───────────────────────────────────
    if state.get("phase") == "scoring":
        for guild in guilds:
            guild_id = guild["id"]
            if guild_id in done_ids:
                continue

            guild_name = guild.get("name") or str(guild_id)
            page = 1
            last_reset_in = None
            while True:
                try:
                    enc_raw, has_more, spent, limit, last_reset_in = fetch_guild_members_page(
                        args.graphql_url,
                        current_token(),
                        guild,
                        zone_ids,
                        args.metric,
                        args.partition,
                        page,
                    )
                except RateLimitExceededError as exc:
                    print(f"rate limited fetching members of {guild_name} ({guild_id}) page {page}: {exc}")
                    save_state(args.state_file, state)
                    if not rotate_token(last_reset_in):
                        return False
                    continue
                except TransientRequestError as exc:
                    print(f"transient failure fetching members of {guild_name} ({guild_id}): {exc}")
                    save_state(args.state_file, state)
                    return False

                merge_state_entries(enc_entries, enc_raw)
                enc_rows = sum(len(v) for v in enc_raw.values())
                print(
                    f"guild {guild_name} ({guild_id}) page {page}: "
                    f"{enc_rows} encounter rows, rate {spent}/{limit}, reset {last_reset_in}s"
                )

                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)

                if not has_more:
                    guilds_done.append(guild_id)
                    done_ids.add(guild_id)
                    break
                page += 1

        save_state(args.state_file, state)

    all_done = bool(guilds) and all(g["id"] in done_ids for g in guilds)
    state["complete"] = all_done
    save_state(args.state_file, state)
    return all_done


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect WCL guild roster scores for LFRaider."
    )
    parser.add_argument("--realms", default="data/realms.json", type=Path)
    parser.add_argument("--output", default="data/scores.json", type=Path)
    parser.add_argument(
        "--state-file",
        default=None,
        type=Path,
        help="Resumable state file. Required.",
    )
    parser.add_argument(
        "--max-guilds",
        default=env_int("WCL_MAX_GUILDS") or 0,
        type=int,
        help="Cap on guilds to collect per zone. 0 = collect all (default).",
    )
    parser.add_argument("--zone-id", default=env_int("WCL_ZONE_ID"), type=int)
    parser.add_argument("--zone-ids", default=env_str("WCL_ZONE_IDS"))
    parser.add_argument("--metric", default=env_str("WCL_METRIC", "dps"))
    parser.add_argument("--partition", default=env_int("WCL_PARTITION"), type=int)
    parser.add_argument(
        "--sleep-seconds",
        default=env_float("WCL_SLEEP_SECONDS", 0.0),
        type=float,
    )
    parser.add_argument("--token-url", default=env_str("WCL_TOKEN_URL", TOKEN_URL))
    parser.add_argument("--graphql-url", default=env_str("WCL_GRAPHQL_URL", GRAPHQL_URL))
    parser.add_argument("--distribution-approved", action="store_true")
    parser.add_argument(
        "--guild-list-only",
        action="store_true",
        help="Collect guild IDs and exit without scoring. Always re-collects.",
    )
    args = parser.parse_args()

    if not args.state_file:
        raise SystemExit("--state-file is required")

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
            print(f"Transient upstream failure requesting OAuth token (key {idx}): {exc}")
            if not tokens:
                print("No tokens available — leaving scores.json unchanged.")
                return 0
    if not tokens:
        return 0
    print(f"Loaded {len(tokens)} API key(s)")

    cycle_complete = run_guild_collection(args, zone_ids, region, realms, tokens)
    if not cycle_complete:
        print("Cycle incomplete — leaving existing scores.json unchanged.")
        return 0

    state = load_state(args.state_file)
    characters = scores_from_state(state)
    if not characters:
        print("Cycle complete but no characters accumulated — leaving scores.json unchanged.")
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
    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {args.output} with {len(characters)} characters")
    print("CYCLE_COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
