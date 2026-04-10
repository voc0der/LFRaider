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

RANKINGS_QUERY = """
query LFRaiderRankings(
  $zoneID: Int!
  $serverRegion: String!
  $serverSlug: String!
  $page: Int!
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


def ranking_entries(payload: Any) -> list[dict[str, Any]]:
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
    if not isinstance(payload, dict):
        return False

    return bool(payload.get("hasMorePages") or payload.get("has_more_pages"))


def percentile_from_ranking(ranking: dict[str, Any]) -> float | None:
    for key in ("rankPercent", "percentile", "historicalPercent", "bracketPercent"):
        value = ranking.get(key)
        if value is not None:
            return float(value)
    return None


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
    return region, realms


def collect_realm_scores(args: argparse.Namespace, token: str, region: str, realm: dict[str, str], zone_id: int) -> dict[tuple[str, str], dict[str, Any]]:
    realm_name = realm["name"]
    realm_slug = realm.get("slug") or realm_name.lower().replace(" ", "")
    by_character: dict[tuple[str, str], dict[str, Any]] = {}

    for page in range(1, args.max_pages + 1):
        variables = {
            "zoneID": zone_id,
            "serverRegion": region,
            "serverSlug": realm_slug,
            "page": page,
            "metric": args.metric,
            "partition": args.partition,
        }
        data = graphql(args.graphql_url, token, RANKINGS_QUERY, variables)
        world_data = data.get("worldData") or {}
        zone = world_data.get("zone") or {}
        encounters = zone.get("encounters") or []
        if not isinstance(encounters, list):
            raise RuntimeError(f"unexpected encounters payload for {realm_name}: {encounters!r}")

        any_more = False
        any_entries = False

        for encounter in encounters:
            if not isinstance(encounter, dict):
                continue

            encounter_id = int(encounter.get("id") or 0)
            encounter_key = f"{zone_id}:{encounter_id}"
            rankings_payload = encounter.get("characterRankings")
            any_more = any_more or payload_has_more(rankings_payload)

            for ranking in ranking_entries(rankings_payload):
                name = name_from_ranking(ranking)
                percentile = percentile_from_ranking(ranking)
                if not name or percentile is None:
                    continue

                any_entries = True
                resolved_realm = realm_from_ranking(ranking, realm_name)
                key = (resolved_realm, name)
                character = by_character.setdefault(key, {"encounters": {}, "itemScores": []})
                character["encounters"][encounter_key] = max(character["encounters"].get(encounter_key, 0), percentile)
                item_score = item_score_from_ranking(ranking)
                if item_score and item_score > 0:
                    character["itemScores"].append(item_score)

        rate_limit = data.get("rateLimitData") or {}
        spent = rate_limit.get("pointsSpentThisHour")
        limit = rate_limit.get("limitPerHour")
        print(f"zone {zone_id} {realm_name} page {page}: {len(by_character)} characters, rate {spent}/{limit}")

        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

        if not any_more or not any_entries:
            break

    return by_character


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--realms", default="data/realms.json", type=Path)
    parser.add_argument("--output", default="data/scores.json", type=Path)
    parser.add_argument("--zone-id", default=env_int("WCL_ZONE_ID"), type=int)
    parser.add_argument("--zone-ids", default=env_str("WCL_ZONE_IDS"), help="Comma-separated Warcraft Logs zone IDs. Overrides --zone-id when set.")
    parser.add_argument("--metric", default=env_str("WCL_METRIC", "dps"))
    parser.add_argument("--partition", default=env_int("WCL_PARTITION"), type=int)
    parser.add_argument("--max-pages", default=env_int("WCL_MAX_PAGES") or 200, type=int)
    parser.add_argument("--sleep-seconds", default=env_float("WCL_SLEEP_SECONDS", 1.0), type=float)
    parser.add_argument("--token-url", default=env_str("WCL_TOKEN_URL", TOKEN_URL))
    parser.add_argument("--graphql-url", default=env_str("WCL_GRAPHQL_URL", GRAPHQL_URL))
    parser.add_argument("--distribution-approved", action="store_true")
    args = parser.parse_args()

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
        entry = {
            "name": name,
            "realm": realm,
            "score": round(score, 1),
            "encounters": len(encounter_scores),
        }
        if character["itemScores"]:
            entry["itemScore"] = round(max(character["itemScores"]), 1)
        characters.append(entry)

    document = {
        "generatedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "warcraftlogs-api-v2",
        "scorePolicy": "Mean of each character's best encounter rank percentiles across the configured zone IDs.",
        "zoneIDs": zone_ids,
        "metric": args.metric,
        "characters": characters,
    }
    if len(zone_ids) == 1:
        document["zoneID"] = zone_ids[0]

    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {args.output} with {len(characters)} characters")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
