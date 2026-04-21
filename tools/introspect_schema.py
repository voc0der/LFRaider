#!/usr/bin/env python3
"""Introspect the WCL GraphQL schema to discover available fields."""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request


TOKEN_URL = "https://www.warcraftlogs.com/oauth/token"
GRAPHQL_URL = os.getenv("WCL_GRAPHQL_URL", "https://www.warcraftlogs.com/api/v2/client")


def get_token() -> str:
    cid = os.environ["WCL_CLIENT_ID"]
    secret = os.environ["WCL_CLIENT_SECRET"]
    body = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode("ascii")
    req = urllib.request.Request(TOKEN_URL, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"})
    import base64
    req.add_header("Authorization", "Basic " + base64.b64encode(f"{cid}:{secret}".encode()).decode())
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())["access_token"]


def query(token: str, q: str) -> dict:
    data = json.dumps({"query": q}).encode()
    req = urllib.request.Request(GRAPHQL_URL, data=data, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def type_fields(token: str, type_name: str) -> list[str]:
    q = f"""{{ __type(name: "{type_name}") {{ fields {{ name }} }} }}"""
    result = query(token, q)
    fields = (result.get("data") or {}).get("__type") or {}
    return [f["name"] for f in (fields.get("fields") or [])]


def main() -> None:
    token = get_token()
    print(f"GraphQL URL: {GRAPHQL_URL}\n")

    for type_name in ["Zone", "GuildData", "WorldData", "Query"]:
        fields = type_fields(token, type_name)
        print(f"{type_name}: {fields}\n")


if __name__ == "__main__":
    main()
