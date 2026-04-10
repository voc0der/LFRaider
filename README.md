# LFRaider

- Adds bundled Warcraft Logs and last-known item score signals to player tooltips
- Adds the same compact signals to LFG rows when a known leader or applicant appears
- Appends compact signals to matching `/who` system-chat results
- Supports `/lfr` lookups for target, self, or `Name-Realm`
- Starts from a minimap menu with display toggles
- Keeps the runtime addon small: one Lua lookup file plus one generated Lua data file
- Includes CI plumbing for weekly data refreshes and release packaging

Current version: `0.1.0`

## Status

The addon shell works today with the seed data in `LFRaider_Data.lua`.

The Warcraft Logs collector is present but guarded. Do not enable scheduled data publishing until Warcraft Logs/RPGLogs approves this addon use case.

The collector supports one or more Warcraft Logs ranking zones. In GitHub Actions, prefer the repository variable `WCL_ZONE_IDS` with a comma-separated value such as `1047,1048`. The older single-zone `WCL_ZONE_ID` variable still works.

## Why The Guard Exists

Warcraft Logs Classic Fresh uses OAuth 2.0 and supports public API access through the client credentials flow. The official docs say to create a client, exchange `client_id` and `client_secret` for an access token, then call the public GraphQL endpoint at `https://www.warcraftlogs.com/api/v2/client`.

The blocker is redistribution, not authentication. RPGLogs' API terms restrict building databases or permanent copies of API content, and call out presenting content through in-game add-ons unless permission applies. They also require approval for commercial use.

Relevant docs:

- Warcraft Logs Classic Fresh API docs: https://www.archon.gg/classic-fresh/articles/help/api-documentation
- RPGLogs API terms: https://www.archon.gg/wow/articles/help/rpg-logs-api-terms-of-service

## Usage

- `/lfr`: Look up your target, or yourself if you have no target
- `/lfr target`: Look up your target
- `/lfr self`: Look up yourself
- `/lfr Vocoder-Dreamscythe`: Look up a specific character
- `/lfr stats`: Show bundled dataset info
- `/lfr menu`: Open the minimap-style toggle menu
- `/lfr minimap`: Show or hide the minimap button
- `/lfr wcl on`: Show Warcraft Logs overall ranking
- `/lfr wcl off`: Hide Warcraft Logs overall ranking
- `/lfr item on`: Show last known item score
- `/lfr item off`: Hide last known item score
- `/lfr lfg on`: Enable LFG pane annotations
- `/lfr lfg off`: Disable LFG pane annotations
- `/lfr who on`: Enable Who pane annotations
- `/lfr who off`: Disable Who pane annotations
- `/lfr whochat on`: Enable `/who` chat annotations
- `/lfr whochat off`: Disable `/who` chat annotations
- `/lfr tooltip on`: Enable tooltip lines
- `/lfr tooltip off`: Disable tooltip lines

## Minimap Menu

Left-click the minimap button for display toggles:

- Warcraft Logs overall
- Last known item score
- LFG pane annotations
- Who pane annotations
- `/who` chat annotations
- Tooltip lines

Right-click the minimap button to look up your target.

## Display Surfaces

- Unit tooltips: adds one line for Warcraft Logs and one line for item score when known
- LFG browse rows: annotates known leaders
- LFG applicants: annotates known applicants
- Who pane: annotates known names in the Who tab
- `/who` chat: appends a compact summary to matching system messages when a bundled character name is found

## Data Shape

Runtime data is generated into `LFRaider_Data.lua`:

```lua
LFRaiderData = {
    scoreScale = 10,
    itemScoreScale = 1,
    fields = {
        wclOverall = 1,
        itemScore = 2,
    },
    realms = {
        ["dreamscythe"] = {
            ["vocoder"] = {747, 126},
        },
    },
}
```

Warcraft Logs scores are stored as tenths, so `747` displays as `74.7`. Item score is stored as a whole number by default.

## Local Development

Regenerate the runtime data from JSON:

```bash
python3 tools/generate_data.py data/scores.json LFRaider_Data.lua
```

Run tests:

```bash
lua tests/run.lua
```

Run a syntax check:

```bash
luac -p LFRaider.lua LFRaider_Data.lua tests/run.lua
```

Verify the runtime-only package:

```bash
./.github/scripts/verify-release-package.sh
```

## Scope

- Target client: TBC Anniversary Classic
- TOC interface: `20505`
- Packaged runtime files: `LFRaider.toc`, `LFRaider_Data.lua`, `LFRaider.lua`
