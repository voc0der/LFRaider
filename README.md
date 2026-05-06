<p align="center">
  <img src="assets/lfraidler-icon.png" alt="LFRaider icon" width="180" />
</p>

<p align="center">
  Warcraft Logs score signals in player tooltips, LFG rows, and <code>/who</code> results.
</p>

# LFRaider

- Adds bundled Warcraft Logs score signals to player tooltips
- Adds the same compact signals to LFG rows and browse hover panes when a known leader or member appears
- Appends compact signals to matching `/who` system-chat results
- Supports `/lfr` lookups for target, self, or `Name-Realm`
- Starts from a minimap menu with display toggles
- Keeps the runtime addon small: one Lua lookup file plus one generated Lua data file
- Includes CI plumbing for weekly data refreshes and release packaging

Current version: `0.1.20`

## Install

1. Download the latest release from [GitHub](https://github.com/voc0der/LFRaider/releases/latest) or [CurseForge](https://www.curseforge.com/wow/addons/lfraider).
2. Extract the `LFRaider` folder into:
   `World of Warcraft/_anniversary_/Interface/AddOns/`
3. Start the game and make sure the addon is enabled.

## Status

The addon shell works today with the seed data in `LFRaider_Data.lua`.

The Warcraft Logs collector is present but guarded. Do not enable scheduled data publishing until Warcraft Logs/RPGLogs approves this addon use case.

The collector fetches the top ranked guilds for each configured zone and scores their members using recent logs (`timeframe: Recent`). Configure zones via the repository variable `WCL_ZONE_IDS` with a comma-separated value such as `1047,1048`. The older single-zone `WCL_ZONE_ID` variable still works.

By default the collector fetches all ranked guilds. Set `WCL_MAX_GUILDS` to cap the number of guilds per zone.

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
- LFG pane annotations
- Who pane annotations
- `/who` chat annotations
- Tooltip lines

Right-click the minimap button to look up your target.

## Display Surfaces

- Unit tooltips: adds one line for Warcraft Logs overall when known
- LFG browse rows and hover panes: annotates known leaders and listed members
- LFG applicants: annotates known applicants
- Who pane: annotates known names in the Who tab
- `/who` chat: appends a compact summary to matching system messages when a bundled character name is found

## Data Shape

Runtime data is generated into `LFRaider_Data.lua`:

```lua
LFRaiderData = {
    scoreScale = 10,
    realms = {
        ["dreamscythe"] = {
            ["vocoder"] = 747,
        },
    },
}
```

Warcraft Logs scores are stored as tenths, so `747` displays as `74.7`.

## Contributing

Development and contribution notes are in [`CONTRIBUTING.md`](CONTRIBUTING.md).
Release workflow notes are in [`RELEASING.md`](RELEASING.md).

## Scope

- Target client: TBC Anniversary Classic
- TOC interface: `20505`
- Packaged runtime files: `LFRaider.toc`, `LFRaider_Data.lua`, `LFRaider.lua`
