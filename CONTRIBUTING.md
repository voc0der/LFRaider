# Contributing

Thanks for working on `LFRaider`.

This repo is split into a tiny runtime addon and heavier off-client tooling. Keep player-facing packages limited to files listed in `LFRaider.toc`.

## Local Setup

- Target client: TBC Anniversary Classic
- Addon install path: `World of Warcraft/_anniversary_/Interface/AddOns/`
- Runtime files are listed in [LFRaider.toc](LFRaider.toc)

Keep a local Blizzard UI mirror at `../wow-ui-source`. If you do not already have it checked out:

```bash
git clone https://github.com/Gethe/wow-ui-source ../wow-ui-source
```

Refresh the Blizzard UI reference before changing addon code:

```bash
git -C ../wow-ui-source pull --ff-only
```

Use `../wow-ui-source` first for TOC, interface number, FrameXML, Blizzard UI/API questions, LFG hook points, Who-frame behavior, and chat filter behavior before guessing at client behavior.

## Development

Current source-reference hook points:

- Who pane: `../wow-ui-source/Interface/AddOns/Blizzard_UIPanels_Game/Classic/FriendsFrame.lua`
- Premade Group Finder: `../wow-ui-source/Interface/AddOns/Blizzard_GroupFinder/Classic/LFGList.lua`
- Vanilla-style LFG browse pane: `../wow-ui-source/Interface/AddOns/Blizzard_GroupFinder_VanillaStyle/Blizzard_LFGVanilla_Browse.lua`
- LFG API shape: `../wow-ui-source/Interface/AddOns/Blizzard_APIDocumentationGenerated/LFGListInfoDocumentation.lua`
- `/who` slash behavior: `../wow-ui-source/Interface/AddOns/Blizzard_ChatFrameBase/Shared/SlashCommands.lua`

Run the local test suite:

```bash
lua tests/run.lua
```

Run a syntax check:

```bash
luac -p LFRaider.lua LFRaider_Data.lua tests/run.lua
```

Regenerate runtime data:

```bash
python3 tools/generate_data.py data/scores.json LFRaider_Data.lua
```

If you change packaging or release behavior, verify the runtime-only package contents:

```bash
./.github/scripts/verify-release-package.sh
```

## Warcraft Logs Data

Do not enable scheduled Warcraft Logs data publication until the redistribution/use case is approved by Warcraft Logs/RPGLogs.

The collector refuses to run unless `LFR_WCL_DISTRIBUTION_APPROVED=true` or `--distribution-approved` is passed. That flag should mean actual permission exists, not just "the code works."

For multi-raid refreshes, use `WCL_ZONE_IDS` as a comma-separated list, for example `1047,1048`. The collector still accepts the legacy single-zone `WCL_ZONE_ID` variable or `--zone-id` argument.

## Project Expectations

- Keep runtime logic small and lookup-oriented.
- Prefer generated data over runtime network calls.
- Keep LFG and Who hooks defensive; Blizzard frame names and mixins drift across Classic clients.
- Keep docs and CI honest about API limits and redistribution approval.
- If you add a runtime file, include it in `LFRaider.toc`.
- If you add development-only files, keep `.pkgmeta` ignoring them.

## Pull Requests

- Use conventional commit titles such as `feat(...)`, `fix(...)`, `docs(...)`, or `ci(...)`.
- Include a short summary of what changed and how you verified it.
- Keep PRs scoped to one logical change when possible.
