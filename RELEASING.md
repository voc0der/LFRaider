# Releasing to CurseForge

## TBC Anniversary Support

LFRaider targets TBC Anniversary Classic. The TOC file specifies:

```text
## Interface: 20505
```

## Workflow Prerequisites

Before automated release can work end-to-end, configure:

1. GitHub Actions secret `RELEASE_PAT`
   - Fine-grained token with repository `Contents: Read and write`
   - Needed so workflow-created tag pushes can trigger the release workflow
2. GitHub Actions secret `CF_API_KEY`
   - CurseForge API token used by `BigWigsMods/packager`
3. CurseForge project metadata in addon TOC
   - Add `## X-Curse-Project-ID: <your_project_id>` to `LFRaider.toc` after CurseForge creates the project
   - Without this, packager can still build archives but cannot upload to CurseForge

## Data Refresh Prerequisites

Only configure these after Warcraft Logs/RPGLogs approves redistribution for this addon use case:

- Secret `WCL_CLIENT_ID`
- Secret `WCL_CLIENT_SECRET`
- Repository variable `LFR_WCL_DISTRIBUTION_APPROVED=true`
- Repository variable `WCL_ZONE_IDS`, comma-separated, for example `1047,1048`
- Legacy repository variable `WCL_ZONE_ID` is still supported for a single zone
- Optional repository variable `WCL_METRIC`, default `dps`
- Optional repository variable `WCL_PARTITION`
- Optional repository variable `WCL_GRAPHQL_URL`, default `https://www.warcraftlogs.com/api/v2/client`
- Optional repository variable `WCL_MAX_PAGES`, default `20`, which is the current WCL API maximum
- Optional repository variable `WCL_PAGE_SIZE`, default `1000`, to request more rankings within each capped WCL page
- Optional repository variable `WCL_SLEEP_SECONDS`, default `1.0`

## Automated Release Process

Normal code release:

1. Update version in `LFRaider.toc`
2. Update `CHANGELOG.md`
3. Commit and push to `main`
4. CI creates a tag from the TOC version and triggers the packager

Data-only release:

1. `Refresh Score Data` runs weekly or by manual dispatch
2. The workflow fetches scores, regenerates `LFRaider_Data.lua`, and checks for changes
3. If data changed, it bumps the patch version, updates the changelog, commits to `main`, and lets the tag workflow publish the new package

## Troubleshooting

- No new tag created:
  - Check `## Version:` in `LFRaider.toc` is bumped
  - If tag already exists, workflow skips by design
- Tag created but no release upload:
  - Confirm `RELEASE_PAT` exists so tag pushes trigger the release workflow
  - Confirm `CF_API_KEY` exists in repo secrets
  - Confirm `## X-Curse-Project-ID:` is set after CurseForge project creation
- Data refresh skipped:
  - Confirm `LFR_WCL_DISTRIBUTION_APPROVED=true`
  - Confirm WCL credentials and `WCL_ZONE_IDS` or `WCL_ZONE_ID` are configured

## What Gets Released

Only runtime addon files should ship to players.

The PR package workflow stages files directly from `LFRaider.toc`, and the release workflow verifies that `.pkgmeta` produces the same runtime-only tree before uploading.

For the current addon, the packaged game files are:

- `LFRaider.toc`
- `LFRaider_Data.lua`
- `LFRaider.lua`
