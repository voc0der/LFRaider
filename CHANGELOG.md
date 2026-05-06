## [Unreleased]

## [0.1.19] - 2026-05-06

### Fixed
- Restrict /who chat annotations to actual /who result lines.

## [0.1.18] - 2026-05-06

### Changed
- Refreshed bundled score dataset.

## [0.1.17] - 2026-05-02

### Changed
- Refreshed bundled score dataset.

## [0.1.16] - 2026-04-30

### Changed
- Refreshed bundled score dataset.

## [0.1.15] - 2026-04-26

### Fixed
- Prevent score annotations on dice roll messages (e.g. "Vocoder rolls 26 (1-100)").

## [0.1.14] - 2026-04-25

### Fixed
- Prevent false-positive score annotations on quest/achievement system messages (e.g. "Quest accepted: Super Hot Stew").

## [0.1.13] - 2026-04-25

### Changed
- Refreshed bundled score dataset.

## [0.1.12] - 2026-04-24

### Changed
- Refreshed bundled score dataset.

## [0.1.11] - 2026-04-22

### Changed
- Refreshed bundled score dataset.

## [0.1.10] - 2026-04-22

### Changed
- Refreshed bundled score dataset.

## [0.1.9] - 2026-04-21

### Changed
- Refreshed bundled score dataset.

## [0.1.8] - 2026-04-14

### Changed
- Wired the release metadata to CurseForge project `1514107` so automated packager uploads are ready once the project is approved
- Added GitHub and CurseForge install links to the README

## [0.1.7] - 2026-04-14

### Fixed
- Kept compact LFG browse row metrics on a single aligned line so they do not overlap Blizzard's stock row layout
- Switched Who-pane and `/who` chat summaries to the same compact `WCL%` and `i###` style

## [0.1.6] - 2026-04-14

### Changed
- Reworked LFG browse row annotations into compact colored `WCL%` and `i###` markers so the list stays readable at a glance
- Added the same compact score snippets to LFG hover panes and search-entry leader tooltips

## [0.1.5] - 2026-04-14

### Changed
- Refreshed bundled score dataset.

### Fixed
- Rewind a small overlap window when chunked Warcraft Logs refreshes resume so live pagination drift is less likely to skip characters between runs

## [0.1.4] - 2026-04-10

### Changed
- Refreshed bundled score dataset.

## [0.1.3] - 2026-04-10

### Changed
- Refreshed bundled score dataset.

## [0.1.2] - 2026-04-10

### Changed
- Refreshed bundled score dataset.

### Fixed
- Refuse empty Warcraft Logs refresh results so CI cannot publish a zero-character dataset

## [0.1.1] - 2026-04-10

### Changed
- Refreshed bundled score dataset.

### Added
- Added a draggable minimap button with a toggle menu for each display surface and score mode
- Added separate display modes for Warcraft Logs overall ranking and last-known item score
- Added LFG browse row, LFG applicant, Who pane, and `/who` chat annotations for bundled characters
- Expanded generated data support to compact per-character tuples with WCL and item score fields
- Added multi-zone Warcraft Logs collection support via `WCL_ZONE_IDS`
- Expanded tests for minimap menu, LFG annotations, Who annotations, and chat filtering

## [0.1.0] - 2026-04-10

### Added
- Initial addon shell with tooltip and slash-command score lookup
- Added generated Lua data payload support with score quantization
- Added guarded Warcraft Logs collection tooling
- Added weekly data-refresh workflow, TOC-version tagging, PR packaging, and release packaging automation
- Added local regression tests for score lookup, slash output, dataset stats, and tooltip dedupe
