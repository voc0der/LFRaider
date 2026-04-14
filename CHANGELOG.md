## [Unreleased]

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
