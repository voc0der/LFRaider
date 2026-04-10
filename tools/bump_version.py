#!/usr/bin/env python3
"""Bump the addon patch version after a generated data refresh."""

from __future__ import annotations

import argparse
import re
from datetime import date
from pathlib import Path


VERSION_RE = re.compile(r"^(## Version:\s*)(\d+)\.(\d+)\.(\d+)\s*$", re.MULTILINE)


def bump_toc(toc_path: Path) -> str:
    text = toc_path.read_text(encoding="utf-8")
    match = VERSION_RE.search(text)
    if not match:
        raise ValueError(f"could not find ## Version in {toc_path}")

    major, minor, patch = (int(match.group(i)) for i in range(2, 5))
    new_version = f"{major}.{minor}.{patch + 1}"
    text = VERSION_RE.sub(rf"\g<1>{new_version}", text, count=1)
    toc_path.write_text(text, encoding="utf-8", newline="\n")
    return new_version


def update_readme(readme_path: Path, version: str) -> None:
    if not readme_path.exists():
        return

    text = readme_path.read_text(encoding="utf-8")
    text = re.sub(r"Current version: `[^`]+`", f"Current version: `{version}`", text)
    readme_path.write_text(text, encoding="utf-8", newline="\n")


def update_changelog(changelog_path: Path, version: str, message: str) -> None:
    if not changelog_path.exists():
        return

    text = changelog_path.read_text(encoding="utf-8")
    heading = f"## [{version}] - {date.today().isoformat()}"
    if heading in text:
        return

    entry = f"\n{heading}\n\n### Changed\n- {message}\n"
    text = text.replace("## [Unreleased]\n", "## [Unreleased]\n" + entry, 1)
    changelog_path.write_text(text, encoding="utf-8", newline="\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--toc", default="LFRaider.toc", type=Path)
    parser.add_argument("--readme", default="README.md", type=Path)
    parser.add_argument("--changelog", default="CHANGELOG.md", type=Path)
    parser.add_argument("--message", default="Refreshed bundled score dataset.")
    args = parser.parse_args()

    version = bump_toc(args.toc)
    update_readme(args.readme, version)
    update_changelog(args.changelog, version, args.message)
    print(version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
