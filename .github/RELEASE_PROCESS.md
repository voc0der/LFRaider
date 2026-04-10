# Release Process

## Automated Process

Every push to `main` triggers automatic tagging via GitHub Actions:

- Reads the version from `LFRaider.toc`
- Creates a git tag if one does not exist for that version
- Tag push triggers the packager workflow
- BigWigsMods/packager builds the release zip and uploads to GitHub and CurseForge when project metadata is configured

Update the version in `LFRaider.toc` before pushing normal releases.

Data refresh releases are different: the refresh workflow bumps the patch version when generated data changes.

## Prerequisites

- `RELEASE_PAT` repository secret:
  - Fine-grained PAT with repo `Contents: Read and write`
  - Required so workflow-created tag pushes can trigger downstream workflows
- `CF_API_KEY` repository secret:
  - Required for CurseForge upload in packager step
- `## X-Curse-Project-ID: <id>` in `LFRaider.toc`:
  - Required by packager to know which CurseForge project to publish to

## Manual Steps

### 1. Update Version

Update `## Version:` in `LFRaider.toc` to a version that is not already tagged.

### 2. Update CHANGELOG.md

```markdown
## [Unreleased]

### Added
- New feature description

### Fixed
- Bug fix description
```

### 3. Commit and Push

```bash
git add LFRaider.toc CHANGELOG.md
git commit -m "Release v0.1.X"
git push
```

The CI pipeline handles tagging and packaging automatically.

If no new tag appears, check whether the tag for that version already exists.
