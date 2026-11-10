# Distillery Development Guide

## Quick Start

### Prerequisites

- Python 3.13 or later
- `uv` ([install](https://docs.astral.sh/uv/getting-started/installation/))

### Set Up a Local Environment

```bash
cd distillery
uv sync --all-groups
```

`arkminter` and `cascflow` are not on PyPI. `[tool.uv.sources]` in
`pyproject.toml` installs them from their GitHub repositories at a release
tag.

## Testing

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run one test file
uv run pytest tests/test_distillery_helpers.py -v
```

The tests replace ArchivesSpace and S3 calls with fakes and work in
temporary directories, so they need no settings file or network access.

## Code Quality

```bash
# Check code with Ruff
uv run ruff check .

# Format code with Ruff
uv run ruff format .
```

## Dependency Management

```bash
# Update all dependencies (generates a new uv.lock)
uv lock --upgrade

# Update one package
uv lock --upgrade-package urllib3

# Add a dependency
uv add <package-name>

# Add a development dependency
uv add --group dev <package-name>
```

**Dependencies:**

- `arkminter`: ARK minting
- `cascflow`: the ArchivesSpace workflow library shared with Alchemist,
  including the OCFL writer
- `python-decouple`: configuration

**Development dependencies:**

- `decouple-types`: type hints for python-decouple
- `pytest`: tests
- `ruff`: linting and formatting

### Developing Distillery with arkminter or cascflow

To work on `arkminter` or `cascflow` alongside Distillery, point
`[tool.uv.sources]` at a local checkout:

```toml
[tool.uv.sources]
arkminter = { path = "../arkminter", editable = true }
cascflow = { path = "../cascflow", editable = true }
```

```bash
uv sync --all-groups
uv run pytest
```

Changes in the sibling checkout are picked up without reinstalling. Before
committing, release the sibling package, point `[tool.uv.sources]` back at
its new tag, and lock again:

```bash
gh release list -R caltechlibrary/cascflow | head -5
```

```toml
[tool.uv.sources]
arkminter = { git = "https://github.com/caltechlibrary/arkminter", tag = "vX.Y.Z" }
cascflow = { git = "https://github.com/caltechlibrary/cascflow", tag = "vX.Y.Z" }
```

```bash
uv lock
uv sync --all-groups
uv run pytest
```

Never push `[tool.uv.sources]` pointing at a local path. To check which
version is installed:

```bash
uv pip show cascflow
```

## Release & Versioning

Distillery follows [Semantic Versioning](https://semver.org/), and the
version lives in `pyproject.toml`. To release:

1. Make sure the tests and Ruff pass.
2. Move the `[Unreleased]` entries in `CHANGELOG.md` under a new version
   heading, and update the compare links at the bottom.
3. Set `version` in `pyproject.toml` and `codemeta.json`, then run `uv lock`.
4. Commit and push to `main`.

`tag-release.yml` tags `vX.Y.Z` when the version in `pyproject.toml`
changes.

## Running Distillery Locally

```bash
# Validate (requires settings.ini)
uv run distillery.py validate --context <VOLUME>

# Execute (requires settings.ini)
uv run distillery.py execute --context <VOLUME>
```

See [CONFIGURATION.md](CONFIGURATION.md) for the settings file.

## Architecture Decisions

Decisions about the preservation storage are recorded as ADRs in
[docs/decisions/](docs/decisions/); see
[ADR 0001](docs/decisions/0001-record-architecture-decisions.md) for the
conventions.

## Continuous Integration

`test.yml` runs the tests and Ruff on every push and pull request, and
`tag-release.yml` tags a release when the version in `pyproject.toml`
changes.

## Troubleshooting

### Import Errors with Local Packages

```bash
uv sync --all-groups --reinstall
```

### Settings File Not Found

Make sure `settings.ini` exists next to `distillery.py`, or pass
`--settings` with its path.
