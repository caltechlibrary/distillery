# Development Workflow for Distillery and cascflow

This guide helps you safely develop Distillery and cascflow together, avoiding version mismatches and common pitfalls.

## Typical Use Case: Updating cascflow and Distillery Together

### 1. Make and Test Local Changes to cascflow

- Edit code in your local `cascflow` repo.
- Increment the version number in `cascflow/pyproject.toml` (e.g., bump patch or minor).
- Commit your changes locally.

### 2. Point Distillery to Your Local cascflow for Testing

Run:

```sh
uv pip install -e ../cascflow
uv pip install -e .
```

This ensures Distillery uses your local, editable cascflow.

Resulting `pyproject.toml` snippet:

```toml
[tool.uv.sources]
cascflow = { path = "../cascflow", editable = true }
```

### 3. Run Distillery Tests

- Run all Distillery tests to verify compatibility:

  ```sh
  uv run pytest
  ```

### 4. Update Distillery's cascflow Dependency Version

To update to a specific released version from PyPI:

```sh
uv pip install 'cascflow==<new_version>'
```

Resulting `pyproject.toml` snippet:

```toml
dependencies = [
    "cascflow==<new_version>",
    ...
]
```

To switch `[tool.uv.sources]` back to the GitHub repo or a pinned release for CI/production:

```sh
uv pip install 'git+https://github.com/caltechlibrary/cascflow@v<new_version>'
```

Resulting `pyproject.toml` snippet:

```toml
[tool.uv.sources]
cascflow = { git = "https://github.com/caltechlibrary/cascflow", rev = "v<new_version>" }
```

After changing dependencies or sources, always run:

```sh
uv pip sync
```

### 5. Push and Release

- Push your changes to the `cascflow` repo and tag the new release.
- Push your changes to the Distillery repo.
- CI and production will now use the correct, released version.

## Common Gotchas and Tips

- **Always bump the version in cascflow before using it in Distillery.**
- **Always update the version in Distillery's `pyproject.toml` to match.**
- **Always run `uv pip sync` or `uv pip install -e .` after changing dependencies.**
- **Never leave `[tool.uv.sources]` pointing to a local path when pushing to main or deploying.**
- **If you see unexpected behavior, check which cascflow version is installed:**

  ```sh
  uv pip show cascflow
  ```

## Quick Reference: Switching cascflow Source

**Local development:**

  Run:

  ```sh
  uv pip install -e ../cascflow
  ```

  Resulting `pyproject.toml` snippet:

  ```toml
  [tool.uv.sources]
  cascflow = { path = "../cascflow", editable = true }
  ```

**Production/CI:**

  Run:

  ```sh
  uv pip install 'git+https://github.com/caltechlibrary/cascflow@v<new_version>'
  ```

  Resulting `pyproject.toml` snippet:

  ```toml
  [tool.uv.sources]
  cascflow = { git = "https://github.com/caltechlibrary/cascflow", rev = "v<new_version>" }
  ```

## See Also

- [uv documentation](https://github.com/astral-sh/uv)
- [PEP 621 - pyproject.toml dependencies](https://peps.python.org/pep-0621/)
- [Python packaging best practices](https://packaging.python.org/)
