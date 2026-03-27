# Development Workflow for Distillery and cascflow

This guide helps you safely develop Distillery and cascflow together, avoiding version mismatches and common pitfalls.

## Typical Use Case: Updating cascflow and Distillery Together

### 1. Make and Test Local Changes to cascflow

- Edit code in your local `cascflow` repo.
- Increment the version number in `cascflow/pyproject.toml` (e.g., bump patch or minor).
- Commit your changes locally:

  ```sh
  git add .
  git commit -m "Bump version to X.Y.Z and update features"
  ```

### 2. Test Distillery Against Local cascflow

- In Distillery, point to your local editable cascflow:

  ```sh
  uv add --editable ../cascflow
  ```

- Run all Distillery tests to verify compatibility:

  ```sh
  uv run pytest
  ```

### 3. Tag and Publish a cascflow Release

- If all tests pass, create and push a version tag for cascflow (replace X.Y.Z with your version):

  ```sh
  git tag X.Y.Z
  git push origin X.Y.Z
  ```

- (Recommended) Publish a GitHub Release for the tag (requires GitHub CLI):

  ```sh
  gh release create X.Y.Z --title "cascflow X.Y.Z" --notes "Release notes or changelog here."
  ```

  Or, do this via the GitHub web UI: Go to your cascflow GitHub repo → Releases → Draft a new release → Choose the tag you just pushed.

This ensures Distillery can reference a specific, immutable version of cascflow.

- (Optional but recommended) Create a GitHub Release for the tag:
  - Go to your cascflow GitHub repo → Releases → Draft a new release → Choose the tag you just pushed.

This ensures Distillery can reference a specific, immutable version of cascflow.

### 4. Update Distillery's cascflow Dependency Version

- Update Distillery to use the new cascflow release/tag:

  ```sh
  uv add git+https://github.com/caltechlibrary/cascflow@X.Y.Z
  uv sync
  ```

- Resulting `pyproject.toml` snippet:

  ```toml
  [tool.uv.sources]
  cascflow = { git = "https://github.com/caltechlibrary/cascflow", rev = "X.Y.Z" }
  ```

### 5. Push and Release

- Push your changes to the Distillery repo.
- CI and production will now use the correct, released version.

## Common Gotchas and Tips

- **Always bump the version in cascflow before using it in Distillery.**
- **Always update the version in Distillery's `pyproject.toml` to match.**
- **Always run `uv sync` after changing dependencies.**
- **Never leave `[tool.uv.sources]` pointing to a local path when pushing to main or deploying.**
- **If you see unexpected behavior, check which cascflow version is installed:**

  ```sh
  uv pip show cascflow
  ```

## See Also

- [uv documentation](https://github.com/astral-sh/uv)
- [PEP 621 - pyproject.toml dependencies](https://peps.python.org/pep-0621/)
- [Python packaging best practices](https://packaging.python.org/)
- Note: `cascflow` is not available on PyPI; use a local path or the GitHub repository.
