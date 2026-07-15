# Distillery Changelog

## 2.4.1

- fix log rename step in the validation and execution workflows to run on both success and failure (it had no `if:` condition, so it was silently skipped on failure -- steps default to running only on success -- and the upload step right after it never found a log to attach)

## 2.4.0

- add settings.ini validation to the `validate` command (checks required variables are set and usable: binary paths, directories, URLs)
- remove unused `ARCHIVESSPACE_BASIC_AUTH_*` keys from example-settings.ini
- pin cascflow to v4.2.0

## 2.3.1

- bump arkminter pin to v0.2.1 (fixes minted ARK names occasionally containing a `/` as the check digit)

## 2.3.0

- add ARK minting for archival objects during `execute` (mints a stub ARK, redirects to the ArchivesSpace staff page, persists `external_ark_url`)
- use arkminter's shared `get_existing_our_ark` for existing-ARK validation
- pin arkminter and cascflow to tagged releases (v0.2.0, v4.1.1)
- add pytest CI workflow

## 2.2.0

- pin workflow dependencies
- add to [README](README.md) and [CONFIGURATION](CONFIGURATION.md) files
- upgrade dependencies
- begin changelog
