# Distillery Changelog

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
