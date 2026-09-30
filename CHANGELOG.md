# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `docs/decisions/`: the OCFL preservation design is now recorded as [ADRs](https://adr.github.io/madr/), following the convention Alchemist adopted; see [ADR 0001](docs/decisions/0001-record-architecture-decisions.md)

### Removed

- `OCFL-PRESERVATION-PLAN.md`: its decisions are carried into ADRs 0002-0006, its build log belongs to git history, and its two genuinely open questions are tracked as issues

### Changed

- rename the `ABSOLUTE_MOUNT_PARENT` setting to `SOURCE_MOUNT_PARENT` (cascflow v5.0.0), which says which mount it is; every `settings.ini` needs the new key
- rename `PRESERVATION_MOUNT_PARENT` to `PRESERVATION_MOUNT_POINT` -- it holds one mounted volume, not the parent of several, unlike `SOURCE_MOUNT_PARENT`
- pin cascflow to v5.0.0, which also brings the `URL-Redirected` relabel of retired legacy `file_version`s and the `OCFL-Object-Root` sort-order entry
- pin arkminter to v1.0.0, which declares its API stable without changing it
- write one `file_version` per OCFL object, pointing at the object root in the vault gateway over HTTPS, so it is clickable for authenticated staff; it carries a new `"OCFL-Object-Root"` `use_statement`, which has to be added to the ArchivesSpace instance's enumeration before these records are accepted
- replace the optional `PRESERVATION_NAS_HOSTNAME` setting with the required `PRESERVATION_VAULT_BASE_URL`

### Removed

- the resolver-bucket `inventory.json` upload and its `file_version` -- the gateway is the web-accessible view of the preservation system, so there is no public copy to maintain (`RESOLVER_BUCKET` remains, used only by ARK minting)
- the `RESOLVER_CLOUDFRONT_DISTRIBUTION_ID` setting and the post-batch CloudFront invalidation, which existed only to evict cached `inventory.json` copies

### Security

- upgrade `urllib3` to 2.8.0, which patches three advisories (two high) affecting the pinned 2.7.0

## [2.5.3] - 2026-09-16

### Removed

- the unused `COMMON_PATH_PREFIX` setting, which only applies to cascflow's `target="metadata"` path that Distillery never takes (it should have gone out with `S3_BUCKET` in 2.5.0)

### Fixed

- record the `_metadata-<component_id>.json` snapshot in the OCFL `fixity` block, not just the `manifest` -- it was the one file in every version written with a sha256 digest but no MD5
- add an `arkminter` logger to the example settings so its ARK warnings and mint errors reach `distillery.log` instead of the console alone

## [2.5.2] - 2026-08-18

### Changed

- drop the digest column from the inventory manifest note's content, leaving one logical path per line -- the digest is already authoritative in `inventory.json` itself

## [2.5.1] - 2026-07-27

### Changed

- `digital_object_id` is now derived from the archival object's ARK instead of `component_id` (cascflow v4.4.0); requires `external_ark_url` to already be set
- reorder `execute`'s loop so the ARK is minted before a new `digital_object` is created, since its id now depends on the ARK

## [2.5.0] - 2026-07-24

### Added

- write preservation content as conformant OCFL objects on local NAS storage (Extension 0007 tuple paths, atomic staging + `os.rename()` writes, `0=ocfl_object_1.1` object declarations)
- add `file_version`s for both the public resolver copy and a NAS-specific copy, sharing an `"OCFL-Object-Inventory"` `use_statement` convention
- invalidate `RESOLVER_CLOUDFRONT_DISTRIBUTION_ID`'s cached `inventory.json` paths once after `execute`'s batch loop (optional setting, matching Alchemist's own opt-in pattern)
- validate the OCFL storage root's conformance (root declaration, `ocfl_layout.json`, Extension 0007 config) on `validate`; auto-initialize it on `execute` if not yet set up
- add `PRESERVATION_MOUNT_PARENT`, `RELATIVE_OCFL_STORAGE_DIRECTORY`, `RELATIVE_OCFL_STAGING_DIRECTORY`, `PRESERVATION_NAS_HOSTNAME`, and `RESOLVER_CLOUDFRONT_DISTRIBUTION_ID` settings

### Changed

- upload each updated object's `inventory.json` directly to `RESOLVER_BUCKET` after writing, replacing the earlier Lambda/S3-Event-Notification design (no separate infrastructure to deploy or maintain)
- surface the actual ineligibility reason instead of the bare `component_id` in validation output
- make `OUR_SHOULDER` optional in settings validation
- pin cascflow to v4.3.3

### Removed

- the now-unused `S3_BUCKET` setting
- dead per-file `digital_object_component` code
- `scripts/post_minimal_archival_objects.py` (moved to an ignored local script)

## [2.4.3] - 2026-07-15

### Changed

- pin cascflow to v4.2.2, which types `config()` with `decouple`'s real overloads instead of an untyped passthrough
- drop now-redundant `str()`/f-string wraps around `config()` call sites

## [2.4.2] - 2026-07-15

### Changed

- log unhandled exceptions through the standard logger (`❌ UNEXPECTED ERROR: ...`) before re-raising, so a crash always leaves a clean error line in the console log instead of jumping straight to a bare traceback

### Fixed

- demote the settings-validation success message to DEBUG so it no longer wins the "first line" used for CI issue titles/comments, restoring the identifier/summary line (or a real error) to that position

## [2.4.1] - 2026-07-15

### Fixed

- log rename step in the validation and execution workflows now runs on both success and failure (it had no `if:` condition, so it was silently skipped on failure -- steps default to running only on success -- and the upload step right after it never found a log to attach)

## [2.4.0] - 2026-07-14

### Added

- settings.ini validation in the `validate` command (checks required variables are set and usable: binary paths, directories, URLs)

### Changed

- pin cascflow to v4.2.0

### Removed

- unused `ARCHIVESSPACE_BASIC_AUTH_*` keys from example-settings.ini

## 2.3.1 - 2026-07-14

### Fixed

- bump arkminter pin to v0.2.1, fixing minted ARK names that occasionally contained a `/` as the check digit

## [2.3.0] - 2026-07-10

### Added

- ARK minting for archival objects during `execute` (mints a stub ARK, redirects to the ArchivesSpace staff page, persists `external_ark_url`)
- pytest CI workflow

### Changed

- use arkminter's shared `get_existing_our_ark` for existing-ARK validation
- pin arkminter and cascflow to tagged releases (v0.2.0, v4.1.1)

## 2.2.0 - 2026-04-01

### Added

- [README](README.md) and [CONFIGURATION](CONFIGURATION.md) content
- this changelog

### Changed

- pin workflow dependencies
- upgrade dependencies

[Unreleased]: https://github.com/caltechlibrary/distillery/compare/v2.5.3...HEAD
[2.5.3]: https://github.com/caltechlibrary/distillery/compare/v2.5.2...v2.5.3
[2.5.2]: https://github.com/caltechlibrary/distillery/compare/v2.5.1...v2.5.2
[2.5.1]: https://github.com/caltechlibrary/distillery/compare/v2.5.0...v2.5.1
[2.5.0]: https://github.com/caltechlibrary/distillery/compare/v2.4.3...v2.5.0
[2.4.3]: https://github.com/caltechlibrary/distillery/compare/v2.4.2...v2.4.3
[2.4.2]: https://github.com/caltechlibrary/distillery/compare/v2.4.1...v2.4.2
[2.4.1]: https://github.com/caltechlibrary/distillery/compare/v2.4.0...v2.4.1
[2.4.0]: https://github.com/caltechlibrary/distillery/compare/v2.3.0...v2.4.0
[2.3.0]: https://github.com/caltechlibrary/distillery/releases/tag/v2.3.0
