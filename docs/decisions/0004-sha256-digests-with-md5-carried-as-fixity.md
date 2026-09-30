# 4. SHA-256 as the OCFL digest, MD5 carried in the fixity block

- Status: accepted
- Date: 2026-07-24 (shipped in 2.5.0)

## Context and Problem Statement

OCFL requires a digest algorithm for the inventory's `manifest`. Distillery
previously computed MD5 only, to populate a per-file
`file_version.checksum` in ArchivesSpace and an MD5 metadata tag on the
`s5cmd` upload. Both of those consumers are gone: per-file
`digital_object_component` records were dropped (ADR 0005) and so was the
`s5cmd` transfer. So the question was open again, with one
practical wrinkle: MD5 is still *operationally* useful.

## Decision Outcome

**SHA-256 as the OCFL inventory digest, with MD5 kept as a secondary digest
in the inventory's OCFL `fixity` block.**

SHA-256 is fully OCFL-conformant and fast. MD5 is retained for one specific
purpose: **eyeball-matching against S3's native `ETag`.** For a simple
non-multipart, non-SSE-KMS upload, S3's built-in ETag *is* a plain MD5 hex
digest, so an archivist verifying that the sync tool moved a file correctly can
compare the two values directly without downloading anything.

MD5 lives in the `fixity` block — OCFL's own sanctioned secondary-digest
mechanism, same shape as `manifest` and keyed by algorithm — rather than in a
bespoke field. `chunk_hashes()` computes both digests in a single file-read
pass.

## Considered Options

- **MD5 as the primary digest.** Rejected: weak, and OCFL implementations
  will expect a modern default.
- **SHA-256 only, dropping MD5.** Rejected for the ETag comparison above,
  which has no equivalent with SHA-256 — S3 does not expose one for free on
  objects the sync tool uploads.
- **A custom S3 object tag carrying the MD5.** Unnecessary: the native ETag
  already equals the MD5 for the uploads in question, and Distillery no
  longer performs the upload, so it could not set a tag anyway.
- **A bespoke inventory field for the second digest.** Rejected: `fixity` is
  exactly this, specified.

## Consequences

**The ETag equivalence holds only for single-part, non-SSE-KMS uploads.** A
large file uploaded in parts, or a change to the bucket's default encryption,
silently breaks the visual match — the ETag becomes a digest-of-digests or an
unrelated value. Distillery does not control this: the sync tool performs the
upload, and its multipart threshold and the bucket's encryption settings are
configured elsewhere. Treat a mismatch as "check how it was uploaded" before
treating it as "the file is corrupt."

Every file in a version carries two digests, so the inventory is slightly
larger and each file is read once but hashed twice. Both costs are negligible
against the read itself.

## More Information

- `distillery.py`: `chunk_hashes()`, `OCFL_DIGEST_ALGORITHM`.
- `cascflow.ocfl.add_version()`, which writes the `fixity` block.
