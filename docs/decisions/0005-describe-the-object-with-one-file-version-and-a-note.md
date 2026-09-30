# 5. Describe the OCFL object with one `file_version` and a regenerated note

- Status: accepted
- Date: 2026-07-24 (shipped in 2.5.0; `file_version` target revised)

## Context and Problem Statement

Distillery used to create one `digital_object_component` record per
preservation file, each carrying that file's checksum and size. Once
preservation content lives in OCFL objects, every one of those facts is
recorded authoritatively in `inventory.json` — and maintaining a parallel copy
inside ArchivesSpace means two places to disagree.

But ArchivesSpace still has to say *something*, for two different audiences: a
person looking at the staff UI, and a future audit tool reading the API.

## Decision Outcome

**Stop creating per-file `digital_object_component` records.** Put a single
`file_version` on the `digital_object` pointing at the OCFL object root,
plus an **unpublished note carrying a human-readable manifest.**

This matches [DadoCM](https://dadocm.github.io/)'s recommendation that
digital-object description stay separate from file-level fixity and
versioning — that becomes OCFL's job, not ArchivesSpace's.

The two are deliberately not redundant. The note serves a person skimming the
staff UI: a legible label plus a logical-path listing. The `file_version` is
ArchivesSpace's structured, API-queryable field, which is what an
audit/reconciliation tool reads. Both encoding "where does this live" is two
serializations for two consumers, not one fact duplicated for no reason.

### The note

- **Regenerated and overwritten on every update**, never appended. Always
  accurate, simplest to implement, and `inventory.json`'s own version history
  is the durable record if change-over-time is ever wanted.
- ArchivesSpace's real `note_digital_object` schema (`jsonmodel_type`, `type`,
  `label`, `content` as a list of strings, `publish`), with
  `type: "altformavail"` — "Existence and Location of Copies", which is
  precisely what this note documents. It first shipped as a generic `"note"`;
  `altformavail` is the accurate type.
- `label` is the object's on-disk path — mounted volume name, storage
  directory, tuple path, object root directory — joined with the fullwidth
  solidus (`／`) for legibility, followed by `(latest: {head})`. The fullwidth
  character stands in for `/` so the real separators read unambiguously.
- `content` is one logical path per line, **sorted alphabetically** rather
  than iterated from the inventory's `state` dict, which is keyed by digest
  and would order the display by hash value.
- Replacement matches on `type == "altformavail"`, **not on `label`**: the
  label changes on every version bump, so label-matching would accumulate a
  duplicate note per version.

### The `file_version`

`publish: False`, and its `use_statement` names what its `file_uri` targets:
`"OCFL-Object-Root"` for the object root, `"OCFL-Object-Inventory"` for an
`inventory.json` copy (planned for the eventual S3 entry, written by the
not-yet-built sync-audit tool, only after it verifies NAS↔S3 sync).

Ordering and the merge semantics are cascflow's, via
`save_digital_object_file_versions()` — see Alchemist's ADR 0002, which
documents them from the other side, since both pipelines write to the same
`digital_object`s.

## Considered Options

- **Keep per-file components, populated from the inventory.** Rejected: a
  synchronized duplicate of data OCFL already owns.
- **Append a new note per version.** Rejected: unbounded growth, and the
  current state becomes something the reader has to reconstruct.
- **The note alone, or the `file_version` alone.** Rejected: different
  audiences, as above.
- **A second `file_version` pointing at `inventory.json` alongside the object
  root one.** Rejected: derivable by stripping a path segment.

## Consequences

**A custom `use_statement` value must be added to the ArchivesSpace
instance's own enumeration before the backend will accept a record using
it.** Confirmed empirically for `"OCFL-Object-Inventory"`, which was not there
by default; `"OCFL-Object-Root"` needed the same one-time step. This is real
setup work for any new instance, including one restored from a backup that
predates the addition.

ArchivesSpace's backend accepts non-`http(s)` `file_uri` values — verified by
POSTing and deleting real `file://` and `s3://` test records against a live
instance — so the planned `s3://` entry needs no workaround.

The functions that built the old records (`save_digital_object_component_record`,
`construct_digital_object_component`, `create_digital_object_component`,
`find_digital_object`, and `construct_file_version` once it was orphaned) are
deleted, not retained. Recovering them is git's job.

## More Information

- `distillery.py`: `construct_vault_file_version()`,
  `build_inventory_manifest_note()`, `build_inventory_manifest_note_label()`,
  `update_inventory_manifest_note()`,
  `process_archival_object_preservation_files()`.
- `alchemist/docs/decisions/0002-file-version-conventions-live-in-cascflow.md`.
