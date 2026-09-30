# 6. Migrate forward only, with a hand-rolled OCFL writer in cascflow

- Status: accepted
- Date: 2026-07-24 (shipped in 2.5.0)

## Context and Problem Statement

Two questions that both come down to "how much do we take on": whether to
convert the preservation content already in S3 under flat keys with
`digital_object_component` records, and whether to adopt an OCFL library or
write the small part of the spec this pipeline needs.

## Decision Outcome

**Forward-only migration.** Existing preservation files keep their current
flat S3 keys and `digital_object_component` records, untouched. New content
always gets the OCFL treatment.

**No special-casing for legacy objects.** New files get object-level treatment
regardless of what a `digital_object` already carries. Under forward-only
migration this is unlikely to arise; if it does, a `digital_object` ends up
with both legacy components and a new top-level `file_version` —
cosmetically odd, functionally harmless. Least code.

**A small internal OCFL writer inside `cascflow`**, not an external library
such as `ocfl-py`. Distillery only ever needs "create object" and "add a
version", never general-purpose OCFL reading or validation. This matches the
project's existing style: `arkminter` is exactly this kind of small,
purpose-built library.

The writer lives in `cascflow`, not `distillery`, because it is pure
functions over data with no S3 or network I/O — the natural home for
something Alchemist or a future audit tool might also need.

## Considered Options

- **Backfill existing content into OCFL objects.** Rejected here, not
  rejected forever: it is a separate, explicitly-scoped project if ever
  undertaken, and bundling it would have made this change unshippable.
- **Detect legacy components and migrate or hide them.** Rejected: code and
  failure modes in service of a cosmetic outcome in a case that may never
  occur.
- **`ocfl-py` or another library.** Rejected: a dependency whose surface is
  mostly features this pipeline will never call, for a spec subset small
  enough to implement against the spec text directly.

## Consequences

Two shapes of preservation record coexist in ArchivesSpace indefinitely —
legacy per-file components and new object-level `file_version`s. Any future
audit or reporting tool has to handle both, and "how much is in OCFL?" is a
question about a moving boundary rather than a fixed one.

Following the spec ourselves means the spec's own behavior has to be honored
deliberately. OCFL's forward-delta versioning — unchanged files referenced by
digest in a new version's manifest rather than re-copied — falls out of
implementing it correctly rather than being a feature that was built.

Writing the OCFL module in `cascflow` means Distillery's use of it is gated on
a cascflow release and pin bump, not on a local edit. cascflow had no test
infrastructure when this started; a `pytest` suite was added there, following
`arkminter`'s convention, rather than testing library code from Distillery's
suite.

## More Information

- `cascflow.ocfl`: `object_root_path()`, `build_inventory()`,
  `add_version()`, `build_extension_0007_config()`.
- ADR 0002 for the layout the writer implements.
