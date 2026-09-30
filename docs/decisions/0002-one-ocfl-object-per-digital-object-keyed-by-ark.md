# 2. One OCFL object per `digital_object`, identified by its ARK

- Status: accepted
- Date: 2026-07-24 (shipped in 2.5.0)

## Context and Problem Statement

Preservation content needed an identifier scheme and an on-disk layout. Two
questions had to be answered together: what constitutes one OCFL object, and
what path does it live at.

## Decision Outcome

**One OCFL object per archival object's existing `digital_object` record**,
with the OCFL object `id` set to the **ARK Base Compact Name** (e.g.
`ark:99999/b3xq7z`), derived from `archival_object["external_ark_url"]` via
`cascflow.ark_base_compact_name()`.

ARKs are already minted at the archival-object level and a `digital_object`
per archival object already exists, so no new identifier scheme was needed.
The compact form is the ARK's own canonical identifier per the ARK spec; the
full resolvable URL is a *rendering* of it, fine for an ArchivesSpace
`file_version` link (ADR 0005) but not as the inventory's bare `id`. Deriving
it rather than storing a second field keeps one source of truth.

### Path layout: OCFL Extension 0007

[Extension 0007 (N Tuple Omit Prefix Storage
Layout)](https://ocfl.github.io/extensions/0007-n-tuple-omit-prefix-storage-layout.html)
derives the object root from the identifier: strip everything up to and
including the right-most `delimiter`, zero-pad the remainder if shorter than
`tupleSize × numberOfTuples`, divide it into tuples as intermediate path
segments, then append the original unpadded remainder as the object root
directory.

Configured as `delimiter="/"`, `tupleSize=3`, `numberOfTuples=2`,
`zeroPadding=left`, digest `sha256` (ADR 0004), version padding 4.

```text
id:              ark:99999/b3xq7z
prefix removed:  b3xq7z                 (6 chars)
tuples:          b3x / q7z              (fills 2x3 exactly, no padding)
object root:     <storage_root>/b3x/q7z/b3xq7z/
```

**The delimiter must be `/`, not the extension's default `:`.** An ARK Base
Compact Name contains a colon inside `ark:99999`. Stripping at the right-most
`:` leaves `99999/b3xq7z`, which still contains a `/` and corrupts tuple
division. Stripping at the right-most `/` leaves exactly `b3xq7z` — the ARK's
assigned name, the same opaque string `arkminter.get_existing_our_ark()`
already extracts.

`numberOfTuples=2` is narrower fan-out than the extension's own default of 3,
chosen deliberately for this collection's scale.

## Considered Options

- **One OCFL object per file.** Rejected: OCFL versions content *within* an
  object, so per-file objects throw away the mechanism that makes the format
  worth adopting, and would need an identifier scheme that does not exist.
- **Passing the full resolvable ARK URL as the `id`.** Produces
  byte-identical tuples and final segment, since the algorithm only looks
  after the right-most `/`. Rejected on identifier correctness, not layout.
- **A flat layout, or a hash-derived one.** Rejected: Extension 0007 is a
  registered OCFL layout that a future OCFL-aware tool can resolve from the
  storage root's own `ocfl_layout.json` without bespoke knowledge.

## Consequences

`OCFL_TUPLE_SIZE`, `OCFL_NUMBER_OF_TUPLES`, `OCFL_DELIMITER` and especially
`OCFL_VERSION_PADDING` are **architectural constants, not per-environment
settings** — they live in `distillery.py`, not `settings.ini`. Version padding
in particular must never change for an object once its first version exists,
which is why versions are `v0001` rather than `v1`: fixed width keeps
lexicographic listings in numeric order past nine versions, and 9999 versions
is more headroom than any archival object will use.

**Tuple capacity has no slack today.** `numberOfTuples=2 × tupleSize=3` is
exactly 6 characters, matching today's `ARK_BLADE_LENGTH=6` with no shoulder
configured. If `OUR_SHOULDER` is ever set, or `ARK_BLADE_LENGTH` increases,
the remainder exceeds 6 characters: the intermediate tuples then reflect only
its first 6 (less fan-out entropy for those objects) while the final path
segment stays the complete, still-unique remainder. Nothing breaks, but the
fan-out silently stops improving.

## More Information

- `cascflow.ocfl.object_root_path()` implements the derivation, generalized
  over the extension's own parameters rather than hard-coded to ARKs.
- `distillery.py`: `OCFL_*` constants, `ocfl_object_root_path()`,
  `ocfl_storage_root()`, `validate_ocfl_storage_root()`.
