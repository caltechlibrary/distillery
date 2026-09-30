# 3. Snapshot the archival object's metadata inside each OCFL version

- Status: accepted
- Date: 2026-07-24 (shipped in 2.5.0)

## Context and Problem Statement

An OCFL object holding only preservation masters describes bytes and nothing
else. Recovering what those bytes *are* would mean querying an ArchivesSpace
instance that may not exist, or may have been edited, decades later. The point
of preservation storage is to survive exactly that.

## Decision Outcome

Each new version's content directory also gets a copy of the archival object's
ArchivesSpace metadata, named **`_metadata-{component_id}.json`** (the
archival object's Component Unique Identifier), **enriched with
fully-resolved ancestor metadata** rather than ancestor ref stubs.

This makes the object self-describing: a snapshot of the archival object's
metadata *context* — its own record plus its resolved ancestor chain — sitting
next to the content it describes. Namespacing the filename by `component_id`
rather than using a fixed name keeps it unambiguous which archival object a
stray copy belongs to.

Ancestor enrichment is `cascflow.enrich_ancestors()`, one `GET` per ancestor
resolving `linked_agents` and `subjects` on each, called once in
`cascflow.execute()` after `find_archival_object()`. It is a separate function
rather than part of `get_arrangement()`: that function's job is building the
curated `arrangement` dict, not mutating `archival_object`.

**One `GET` per ancestor is not laziness.** Checked against the
[ArchivesSpace API docs](https://archivesspace.github.io/archivesspace/api/)
and every `resolve[]=` call site in this codebase: there is no nested or
chained resolve syntax, so a single expanded `resolve[]` list on the archival
object fetch cannot pull each ancestor's own linked agents and subjects
inline.

### Version metadata

The OCFL version's `user` is resolved by `resolve_ocfl_user()`: `GITHUB_ACTOR`
when running under GitHub Actions, falling back to identifying the pipeline
itself for CLI runs. `message` is deliberately left unwritten rather than
filled with a generic placeholder — a meaningful per-version commit message is
a larger workflow question — and `cascflow.ocfl.add_version()` omits absent
fields entirely rather than writing empty strings.

## Considered Options

- **A separate collection-level metadata file**, as earlier versions of the
  design had. **Dropped entirely**, not deferred: once ancestor metadata
  (including the collection-level ancestor) resolves inline on the archival
  object itself, a collection-level file duplicates the same context at a
  coarser grain for no benefit. The mechanism that wrote it
  (`get_collection_data`, `get_collection_tree`, `save_collection_datafile`
  and friends) was deleted rather than left unused, since it called key
  builders that went away with the S3 transfer path.
- **A fixed filename** such as `_metadata.json`. Rejected: a copy found
  out of context cannot be traced back to its archival object.
- **No snapshot, relying on ArchivesSpace.** Rejected as above — it defeats
  the purpose of preservation storage.

## Consequences

A source file named exactly `_metadata-{component_id}.json` would silently
collide with the reserved logical path. That is caught at `validate` time by
`validate_no_reserved_filename_conflicts()`, comparing each top-level source
entry against its own exact derived name rather than a blanket pattern —
caught before the OCFL writer runs, not handled inside it.

**Not checked: the same filename reused across versions** in separate
`execute` runs — an unrelated past file sharing a name with a new one. Each
run's source directory is a fresh `iterdir()`, so nothing prevents it, and the
right resolution was judged too ambiguous to settle here. Tracked as an issue.

A same-batch filename collision is structurally impossible and needs no
guard: one archival object's `filepaths` always comes from a single directory
listing, and a filesystem cannot hold two entries with the same name in one
directory. An earlier crockford-id disambiguation suffix guarded against this
non-risk and was removed.

If the archival object's metadata has not changed since the last version, its
digest is identical and OCFL references the existing copy rather than
duplicating it — the same forward-delta behavior as any other unchanged file,
which falls out of following the spec rather than being built.

## More Information

- `distillery.py`: `build_version_files()`, `resolve_ocfl_user()`,
  `validate_no_reserved_filename_conflicts()`.
- `cascflow`: `enrich_ancestors()`, `ocfl.add_version()`.
