# 7. Replace uncompressed video with verified FFV1 before preservation

- Status: accepted
- Date: 2026-11-16 (shipped in 2.7.0)

## Context and Problem Statement

Digitized film and video arrive from vendors as uncompressed masters, most
often 10-bit 4:2:2 v210 in QuickTime, at roughly 1.4 MB per frame. Lossless
FFV1 in Matroska is the format Caltech Archives has chosen for video
preservation: it decodes to exactly the same pictures and, for this material,
takes a third to a half of the space. Because OCFL versions are immutable
(ADR 0002), whatever Distillery writes stays forever, so the choice between
keeping an uncompressed master and replacing it with FFV1 has to be made
before Distillery writes the version, not afterward.

A conversion can lose more than pictures. QuickTime edit lists can hide
stored frames from ffmpeg and drop real ones at segment joins; timecode and
other data tracks have no place in MKV; some audio can’t be stored as FLAC.
Whatever replaces a master has to be proven complete first.

## Decision Outcome

**Distillery transcodes uncompressed video to FFV1 with
[Athanor](https://github.com/caltechlibrary/athanor) during `execute`, and
preserves the verified MKV and its `TRANSCODE.md` log in place of the
master.**

- `validate` asks Athanor to assess every video in the batch from its
  header. The validation comment lists each file that will be transcoded and
  each lossy video, such as an H.264 access copy, that will be preserved as
  it is. A file with anything that would be lost stops validation for
  review.
- `execute` transcodes each uncompressed master before changing anything in
  ArchivesSpace. Athanor checks the master against its saved `.md5` file,
  keeps every stored frame, and verifies the MKV by stream hashes, frame
  count, and significant video properties. The OCFL version holds the MKV
  and its log; the master and its `.md5` file are left out, and both
  checksums are recorded in the log.
- Any failure stops the batch, as other Distillery errors do. The item stays
  in `STAGE_2_WORKING` and nothing is written for it.
- The master itself is not deleted: it moves with the rest of the item to
  `STAGE_3_COMPLETE`, where it stays until someone removes it.

Athanor is a separate library, pinned at a release tag like arkminter, so it
can also be run by hand on files that never pass through Distillery.

## Considered Options

- **Preserve both the master and the FFV1 copy.** Rejected: it stores more
  than the master alone, defeating the purpose, and the master adds nothing
  once every frame is proven to survive. The master is itself a product of
  digitization; the film or tape is the original.
- **A separate transcoding run before Distillery.** Rejected: archivists
  would start and approve two runs per batch. Transcoding runs at roughly
  10 GB of v210 per two minutes on the runner, so it fits in the
  preservation run.
- **Preserve the master unconverted when a transcode fails.** Rejected: it
  bypasses review and spends permanent storage on a file that should have
  shrunk. A failure is investigated and the batch rerun.

## Consequences

The runner needs ffmpeg and ffprobe with the FFV1 and FLAC encoders, set as
`FFMPEG_PATH` and `FFPROBE_PATH`. Batches without video need neither, but a
batch with any video file can’t be validated without `FFPROBE_PATH`.

The stream hashes in each `TRANSCODE.md` are comparable only between the same
or compatible ffmpeg versions, so the runner keeps one ffmpeg version for a
collection, and an upgrade is a deliberate step.

Where a master leaves field order or color range unspecified, ffmpeg labels
them in the MKV. The log records these labels as ffmpeg’s, and the stream
hashes show the pictures themselves are unchanged.

A batch with large masters keeps the runner busy longer, and the
preservation volume holds the MKV twice while a version is staged: once in
Distillery’s working directory and once in the version being built. Both
are smaller than the master, which the existing space check already
reserves.

## More Information

- `distillery.py`: `assess_videos()`, `check_video_assessments()`,
  `validate_videos_for_preservation()`, and `transcoded_videos()`.
- Athanor’s `assess()` and `transcode()`.
- ADR 0004: the MKV’s MD5 is carried in the inventory’s fixity block like
  any other file’s.
