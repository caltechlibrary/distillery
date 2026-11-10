# Distillery Configuration

Distillery reads its configuration from a `settings.ini` file. Copy
[example-settings.ini](example-settings.ini) and fill in the values for your
environment; every setting is described there.

```bash
cp example-settings.ini settings.ini
```

By default `distillery.py` reads `settings.ini` from its own directory. Pass
`--settings /path/to/settings.ini` to use a file elsewhere:

```bash
uv run distillery.py --settings /path/to/settings.ini validate --context <VOLUME>
```

`settings.ini` holds credentials, so keep it out of version control
(`settings*.ini` is ignored).

## Host requirements

The machine that runs Distillery needs:

- the source volumes mounted under `SOURCE_MOUNT_PARENT`; `--context` names
  one of them, and the source files are in its `RELATIVE_SOURCE_DIRECTORY`
- the preservation volume mounted at `PRESERVATION_MOUNT_POINT`, with room
  for the files being preserved; the OCFL storage and staging directories
  must both be on it, so that versions can be moved into place atomically
- network access to the ArchivesSpace API, to the S3 bucket that ARK minting
  writes to, and to github.com for the `arkminter` and `cascflow`
  dependencies

`execute` creates the OCFL storage root on first use; `validate` reports a
storage root that exists but does not conform.

## ArchivesSpace

Each OCFL object’s `file_version` uses the `use_statement` value
`OCFL-Object-Root`. Add it to the ArchivesSpace instance’s `file_version`
use statement enumeration before running `execute`; the backend rejects
records that use a value missing from the enumeration.

## Logging

The `[loggers]`, `[handlers]`, and `[formatters]` sections configure logging.
The example writes `distillery.log` to the current working directory.
