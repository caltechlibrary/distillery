# Distillery

Distillery is the digital preservation system for Caltech Archives. It takes
digital files described in ArchivesSpace and writes them to preservation
storage as [OCFL](https://ocfl.io/) objects, one per digital object and
identified by its ARK. Each OCFL version carries a snapshot of the archival
object’s ArchivesSpace metadata, and Distillery records the object in
ArchivesSpace with a `file_version` link and a manifest note. Much of how it
works is specific to our decisions and infrastructure; it is not standalone
software. It is published for anyone to look at and adapt for other uses.

## Usage

```bash
uv run distillery.py --help
uv run distillery.py validate --help
uv run distillery.py execute --help
```

`validate` checks the settings, the OCFL storage root, and the source files
on the mounted volume named by `--context`, and reports what `execute` would
preserve. `execute` mints any missing ARKs, writes a new OCFL version for each
archival object, and updates ArchivesSpace.

The design decisions behind the preservation storage are recorded in
[docs/decisions/](docs/decisions/).

## Configuration

See [CONFIGURATION.md](CONFIGURATION.md) for getting started.

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for getting started.

## License

Software produced by the Caltech Library is Copyright © 2026 California
Institute of Technology. This software is freely distributed under a modified
BSD 3-clause license. Please see the [LICENSE](LICENSE) file for more
information.
