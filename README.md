# Distillery

Caltech Archives & Special Collections automated system for preparing and sending preservation files to the cloud (S3), preparing and sending access files to a viewing system (Islandora), and recording all the files and locations in ArchivesSpace.

## Architecture

- `distillery.py` checks user authorization, runs the web form, triggers the status files, and displays status
- `alchemist.py` should be scheduled every minute to check for status files, and upon finding one, initiates `distill.py`, `islandora.py`, or both
- `distill.py` retrieves metadata, converts images, writes metadata, uploads to S3, and creates ArchivesSpace records
- `islandora.py` converts metadata and TIFFs to formats for Islandora, uploads and ingests the archival object folders as “book” items, records the proper digital object URLs in ArchivesSpace

## TODO & Ideas

- Move files to a structure and location appropriate for copying to LTO tape. [Caltech Library Wiki: Transferring to LTO tape for preservation](https://caltechlibrary.atlassian.net/l/c/yJFLPJtJ)

### order of operations

create structure of preservation files
run destination-specific calculations
send to destination(s)
delete structure
