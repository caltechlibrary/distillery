# Distillery

Caltech Archives & Special Collections automated system for preparing and sending preservation files to the cloud (S3), preparing and sending access files to a viewing system (Islandora), and recording all the files and locations in ArchivesSpace.

## Architecture

- `distillery.py` checks user authorization, runs the web form, triggers the status files, and displays status
- `alchemist.py` should be scheduled every minute to check for status files, and upon finding one, initiates processing for local copies (default: `tape.py`), cloud copies (default: `s3.py`), and/or access copies (default: `islandora.py`)
- `distill.py` contains shared code that retrieves metadata, converts images, writes metadata, uploads to S3, and creates ArchivesSpace records
- `tape.py`
- `s3.py`
- `islandora.py` converts metadata and TIFFs to formats for Islandora, uploads and ingests the archival object folders as “book” items, records the proper digital object URLs in ArchivesSpace

## Server Setup

`distillery.py` is a [Bottle](https://bottlepy.org/) application and can be run on localhost or a web server such as Apache with mod_wsgi.

We have separated the web interface and the file processing on different servers. `alchemist.py` runs on a different server, checking a shared filesystem for the status files to initiate file processing.

We assume another server for ArchivesSpace and another that has a tape drive attached.

For copying to the cloud we are using and assuming AWS S3. For publishing access copies we are currently using a separate server that runs Islandora 7.

## Configuration

1. Copy the `example-settings.ini` to `settings.ini` and set appropriate values.
1. Copy the `example-users.csv` to `users.csv` and add authorized users.

## TODO & Ideas

- Move files to a structure and location appropriate for copying to LTO tape. [Caltech Library Wiki: Transferring to LTO tape for preservation](https://caltechlibrary.atlassian.net/l/c/yJFLPJtJ)

### order of operations

create structure of preservation files
run destination-specific calculations
send to destination(s)
delete structure
