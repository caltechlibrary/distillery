# Distillery

Caltech Archives & Special Collections automated system for preparing and sending preservation files to tape and the cloud (S3), preparing and sending access files to a viewing system (Islandora), and recording all the files and locations in ArchivesSpace.

## Architecture

- `web.py` checks user authorization, runs the web form, triggers the status files, and displays status
- `alchemist.py` should be scheduled every minute to check for status files, and upon finding one, initiates processing
- `distillery.py` contains the main script that, depending on the options selected, retrieves metadata, converts images, writes metadata, uploads to S3, and creates ArchivesSpace records
- `tape.py`
- `s3.py`
- `islandora.py` converts metadata and TIFFs to formats for Islandora, uploads and ingests the archival object folders as “book” items, records the proper digital object URLs in ArchivesSpace

## Server Explanation

`web.py` is a [Bottle](https://bottlepy.org/) application and can be run on localhost or a web server such as Apache with mod_wsgi.

We have separated the web interface and the file processing on different servers. `alchemist.py` runs on a different server, checking a shared filesystem for the status files to initiate file processing.

We assume another server for ArchivesSpace and another that has a tape drive attached.

For copying to the cloud we are using and assuming AWS S3. For publishing access copies we are currently using a separate server that runs Islandora 7.

## General Requirements

- ArchivesSpace

## Web Server Requirements

- WSGI support (for example, `mod_wsgi` with Apache)
- User authentication (for example, HTTP Basic authentication or Shibboleth)

## Web Server Setup Steps

1. Clone the [Distillery](https://github.com/caltechlibrary/distillery) repository.
1. Run `pipenv install` within the project directory.
1. Copy the `example-settings.ini` file to `settings.ini` and set appropriate values.
    - ensure the `WEB_NAS_APPS_MOUNTPOINT` and `NAS_STATUS_FILES_RELATIVE_PATH` values are set
1. Copy the `example-users.csv` file to `users.csv` and add authorized users.

## Processing Server Requirements

- common utilities (`cut`, `sha512sum`, `rsync`)
- specialized utilities (`exiftool`, `kdu_compress`, `magick`, `tesseract`)

## Processing Server Setup Steps

1. Clone the [Distillery](https://github.com/caltechlibrary/distillery) repository.
1. Run `pipenv install` within the project directory.
1. Copy the `example-settings.ini` file to `settings.ini` and set appropriate values.
    - most values need to be set
1. Set up a cron job to run `alchemist.py` every minute.
