# Distillery

Caltech Archives & Special Collections automated system for preparing and sending preservation files to tape and the cloud (S3), preparing and sending access files to a viewing system (Islandora), and recording all the files and locations in ArchivesSpace.

## Architecture

- `web.py` checks user authorization, runs the web form, triggers the status files, and displays status
- `distillery.py` contains the main script that, depending on the options selected, retrieves metadata, converts images, writes metadata, uploads to S3, and creates ArchivesSpace records
- `tape.py`
- `s3.py`
- `islandora.py` converts metadata and TIFFs to formats for Islandora, uploads and ingests the archival object folders as “book” items, records the proper digital object URLs in ArchivesSpace

## Server Explanation

`web.py` is a [Bottle](https://bottlepy.org/) application and can be run on localhost or a web server such as Apache with mod_wsgi.

The WORK server needs a service listening for [RPyC](https://rpyc.readthedocs.io/) connections to trigger the processing. See `distillery-example.service` for a starting point with `systemd`.

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
1. Copy the `settings.ini-example` file to `settings.ini` and set appropriate values.
    - ensure the `WEB_NAS_APPS_MOUNTPOINT` and `NAS_STATUS_FILES_RELATIVE_PATH` values are set
1. Copy the `example-users.csv` file to `users.csv` and add authorized users.

## Processing Server Requirements

- common utilities (`cut`, `sha512sum`, `rsync`)
- specialized utilities (`exiftool`, `kdu_compress`, `magick`, `tesseract`)

## Processing Server Setup Steps

1. Clone the [Distillery](https://github.com/caltechlibrary/distillery) repository.
1. Run `pipenv install` within the project directory.
1. Copy the `settings.ini-example` file to `settings.ini` and set appropriate values.
    - most values need to be set

For the preservation and publication components:

1. Copy the `distillery-example.service` file to `/etc/systemd/system/distillery.service` and set appropriate values.
1. Enable the service with `systemctl enable distillery`.
1. Start the service with `systemctl start distillery`.

For the oral histories component:

1. Copy the `example-oralhistories.service` file to `/etc/systemd/system/oralhistories.service` and set appropriate values.
1. Enable the service with `systemctl enable oralhistories`.
1. Start the service with `systemctl start oralhistories`.

## [`oralhistories.py`](caltechlibrary/distillery/blob/main/oralhistories.py)

Starting with an initial transcript of an oral history interview in Microsoft Word (.docx) format, this script will:

- retrieve metadata from ArchivesSpace
- generate a Markdown document from the Word file that includes the ArchivesSpace metadata
- upload the Markdown file to GitHub

A GitHub Actions script runs after the addition of the Markdown file that will:

- generate HTML and PDF documents that incorporate the metadata and the Markdown content
- commit the new generated files to the repository

With the `--publish` parameter, this script will:

- clone the latest versions of the generated HTML files along with any assets
- sync any updates with an AWS S3 bucket to be accessible on the web

### GitHub Actions

The [Generate Files](https://github.com/caltechlibrary/distillery/blob/main/oralhistories/generate.yml) workflow commits HTML and PDF versions of transcripts to the oralhistories repository for any new or modified Markdown files.

The [Regenerate Files](https://github.com/caltechlibrary/distillery/blob/main/oralhistories/regenerate.yml) workflow allows regeneration of HTML and PDF transcripts without corresponding modification of Markdown files. This can be useful if a template changes, for example. The workflow requires a transcript identifier to be entered before it can be run. A repository secret named `TOKEN` consisting of a personal access token for an authorized account must also be set.
