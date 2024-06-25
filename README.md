# Distillery

Caltech Archives & Special Collections automated system for preparing and sending preservation files to tape and the cloud, preparing and sending access files to a display system, and recording all the files and locations in ArchivesSpace.

## Architecture

- `web.py` checks user authorization, runs the web form, triggers the status files, and displays status
- `distillery.py` contains the main script that, depending on the options selected, retrieves metadata, converts images, writes metadata, transmits preservation files, and creates ArchivesSpace records
- `tape.py`
- `s3.py`
- `alchemist.py` IN DEVELOPMENT

## Server Explanation

`web.py` is a [Bottle](https://bottlepy.org/) application and can be run on localhost or a web server such as Apache with mod_wsgi.

The WORK server needs a service listening for [RPyC](https://rpyc.readthedocs.io/) connections to trigger the processing. See `distillery-example.service` for a starting point with `systemd`.

We assume another server for ArchivesSpace and another that has a tape drive attached.

For copying to the cloud we are using and assuming AWS S3. For publishing access copies we are developing a IIIF-based static HTML system.

## General Requirements

- ArchivesSpace

## WEB Server Requirements

- WSGI support (for example, `mod_wsgi` with Apache)
- User authentication (for example, HTTP Basic authentication or Shibboleth)

## WEB Server Setup Steps

1. Clone the [Distillery](https://github.com/caltechlibrary/distillery) repository.
1. Run `pipenv install` within the project directory.
1. Copy the `settings.ini-example` file to `settings.ini` and set appropriate values.
1. Copy the `example-users.csv` file to `users.csv` and add authorized users.

## WORK Server Requirements

- common utilities (`cut`, `sha512sum`, `rsync`)
- specialized utilities (`exiftool`, `kdu_compress`, `magick`, `tesseract`)

## WORK Server Setup Steps

1. Clone the [Distillery](https://github.com/caltechlibrary/distillery) repository.
1. Run `pipenv install` within the project directory.
1. Copy the `settings.ini-example` file to `settings.ini` and set appropriate values.
    - most values need to be set

For the preservation and publication components:

1. Copy the `example-distillery.service` file to `/etc/systemd/system/distillery.service` and set appropriate values.
1. Enable the service with `systemctl enable distillery`.
1. Start the service with `systemctl start distillery`.
