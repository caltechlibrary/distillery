#!/bin/bash

# NOTE: this file is intended to be run via cron every minute

# USAGE:
# /bin/bash /path/to/this/script.sh /path/to/status/files/directory

# set the nullglob in case there are no `*-processing` files
shopt -s nullglob

# TODO how to sanitize input for passing as arguments?
# NOTE expecting an absolute path as an argument
for FILE in "$1"/*-processing; do
    collection_id=$(basename "$FILE" | cut -d '-' -f 1)
    python=$(which python3)
    $python "$(dirname "$0")"/distill.py "$collection_id"
done
