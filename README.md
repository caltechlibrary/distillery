# Distillery

Caltech Archives & Special Collections automated system for preparing and sending preservation files to the cloud (S3), preparing and sending access files to a viewing system (Islandora), and recording all the files and locations in ArchivesSpace.

## Architecture

- `distillery.py` runs the web form, triggers the status files, and displays status
- `alchemist.py` should be scheduled every minute to check for status files, and upon finding one, initiates processing
- `distill.py` is called from `alchemist.py` when the correct status file exists and processes a directory of archival object folders containing TIFFs: retrieving metadata, converting images, writing metadata, uploading to S3, and creating ArchivesSpace records
- `islandora.py` converts metadata and TIFFs to formats for Islandora, uploads and ingests the archival object folders as “book” items, records the proper URL in ArchivesSpace

## TODO & Ideas

With the addition of the islandora script, we probably need a choice up front in the UI for a collection to be processed for preservation _and/or_ for access.

Currently `distillery.py` writes a file called `*CollectionID*-processing`. We could change this to a filename that indicates what kind of processing needs to happen for the collection. This seems reasonable using `split()`:

```python
>>> print("id-preservation-".split("-"))
['id', 'preservation', '']
>>> print("id-preservation-access".split("-"))
['id', 'preservation', 'access']
>>> print("id--access".split("-"))
['id', '', 'access']
```

If the index is empty, we would skip the appropriate processing.

One thing to look out for would be the expected file locations. Currently the distill script moves files out of the initial NAS directory upon completion, and the islandora script looks for them in a different directory. If the distill script does not run, the files will not get moved. We need to figure out how to move files in all three script combinations.

If we implement a `__main__.py` file, this is where we could do the parsing of the  settings file and the status file name. `alchemist.py` could simply call the application without arguments like:

```bash
python distillery
```

In `__main__.py` we could first check the settings file to know if this is the web server or the processing server (or both?). This depends on how we need to run the web application and if this setup makes sense. Does the ability to just run `python distillery` on any machine and have it do the right thing based on settings make sense? Is this an example of trying to simplify something too much and actually making it more complex? One goal is to have this package be self contained, even though it is designed for parts of it to run on separate machines.

In any case, the second step on the processing server will be to parse the status file name. It should be a straightforward conditional check to decide when `__main__.py` does the file moving. If the distill script is not set to run, we need to move the files as if it had completed so that the islandora script can take over from there. (Initial thoughts about the process are that the distill script must run before the islandora script, and that they cannot be run in parallel. Running them in parallel might be an option if the only obstacle is that files need to be in a known location for each script. We just need to figure out when to ultimately move the files when they are completed.)

NB: Mike probably knows about setting this up to work with plac https://github.com/ialbert/plac/issues/31#issuecomment-572239360
