# CALTECH ARCHIVES AND SPECIAL COLLECTIONS
# digital object preservation workflow

# processing functionality; see web.py for bottlepy web application

import hashlib
import importlib
import json
import logging
import logging.config
import logging.handlers
import mimetypes
import os
import random
import shutil
import string
import tempfile
import time

from pathlib import Path

import rpyc
import sh

from asnake.client import ASnakeClient
from decouple import config
from jpylyzer import jpylyzer
from requests import HTTPError

import statuslogger

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.ini"),
    disable_existing_loggers=False,  # log messages from sh will come through
)
logger = logging.getLogger("distillery")
archivesspace_logger = logging.getLogger("archivesspace")
status_logger = logging.getLogger("status")
status_logger.setLevel(logging.INFO)
status_logfile = Path(config("WORK_STATUS_FILES")).joinpath("status.log")
status_handler = logging.FileHandler(status_logfile)
status_handler.setLevel(logging.INFO)
status_handler.setFormatter(statuslogger.StatusFormatter("%(message)s"))
# HACK prevent duplicate handlers with circular imports
if len(status_logger.handlers) == 0:
    status_logger.addHandler(status_handler)

# TODO do we need a class? https://stackoverflow.com/a/16502408/4100024
# we have 8 functions that need an authorized connection to ArchivesSpace
asnake_client = ASnakeClient(
    baseurl=config("ASPACE_API_URL"),
    username=config("ASPACE_USERNAME"),
    password=config("ASPACE_PASSWORD"),
)
asnake_client.authorize()


@rpyc.service
class DistilleryService(rpyc.Service):
    def _initiate_variables(self, destinations):
        self.destinations = destinations
        self.onsite_medium = None
        self.cloud_platform = None
        self.access_platform = None
        self.variables = {}

    def _import_modules(self):
        # NOTE self.destinations is a JSON string
        logger.debug(f"🐞 self.destinations: {self.destinations}")
        if "onsite" in self.destinations and config("ONSITE_MEDIUM"):
            # import ONSITE_MEDIUM module
            try:
                self.onsite_medium = importlib.import_module(config("ONSITE_MEDIUM"))
                logger.debug(f"🐞 self.onsite_medium: {self.onsite_medium}")
            except Exception:
                message = f'❌ UNABLE TO IMPORT MODULE: {config("ONSITE_MEDIUM")}'
                status_logger.error(message)
                raise
        if "cloud" in self.destinations and config("CLOUD_PLATFORM"):
            # import CLOUD_PLATFORM module
            try:
                self.cloud_platform = importlib.import_module(config("CLOUD_PLATFORM"))
                logger.debug(f"🐞 self.cloud_platform: {self.cloud_platform}")
            except Exception:
                message = f'❌ UNABLE TO IMPORT MODULE: {config("CLOUD_PLATFORM")}'
                status_logger.error(message)
                raise
        if "access" in self.destinations and config("ACCESS_PLATFORM"):
            # import ACCESS_PLATFORM module
            try:
                self.access_platform = importlib.import_module(
                    config("ACCESS_PLATFORM")
                )
                logger.debug(f"🐞 self.access_platform: {self.access_platform}")
                logger.debug(
                    f"🐞 bool(self.access_platform): {bool(self.access_platform)}"
                )
            except Exception:
                message = f'❌ UNABLE TO IMPORT MODULE: {config("ACCESS_PLATFORM")}'
                status_logger.error(message)
                raise

    @rpyc.exposed
    def validate(self, destinations):
        """Validate connections, files, and data."""

        # reset status_logfile
        with open(status_logfile, "w") as f:
            pass

        self._initiate_variables(destinations)

        status_logger.info(f"🟢 BEGIN VALIDATING COMPONENTS")

        self._import_modules()

        if self.onsite_medium:
            # validate ONSITE_MEDIUM connection
            if self.onsite_medium.validate_connection():
                message = f'☑️  CONNECTION SUCCESS: {config("ONSITE_MEDIUM")}'
                status_logger.info(message)
            else:
                message = f'❌ CONNECTION FAILURE: {config("ONSITE_MEDIUM")}'
                status_logger.error(message)
                raise ConnectionError(message)
            if not config("TAPE_CONTAINER_PROFILE_URI", default=""):
                message = "❌ MISSING TAPE_CONTAINER_PROFILE_URI SETTING"
                status_logger.error(message)
                raise ValueError(message)
            elif not archivessnake_get(config("TAPE_CONTAINER_PROFILE_URI")).ok:
                message = f'❌ INVALID TAPE_CONTAINER_PROFILE_URI SETTING: {config("TAPE_CONTAINER_PROFILE_URI")}'
                status_logger.error(message)
                raise ValueError(message)
        if self.cloud_platform:
            # validate CLOUD_PLATFORM connection
            if self.cloud_platform.validate_connection():
                message = f'☑️  CONNECTION SUCCESS: {config("CLOUD_PLATFORM")}'
                status_logger.info(message)
            else:
                message = f'❌ CONNECTION FAILURE: {config("CLOUD_PLATFORM")}'
                status_logger.error(message)
                raise ConnectionError(message)
        if self.access_platform:
            # validate ACCESS_PLATFORM connection
            if self.access_platform.validate_connection():
                message = f'☑️  CONNECTION SUCCESS: {config("ACCESS_PLATFORM")}'
                status_logger.info(message)
            else:
                message = f'❌ CONNECTION FAILURE: {config("ACCESS_PLATFORM")}'
                status_logger.error(message)
                raise ConnectionError(message)

        # validate WORK_PRESERVATION_FILES directory
        if self.onsite_medium or self.cloud_platform:
            try:
                Path(config("WORK_PRESERVATION_FILES")).resolve(strict=True)
            except:
                message = f'❌ INVALID WORK_PRESERVATION_FILES DIRECTORY: {config("WORK_PRESERVATION_FILES")}'
                status_logger.error(message)
                logger.exception("‼️")
                raise

        initial_original_directorycount = 0
        initial_original_filecount = 0
        for dirpath, dirnames, filenames in os.walk(config("INITIAL_ORIGINAL_FILES")):
            logger.debug(
                f"🐞 dirpath: {dirpath}; dirnames: {dirnames}; filenames: {filenames}"
            )
            for dirname in dirnames:
                # check archival_object status
                # TODO raise exception after looping through all directories
                #   in order to report all problems instead of just one
                archival_object = find_archival_object(dirname)
                if not archival_object:
                    message = f"❌ NO ARCHIVAL OBJECT FOUND FOR: {dirname}"
                    status_logger.error(message)
                    raise RuntimeError(message)
                elif not archival_object["publish"] and self.access_platform:
                    message = f'❌ ARCHIVAL OBJECT NOT PUBLISHED: [**{archival_object["title"]}**]({config("ASPACE_STAFF_URL")}/resolve/readonly?uri={archival_object["uri"]})'
                    status_logger.error(message)
                    raise ValueError(message)
                elif (
                    archival_object["has_unpublished_ancestor"] and self.access_platform
                ):
                    message = f'❌ ARCHIVAL OBJECT HAS UNPUBLISHED ANCESTOR: [**{archival_object["title"]}**]({config("ASPACE_STAFF_URL")}/resolve/readonly?uri={archival_object["uri"]})'
                    status_logger.error(message)
                    raise ValueError(message)
                # raise for existing digital_object["file_versions"]
                elif bool(archival_object.get("instances")) and self.access_platform:
                    for instance in archival_object["instances"]:
                        if "digital_object" not in instance.keys():
                            continue
                        # NOTE self.destinations is a JSON string
                        if (
                            bool(
                                instance["digital_object"]["_resolved"].get(
                                    "file_versions"
                                )
                            )
                            and "fail" in self.destinations
                        ):
                            message = "❌ DIGITAL OBJECT ALREADY HAS FILE VERSIONS: [**{}**]({}/resolve/readonly?uri={})".format(
                                instance["digital_object"]["_resolved"]["title"],
                                config("ASPACE_STAFF_URL"),
                                instance["digital_object"]["ref"],
                            )
                            status_logger.error(message)
                            raise ValueError(message)
                        elif (
                            bool(
                                instance["digital_object"]["_resolved"].get(
                                    "file_versions"
                                )
                            )
                            and "overwrite" in self.destinations
                        ):
                            message = "⚠️  DIGITAL OBJECT FILE VERSIONS WILL BE OVERWRITTEN: [**{}**]({}/resolve/readonly?uri={})".format(
                                instance["digital_object"]["_resolved"]["title"],
                                config("ASPACE_STAFF_URL"),
                                instance["digital_object"]["ref"],
                            )
                            status_logger.warning(message)
                        elif (
                            bool(
                                instance["digital_object"]["_resolved"].get(
                                    "file_versions"
                                )
                            )
                            and "unpublish" in self.destinations
                        ):
                            message = "⚠️  DIGITAL OBJECT FILE VERSIONS WILL BE UNPUBLISHED: [**{}**]({}/resolve/readonly?uri={})".format(
                                instance["digital_object"]["_resolved"]["title"],
                                config("ASPACE_STAFF_URL"),
                                instance["digital_object"]["ref"],
                            )
                            status_logger.warning(message)
                # count and list initial directories
                initial_original_directorycount += 1
                status_logger.info(f"📁 {dirname}")
            for filename in filenames:
                # count files
                if filename not in [".DS_Store", "Thumbs.db"]:
                    initial_original_filecount += 1
        if not initial_original_directorycount:
            message = "❌ NO DIRECTORIES FOUND"
            status_logger.error(message)
            raise FileNotFoundError(message)
        if initial_original_filecount:
            status_logger.info(f"📄 FILE COUNT: {initial_original_filecount}")
        else:
            message = "❌ NO FILES FOUND"
            status_logger.error(message)
            raise FileNotFoundError(message)

        # send the character that stops javascript reloading in the web ui
        status_logger.info(f"🈺")  # Japanese “Open for Business” Button
        # copy the status_logfile to the logs directory
        logfile_dst = Path(config("WORK_LOG_FILES")).joinpath(
            f"{str(int(time.time()))}.validate.log"
        )
        shutil.copy2(status_logfile, logfile_dst)
        logger.info(f"☑️  COPIED VALIDATE LOG FILE: {logfile_dst}")

    @rpyc.exposed
    def run(self, destinations):
        """Run Distillery."""
        # reset status_logfile
        with open(status_logfile, "w") as f:
            pass
        try:
            self._initiate_variables(destinations)
            status_logger.info(f"🟢 BEGIN DISTILLING")
            self._import_modules()
            status_logger.info(
                f'☑️  DESTINATIONS: {", ".join(list(json.loads(self.destinations)))}'
            )
            onsiteDistiller = None
            cloudDistiller = None
            accessDistiller = None
            if "onsite" in self.destinations or "cloud" in self.destinations:
                either_preservation_destination = True
            else:
                either_preservation_destination = False

            for dir_entry in sorted(
                os.scandir(config("INITIAL_ORIGINAL_FILES")),
                key=lambda dir_entry: dir_entry.name,
            ):
                if dir_entry.is_file():
                    continue

                # Get archival_object data via component_id from directory name.
                self.variables["archival_object"] = find_archival_object(dir_entry.name)
                self.variables["arrangement"] = get_arrangement(
                    self.variables["archival_object"]
                )

                if either_preservation_destination:
                    # retrieve collection data from ArchivesSpace
                    collection_data = get_collection_data(
                        self.variables["arrangement"]["collection_id"]
                    )
                    status_logger.info(
                        f'☑️  ARCHIVESSPACE COLLECTION DATA RETRIEVED: [**{collection_data["title"]}**]({config("ASPACE_STAFF_URL")}/resolve/readonly?uri={collection_data["uri"]})'
                    )
                    # save collection metadata
                    save_collection_datafile(
                        collection_data, config("WORK_PRESERVATION_FILES")
                    )
                    # run collection-level preprocessing
                    if self.onsite_medium:
                        self.onsite_medium.collection_level_preprocessing(
                            self.variables["arrangement"]["collection_id"],
                            config("WORK_PRESERVATION_FILES"),
                        )
                        onsiteDistiller = True
                    if self.cloud_platform:
                        self.cloud_platform.collection_level_preprocessing(
                            self.variables["arrangement"]["collection_id"],
                            config("WORK_PRESERVATION_FILES"),
                        )
                        cloudDistiller = True

                # for publication destinations
                accessDistiller = None
                if self.access_platform:
                    self.variables["file_versions_op"] = json.loads(self.destinations)[
                        "access"
                    ]["file_versions_op"]
                    self.variables["thumbnail_label"] = json.loads(self.destinations)[
                        "access"
                    ]["thumbnail_label"]
                    accessDistiller = self.access_platform.AccessPlatform()
                    accessDistiller.collection_structure_processing()

                initial_archival_object_directory = dir_entry.path
                working_archival_object_directory = str(
                    os.path.join(config("WORKING_ORIGINAL_FILES"), dir_entry.name)
                )
                try:
                    shutil.move(
                        initial_archival_object_directory,
                        working_archival_object_directory,
                    )
                except BaseException:
                    message = "❌ UNABLE TO MOVE THE SOURCE FILES FOR PROCESSING"
                    status_logger.error(message)
                    logger.exception(f"‼️")
                    raise

                # Set up list of file paths for the current directory.
                self.variables["filepaths"] = [
                    f.path
                    for f in os.scandir(working_archival_object_directory)
                    if f.is_file() and f.name not in [".DS_Store", "Thumbs.db"]
                ]

                if either_preservation_destination:
                    archival_object_datafile_key = save_archival_object_datafile(
                        self.variables["arrangement"],
                        self.variables["archival_object"],
                        config("WORK_PRESERVATION_FILES"),
                    )
                    status_logger.info(
                        f"☑️  ARCHIVAL OBJECT DATA FILE CREATED: {archival_object_datafile_key}"
                    )

                if accessDistiller:
                    accessDistiller.archival_object_level_processing(self.variables)

                create_derivative_files(
                    self.variables,
                    onsite=onsiteDistiller,
                    cloud=cloudDistiller,
                    access=accessDistiller,
                )

                if accessDistiller:
                    # NOTE working on variables["archival_object"]["component_id"]
                    accessDistiller.transfer_archival_object_derivative_files(
                        self.variables
                    )
                    status_logger.info(
                        "☑️  ACCESS PAGE CREATED: [**{}**]({}/{}/{}/index.html)".format(
                            self.variables["archival_object"]["component_id"],
                            config("ACCESS_SITE_BASE_URL").rstrip("/"),
                            self.variables["arrangement"]["collection_id"],
                            self.variables["archival_object"]["component_id"],
                        )
                    )

                    # NOTE this is where we create_digital_object_file_versions()
                    accessDistiller.loop_over_derivative_structure(self.variables)

                if either_preservation_destination:
                    # Confirm existing or create digital_object with component_id.
                    # NOTE digital_object needs to exist for digital_object_component records to be attached
                    try:
                        digital_object_count = len(
                            [
                                i
                                for i in self.variables["archival_object"]["instances"]
                                if "digital_object" in i.keys()
                            ]
                        )
                        logger.debug(f"🐞 DIGITAL OBJECT COUNT: {digital_object_count}")
                        if digital_object_count > 1:
                            raise ValueError(
                                "❌ MULTIPLE DIGITAL OBJECTS FOUND: {}".format(
                                    self.variables["archival_object"]["component_id"]
                                )
                            )
                        elif digital_object_count < 1:
                            # returns new archival_object with digital_object instance included
                            (
                                digital_object_uri,
                                self.variables["archival_object"],
                            ) = create_digital_object(self.variables["archival_object"])
                    except:
                        logger.exception("‼️")
                        raise

                    self.variables["current_archival_object_datafile"] = (
                        Path(config("WORK_PRESERVATION_FILES"))
                        .joinpath(archival_object_datafile_key)
                        .resolve()
                    )
                    logger.debug(
                        f'🐞 ARCHIVAL OBJECT DATAFILE: {self.variables["current_archival_object_datafile"]}'
                    )

                    if self.onsite_medium:
                        # tape_top_container_uri added to self.variables
                        self.variables = self.onsite_medium.transfer_archival_object_derivative_files(
                            self.variables
                        )
                        # NOTE writes top_container records to ArchivesSpace
                        self.onsite_medium.process_archival_object_datafile(
                            self.variables
                        )
                    if self.cloud_platform:
                        self.cloud_platform.process_archival_object_datafile(
                            self.variables
                        )

                    # see https://stackoverflow.com/a/54790514 for os.walk explainer
                    for dirpath, dirnames, filenames in sorted(
                        os.walk(
                            Path(config("WORK_PRESERVATION_FILES"))
                            .joinpath(
                                get_archival_object_directory_prefix(
                                    self.variables["arrangement"],
                                    self.variables["archival_object"],
                                )
                            )
                            .resolve()
                        )
                    ):
                        for filename in filenames:
                            filepath = Path(dirpath).joinpath(filename)
                            if Path(filename).suffix == ".json" and Path(
                                dirpath
                            ).name.startswith(Path(filename).stem):
                                # skip archival_object JSON metadata
                                continue
                            logger.info(
                                f"▶️  GETTING PRESERVATION FILE INFO: {filepath}"
                            )
                            type, encoding = mimetypes.guess_type(filepath)
                            if type == "image/jp2":
                                self.variables[
                                    "preservation_file_info"
                                ] = get_preservation_image_data(filepath)
                                self.variables["preservation_file_info"][
                                    "mimetype"
                                ] = type
                            else:
                                self.variables["preservation_file_info"] = {}
                                self.variables["preservation_file_info"][
                                    "filepath"
                                ] = filepath
                                self.variables["preservation_file_info"][
                                    "filesize"
                                ] = filepath.stat().st_size
                                with open(filepath, "rb") as fb:
                                    self.variables["preservation_file_info"][
                                        "md5"
                                    ] = hashlib.md5(fb.read())
                                self.variables["preservation_file_info"][
                                    "mimetype"
                                ] = type
                            if self.onsite_medium:
                                self.onsite_medium.process_digital_object_component_file(
                                    self.variables
                                )
                            if self.cloud_platform:
                                self.cloud_platform.process_digital_object_component_file(
                                    self.variables
                                )

                    # move preservation files on each iteration because the
                    # rsync transfer to tape copies the entire contents of the
                    # directory; use a temporary directory for deletion because
                    # deleting across slow file systems can result in cruft
                    # still existing on the next iteration
                    stillage_tmp = tempfile.TemporaryDirectory()
                    logger.debug(f"🐞 STILLAGE_TMP: {stillage_tmp.name}")
                    shutil.move(
                        Path(config("WORK_PRESERVATION_FILES"))
                        .joinpath(self.variables["arrangement"]["collection_id"])
                        .as_posix(),
                        Path(stillage_tmp.name).resolve().as_posix(),
                    )
                    os.system(
                        "/bin/rm -rf {}".format(
                            Path(stillage_tmp.name)
                            .joinpath(self.variables["arrangement"]["collection_id"])
                            .resolve()
                            .as_posix()
                        )
                    )

        except Exception as e:
            status_logger.error("❌ SOMETHING WENT WRONG")
            status_logger.error(e)
            logger.exception("‼️")
            raise
        # complete the process if there is no error
        else:
            # send the character that stops javascript reloading in the web ui
            status_logger.info(f"🏁")
            # copy the status_logfile to the logs directory
            logfile_dst = Path(config("WORK_LOG_FILES")).joinpath(
                f"{str(int(time.time()))}.run.log"
            )
            shutil.copy2(status_logfile, logfile_dst)
            logger.info(f"☑️  COPIED RUN LOG FILE: {logfile_dst}")
            # TODO delete PRESERVATION_FILES/CollectionID directory

    @rpyc.exposed
    def alchemist_regenerate(self, component_id="", logfile=""):
        if component_id:
            status_logger = logging.getLogger(component_id)
        else:
            status_logger = logging.getLogger("_")
        status_logger.setLevel(logging.INFO)
        status_handler = logging.FileHandler(logfile)
        status_handler.setLevel(logging.INFO)
        status_handler.setFormatter(statuslogger.StatusFormatter("%(message)s"))
        status_logger.addHandler(status_handler)

        # import ACCESS_PLATFORM module
        try:
            self.access_platform = importlib.import_module(config("ACCESS_PLATFORM"))
        except Exception:
            message = f'❌ UNABLE TO IMPORT MODULE: {config("ACCESS_PLATFORM")}'
            status_logger.exception(message)
            raise

        try:
            accessDistiller = self.access_platform.AccessPlatform()
        except Exception:
            message = "❌ PROBLEM WITH ACCESSPLATFORM CLASS"
            status_logger.exception(message)
            raise

        variables = {"alchemist_regenerate": True}

        try:
            if component_id:
                # regenerate files for one item
                status_logger.info(f"🟢 BEGIN REGENERATING: {component_id}")
                variables["archival_object"] = find_archival_object(component_id)
                variables["arrangement"] = get_arrangement(variables["archival_object"])
                accessDistiller.archival_object_level_processing(variables)
                accessDistiller.transfer_archival_object_derivative_files(variables)
                status_logger.info(
                    "☑️  ALCHEMIST FILES REGENERATED: [**{}**]({}/{}/{})".format(
                        variables["archival_object"]["component_id"],
                        config("ACCESS_SITE_BASE_URL").rstrip("/"),
                        variables["arrangement"]["collection_id"],
                        variables["archival_object"]["component_id"],
                    )
                )
            else:
                # TODO DRY this out
                # regenerate files for all items
                status_logger.info("🟢 BEGIN REGENERATING ALL")
                archival_object_prefixes = accessDistiller.regenerate_all(variables)
                for archival_object_prefix in archival_object_prefixes:
                    component_id = archival_object_prefix.split("/")[-2]
                    variables["archival_object"] = find_archival_object(component_id)
                    variables["arrangement"] = get_arrangement(
                        variables["archival_object"]
                    )
                    accessDistiller.archival_object_level_processing(variables)
                    accessDistiller.transfer_archival_object_derivative_files(variables)
                    status_logger.info(
                        "☑️  ALCHEMIST FILES REGENERATED: [**{}**]({}/{}/{})".format(
                            variables["archival_object"]["component_id"],
                            config("ACCESS_SITE_BASE_URL").rstrip("/"),
                            variables["arrangement"]["collection_id"],
                            variables["archival_object"]["component_id"],
                        )
                    )
        except Exception as e:
            status_logger.error("❌ SOMETHING WENT WRONG")
            status_logger.error(e)
            logger.exception("‼️")
            raise
        # complete the process if there is no error
        else:
            # send the character that stops javascript reloading in the web ui
            status_logger.info(f"🏁")
            # copy the status_logfile to the logs directory
            logfile_dst = Path(config("WORK_LOG_FILES")).joinpath(
                f"{self.collection_id}.{str(int(time.time()))}.alchemist_regenerate.log"
            )
            shutil.copy2(status_logfile, logfile_dst)
            logger.info(f"☑️  COPIED ALCHEMIST_REGENERATE LOG FILE: {logfile_dst}")


def confirm_collection_directory(collection_id, parent_directory):
    # make a list of directory names to check against
    entries = []
    for entry in os.scandir(parent_directory):
        if entry.is_dir:
            entries.append(entry.name)
    # check that collection_id case matches directory name
    if collection_id in entries:
        message = f"☑️  COLLECTION DIRECTORY FOUND: {collection_id}"
        logger.info(message)
        return os.path.join(parent_directory, collection_id)
    else:
        raise NotADirectoryError(os.path.join(parent_directory, collection_id))


def get_collection_data(collection_id):
    # raises an HTTPError exception if unsuccessful
    collection_uri = get_collection_uri(collection_id)
    collection_data = archivessnake_get(collection_uri).json()
    if not collection_identifiers_match(collection_id, collection_data):
        message = f"❌ The Collection ID from the form, {collection_id}, must exactly match the identifier in ArchivesSpace, {collection_data['id_0']}, including case-sensitively.\n"
        raise ValueError(message)
    if collection_data:
        collection_data["tree"]["_resolved"] = get_collection_tree(collection_uri)
        if collection_data["tree"]["_resolved"]:
            logger.info(
                f'☑️  ARCHIVESSPACE COLLECTION DATA RETRIEVED: {collection_data["uri"]}'
            )
            return collection_data
    else:
        raise RuntimeError(
            f"There was a problem retrieving the collection data from ArchivesSpace.\n"
        )


def get_collection_uri(collection_id):
    # raises an HTTPError exception if unsuccessful
    search_results_json = archivessnake_get(
        f'/repositories/2/find_by_id/resources?identifier[]=["{collection_id}"]'
    ).json()
    if len(search_results_json["resources"]) < 1:
        raise ValueError(
            f"No collection found in ArchivesSpace with the ID: {collection_id}\n"
        )
    return search_results_json["resources"][0]["ref"]


def archivessnake_get(uri):
    response = asnake_client.get(uri)
    response.raise_for_status()
    return response


def collection_identifiers_match(collection_id, collection_data):
    if collection_id != collection_data["id_0"]:
        return False
    return True


def get_collection_tree(collection_uri):
    # raises an HTTPError exception if unsuccessful
    collection_tree = archivessnake_get(collection_uri + "/ordered_records").json()
    if collection_tree:
        return collection_tree
    else:
        raise RuntimeError(
            f"There was a problem retrieving the collection tree from ArchivesSpace.\n"
        )


def save_collection_datafile(collection_data, directory):
    """Save the collection data to a JSON file."""
    collection_datafile_key = os.path.join(
        collection_data["id_0"],
        f"{collection_data['id_0']}.json",
    )
    filename = os.path.join(
        directory,
        collection_datafile_key,
    )
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        f.write(json.dumps(collection_data, indent=4))
    logger.info(f"☑️  COLLECTION DATA FILE SAVED: {filename}")
    return collection_datafile_key


# IDEA rename this to something like "loop_over_archival_object_directories"
def create_derivative_structure(
    variables, collection_directory, collection_data, onsite, cloud, access
):
    """Loop over subdirectories inside ORIGINAL_FILES/CollectionID directory.

    Example:
    ORIGINAL_FILES/CollectionID <-- looping over directories under here
    ├── CollectionID_000_XX
    ├── CollectionID_001_02
    │   ├── CollectionID_001_02_01.tif
    │   ├── CollectionID_001_02_02.tif
    │   ├── CollectionID_001_02_03.tif
    │   └── CollectionID_001_02_04.tif
    └── CollectionID_007_08
    """
    # NOTE [::-1] makes a reverse copy of the list for use with pop() below
    subdirectories = [
        str(s) for s in Path(collection_directory).iterdir() if s.is_dir()
    ][::-1]
    for _ in range(len(subdirectories)):
        # Using pop() (and/or range(len()) above) maybe helps to be sure that if
        # archival object metadata fails to process properly, it and its files
        # are skipped completely and the script moves on to the next directory.
        subdirectory = subdirectories.pop()
        logger.info(f"▶️  PROCESSING DIRECTORY: {subdirectory}")

        # Set up list of file paths for the current directory.
        variables["filepaths"] = [
            f.path
            for f in os.scandir(subdirectory)
            if f.is_file() and f.name not in [".DS_Store", "Thumbs.db"]
        ]

        # Avoid processing directory when there are no files.
        # TODO check for empty directories in validate()
        if not variables["filepaths"]:
            logger.warning(f"⚠️  NO FILES IN DIRECTORY: {subdirectory}")
            continue

        # get archival_object data via component_id from subdirectory name
        variables["archival_object"] = find_archival_object(
            os.path.basename(subdirectory)
        )
        if not variables["archival_object"]:
            logger.warning(f"⚠️  ARCHIVAL OBJECT NOT FOUND: {subdirectory}")
            continue
        variables["arrangement"] = get_arrangement(variables["archival_object"])

        if onsite or cloud:
            archival_object_datafile_key = save_archival_object_datafile(
                variables["arrangement"],
                variables["archival_object"],
                config("WORK_PRESERVATION_FILES"),
            )
            status_logger.info(
                f"☑️  ARCHIVAL OBJECT DATA FILE CREATED: {archival_object_datafile_key}"
            )
            # TODO check for file type and create derivatives or copy originals

        if access:
            access.archival_object_level_processing(variables)

        create_derivative_files(variables, collection_data, onsite, cloud, access)

        if access:
            # NOTE working on variables["archival_object"]["component_id"]
            access.transfer_archival_object_derivative_files(variables)
            status_logger.info(
                "☑️  ACCESS PAGE CREATED: [**{}**]({}/{}/{}/index.html)".format(
                    variables["archival_object"]["component_id"],
                    config("ACCESS_SITE_BASE_URL").rstrip("/"),
                    variables["arrangement"]["collection_id"],
                    variables["archival_object"]["component_id"],
                )
            )


def archivessnake_post(uri, object):
    try:
        response = asnake_client.post(uri, json=object)
        logger.debug(f"🐞 RESPONSE: {response.json()}")
        return response
    except:
        logger.exception("‼️")
        raise


def archivessnake_delete(uri):
    response = asnake_client.delete(uri)
    response.raise_for_status()
    # TODO handle error responses
    return response


def save_digital_object_file_versions(archival_object, file_versions, file_versions_op):
    try:
        for instance in archival_object["instances"]:
            if "digital_object" in instance.keys():
                # ASSUMPTION: only one digital_object exists per archival_object
                # TODO handle multiple digital_objects per archival_object
                existing_file_versions = []
                if file_versions_op == "unpublish" and instance["digital_object"][
                    "_resolved"
                ].get("file_versions"):
                    for file_version in instance["digital_object"]["_resolved"][
                        "file_versions"
                    ]:
                        file_version["publish"] = False
                    existing_file_versions = instance["digital_object"]["_resolved"][
                        "file_versions"
                    ]
                file_versions.extend(existing_file_versions)
                digital_object = instance["digital_object"]["_resolved"]
                digital_object["file_versions"] = file_versions
                digital_object["publish"] = True
                digital_object_post_response = update_digital_object(
                    digital_object["uri"], digital_object
                ).json()
    except:
        logger.exception("‼️")
        raise


def create_digital_object(archival_object):
    digital_object = {}
    digital_object["digital_object_id"] = archival_object["component_id"]  # required
    digital_object["title"] = archival_object["title"]  # required
    # NOTE leaving created digital objects unpublished
    # digital_object['publish'] = True

    digital_object_post_response = archivessnake_post(
        "/repositories/2/digital_objects", digital_object
    )
    # example success response:
    # {
    #     "id": 9189,
    #     "lock_version": 0,
    #     "stale": true,
    #     "status": "Created",
    #     "uri": "/repositories/2/digital_objects/9189",
    #     "warnings": []
    # }
    # example error response:
    # {
    #     "error": {
    #         "digital_object_id": [
    #             "Must be unique"
    #         ]
    #     }
    # }
    # TODO check for existing digital_object_id in validate()
    if "error" in digital_object_post_response.json():
        if "digital_object_id" in digital_object_post_response.json()["error"]:
            if (
                "Must be unique"
                in digital_object_post_response.json()["error"]["digital_object_id"]
            ):
                raise ValueError(
                    f"❌ NON-UNIQUE DIGITAL_OBJECT_ID: {archival_object['component_id']}"
                )
        else:
            raise RuntimeError(
                f"❌ UNEXPECTED ERROR: {digital_object_post_response.json()}"
            )
    else:
        digital_object_uri = digital_object_post_response.json()["uri"]
        logger.info(f"✳️  DIGITAL OBJECT CREATED: {digital_object_uri}")

    # set up a digital object instance to add to the archival object
    digital_object_instance = {
        "instance_type": "digital_object",
        "digital_object": {"ref": digital_object_uri},
    }
    # add digital object instance to archival object
    archival_object["instances"].append(digital_object_instance)
    # post updated archival object
    archival_object_post_response = archivessnake_post(
        archival_object["uri"], archival_object
    )
    logger.info(
        f'☑️  ARCHIVAL OBJECT UPDATED: {archival_object_post_response.json()["uri"]}'
    )

    # TODO investigate how to roll back adding digital object to archival object

    # find_archival_object() again to include digital object instance
    archival_object = find_archival_object(archival_object["component_id"])

    return digital_object_uri, archival_object


def directory_setup(directory):
    if not Path(directory).exists():
        Path(directory).mkdir()
    elif Path(directory).is_file():
        raise FileExistsError(f"a non-directory file exists at: {directory}")
    return Path(directory)


def get_aip_image_data(filepath):
    aip_image_data = {}
    aip_image_data["filepath"] = filepath
    jpylyzer_xml = jpylyzer.checkOneFile(aip_image_data["filepath"])
    aip_image_data["filesize"] = jpylyzer_xml.findtext("./fileInfo/fileSizeInBytes")
    aip_image_data["width"] = jpylyzer_xml.findtext(
        "./properties/jp2HeaderBox/imageHeaderBox/width"
    )
    aip_image_data["height"] = jpylyzer_xml.findtext(
        "./properties/jp2HeaderBox/imageHeaderBox/height"
    )
    aip_image_data["standard"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestreamBox/siz/rsiz"
    )
    aip_image_data["transformation"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestreamBox/cod/transformation"
    )
    aip_image_data["quantization"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestreamBox/qcd/qStyle"
    )
    with open(aip_image_data["filepath"], "rb") as f:
        aip_image_data["md5"] = hashlib.md5(f.read())
    return aip_image_data


def get_crockford_characters(n=4):
    return "".join(random.choices("abcdefghjkmnpqrstvwxyz" + string.digits, k=n))


def get_crockford_id():
    return get_crockford_characters() + "_" + get_crockford_characters()


# TODO rename to get_filepath_components
def get_file_parts(filepath):
    file_parts = {}
    file_parts["filepath"] = filepath
    file_parts["filename"] = file_parts["filepath"].split("/")[-1]
    file_parts["filestem"] = file_parts["filename"].split(".")[0]
    file_parts["extension"] = file_parts["filename"].split(".")[-1]
    file_parts["sequence"] = file_parts["filestem"].split("_")[-1]
    file_parts["crockford_id"] = get_crockford_id()
    return file_parts


def get_arrangement(archival_object):
    """Return a dictionary of the arragement levels for an archival object.

    EXAMPLES:
    arrangement["repository_name"]
    arrangement["repository_code"]
    arrangement["folder_display"]
    arrangement["folder_title"]
    arrangement["collection_title"]
    arrangement["collection_id"]
    arrangement["series_display"]
    arrangement["series_id"]
    arrangement["series_title"]
    arrangement["subseries_display"]
    arrangement["subseries_id"]
    arrangement["subseries_title"]
    """
    try:
        # TODO document assumptions about arrangement
        arrangement = {}
        arrangement["repository_name"] = archival_object["repository"]["_resolved"][
            "name"
        ]
        arrangement["repository_code"] = archival_object["repository"]["_resolved"][
            "repo_code"
        ]
        arrangement["folder_display"] = archival_object["display_string"]
        arrangement["folder_title"] = archival_object.get("title")
        for ancestor in archival_object["ancestors"]:
            if ancestor["level"] == "collection":
                arrangement["collection_title"] = ancestor["_resolved"]["title"]
                arrangement["collection_id"] = ancestor["_resolved"]["id_0"]
            elif ancestor["level"] == "series":
                arrangement["series_display"] = ancestor["_resolved"]["display_string"]
                arrangement["series_id"] = ancestor["_resolved"].get("component_id")
                arrangement["series_title"] = ancestor["_resolved"].get("title")
            elif ancestor["level"] == "subseries":
                arrangement["subseries_display"] = ancestor["_resolved"][
                    "display_string"
                ]
                arrangement["subseries_id"] = ancestor["_resolved"].get("component_id")
                arrangement["subseries_title"] = ancestor["_resolved"].get("title")
        logger.info("☑️  ARRANGEMENT LEVELS AGGREGATED")
        return arrangement
    except:
        logger.exception("‼️")
        raise


def find_archival_object(component_id):
    """Finds an archival object by component_id; Returns dict or None."""
    find_uri = (
        f"/repositories/2/find_by_id/archival_objects?component_id[]={component_id}"
    )
    find_by_id_response = archivessnake_get(find_uri)
    if len(find_by_id_response.json()["archival_objects"]) < 1:
        logger.warning(f"⚠️  ARCHIVAL OBJECT NOT FOUND: {component_id}")
        return None
    elif len(find_by_id_response.json()["archival_objects"]) > 1:
        logger.warning(f"⚠️  MULTIPLE ARCHIVAL OBJECTS FOUND: {component_id}")
        return None
    else:
        try:
            archival_object = archivessnake_get(
                find_by_id_response.json()["archival_objects"][0]["ref"]
                + "?resolve[]=ancestors"
                + "&resolve[]=digital_object"
                + "&resolve[]=linked_agents"
                + "&resolve[]=repository"
                + "&resolve[]=subjects"
                + "&resolve[]=top_container"
            ).json()
            logger.info(f"☑️  ARCHIVAL OBJECT FOUND: {component_id}")
            return archival_object
        except Exception as e:
            logger.exception(e)
            raise


def get_archival_object_datafile_key(prefix, archival_object):
    """Return the key (file path) of an archival object datafile."""
    # NOTE the prefix includes a trailing slash
    return f'{prefix}{archival_object["component_id"]}.json'


def get_archival_object_directory_prefix(arrangement, archival_object):
    """Return the prefix (directory path) of an archival object.

    Non-alphanumeric characters are replaced with hyphens. The prefix includes a
    trailing slash.

    FORMAT: resource:id_0/archival_object:component_id--archival_object:title/
    """
    archival_object_title = "".join(
        [c if c.isalnum() else "-" for c in arrangement["folder_title"]]
    )
    prefix = "/".join(
        [
            arrangement["collection_id"],
            f'{archival_object["component_id"]}--{archival_object_title}',
            "",
        ]
    )
    return prefix


def get_digital_object_component_file_key(prefix, file_parts):
    """Return the key (file path) of a digital object component file."""
    # file_parts: {
    #     "crockford_id": "me5v-z1yp",
    #     "extension": "tiff",
    #     "filename": "HaleGE_02_0B_056_07_0001.tiff",
    #     "filepath": "/path/to/archives/data/WORKING_ORIGINAL_FILES/HaleGE/HaleGE_02_0B_056_07_0001.tiff",
    #     "filestem": "HaleGE_02_0B_056_07_0001",
    #     "sequence": "0001"
    # }
    # NOTE the prefix includes a trailing slash
    return (
        prefix
        + file_parts["filename"]
        + "/"
        + file_parts["crockford_id"]
        + "."
        + file_parts["extension"]
    )


def get_xmp_dc_metadata(arrangement, file_parts, archival_object):
    xmp_dc = {}
    xmp_dc["title"] = (
        arrangement["folder_display"] + " [" + file_parts["sequence"] + "]"
    )
    # TODO(tk) check extent type for pages/images/computer files/etc
    if len(archival_object["extents"]) == 1:
        xmp_dc["title"] = (
            xmp_dc["title"].rstrip("]")
            + "/"
            + archival_object["extents"][0]["number"].zfill(4)
            + "]"
        )
    xmp_dc["identifier"] = file_parts["crockford_id"]
    xmp_dc["publisher"] = arrangement["repository_name"]
    xmp_dc["source"] = (
        arrangement["repository_code"] + ": " + arrangement["collection_title"]
    )
    for instance in archival_object["instances"]:
        if "sub_container" in instance.keys():
            if (
                "series"
                in instance["sub_container"]["top_container"]["_resolved"].keys()
            ):
                xmp_dc["source"] += (
                    " / "
                    + instance["sub_container"]["top_container"]["_resolved"]["series"][
                        0
                    ]["display_string"]
                )
                for ancestor in archival_object["ancestors"]:
                    if ancestor["level"] == "subseries":
                        xmp_dc["source"] += " / " + arrangement["subseries_display"]
    xmp_dc[
        "rights"
    ] = "Caltech Archives has not determined the copyright in this image."
    for ancestor in archival_object["ancestors"]:
        if ancestor["level"] == "collection":
            for note in ancestor["_resolved"]["notes"]:
                if note["type"] == "userestrict":
                    if (
                        bool(note["subnotes"][0]["content"])
                        and note["subnotes"][0]["publish"]
                    ):
                        xmp_dc["rights"] = note["subnotes"][0]["content"]
        elif ancestor["level"] == "series":
            for note in ancestor["_resolved"]["notes"]:
                if note["type"] == "userestrict":
                    if (
                        bool(note["subnotes"][0]["content"])
                        and note["subnotes"][0]["publish"]
                    ):
                        xmp_dc["rights"] = note["subnotes"][0]["content"]
        elif ancestor["level"] == "subseries":
            for note in ancestor["_resolved"]["notes"]:
                if note["type"] == "userestrict":
                    if (
                        bool(note["subnotes"][0]["content"])
                        and note["subnotes"][0]["publish"]
                    ):
                        xmp_dc["rights"] = note["subnotes"][0]["content"]
    for note in archival_object["notes"]:
        if note["type"] == "userestrict":
            if bool(note["subnotes"][0]["content"]) and note["subnotes"][0]["publish"]:
                xmp_dc["rights"] = note["subnotes"][0]["content"]
    return xmp_dc


def post_digital_object_component(json_data):
    post_response = asnake_client.post(
        "/repositories/2/digital_object_components", json=json_data
    )
    post_response.raise_for_status()
    archivesspace_logger.info(post_response.json()["uri"])
    return post_response


def find_digital_object(digital_object_digital_object_id):
    """Return digital_object URI using the digital_object_id."""
    find_by_id_response = asnake_client.get(
        f"/repositories/2/find_by_id/digital_objects?digital_object_id[]={digital_object_digital_object_id}"
    )
    find_by_id_response.raise_for_status()
    if len(find_by_id_response.json()["digital_objects"]) < 1:
        return None
    if len(find_by_id_response.json()["digital_objects"]) > 1:
        raise ValueError(
            f"Multiple digital_objects found with digital_object_id: {digital_object_digital_object_id}"
        )
    return find_by_id_response.json()["digital_objects"][0]["ref"]


def get_digital_object(digital_object_component_id):
    """Return digital_object metadata using the digital_object_component_id."""
    find_by_id_response = asnake_client.get(
        f"/repositories/2/find_by_id/digital_object_components?digital_object_id[]={digital_object_component_id}"
    )
    find_by_id_response.raise_for_status()
    if len(find_by_id_response.json()["digital_objects"]) < 1:
        return None
    if len(find_by_id_response.json()["digital_objects"]) > 1:
        raise ValueError(
            f"Multiple digital_objects found with digital_object_component_id: {digital_object_component_id}"
        )
    digital_object_get_response = asnake_client.get(
        f"{find_by_id_response.json()['digital_objects'][0]['ref']}"
    )
    digital_object_get_response.raise_for_status()
    return digital_object_get_response.json()


def get_directory_bytes(directory):
    """Return the total bytes of all files under the given directory."""
    return sum(f.stat().st_size for f in Path(directory).glob("**/*") if f.is_file())


def save_digital_object_component_record(variables):
    if variables.get("file_uri_scheme") == None:
        logger.warning('⚠️  MISSING variables["file_uri_scheme"]')
        return
    # NOTE indexing status of a newly-created digital_object_component is
    # unreliable; the precomputed_waypoints of the digital_object tree is a
    # better indicator of digital_object_component existence
    digital_objects = [
        i
        for i in variables["archival_object"]["instances"]
        if "digital_object" in i.keys()
    ]
    if len(digital_objects) > 1:
        # TODO handle multiple digital_objects
        logger.debug(f"🐞 MULTIPLE DIGITAL_OBJECTS: {digital_objects}")
    if len(digital_objects) == 1:
        logger.debug(f"🐞 DIGITAL_OBJECTS[0]: {digital_objects[0]}")
        digital_object_tree = archivessnake_get(
            digital_objects[0]["digital_object"]["ref"] + "/tree/root"
        ).json()
        logger.debug(f"🐞 DIGITAL_OBJECT_TREE: {digital_object_tree}")
        if not digital_object_tree.get("precomputed_waypoints"):
            # NOTE without precomputed_waypoints we assume there are no
            # digital_object_components
            logger.debug(
                f"🐞 NO PRECOMPUTED_WAYPOINTS; CREATING DIGITAL_OBJECT_COMPONENT"
            )
            return create_digital_object_component(variables)
    else:
        raise RuntimeError(f"❌ NO DIGITAL_OBJECTS FOUND: {variables}")
    digital_object_components_summary = [
        _ for _ in digital_object_tree["precomputed_waypoints"][""]["0"]
    ]
    logger.debug(f"🐞 DIGITAL_OBJECT_COMPONENTS: {digital_object_components_summary}")
    if not digital_object_components_summary:
        logger.debug(
            f"🐞 NO DIGITAL_OBJECT_COMPONENTS_SUMMARY; CREATING DIGITAL_OBJECT_COMPONENT"
        )
        return create_digital_object_component(variables)
    if variables["preservation_file_info"]["filepath"].parent.name not in [
        _["label"] for _ in digital_object_components_summary
    ]:
        logger.debug(
            "🐞 DIGITAL_OBJECT_COMPONENT LABEL NOT FOUND: {}; CREATING DIGITAL_OBJECT_COMPONENT".format(
                variables["preservation_file_info"]["filepath"].parent.name,
            )
        )
        return create_digital_object_component(variables)
    # NOTE assuming there is a file_uri_summary in each digital_object_component
    for digital_object_component_summary in digital_object_components_summary:
        file_uri_summary = digital_object_component_summary["file_uri_summary"].split(
            ","
        )
        for file_uri in file_uri_summary:
            if file_uri.strip().startswith(
                f'{variables["file_uri_scheme"]}://'
            ) and file_uri.strip().endswith(
                variables["preservation_file_info"]["filepath"].name
            ):
                raise RuntimeError(
                    "❌ EXISTING {} FILE_URI FOUND ON DIGITAL_OBJECT_COMPONENT: {}".format(
                        variables["file_uri_scheme"],
                        digital_object_component_summary["label"],
                    )
                )
            else:
                logger.debug(
                    "🐞 NO EXISTING {} FILE_URI FOUND; ADDING FILE_VERSION TO DIGITAL_OBJECT_COMPONENT: {}".format(
                        variables["file_uri_scheme"],
                        digital_object_component_summary["label"],
                    )
                )
                # load the full digital_object_component
                digital_object_component = archivessnake_get(
                    digital_object_component_summary["uri"]
                ).json()
                file_version = construct_file_version(variables)
                digital_object_component["file_versions"].append(file_version)
                archivessnake_post(
                    digital_object_component["uri"], digital_object_component
                )
                logger.info(
                    f'☑️  DIGITAL OBJECT COMPONENT UPDATED: {digital_object_component["uri"]}'
                )
                return digital_object_component["uri"]


def construct_digital_object_component(variables):
    digital_object_component = {}
    digital_object_component["component_id"] = Path(
        variables["preservation_file_info"]["filepath"]
    ).stem
    digital_object_component["label"] = Path(
        Path(variables["preservation_file_info"]["filepath"]).parent
    ).name
    logger.debug(f"🐞 DIGITAL_OBJECT_COMPONENT: {digital_object_component}")
    # NOTE digital_object_digital_object_id will be the same as the component_id
    # of the corresponding archival_object; this should be the same as the name
    # of the JSON file that is a sibling of the preservation_file parent
    found_archival_object_datafiles = sorted(
        Path(variables["preservation_file_info"]["filepath"]).parent.parent.glob(
            "*.json"
        )
    )
    logger.debug(
        f"🐞 FOUND_ARCHIVAL_OBJECT_DATAFILES: {found_archival_object_datafiles}"
    )
    if len(found_archival_object_datafiles) == 1:
        digital_object_digital_object_id = found_archival_object_datafiles[0].stem
    else:
        raise RuntimeError(
            f"❌ {len(found_archival_object_datafiles)} JSON FILES FOUND IN: {Path(variables['preservation_file_info']['filepath']).parent.parent.name}"
        )
    logger.debug(
        f"🐞 DIGITAL_OBJECT_DIGITAL_OBJECT_ID: {digital_object_digital_object_id}"
    )
    digital_object_uri = find_digital_object(digital_object_digital_object_id)
    logger.debug(f"🐞 DIGITAL_OBJECT_URI: {digital_object_uri}")
    if digital_object_uri:
        digital_object_component["digital_object"] = {"ref": digital_object_uri}
    else:
        digital_object_uri, archival_object = create_digital_object(
            variables["archival_object"]
        )
        digital_object_component["digital_object"] = {"ref": digital_object_uri}
    digital_object_component["file_versions"] = [construct_file_version(variables)]
    return digital_object_component


def create_digital_object_component(variables):
    digital_object_component = construct_digital_object_component(variables)
    digital_object_component_post_response = archivessnake_post(
        "/repositories/2/digital_object_components", digital_object_component
    )
    logger.info(
        f'✳️  DIGITAL OBJECT COMPONENT CREATED: {digital_object_component_post_response.json()["uri"]}'
    )
    return digital_object_component_post_response.json()["uri"]


def construct_file_version(variables):
    """
    file_version["file_uri"]
    file_version["publish"]  # defaults to false
    file_version["use_statement"]
    file_version["file_format_name"]
    file_version["file_format_version"]
    file_version["file_size_bytes"]
    file_version["checksum"]
    file_version["checksum_method"]
    file_version["caption"]
    """
    file_version = {}
    file_version["checksum_method"] = "md5"
    file_version["checksum"] = variables["preservation_file_info"]["md5"].hexdigest()
    file_version["file_size_bytes"] = int(
        variables["preservation_file_info"]["filesize"]
    )
    file_key = str(variables["preservation_file_info"]["filepath"])[
        len(f'{config("WORK_PRESERVATION_FILES")}/') :
    ]
    file_version[
        "file_uri"
    ] = f'{variables["file_uri_scheme"]}://{variables["file_uri_host"]}/{file_key}'
    # NOTE additional mimetypes TBD
    if variables["preservation_file_info"]["mimetype"] == "image/jp2":
        file_version["file_format_name"] = "JPEG 2000"
        file_version["use_statement"] = "image-master"
        if (
            variables["preservation_file_info"]["transformation"] == "5-3 reversible"
            and variables["preservation_file_info"]["quantization"] == "no quantization"
        ):
            file_version[
                "caption"
            ] = f'width: {variables["preservation_file_info"]["width"]}; height: {variables["preservation_file_info"]["height"]}; compression: lossless'
            file_version[
                "file_format_version"
            ] = f'{variables["preservation_file_info"]["standard"]}; lossless (wavelet transformation: 5/3 reversible with no quantization)'
        elif (
            variables["preservation_file_info"]["transformation"] == "9-7 irreversible"
            and variables["preservation_file_info"]["quantization"]
            == "scalar expounded"
        ):
            file_version[
                "caption"
            ] = f'width: {variables["preservation_file_info"]["width"]}; height: {variables["preservation_file_info"]["height"]}; compression: lossy'
            file_version[
                "file_format_version"
            ] = f'{variables["preservation_file_info"]["standard"]}; lossy (wavelet transformation: 9/7 irreversible with scalar expounded quantization)'
        else:
            file_version[
                "caption"
            ] = f'width: {variables["preservation_file_info"]["width"]}; height: {variables["preservation_file_info"]["height"]}'
            file_version["file_format_version"] = variables["preservation_file_info"][
                "standard"
            ]
    return file_version


def create_lossless_jpeg2000_image(variables):
    """Convert original image and ensure matching image signatures."""
    cut_cmd = sh.Command(config("WORK_CUT_CMD"))
    sha512sum_cmd = sh.Command(config("WORK_SHA512SUM_CMD"))
    magick_cmd = sh.Command(config("WORK_MAGICK_CMD"))
    # Get checksum characters only by using `cut` (in the background).
    logger.info("🧮 CALCULATING ORIGINAL IMAGE SIGNATURE...")
    original_image_signature = cut_cmd(
        sha512sum_cmd(
            magick_cmd.stream(
                "-quiet",
                "-map",
                "rgb",
                "-storage-type",
                "short",
                variables["original_image_path"],
                "-",
                _piped=True,
                _bg=True,
            ),
            _bg=True,
        ),
        "-d",
        " ",
        "-f",
        "1",
        _bg=True,
    )
    # Compile filepath components.
    filepath_components = get_file_parts(
        variables["original_image_path"]
    )  # TODO rename function
    # Replace the original extension with `jp2` and store
    # `preservation_image_key` for the function to return as a string.
    preservation_image_key = "jp2".join(
        get_digital_object_component_file_key(
            get_archival_object_directory_prefix(
                variables["arrangement"], variables["archival_object"]
            ),
            filepath_components,
        ).rsplit(filepath_components["extension"], 1)
    )
    preservation_image_path = (
        Path(config("WORK_PRESERVATION_FILES"))
        .joinpath(preservation_image_key)
        .as_posix()
    )
    Path(Path(preservation_image_path).parent).mkdir(parents=True, exist_ok=True)
    # Convert the image (in the background).
    logger.info("⏳ CONVERTING IMAGE...")
    image_conversion = magick_cmd.convert(
        "-quiet",
        variables["original_image_path"],
        "-quality",
        "0",
        preservation_image_path,
        _bg=True,
    )
    # Gather metadata for embedding into the JPEG 2000.
    xmp_dc = get_xmp_dc_metadata(
        variables["arrangement"],
        filepath_components,
        variables["archival_object"],
    )
    # Catch any conversion errors in order to skip file and continue.
    # TODO needs testing
    try:
        image_conversion.wait()
    except Exception as e:
        # TODO log unfriendly `str(e)` instead of sending it along
        # EXAMPLE:
        # RAN: /usr/local/bin/magick convert -quiet /path/to/HBF/HBF_001_02/HBF_001_02_00.tif -quality 0 /path/to/HBF/HBF_001_02/HBF_001_02_00-LOSSLESS.jp2
        # STDOUT:
        # STDERR:
        # convert: Cannot read TIFF header. `/path/to/HBF/HBF_001_02/HBF_001_02_00.tif' @ error/tiff.c/TIFFErrors/595.
        # convert: no images defined `/path/to/HBF/HBF_001_02/HBF_001_02_00-LOSSLESS.jp2' @ error/convert.c/ConvertImageCommand/3304.
        raise RuntimeError(str(e))
    # Embed metadata into the JPEG 2000.
    write_xmp_metadata(preservation_image_path, xmp_dc)
    # Get checksum characters only by using `cut` (in the background).
    logger.info("🧮 CALCULATING PRESERVATION IMAGE SIGNATURE...")
    preservation_image_signature = cut_cmd(
        sha512sum_cmd(
            magick_cmd.stream(
                "-quiet",
                "-map",
                "rgb",
                "-storage-type",
                "short",
                preservation_image_path,
                "-",
                _piped=True,
                _bg=True,
            ),
            _bg=True,
        ),
        "-d",
        " ",
        "-f",
        "1",
        _bg=True,
    )
    # Wait for image signatures.
    original_image_signature.wait()
    preservation_image_signature.wait()
    # Verify that image signatures match.
    if original_image_signature != preservation_image_signature:
        raise RuntimeError(
            f'❌ image signatures did not match: {filepath_components["filestem"]}'
        )
    logger.info(
        f'☑️  IMAGE SIGNATURES MATCH:\n{original_image_signature.strip()} {variables["original_image_path"].split("/")[-1]}\n{preservation_image_signature.strip()} {preservation_image_path.split("/")[-1]}'
    )
    return preservation_image_key


def save_archival_object_datafile(arrangement, archival_object, directory):
    """Save the archival object data to a JSON file."""
    # TODO rename functions to be more abstract
    archival_object_datafile_key = get_archival_object_datafile_key(
        get_archival_object_directory_prefix(arrangement, archival_object),
        archival_object,
    )
    filename = os.path.join(
        directory,
        archival_object_datafile_key,
    )
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        f.write(json.dumps(archival_object, indent=4))
    logger.info(f"☑️  ARCHIVAL OBJECT DATA SAVED: {filename}")
    return archival_object_datafile_key


def save_preservation_file(source, destination):
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    shutil.copy2(source, destination)


def set_digital_object_id(uri, digital_object_id):
    # raises an HTTPError exception if unsuccessful
    get_response_json = asnake_client.get(uri).json()
    get_response_json["digital_object_id"] = digital_object_id
    post_response = asnake_client.post(uri, json=get_response_json)
    post_response.raise_for_status()
    return


def update_digital_object(uri, data):
    # raises an HTTPError exception if unsuccessful
    response = asnake_client.post(uri, json=data)
    response.raise_for_status()
    archivesspace_logger.info(response.json()["uri"])
    return response


def write_xmp_metadata(filepath, metadata):
    # NOTE: except `source` all the dc elements here are keywords in exiftool
    exiftool_cmd = sh.Command(config("WORK_EXIFTOOL_CMD"))
    return exiftool_cmd(
        "-title=" + metadata["title"],
        "-identifier=" + metadata["identifier"],
        "-XMP-dc:source=" + metadata["source"],
        "-publisher=" + metadata["publisher"],
        "-rights=" + metadata["rights"],
        "-overwrite_original",
        filepath,
    )


def get_preservation_image_data(filepath):
    preservation_image_data = {}
    preservation_image_data["filepath"] = filepath
    jpylyzer_xml = jpylyzer.checkOneFile(preservation_image_data["filepath"])
    preservation_image_data["filesize"] = jpylyzer_xml.findtext(
        "./fileInfo/fileSizeInBytes"
    )
    preservation_image_data["width"] = jpylyzer_xml.findtext(
        "./properties/jp2HeaderBox/imageHeaderBox/width"
    )
    preservation_image_data["height"] = jpylyzer_xml.findtext(
        "./properties/jp2HeaderBox/imageHeaderBox/height"
    )
    preservation_image_data["standard"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestreamBox/siz/rsiz"
    )
    preservation_image_data["transformation"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestreamBox/cod/transformation"
    )
    preservation_image_data["quantization"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestreamBox/qcd/qStyle"
    )
    with open(preservation_image_data["filepath"], "rb") as f:
        preservation_image_data["md5"] = hashlib.md5(f.read())
    return preservation_image_data


def create_derivative_files(variables, onsite=None, cloud=None, access=None):
    """Loop over files in working_archival_object_directory.

    Example:
    WORKING_ORIGINAL_FILES
    ├── CollectionID_000_XX
    ├── CollectionID_001_02 <-- looping over files under here
    │   ├── CollectionID_001_02_01.tif
    │   ├── CollectionID_001_02_02.tif
    │   ├── CollectionID_001_02_03.tif
    │   └── CollectionID_001_02_04.tif
    └── CollectionID_007_08

    Create derivative files for implemented mime types or copy the original file
    to the appropriate directory.
    """
    # NOTE We use a reversed list so the components will be ingested in
    # the correct order for the digital object tree and use it with pop() so the
    # count of remaining items is accurate during the loop.
    variables["filepaths_popped"] = sorted(variables["filepaths"], reverse=True)
    variables["filepaths_count_initial"] = len(variables["filepaths"])
    for f in range(variables["filepaths_count_initial"]):
        # TODO rename variable to original_file_path
        variables["original_image_path"] = variables["filepaths_popped"].pop()
        logger.info(
            f'▶️  PROCESSING ITEM: {variables["original_image_path"][len(config("WORKING_ORIGINAL_FILES")) + 1:]}'
        )

        type, encoding = mimetypes.guess_type(variables["original_image_path"])

        if onsite or cloud:
            if type and type.startswith("image/"):
                try:
                    # Create lossless JPEG 2000 image from original.
                    preservation_image_key = create_lossless_jpeg2000_image(variables)
                except Exception as e:
                    logger.exception(e)
                    continue
                else:
                    status_logger.info(
                        f"☑️  LOSSLESS JPEG 2000 DERIVATIVE CREATED: {preservation_image_key}"
                    )
            else:
                filepath_components = get_file_parts(variables["original_image_path"])
                preservation_file_key = get_digital_object_component_file_key(
                    get_archival_object_directory_prefix(
                        variables["arrangement"],
                        variables["archival_object"],
                    ),
                    filepath_components,
                )
                preservation_file_path = Path(
                    config("WORK_PRESERVATION_FILES")
                ).joinpath(preservation_file_key)
                try:
                    # Make necessary destination directory path and copy file.
                    preservation_file_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(
                        variables["original_image_path"],
                        preservation_file_path,
                    )
                except Exception as e:
                    logger.exception(e)
                    continue
                else:
                    status_logger.info(
                        f"☑️  ORIGINAL FILE COPIED: {preservation_file_key}"
                    )

        if access:
            if type.startswith("image/"):
                result = access.create_access_file(variables)
            else:
                raise NotImplementedError(
                    "Only image files are supported at this time."
                )


if __name__ == "__main__":
    # fmt: off
    from rpyc.utils.server import ThreadedServer
    ThreadedServer(DistilleryService, port=config("DISTILLERY_RPYC_PORT")).start()
