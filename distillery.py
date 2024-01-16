# CALTECH ARCHIVES AND SPECIAL COLLECTIONS
# digital object preservation workflow

# processing functionality; see web.py for bottlepy web application

import hashlib
import http
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

from pathlib import Path

import backoff
import requests
import rpyc
import urllib3

from asnake.client import ASnakeClient
from decouple import config

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
status_logfile = Path(config("WORK_LOG_FILES")).joinpath("status.log")
status_logfile.touch()
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
        # TODO change all references from self.destinations to self.variables["destinations"]
        self.destinations = destinations
        self.onsite_medium = None
        self.cloud_platform = None
        self.access_platform = None
        self.variables = {}
        self.variables["destinations"] = destinations

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
    def validate(self, destinations, batch_set_id):
        """Validate connections, files, and data."""

        # reset status_logfile
        status_logger = logging.getLogger("status")
        for status_logger_handler in status_logger.handlers[:]:
            # remove the existing file handlers
            if isinstance(status_logger_handler, logging.FileHandler):
                status_logger.removeHandler(status_logger_handler)
        # set the new handler
        status_logfile = Path(config("WORK_LOG_FILES")).joinpath(
            f"{batch_set_id}.validate.log"
        )
        status_logfile.touch()
        status_handler = logging.FileHandler(status_logfile)
        status_handler.setLevel(logging.INFO)
        status_handler.setFormatter(statuslogger.StatusFormatter("%(message)s"))
        status_logger.addHandler(status_handler)

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

        def is_archival_object_valid(component_id):
            try:
                archival_object = find_archival_object(component_id)
            except Exception as e:
                status_logger.error(e)
                logger.exception(e)
            if not self.access_platform:
                # no need to check more conditions
                return True
            elif not archival_object["publish"]:
                message = "‼️ ARCHIVAL OBJECT NOT PUBLISHED: [**{}**]({}/resolve/readonly?uri={})".format(
                    archival_object["title"],
                    config("ASPACE_STAFF_URL"),
                    archival_object["uri"],
                )
                status_logger.error(message)
                return False
            elif archival_object["has_unpublished_ancestor"]:
                message = "‼️ ARCHIVAL OBJECT HAS UNPUBLISHED ANCESTOR: [**{}**]({}/resolve/readonly?uri={})".format(
                    archival_object["title"],
                    config("ASPACE_STAFF_URL"),
                    archival_object["uri"],
                )
                status_logger.error(message)
                return False
            # check for existing digital_object["file_versions"]
            elif archival_object.get("instances"):
                digital_object_count = 0
                for instance in archival_object["instances"]:
                    if "digital_object" in instance.keys():
                        digital_object_count += 1
                if digital_object_count > 1:
                    message = "‼️ MULTIPLE DIGITAL OBJECTS FOUND: [**{}**]({}/resolve/readonly?uri={})".format(
                        archival_object["title"],
                        config("ASPACE_STAFF_URL"),
                        archival_object["uri"],
                    )
                    status_logger.error(message)
                    return False
                elif digital_object_count == 1:
                    # NOTE self.destinations is a JSON string
                    if (
                        instance["digital_object"]["_resolved"].get("file_versions")
                        and "fail" in self.destinations
                    ):
                        message = "‼️  DIGITAL OBJECT ALREADY HAS FILE VERSIONS: [**{}**]({}/resolve/readonly?uri={})".format(
                            instance["digital_object"]["_resolved"]["title"],
                            config("ASPACE_STAFF_URL"),
                            instance["digital_object"]["ref"],
                        )
                        status_logger.error(message)
                        return False
                    elif (
                        instance["digital_object"]["_resolved"].get("file_versions")
                        and "overwrite" in self.destinations
                    ):
                        message = "⚠️ DIGITAL OBJECT FILE VERSIONS WILL BE OVERWRITTEN: [**{}**]({}/resolve/readonly?uri={})".format(
                            instance["digital_object"]["_resolved"]["title"],
                            config("ASPACE_STAFF_URL"),
                            instance["digital_object"]["ref"],
                        )
                        status_logger.warning(message)
                        return True
                    elif (
                        instance["digital_object"]["_resolved"].get("file_versions")
                        and "unpublish" in self.destinations
                    ):
                        message = "⚠️ DIGITAL OBJECT FILE VERSIONS WILL BE UNPUBLISHED: [**{}**]({}/resolve/readonly?uri={})".format(
                            instance["digital_object"]["_resolved"]["title"],
                            config("ASPACE_STAFF_URL"),
                            instance["digital_object"]["ref"],
                        )
                        status_logger.warning(message)
                        return True
                else:
                    # go through with creating a new digital_object
                    return True
            else:
                # ensure there is a return value
                return True

        archival_object_count = 0
        file_count = 0
        for dirpath, dirnames, filenames in os.walk(config("INITIAL_ORIGINAL_FILES")):
            logger.debug(f"🐞 DIRPATH: {dirpath}")
            logger.debug(f"🐞 DIRNAMES: {dirnames}")
            logger.debug(f"🐞 FILENAMES: {filenames}")
            filetype_supported = True
            validation_status = True
            if not dirnames and not filenames:
                message = "❌ NO DIRECTORIES OR FILES FOUND"
                status_logger.error(message)
                raise FileNotFoundError(message)
            # check filenames in root directory
            for filename in filenames:
                if filename in [".DS_Store", "Thumbs.db"]:
                    os.remove(Path(dirpath).joinpath(filename))
                else:
                    status_logger.info(f"📄 {filename}")
                    if self.access_platform:
                        type, encoding = mimetypes.guess_type(
                            Path(dirpath).joinpath(filename)
                        )
                        if not (type.startswith("image/") or type.endswith("/mp4")):
                            filetype_supported = False
                    validation_status = is_archival_object_valid(
                        filename.rsplit(".", maxsplit=1)[0]
                    )
                    file_count += 1
                    archival_object_count += 1
            # check dirnames in root directory
            for dirname in dirnames:
                status_logger.info(f"📁 {dirname}")
                validation_status = is_archival_object_valid(dirname)
                for dir_entry in os.scandir(Path(dirpath).joinpath(dirname)):
                    if dir_entry.name in [".DS_Store", "Thumbs.db"]:
                        os.remove(dir_entry.path)
                    else:
                        logger.debug(f"🐞 DIR_ENTRY.NAME: {dir_entry.name}")
                        file_count += 1
                archival_object_count += 1
            # reset dirnames to stop os.walk from descending into subdirectories
            # and continuing to loop over filenames
            # https://stackoverflow.com/a/43618972
            dirnames[:] = []
            if not filetype_supported:
                message = "❌ UNSUPPORTED FILE TYPE"
                status_logger.error(message)
                raise RuntimeError(message)
            if not validation_status:
                message = "❌ VALIDATION FAILURE"
                status_logger.error(message)
                raise RuntimeError(message)
        if archival_object_count and file_count:
            status_logger.info(f"🗂 ARCHIVAL OBJECT COUNT: {archival_object_count}")
            status_logger.info(f"📄 FILE COUNT: {file_count}")
        else:
            message = "❌ NO FILES FOUND"
            status_logger.error(message)
            raise FileNotFoundError(message)

        # send the character that stops javascript reloading in the web ui
        status_logger.info(f"🈺")  # Japanese “Open for Business” Button

    @rpyc.exposed
    def run(self, destinations, batch_set_id):
        """Run Distillery."""

        # reset status_logfile
        status_logger = logging.getLogger("status")
        for status_logger_handler in status_logger.handlers[:]:
            # remove the existing file handlers
            if isinstance(status_logger_handler, logging.FileHandler):
                status_logger.removeHandler(status_logger_handler)
        # set the new handler
        status_logfile = Path(config("WORK_LOG_FILES")).joinpath(
            f"{batch_set_id}.run.log"
        )
        status_logfile.touch()
        status_handler = logging.FileHandler(status_logfile)
        status_handler.setLevel(logging.INFO)
        status_handler.setFormatter(statuslogger.StatusFormatter("%(message)s"))
        status_logger.addHandler(status_handler)

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

            try:
                batch_directory = Path(config("BATCH_SETS_DIRECTORY")).joinpath(
                    batch_set_id
                )
                batch_directory.mkdir(parents=True, exist_ok=True)
                Path(config("INITIAL_ORIGINAL_FILES")).rename(
                    batch_directory.joinpath("STAGE_1_INITIAL")
                )
                batch_directory.joinpath("STAGE_2_WORKING").mkdir(
                    parents=True, exist_ok=True
                )
                batch_directory.joinpath("STAGE_3_COMPLETE").mkdir(
                    parents=True, exist_ok=True
                )
                Path(config("INITIAL_ORIGINAL_FILES")).mkdir()
            except BaseException:
                message = "❌ UNABLE TO MOVE THE SOURCE FILES FOR PROCESSING"
                status_logger.error(message)
                logger.exception(f"‼️")
                raise

            for dir_entry in sorted(
                os.scandir(batch_directory.joinpath("STAGE_1_INITIAL")),
                key=lambda dir_entry: dir_entry.name,
            ):
                if dir_entry.name in [".DS_Store", "Thumbs.db"]:
                    os.remove(dir_entry.path)
                    continue
                elif dir_entry.is_file():
                    dir_entry_stem = dir_entry.name.rsplit(".", maxsplit=1)[0]
                elif dir_entry.is_dir():
                    dir_entry_stem = dir_entry.name
                else:
                    logger.warning(
                        f"⚠️ UNEXPECTED OS.DIRENTRY OBJECT: {dir_entry.name}"
                    )
                    continue

                try:
                    self.variables["archival_object"] = find_archival_object(
                        dir_entry_stem
                    )
                except Exception as e:
                    status_logger.error(e)
                    logger.exception(e)
                self.variables["arrangement"] = get_arrangement(
                    self.variables["archival_object"]
                )

                # collection-level preprocessing
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

                initial_archival_object = dir_entry.path
                working_archival_object = str(
                    batch_directory.joinpath("STAGE_2_WORKING", dir_entry.name)
                )
                try:
                    shutil.move(initial_archival_object, working_archival_object)
                except BaseException:
                    message = "❌ UNABLE TO MOVE THE INITIAL FILES FOR WORKING"
                    status_logger.error(message)
                    logger.exception(f"‼️")
                    raise

                # Set up list of file paths for the current directory.
                if Path(working_archival_object).is_file():
                    self.variables["filepaths"] = [working_archival_object]
                elif Path(working_archival_object).is_dir():
                    self.variables["filepaths"] = [
                        f.path
                        for f in os.scandir(working_archival_object)
                        if f.is_file()
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
                    prepare_preservation_files(self.variables)

                if accessDistiller:
                    accessDistiller.archival_object_level_processing(self.variables)
                    build_directory = accessDistiller.get_build_directory()
                    self.access_platform.loop_over_archival_object_files(
                        build_directory, self.variables
                    )

                if accessDistiller:
                    # NOTE working on variables["archival_object"]["component_id"]
                    accessDistiller.transfer_archival_object_derivative_files(
                        self.variables
                    )
                    status_logger.info(
                        "☑️  ACCESS PAGE CREATED: [**{}**]({}/{}/{}/{})".format(
                            self.variables["archival_object"]["component_id"],
                            config("ALCHEMIST_BASE_URL").rstrip("/"),
                            config("ALCHEMIST_URL_PREFIX"),
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
                            if filename in [".DS_Store", "Thumbs.db"]:
                                os.remove(filepath)
                                continue
                            if Path(filename).suffix == ".json" and Path(
                                dirpath
                            ).name.startswith(Path(filename).stem):
                                # skip archival_object JSON metadata
                                continue
                            logger.info(
                                f"▶️  GETTING PRESERVATION FILE INFO: {filepath}"
                            )
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
                    # directory; move them into an intermediate directory for
                    # deletion because deleting across slow file systems can
                    # result in cruft still existing on the next iteration
                    if not config("WORK_STILLAGE_FILES", default=""):
                        # create a .STILLAGE directory
                        stillage_default = Path(
                            config("WORK_PRESERVATION_FILES")
                        ).parent.joinpath(".STILLAGE")
                        stillage_default.mkdir(exist_ok=True)
                    logger.debug(
                        "🐞 STILLAGE DIRECTORY: {}".format(
                            Path(
                                config("WORK_STILLAGE_FILES", default=stillage_default)
                            ).as_posix()
                        )
                    )
                    if (
                        Path(config("WORK_STILLAGE_FILES", default=stillage_default))
                        .resolve()
                        .joinpath(self.variables["arrangement"]["collection_id"])
                        .is_dir()
                    ):
                        for dir_entry in os.scandir(
                            Path(config("WORK_PRESERVATION_FILES")).joinpath(
                                self.variables["arrangement"]["collection_id"]
                            )
                        ):
                            shutil.move(
                                dir_entry.path,
                                Path(
                                    config(
                                        "WORK_STILLAGE_FILES", default=stillage_default
                                    )
                                )
                                .resolve()
                                .joinpath(
                                    self.variables["arrangement"]["collection_id"]
                                )
                                .as_posix(),
                            )
                    else:
                        shutil.move(
                            Path(config("WORK_PRESERVATION_FILES"))
                            .joinpath(self.variables["arrangement"]["collection_id"])
                            .as_posix(),
                            Path(
                                config("WORK_STILLAGE_FILES", default=stillage_default)
                            )
                            .resolve()
                            .as_posix(),
                        )
                    os.system(
                        "/bin/rm -rf {}".format(
                            Path(
                                config("WORK_STILLAGE_FILES", default=stillage_default)
                            )
                            .joinpath(self.variables["arrangement"]["collection_id"])
                            .resolve()
                            .as_posix()
                        )
                    )

                try:
                    shutil.move(
                        working_archival_object,
                        str(
                            batch_directory.joinpath("STAGE_3_COMPLETE", dir_entry.name)
                        ),
                    )
                except BaseException:
                    message = "❌ UNABLE TO MOVE THE WORKING FILES FOR COMPLETION"
                    status_logger.error(message)
                    logger.exception(f"‼️")
                    raise

        except Exception as e:
            status_logger.error("❌ SOMETHING WENT WRONG")
            status_logger.error(e)
            logger.exception("‼️")
            raise
        # complete the process if there is no error
        else:
            # send the character that stops javascript reloading in the web ui
            status_logger.info(f"🏁")
            # TODO delete PRESERVATION_FILES/CollectionID directory

    @rpyc.exposed
    def alchemist_regenerate(self, component_id="", collection_id="", logfile=""):
        if component_id:
            status_logger = logging.getLogger(component_id)
        elif collection_id:
            status_logger = logging.getLogger(collection_id)
        else:
            status_logger = logging.getLogger("_")
        status_logger.setLevel(logging.INFO)
        Path(logfile).touch()
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
                archival_object_path = "/".join(
                    [
                        config("ALCHEMIST_URL_PREFIX"),
                        variables["arrangement"]["collection_id"],
                        variables["archival_object"]["component_id"],
                    ]
                )
                if config("ALCHEMIST_CLOUDFRONT_DISTRIBUTION_ID", default=False):
                    self.access_platform.invalidate_cloudfront_path(
                        path=f"/{archival_object_path}/*"
                    )
                status_logger.info(
                    "☑️  ALCHEMIST FILES REGENERATED: [**{}**]({})".format(
                        variables["archival_object"]["component_id"],
                        "/".join(
                            [
                                config("ALCHEMIST_BASE_URL").rstrip("/"),
                                archival_object_path,
                            ]
                        ),
                    )
                )
            elif collection_id:
                # TODO DRY this out
                # regenerate files for a collection
                status_logger.info(f"🟢 BEGIN REGENERATING ITEMS FOR: {collection_id}")
                archival_object_prefixes = accessDistiller.regenerate_collection(
                    collection_id
                )
                if config("ALCHEMIST_CLOUDFRONT_DISTRIBUTION_ID", default=False):
                    # invalidate existing paths so the status_logger links work
                    self.access_platform.invalidate_cloudfront_path(
                        path=f'/{config("ALCHEMIST_URL_PREFIX")}/{collection_id}/*'
                    )
                for archival_object_prefix in archival_object_prefixes:
                    component_id = archival_object_prefix.split("/")[-2]
                    variables["archival_object"] = find_archival_object(component_id)
                    variables["arrangement"] = get_arrangement(
                        variables["archival_object"]
                    )
                    accessDistiller.archival_object_level_processing(variables)
                    accessDistiller.transfer_archival_object_derivative_files(variables)
                    status_logger.info(
                        "☑️  ALCHEMIST FILES REGENERATED: [**{}**]({}/{}/{}/{})".format(
                            variables["archival_object"]["component_id"],
                            config("ALCHEMIST_BASE_URL").rstrip("/"),
                            config("ALCHEMIST_URL_PREFIX"),
                            variables["arrangement"]["collection_id"],
                            variables["archival_object"]["component_id"],
                        )
                    )
                if config("ALCHEMIST_CLOUDFRONT_DISTRIBUTION_ID", default=False):
                    # invalidate again to ensure all paths serve fresh content
                    self.access_platform.invalidate_cloudfront_path(
                        path=f'/{config("ALCHEMIST_URL_PREFIX")}/{collection_id}/*'
                    )
            else:
                # TODO DRY this out
                # regenerate files for all items
                status_logger.info("🟢 BEGIN REGENERATING ALL")
                archival_object_prefixes = accessDistiller.regenerate_all()
                if config("ALCHEMIST_CLOUDFRONT_DISTRIBUTION_ID", default=False):
                    # invalidate existing paths so the status_logger links work
                    self.access_platform.invalidate_cloudfront_path()
                for archival_object_prefix in archival_object_prefixes:
                    component_id = archival_object_prefix.split("/")[-2]
                    variables["archival_object"] = find_archival_object(component_id)
                    variables["arrangement"] = get_arrangement(
                        variables["archival_object"]
                    )
                    accessDistiller.archival_object_level_processing(variables)
                    accessDistiller.transfer_archival_object_derivative_files(variables)
                    status_logger.info(
                        "☑️  ALCHEMIST FILES REGENERATED: [**{}**]({}/{}/{}/{})".format(
                            variables["archival_object"]["component_id"],
                            config("ALCHEMIST_BASE_URL").rstrip("/"),
                            config("ALCHEMIST_URL_PREFIX"),
                            variables["arrangement"]["collection_id"],
                            variables["archival_object"]["component_id"],
                        )
                    )
                if config("ALCHEMIST_CLOUDFRONT_DISTRIBUTION_ID", default=False):
                    # invalidate again to ensure all paths serve fresh content
                    self.access_platform.invalidate_cloudfront_path()
        except Exception as e:
            status_logger.error("❌ SOMETHING WENT WRONG")
            status_logger.error(e)
            logger.exception("‼️")
            raise
        # complete the process if there is no error
        else:
            # send the character that stops javascript reloading in the web ui
            status_logger.info(f"🏁")


def get_collection_data(collection_id):
    # raises an HTTPError exception if unsuccessful
    collection_uri = get_collection_uri(collection_id)
    collection_data = archivessnake_get(collection_uri).json()
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


@backoff.on_exception(
    backoff.expo,
    (
        http.client.RemoteDisconnected,
        urllib3.exceptions.ProtocolError,
        urllib3.exceptions.NewConnectionError,
        urllib3.exceptions.MaxRetryError,
        requests.exceptions.ConnectionError,
    ),
    max_time=1800,
)
def archivessnake_get(uri):
    return asnake_client.get(uri)


@backoff.on_exception(
    backoff.expo,
    (
        http.client.RemoteDisconnected,
        urllib3.exceptions.ProtocolError,
        urllib3.exceptions.NewConnectionError,
        urllib3.exceptions.MaxRetryError,
        requests.exceptions.ConnectionError,
    ),
    max_time=1800,
)
def archivessnake_post(uri, object):
    return asnake_client.post(uri, json=object)


@backoff.on_exception(
    backoff.expo,
    (
        http.client.RemoteDisconnected,
        urllib3.exceptions.ProtocolError,
        urllib3.exceptions.NewConnectionError,
        urllib3.exceptions.MaxRetryError,
        requests.exceptions.ConnectionError,
    ),
    max_time=1800,
)
def archivessnake_delete(uri):
    return asnake_client.delete(uri)


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
        collection_data["id_0"], f"{collection_data['id_0']}.json"
    )
    filename = os.path.join(directory, collection_datafile_key)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        f.write(json.dumps(collection_data, indent=4))
    logger.info(f"☑️  COLLECTION DATA FILE SAVED: {filename}")
    return collection_datafile_key


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
                        file_version["is_representative"] = False
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


def create_digital_object(archival_object, digital_object_type=""):
    digital_object = {}
    digital_object["digital_object_id"] = archival_object["component_id"]  # required
    digital_object["title"] = archival_object["title"]  # required
    if digital_object_type:
        digital_object["digital_object_type"] = digital_object_type
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
    arrangement["archival_object_display_string"]
    arrangement["archival_object_level"]
    arrangement["archival_object_title"]
    arrangement["collection_title"]
    arrangement["collection_id"]
    arrangement["collection_uri"]
    arrangement["series_display_string"]
    arrangement["series_id"]
    arrangement["series_title"]
    arrangement["series_uri"]
    arrangement["subseries_display_string"]
    arrangement["subseries_id"]
    arrangement["subseries_title"]
    arrangement["subseries_uri"]
    arrangement["file_display_string"]
    arrangement["file_id"]
    arrangement["file_title"]
    arrangement["file_uri"]
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
        arrangement["archival_object_display_string"] = archival_object[
            "display_string"
        ]
        arrangement["archival_object_level"] = archival_object["level"]
        arrangement["archival_object_title"] = archival_object.get("title")
        for ancestor in archival_object["ancestors"]:
            if ancestor["level"] == "collection":
                arrangement["collection_title"] = ancestor["_resolved"]["title"]
                arrangement["collection_id"] = ancestor["_resolved"]["id_0"]
                arrangement["collection_uri"] = ancestor["ref"]
            elif ancestor["level"] == "series":
                arrangement["series_display_string"] = ancestor["_resolved"][
                    "display_string"
                ]
                arrangement["series_id"] = ancestor["_resolved"].get("component_id")
                arrangement["series_title"] = ancestor["_resolved"].get("title")
                arrangement["series_uri"] = ancestor["ref"]
            elif ancestor["level"] == "subseries":
                arrangement["subseries_display_string"] = ancestor["_resolved"][
                    "display_string"
                ]
                arrangement["subseries_id"] = ancestor["_resolved"].get("component_id")
                arrangement["subseries_title"] = ancestor["_resolved"].get("title")
                arrangement["subseries_uri"] = ancestor["ref"]
            elif ancestor["level"] == "file":
                arrangement["file_display_string"] = ancestor["_resolved"][
                    "display_string"
                ]
                arrangement["file_id"] = ancestor["_resolved"].get("component_id")
                arrangement["file_title"] = ancestor["_resolved"].get("title")
                arrangement["file_uri"] = ancestor["ref"]
        logger.info("☑️  ARRANGEMENT LEVELS AGGREGATED")
        return arrangement
    except:
        logger.exception("‼️")
        raise


def find_archival_object(component_id):
    """Returns a dict of the archival object data for a given component_id.

    Raises a ValueError if no archival object is found or if multiple archival
    objects are found.
    """
    find_uri = (
        f"/repositories/2/find_by_id/archival_objects?component_id[]={component_id}"
    )
    find_by_id_response = archivessnake_get(find_uri)
    if len(find_by_id_response.json()["archival_objects"]) < 1:
        message = f"❌ ARCHIVAL OBJECT NOT FOUND: {component_id}"
        raise ValueError(message)
    elif len(find_by_id_response.json()["archival_objects"]) > 1:
        message = f"❌ MULTIPLE ARCHIVAL OBJECTS FOUND: {component_id}"
        raise ValueError(message)
    else:
        archival_object = archivessnake_get(
            find_by_id_response.json()["archival_objects"][0]["ref"]
            + "?resolve[]=ancestors"
            + "&resolve[]=digital_object"
            + "&resolve[]=linked_agents"
            + "&resolve[]=repository"
            + "&resolve[]=subjects"
            + "&resolve[]=top_container"
        ).json()
        logger.info(f"☑️ ARCHIVAL OBJECT FOUND: {component_id}")
        return archival_object


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
        [c if c.isalnum() else "-" for c in arrangement["archival_object_title"]]
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
    #     "filepath": "/path/to/archives/data/BATCH/HaleGE/HaleGE_02_0B_056_07_0001.tiff",
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


def find_digital_object(digital_object_digital_object_id):
    """Return digital_object URI using the digital_object_id."""
    find_by_id_response = archivessnake_get(
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
    find_by_id_response = archivessnake_get(
        f"/repositories/2/find_by_id/digital_object_components?digital_object_id[]={digital_object_component_id}"
    )
    find_by_id_response.raise_for_status()
    if len(find_by_id_response.json()["digital_objects"]) < 1:
        return None
    if len(find_by_id_response.json()["digital_objects"]) > 1:
        raise ValueError(
            f"Multiple digital_objects found with digital_object_component_id: {digital_object_component_id}"
        )
    digital_object_get_response = archivessnake_get(
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
                variables["preservation_file_info"]["filepath"].parent.name
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
    return file_version


def save_archival_object_datafile(arrangement, archival_object, directory):
    """Save the archival object data to a JSON file."""
    # TODO rename functions to be more abstract
    archival_object_datafile_key = get_archival_object_datafile_key(
        get_archival_object_directory_prefix(arrangement, archival_object),
        archival_object,
    )
    filename = os.path.join(directory, archival_object_datafile_key)
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
    get_response_json = archivessnake_get(uri).json()
    get_response_json["digital_object_id"] = digital_object_id
    post_response = archivessnake_post(uri, get_response_json)
    post_response.raise_for_status()
    return


def update_digital_object(uri, data):
    # raises an HTTPError exception if unsuccessful
    response = archivessnake_post(uri, data)
    logger.debug(f"🐞 RESPONSE: {response.json()}")
    response.raise_for_status()
    archivesspace_logger.info(response.json()["uri"])
    return response


def prepare_preservation_files(variables):
    """Copy preservation files."""
    for filepath in variables["filepaths"]:
        variables["original_file_path"] = filepath
        logger.debug(f"🐞 ORIGINAL_FILE_PATH: {variables['original_file_path']}")
        filepath_components = get_file_parts(variables["original_file_path"])
        logger.debug(f"🐞 FILEPATH_COMPONENTS: {filepath_components}")
        preservation_file_key = get_digital_object_component_file_key(
            get_archival_object_directory_prefix(
                variables["arrangement"], variables["archival_object"]
            ),
            filepath_components,
        )
        logger.debug(f"🐞 PRESERVATION_FILE_KEY: {preservation_file_key}")
        preservation_file_path = Path(config("WORK_PRESERVATION_FILES")).joinpath(
            preservation_file_key
        )
        logger.debug(f"🐞 PRESERVATION_FILE_PATH: {preservation_file_path}")
        try:
            preservation_file_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(variables["original_file_path"], preservation_file_path)
        except Exception:
            logger.exception(
                "❌ ORIGINAL FILE COPY FAILED: {}".format(
                    variables["original_file_path"]
                )
            )
        else:
            status_logger.info(f"☑️  ORIGINAL FILE COPIED: {preservation_file_key}")


if __name__ == "__main__":
    # fmt: off
    from rpyc.utils.server import ThreadedServer
    ThreadedServer(DistilleryService, port=config("DISTILLERY_RPYC_PORT")).start()
