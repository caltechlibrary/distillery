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


def initiate_variables(destinations):
    """Initialize variables based on destinations."""
    variables = {}
    variables["destinations"] = destinations
    source_files = None

    if "cloud" in destinations and config("CLOUD_PLATFORM"):
        source_files = Path(config("COMMON_MOUNT_PARENT")).joinpath(
            json.loads(destinations)["cloud"]["network_volume"],
            config("COMMON_SOURCE_PATH"),
        )
    else:
        source_files = Path(config("COMMON_MOUNT_PARENT")).joinpath(
            "EXAMPLE_VOLUME_2", config("COMMON_SOURCE_PATH")
        )

    variables["source_files"] = source_files
    return variables


def import_modules(destinations):
    """Dynamically import modules based on destinations."""
    onsite_medium = None
    cloud_platform = None
    access_platform = None

    if "onsite" in destinations and config("ONSITE_MEDIUM"):
        try:
            onsite_medium = importlib.import_module(config("ONSITE_MEDIUM"))
            logger.debug(f"🐞 onsite_medium: {onsite_medium}")
        except Exception:
            message = f'❌ UNABLE TO IMPORT MODULE: {config("ONSITE_MEDIUM")}'
            status_logger.error(message)
            raise

    if "cloud" in destinations and config("CLOUD_PLATFORM"):
        try:
            cloud_platform = importlib.import_module(config("CLOUD_PLATFORM"))
            logger.debug(f"🐞 cloud_platform: {cloud_platform}")
        except Exception:
            message = f'❌ UNABLE TO IMPORT MODULE: {config("CLOUD_PLATFORM")}'
            status_logger.error(message)
            raise

    if "access" in destinations and config("ACCESS_PLATFORM"):
        try:
            access_platform = importlib.import_module(config("ACCESS_PLATFORM"))
            logger.debug(f"🐞 access_platform: {access_platform}")
        except Exception:
            message = f'❌ UNABLE TO IMPORT MODULE: {config("ACCESS_PLATFORM")}'
            status_logger.error(message)
            raise

    return onsite_medium, cloud_platform, access_platform


def validate(destinations, batch_set_id):
    status_logger = logging.getLogger("status")
    for handler in status_logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            status_logger.removeHandler(handler)

    status_logfile = Path(config("WORK_LOG_FILES")).joinpath(f"{batch_set_id}.validate.log")
    status_logfile.touch()
    status_handler = logging.FileHandler(status_logfile)
    status_handler.setLevel(logging.INFO)
    status_handler.setFormatter(statuslogger.StatusFormatter("%(message)s"))
    status_logger.addHandler(status_handler)

    variables = initiate_variables(destinations)
    status_logger.info(f"🟢 BEGIN VALIDATING COMPONENTS")

    onsite_medium, cloud_platform, access_platform = import_modules(destinations)

    if onsite_medium:
        raise NotImplementedError("❌ tape destinations have been deprecated")

    if cloud_platform:
        if cloud_platform.validate_connection():
            status_logger.info(f'☑️  CONNECTION SUCCESS: {config("CLOUD_PLATFORM")}')
        else:
            message = f'❌ CONNECTION FAILURE: {config("CLOUD_PLATFORM")}'
            status_logger.error(message)
            raise ConnectionError(message)

    if access_platform:
        if access_platform.validate_connection():
            status_logger.info(f'☑️  CONNECTION SUCCESS: {config("ACCESS_PLATFORM")}')
        else:
            message = f'❌ CONNECTION FAILURE: {config("ACCESS_PLATFORM")}'
            status_logger.error(message)
            raise ConnectionError(message)

    if onsite_medium or cloud_platform:
        try:
            Path(variables["source_files"]).parent.joinpath(
                config("WORK_PRESERVATION_FILES")
            ).resolve(strict=True)
        except Exception as e:
            message = f"❌ INVALID WORK_PRESERVATION_FILES DIRECTORY: {e}"
            status_logger.error(message)
            raise

    def is_archival_object_valid(component_id):
        try:
            archival_object = find_archival_object(component_id)
        except Exception as e:
            status_logger.error(e)
            logger.exception(e)
        if not access_platform:
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
                # NOTE destinations is a JSON string
                if (
                    instance["digital_object"]["_resolved"].get("file_versions")
                    and "fail" in destinations
                ):
                    message = "‼️  DIGITAL OBJECT ALREADY HAS FILE VERSIONS: [**{}**]({}/resolve/readonly?uri={})".format(
                        instance["digital_object"]["_resolved"]["title"],
                        config("ASPACE_STAFF_URL"),
                        instance["digital_object"]["ref"],
                    )
                    status_logger.error(message)
                    return False
                elif instance["digital_object"]["_resolved"].get("file_versions"):
                    message = "⚠️ EXISTING WEB ACCESS FILES WILL BE REPLACED AND DIGITAL OBJECT RECORDS WILL BE UPDATED: [**{}**]({}/resolve/readonly?uri={})".format(
                        instance["digital_object"]["_resolved"]["title"],
                        config("ASPACE_STAFF_URL"),
                        instance["digital_object"]["ref"],
                    )
                    status_logger.warning(message)
                    return True
                else:
                    # no file versions exist yet
                    return True
            else:
                # go through with creating a new digital_object
                return True
        else:
            # ensure there is a return value
            return True

    archival_object_count = 0
    file_count = 0
    unsupported_filetypes = 0
    validation_failures = 0

    for dirpath, dirnames, filenames in os.walk(variables["source_files"]):
        if not dirnames and not filenames:
            message = "❌ NO DIRECTORIES OR FILES FOUND"
            status_logger.error(message)
            raise FileNotFoundError(message)

        for filename in filenames:
            if filename in [".DS_Store", "Thumbs.db"]:
                os.remove(Path(dirpath).joinpath(filename))
            else:
                status_logger.info(f"📄 {filename}")
                if access_platform:
                    type, encoding = mimetypes.guess_type(Path(dirpath).joinpath(filename))
                    if not (type.startswith("image/") or type.endswith("/mp4")):
                        unsupported_filetypes += 1
                if not is_archival_object_valid(
                    filename.rsplit(".", maxsplit=1)[0]
                ):
                    validation_failures += 1
                archival_object_count += 1
                file_count += 1

        for dirname in dirnames:
            status_logger.info(f"📁 {dirname}")
            if not is_archival_object_valid(dirname):
                validation_failures += 1
            for dir_entry in os.scandir(Path(dirpath).joinpath(dirname)):
                if dir_entry.name in [".DS_Store", "Thumbs.db"]:
                    os.remove(dir_entry.path)
                else:
                    logger.debug(f"🐞 DIR_ENTRY.NAME: {dir_entry.name}")
                    file_count += 1
            archival_object_count += 1

        dirnames[:] = []

    if unsupported_filetypes > 0:
        message = "❌ UNSUPPORTED FILE TYPE"
        status_logger.error(message)
        raise RuntimeError(message)

    if validation_failures > 0:
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

    status_logger.info(f"🈺")


def run(destinations, batch_set_id):
    """Run Distillery."""
    status_logger = logging.getLogger("status")
    for status_logger_handler in status_logger.handlers[:]:
        if isinstance(status_logger_handler, logging.FileHandler):
            status_logger.removeHandler(status_logger_handler)

    status_logfile = Path(config("WORK_LOG_FILES")).joinpath(f"{batch_set_id}.run.log")
    status_logfile.touch()
    status_handler = logging.FileHandler(status_logfile)
    status_handler.setLevel(logging.INFO)
    status_handler.setFormatter(statuslogger.StatusFormatter("%(message)s"))
    status_logger.addHandler(status_handler)

    try:
        variables = initiate_variables(destinations)
        status_logger.info(f"🟢 BEGIN DISTILLING")
        onsite_medium, cloud_platform, access_platform = import_modules(destinations)

        if "onsite" in destinations or "cloud" in destinations:
            either_preservation_destination = True
            variables["work_preservation_files"] = (
                Path(variables["source_files"])
                .parent.joinpath(config("WORK_PRESERVATION_FILES"))
                .resolve(strict=True)
            )
        else:
            either_preservation_destination = False

        batch_directory = (
            Path(variables["source_files"])
            .parent.joinpath(config("BATCH_SETS_DIRECTORY"))
            .joinpath(batch_set_id)
        )
        batch_directory.mkdir(parents=True, exist_ok=True)
        Path(variables["source_files"]).rename(
            batch_directory.joinpath("STAGE_1_INITIAL")
        )
        batch_directory.joinpath("STAGE_2_WORKING").mkdir(parents=True, exist_ok=True)
        batch_directory.joinpath("STAGE_3_COMPLETE").mkdir(parents=True, exist_ok=True)
        Path(variables["source_files"]).mkdir()

        for dir_entry in sorted(
            os.scandir(batch_directory.joinpath("STAGE_1_INITIAL")),
            key=lambda dir_entry: dir_entry.name,
        ):
            if dir_entry.name in [".DS_Store", "Thumbs.db"]:
                os.remove(dir_entry.path)
                continue

            variables["archival_object"] = find_archival_object(
                dir_entry.name.rsplit(".", maxsplit=1)[0]
            )
            variables["arrangement"] = get_arrangement(variables["archival_object"])

            if either_preservation_destination:
                collection_data = get_collection_data(
                    variables["arrangement"]["collection_id"]
                )
                save_collection_datafile(
                    collection_data, variables["work_preservation_files"]
                )

                if cloud_platform:
                    cloud_platform.collection_level_preprocessing(
                        variables["arrangement"]["collection_id"],
                        variables["work_preservation_files"],
                    )
                    cloudDistiller = True

            working_archival_object = str(
                batch_directory.joinpath("STAGE_2_WORKING", dir_entry.name)
            )
            shutil.move(dir_entry.path, working_archival_object)

            if Path(working_archival_object).is_file():
                variables["filepaths"] = [working_archival_object]
            elif Path(working_archival_object).is_dir():
                variables["filepaths"] = [
                    f.path for f in os.scandir(working_archival_object) if f.is_file()
                ]

            if either_preservation_destination:
                archival_object_datafile_key = save_archival_object_datafile(
                    variables["arrangement"],
                    variables["archival_object"],
                    variables["work_preservation_files"],
                )
                prepare_preservation_files(variables)

            if either_preservation_destination:
                # Confirm existing or create digital_object with component_id.
                # NOTE digital_object needs to exist for digital_object_component records to be attached
                try:
                    digital_object_count = len(
                        [
                            i
                            for i in variables["archival_object"]["instances"]
                            if "digital_object" in i.keys()
                        ]
                    )
                    logger.debug(f"🐞 DIGITAL OBJECT COUNT: {digital_object_count}")
                    if digital_object_count > 1:
                        raise ValueError(
                            "❌ MULTIPLE DIGITAL OBJECTS FOUND: {}".format(
                                variables["archival_object"]["component_id"]
                            )
                        )
                    elif digital_object_count < 1:
                        # returns new archival_object with digital_object instance included
                        (
                            digital_object_uri,
                            variables["archival_object"],
                        ) = create_digital_object(variables["archival_object"])
                except:
                    logger.exception("‼️")
                    raise

                variables["current_archival_object_datafile"] = (
                    Path(variables["work_preservation_files"])
                    .joinpath(
                        archival_object_datafile_key,
                    )
                    .resolve()
                )
                logger.debug(
                    f'🐞 ARCHIVAL OBJECT DATAFILE: {variables["current_archival_object_datafile"]}'
                )

                if cloud_platform:
                    cloud_platform.process_archival_object_datafile(
                        variables
                    )

                # see https://stackoverflow.com/a/54790514 for os.walk explainer
                for dirpath, dirnames, filenames in sorted(
                    os.walk(
                        Path(variables["work_preservation_files"])
                        .joinpath(
                            get_archival_object_directory_prefix(
                                variables["arrangement"],
                                variables["archival_object"],
                            ),
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
                        variables["preservation_file_info"] = {}
                        variables["preservation_file_info"][
                            "filepath"
                        ] = filepath
                        variables["preservation_file_info"][
                            "filesize"
                        ] = filepath.stat().st_size
                        with open(filepath, "rb") as fb:
                            variables["preservation_file_info"][
                                "md5"
                            ] = hashlib.md5(fb.read())
                        if onsite_medium:
                            onsite_medium.process_digital_object_component_file(
                                variables
                            )
                        if cloud_platform:
                            cloud_platform.process_digital_object_component_file(
                                variables
                            )

                # move preservation files on each iteration because the
                # rsync transfer to tape copies the entire contents of the
                # directory; move them into an intermediate directory for
                # deletion because deleting across slow file systems can
                # result in cruft still existing on the next iteration
                if not config("WORK_STILLAGE_FILES", default=""):
                    # create a .STILLAGE directory
                    stillage_default = Path(variables["source_files"]).parent.joinpath(
                        ".STILLAGE"
                    )
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
                    .joinpath(variables["arrangement"]["collection_id"])
                    .is_dir()
                ):
                    for dir_entry in os.scandir(
                        Path(variables["work_preservation_files"]).joinpath(
                            variables["arrangement"]["collection_id"],
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
                                variables["arrangement"]["collection_id"]
                            )
                            .as_posix(),
                        )
                else:
                    shutil.move(
                        Path(variables["work_preservation_files"])
                        .joinpath(
                            variables["arrangement"]["collection_id"],
                        )
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
                        .joinpath(variables["arrangement"]["collection_id"])
                        .resolve()
                        .as_posix()
                    )
                )

            shutil.move(
                working_archival_object,
                str(batch_directory.joinpath("STAGE_3_COMPLETE", dir_entry.name)),
            )

    except Exception as e:
        status_logger.error("❌ SOMETHING WENT WRONG")
        status_logger.error(e)
        logger.exception("‼️")
        raise
    else:
        status_logger.info(f"🏁")


def alchemist_regenerate(component_id="", collection_id="", logfile=""):
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
        access_platform = importlib.import_module(config("ACCESS_PLATFORM"))
    except Exception:
        message = f'❌ UNABLE TO IMPORT MODULE: {config("ACCESS_PLATFORM")}'
        status_logger.exception(message)
        raise

    try:
        accessDistiller = access_platform.AccessPlatform()
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
                access_platform.invalidate_cloudfront_path(
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
            # regenerate files for a collection
            status_logger.info(f"🟢 BEGIN REGENERATING ITEMS FOR: {collection_id}")
            archival_object_prefixes = accessDistiller.regenerate_collection(collection_id)
            if config("ALCHEMIST_CLOUDFRONT_DISTRIBUTION_ID", default=False):
                access_platform.invalidate_cloudfront_path(
                    path=f'/{config("ALCHEMIST_URL_PREFIX")}/{collection_id}/*'
                )
            for archival_object_prefix in archival_object_prefixes:
                component_id = archival_object_prefix.split("/")[-2]
                variables["archival_object"] = find_archival_object(component_id)
                variables["arrangement"] = get_arrangement(variables["archival_object"])
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
                access_platform.invalidate_cloudfront_path(
                    path=f'/{config("ALCHEMIST_URL_PREFIX")}/{collection_id}/*'
                )
        else:
            # regenerate files for all items
            status_logger.info("🟢 BEGIN REGENERATING ALL")
            archival_object_prefixes = accessDistiller.regenerate_all()
            if config("ALCHEMIST_CLOUDFRONT_DISTRIBUTION_ID", default=False):
                access_platform.invalidate_cloudfront_path()
            for archival_object_prefix in archival_object_prefixes:
                component_id = archival_object_prefix.split("/")[-2]
                variables["archival_object"] = find_archival_object(component_id)
                variables["arrangement"] = get_arrangement(variables["archival_object"])
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
                access_platform.invalidate_cloudfront_path()
    except Exception as e:
        status_logger.error("❌ SOMETHING WENT WRONG")
        status_logger.error(e)
        logger.exception("‼️")
        raise
    else:
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


def save_digital_object_file_versions(archival_object, new_file_versions):
    for instance in archival_object["instances"]:
        if "digital_object" in instance.keys():
            # ASSUMPTION: only one digital_object exists per archival_object
            # TODO handle multiple digital_objects per archival_object
            existing_file_versions = instance["digital_object"]["_resolved"].get(
                "file_versions"
            )
            # create temporary dictionary of new_file_version values keyed by file_uri
            new_file_uri_values = {
                new_file_version["file_uri"]: new_file_version
                for new_file_version in new_file_versions
            }
            # add existing_file_version values to new_file_uri_values if file_uri not already present
            for existing_file_version in existing_file_versions:
                if existing_file_version["file_uri"] not in new_file_uri_values:
                    existing_file_version["publish"] = False
                    existing_file_version["is_representative"] = False
                    new_file_uri_values[
                        existing_file_version["file_uri"]
                    ] = existing_file_version
            # discard keys and create list of unique file_version dictionaries
            file_versions = list(new_file_uri_values.values())
            digital_object = instance["digital_object"]["_resolved"]
            digital_object["file_versions"] = file_versions
            digital_object["publish"] = True
            digital_object_post_response = update_digital_object(
                digital_object["uri"], digital_object
            ).json()


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
    #     "filename": "EXAMPLE_02_0B_056_07_0001.tiff",
    #     "filepath": "/path/to/archives/data/BATCH/EXAMPLE/EXAMPLE_02_0B_056_07_0001.tiff",
    #     "filestem": "EXAMPLE_02_0B_056_07_0001",
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
        len(f'{variables["work_preservation_files"]}/') :
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
        preservation_file_path = Path(variables["work_preservation_files"]).joinpath(
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


import argparse
import logging
import os
from pathlib import Path

# Import other necessary modules and functions here
# ...

def main():
    parser = argparse.ArgumentParser(
        description="Digital object preservation workflow for Caltech Archives."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: validate
    validate_parser = subparsers.add_parser(
        "validate", help="Validate connections, files, and data."
    )
    validate_parser.add_argument(
        "--destinations", required=True, help="JSON string of destinations."
    )
    validate_parser.add_argument(
        "--batch-set-id", required=True, help="Batch set ID for validation."
    )

    # Subcommand: run
    run_parser = subparsers.add_parser("run", help="Run the distillery process.")
    run_parser.add_argument(
        "--destinations", required=True, help="JSON string of destinations."
    )
    run_parser.add_argument(
        "--batch-set-id", required=True, help="Batch set ID for processing."
    )

    # Subcommand: regenerate
    regenerate_parser = subparsers.add_parser(
        "regenerate", help="Regenerate files for a component, collection, or all items."
    )
    regenerate_parser.add_argument(
        "--component-id", help="Component ID for regeneration."
    )
    regenerate_parser.add_argument(
        "--collection-id", help="Collection ID for regeneration."
    )
    regenerate_parser.add_argument(
        "--logfile", required=True, help="Path to the logfile."
    )

    args = parser.parse_args()

    # Handle the commands
    if args.command == "validate":
        validate(args.destinations, args.batch_set_id)
    elif args.command == "run":
        run(args.destinations, args.batch_set_id)
    elif args.command == "regenerate":
        alchemist_regenerate(
            component_id=args.component_id,
            collection_id=args.collection_id,
            logfile=args.logfile,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
