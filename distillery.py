# file: distillery.py
# DIGITAL OBJECT PRESERVATION WORKFLOW

import argparse
import datetime
import hashlib
import json
import logging
import os
import random
import shutil
import string
import subprocess
import tempfile

from pathlib import Path
from decouple import Config, Csv, RepositoryIni  # pypi: python-decouple

import cascflow

parser = argparse.ArgumentParser(
    description="Digital object preservation workflow for Caltech Archives."
)
parser.add_argument(
    "--settings",
    default=None,
    help="Path to settings.ini file (optional). Defaults to working directory.",
)
subparsers = parser.add_subparsers(dest="command", required=True)

# subcommand: validate
validate_parser = subparsers.add_parser(
    "validate", help="Validate connections, files, and data."
)
validate_parser.add_argument(
    "--context", required=True, help="String. Supply a volume name."
)

# subcommand: execute
execute_parser = subparsers.add_parser(
    "execute", help="Execute the preservation process."
)
execute_parser.add_argument(
    "--context", required=True, help="String. Supply a volume name."
)

args = None
config = cascflow.config
logger = logging.getLogger("distillery")

DISTILLERY_SETTINGS_SPEC = [
    {"key": "ARCHIVESSPACE_API_URL", "kind": "url"},
    {"key": "ARCHIVESSPACE_USERNAME", "kind": "string"},
    {"key": "ARCHIVESSPACE_PASSWORD", "kind": "string"},
    {"key": "ARCHIVESSPACE_STAFF_URL", "kind": "url"},
    {"key": "ARCHIVESSPACE_REPOSITORY_ID", "kind": "int", "required": False},
    {"key": "AWS_ACCESS_KEY_ID", "kind": "string"},
    {"key": "AWS_SECRET_ACCESS_KEY", "kind": "string"},
    {"key": "S3_BUCKET", "kind": "string"},
    {"key": "RESOLVER_BUCKET", "kind": "string"},
    {"key": "OUR_NMA", "kind": "url"},
    {"key": "OUR_NAAN", "kind": "string"},
    {"key": "OUR_SHOULDER", "kind": "string"},
    {"key": "ARK_BLADE_LENGTH", "kind": "int", "required": False},
    {"key": "ABSOLUTE_MOUNT_PARENT", "kind": "directory"},
    {"key": "RELATIVE_SOURCE_DIRECTORY", "kind": "string"},
    {"key": "RELATIVE_BATCH_DIRECTORY", "kind": "string"},
    {"key": "FILES_TO_REMOVE", "kind": "csv", "required": False},
    {"key": "S5CMD_CMD", "kind": "executable"},
]


def mint_ark_for_archival_object(archival_object: dict) -> str | None:
    import arkminter

    nma = config("OUR_NMA", default="").strip()
    naan = config("OUR_NAAN", default="").strip()
    if not nma or not naan:
        logger.warning(
            "⚠️ OUR_NMA OR OUR_NAAN NOT CONFIGURED; SKIPPING ARK MINTING: %s",
            archival_object.get("component_id", ""),
        )
        return None

    # Validate existing ARK (returns None if doesn't exist or is invalid)
    existing_ark = arkminter.get_existing_our_ark(archival_object, naan)
    if existing_ark:
        logger.info(
            "⊘ SKIPPING ARK MINTING (EXISTING ARK FOUND): %s → %s",
            archival_object.get("component_id", ""),
            existing_ark,
        )
        return None

    s3_client = cascflow.establish_s3_connection()
    if s3_client is None:
        raise RuntimeError("Failed to establish S3 connection")

    redirect_location = (
        config("ARCHIVESSPACE_STAFF_URL").rstrip("/")
        + f"/resolve/readonly?uri={archival_object['uri']}"
    )

    # Mint stub ARK with minimal metadata (title only)
    minted_ark = arkminter.mint_ark(
        bucket_name=config("RESOLVER_BUCKET"),
        redirect_target=redirect_location,
        nma=nma,
        naan=naan,
        title=str(archival_object.get("title", "")),
        shoulder=config("OUR_SHOULDER", default=""),
        blade_length=config("ARK_BLADE_LENGTH", default=6, cast=int),
        s3_client=s3_client,
    )

    # Set external_ark_url on archival_object
    archival_object["external_ark_url"] = f"https://{nma}/{minted_ark}"

    logger.info(f"✳️ ARK MINTED: {minted_ark}")
    return minted_ark


def get_collection_data(collection_id):
    # raises an HTTPError exception if unsuccessful
    collection_uri = get_collection_uri(collection_id)
    collection_data = cascflow.archivesspace_get(collection_uri).json()
    if collection_data:
        collection_data["tree"]["_resolved"] = get_collection_tree(collection_uri)
        if collection_data["tree"]["_resolved"]:
            logger.info(
                f"☑️ COLLECTION: [{collection_data['title']}]({config('ARCHIVESSPACE_STAFF_URL').rstrip('/')}/resolve/readonly?uri={collection_data['uri']})"
            )
            return collection_data
    else:
        raise RuntimeError(
            "There was a problem retrieving the collection data from ArchivesSpace.\n"
        )


def get_collection_uri(collection_id):
    # raises an HTTPError exception if unsuccessful
    repository_id = config("ARCHIVESSPACE_REPOSITORY_ID", default="2")
    search_results_json = cascflow.archivesspace_get(
        f'/repositories/{repository_id}/find_by_id/resources?identifier[]=["{collection_id}"]'
    ).json()
    if len(search_results_json["resources"]) < 1:
        raise ValueError(
            f"No collection found in ArchivesSpace with the ID: {collection_id}\n"
        )
    return search_results_json["resources"][0]["ref"]


def get_collection_tree(collection_uri):
    # raises an HTTPError exception if unsuccessful
    collection_tree = cascflow.archivesspace_get(
        collection_uri + "/ordered_records"
    ).json()
    if collection_tree:
        return collection_tree
    else:
        raise RuntimeError(
            "There was a problem retrieving the collection tree from ArchivesSpace.\n"
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
    logger.debug(f"☑️  COLLECTION DATA FILE SAVED: {filename}")
    return collection_datafile_key


def transfer_collection_datafile(collection_id, directory):
    """POST collection data to S3 bucket as a JSON file."""
    collection_datafile_key = Path(collection_id).joinpath(f"{collection_id}.json")
    collection_datafile_path = (
        Path(directory).joinpath(collection_datafile_key).resolve()
    )
    with open(collection_datafile_path, "rb") as f:
        file_bytes = f.read()
        cascflow.s3_put_object(
            config("S3_BUCKET"),
            str(collection_datafile_key),
            file_bytes,
        )
    logger.info(
        f"☑️  COLLECTION DATAFILE UPLOADED TO S3: {config('S3_BUCKET')}/{str(collection_datafile_key)}"
    )


def ensure_digital_object(archival_object):
    """Ensure a digital object exists for the archival object."""
    digital_object_count = len(
        [i for i in archival_object["instances"] if "digital_object" in i.keys()]
    )
    logger.debug(f"🐞 DIGITAL OBJECT COUNT: {digital_object_count}")
    if digital_object_count > 1:
        raise ValueError(
            "❌ MULTIPLE DIGITAL OBJECTS FOUND: {}".format(
                archival_object["component_id"]
            )
        )
    elif digital_object_count < 1:
        digital_object_uri, archival_object = cascflow.create_digital_object(
            archival_object
        )
    else:
        digital_object_uri = archival_object["instances"][0]["digital_object"]["ref"]

    # returns new archival_object with digital_object instance included
    return digital_object_uri, archival_object


def transfer_archival_object_datafile(
    archival_object_datafile_key: str, build_directory: tempfile.TemporaryDirectory
):
    """POST archival object data to S3 bucket as a JSON file."""
    current_archival_object_datafile = (
        Path(build_directory.name).joinpath(archival_object_datafile_key).resolve()
    )
    with open(current_archival_object_datafile, "rb") as f:
        file_bytes = f.read()
        cascflow.s3_put_object(
            config("S3_BUCKET"),
            archival_object_datafile_key,
            file_bytes,
        )
    logger.info(
        f"☑️  ARCHIVAL OBJECT DATAFILE UPLOADED TO S3: {config('S3_BUCKET')}/{archival_object_datafile_key}"
    )


def chunk_hash(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        # read the file in 8MB chunks to avoid memory issues with large files
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h


def process_digital_object_component_file(variables, build_directory):
    """transfer file to S3; create ArchivesSpace record"""
    if not transfer_digital_object_component_file(variables, build_directory):
        logger.warning("⚠️ FAILED TO TRANSFER DIGITAL OBJECT COMPONENT FILE")
        return
    variables["file_uri_scheme"] = "s3"
    variables["file_uri_host"] = config("S3_BUCKET")
    if not save_digital_object_component_record(variables, build_directory):
        logger.warning("⚠️ FAILED TO SAVE DIGITAL OBJECT COMPONENT RECORD")
        return


def transfer_digital_object_component_file(variables, build_directory):
    """s5cmd cp digital object component file to S3 bucket"""
    filepath = variables["preservation_file_info"]["filepath"]
    build_dir_path = Path(build_directory.name)
    # calculate relative path (build_directory.name is already resolved)
    preservation_file_key = str(filepath.resolve().relative_to(build_dir_path))
    for _attempt in range(3):
        s5cmd_args = [
            config("S5CMD_CMD"),
            "cp",
            "--concurrency",
            "20",
            "--metadata",
            f"md5={variables['preservation_file_info']['md5'].hexdigest()}",
            str(variables["preservation_file_info"]["filepath"]),
            f"s3://{config('S3_BUCKET')}/{preservation_file_key}",
        ]
        try:
            subprocess.run(
                s5cmd_args,
                env={
                    "AWS_ACCESS_KEY_ID": config("AWS_ACCESS_KEY_ID"),
                    "AWS_SECRET_ACCESS_KEY": config("AWS_SECRET_ACCESS_KEY"),
                },
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            if _attempt > 1:
                logger.exception(
                    f"❌ S5CMD FAILED AFTER {_attempt + 1} ATTEMPTS (EXIT CODE: {e.returncode})"
                )
                raise
            else:
                logger.warning(
                    f"⚠️ S5CMD ATTEMPT {_attempt + 1} FAILED, RETRYING... (EXIT CODE: {e.returncode})"
                )
        else:
            logger.info(
                "☑️ DIGITAL OBJECT COMPONENT FILE UPLOADED TO S3: {}/{}".format(
                    config("S3_BUCKET"), preservation_file_key
                )
            )
            return variables["preservation_file_info"]["md5"].hexdigest()
    # if we get here, all attempts failed
    return None


def get_crockford_characters(n=4):
    return "".join(random.choices("abcdefghjkmnpqrstvwxyz" + string.digits, k=n))


def get_crockford_id():
    return get_crockford_characters() + "_" + get_crockford_characters()


# TODO rename to get_filepath_components
def get_file_parts(filepath: Path):
    file_parts = {}
    file_parts["filepath"] = filepath.as_posix()
    file_parts["filename"] = file_parts["filepath"].split("/")[-1]
    file_parts["filestem"] = file_parts["filename"].split(".")[0]
    file_parts["extension"] = file_parts["filename"].split(".")[-1]
    file_parts["sequence"] = file_parts["filestem"].split("_")[-1]
    file_parts["crockford_id"] = get_crockford_id()
    return file_parts


def get_archival_object_datafile_key(prefix, archival_object):
    """Return the key (file path) of an archival object datafile."""
    # NOTE the prefix includes a trailing slash
    return f"{prefix}{archival_object['component_id']}.json"


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
            f"{archival_object['component_id']}--{archival_object_title}",
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
    repository_id = config("ARCHIVESSPACE_REPOSITORY_ID", default="2")
    find_by_id_response = cascflow.archivesspace_get(
        f"/repositories/{repository_id}/find_by_id/digital_objects?digital_object_id[]={digital_object_digital_object_id}"
    )
    find_by_id_response.raise_for_status()
    if len(find_by_id_response.json()["digital_objects"]) < 1:
        return None
    if len(find_by_id_response.json()["digital_objects"]) > 1:
        raise ValueError(
            f"Multiple digital_objects found with digital_object_id: {digital_object_digital_object_id}"
        )
    return find_by_id_response.json()["digital_objects"][0]["ref"]


def save_digital_object_component_record(variables, build_directory):
    if variables.get("file_uri_scheme") is None:
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
        digital_object_tree = cascflow.archivesspace_get(
            digital_objects[0]["digital_object"]["ref"] + "/tree/root"
        ).json()
        logger.debug(f"🐞 DIGITAL_OBJECT_TREE: {digital_object_tree}")
        if not digital_object_tree.get("precomputed_waypoints"):
            # NOTE without precomputed_waypoints we assume there are no
            # digital_object_components
            logger.debug(
                "🐞 NO PRECOMPUTED_WAYPOINTS; CREATING DIGITAL_OBJECT_COMPONENT"
            )
            return create_digital_object_component(variables, build_directory)
    else:
        raise RuntimeError(f"❌ NO DIGITAL_OBJECTS FOUND: {variables}")
    digital_object_components_summary = [
        _ for _ in digital_object_tree["precomputed_waypoints"][""]["0"]
    ]
    logger.debug(f"🐞 DIGITAL_OBJECT_COMPONENTS: {digital_object_components_summary}")
    if not digital_object_components_summary:
        logger.debug(
            "🐞 NO DIGITAL_OBJECT_COMPONENTS_SUMMARY; CREATING DIGITAL_OBJECT_COMPONENT"
        )
        return create_digital_object_component(variables, build_directory)
    if variables["preservation_file_info"]["filepath"].parent.name not in [
        _["label"] for _ in digital_object_components_summary
    ]:
        logger.debug(
            "🐞 DIGITAL_OBJECT_COMPONENT LABEL NOT FOUND: {}; CREATING DIGITAL_OBJECT_COMPONENT".format(
                variables["preservation_file_info"]["filepath"].parent.name
            )
        )
        return create_digital_object_component(variables, build_directory)
    # NOTE assuming there is a file_uri_summary in each digital_object_component
    for digital_object_component_summary in digital_object_components_summary:
        file_uri_summary = digital_object_component_summary["file_uri_summary"].split(
            ","
        )
        for file_uri in file_uri_summary:
            if file_uri.strip().startswith(
                f"{variables['file_uri_scheme']}://"
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
                digital_object_component = cascflow.archivesspace_get(
                    digital_object_component_summary["uri"]
                ).json()
                file_version = construct_file_version(variables, build_directory)
                digital_object_component["file_versions"].append(file_version)
                cascflow.archivesspace_post(
                    digital_object_component["uri"], digital_object_component
                )
                logger.info(
                    f"☑️  DIGITAL OBJECT COMPONENT UPDATED: {digital_object_component['uri']}"
                )
                return digital_object_component["uri"]


def construct_digital_object_component(variables, build_directory):
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
        digital_object_uri, archival_object = cascflow.create_digital_object(
            variables["archival_object"]
        )
        digital_object_component["digital_object"] = {"ref": digital_object_uri}
    digital_object_component["file_versions"] = [
        construct_file_version(variables, build_directory)
    ]
    return digital_object_component


def create_digital_object_component(variables, build_directory):
    digital_object_component = construct_digital_object_component(
        variables, build_directory
    )
    repository_id = config("ARCHIVESSPACE_REPOSITORY_ID", default="2")
    digital_object_component_post_response = cascflow.archivesspace_post(
        f"/repositories/{repository_id}/digital_object_components",
        digital_object_component,
    )
    logger.info(
        f"✳️ DIGITAL OBJECT COMPONENT CREATED: [{digital_object_component['label']}]({config('ARCHIVESSPACE_STAFF_URL').rstrip('/')}/resolve/readonly?uri={digital_object_component_post_response.json()['uri']})"
    )
    return digital_object_component_post_response.json()["uri"]


def construct_file_version(variables, build_directory):
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
    # calculate relative path (build_directory.name is already resolved)
    filepath = variables["preservation_file_info"]["filepath"]
    build_dir_path = Path(build_directory.name)
    file_key = str(filepath.resolve().relative_to(build_dir_path))
    file_version["file_uri"] = (
        f"{variables['file_uri_scheme']}://{variables['file_uri_host']}/{file_key}"
    )
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
    logger.debug(f"☑️  ARCHIVAL OBJECT DATA SAVED: {filename}")
    return archival_object_datafile_key


def prepare_preservation_files(variables, build_directory):
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
        preservation_file_path = Path(build_directory.name).joinpath(
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
            logger.debug(f"☑️  ORIGINAL FILE COPIED: {preservation_file_key}")


def validate_settings_for_preservation(settings_validation_results: dict) -> None:
    missing = settings_validation_results.get("missing", [])
    invalid = settings_validation_results.get("invalid", [])
    if missing:
        message = "❌ REQUIRED SETTINGS NOT SET"
        logger.error(message)
        for setting in missing:
            logger.error(f"❌ {setting['key']}: {setting['detail']}")
    if invalid:
        message = "❌ INVALID SETTINGS VALUES"
        logger.error(message)
        for setting in invalid:
            logger.error(f"❌ {setting['key']}: {setting['detail']}")
    if missing or invalid:
        raise RuntimeError("❌ SETTINGS VALIDATION FAILED")
    logger.debug("✅ ALL REQUIRED SETTINGS ARE SET AND USABLE")


def validate_digital_files_for_preservation(validation_results: dict) -> None:
    # Return total bytes for one top-level archival-object entry.
    def get_entry_size_bytes(entry: Path) -> int:
        if entry.is_file():
            return entry.stat().st_size
        if entry.is_dir():
            return sum(
                filepath.stat().st_size
                for filepath in entry.rglob("*")
                if filepath.is_file()
            )
        return 0

    # Compare each top-level entry size against free bytes on the temp directory mount,
    # since that is where the build_directory will be created during execute.
    def validate_available_storage_for_archival_objects(source_path: Path) -> None:
        # Warn/fail thresholds based on ratio-to-free-space and projected free-space floors.
        warning_threshold_ratio = 0.80
        fail_threshold_ratio = 0.90
        staging_overhead_ratio = 1.05
        warning_free_floor_bytes = 20 * (1024**3)
        fail_free_floor_bytes = 5 * (1024**3)
        temp_dir = tempfile.gettempdir()
        free_bytes = shutil.disk_usage(temp_dir).free
        logger.info(
            f"💽 AVAILABLE DISK SPACE FOR TEMP BUILD DIRECTORY ({temp_dir}): {free_bytes / (1024**3):.2f} GiB"
        )

        oversized_entries = []
        near_capacity_entries = []

        for entry in sorted(source_path.iterdir(), key=lambda p: p.name):
            if not entry.exists():
                continue
            entry_size_bytes = get_entry_size_bytes(entry)
            if entry_size_bytes == 0:
                continue
            usage_ratio = (
                entry_size_bytes / free_bytes if free_bytes > 0 else float("inf")
            )
            # Account for staging/log overhead when estimating post-copy free space.
            projected_bytes_needed = int(entry_size_bytes * staging_overhead_ratio)
            projected_free_bytes = free_bytes - projected_bytes_needed
            if (
                usage_ratio >= fail_threshold_ratio
                or projected_free_bytes < fail_free_floor_bytes
            ):
                oversized_entries.append(
                    (entry.name, entry_size_bytes, usage_ratio, projected_free_bytes)
                )
            elif (
                usage_ratio >= warning_threshold_ratio
                or projected_free_bytes < warning_free_floor_bytes
            ):
                near_capacity_entries.append(
                    (entry.name, entry_size_bytes, usage_ratio, projected_free_bytes)
                )

        for (
            entry_name,
            entry_size_bytes,
            usage_ratio,
            projected_free_bytes,
        ) in near_capacity_entries:
            logger.warning(
                "⚠️ ARCHIVAL OBJECT APPROACHES AVAILABLE SPACE: "
                f"{entry_name} ({entry_size_bytes / (1024**3):.2f} GiB, "
                f"{usage_ratio * 100:.1f}% of free space, "
                f"projected free: {projected_free_bytes / (1024**3):.2f} GiB)"
            )

        if oversized_entries:
            message = "❌ INSUFFICIENT LOCAL SPACE FOR ONE OR MORE ARCHIVAL OBJECTS"
            logger.error(message)
            for (
                entry_name,
                entry_size_bytes,
                usage_ratio,
                projected_free_bytes,
            ) in oversized_entries:
                logger.error(
                    f"❌ {entry_name} requires {entry_size_bytes / (1024**3):.2f} GiB "
                    f"({usage_ratio * 100:.1f}% of free space, "
                    f"projected free: {projected_free_bytes / (1024**3):.2f} GiB)"
                )
            raise RuntimeError(message)

    eligible_archival_objects = validation_results.get("eligible_archival_objects", {})
    ineligible_archival_objects = validation_results.get(
        "ineligible_archival_objects", []
    )
    nested_directories = validation_results.get("nested_directories", [])
    empty_directories = validation_results.get("empty_directories", [])
    file_count = validation_results.get("file_count", 0)
    source_path = validation_results.get("source_path")
    ## messaging
    if eligible_archival_objects:
        logger.info(
            f"{next(iter(eligible_archival_objects))}{', …' if len(eligible_archival_objects) > 1 else ''}"
        )
        for component_id in eligible_archival_objects.keys():
            logger.info(f"🉑 {component_id}")
        logger.info(f"🗂 ARCHIVAL OBJECT COUNT: {len(eligible_archival_objects)}")
        logger.info(f"📄 FILE COUNT: {file_count}")
    ## validation and messaging
    if file_count == 0:
        message = "❌ NO FILES FOUND"
        logger.error(message)
        raise FileNotFoundError(message)
    if ineligible_archival_objects:
        message = "❌ INELIGIBLE ARCHIVAL OBJECTS FOUND"
        logger.error(message)
        for ineligible_archival_object in ineligible_archival_objects:
            logger.error(f"❌ {ineligible_archival_object}")
        raise RuntimeError(message)
    if nested_directories:
        message = "❌ NESTED DIRECTORIES FOUND"
        logger.error(message)
        for nested_directory in nested_directories:
            logger.error(f"❌ {nested_directory}")
        raise RuntimeError(message)
    if empty_directories:
        message = "❌ EMPTY DIRECTORIES FOUND"
        logger.error(message)
        for empty_directory in empty_directories:
            logger.error(f"❌ {empty_directory}")
        raise RuntimeError(message)
    if isinstance(source_path, Path):
        validate_available_storage_for_archival_objects(source_path)


def main():
    global args, config, logger
    args = parser.parse_args()
    # set up settings path and logging
    if args.settings is not None:
        settings_path = Path(args.settings).expanduser().resolve()
    else:
        settings_path = Path(__file__).parent.joinpath("settings.ini").resolve()
    if not settings_path.exists():
        raise FileNotFoundError(f"Settings file not found: {settings_path}")
    # configure cascflow to use our settings.ini file
    config_instance = Config(RepositoryIni(settings_path.as_posix()))
    cascflow.set_config(config_instance)
    # reassign module-level config to use the cascflow config instance with our settings
    config = cascflow.config

    cascflow.setup_logging(str(settings_path))
    logger = logging.getLogger("distillery")
    # handle the commands
    if args.command == "validate":
        settings_validation_results = cascflow.validate_settings(DISTILLERY_SETTINGS_SPEC)
        validate_settings_for_preservation(settings_validation_results)
        validation_results = cascflow.validate_digital_files(args.context)
        validate_digital_files_for_preservation(validation_results)
    elif args.command == "execute":
        batch_set_id = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
        for (
            batch_directory,
            stage_2_path_obj,
            filepaths,
            archival_object,
            arrangement,
        ) in cascflow.execute(args.context, batch_set_id, "preservation"):
            build_directory = tempfile.TemporaryDirectory()
            # resolve the build directory path once to handle symlink differences across platforms
            build_directory.name = str(Path(build_directory.name).resolve())
            try:
                collection_data = get_collection_data(arrangement["collection_id"])
                save_collection_datafile(collection_data, build_directory.name)
                transfer_collection_datafile(
                    arrangement["collection_id"], build_directory.name
                )
                archival_object_datafile_key = save_archival_object_datafile(
                    arrangement, archival_object, build_directory.name
                )
                prepare_preservation_files(
                    {
                        "filepaths": filepaths,
                        "archival_object": archival_object,
                        "arrangement": arrangement,
                    },
                    build_directory,
                )
                # NOTE returned archival_object will include any created digital_object instance
                digital_object_uri, archival_object = ensure_digital_object(
                    archival_object
                )
                transfer_archival_object_datafile(
                    archival_object_datafile_key, build_directory
                )
                # Mint ARK (or skip if already exists); this also sets external_ark_url on archival_object
                mint_ark_for_archival_object(archival_object)
                # POST archival_object back to ArchivesSpace to persist external_ark_url if it was set
                if archival_object.get("external_ark_url"):
                    cascflow.archivesspace_post(archival_object["uri"], archival_object)
                    logger.debug(
                        f"☑️  ARCHIVAL OBJECT UPDATED WITH external_ark_url: {archival_object['uri']}"
                    )
                # COLLECT AND SORT ALL FILES IN THE ARCHIVAL OBJECT DIRECTORY
                archival_object_dir = (
                    Path(build_directory.name)
                    .joinpath(
                        get_archival_object_directory_prefix(
                            arrangement, archival_object
                        )
                    )
                    .resolve()
                )
                # collect all files recursively
                all_files = []
                for filepath in archival_object_dir.rglob("*"):
                    if filepath.is_file():
                        all_files.append(filepath)
                # sort files by their parent directory name for consistent processing order
                sorted_files = sorted(all_files, key=lambda f: f.parent.name)
                logger.debug(
                    f"🐞 SORTED FILES: {[f'{f.parent.name}/{f.name}' for f in sorted_files]}"
                )
                for filepath in sorted_files:
                    filename = filepath.name
                    logger.debug(
                        f"🐞 PROCESSING FILE: {filepath.parent.name}/{filename}"
                    )
                    files_to_remove = config("FILES_TO_REMOVE", default="", cast=Csv())
                    if not isinstance(files_to_remove, list):
                        files_to_remove = []
                    if filename in files_to_remove:
                        filepath.unlink()
                        continue
                    # TODO: this is an assumption that needs to be monitored for exceptions
                    if filepath.suffix == ".json" and filepath.name.startswith(
                        filepath.stem
                    ):
                        # skip JSON files that are not digital object component files
                        continue
                    process_digital_object_component_file(
                        {
                            "preservation_file_info": {
                                "filepath": filepath,
                                "md5": chunk_hash(filepath),
                                "filesize": filepath.stat().st_size,
                            },
                            "archival_object": archival_object,
                            "arrangement": arrangement,
                        },
                        build_directory,
                    )
                cascflow.move_to_stage_3(stage_2_path_obj, batch_directory)
            finally:
                build_directory.cleanup()
    else:
        parser.print_help()


if __name__ == "__main__":
    try:
        main()
    except Exception as exception:
        logger.error(f"❌ UNEXPECTED ERROR: {exception}")
        raise
