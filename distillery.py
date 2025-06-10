# CALTECH ARCHIVES AND SPECIAL COLLECTIONS
# digital object preservation workflow

# processing functionality; see web.py for bottlepy web application

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

from decouple import config, Csv

import cascflow

cascflow.setup_logging('settings.ini')
logger = logging.getLogger('distillery')


def get_collection_data(collection_id):
    # raises an HTTPError exception if unsuccessful
    collection_uri = get_collection_uri(collection_id)
    collection_data = cascflow.archivessnake_get(collection_uri).json()
    if collection_data:
        collection_data["tree"]["_resolved"] = get_collection_tree(collection_uri)
        if collection_data["tree"]["_resolved"]:
            logger.info(
                f'☑️  ARCHIVESSPACE COLLECTION DATA RETRIEVED: {collection_data["uri"]}'
            )
            return collection_data
    else:
        raise RuntimeError(
            "There was a problem retrieving the collection data from ArchivesSpace.\n"
        )


def get_collection_uri(collection_id):
    # raises an HTTPError exception if unsuccessful
    search_results_json = cascflow.archivessnake_get(
        f'/repositories/2/find_by_id/resources?identifier[]=["{collection_id}"]'
    ).json()
    if len(search_results_json["resources"]) < 1:
        raise ValueError(
            f"No collection found in ArchivesSpace with the ID: {collection_id}\n"
        )
    return search_results_json["resources"][0]["ref"]


def get_collection_tree(collection_uri):
    # raises an HTTPError exception if unsuccessful
    collection_tree = cascflow.archivessnake_get(collection_uri + "/ordered_records").json()
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
    logger.info(f"☑️  COLLECTION DATA FILE SAVED: {filename}")
    return collection_datafile_key


def transfer_collection_datafile(collection_id, directory):
    """POST collection data to S3 bucket as a JSON file."""
    collection_datafile_key = Path(collection_id).joinpath(f"{collection_id}.json")
    collection_datafile_path = (
        Path(directory).joinpath(collection_datafile_key).resolve()
    )
    with open(collection_datafile_path, "rb") as body:
        cascflow.s3_put_object(
            config("PRESERVATION_BUCKET"),
            str(collection_datafile_key),
            body,
        )
    logger.info(
        f'☑️  COLLECTION DATAFILE UPLOADED TO S3: {config("PRESERVATION_BUCKET")}/{str(collection_datafile_key)}'
    )


def ensure_digital_object(archival_object):
    """Ensure a digital object exists for the archival object."""
    digital_object_count = len(
        [
            i
            for i in archival_object["instances"]
            if "digital_object" in i.keys()
        ]
    )
    logger.debug(f"🐞 DIGITAL OBJECT COUNT: {digital_object_count}")
    if digital_object_count > 1:
        raise ValueError(
            "❌ MULTIPLE DIGITAL OBJECTS FOUND: {}".format(
                archival_object["component_id"]
            )
        )
    elif digital_object_count < 1:
        digital_object_uri, archival_object = cascflow.create_digital_object(archival_object)
    else:
        digital_object_uri = archival_object["instances"][0]["digital_object"]["ref"]

    # returns new archival_object with digital_object instance included
    return digital_object_uri, archival_object


def transfer_archival_object_datafile(archival_object_datafile_key: str, build_directory: tempfile.TemporaryDirectory):
    """POST archival object data to S3 bucket as a JSON file."""
    current_archival_object_datafile = Path(build_directory.name).joinpath(archival_object_datafile_key).resolve()
    with open(current_archival_object_datafile, "rb") as body:
        cascflow.s3_put_object(
            config("PRESERVATION_BUCKET"),
            archival_object_datafile_key,
            body,
        )
    logger.info(
        f'☑️  ARCHIVAL OBJECT DATAFILE UPLOADED TO S3: {config("PRESERVATION_BUCKET")}/{archival_object_datafile_key}'
    )


def process_digital_object_component_file(variables):
    """transfer file to S3; create ArchivesSpace record"""
    if not transfer_digital_object_component_file(variables):
        logger.warning()
        return
    variables["file_uri_scheme"] = "s3"
    variables["file_uri_host"] = config("PRESERVATION_BUCKET")
    if not save_digital_object_component_record(variables):
        logger.warning()
        return


def transfer_digital_object_component_file(variables):
    """s5cmd cp digital object component file to S3 bucket"""
    preservation_file_key = str(variables["preservation_file_info"]["filepath"])[
        len(f'{variables["work_preservation_files"]}/') :
    ]
    for _attempt in range(3):
        try:
            subprocess.run(
                [
                    "s5cmd",
                    "cp",
                    "--concurrency",
                    "20",
                    "--metadata",
                    f'md5={variables["preservation_file_info"]["md5"].hexdigest()}',
                    variables["preservation_file_info"]["filepath"],
                    f's3://{config("PRESERVATION_BUCKET")}/{preservation_file_key}',
                ],
                env={
                    "AWS_ACCESS_KEY_ID": config("DISTILLERY_AWS_ACCESS_KEY_ID"),
                    "AWS_SECRET_ACCESS_KEY": config("DISTILLERY_AWS_SECRET_ACCESS_KEY"),
                },
                check=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            if _attempt > 1:
                logger.exception(f"❌ S5CMD STDOUT: {e.stdout}")
                logger.exception(f"❌ S5CMD STDERR: {e.stderr}")
                raise
        else:
            logger.info(
                "☑️ DIGITAL OBJECT COMPONENT FILE UPLOADED TO S3: {}/{}".format(
                    config("PRESERVATION_BUCKET"), preservation_file_key
                )
            )
    return variables["preservation_file_info"]["md5"].hexdigest()


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
    find_by_id_response = cascflow.archivessnake_get(
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
    find_by_id_response = cascflow.archivessnake_get(
        f"/repositories/2/find_by_id/digital_object_components?digital_object_id[]={digital_object_component_id}"
    )
    find_by_id_response.raise_for_status()
    if len(find_by_id_response.json()["digital_objects"]) < 1:
        return None
    if len(find_by_id_response.json()["digital_objects"]) > 1:
        raise ValueError(
            f"Multiple digital_objects found with digital_object_component_id: {digital_object_component_id}"
        )
    digital_object_get_response = cascflow.archivessnake_get(
        f"{find_by_id_response.json()['digital_objects'][0]['ref']}"
    )
    digital_object_get_response.raise_for_status()
    return digital_object_get_response.json()


def save_digital_object_component_record(variables):
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
        digital_object_tree = cascflow.archivessnake_get(
            digital_objects[0]["digital_object"]["ref"] + "/tree/root"
        ).json()
        logger.debug(f"🐞 DIGITAL_OBJECT_TREE: {digital_object_tree}")
        if not digital_object_tree.get("precomputed_waypoints"):
            # NOTE without precomputed_waypoints we assume there are no
            # digital_object_components
            logger.debug(
                "🐞 NO PRECOMPUTED_WAYPOINTS; CREATING DIGITAL_OBJECT_COMPONENT"
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
            "🐞 NO DIGITAL_OBJECT_COMPONENTS_SUMMARY; CREATING DIGITAL_OBJECT_COMPONENT"
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
                digital_object_component = cascflow.archivessnake_get(
                    digital_object_component_summary["uri"]
                ).json()
                file_version = construct_file_version(variables)
                digital_object_component["file_versions"].append(file_version)
                cascflow.archivessnake_post(
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
        digital_object_uri, archival_object = cascflow.create_digital_object(
            variables["archival_object"]
        )
        digital_object_component["digital_object"] = {"ref": digital_object_uri}
    digital_object_component["file_versions"] = [construct_file_version(variables)]
    return digital_object_component


def create_digital_object_component(variables):
    digital_object_component = construct_digital_object_component(variables)
    digital_object_component_post_response = cascflow.archivessnake_post(
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
            logger.info(f"☑️  ORIGINAL FILE COPIED: {preservation_file_key}")


def validate_digital_files_for_preservation(validation_results: dict) -> None:
    valid_archival_objects = validation_results.get("valid_archival_objects", [])
    invalid_archival_objects = validation_results.get("invalid_archival_objects", [])
    nested_directories = validation_results.get("nested_directories", [])
    empty_directories = validation_results.get("empty_directories", [])
    file_count = validation_results.get("file_count", 0)
    ## messaging
    if valid_archival_objects:
        for valid_archival_object in valid_archival_objects:
            logger.info(f"🉑 {valid_archival_object}")
        logger.info(f"🗂 ARCHIVAL OBJECT COUNT: {len(valid_archival_objects)}")
        logger.info(f"📄 FILE COUNT: {file_count}")
    ## validation and messaging
    if file_count == 0:
        message = "❌ NO FILES FOUND"
        logger.error(message)
        raise FileNotFoundError(message)
    if invalid_archival_objects:
        message = "❌ INVALID ARCHIVAL OBJECTS FOUND"
        logger.error(message)
        for invalid_archival_object in invalid_archival_objects:
            logger.error(f"❌ {invalid_archival_object}")
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


def main():
    parser = argparse.ArgumentParser(
        description="Digital object preservation workflow for Caltech Archives."
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
    execute_parser = subparsers.add_parser("execute", help="Execute the preservation process.")
    execute_parser.add_argument(
        "--context", required=True, help="String. Supply a volume name."
    )

    args = parser.parse_args()

    # handle the commands
    if args.command == "validate":
        validation_results = cascflow.validate_digital_files(args.context)
        validate_digital_files_for_preservation(validation_results)
    elif args.command == "execute":
        build_directory = tempfile.TemporaryDirectory()
        batch_set_id = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
        for batch_directory, stage_2_path_obj, filepaths, archival_object, arrangement in cascflow.execute(args.context, batch_set_id, "preservation"):
            collection_data = get_collection_data(arrangement["collection_id"])
            save_collection_datafile(collection_data, build_directory.name)
            transfer_collection_datafile(arrangement["collection_id"], build_directory.name)
            archival_object_datafile_key = save_archival_object_datafile(arrangement, archival_object, build_directory.name)
            prepare_preservation_files({
                "filepaths": filepaths,
                "archival_object": archival_object,
                "arrangement": arrangement,
                "work_preservation_files": build_directory.name
            })
            # NOTE returned archival_object will include any created digital_object instance
            digital_object_uri, archival_object = ensure_digital_object(archival_object)
            transfer_archival_object_datafile(archival_object_datafile_key, build_directory)
            # WALK EVERY FILE IN THE ARCHIVAL OBJECT DIRECTORY
            for dirpath, dirnames, filenames in os.walk(Path(build_directory.name).joinpath(get_archival_object_directory_prefix(arrangement, archival_object)).resolve()):
                for filename in filenames:
                    filepath = Path(dirpath).joinpath(filename)
                    if filename in config("FILES_TO_REMOVE", default=None, cast=Csv()):
                        filepath.unlink()
                        continue
                    if filepath.suffix == ".json" and filepath.name.startswith(filepath.stem):
                        # skip JSON files that are not digital object component files
                        continue
                    process_digital_object_component_file({
                        "preservation_file_info": {
                            "filepath": filepath,
                            "md5": hashlib.md5(filepath.read_bytes()),
                            "filesize": filepath.stat().st_size,
                        },
                        "work_preservation_files": build_directory.name,
                        "archival_object": archival_object,
                        "arrangement": arrangement,
                    })
            cascflow.move_to_stage_3(stage_2_path_obj, batch_directory)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
