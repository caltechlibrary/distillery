# PREPARE FILES AND METADATA FOR COPYING TO TAPE STORAGE

import logging
import os
import tempfile
import urllib
from datetime import date
from pathlib import Path

import sh
from decouple import config

import distillery

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    Path(__file__).resolve().parent.joinpath("settings.ini"),
    disable_existing_loggers=False,
)
logger = logging.getLogger("tape")
validation_logger = logging.getLogger("validation")


# NOTE known_hosts file of user on WORK server must include TAPE server keys
# `ssh-keyscan -H $TAPE_SSH_HOST >> ~/.ssh/known_hosts`
tape_server = sh.ssh.bake(
    "-o",
    "IdentitiesOnly=yes",
    "-i",
    f'{config("TAPE_SSH_AUTHORIZED_KEY")}',
    "-p",
    f"{config('TAPE_SSH_PORT')}",
    f"{config('TAPE_SSH_USER')}@{config('TAPE_SSH_HOST')}",
)


def validate_connection():
    """If WORK server can successfully SSH into TAPE server."""
    try:
        # attempt an SSH connection; will raise on failure
        tape_server_connection = tape_server()
        logger.info(f"📼 TAPE SERVER CONNECTION SUCCESS: {tape_server}")
        tape_indicator = get_tape_indicator()
        logger.info(f"📼 TAPE INDICATOR: {tape_indicator}")
    except:
        logger.exception(f"❌ TAPE SERVER CONNECTION FAILURE: {tape_server}")
        return False
    else:
        return True


def collection_level_preprocessing(collection_id, work_preservation_files):
    """Run before any files are moved or records are created."""
    pass


def transfer_derivative_collection(variables):
    """Transfer PRESERVATION_FILES/CollectionID directory as a whole to tape."""
    # Calculate whether the collection_id directory will fit on the current tape.
    collection_id_directory_bytes = distillery.get_directory_bytes(
        "/".join(
            [
                config("WORK_PRESERVATION_FILES").rstrip("/"),
                variables["arrangement"]["collection_id"],
            ]
        )
    )
    logger.info(f"🔢 BYTECOUNT OF PRESERVATION FILES: {collection_id_directory_bytes}")
    # NOTE output from tape_server connection is a string formatted like:
    # `5732142415872 5690046283776`
    tape_bytes = tape_server(
        f'{config("TAPE_PYTHON3_CMD")} -c \'import shutil; total, used, free = shutil.disk_usage("{config("TAPE_LTO_MOUNTPOINT")}"); print(total, free)\'',
    ).strip()
    # convert the string to a tuple and get the parts
    tape_total_bytes = tuple(map(int, tape_bytes.split(" ")))[0]
    tape_free_bytes = tuple(map(int, tape_bytes.split(" ")))[1]
    logger.info(f"🔢 FREE BYTES ON TAPE: {tape_free_bytes}")
    tape_capacity_buffer = tape_total_bytes * 0.01  # reserve 1% for tape index
    if not tape_free_bytes - collection_id_directory_bytes > tape_capacity_buffer:
        # TODO unmount tape
        # TODO send mail to LIT
        # TODO send mail to Archives
        # TODO create mechanism to start this up after new tape inserted
        #   OR reset original files so the whole process gets redone
        message = "❌ the set of preservation files will not fit on the current tape"
        logger.error(message)
        # with open(stream_path, "a") as stream:
        #     stream.write(message)
        raise RuntimeError(message)

    # Copy LOSSLESS_PRESERVATION_FILES to tape using rsync.
    rsync_to_tape(variables)  # TODO handle failure


def process_archival_object_datafile(variables):
    ensure_top_container(variables)


def ensure_top_container(variables):
    """Create or confirm existence of 'Tape' Top Container for Archival Object."""
    # Read from the INDICATOR file on the mounted tape.
    variables["tape_indicator"] = get_tape_indicator()

    # Check ArchivesSpace archival_object for an existing top_container with the
    # same type and indicator values.
    if tape_container_attached(
        variables["archival_object"], variables["tape_indicator"]
    ):
        return

    # Set up and add a container instance to the ArchivesSpace archival_object.
    top_container = {}
    # indicator is required
    top_container["indicator"] = variables["tape_indicator"]
    # /container_profiles/5 is LTO-7 tape
    top_container["container_profile"] = {"ref": "/container_profiles/5"}
    top_container["type"] = "Tape"
    # create via post
    top_containers_post_response = distillery.archivessnake_post(
        "/repositories/2/top_containers", top_container
    )
    top_container_uri = top_containers_post_response.json()["uri"]
    logger.info(f"✳️  TOP CONTAINER CREATED: {top_container_uri}")
    # set up a container instance to add to the archival_object
    container_instance = {
        "instance_type": "mixed_materials",  # per policy # TODO set up new type
        "sub_container": {"top_container": {"ref": top_container_uri}},
    }
    # add container instance to archival_object
    variables["archival_object"]["instances"].append(container_instance)
    # post updated archival_object
    distillery.archivessnake_post(
        variables["archival_object"]["uri"], variables["archival_object"]
    )
    logger.info(f'☑️  ARCHIVAL OBJECT UPDATED: {variables["archival_object"]["uri"]}')


def process_digital_object_component_file(variables):
    """create ArchivesSpace record"""
    variables["file_uri_scheme"] = "tape"
    variables["file_uri_host"] = variables["tape_indicator"]
    if not distillery.save_digital_object_component_record(variables):
        logger.warning()
        return


def rsync_to_tape(variables):
    """Ensure NAS is mounted and copy collection directory tree to tape."""

    def process_output(line):
        if line.strip().startswith(variables["arrangement"]["collection_id"]):
            validation_logger.info(f"TAPE: {line.strip()}")
        # with open(variables["stream_path"], "a") as f:
        #     if line.strip():
        #         f.write(line)

    def perform_rsync():
        # NOTE LTFS will not save group, permission, or time attributes
        # NOTE running with `_bg=True` and `_out` to process each line of output
        logger.info("⏳ PERFORMING RSYNC TO TAPE...")
        rsync_process = tape_server(
            config("TAPE_RSYNC_CMD"),
            "-rv",
            "--exclude=.DS_Store",
            f'{config("TAPE_NAS_ARCHIVES_MOUNTPOINT")}/{config("NAS_LOSSLESS_PRESERVATION_FILES_RELATIVE_PATH")}/',
            config("TAPE_LTO_MOUNTPOINT"),
            _out=process_output,
            _bg=True,
        )
        rsync_process.wait()
        return

    if nas_is_mounted():
        perform_rsync()
    else:
        mount_nas()
        perform_rsync()


def get_tape_indicator():
    """Ensure tape is mounted and return INDICATOR string."""
    if tape_is_mounted():
        return read_tape_indicator()
    else:
        mount_tape()
        return read_tape_indicator()


def read_tape_indicator():
    """"Find and return INDICATOR string."""
    try:
        tape_indicator = (
            tape_server(
                "find",
                config("TAPE_LTO_MOUNTPOINT"),
                "-type",
                "f",
                "-name",
                "'*.indicator'",
            )
            .strip()
            .split(".")[0]
            .split("/")[-1]
        )
        logger.info(f"☑️  TAPE INDICATOR FOUND: {tape_indicator}")
        return tape_indicator
    except:
        logger.exception("❌  TAPE INDICATOR NOT FOUND")
        raise


def nas_is_mounted():
    """Returns boolean True or False for NAS mounted on TAPE server."""
    # NOTE is_mounted is set as a string from the tape_server() command
    is_mounted = tape_server(
        f'{config("TAPE_PYTHON3_CMD")} -c \'import os; print(os.path.ismount("{config("TAPE_NAS_ARCHIVES_MOUNTPOINT")}"))\''
    ).strip()
    if is_mounted == "True":
        logger.info(f'☑️  NAS IS MOUNTED: {config("TAPE_NAS_ARCHIVES_MOUNTPOINT")}')
        return True
    else:
        return False


def tape_is_mounted():
    """Returns boolean True or False."""
    # NOTE is_mounted is set as a string
    is_mounted = tape_server(
        f'{config("TAPE_PYTHON3_CMD")} -c \'import os; print(os.path.ismount("{config("TAPE_LTO_MOUNTPOINT")}"))\''
    ).strip()
    if is_mounted == "True":
        logger.info(f'☑️  TAPE IS MOUNTED: {config("TAPE_LTO_MOUNTPOINT")}')
        return True
    else:
        return False


def mount_tape():
    logger.info(f'🤞 MOUNTING TAPE: {config("TAPE_LTO_MOUNTPOINT")}')
    tape_server(config("TAPE_LTFS_CMD"), config("TAPE_LTO_MOUNTPOINT"))


def mount_nas():
    """Runs a script to mount the NAS on the TAPE server."""
    # TODO make platform indpendent; macOS and Linux have different mount options
    # create a temporary directory on the WORK server
    work_mount_nas_tmpdir = tempfile.mkdtemp()
    # create a local script file inside the WORK server temporary directory
    # NOTE using urllib.parse.quote() to URL-encode special characters
    with open(f"{work_mount_nas_tmpdir}/distillery_tape_mount_nas.sh", "w") as f:
        f.write("#!/bin/bash\n")
        f.write(
            f'{config("TAPE_NAS_MOUNT_CMD")} //{config("TAPE_NAS_USER")}:{urllib.parse.quote(config("TAPE_NAS_PASS"))}@{config("NAS_IP_ADDRESS")}/{config("NAS_SHARE")} {config("TAPE_NAS_ARCHIVES_MOUNTPOINT")}\n'
        )
    # create a temporary directory on TAPE server
    tape_mount_nas_tmpdir = tape_server("mktemp", "-d").strip()  # macOS
    # rsync WORK server script file to temporary directory on TAPE server
    try:
        rsync_cmd = sh.Command(config("WORK_RSYNC_CMD"))
        rsync_cmd(
            f"{work_mount_nas_tmpdir}/distillery_tape_mount_nas.sh",
            f"{config('TAPE_SSH_USER')}@{config('TAPE_SSH_HOST')}:{tape_mount_nas_tmpdir}/distillery_tape_mount_nas.sh",
        )
    except sh.ErrorReturnCode as e:
        print("❌  COULD NOT RSYNC THE SCRIPT FILE TO THE TAPE SERVER")
        raise e
    # run script on TAPE server via sh
    # TODO create TAPE_BASH_CMD in settings.ini
    try:
        tape_server(
            "/bin/bash", f"{tape_mount_nas_tmpdir}/distillery_tape_mount_nas.sh"
        )
    except sh.ErrorReturnCode as e:
        print("❌  COULD NOT MOUNT THE NAS ON THE TAPE SERVER")
        raise e


def process_during_original_files_loop(variables):
    """Called inside create_derivative_files function."""
    # # Save Preservation Image in local filesystem structure.
    # distillery.save_preservation_file(
    #     variables["preservation_file_info"]["filepath"],
    #     f'{variables["WORK_PRESERVATION_FILES"]}/{variables["preservation_file_info"]["s3key"]}',
    # ) # TODO pass only variables
    if variables["step"] == "save_tape_info_to_archivesspace":
        # Add file versions.
        pass


def process_during_original_structure_loop(variables):
    """Called inside create_derivative_structure function."""
    pass


def tape_container_attached(archival_object, top_container_indicator):
    """Returns True when top_container type is Tape and indicator matches."""
    for instance in archival_object["instances"]:
        if "sub_container" in instance.keys():
            if (
                instance["sub_container"]["top_container"]["_resolved"]["type"]
                == "Tape"
                and instance["sub_container"]["top_container"]["_resolved"]["indicator"]
                == top_container_indicator
            ):
                logger.info(
                    f'☑️  TAPE TOP CONTAINER EXISTS: {instance["sub_container"]["top_container"]["ref"]}'
                )
                return True
