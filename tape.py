# PREPARE FILES AND METADATA FOR COPYING TO TAPE STORAGE

import logging
import os
import tempfile
import urllib
from datetime import date
from pathlib import Path

import sh
from decouple import config

import distill

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    Path(__file__).resolve().parent.joinpath("settings.ini"),
    disable_existing_loggers=False,
)
logger = logging.getLogger("tape")

tape_server = sh.ssh.bake(
    f"{config('TAPE_SSH_USER')}@{config('TAPE_SSH_HOST')}",
    "-p",
    f"{config('TAPE_SSH_PORT')}",
    "-o",
    "StrictHostKeyChecking=no",
    "-i",
    f'{config("TAPE_SSH_KEY")}',
)


def main(
    cloud: ("sending to cloud storage", "flag", "c"),  # type: ignore
    onsite: ("preparing for onsite storage", "flag", "o"),  # type: ignore
    access: ("publishing access copies", "flag", "a"),  # type: ignore
    collection_id: "the Collection ID from ArchivesSpace",  # type: ignore
):

    variables = {}

    variables["cloud"] = cloud
    variables["onsite"] = onsite
    variables["access"] = access
    variables["collection_id"] = collection_id

    # NOTE we have to assume that STATUS_FILES is set correctly
    stream_path = Path(config("WORK_NAS_APPS_MOUNTPOINT")).joinpath(
        config("NAS_STATUS_FILES_RELATIVE_PATH"), f"{collection_id}-processing"
    )

    variables["stream_path"] = stream_path.as_posix()

    if not onsite:
        message = "❌ tape.py script was initiated without onsite being selected"
        logger.error(message)
        with open(stream_path, "a") as stream:
            stream.write(message)
        raise RuntimeError(message)

    (
        WORKING_ORIGINAL_FILES,
        WORK_LOSSLESS_PRESERVATION_FILES,
    ) = validate_settings()

    variables["WORKING_ORIGINAL_FILES"] = WORKING_ORIGINAL_FILES.as_posix()
    variables[
        "WORK_LOSSLESS_PRESERVATION_FILES"
    ] = WORK_LOSSLESS_PRESERVATION_FILES.as_posix()

    # verify TAPE NAS mount
    if not nas_is_mounted():
        with open(stream_path, "a") as stream:
            stream.write("🤖  WE ARE GOING TO TRY TO MOUNT THE NAS\n")
        mount_nas()
    # verify TAPE tape mount
    # TODO confirm ASSUMPTION that any tape in the drive can be used
    if not tape_is_mounted():
        with open(stream_path, "a") as stream:
            stream.write("🤖  WE ARE GOING TO TRY TO MOUNT THE TAPE\n")
        mount_tape()

    # TODO distill.create_preservation_structure()

    # TODO possibly rename; "collection_directory" is becoming ambiguous
    #      "original_files_working_directory" maybe
    variables["collection_directory"] = distill.get_collection_directory(
        WORKING_ORIGINAL_FILES, collection_id
    )
    variables["collection_data"] = distill.get_collection_data(collection_id)

    # NOTE this script is running on WORK and this metadata is being saved
    # to the NAS mounted on WORK
    distill.save_collection_metadata(
        variables["collection_data"], WORK_LOSSLESS_PRESERVATION_FILES
    )

    # NOTE this loop contains the following sequence:
    # - process_during_subdirectories_loop()
    # - distill.loop_over_digital_files()
    # - process_during_files_loop()
    # in the end we have our preservation directory structure containing files
    # and JSON metadata
    variables["step"] = "prepare_preservation_files"
    distill.loop_over_collection_subdirectories(variables)

    # NOTE the tape-specific steps are
    # - make sure the mounted tape has capacity for the current files
    # - copy the current files to the tape
    # - save container information in ArchivesSpace

    # get the (approximate) size of the collection directory in LOSSLESS_PRESERVATION_FILES
    collection_directory_bytes = get_directory_bytes(
        f'{variables["WORK_LOSSLESS_PRESERVATION_FILES"]}/{variables["collection_id"]}'
    )
    with open(stream_path, "a") as stream:
        stream.write(f"collection_directory_bytes: {str(collection_directory_bytes)}\n")

    # get the indicator and available capacity of current tape
    variables["tape_indicator"] = get_tape_indicator()
    with open(stream_path, "a") as stream:
        stream.write(f'tape_indicator: {variables["tape_indicator"]}\n')

    # NOTE output from tape_server connection is a string formatted like:
    # `5732142415872 5690046283776`
    tape_bytes = tape_server(
        f'{config("TAPE_PYTHON3_CMD")} -c \'import shutil; total, used, free = shutil.disk_usage("{config("TAPE_LTO_MOUNTPOINT")}"); print(total, free)\'',
    ).strip()
    # convert the string to a tuple and get the parts
    tape_total_bytes = tuple(map(int, tape_bytes.split(" ")))[0]
    tape_free_bytes = tuple(map(int, tape_bytes.split(" ")))[1]
    with open(stream_path, "a") as stream:
        stream.write(f"tape_total_bytes: {tape_total_bytes}\n")
    with open(stream_path, "a") as stream:
        stream.write(f"tape_free_bytes: {tape_free_bytes}\n")

    # calculate whether collection directory will fit on current tape
    tape_capacity_buffer = tape_total_bytes * 0.01  # reserve 1% for tape index
    if not tape_free_bytes - collection_directory_bytes > tape_capacity_buffer:
        # TODO unmount tape
        # TODO send mail to LIT
        # TODO send mail to Archives
        # TODO create mechanism to start this up after new tape inserted
        #   OR reset original files so the whole process gets redone
        with open(stream_path, "a") as stream:
            stream.write("THIS DOES NOT FIT ON THE TAPE")

    # rsync LOSSLESS_PRESERVATION_FILES to tape
    with open(stream_path, "a") as stream:
        stream.write("🎬 begin copying files to tape\n")
    rsync_to_tape(variables)  # TODO handle failure
    with open(stream_path, "a") as stream:
        stream.write("copying files to tape complete\n")

    # TODO create UI for adding top containers in bulk to ArchivesSpace records

    # run `distill.loop_over_collection_subdirectories(variables)` again and
    # pass a `step` variable that tells the loop function which conditional code
    # to execute; this step will add a top container instance to the archival
    # object and add file versions for each digital object component
    variables["step"] = "save_tape_info_to_archivesspace"
    distill.loop_over_collection_subdirectories(variables)

    with open(stream_path, "a") as stream:
        stream.write("✅ end tape process\n")


def rsync_to_tape(variables):
    """Ensure NAS is mounted and copy collection directory tree to tape."""

    def process_output(line):
        with open(variables["stream_path"], "a") as f:
            if line.strip():
                f.write(line)

    def perform_rsync():
        # NOTE LTFS will not save group, permission, or time attributes
        # NOTE running with `_bg=True` and `_out` to process each line of output
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


def get_directory_bytes(directory):
    """Returns the total bytes of all files under a given directory."""
    return sum(f.stat().st_size for f in Path(directory).glob("**/*") if f.is_file())


def get_tape_indicator():
    """Ensure tape is mounted and return contents of existing or created INDICATOR file."""
    if tape_is_mounted():
        return try_tape_indicator()
    else:
        mount_tape()
        return try_tape_indicator()


def try_tape_indicator():
    """Return contents of existing or created INDICATOR file."""
    try:
        tape_indicator = read_tape_indicator()
        return tape_indicator
    except sh.ErrorReturnCode_1:
        write_tape_indicator()
        tape_indicator = read_tape_indicator()
        return tape_indicator
    except Exception as e:
        raise e


def read_tape_indicator():
    """"Return contents of existing INDICATOR file."""
    tape_indicator = tape_server(
        f'{config("TAPE_PYTHON3_CMD")} -c \'with open("{config("TAPE_LTO_MOUNTPOINT")}/INDICATOR") as f: print(f.read())\''
    ).strip()
    return tape_indicator


def write_tape_indicator():
    """Write INDICATOR file to tape with contents of: YYYYMMDD_01"""
    tape_indicator = f'{date.today().strftime("%Y%m%d")}_01'
    # ASSUMPTION no other indicator with same date exists
    # TODO check if indicator value already exists
    # (unlikely, as it would require filling up an entire 6 TB tape in a day)
    tape_server(
        f'{config("TAPE_PYTHON3_CMD")} -c \'with open("{config("TAPE_LTO_MOUNTPOINT")}/INDICATOR", "w") as f: f.write("{tape_indicator}\\n")\''
    )


def nas_is_mounted():
    """Returns boolean True or False for NAS mounted on TAPE server."""
    # NOTE is_mounted is set as a string from the tape_server() command
    is_mounted = tape_server(
        f'{config("TAPE_PYTHON3_CMD")} -c \'import os; print(os.path.ismount("{config("TAPE_NAS_ARCHIVES_MOUNTPOINT")}"))\''
    ).strip()
    if is_mounted == "True":
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
        return True
    else:
        return False


def mount_tape():
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


def process_during_files_loop(variables):
    """Called inside loop_over_digital_files function."""
    if variables["step"] == "prepare_preservation_files":
        # Save Preservation Image in local filesystem structure.
        distill.save_preservation_file(
            variables["preservation_image_data"]["filepath"],
            f'{variables["WORK_LOSSLESS_PRESERVATION_FILES"]}/{variables["preservation_image_data"]["s3key"]}',
        )


def process_during_subdirectories_loop(variables):
    """Called inside loop_over_collection_subdirectories function."""
    if variables["step"] == "prepare_preservation_files":
        distill.save_folder_data(
            variables["folder_arrangement"],
            variables["folder_data"],
            variables["WORK_LOSSLESS_PRESERVATION_FILES"],
        )
    if variables["step"] == "save_tape_info_to_archivesspace":
        # Ignore identical existing top container.
        if tape_container_attached(
            variables["folder_data"], variables["tape_indicator"]
        ):
            return

        # Add container instance.
        top_container = {}
        # indicator is required
        top_container["indicator"] = variables["tape_indicator"]
        # /container_profiles/5 is LTO-7 tape
        top_container["container_profile"] = {"ref": "/container_profiles/5"}
        top_container["type"] = "Tape"
        # create via post
        top_containers_post_response = distill.archivessnake_post(
            "/repositories/2/top_containers", top_container
        )
        top_container_uri = top_containers_post_response.json()["uri"]
        # set up a container instance to add to the archival object
        container_instance = {
            "instance_type": "mixed_materials",  # per policy
            "sub_container": {"top_container": {"ref": top_container_uri}},
        }
        # add container instance to archival object
        variables["folder_data"]["instances"].append(container_instance)
        # post updated archival object
        distill.archivessnake_post(
            variables["folder_data"]["uri"], variables["folder_data"]
        )


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
                return True


def validate_settings():
    WORKING_ORIGINAL_FILES = Path(
        os.path.expanduser(config("WORKING_ORIGINAL_FILES"))
    ).resolve(
        strict=True
    )  # NOTE do not create missing `WORKING_ORIGINAL_FILES`
    WORK_LOSSLESS_PRESERVATION_FILES = distill.directory_setup(
        os.path.expanduser(
            f'{config("WORK_NAS_ARCHIVES_MOUNTPOINT")}/{config("NAS_LOSSLESS_PRESERVATION_FILES_RELATIVE_PATH")}'
        )
    ).resolve(strict=True)
    return (
        WORKING_ORIGINAL_FILES,
        WORK_LOSSLESS_PRESERVATION_FILES,
    )


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
    # fmt: on
