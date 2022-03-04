# PREPARE FILES AND METADATA FOR COPYING TO TAPE STORAGE

import logging
import os
import shutil
import subprocess
import tempfile
import time
import urllib
from datetime import date, datetime
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

    logger.info("📼 tape")

    variables = {}

    variables["cloud"] = cloud
    variables["onsite"] = onsite
    variables["access"] = access
    variables["collection_id"] = collection_id

    # NOTE we have to assume that STATUS_FILES is set correctly
    stream_path = Path(
        f'{config("WORK_NAS_APPS_MOUNTPOINT")}/{config("NAS_STATUS_FILES_RELATIVE_PATH")}'
    ).joinpath(f"{collection_id}-processing")

    variables["stream_path"] = stream_path.as_posix()

    if not onsite:
        message = "❌ tape.py script was initiated without onsite being selected"
        logger.error(message)
        with open(stream_path, "a") as stream:
            stream.write(message)
        raise RuntimeError(message)

    if cloud:
        # the cloud process has run, so the JP2 files will exist
        logger.info("the cloud process has run, so the JP2 files will exist")
        with open(stream_path, "a") as stream:
            stream.write("the cloud process has run, so the JP2 files will exist")
    else:
        # the cloud process has not run, so we need to create JP2 files
        logger.info("the cloud process has not run, so we need to create JP2 files")
        with open(stream_path, "a") as stream:
            stream.write(
                "the cloud process has not run, so we need to create JP2 files"
            )

        (
            WORKING_ORIGINAL_FILES,
            WORK_LOSSLESS_PRESERVATION_FILES,
        ) = validate_settings()

        variables["WORKING_ORIGINAL_FILES"] = WORKING_ORIGINAL_FILES.as_posix()
        variables[
            "WORK_LOSSLESS_PRESERVATION_FILES"
        ] = WORK_LOSSLESS_PRESERVATION_FILES.as_posix()

        # TODO test TAPE mounts
        if not nas_is_mounted():
            with open(stream_path, "a") as stream:
                stream.write("🤖  WE ARE GOING TO TRY TO MOUNT THE NAS\n")
            mount_nas()

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
        # in the end we have our preservation directory structure with
        # files and JSON metadata
        distill.loop_over_collection_subdirectories(variables)

        # NOTE the tape-specific steps are
        # - make sure the mounted tape has capacity for the current files
        # - copy the current files to the tape
        # - save container information in ArchivesSpace

        # get the (approximate) size of the collection directory in LOSSLESS_PRESERVATION_FILES
        collection_directory_bytes = get_directory_bytes(
            f'{variables["WORK_LOSSLESS_PRESERVATION_FILES"]}/{variables["collection_id"]}'
        )
        print(f"‼️{str(collection_directory_bytes)}‼️")
        with open(stream_path, "a") as stream:
            stream.write(
                f"collection_directory_bytes: {str(collection_directory_bytes)}\n"
            )
        # get the indicator and available capacity of current tape
        tape_indicator = get_tape_indicator()
        print(f"‼️{tape_indicator}‼️")
        with open(stream_path, "a") as stream:
            stream.write(f"tape_indicator: {tape_indicator}\n")
        # NOTE output from tape_server connection is a string
        tape_bytes = tape_server(
            f'{config("TAPE_PYTHON3_CMD")} -c \'import shutil; total, used, free = shutil.disk_usage("{config("TAPE_LTO_MOUNTPOINT")}"); print(total, free)\'',
        ).strip()
        tape_total_bytes = tuple(map(int, tape_bytes.split(" ")))[0]
        tape_free_bytes = tuple(map(int, tape_bytes.split(" ")))[1]
        print(f"‼️{tape_total_bytes}‼️")
        with open(stream_path, "a") as stream:
            stream.write(f"tape_total_bytes: {tape_total_bytes}\n")
        print(f"‼️{tape_free_bytes}‼️")
        with open(stream_path, "a") as stream:
            stream.write(f"tape_free_bytes: {tape_free_bytes}\n")
        # TODO calculate whether collection directory will fit on current tape
        tape_capacity_buffer = tape_total_bytes * 0.01  # 1% for tape index
        if not tape_free_bytes - collection_directory_bytes > tape_capacity_buffer:
            # TODO unmount tape
            # TODO send mail to LIT
            # TODO send mail to Archives
            # TODO create mechanism to start this up after new tape inserted
            #   OR reset original files so the whole process gets redone
            with open(stream_path, "a") as stream:
                stream.write("THIS DOES NOT FIT ON THE TAPE")
        # TODO rsync to tape

        # TODO subprocess this and print dots...
        # TODO make a note everywhere that printing dots is simply for UI output

        logger.info(f"‼️rsync begin: {datetime.now()}")
        with open(stream_path, "a") as stream:
            stream.write("🎬 begin copying files to tape\n")
        rsync_to_tape(stream_path)  # TODO handle failure
        with open(stream_path, "a") as stream:
            stream.write("copying files to tape complete\n")
        logger.info(f"‼️rsync end: {datetime.now()}")
        # TODO create UI for adding top containers in bulk to ArchivesSpace records

        with open(stream_path, "a") as stream:
            stream.write("✅ end tape process\n")


def rsync_to_tape(stream_path):
    """Ensure NAS is mounted and copy collection directory tree to tape."""
    def process_output(line):
        with open(stream_path, "a") as f:
            if line.strip():
                f.write(line)
    def perform_rsync():
        # NOTE LTFS will not save group, permission, or time attributes
        output = tape_server(
            config("TAPE_RSYNC_CMD"),
            "-rv",
            "--exclude=.DS_Store",
            f'{config("TAPE_NAS_ARCHIVES_MOUNTPOINT")}/{config("NAS_LOSSLESS_PRESERVATION_FILES_RELATIVE_PATH")}/',
            config("TAPE_LTO_MOUNTPOINT"),
            _out=process_output, _bg=True,
        )
        print(f"‼️{output.exit_code}‼️")
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
    # TODO make platform indpendent; macOS and Linux have different mount options
    # NOTE using urllib.parse.quote() to URL-encode special characters
    # tape_server("source", config("TAPE_NAS_SECRETS"))
    # tape_server(
    #     config("TAPE_NAS_MOUNT_CMD"),
    #     f'//"$TAPE_NAS_USER":"$TAPE_NAS_PASS"@{config("NAS_IP_ADDRESS")}/{config("NAS_SHARE")}',
    #     config("TAPE_NAS_ARCHIVES_MOUNTPOINT"),
    #     _env={"TAPE_NAS_USER": config("TAPE_NAS_USER"), "TAPE_NAS_PASS": urllib.parse.quote(config("TAPE_NAS_PASS"))},
    # )
    # TODO create local tmpdir
    work_mount_nas_tmpdir = tempfile.mkdtemp()
    # TODO loosen WORK tmpdir permissions
    # os.chmod(work_mount_nas_tmpdir, 0o777)
    # TODO create local tmp script file
    with open(f"{work_mount_nas_tmpdir}/distillery_tape_mount_nas.sh", "w") as f:
        f.write("#!/bin/bash\n")
        f.write(f'{config("TAPE_NAS_MOUNT_CMD")} //{config("TAPE_NAS_USER")}:{urllib.parse.quote(config("TAPE_NAS_PASS"))}@{config("NAS_IP_ADDRESS")}/{config("NAS_SHARE")} {config("TAPE_NAS_ARCHIVES_MOUNTPOINT")}\n')
    # TODO create tmpdir on TAPE server
    # tape_mount_nas_tmpdir = tape_server("mktemp", "-d", "${TMPDIR:-/tmp}/tmp.XXXXXXXXX")
    tape_mount_nas_tmpdir = tape_server("mktemp", "-d").strip()  # macOS
    # TODO rsync local tmp script file to tmpdir on TAPE server
    rsync_output = subprocess.run([config("WORK_RSYNC_CMD"), f"{work_mount_nas_tmpdir}/distillery_tape_mount_nas.sh", f"{config('TAPE_SSH_USER')}@{config('TAPE_SSH_HOST')}:{tape_mount_nas_tmpdir}/distillery_tape_mount_nas.sh"])
    # rsync_output = tape_server(
    #     config("TAPE_RSYNC_CMD"),
    #     "-r",
    #     f"{work_mount_nas_tmpdir}/distillery_tape_mount_nas.sh",
    #     f"{tape_mount_nas_tmpdir}/distillery_tape_mount_nas.sh",
    # )
    # print(f"‼️{rsync_output.exit_code}‼️")
    # TODO run script with tape_server() sh wrapper
    try:
        tape_server("/bin/bash", f"{tape_mount_nas_tmpdir}/distillery_tape_mount_nas.sh")
    except sh.ErrorReturnCode as e:
        print("❌  COULD NOT MOUNT THE NAS ON THE TAPE SERVER")
        raise e


def process_during_files_loop(variables):
    # Save Preservation Image in local filesystem structure.
    distill.save_preservation_file(
        variables["preservation_image_data"]["filepath"],
        f'{variables["WORK_LOSSLESS_PRESERVATION_FILES"]}/{variables["preservation_image_data"]["s3key"]}',
    )


def process_during_subdirectories_loop(variables):
    """Called inside loop_over_collection_subdirectories function."""
    distill.save_folder_data(
        variables["folder_arrangement"],
        variables["folder_data"],
        variables["WORK_LOSSLESS_PRESERVATION_FILES"],
    )


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
