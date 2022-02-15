# PREPARE FILES AND METADATA FOR COPYING TO TAPE STORAGE

import logging
import os
import shutil
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
    f"-p{config('TAPE_SSH_PORT')}",
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
                f"collection_directory_bytes: {str(collection_directory_bytes)}"
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
        print(f"‼️rsync begin: {datetime.now()}")
        rsync_to_tape()  # TODO handle failure
        print(f"‼️rsync end: {datetime.now()}")
        # TODO create UI for adding top containers in bulk to ArchivesSpace records

        with open(stream_path, "a") as stream:
            stream.write("end tape process")


def rsync_to_tape():
    """Ensure NAS is mounted and copy collection directory tree to tape."""
    if nas_is_mounted():
        # NOTE LTFS will not save group, permission, or time attributes
        output = tape_server(
            config("TAPE_RSYNC_CMD"),
            "-r",
            "--exclude=.DS_Store",
            f'{config("TAPE_NAS_ARCHIVES_MOUNTPOINT")}/{config("NAS_LOSSLESS_PRESERVATION_FILES_RELATIVE_PATH")}/',
            config("TAPE_LTO_MOUNTPOINT"),
        )
        print(f"‼️{output.exit_code}‼️")
        return
    else:
        mount_nas()
        # NOTE LTFS will not save group, permission, or time attributes
        output = tape_server(
            config("TAPE_RSYNC_CMD"),
            "-r",
            "--exclude=.DS_Store",
            f'{config("TAPE_NAS_ARCHIVES_MOUNTPOINT")}/{config("NAS_LOSSLESS_PRESERVATION_FILES_RELATIVE_PATH")}/',
            config("TAPE_LTO_MOUNTPOINT"),
        )
        print(f"‼️{output.exit_code}‼️")
        return


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
    """Returns boolean True or False."""
    # NOTE is_mounted is set as a string
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
    tape_server(
        "mount",
        "-t",
        "smbfs",
        f'//{config("TAPE_NAS_USER")}:{urllib.parse.quote(config("TAPE_NAS_PASS"))}@{config("NAS_IP_ADDRESS")}/{config("NAS_SHARE")}',
        config("TAPE_NAS_ARCHIVES_MOUNTPOINT"),
    )


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
