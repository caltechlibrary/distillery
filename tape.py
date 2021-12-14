# PREPARE FILES AND METADATA FOR COPYING TO TAPE STORAGE

import logging
import os

from pathlib import Path

from decouple import config

import distill

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    Path(__file__).resolve().parent.joinpath("settings.ini"),
    disable_existing_loggers=False,
)
logger = logging.getLogger("tape")


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

    # NOTE we have to assume that PROCESSING_FILES is set correctly
    stream_path = Path(config("PROCESSING_FILES")).joinpath(
        f"{collection_id}-processing"
    )

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
            stream.write("the cloud process has not run, so we need to create JP2 files")

        (
            IN_PROCESS_ORIGINAL_FILES,
            LOSSLESS_PRESERVATION_FILES,
        ) = validate_settings()

        variables["IN_PROCESS_ORIGINAL_FILES"] = IN_PROCESS_ORIGINAL_FILES.as_posix()
        variables[
            "LOSSLESS_PRESERVATION_FILES"
        ] = LOSSLESS_PRESERVATION_FILES.as_posix()

        variables["collection_directory"] = distill.get_collection_directory(
            IN_PROCESS_ORIGINAL_FILES, collection_id
        )
        variables["collection_data"] = distill.get_collection_data(collection_id)

        distill.save_collection_metadata(
            variables["collection_data"], LOSSLESS_PRESERVATION_FILES
        )

        distill.loop_over_collection_subdirectories(variables)


def process_during_files_loop(variables):
    # Save Preservation Image in local filesystem structure.
    distill.save_preservation_file(
        variables["preservation_image_data"]["filepath"],
        f'{variables["LOSSLESS_PRESERVATION_FILES"]}/{variables["preservation_image_data"]["s3key"]}',
    )


def process_during_subdirectories_loop(variables):
    """Called inside loop_over_collection_subdirectories function."""
    distill.save_folder_data(
        variables["folder_arrangement"],
        variables["folder_data"],
        variables["LOSSLESS_PRESERVATION_FILES"],
    )


def validate_settings():
    IN_PROCESS_ORIGINAL_FILES = Path(
        os.path.expanduser(config("STAGE_2_ORIGINAL_FILES"))
    ).resolve(
        strict=True
    )  # NOTE do not create missing `IN_PROCESS_ORIGINAL_FILES`
    LOSSLESS_PRESERVATION_FILES = distill.directory_setup(
        os.path.expanduser(config("LOSSLESS_PRESERVATION_FILES"))
    ).resolve(strict=True)
    return (
        IN_PROCESS_ORIGINAL_FILES,
        LOSSLESS_PRESERVATION_FILES,
    )


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
    # fmt: on
