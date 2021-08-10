# PREPARE FILES AND METADATA FOR COPYING TO TAPE STORAGE

import logging

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
    cloud: ("sending to cloud storage", "flag", "c"),
    onsite: ("preparing for onsite storage", "flag", "o"),
    access: ("publishing access copies", "flag", "a"),
    collection_id: "the Collection ID from ArchivesSpace",
):

    logger.info("📼 tape")

    # NOTE we have to assume that PROCESSING_FILES is set correctly
    stream_path = Path(config("PROCESSING_FILES")).joinpath(
        f"{collection_id}-processing"
    )

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


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
    # fmt: on
