# NOTE separate code that generates a preview report on the web from
# the main processing code

import importlib
import logging
import logging.config
import mimetypes
import os
from pathlib import Path

from asnake.client import ASnakeClient
from decouple import config

import distillery

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.ini"),
    disable_existing_loggers=True,
)
logger = logging.getLogger("preview")

# TODO do we need a class? https://stackoverflow.com/a/16502408/4100024
asnake_client = ASnakeClient(
    baseurl=config("ASPACE_API_URL"),
    username=config("ASPACE_USERNAME"),
    password=config("ASPACE_PASSWORD"),
)
asnake_client.authorize()


def main(
    cloud: ("sending to cloud storage", "flag", "c"),  # type: ignore
    onsite: ("preparing for onsite storage", "flag", "o"),  # type: ignore
    access: ("publishing access copies", "flag", "a"),  # type: ignore
    collection_id: "the Collection ID from ArchivesSpace",  # type: ignore
):
    logger.info(f"☑️  RUNNING PREVIEW CHECKS FOR: {collection_id}")
    variables = {}

    if variables.get("onsite"):
        variables["onsite_medium"].preview(variables)


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
