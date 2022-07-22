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
    if onsite and config("ONSITE_MEDIUM"):
        # Import a module named the same as the ONSITE_MEDIUM setting.
        variables["onsite_medium"] = importlib.import_module(config("ONSITE_MEDIUM"))
        variables["onsite"] = onsite
        # TODO create init function that confirms everything is set to continue
    if cloud and config("CLOUD_PLATFORM"):
        # Import a module named the same as the CLOUD_PLATFORM setting.
        variables["cloud_platform"] = importlib.import_module(config("CLOUD_PLATFORM"))
        variables["cloud"] = cloud
        # TODO create init function that confirms everything is set to continue
    if access and config("ACCESS_PLATFORM"):
        # Import a module named the same as the ACCESS_PLATFORM setting.
        variables["access_platform"] = importlib.import_module(
            config("ACCESS_PLATFORM")
        )
        variables["access"] = access
        # TODO create init function that confirms everything is set to continue
    variables["collection_id"] = collection_id

    variables["stream_path"] = stream_path = Path(
        config("WORK_NAS_APPS_MOUNTPOINT")
    ).joinpath(config("NAS_STATUS_FILES_RELATIVE_PATH"), f"{collection_id}-processing")

    # Report on directories found.
    try:
        distillery.confirm_collection_directory(
            config("INITIAL_ORIGINAL_FILES"), collection_id
        )
    except FileNotFoundError as e:
        message = f"❌ {collection_id} directory not found in {config('INITIAL_ORIGINAL_FILES')}\n"
        with open(stream_path, "a") as stream:
            stream.write(message)
        # re-raise the exception because we cannot continue without the files
        raise

    # Report on subdirectories found and filecount.
    initial_original_subdirectorycount = 0
    initial_original_filecount = 0
    for dirpath, dirnames, filenames in os.walk(
        os.path.join(config("INITIAL_ORIGINAL_FILES"), collection_id)
    ):
        if dirnames:
            for dirname in dirnames:
                initial_original_subdirectorycount += 1
                with open(stream_path, "a") as stream:
                    stream.write(f"📁 {collection_id}/{dirname}\n")
        if filenames:
            for filename in filenames:
                type, encoding = mimetypes.guess_type(Path(dirpath).joinpath(filename))
                # NOTE additional mimetypes TBD
                if type == "image/tiff":
                    initial_original_filecount += 1
    if not initial_original_subdirectorycount:
        message = f"❌ No subdirectories found under {collection_id} directory in {config('INITIAL_ORIGINAL_FILES')}\n"
        with open(stream_path, "a") as stream:
            stream.write(message)
        raise FileNotFoundError(message)
    if initial_original_filecount:
        logger.info(f"☑️  FILE COUNT: {initial_original_filecount}")
        with open(stream_path, "a") as stream:
            stream.write(
                f"📄 Number of files to be processed: {initial_original_filecount}\n"
            )
    else:
        message = f"❌ No files found for {collection_id} that can be processed by Distillery\n"
        with open(stream_path, "a") as stream:
            stream.write(message)
        raise FileNotFoundError(message)

    variables["collection_data"] = distillery.get_collection_data(
        variables["collection_id"]
    )

    message = f'✅ Collection found in ArchivesSpace: {variables["collection_data"]["title"]} [{config("ASPACE_STAFF_URL")}/resolve/readonly?uri={variables["collection_data"]["uri"]}]\n'
    with open(stream_path, "a") as stream:
        stream.write(message)

    if variables.get("onsite"):
        variables["onsite_medium"].preview(variables)
    if variables.get("cloud"):
        if variables["cloud_platform"].is_bucket_writable(
            config("PRESERVATION_BUCKET")
        ):
            message = (
                f'✅ S3 Bucket connection successful: {config("PRESERVATION_BUCKET")}\n'
            )
            with open(variables["stream_path"], "a") as stream:
                stream.write(message)
    if variables.get("access"):
        if variables["access_platform"].islandora_server_connection_is_successful:
            message = f'✅ Islandora server connection successful: {config("ISLANDORA_SSH_HOST")}\n'
            with open(variables["stream_path"], "a") as stream:
                stream.write(message)


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
