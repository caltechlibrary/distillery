# PREPARE FILES AND METADATA FOR COPYING TO TAPE STORAGE

import base64
import concurrent.futures
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import boto3
from decouple import config

import distill

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    Path(__file__).resolve().parent.joinpath("settings.ini"),
    disable_existing_loggers=False,
)
logger = logging.getLogger("s3")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=config("AWS_ACCESS_KEY"),
    aws_secret_access_key=config("AWS_SECRET_KEY"),
)


def main(
    cloud: ("sending to cloud storage", "flag", "c"),  # type: ignore
    onsite: ("preparing for onsite storage", "flag", "o"),  # type: ignore
    access: ("publishing access copies", "flag", "a"),  # type: ignore
    collection_id: "the Collection ID from ArchivesSpace",  # type: ignore
):

    logger.info("☁️ s3")

    variables = {}

    variables["cloud"] = cloud
    variables["onsite"] = onsite
    variables["access"] = access

    # NOTE we have to assume that PROCESSING_FILES is set correctly
    stream_path = Path(config("PROCESSING_FILES")).joinpath(
        f"{collection_id}-processing"
    )

    variables["stream_path"] = stream_path.as_posix()

    if not cloud:
        message = "❌ s3.py script was initiated without cloud being selected"
        logger.error(message)
        with open(stream_path, "a") as stream:
            stream.write(message)
        raise RuntimeError(message)

    try:
        (
            IN_PROCESS_ORIGINAL_FILES,
            STAGE_3_ORIGINAL_FILES,
            PRESERVATION_BUCKET,
            LOSSLESS_PRESERVATION_FILES,
        ) = validate_settings()
    except Exception as e:
        message = "❌ There was a problem with the settings for the processing script.\n"
        with open(stream_path, "a") as f:
            f.write(message)
        # logging.error(message, exc_info=True)
        # TODO set up notify
        # subprocess.run(["/bin/bash", "./notify.sh", str(e), message])
        raise

    variables["PRESERVATION_BUCKET"] = PRESERVATION_BUCKET
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
    with open(stream_path, "a") as f:
        f.write(
            f'✅ {variables["LOSSLESS_PRESERVATION_FILES"]}/{collection_id}/{collection_id}.json\n'
        )

    s3_client.put_object(
        Bucket=PRESERVATION_BUCKET,
        Key=collection_id + "/" + collection_id + ".json",
        Body=json.dumps(variables["collection_data"], indent=4, sort_keys=True),
    )
    with open(stream_path, "a") as f:
        f.write(
            f"✅ https://{PRESERVATION_BUCKET}.s3-us-west-2.amazonaws.com/{collection_id}/{collection_id}.json\n"
        )

    distill.loop_over_collection_subdirectories(variables)

    with open(stream_path, "a") as f:
        f.write(f"🗄 Finished processing {collection_id}.\n📆 {datetime.now()}\n")


def process_during_files_loop(variables):
    # Save Preservation Image in local filesystem structure.
    distill.save_preservation_file(
        variables["preservation_image_data"]["filepath"],
        f'{variables["LOSSLESS_PRESERVATION_FILES"]}/{variables["preservation_image_data"]["s3key"]}',
    )
    # Send Preservation File to S3.
    """example success response:
    {
        "ResponseMetadata": {
            "RequestId": "6BBE41DE8A1CABCE",
            "HostId": "c473fwfRMo+soCkOUwMsNZwR5fw0RIw2qcDVIXQOXVm1aGLV5clcL8JgBXojEJL99Umo4HYEzng=",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "x-amz-id-2": "c473fwfRMo+soCkOUwMsNZwR5fw0RIw2qcDVIXQOXVm1aGLV5clcL8JgBXojEJL99Umo4HYEzng=",
                "x-amz-request-id": "6BBE41DE8A1CABCE",
                "date": "Mon, 30 Nov 2020 22:58:33 GMT",
                "etag": "\"614bccea2760f37f41be65c62c41d66e\"",
                "content-length": "0",
                "server": "AmazonS3"
            },
            "RetryAttempts": 0
        },
        "ETag": "\"614bccea2760f37f41be65c62c41d66e\""
    }"""
    with open(variables["preservation_image_data"]["filepath"], "rb") as body:
        # start this in the background
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(
                s3_client.put_object,
                Bucket=variables["PRESERVATION_BUCKET"],
                Key=variables["preservation_image_data"]["s3key"],
                Body=body,
                ContentMD5=base64.b64encode(
                    variables["preservation_image_data"]["md5"].digest()
                ).decode(),
            )
            # run a loop checking for background process to be done
            # indicate processing by printing a dot every second to the stream
            iteration = 0
            while "state=running" in str(future):
                time.sleep(1)
                with open(variables["stream_path"], "a", newline="") as f:
                    f.write(".")
                iteration += 1
            s3_put_response = future.result()
    with open(variables["stream_path"], "a") as f:
        f.write(
            f'\n✅ https://{variables["PRESERVATION_BUCKET"]}.s3-us-west-2.amazonaws.com/{variables["preservation_image_data"]["s3key"]}\n'
        )
    # Verify S3 ETag.
    if (
        s3_put_response["ETag"].strip('"')
        != variables["preservation_image_data"]["md5"].hexdigest()
    ):
        message = (
            f'⚠️ the S3 ETag did not match for {variables["preservation_image_data"]["filepath"]}'
        )
        with open(variables["stream_path"], "a") as f:
            f.write(message)
    # Set up ArchivesSpace record.
    digital_object_component = distill.prepare_digital_object_component(
        variables["folder_data"], variables["PRESERVATION_BUCKET"], variables["preservation_image_data"]
    )
    # Post Digital Object Component to ArchivesSpace.
    digital_object_component_post_response = distill.post_digital_object_component(
        digital_object_component
    ).json()
    with open(variables["stream_path"], "a") as f:
        f.write(
            f'✅ {config("ASPACE_STAFF_URL")}/resolve/readonly?uri={digital_object_component_post_response["uri"]}\n'
        )


def process_during_subdirectories_loop(variables):
    """Called inside loop_over_collection_subdirectories function."""
    distill.save_folder_data(
        variables["folder_arrangement"],
        variables["folder_data"],
        variables["LOSSLESS_PRESERVATION_FILES"],
    )
    folder_data_key = distill.get_s3_aip_folder_key(
            distill.get_s3_aip_folder_prefix(variables["folder_arrangement"], variables["folder_data"]),
            variables["folder_data"],
        )
    # Send ArchivesSpace folder metadata to S3 as a JSON file.
    s3_client.put_object(
        Bucket=variables["PRESERVATION_BUCKET"],
        Key=folder_data_key,
        Body=json.dumps(variables["folder_data"], indent=4, sort_keys=True),
    )
    with open(variables["stream_path"], "a") as f:
        f.write(
            f'✅ https://{variables["PRESERVATION_BUCKET"]}.s3-us-west-2.amazonaws.com/{folder_data_key}\n'
        )


def validate_settings():
    IN_PROCESS_ORIGINAL_FILES = Path(
        os.path.expanduser(config("STAGE_2_ORIGINAL_FILES"))
    ).resolve(
        strict=True
    )  # NOTE do not create missing `IN_PROCESS_ORIGINAL_FILES`
    STAGE_3_ORIGINAL_FILES = distill.directory_setup(
        os.path.expanduser(config("STAGE_3_ORIGINAL_FILES"))
    ).resolve(strict=True)
    PRESERVATION_BUCKET = config(
        "PRESERVATION_BUCKET"
    )  # TODO validate access to bucket
    LOSSLESS_PRESERVATION_FILES = distill.directory_setup(
        os.path.expanduser(config("LOSSLESS_PRESERVATION_FILES"))
    ).resolve(strict=True)
    return (
        IN_PROCESS_ORIGINAL_FILES,
        STAGE_3_ORIGINAL_FILES,
        PRESERVATION_BUCKET,
        LOSSLESS_PRESERVATION_FILES,
    )


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
    # fmt: on
