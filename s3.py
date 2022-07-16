# PREPARE FILES AND METADATA FOR COPYING TO S3 STORAGE

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

import distillery

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    Path(__file__).resolve().parent.joinpath("settings.ini"),
    disable_existing_loggers=False,
)
logger = logging.getLogger("s3")
validation_logger = logging.getLogger("validation")

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
    variables = {}

    variables["cloud"] = cloud
    variables["onsite"] = onsite
    variables["access"] = access

    # NOTE we have to assume that STATUS_FILES is set correctly
    stream_path = Path(
        f'{config("WORK_NAS_APPS_MOUNTPOINT")}/{config("NAS_STATUS_FILES_RELATIVE_PATH")}'
    ).joinpath(f"{collection_id}-processing")

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
            WORK_LOSSLESS_PRESERVATION_FILES,
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
        "WORK_LOSSLESS_PRESERVATION_FILES"
    ] = WORK_LOSSLESS_PRESERVATION_FILES.as_posix()

    variables["collection_directory"] = distillery.confirm_collection_directory(
        IN_PROCESS_ORIGINAL_FILES, collection_id
    )
    variables["collection_data"] = distillery.get_collection_data(collection_id)

    distillery.save_collection_metadata(
        variables["collection_data"], WORK_LOSSLESS_PRESERVATION_FILES
    )
    with open(stream_path, "a") as f:
        f.write(
            f'✅ {variables["WORK_LOSSLESS_PRESERVATION_FILES"]}/{collection_id}/{collection_id}.json\n'
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

    distillery.loop_over_collection_subdirectories(variables)

    with open(stream_path, "a") as f:
        f.write(f"🗄 Finished processing {collection_id}.\n📆 {datetime.now()}\n")


def collection_level_preprocessing(variables):
    """Run before any files are moved or records are created."""
    # logger.info("🐞 INSIDE s3.collection_level_preprocessing()")
    transfer_collection_datafile(variables)


def transfer_collection_datafile(variables):
    """POST collection data to S3 bucket as a JSON file."""
    # logger.info("🐞 INSIDE s3.transfer_collection_datafile()")
    # logger.info(f'🐞 type(variables["WORK_LOSSLESS_PRESERVATION_FILES"]): {type(variables["WORK_LOSSLESS_PRESERVATION_FILES"])}')
    collection_datafile_key = Path(variables["collection_id"]).joinpath(
        f'{variables["collection_id"]}.json'
    )
    # logger.info(f'🐞 collection_datafile_key: {str(collection_datafile_key)}')
    collection_datafile_path = (
        Path(variables["WORK_LOSSLESS_PRESERVATION_FILES"])
        .joinpath(collection_datafile_key)
        .resolve()
    )
    # logger.info(f"🐞 collection_datafile_path: {str(collection_datafile_path)}")
    s3_client.put_object(
        Bucket=config("PRESERVATION_BUCKET"),
        Key=str(collection_datafile_key),
        Body=str(collection_datafile_path),
    )
    logger.info(
        f'☑️  S3 OBJECT UPLOADED: {config("PRESERVATION_BUCKET")}/{str(collection_datafile_key)}'
    )


def transfer_derivative_structure(variables):
    """Transfer PRESERVATION_FILES/CollectionID to S3 bucket.

    If something goes wrong with in copying the files to S3 there will
    be no ArchivesSpace records to clean up.

    VARIABLES USED:

    """


def transfer_archival_object_datafile(variables):
    """POST archival object data to S3 bucket as a JSON file."""
    # logger.info(f'🐞 str(variables["current_archival_object_datafile"]): {str(variables["current_archival_object_datafile"])}')
    archival_object_datafile_key = str(
        variables["current_archival_object_datafile"]
    ).split(f'{variables["WORK_LOSSLESS_PRESERVATION_FILES"]}/')[-1]
    # logger.info(f'🐞 archival_object_datafile_key: {archival_object_datafile_key}')
    s3_client.put_object(
        Bucket=config("PRESERVATION_BUCKET"),
        Key=archival_object_datafile_key,
        Body=str(variables["current_archival_object_datafile"]),
    )
    logger.info(
        f'☑️  S3 OBJECT UPLOADED: {config("PRESERVATION_BUCKET")}/{str(archival_object_datafile_key)}'
    )


def transfer_digital_object_component_file(variables):
    """POST digital object component file to S3 bucket.

    EXAMPLE SUCCESS RESPONSE: {
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
    preservation_file_key = str(variables["preservation_file_info"]["filepath"])[
        len(f'{str(variables["WORK_LOSSLESS_PRESERVATION_FILES"])}/') :
    ]
    with open(variables["preservation_file_info"]["filepath"], "rb") as body:
        response = s3_client.put_object(
            Bucket=config("PRESERVATION_BUCKET"),
            Key=preservation_file_key,
            Body=body,
            ContentMD5=base64.b64encode(
                variables["preservation_file_info"]["md5"].digest()
            ).decode(),
        )
    if (
        response["ETag"].strip('"')
        != variables["preservation_file_info"]["md5"].hexdigest()
    ):
        logger.warning(f"⚠️  S3 ETag DID NOT MATCH: {preservation_file_key}")
        return
    else:
        logger.info(
            f'☑️  S3 OBJECT UPLOADED: {config("PRESERVATION_BUCKET")}/{preservation_file_key}'
        )
        return response["ETag"].strip('"')


def process_archival_object_datafile(variables):
    transfer_archival_object_datafile(variables)


def process_digital_object_component_file(variables):
    """transfer file to S3; create ArchivesSpace record"""
    if not transfer_digital_object_component_file(variables):
        logger.warning()
        return
    logger.info(f"🐞 str(__name__): {str(__name__)}")
    variables["file_uri_scheme"] = "s3"
    variables["file_uri_host"] = config("PRESERVATION_BUCKET")
    if not distillery.save_digital_object_component_record(variables):
        logger.warning()
        return


def is_bucket_writable(bucket):
    if (
        s3_client.put_object(Bucket=bucket, Key=".distillery")["ResponseMetadata"][
            "HTTPStatusCode"
        ]
        == 200
    ):
        logger.info(f"☁️  S3 BUCKET WRITABLE: {bucket}")
        return True


def process_during_files_loop(variables):
    # Save Preservation Image in local filesystem structure.
    distillery.save_preservation_file(
        variables["preservation_file_info"]["filepath"],
        f'{variables["WORK_LOSSLESS_PRESERVATION_FILES"]}/{variables["preservation_file_info"]["s3key"]}',
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
    with open(variables["preservation_file_info"]["filepath"], "rb") as body:
        # start this in the background
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(
                s3_client.put_object,
                Bucket=variables["PRESERVATION_BUCKET"],
                Key=variables["preservation_file_info"]["s3key"],
                Body=body,
                ContentMD5=base64.b64encode(
                    variables["preservation_file_info"]["md5"].digest()
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
            f'\n✅ https://{variables["PRESERVATION_BUCKET"]}.s3-us-west-2.amazonaws.com/{variables["preservation_file_info"]["s3key"]}\n'
        )
    # Verify S3 ETag.
    if (
        s3_put_response["ETag"].strip('"')
        != variables["preservation_file_info"]["md5"].hexdigest()
    ):
        message = f'⚠️ the S3 ETag did not match for {variables["preservation_file_info"]["filepath"]}'
        with open(variables["stream_path"], "a") as f:
            f.write(message)
    # Set up ArchivesSpace record.
    digital_object_component = distillery.prepare_digital_object_component(
        variables["folder_data"],
        variables["PRESERVATION_BUCKET"],
        variables["preservation_file_info"],
    )
    # Post Digital Object Component to ArchivesSpace.
    digital_object_component_post_response = distillery.post_digital_object_component(
        digital_object_component
    ).json()
    with open(variables["stream_path"], "a") as f:
        f.write(
            f'✅ {config("ASPACE_STAFF_URL")}/resolve/readonly?uri={digital_object_component_post_response["uri"]}\n'
        )


def process_during_subdirectories_loop(variables):
    """Called inside loop_over_collection_subdirectories function."""
    distillery.save_folder_data(
        variables["folder_arrangement"],
        variables["folder_data"],
        variables["WORK_LOSSLESS_PRESERVATION_FILES"],
    )
    folder_data_key = distillery.get_s3_aip_folder_key(
        distillery.get_s3_aip_folder_prefix(
            variables["folder_arrangement"], variables["folder_data"]
        ),
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
        os.path.expanduser(config("WORKING_ORIGINAL_FILES"))
    ).resolve(
        strict=True
    )  # NOTE do not create missing `IN_PROCESS_ORIGINAL_FILES`
    STAGE_3_ORIGINAL_FILES = distillery.directory_setup(
        os.path.expanduser(config("STAGE_3_ORIGINAL_FILES"))
    ).resolve(strict=True)
    PRESERVATION_BUCKET = config(
        "PRESERVATION_BUCKET"
    )  # TODO validate access to bucket
    WORK_LOSSLESS_PRESERVATION_FILES = distillery.directory_setup(
        os.path.expanduser(
            f'{config("WORK_NAS_ARCHIVES_MOUNTPOINT")}/{config("NAS_LOSSLESS_PRESERVATION_FILES_RELATIVE_PATH")}'
        )
    ).resolve(strict=True)
    return (
        IN_PROCESS_ORIGINAL_FILES,
        STAGE_3_ORIGINAL_FILES,
        PRESERVATION_BUCKET,
        WORK_LOSSLESS_PRESERVATION_FILES,
    )


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
