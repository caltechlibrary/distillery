# PREPARE FILES AND METADATA FOR COPYING TO S3 STORAGE

import base64
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import boto3
import botocore

from decouple import config

import distillery

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    Path(__file__).resolve().parent.joinpath("settings.ini"),
    disable_existing_loggers=False,
)
logger = logging.getLogger("s3")

s3_client = boto3.client(
    "s3",
    region_name=config("DISTILLERY_AWS_REGION", default="us-west-2"),
    aws_access_key_id=config("DISTILLERY_AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=config("DISTILLERY_AWS_SECRET_ACCESS_KEY"),
)


def collection_level_preprocessing(collection_id, work_preservation_files):
    """Run before any files are moved or records are created."""
    transfer_collection_datafile(collection_id, work_preservation_files)


def transfer_collection_datafile(collection_id, work_preservation_files):
    """POST collection data to S3 bucket as a JSON file."""
    collection_datafile_key = Path(collection_id).joinpath(f"{collection_id}.json")
    collection_datafile_path = (
        Path(work_preservation_files).joinpath(collection_datafile_key).resolve()
    )
    with open(collection_datafile_path, "rb") as body:
        s3_client.put_object(
            Bucket=config("PRESERVATION_BUCKET"),
            Key=str(collection_datafile_key),
            Body=body,
        )
    logger.info(
        f'☑️  COLLECTION DATAFILE UPLOADED TO S3: {config("PRESERVATION_BUCKET")}/{str(collection_datafile_key)}'
    )


def transfer_archival_object_datafile(variables):
    """POST archival object data to S3 bucket as a JSON file."""
    # logger.info(f'🐞 str(variables["current_archival_object_datafile"]): {str(variables["current_archival_object_datafile"])}')
    archival_object_datafile_key = str(
        variables["current_archival_object_datafile"]
    ).split(f'{config("WORK_PRESERVATION_FILES")}/')[-1]
    # logger.info(f'🐞 archival_object_datafile_key: {archival_object_datafile_key}')
    with open(variables["current_archival_object_datafile"], "rb") as body:
        s3_client.put_object(
            Bucket=config("PRESERVATION_BUCKET"),
            Key=archival_object_datafile_key,
            Body=body,
        )
    logger.info(
        f'☑️  ARCHIVAL OBJECT DATAFILE UPLOADED TO S3: {config("PRESERVATION_BUCKET")}/{str(archival_object_datafile_key)}'
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
        len(f'{config("WORK_PRESERVATION_FILES")}/') :
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
            f'☑️  DIGITAL OBJECT COMPONENT FILE UPLOADED TO S3: {config("PRESERVATION_BUCKET")}/{preservation_file_key}'
        )
        return response["ETag"].strip('"')


def process_archival_object_datafile(variables):
    transfer_archival_object_datafile(variables)


def process_digital_object_component_file(variables):
    """transfer file to S3; create ArchivesSpace record"""
    if not transfer_digital_object_component_file(variables):
        logger.warning()
        return
    variables["file_uri_scheme"] = "s3"
    variables["file_uri_host"] = config("PRESERVATION_BUCKET")
    if not distillery.save_digital_object_component_record(variables):
        logger.warning()
        return


def validate_connection():
    try:
        response = s3_client.put_object(
            Bucket=config("PRESERVATION_BUCKET"), Key=".distillery"
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            logger.info(f'☁️  S3 BUCKET WRITABLE: {config("PRESERVATION_BUCKET")}')
            return True
        else:
            logger.error(f'❌ S3 BUCKET NOT WRITABLE: {config("PRESERVATION_BUCKET")}')
            logger.error(f"❌ S3 BUCKET RESPONSE: {response}")
            return False
    except botocore.exceptions.ClientError as error:
        logger.error(f"❌ S3 ERROR: {error.response}")
        return False
