# PREPARE FILES AND METADATA FOR COPYING TO S3 STORAGE

import logging
import subprocess

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
    region_name=config("DISTILLERY_AWS_REGION", default="us-example-1"),
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
    ).split(f'{variables["work_preservation_files"]}/')[-1]
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
    """s5cmd cp digital object component file to S3 bucket"""
    preservation_file_key = str(variables["preservation_file_info"]["filepath"])[
        len(f'{variables["work_preservation_files"]}/') :
    ]
    for _attempt in range(3):
        try:
            subprocess.run(
                [
                    config("WORK_S5CMD_CMD"),
                    "cp",
                    "--concurrency",
                    "20",
                    "--metadata",
                    f'md5={variables["preservation_file_info"]["md5"].hexdigest()}',
                    variables["preservation_file_info"]["filepath"],
                    f's3://{config("PRESERVATION_BUCKET")}/{preservation_file_key}',
                ],
                env={
                    "AWS_ACCESS_KEY_ID": config("DISTILLERY_AWS_ACCESS_KEY_ID"),
                    "AWS_SECRET_ACCESS_KEY": config("DISTILLERY_AWS_SECRET_ACCESS_KEY"),
                },
                check=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            if _attempt > 1:
                logger.exception(f"❌ S5CMD STDOUT: {e.stdout}")
                logger.exception(f"❌ S5CMD STDERR: {e.stderr}")
                raise
        else:
            logger.info(
                "☑️ DIGITAL OBJECT COMPONENT FILE UPLOADED TO S3: {}/{}".format(
                    config("PRESERVATION_BUCKET"), preservation_file_key
                )
            )
    return variables["preservation_file_info"]["md5"].hexdigest()


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
