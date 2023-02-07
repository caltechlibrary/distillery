# file: alchemist.py
# RENDER AND PUBLISH ACCESS PAGES AND ASSETS

import logging
import os

import boto3
import botocore

from decouple import config

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.ini"),
    disable_existing_loggers=False,
)
logger = logging.getLogger(__name__)

s3_client = boto3.client(
    "s3",
    aws_access_key_id=config("DISTILLERY_AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=config("DISTILLERY_AWS_SECRET_ACCESS_KEY"),
)

class AccessPlatform:
    def __init__(self, collection_id, collection_data):
        self.collection_id = collection_id
        self.collection_data = collection_data

    def collection_structure_processing(self):
        # TODO build html metadata/thumbnail page?
        logger.info(f"🐞 COLLECTION STRUCTURE PROCESSING: {self.collection_id}")

    def archival_object_level_processing(self, variables):
        # we have a list of file paths and all the metadata
        # TODO build html metadata/thumbnail page?
        # TODO build manifest?
        logger.info(f"🐞 ARCHIVAL OBJECT LEVEL PROCESSING: {self.collection_id}")

    def create_access_files(self, variables):
        # TODO create the Pyramid TIFFs for iiif-serverless
        logger.info(f"🐞 CREATE ACCESS FILES: {self.collection_id}")

    def transfer_derivative_files(self, variables):
        logger.info(f"🐞 TRANSFER DERIVATIVE FILES: {self.collection_id}")

    def ingest_derivative_files(self, variables):
        logger.info(f"🐞 INGEST DERIVATIVE FILES: {self.collection_id}")

    def loop_over_derivative_structure(self, variables):
        logger.info(f"🐞 LOOP OVER DERIVATIVE STRUCTURE: {self.collection_id}")

def validate_connection():
    try:
        response = s3_client.put_object(
            Bucket=config("ACCESS_BUCKET"), Key=".distillery"
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            logger.info(f'☁️  S3 BUCKET WRITABLE: {config("ACCESS_BUCKET")}')
            return True
        else:
            logger.error(f'❌ S3 BUCKET NOT WRITABLE: {config("ACCESS_BUCKET")}')
            logger.error(f"❌ S3 BUCKET RESPONSE: {response}")
            return False
    except botocore.exceptions.ClientError as error:
        logger.error(f"❌ S3 ERROR: {error.response}")
        return False
