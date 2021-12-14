# CALTECH ARCHIVES AND SPECIAL COLLECTIONS
# digital object preservation workflow

# processing functionality; see distillery.py for bottlepy web application

import base64
import concurrent.futures
import hashlib
import json
import logging
import logging.config
import logging.handlers
import os
import random
import shutil
import string
import sys
import time
from datetime import datetime
from pathlib import Path

import boto3
import botocore
import plac
import sh
from asnake.client import ASnakeClient
from decouple import config
from jpylyzer import jpylyzer
from requests import HTTPError

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.ini"),
    disable_existing_loggers=False,
)
logger = logging.getLogger("distill")
archivesspace_logger = logging.getLogger("archivesspace")

# TODO do we need a class? https://stackoverflow.com/a/16502408/4100024
# we have 8 functions that need an authorized connection to ArchivesSpace
asnake_client = ASnakeClient(
    baseurl=config("ASPACE_API_URL"),
    username=config("ASPACE_USERNAME"),
    password=config("ASPACE_PASSWORD"),
)
asnake_client.authorize()

s3_client = boto3.client(
    "s3",
    aws_access_key_id=config("AWS_ACCESS_KEY"),
    aws_secret_access_key=config("AWS_SECRET_KEY"),
)


def distill(
    cloud: ("sending to cloud storage", "flag", "c"),  # type: ignore
    onsite: ("preparing for onsite storage", "flag", "o"),  # type: ignore
    access: ("publishing access copies", "flag", "a"),  # type: ignore
    collection_id: "the Collection ID from ArchivesSpace",  # type: ignore
):

    logger.info("🛁 distilling")

    variables = {}

    variables["cloud"] = cloud
    variables["onsite"] = onsite
    variables["access"] = access

    # NOTE we have to assume that PROCESSING_FILES is set correctly
    stream_path = Path(config("PROCESSING_FILES")).joinpath(
        f"{collection_id}-processing"
    )

    if not cloud:
        message = "❌ distill.py script was initiated without cloud being selected"
        logger.error(message)
        with open(stream_path, "a") as stream:
            stream.write(message)
        raise RuntimeError(message)

    try:
        (
            STAGE_2_ORIGINAL_FILES,
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

    with open(stream_path, "a") as f:
        f.write(f"📅 {datetime.now()}\n🗄 {collection_id}\n")

    # TODO refactor so that we can get an initial report on the results of both
    # the directory and the uri so that users can know if one or both of the
    # points of failure are messed up right away

    try:
        collection_directory = get_collection_directory(
            STAGE_2_ORIGINAL_FILES, collection_id
        )
        if collection_directory:
            with open(stream_path, "a") as f:
                f.write(
                    f"✅ Collection directory for {collection_id} found on filesystem: {collection_directory}\n"
                )
        # TODO report on contents of collection_directory
    except NotADirectoryError as e:
        message = f"❌ No valid directory for {collection_id} was found on filesystem: {os.path.join(STAGE_2_ORIGINAL_FILES, collection_id)}\n"
        with open(stream_path, "a") as f:
            f.write(message)
        # logging.error(message, exc_info=True)
        raise

    variables["collection_directory"] = collection_directory

    try:
        collection_data = get_collection_data(collection_id)
        if collection_data:
            with open(stream_path, "a") as f:
                f.write(
                    f"✅ Collection data for {collection_id} retrieved from ArchivesSpace.\n"
                )
        # TODO report on contents of collection_data
    except ValueError as e:
        message = (
            f"❌ No collection URI for {collection_id} was found in ArchivesSpace.\n"
        )
        with open(stream_path, "a") as f:
            f.write(message)
        # logging.error(message, exc_info=True)
        raise
    except RuntimeError as e:
        message = (
            f"❌ No collection data for {collection_id} retrieved from ArchivesSpace.\n"
        )
        with open(stream_path, "a") as f:
            f.write(message)
        # logging.error(message, exc_info=True)
        raise
    except HTTPError as e:
        message = f"❌ There was a problem with the connection to ArchivesSpace.\n"
        with open(stream_path, "a") as f:
            f.write(message)
        # logging.error(message, exc_info=True)
        raise
    except Exception as e:
        message = (
            f"❌ There was a problem retrieving collection data for {collection_id} from ArchivesSpace.\n"
        )
        with open(stream_path, "a") as f:
            f.write(message)
        # logging.error(message, exc_info=True)
        raise

    variables["collection_data"] = collection_data

    # Save collection metadata to LOSSLESS_PRESERVATION_FILES directory.
    try:
        save_collection_metadata(collection_data, LOSSLESS_PRESERVATION_FILES)
        with open(stream_path, "a") as f:
            f.write(
                f"✅ Collection metadata for {collection_id} saved to: {LOSSLESS_PRESERVATION_FILES}/{collection_id}/{collection_id}.json\n"
            )
    except OSError as e:
        message = f"❌ Unable to save {collection_id}.json file to: {LOSSLESS_PRESERVATION_FILES}/{collection_id}\n"
        with open(stream_path, "a") as f:
            f.write(message)
        # logging.error(message, exc_info=True)
        raise

    # Send collection metadata to S3.
    try:
        s3_client.put_object(
            Bucket=PRESERVATION_BUCKET,
            Key=collection_id + "/" + collection_id + ".json",
            Body=json.dumps(collection_data, sort_keys=True, indent=4),
        )
        with open(stream_path, "a") as f:
            f.write(
                f"✅ Collection metadata for {collection_id} sent to {PRESERVATION_BUCKET} on S3.\n"
            )
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InternalError":
            message = (
                f"❌ Unable to send collection metadata for {collection_id} to {PRESERVATION_BUCKET} on S3.\n"
                f"Error Message: {e.response['Error']['Message']}\n"
                f"Request ID: {e.response['ResponseMetadata']['RequestId']}\n"
                f"HTTP Code: {e.response['ResponseMetadata']['HTTPStatusCode']}\n"
            )
            with open(stream_path, "a") as f:
                f.write(message)
            # logging.error(message, exc_info=True)
            raise
        else:
            raise e

    folders, filecount = prepare_folder_list(collection_directory)
    filecounter = 0

    # Loop over folders list.
    # The folders here will end up as Islandora Books.
    # _data/HBF <-- looping over folders under here
    # ├── HBF_000_XX
    # ├── HBF_001_02
    # │   ├── HBF_001_02_01.tif
    # │   ├── HBF_001_02_02.tif
    # │   ├── HBF_001_02_03.tif
    # │   └── HBF_001_02_04.tif
    # └── HBF_007_08
    folders.sort(reverse=True)
    for _ in range(len(folders)):
        # Using pop() (and/or range(len()) above) maybe helps to be sure that
        # if folder metadata fails to process properly, it and its images are
        # skipped completely and the script moves on to the next folder.
        folderpath = folders.pop()

        # Set up list of TIFF paths for the current folder.
        filepaths = prepare_filepaths_list(folderpath)
        # Avoid processing folder when there are no files.
        if not filepaths:
            continue

        try:
            folder_arrangement, folder_data = process_folder_metadata(folderpath)
            with open(stream_path, "a") as f:
                f.write(
                    f"✅ Folder data for {folder_data['component_id']} ({folder_data['display_string']}) retrieved from ArchivesSpace. [{config('ASPACE_STAFF_URL')}/resolve/readonly?uri={folder_data['uri']}]\n"
                )
        except RuntimeError as e:
            # NOTE possible error strings include:
            # f"The component_id cannot be determined from the directory name: {os.path.basename(folderpath)}"
            # f"The directory name does not correspond to the collection_id: {os.path.basename(folderpath)}"
            # f"No records found with component_id: {component_id}"
            # f"Multiple records found with component_id: {component_id}"
            # f"The ArchivesSpace record for {folder_data['component_id']} contains multiple digital objects."
            # f"Missing collection data for: {folder_data['component_id']}"
            # f"Sub-Series record is missing component_id: {subseries['display_string']} {ancestor['ref']}"
            # f"Missing series data for: {folder_data['component_id']}"
            message = f"⚠️ Unable to retrieve metadata for: {folderpath}\n↩️ Skipping {folderpath} folder.\n"
            with open(stream_path, "a") as f:
                f.write(message)
            # logging.warning(message, exc_info=True)
            # TODO increment file counter by the count of files in this folder
            continue

        # Save folder metadata to LOSSLESS_PRESERVATION_FILES directory.
        try:
            save_folder_data(folder_arrangement, folder_data, LOSSLESS_PRESERVATION_FILES)
            with open(stream_path, "a") as f:
                f.write(
                    f"✅ Folder metadata for {folder_data['component_id']} saved under: {LOSSLESS_PRESERVATION_FILES}/{collection_id}\n"
                )
        except Exception as e:
            message = f"❌ Unable to save {folder_data['component_id']}.json file to: {LOSSLESS_PRESERVATION_FILES}/{collection_id}\n"
            with open(stream_path, "a") as f:
                f.write(message)
            # logging.error(message, exc_info=True)
            raise

        # Send ArchivesSpace folder metadata to S3 as a JSON file.
        try:
            s3_client.put_object(
                Bucket=PRESERVATION_BUCKET,
                Key=get_s3_aip_folder_key(
                    get_s3_aip_folder_prefix(folder_arrangement, folder_data),
                    folder_data,
                ),
                Body=json.dumps(folder_data, sort_keys=True, indent=4),
            )
            with open(stream_path, "a") as f:
                f.write(
                    f"✅ Folder metadata for {folder_data['component_id']} sent to {PRESERVATION_BUCKET} on S3.\n"
                )
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "InternalError":
                message = (
                    f"⚠️ Unable to send folder metadata for {folder_data['component_id']} to {PRESERVATION_BUCKET} on S3.\n"
                    f"Error Message: {e.response['Error']['Message']}\n"
                    f"Request ID: {e.response['ResponseMetadata']['RequestId']}\n"
                    f"HTTP Code: {e.response['ResponseMetadata']['HTTPStatusCode']}\n"
                    f"↩️ Skipping {folderpath} folder.\n"
                )
                with open(stream_path, "a") as f:
                    f.write(message)
                # logging.warning(message, exc_info=True)
                continue
            else:
                raise e

        # NOTE: We reverse the sort for use with pop() and so the components
        # will be ingested in the correct order for the digital object tree.
        filepaths.sort(reverse=True)
        for f in range(len(filepaths)):
            filepath = filepaths.pop()
            filecounter += 1
            try:
                with open(stream_path, "a") as f:
                    f.write(f"⏳ Converting {os.path.basename(filepath)} to JPEG 2000")
                # start this in the background
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        process_aip_image,
                        filepath,
                        collection_data,
                        folder_arrangement,
                        folder_data,
                    )
                    # run a loop checking for it to be done
                    # indicate processing by printing a dot every second to the web
                    iteration = 0
                    while "state=running" in str(future):
                        time.sleep(1)
                        with open(stream_path, "a", newline="") as f:
                            f.write(".")
                        iteration += 1
                    aip_image_data = future.result()
                with open(stream_path, "a") as f:
                    f.write(
                        f"\n✅ Successfully converted {os.path.basename(filepath)} to JPEG 2000. [image {filecounter}/{filecount}]\n"
                    )
            except RuntimeError as e:
                message = (
                    f"\n⚠️ There was a problem converting {os.path.basename(filepath)} to JPEG 2000.\n"
                    f"↩️ Skipping {os.path.basename(filepath)} file. [image {filecounter}/{filecount}]\n"
                )
                with open(stream_path, "a") as f:
                    f.write(message)
                # logging.warning(message, exc_info=True)
                continue

            # # DEBUG
            # with open(stream_path, "a") as f:
            #     f.write(
            #         f"🔍 {aip_image_data}\n"
            #     )

            # Save Preservation Image in local filesystem structure.
            try:
                with open(stream_path, "a") as f:
                    f.write(
                        f"📂 Saving lossless JPEG 2000 for {Path(filepath).stem} under {LOSSLESS_PRESERVATION_FILES}/{collection_id}\n"
                    )
                # TODO change variable names (“aip” is always confusing; “s3key” is too specific)
                save_preservation_file(aip_image_data["filepath"], f'{LOSSLESS_PRESERVATION_FILES}/{aip_image_data["s3key"]}')
            except Exception as e:
                message = f'❌ There was a problem saving the file: {LOSSLESS_PRESERVATION_FILES}/{aip_image_data["s3key"]}\n'
                with open(stream_path, "a") as f:
                    f.write(message)
                # logging.error(message, exc_info=True)
                raise

            # Send AIP image to S3.
            # example success response:
            """{
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
            try:
                with open(stream_path, "a") as f:
                    f.write(
                        f"☁️ Sending JPEG 2000 for {Path(filepath).stem} to {PRESERVATION_BUCKET} on S3"
                    )
                with open(aip_image_data["filepath"], "rb") as body:
                    # start this in the background
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(
                            s3_client.put_object,
                            Bucket=PRESERVATION_BUCKET,
                            Key=aip_image_data["s3key"],
                            Body=body,
                            ContentMD5=base64.b64encode(
                                aip_image_data["md5"].digest()
                            ).decode(),
                        )
                        # run a loop checking for it to be done
                        # indicate processing by printing a dot every second to the web
                        iteration = 0
                        while "state=running" in str(future):
                            time.sleep(1)
                            with open(stream_path, "a", newline="") as f:
                                f.write(".")
                            iteration += 1
                        aip_image_put_response = future.result()
                with open(stream_path, "a") as f:
                    f.write(
                        f"\n✅ Sent JPEG 2000 for {Path(filepath).stem} to {PRESERVATION_BUCKET} on S3.\n"
                    )
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "InternalError":
                    message = (
                        f"⚠️ Unable to send JPEG 2000 for {Path(filepath).stem} to {PRESERVATION_BUCKET} on S3.\n"
                        f"Error Message: {e.response['Error']['Message']}\n"
                        f"Request ID: {e.response['ResponseMetadata']['RequestId']}\n"
                        f"HTTP Code: {e.response['ResponseMetadata']['HTTPStatusCode']}\n"
                        f"↩️ Skipping {Path(filepath).stem} file.\n"
                    )
                    with open(stream_path, "a") as f:
                        f.write(message)
                    # logging.warning(message, exc_info=True)
                    # TODO cleanup
                    continue
                else:
                    # logging.error(exc_info=True)
                    raise e

            # Verify S3 ETag.
            if (
                aip_image_put_response["ETag"].strip('"')
                == aip_image_data["md5"].hexdigest()
            ):
                with open(stream_path, "a") as f:
                    f.write(
                        f"✅ Verified checksums for JPEG 2000 of {Path(filepath).stem} sent to {PRESERVATION_BUCKET} on S3.\n"
                    )
            else:
                message = (
                    f"⚠️ the S3 ETag did not match for {aip_image_data['filepath']}"
                )
                with open(stream_path, "a") as f:
                    f.write(message)
                # logging.warning(message)
                continue

            # Set up ArchivesSpace record.
            digital_object_component = prepare_digital_object_component(
                folder_data, PRESERVATION_BUCKET, aip_image_data
            )

            # Post Digital Object Component to ArchivesSpace.
            try:
                # TODO return URI of digital object component
                # http://localhost:4321/resolve/readonly?uri=%2Frepositories%2F2%2Fdigital_object_components%2F108109
                # http://localhost:4321/resolve/edit?uri=%2Frepositories%2F2%2Fdigital_object_components%2F108109
                digital_object_component_post_response = post_digital_object_component(
                    digital_object_component
                ).json()
                with open(stream_path, "a") as f:
                    f.write(
                        f"✅ Created Digital Object Component for {Path(filepath).stem} ({digital_object_component['component_id']}) in ArchivesSpace. [{config('ASPACE_STAFF_URL')}/resolve/readonly?uri={digital_object_component_post_response['uri']}]\n"
                    )
            except BaseException as e:
                message = (
                    f"⚠️ Unable to create Digital Object Component for {Path(filepath).stem} in ArchivesSpace.\n"
                    f"↩️ Skipping. //TODO: DLD has been notified.\n"
                )
                with open(stream_path, "a") as f:
                    f.write(message)
                # logging.warning(message, exc_info=True)
                # TODO programmatically remove file from bucket?
                # logging.warning(
                #     f"‼️ Clean up {aip_image_data['s3key']} file in {PRESERVATION_BUCKET} bucket.\n"
                # )
                continue

            # # Move processed source file into `STAGE_3_ORIGINAL_FILES` with the structure
            # # under `STAGE_2_ORIGINAL_FILES` (the `+ 1` strips a path seperator).

            # # TODO need to rethink file locations; some files may be explicitly not sent
            # # to islandora, so islandora.py should not look for files in the same place;
            # # probably need two different locations: one for when originals are not to
            # # be published and one for when they may be
            # try:
            #     os.renames(
            #         filepath,
            #         os.path.join(
            #             STAGE_3_ORIGINAL_FILES,
            #             filepath[len(str(STAGE_2_ORIGINAL_FILES)) + 1 :],
            #         ),
            #     )
            # except OSError as e:
            #     message = (
            #         f"⚠️ Unable to move {filepath} to {STAGE_3_ORIGINAL_FILES}/.\n"
            #     )
            #     # logging.warning(message, exc_info=True)
            #     continue

            # Remove generated `*-LOSSLESS.jp2` file.
            # TODO redirect LOSSLESS.jp2 files for tape storage
            try:
                os.remove(aip_image_data["filepath"])
            except OSError as e:
                message = f"⚠️ Unable to remove {aip_image_data['filepath']} file.\n"
                # logging.warning(message, exc_info=True)
                continue

            with open(stream_path, "a") as f:
                f.write(f"📄 Finished processing {os.path.basename(filepath)} file.\n")

        with open(stream_path, "a") as f:
            f.write(
                f"📁 Finished processing folder {folder_data['component_id']} ({folder_data['display_string']}).\n"
            )

    with open(stream_path, "a") as f:
        f.write(f"🗄 Finished processing {collection_id}.\n📆 {datetime.now()}\n")

    # logging.info(f"🥃 finished distilling in {datetime.now() - time_start}\n")


def calculate_pixel_signature(filepath):
    return sh.cut(
        sh.sha512sum(
            sh.magick.stream_path(
                "-quiet",
                "-map",
                "rgb",
                "-storage-type",
                "short",
                filepath,
                "-",
                _piped=True,
            )
        ),
        "-d",
        " ",
        "-f",
        "1",
    )


def collection_identifiers_match(collection_id, collection_data):
    if collection_id != collection_data["id_0"]:
        return False
    return True


def confirm_digital_object(folder_data):
    digital_object_count = 0
    for instance in folder_data["instances"]:
        if "digital_object" in instance.keys():
            digital_object_count += 1
    if digital_object_count > 1:
        raise ValueError(
            f"The ArchivesSpace record for {folder_data['component_id']} contains multiple digital objects."
        )
    if digital_object_count < 1:
        # returns new folder_data with digital object info included
        folder_data = create_digital_object(folder_data)
    return folder_data


def confirm_digital_object_id(folder_data):
    # returns folder_data always in case digital_object_id was updated
    for instance in folder_data["instances"]:
        # TODO(tk) create script/report to periodically check for violations
        if "digital_object" in instance:
            if (
                instance["digital_object"]["_resolved"]["digital_object_id"]
                != folder_data["component_id"]
            ):
                # TODO confirm with Archives that replacing a digital_object_id is acceptable in all foreseen circumstances
                set_digital_object_id(
                    instance["digital_object"]["ref"], folder_data["component_id"]
                )
                # call get_folder_data() again to include updated digital_object_id
                folder_data = get_folder_data(folder_data["component_id"])
                # logging.info(
                #     f"❇️ updated digital_object_id: {instance['digital_object']['_resolved']['digital_object_id']} ➡️ {folder_data['component_id']} {instance['digital_object']['ref']}"
                # )
    return folder_data


def confirm_file(filepath):
    # confirm file exists and has the proper extention
    # valid extensions are: .tif, .tiff
    # NOTE: no mime type checking at this point, some TIFFs were troublesome
    if os.path.isfile(filepath):
        if os.path.splitext(filepath)[1] not in [".tif", ".tiff"]:
            print("❌  invalid file type: " + filepath)
            # TODO raise exception
            exit()
    else:
        print("❌  invalid file path: " + filepath)
        # TODO raise exception
        exit()


def create_digital_object(folder_data):
    digital_object = {}
    digital_object["digital_object_id"] = folder_data["component_id"]  # required
    digital_object["title"] = folder_data["title"]  # required
    # NOTE leaving created digital objects unpublished
    # digital_object['publish'] = True

    digital_object_post_response = asnake_client.post(
        "/repositories/2/digital_objects", json=digital_object
    )
    # example success response:
    # {
    #     "id": 9189,
    #     "lock_version": 0,
    #     "stale": true,
    #     "status": "Created",
    #     "uri": "/repositories/2/digital_objects/9189",
    #     "warnings": []
    # }
    # example error response:
    # {
    #     "error": {
    #         "digital_object_id": [
    #             "Must be unique"
    #         ]
    #     }
    # }
    # skip folder processing if digital_object_id already exists
    if "error" in digital_object_post_response.json():
        if "digital_object_id" in digital_object_post_response.json()["error"]:
            if (
                "Must be unique"
                in digital_object_post_response.json()["error"]["digital_object_id"]
            ):
                raise ValueError(
                    f" ⚠️\t non-unique digital_object_id: {folder_data['component_id']}"
                )
    digital_object_post_response.raise_for_status()

    archivesspace_logger.info(digital_object_post_response.json()["uri"])

    # set up a digital object instance to add to the archival object
    digital_object_instance = {
        "instance_type": "digital_object",
        "digital_object": {"ref": digital_object_post_response.json()["uri"]},
    }
    # get archival object
    archival_object_get_response = asnake_client.get(folder_data["uri"])
    archival_object_get_response.raise_for_status()
    archival_object = archival_object_get_response.json()
    # add digital object instance to archival object
    archival_object["instances"].append(digital_object_instance)
    # post updated archival object
    archival_object_post_response = asnake_client.post(
        folder_data["uri"], json=archival_object
    )
    archival_object_post_response.raise_for_status()

    # call get_folder_data() again to include digital object instance
    folder_data = get_folder_data(folder_data["component_id"])

    # logging.info(
    #     f"✳️ created digital object {digital_object['digital_object_id']} {digital_object_post_response.json()['uri']}"
    # )
    return folder_data


def directory_setup(directory):
    if not Path(directory).exists():
        Path(directory).mkdir()
    elif Path(directory).is_file():
        raise FileExistsError(f"a non-directory file exists at: {directory}")
    return Path(directory)


def get_aip_image_data(filepath):
    aip_image_data = {}
    aip_image_data["filepath"] = filepath
    jpylyzer_xml = jpylyzer.checkOneFile(aip_image_data["filepath"])
    aip_image_data["filesize"] = jpylyzer_xml.findtext("./fileInfo/fileSizeInBytes")
    aip_image_data["width"] = jpylyzer_xml.findtext(
        "./properties/jp2HeaderBox/imageHeaderBox/width"
    )
    aip_image_data["height"] = jpylyzer_xml.findtext(
        "./properties/jp2HeaderBox/imageHeaderBox/height"
    )
    aip_image_data["standard"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestreamBox/siz/rsiz"
    )
    aip_image_data["transformation"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestreamBox/cod/transformation"
    )
    aip_image_data["quantization"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestreamBox/qcd/qStyle"
    )
    with open(aip_image_data["filepath"], "rb") as f:
        aip_image_data["md5"] = hashlib.md5(f.read())
    return aip_image_data


def get_archival_object(id):
    response = asnake_client.get("/repositories/2/archival_objects/" + id)
    response.raise_for_status()
    return response.json()


def get_collection_data(collection_id):
    # raises an HTTPError exception if unsuccessful
    collection_uri = get_collection_uri(collection_id)
    collection_data = asnake_client.get(collection_uri).json()
    if not collection_identifiers_match(collection_id, collection_data):
        message = f"❌ The Collection ID from the form, {collection_id}, must exactly match the identifier in ArchivesSpace, {collection_data['id_0']}, including case-sensitively.\n"
        raise ValueError(message)
    if collection_data:
        collection_data["tree"]["_resolved"] = get_collection_tree(collection_uri)
        if collection_data["tree"]["_resolved"]:
            return collection_data
    else:
        raise RuntimeError(
            f"There was a problem retrieving the collection data from ArchivesSpace.\n"
        )


def get_collection_directory(STAGE_2_ORIGINAL_FILES, collection_id):
    # make a list of directory names to check against
    entries = []
    for entry in os.scandir(STAGE_2_ORIGINAL_FILES):
        if entry.is_dir:
            entries.append(entry.name)
    # check that collection_id case matches directory name
    if collection_id in entries:
        return os.path.join(STAGE_2_ORIGINAL_FILES, collection_id)
    else:
        raise NotADirectoryError(
            f"Missing or invalid collection directory: {os.path.join(STAGE_2_ORIGINAL_FILES, collection_id)}\n"
        )


def get_collection_tree(collection_uri):
    # raises an HTTPError exception if unsuccessful
    collection_tree = asnake_client.get(collection_uri + "/ordered_records").json()
    if collection_tree:
        return collection_tree
    else:
        raise RuntimeError(
            f"There was a problem retrieving the collection tree from ArchivesSpace.\n"
        )


def get_collection_uri(collection_id):
    # raises an HTTPError exception if unsuccessful
    search_results_json = asnake_client.get(
        f'/repositories/2/find_by_id/resources?identifier[]=["{collection_id}"]'
    ).json()
    if len(search_results_json["resources"]) < 1:
        raise ValueError(
            f"No collection found in ArchivesSpace with the ID: {collection_id}\n"
        )
    else:
        return search_results_json["resources"][0]["ref"]


def get_crockford_characters(n=4):
    return "".join(random.choices("abcdefghjkmnpqrstvwxyz" + string.digits, k=n))


def get_digital_object_component_id():
    return get_crockford_characters() + "_" + get_crockford_characters()


def get_file_parts(filepath):
    file_parts = {}
    file_parts["filepath"] = filepath
    file_parts["filename"] = file_parts["filepath"].split("/")[-1]
    file_parts["image_id"] = file_parts["filename"].split(".")[0]
    file_parts["extension"] = file_parts["filename"].split(".")[-1]
    file_parts["folder_id"] = file_parts["image_id"].rsplit("_", 1)[0]
    # TODO rename 'sequence' because it is not always numeric
    file_parts["sequence"] = file_parts["image_id"].split("_")[-1].zfill(4)
    file_parts["component_id"] = get_digital_object_component_id()
    return file_parts


def get_folder_arrangement(folder_data):
    # returns names and identifers of the arragement levels for a folder
    folder_arrangement = {}
    folder_arrangement["repository_name"] = folder_data["repository"]["_resolved"][
        "name"
    ]
    folder_arrangement["repository_code"] = folder_data["repository"]["_resolved"][
        "repo_code"
    ]
    folder_arrangement["folder_display"] = folder_data["display_string"]
    folder_arrangement["folder_title"] = folder_data["title"]
    for instance in folder_data["instances"]:
        if "sub_container" in instance:
            if instance["sub_container"]["top_container"]["_resolved"].get(
                "collection"
            ):
                folder_arrangement["collection_display"] = instance["sub_container"][
                    "top_container"
                ]["_resolved"]["collection"][0]["display_string"]
                folder_arrangement["collection_id"] = instance["sub_container"][
                    "top_container"
                ]["_resolved"]["collection"][0]["identifier"]
            else:
                raise ValueError(
                    f"Missing collection data for: {folder_data['component_id']}"
                )
            if instance["sub_container"]["top_container"]["_resolved"].get("series"):
                folder_arrangement["series_display"] = instance["sub_container"][
                    "top_container"
                ]["_resolved"]["series"][0]["display_string"]
                folder_arrangement["series_id"] = instance["sub_container"][
                    "top_container"
                ]["_resolved"]["series"][0]["identifier"]
                for ancestor in folder_data["ancestors"]:
                    if ancestor["level"] == "subseries":
                        subseries = get_archival_object(ancestor["ref"].split("/")[-1])
                        folder_arrangement["subseries_display"] = subseries[
                            "display_string"
                        ]
                        if "component_id" in subseries:
                            folder_arrangement["subseries_id"] = subseries[
                                "component_id"
                            ]
                        else:
                            raise ValueError(
                                f"Sub-Series record is missing component_id: {subseries['display_string']} {ancestor['ref']}"
                            )
            else:
                # logging.info(
                #     f"👀 series: {instance['sub_container']['top_container']['_resolved']['series']}"
                # )
                raise ValueError(
                    f"Missing series data for: {folder_data['component_id']}"
                )
    return folder_arrangement


def get_folder_data(component_id):
    # returns archival object metadata using the component_id; two API calls
    find_by_id_response = asnake_client.get(
        f"/repositories/2/find_by_id/archival_objects?component_id[]={component_id}"
    )
    find_by_id_response.raise_for_status()
    if len(find_by_id_response.json()["archival_objects"]) < 1:
        # figure out the box folder
        raise ValueError(f"No records found with component_id: {component_id}")
    if len(find_by_id_response.json()["archival_objects"]) > 1:
        raise ValueError(f"Multiple records found with component_id: {component_id}")
    archival_object_get_response = asnake_client.get(
        f"{find_by_id_response.json()['archival_objects'][0]['ref']}?resolve[]=digital_object&resolve[]=repository&resolve[]=top_container"
    )
    archival_object_get_response.raise_for_status()
    return archival_object_get_response.json()


def get_folder_id(filepath):
    # isolate the filename and then get the folder id
    return filepath.split("/")[-1].rsplit("_", 1)[0]


def get_s3_aip_folder_key(prefix, folder_data):
    # exception for extended identifiers like HaleGE_02_0B_056_07
    # TODO(tk) remove once no more exception files exist
    # TODO(tk) use older_data['component_id'] directly
    folder_id_parts = folder_data["component_id"].split("_")
    folder_id = "_".join([folder_id_parts[0], folder_id_parts[-2], folder_id_parts[-1]])
    return prefix + folder_id + ".json"


def get_s3_aip_folder_prefix(folder_arrangement, folder_data):
    prefix = folder_arrangement["collection_id"] + "/"
    if "series_id" in folder_arrangement.keys():
        prefix += (
            folder_arrangement["collection_id"]
            + "-s"
            + folder_arrangement["series_id"].zfill(2)
            + "-"
        )
        if "series_display" in folder_arrangement.keys():
            series_display = "".join(
                [
                    c if c.isalnum() else "-"
                    for c in folder_arrangement["series_display"]
                ]
            )
            prefix += series_display + "/"
            if "subseries_id" in folder_arrangement.keys():
                prefix += (
                    folder_arrangement["collection_id"]
                    + "-s"
                    + folder_arrangement["series_id"].zfill(2)
                    + "-ss"
                    + folder_arrangement["subseries_id"].zfill(2)
                    + "-"
                )
                if "subseries_display" in folder_arrangement.keys():
                    subseries_display = "".join(
                        [
                            c if c.isalnum() else "-"
                            for c in folder_arrangement["subseries_display"]
                        ]
                    )
                    prefix += subseries_display + "/"
    # exception for extended identifiers like HaleGE_02_0B_056_07
    # TODO(tk) remove once no more exception files exist
    # TODO(tk) use folder_data['component_id'] directly
    folder_id_parts = folder_data["component_id"].split("_")
    folder_id = "_".join([folder_id_parts[0], folder_id_parts[-2], folder_id_parts[-1]])
    folder_display = "".join(
        [c if c.isalnum() else "-" for c in folder_arrangement["folder_display"]]
    )
    prefix += folder_id + "-" + folder_display + "/"
    return prefix


def get_s3_aip_image_key(prefix, file_parts):
    # NOTE: '.jp2' is hardcoded as the extension
    # HaleGE/HaleGE_s02_Correspondence_and_Documents_Relating_to_Organizations/HaleGE_s02_ss0B_National_Academy_of_Sciences/HaleGE_056_07_Section_on_Astronomy/HaleGE_056_07_0001/8c38-d9cy.jp2
    # {
    #     "component_id": "me5v-z1yp",
    #     "extension": "tiff",
    #     "filename": "HaleGE_02_0B_056_07_0001.tiff",
    #     "filepath": "/path/to/archives/data/STAGE_2_ORIGINAL_FILES/HaleGE/HaleGE_02_0B_056_07_0001.tiff",
    #     "folder_id": "HaleGE_02_0B_056_07",
    #     "image_id": "HaleGE_02_0B_056_07_0001",
    #     "sequence": "0001"
    # }
    # exception for extended identifiers like HaleGE_02_0B_056_07
    # TODO(tk) remove once no more exception files exist
    # TODO(tk) use file_parts['folder_id'] directly
    folder_id_parts = file_parts["folder_id"].split("_")
    folder_id = "_".join([folder_id_parts[0], folder_id_parts[-2], folder_id_parts[-1]])
    return (
        prefix
        + folder_id
        + "_"
        + file_parts["sequence"]
        + "/"
        + file_parts["component_id"]
        + ".jp2"
    )


def get_xmp_dc_metadata(folder_arrangement, file_parts, folder_data, collection_data):
    xmp_dc = {}
    xmp_dc["title"] = (
        folder_arrangement["folder_display"] + " [image " + file_parts["sequence"] + "]"
    )
    # TODO(tk) check extent type for pages/images/computer files/etc
    if len(folder_data["extents"]) == 1:
        xmp_dc["title"] = (
            xmp_dc["title"].rstrip("]")
            + "/"
            + folder_data["extents"][0]["number"].zfill(4)
            + "]"
        )
    xmp_dc["identifier"] = file_parts["component_id"]
    xmp_dc["publisher"] = folder_arrangement["repository_name"]
    xmp_dc["source"] = (
        folder_arrangement["repository_code"]
        + ": "
        + folder_arrangement["collection_display"]
    )
    for instance in folder_data["instances"]:
        if "sub_container" in instance.keys():
            if (
                "series"
                in instance["sub_container"]["top_container"]["_resolved"].keys()
            ):
                xmp_dc["source"] += (
                    " / "
                    + instance["sub_container"]["top_container"]["_resolved"]["series"][
                        0
                    ]["display_string"]
                )
                for ancestor in folder_data["ancestors"]:
                    if ancestor["level"] == "subseries":
                        xmp_dc["source"] += (
                            " / " + folder_arrangement["subseries_display"]
                        )
    xmp_dc[
        "rights"
    ] = "Caltech Archives has not determined the copyright in this image."
    for note in collection_data["notes"]:
        if note["type"] == "userestrict":
            if bool(note["subnotes"][0]["content"]) and note["subnotes"][0]["publish"]:
                xmp_dc["rights"] = note["subnotes"][0]["content"]
    return xmp_dc


def normalize_directory_component_id(folderpath):
    component_id_parts = os.path.basename(folderpath).split("_")
    if len(component_id_parts) > 3:
        raise ValueError(
            f"The component_id cannot be determined from the directory name: {os.path.basename(folderpath)}"
        )
    collection_id = component_id_parts[0]
    if collection_id != os.path.basename(os.path.dirname(folderpath)):
        raise ValueError(
            f"The directory name does not correspond to the collection_id: {os.path.basename(folderpath)}"
        )
    box_number = component_id_parts[1].lstrip("0")
    # TODO parse non-numeric folder identifiers, like: 03b
    return "_".join([collection_id, box_number.zfill(3), component_id_parts[2]])


def post_digital_object_component(json_data):
    post_response = asnake_client.post(
        "/repositories/2/digital_object_components", json=json_data
    )
    post_response.raise_for_status()
    archivesspace_logger.info(post_response.json()["uri"])
    return post_response


def prepare_digital_object_component(folder_data, PRESERVATION_BUCKET, aip_image_data):
    # MINIMAL REQUIREMENTS: digital_object and one of label, title, or date
    # FILE VERSIONS MINIMAL REQUIREMENTS: file_uri
    # 'publish': false is the default value
    digital_object_component = {
        "file_versions": [
            {
                "checksum_method": "md5",
                "file_format_name": "JPEG 2000",
                "use_statement": "image-master",
            }
        ]
    }
    for instance in folder_data["instances"]:
        # not checking if there is more than one digital object
        if "digital_object" in instance.keys():
            digital_object_component["digital_object"] = {}
            digital_object_component["digital_object"]["ref"] = instance[
                "digital_object"
            ]["_resolved"]["uri"]
    if digital_object_component["digital_object"]["ref"]:
        pass
    # else:
    #     # TODO(tk) figure out what to do if the folder has no digital objects
    #     logging.info("😶 no digital object")
    digital_object_component["component_id"] = aip_image_data["component_id"]
    if (
        aip_image_data["transformation"] == "5-3 reversible"
        and aip_image_data["quantization"] == "no quantization"
    ):
        digital_object_component["file_versions"][0]["caption"] = (
            "width: "
            + aip_image_data["width"]
            + "; height: "
            + aip_image_data["height"]
            + "; compression: lossless"
        )
        digital_object_component["file_versions"][0]["file_format_version"] = (
            aip_image_data["standard"]
            + "; lossless (wavelet transformation: 5/3 reversible with no quantization)"
        )
    elif (
        aip_image_data["transformation"] == "9-7 irreversible"
        and aip_image_data["quantization"] == "scalar expounded"
    ):
        digital_object_component["file_versions"][0]["caption"] = (
            "width: "
            + aip_image_data["width"]
            + "; height: "
            + aip_image_data["height"]
            + "; compression: lossy"
        )
        digital_object_component["file_versions"][0]["file_format_version"] = (
            aip_image_data["standard"]
            + "; lossy (wavelet transformation: 9/7 irreversible with scalar expounded quantization)"
        )
    else:
        digital_object_component["file_versions"][0]["caption"] = (
            "width: "
            + aip_image_data["width"]
            + "; height: "
            + aip_image_data["height"]
        )
        digital_object_component["file_versions"][0][
            "file_format_version"
        ] = aip_image_data["standard"]
    digital_object_component["file_versions"][0]["checksum"] = aip_image_data[
        "md5"
    ].hexdigest()
    digital_object_component["file_versions"][0]["file_size_bytes"] = int(
        aip_image_data["filesize"]
    )
    digital_object_component["file_versions"][0]["file_uri"] = (
        "https://"
        + PRESERVATION_BUCKET
        + ".s3-us-west-2.amazonaws.com/"
        + aip_image_data["s3key"]
    )
    digital_object_component["label"] = "Image " + aip_image_data["sequence"]
    return digital_object_component


def prepare_filepaths_list(folderpath):
    filepaths = []
    with os.scandir(folderpath) as contents:
        for entry in contents:
            if entry.is_file() and os.path.splitext(entry.path)[1] in [".tif", ".tiff"]:
                filepaths.append(entry.path)
    return filepaths


def prepare_folder_list(collection_directory):
    # `depth = 2` means do not recurse past one set of subdirectories.
    # [collection]/
    # ├── [collection]_[box]_[folder]/
    # │   ├── [directory_not_traversed]/
    # │   │   └── [file_not_included].tiff
    # │   ├── [collection]_[box]_[folder]_[leaf].tiff
    # │   └── [collection]_[box]_[folder]_[leaf].tiff
    # └── [collection]_[box]_[folder]/
    #     ├── [collection]_[box]_[folder]_[leaf].tif
    #     └── [collection]_[box]_[folder]_[leaf].tif
    depth = 2
    filecounter = 0
    folders = []
    for root, dirs, files in os.walk(collection_directory):
        if root[len(collection_directory) :].count(os.sep) == 0:
            for d in dirs:
                folders.append(os.path.join(root, d))
        if root[len(collection_directory) :].count(os.sep) < depth:
            for f in files:
                if os.path.splitext(f)[1] in [".tif", ".tiff"]:
                    filecounter += 1
    filecount = filecounter
    return folders, filecount


def process_aip_image(filepath, collection_data, folder_arrangement, folder_data):
    # cut out only the checksum string for the pixel stream
    # NOTE running this process in the background saves time because
    # the conversion starts soon after in a different subprocess
    cut_cmd = sh.Command(config("CUT_CMD"))
    sha512sum_cmd = sh.Command(config("SHA512SUM_CMD"))
    magick_cmd = sh.Command(config("MAGICK_CMD"))
    sip_image_signature = cut_cmd(
        sha512sum_cmd(
            magick_cmd.stream(
                "-quiet",
                "-map",
                "rgb",
                "-storage-type",
                "short",
                filepath,
                "-",
                _piped=True,
                _bg=True,
            ),
            _bg=True,
        ),
        "-d",
        " ",
        "-f",
        "1",
        _bg=True,
    )
    aip_image_path = os.path.splitext(filepath)[0] + "-LOSSLESS.jp2"
    aip_image_conversion = magick_cmd.convert(
        "-quiet", filepath, "-quality", "0", aip_image_path, _bg=True
    )
    file_parts = get_file_parts(filepath)
    xmp_dc = get_xmp_dc_metadata(
        folder_arrangement, file_parts, folder_data, collection_data
    )
    # catch any conversion errors in order to skip file and continue
    try:
        aip_image_conversion.wait()
    except Exception as e:
        # TODO log unfriendly `str(e)` instead of sending it along
        # EXAMPLE:
        # RAN: /usr/local/bin/magick convert -quiet /path/to/HBF/HBF_001_02/HBF_001_02_00.tif -quality 0 /path/to/HBF/HBF_001_02/HBF_001_02_00-LOSSLESS.jp2
        # STDOUT:
        # STDERR:
        # convert: Cannot read TIFF header. `/path/to/HBF/HBF_001_02/HBF_001_02_00.tif' @ error/tiff.c/TIFFErrors/595.
        # convert: no images defined `/path/to/HBF/HBF_001_02/HBF_001_02_00-LOSSLESS.jp2' @ error/convert.c/ConvertImageCommand/3304.
        raise RuntimeError(str(e))
    write_xmp_metadata(aip_image_path, xmp_dc)
    # cut out only the checksum string for the pixel stream
    aip_image_signature = cut_cmd(
        sha512sum_cmd(
            magick_cmd.stream(
                "-quiet",
                "-map",
                "rgb",
                "-storage-type",
                "short",
                aip_image_path,
                "-",
                _piped=True,
                _bg=True,
            ),
            _bg=True,
        ),
        "-d",
        " ",
        "-f",
        "1",
        _bg=True,
    )
    # TODO change `get_aip_image_data()` to `get_initial_aip_image_data()`
    aip_image_data = get_aip_image_data(aip_image_path)
    sip_image_signature.wait()
    aip_image_signature.wait()
    # verify image signatures match
    if aip_image_signature != sip_image_signature:
        raise RuntimeError(
            f"❌ image signatures did not match: {file_parts['image_id']}"
        )
    aip_image_s3key = get_s3_aip_image_key(
        get_s3_aip_folder_prefix(folder_arrangement, folder_data), file_parts
    )
    # Add more values to `aip_image_data` dictionary.
    aip_image_data["component_id"] = file_parts["component_id"]
    aip_image_data["sequence"] = file_parts["sequence"]
    # TODO change `s3key` to something more generic; also use for tape filepath
    aip_image_data["s3key"] = aip_image_s3key
    return aip_image_data


def process_folder_metadata(folderpath):
    try:
        folder_data = get_folder_data(normalize_directory_component_id(folderpath))
    except ValueError as e:
        raise RuntimeError(str(e))

    try:
        folder_data = confirm_digital_object(folder_data)
    except ValueError as e:
        raise RuntimeError(str(e))

    try:
        folder_data = confirm_digital_object_id(folder_data)
    except HTTPError as e:
        raise RuntimeError(str(e))

    try:
        folder_arrangement = get_folder_arrangement(folder_data)
    except HTTPError as e:
        raise RuntimeError(str(e))

    return folder_arrangement, folder_data


def save_collection_metadata(collection_data, directory):
    filename = os.path.join(
        directory,
        collection_data["id_0"],
        f"{collection_data['id_0']}.json",
    )
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        f.write(json.dumps(collection_data, indent=4))


def save_folder_data(folder_arrangement, folder_data, directory):
    filename = os.path.join(
        directory,
        # TODO rename functions to be more abstract
        get_s3_aip_folder_key(
            get_s3_aip_folder_prefix(folder_arrangement, folder_data),
            folder_data,
        ),
    )
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        f.write(json.dumps(folder_data, indent=4))


def save_preservation_file(source, destination):
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    shutil.copy2(source, destination)


def set_digital_object_id(uri, digital_object_id):
    # raises an HTTPError exception if unsuccessful
    get_response_json = asnake_client.get(uri).json()
    get_response_json["digital_object_id"] = digital_object_id
    post_response = asnake_client.post(uri, json=get_response_json)
    post_response.raise_for_status()
    return


def update_digital_object(uri, data):
    # raises an HTTPError exception if unsuccessful
    response = asnake_client.post(uri, json=data)
    response.raise_for_status()
    archivesspace_logger.info(response.json()["uri"])
    return response


def validate_settings():
    STAGE_2_ORIGINAL_FILES = Path(
        os.path.expanduser(config("STAGE_2_ORIGINAL_FILES"))
    ).resolve(
        strict=True
    )  # NOTE do not create missing `STAGE_2_ORIGINAL_FILES`
    STAGE_3_ORIGINAL_FILES = directory_setup(
        os.path.expanduser(config("STAGE_3_ORIGINAL_FILES"))
    ).resolve(strict=True)
    PRESERVATION_BUCKET = config(
        "PRESERVATION_BUCKET"
    )  # TODO validate access to bucket
    LOSSLESS_PRESERVATION_FILES = directory_setup(
        os.path.expanduser(config("LOSSLESS_PRESERVATION_FILES"))
    ).resolve(strict=True)
    return (
        STAGE_2_ORIGINAL_FILES,
        STAGE_3_ORIGINAL_FILES,
        PRESERVATION_BUCKET,
        LOSSLESS_PRESERVATION_FILES,
    )


def write_xmp_metadata(filepath, metadata):
    # NOTE: except `source` all the dc elements here are keywords in exiftool
    exiftool_cmd = sh.Command(config("EXIFTOOL_CMD"))
    return exiftool_cmd(
        "-title=" + metadata["title"],
        "-identifier=" + metadata["identifier"],
        "-XMP-dc:source=" + metadata["source"],
        "-publisher=" + metadata["publisher"],
        "-rights=" + metadata["rights"],
        "-overwrite_original",
        filepath,
    )


def loop_over_collection_subdirectories(variables):
    """Loop over subdirectories in `collection_directory`.

    Example:
    _data/HBF <-- looping over folders under here
    ├── HBF_000_XX
    ├── HBF_001_02
    │   ├── HBF_001_02_01.tif
    │   ├── HBF_001_02_02.tif
    │   ├── HBF_001_02_03.tif
    │   └── HBF_001_02_04.tif
    └── HBF_007_08
    """
    variables["folders"], variables["filecount"] = prepare_folder_list(
        variables["collection_directory"]
    )
    variables["folders"].sort(reverse=True)
    for _ in range(len(variables["folders"])):
        # Using pop() (and/or range(len()) above) maybe helps to be sure that
        # if folder metadata fails to process properly, it and its images are
        # skipped completely and the script moves on to the next folder.
        variables["folderpath"] = variables["folders"].pop()
        # Set up list of TIFF paths for the current folder.
        variables["filepaths"] = prepare_filepaths_list(variables["folderpath"])
        # Avoid processing folder when there are no files.
        if not variables["filepaths"]:
            continue
        (
            variables["folder_arrangement"],
            variables["folder_data"],
        ) = process_folder_metadata(variables["folderpath"])

        if variables["onsite"] and config("ONSITE_MEDIUM"):
            # Import a module named the same as the ONSITE_MEDIUM setting.
            onsite_medium = __import__(config("ONSITE_MEDIUM"))
            onsite_medium.process_during_subdirectories_loop(variables)

        """
        if variables["cloud"] and config("CLOUD_PLATFORM"):
            # Import a module named the same as the CLOUD_PLATFORM setting.
            cloud_platform = __import__(config("CLOUD_PLATFORM"))
            cloud_platform.process_during_subdirectories_loop(variables)

        if variables["access"] and config("ACCESS_PLATFORM"):
            # Import a module named the same as the ACCESS_PLATFORM setting.
            access_platform = __import__(config("ACCESS_PLATFORM"))
            access_platform.process_during_subdirectories_loop(variables)
        """

        loop_over_digital_files(variables)


def loop_over_digital_files(variables):
    """Loop over files in `collection_directory` subdirectory.

    Example:
    _data/HBF
    ├── HBF_000_XX
    ├── HBF_001_02 <-- looping over files under here
    │   ├── HBF_001_02_01.tif
    │   ├── HBF_001_02_02.tif
    │   ├── HBF_001_02_03.tif
    │   └── HBF_001_02_04.tif
    └── HBF_007_08
    """
    # NOTE: We reverse the sort for use with pop() and so the components
    # will be ingested in the correct order for the digital object tree.
    filepaths = variables["filepaths"]
    filepaths.sort(reverse=True)
    for f in range(len(filepaths)):
        filepath = filepaths.pop()
        try:
            # start this in the background
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    process_aip_image,
                    filepath,
                    variables["collection_data"],
                    variables["folder_arrangement"],
                    variables["folder_data"],
                )
                # run a loop checking for it to be done
                # indicate processing by printing a dot every second to the web
                iteration = 0
                while "state=running" in str(future):
                    time.sleep(1)
                    with open(variables["stream_path"], "a", newline="") as f:
                        f.write(".")
                    iteration += 1
                variables["preservation_image_data"] = future.result()
            with open(variables["stream_path"], "a") as f:
                f.write(
                    f"\n✅ Successfully converted {os.path.basename(filepath)} to JPEG 2000.\n"
                )
        except RuntimeError as e:
            message = (
                f"\n⚠️ There was a problem converting {os.path.basename(filepath)} to JPEG 2000.\n"
                f"↩️ Skipping {os.path.basename(filepath)} file.\n"
            )
            with open(variables["stream_path"], "a") as f:
                f.write(message)
            # logging.warning(message, exc_info=True)
            continue

        if variables["onsite"] and config("ONSITE_MEDIUM"):
            # Import a module named the same as the ONSITE_MEDIUM setting.
            onsite_medium = __import__(config("ONSITE_MEDIUM"))
            onsite_medium.process_during_files_loop(variables)

        # Remove generated `*-LOSSLESS.jp2` file.
        try:
            os.remove(variables["preservation_image_data"]["filepath"])
        except OSError as e:
            message = f'⚠️ Unable to remove {variables["preservation_image_data"]["filepath"]} file.\n'
            # logging.warning(message, exc_info=True)
            continue


if __name__ == "__main__":
    plac.call(distill)
