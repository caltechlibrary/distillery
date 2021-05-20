# CALTECH ARCHIVES AND SPECIAL COLLECTIONS
# digital object preservation workflow

# processing functionality; see distillery.py for bottlepy web application

import base64
import concurrent.futures
import hashlib
import json
import os
import pprint
import random
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

if __debug__:
    from sidetrack import set_debug, log, logr

# TODO do we need a class? https://stackoverflow.com/a/16502408/4100024
# we have 8 functions that need an authorized connection to ArchivesSpace
asnake_client = ASnakeClient(
    baseurl=config("ASPACE_BASEURL"),
    username=config("ASPACE_USERNAME"),
    password=config("ASPACE_PASSWORD"),
)
asnake_client.authorize()

s3_client = boto3.client(
    "s3",
    aws_access_key_id=config("AWS_ACCESS_KEY"),
    aws_secret_access_key=config("AWS_SECRET_KEY"),
)


def distill(collection_id: "the Collection ID from ArchivesSpace"):

    time_start = datetime.now()

    try:
        (
            SOURCE_DIRECTORY,
            COMPLETED_DIRECTORY,
            PRESERVATION_BUCKET,
        ) = validate_settings()
    except Exception as e:
        # NOTE we cannot guarantee that `STATUS_FILES_DIR` is set
        # - it must exist if script is started from `watcher.sh`
        # TODO figure out how to not send a message every minute
        message = "❌ there was a problem with the settings for the processing script"
        print(message)
        # TODO set up notify
        # subprocess.run(["/bin/bash", "./notify.sh", str(e), message])
        raise

    stream_path = Path(config("STATUS_FILES_DIR")).joinpath(f"{collection_id}-processing")

    # TODO this is an example of how to write to the processing file
    # repeat this whereever we are yielding
    iteration = 0
    while iteration < 10:
        with open(stream_path, "a") as f:
            f.write(f"{collection_id} {iteration}\n")
        # print(f"{collection_id} {iteration}")
        time.sleep(1)
        iteration += 1
    # TODO this is to be run at the very end of the process, delete the file
    stream_path.unlink(missing_ok=True)

    # TODO refactor so that we can get an initial report on the results of both
    # the directory and the uri so that users can know if one or both of the
    # points of failure are messed up right away

    try:
        collection_directory = get_collection_directory(SOURCE_DIRECTORY, collection_id)
        if collection_directory:
            # yield f"✅ Collection directory for {collection_id} found on filesystem: {collection_directory}\n"
            with open(stream_path, "a") as f:
                f.write(f"✅ Collection directory for {collection_id} found on filesystem: {collection_directory}\n")
        # TODO report on contents of collection_directory
    except NotADirectoryError as e:
        # TODO write friendly message to stream
        # TODO write traceback to some log file
        # TODO notify DLD

        # yield f"⚠️ {str(e)}\n"
        # yield "❌ exiting…\n"
        # yield "<p><a href='/'>return to form</a>"
        # TODO figure out how to not send a message every minute
        message = "❌ there was a problem with the settings for the processing script"
        print(message)
        # TODO set up notify
        # subprocess.run(["/bin/bash", "./notify.sh", str(e), message])
        raise

    sys.exit()

    try:
        collection_uri = get_collection_uri(collection_id)
        if collection_uri:
            yield f"✅ Collection URI for {collection_id} found in ArchivesSpace: {collection_uri}\n"
    except ValueError as e:
        yield f"⚠️ {str(e)}\n"
        yield "❌ exiting…\n"
        yield "<p><a href='/'>return to form</a>"
        sys.exit()
    except HTTPError as e:
        yield f"⚠️ There was a problem with the connection to ArchivesSpace.\n"
        yield "❌ exiting…\n"
        yield "<p><a href='/'>return to form</a>"
        sys.exit()

    try:
        collection_data = get_collection_data(collection_uri)
        if collection_data:
            yield f"✅ Collection data for {collection_id} retrieved from ArchivesSpace.\n"
        # TODO report on contents of collection_data
    except RuntimeError as e:
        yield f"⚠️ {str(e)}\n"
        yield "❌ exiting…\n"
        yield "<p><a href='/'>return to form</a>"
        sys.exit()
    except HTTPError as e:
        yield f"⚠️ There was a problem with the connection to ArchivesSpace.\n"
        yield "❌ exiting…\n"
        yield "<p><a href='/'>return to form</a>"
        sys.exit()

    try:
        collection_data["tree"]["_resolved"] = get_collection_tree(collection_uri)
        if collection_data["tree"]["_resolved"]:
            yield f"✅ Collection tree for {collection_id} retrieved from ArchivesSpace.\n"
        # TODO report on contents of collection_tree
    except RuntimeError as e:
        yield f"⚠️ {str(e)}\n"
        yield "❌ exiting…\n"
        yield "<p><a href='/'>return to form</a>"
        sys.exit()
    except HTTPError as e:
        yield f"⚠️ There was a problem with the connection to ArchivesSpace.\n"
        yield "❌ exiting…\n"
        yield "<p><a href='/'>return to form</a>"
        sys.exit()

    # Verify write permission on `COMPLETED_DIRECTORY` by saving collection metadata.
    try:
        save_collection_metadata(collection_data, COMPLETED_DIRECTORY)
        yield f"✅ Collection metadata for {collection_id} saved to: {COMPLETED_DIRECTORY}/{collection_id}.json\n"
    except OSError as e:
        yield f"⚠️ Unable to save {collection_id}.json file to: {COMPLETED_DIRECTORY}\n"
        yield "❌ exiting…\n"
        yield "<p><a href='/'>return to form</a>"
        sys.exit()

    # Send collection metadata to S3.
    try:
        s3_client.put_object(
            Bucket=PRESERVATION_BUCKET,
            Key=collection_id + "/" + collection_id + ".json",
            Body=json.dumps(collection_data, sort_keys=True, indent=4),
        )
        yield f"✅ Collection metadata for {collection_id} sent to {PRESERVATION_BUCKET} on S3.\n"
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InternalError":
            yield f"⚠️ Unable to send collection metadata for {collection_id} to {PRESERVATION_BUCKET} on S3.\n"
            yield f"Error Message: {e.response['Error']['Message']}\n"
            yield f"Request ID: {e.response['ResponseMetadata']['RequestId']}\n"
            yield f"HTTP Code: {e.response['ResponseMetadata']['HTTPStatusCode']}\n"
            yield "❌ exiting…\n"
            yield "<p><a href='/'>return to form</a>"
            sys.exit()
        else:
            raise e

    folders, filecount = prepare_folder_list(collection_directory)
    filecounter = filecount

    # Loop over folders list.
    folders.sort(reverse=True)
    for _ in range(len(folders)):
        # Using pop() (and/or range(len()) above) maybe helps to be sure that
        # if folder metadata fails to process properly, it and its images are
        # skipped completely and the script moves on to the next folder.
        folderpath = folders.pop()
        # TODO find out how to properly catch exceptions here
        try:
            folder_arrangement, folder_data = process_folder_metadata(folderpath)
            yield f"✅ Folder data for {folder_data['component_id']} [{folder_data['display_string']}] retrieved from ArchivesSpace.\n"
        except RuntimeError as e:
            yield f"⚠️ Unable to retrieve metadata for: {folderpath}\n"
            # NOTE possible error strings include:
            # f"The component_id cannot be determined from the directory name: {os.path.basename(folderpath)}"
            # f"The directory name does not correspond to the collection_id: {os.path.basename(folderpath)}"
            # f"No records found with component_id: {component_id}"
            # f"Multiple records found with component_id: {component_id}"
            # f"The ArchivesSpace record for {folder_data['component_id']} contains multiple digital objects."
            # f"Missing collection data for: {folder_data['component_id']}"
            # f"Sub-Series record is missing component_id: {subseries['display_string']} {ancestor['ref']}"
            # f"Missing series data for: {folder_data['component_id']}"
            yield f"⚠️ {str(e)}\n"
            yield f"↩️ …skipping: {folderpath}\n"
            # TODO increment file counter by the count of files in this folder
            continue

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
            yield f"✅ Folder metadata for {folder_data['component_id']} sent to {PRESERVATION_BUCKET} on S3.\n"
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "InternalError":
                yield f"⚠️ Unable to send folder metadata for {folder_data['component_id']} to {PRESERVATION_BUCKET} on S3.\n"
                yield f"Error Message: {e.response['Error']['Message']}\n"
                yield f"Request ID: {e.response['ResponseMetadata']['RequestId']}\n"
                yield f"HTTP Code: {e.response['ResponseMetadata']['HTTPStatusCode']}\n"
                yield f"↩️ …skipping: {folderpath}\n"
                continue
            else:
                raise e

        # Set up list of TIFF paths for the current folder.
        filepaths = prepare_filepaths_list(folderpath)

        # NOTE: We reverse the sort for use with pop() and so the components
        # will be ingested in the correct order for the digital object tree.
        filepaths.sort(reverse=True)
        for f in range(len(filepaths)):
            filepath = filepaths.pop()
            filecounter -= 1
            try:
                yield f"⏳ Converting {os.path.basename(filepath)} to JPEG 2000."
                # yield f"⏱ {datetime.now()}\n"
                # start this in the background
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        process_aip_image,
                        filepath,
                        collection_data,
                        folder_arrangement,
                        folder_data,
                    )
                    # DEBUG
                    print("DEBUG:")
                    print(str(future))
                    print(":GUBED")
                    # run a loop checking for it to be done
                    iteration = 0
                    while "state=running" in str(future):
                        time.sleep(1)
                        # NOTE: the following `yield` must begin with `\n`
                        yield "."
                        iteration += 1
                    aip_image_data = future.result()
                # aip_image_data = process_aip_image(
                #     filepath, collection_data, folder_arrangement, folder_data
                # )
                yield f"\n✅ Successfully converted {os.path.basename(filepath)} to JPEG&nbsp;2000. [images remaining: {filecounter}/{filecount}]\n"
                # yield f"⏱ {datetime.now()}\n"
            except RuntimeError as e:
                yield f"\n⚠️ There was a problem converting {os.path.basename(filepath)} to JPEG 2000.\n"
                yield f"⚠️ {str(e)}\n"
                yield f"↩️ Skipping: {os.path.basename(filepath)} [images remaining: {filecounter}/{filecount}]\n"
                continue

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
                yield f"☁️ Sending JPEG 2000 for {Path(filepath).stem} to {PRESERVATION_BUCKET} on S3.\n"
                with open(aip_image_data["filepath"], "rb") as body:
                    aip_image_put_response = s3_client.put_object(
                        Bucket=PRESERVATION_BUCKET,
                        Key=aip_image_data["s3key"],
                        Body=body,
                        ContentMD5=base64.b64encode(
                            aip_image_data["md5"].digest()
                        ).decode(),
                    )
                yield f"✅ Sent JPEG 2000 for {Path(filepath).stem} to {PRESERVATION_BUCKET} on S3.\n"
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "InternalError":
                    yield f"⚠️ Unable to send JPEG 2000 for {Path(filepath).stem} to {PRESERVATION_BUCKET} on S3.\n"
                    yield f"Error Message: {e.response['Error']['Message']}\n"
                    yield f"Request ID: {e.response['ResponseMetadata']['RequestId']}\n"
                    yield f"HTTP Code: {e.response['ResponseMetadata']['HTTPStatusCode']}\n"
                    yield f"↩️ …skipping: {Path(filepath).stem}\n"
                    # TODO cleanup
                    continue
                else:
                    raise e

            # Verify S3 ETag.
            if (
                aip_image_put_response["ETag"].strip('"')
                == aip_image_data["md5"].hexdigest()
            ):
                yield f"✅ Verified checksums for JPEG 2000 of {Path(filepath).stem} sent to {PRESERVATION_BUCKET} on S3.\n"
            else:
                raise ValueError(
                    f"❌ the S3 ETag did not match for {aip_image_data['filepath']}"
                )

            # Set up ArchivesSpace record.
            digital_object_component = prepare_digital_object_component(
                folder_data, PRESERVATION_BUCKET, aip_image_data
            )

            # Post Digital Object Component to ArchivesSpace.
            try:
                post_digital_object_component(digital_object_component)
            except HTTPError as e:
                print(str(e))
                print(
                    f"❌ unable to create Digital Object Component for {folder_data['component_id']}; skipping...\n"
                )
                print(
                    f"⚠️  clean up {aip_image_data['s3key']} file in {PRESERVATION_BUCKET} bucket\n"
                )
                # TODO programmatically remove file from bucket?
                continue

            # Move processed source file into `COMPLETED_DIRECTORY` with the structure
            # under `SOURCE_DIRECTORY` (the `+ 1` strips a path seperator).
            try:
                os.renames(
                    filepath,
                    os.path.join(
                        COMPLETED_DIRECTORY, filepath[len(SOURCE_DIRECTORY) + 1 :]
                    ),
                )
            except OSError as e:
                print(str(e))
                print(f"⚠️  unable to move {filepath} to {COMPLETED_DIRECTORY}/\n")
                continue

            # Remove generated `*-LOSSLESS.jp2` file.
            try:
                os.remove(aip_image_data["filepath"])
            except OSError as e:
                print(str(e))
                print(f"⚠️  unable to remove {aip_image_data['filepath']}\n")
                continue

            print(f"✅ {os.path.basename(filepath)} processed successfully\n")

            print(f"⏳ time elpased: {datetime.now() - time_start}\n")


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
                if __debug__:
                    log(
                        f"❇️  updated digital_object_id: {instance['digital_object']['_resolved']['digital_object_id']} ➡️  {folder_data['component_id']} {instance['digital_object']['ref']}"
                    )
    return folder_data


def confirm_file(filepath):
    # confirm file exists and has the proper extention
    # valid extensions are: .tif, .tiff
    # NOTE: no mime type checking at this point, some TIFFs were troublesome
    if os.path.isfile(filepath):
        # print(os.path.splitext(filepath)[1])
        if os.path.splitext(filepath)[1] not in [".tif", ".tiff"]:
            print("❌  invalid file type: " + filepath)
            exit()
    else:
        print("❌  invalid file path: " + filepath)
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

    if __debug__:
        log(
            f"✳️  created digital object {digital_object['digital_object_id']} {digital_object_post_response.json()['uri']}"
        )
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
        "./properties/contiguousCodestream_pathBox/siz/rsiz"
    )
    aip_image_data["transformation"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestream_pathBox/cod/transformation"
    )
    aip_image_data["quantization"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestream_pathBox/qcd/qStyle"
    )
    with open(aip_image_data["filepath"], "rb") as f:
        aip_image_data["md5"] = hashlib.md5(f.read())
    return aip_image_data


def get_archival_object(id):
    response = asnake_client.get("/repositories/2/archival_objects/" + id)
    response.raise_for_status()
    return response.json()


def get_collection_data(collection_uri):
    # raises an HTTPError exception if unsuccessful
    collection_data = asnake_client.get(collection_uri).json()
    if collection_data:
        return collection_data
    else:
        raise RuntimeError(
            f"There was a problem retrieving the collection data from ArchivesSpace.\n"
        )


def get_collection_directory(SOURCE_DIRECTORY, collection_id):
    if os.path.isdir(os.path.join(SOURCE_DIRECTORY, collection_id)):
        return os.path.join(SOURCE_DIRECTORY, collection_id)
    else:
        raise NotADirectoryError(
            f"Missing or invalid collection directory: {os.path.join(SOURCE_DIRECTORY, collection_id)}\n"
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
        '/repositories/2/search?page=1&page_size=1&type[]=resource&fields[]=uri&aq={"query":{"field":"identifier","value":"'
        + collection_id
        + '","jsonmodel_type":"field_query","negated":false,"literal":false}}'
    ).json()
    if len(search_results_json["results"]) < 1:
        raise ValueError(
            f"No collection found in ArchivesSpace with the ID: {collection_id}\n"
        )
    elif len(search_results_json["results"]) > 1:
        raise ValueError(
            f"Multiple collections found in ArchivesSpace with the ID: {collection_id}\n"
        )
    else:
        return search_results_json["results"][0]["uri"]


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
    file_parts["sequence"] = file_parts["image_id"].split("_")[-1]
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
                if __debug__:
                    print(
                        f"👀 series: {instance['sub_container']['top_container']['_resolved']['series']}"
                    )
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
    #     "filepath": "/path/to/archives/data/SOURCE_DIRECTORY/HaleGE/HaleGE_02_0B_056_07_0001.tiff",
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
        + "-lossless.jp2"
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
    else:
        # TODO(tk) figure out what to do if the folder has no digital objects
        print("😶 no digital object")
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
    print(f"⏱ {datetime.now()} sip_image_signature")
    # cut out only the checksum string for the pixel stream_path
    # NOTE running this process in the background saves time because
    # the conversion starts soon after in a different subprocess
    sip_image_signature = sh.cut(
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
    print(f"⏱ {datetime.now()} aip_image_path")
    aip_image_path = os.path.splitext(filepath)[0] + "-LOSSLESS.jp2"
    print(f"⏱ {datetime.now()} aip_image_conversion")
    aip_image_conversion = sh.magick.convert(
        "-quiet", filepath, "-quality", "0", aip_image_path, _bg=True
    )
    print(f"⏱ {datetime.now()} file_parts")
    file_parts = get_file_parts(filepath)
    # if __debug__: log('file_parts ⬇️'); print(json.dumps(file_parts, sort_keys=True, indent=4))
    print(f"⏱ {datetime.now()} xmp_dc")
    xmp_dc = get_xmp_dc_metadata(
        folder_arrangement, file_parts, folder_data, collection_data
    )
    # catch any conversion errors in order to skip file and continue
    try:
        print(f"⏱ {datetime.now()} aip_image_conversion.wait")
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
    print(f"⏱ {datetime.now()} write_xmp_metadata")
    write_xmp_metadata(aip_image_path, xmp_dc)
    # cut out only the checksum string for the pixel stream_path
    print(f"⏱ {datetime.now()} aip_image_signature")
    aip_image_signature = sh.cut(
        sh.sha512sum(
            sh.magick.stream_path(
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
    print(f"⏱ {datetime.now()} aip_image_data")
    aip_image_data = get_aip_image_data(aip_image_path)
    print(f"⏱ {datetime.now()} sip_image_signature.wait")
    sip_image_signature.wait()
    print(f"⏱ {datetime.now()} aip_image_signature.wait")
    aip_image_signature.wait()
    # verify image signatures match
    print(f"⏱ {datetime.now()} if aip_image_signature != sip_image_signature")
    if aip_image_signature != sip_image_signature:
        raise RuntimeError(
            f"❌ image signatures did not match: {file_parts['image_id']}"
        )
    print(f"⏱ {datetime.now()} aip_image_s3key")
    aip_image_s3key = get_s3_aip_image_key(
        get_s3_aip_folder_prefix(folder_arrangement, folder_data), file_parts
    )
    # if __debug__: log(f'🔑 aip_image_s3key: {aip_image_s3key}')
    # Add more values to `aip_image_data` dictionary.
    aip_image_data["component_id"] = file_parts["component_id"]
    aip_image_data["sequence"] = file_parts["sequence"]
    aip_image_data["s3key"] = aip_image_s3key
    # if __debug__: log('aip_image_data ⬇️'); print(json.dumps(aip_image_data, sort_keys=True, indent=4))
    print(f"⏱ {datetime.now()} return")
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


def save_collection_metadata(collection_data, COMPLETED_DIRECTORY):
    filename = os.path.join(
        COMPLETED_DIRECTORY, collection_data["id_0"], f"{collection_data['id_0']}.json"
    )
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        f.write(json.dumps(collection_data, indent=4))


def set_digital_object_id(uri, digital_object_id):
    # raises an HTTPError exception if unsuccessful
    get_response_json = asnake_client.get(uri).json()
    get_response_json["digital_object_id"] = digital_object_id
    post_response = asnake_client.post(uri, json=get_response_json)
    post_response.raise_for_status()
    return


def validate_settings():
    SOURCE_DIRECTORY = Path(os.path.expanduser(config("SOURCE_DIRECTORY"))).resolve(
        strict=True
    )  # NOTE do not create missing `SOURCE_DIRECTORY`
    COMPLETED_DIRECTORY = directory_setup(
        os.path.expanduser(config("COMPLETED_DIRECTORY", f"{SOURCE_DIRECTORY}/S3"))
    ).resolve(strict=True)
    PRESERVATION_BUCKET = config(
        "PRESERVATION_BUCKET"
    )  # TODO validate access to bucket
    return SOURCE_DIRECTORY, COMPLETED_DIRECTORY, PRESERVATION_BUCKET


def write_xmp_metadata(filepath, metadata):
    # NOTE: except `source` all the dc elements here are keywords in exiftool
    return sh.exiftool(
        "-title=" + metadata["title"],
        "-identifier=" + metadata["identifier"],
        "-XMP-dc:source=" + metadata["source"],
        "-publisher=" + metadata["publisher"],
        "-rights=" + metadata["rights"],
        "-overwrite_original",
        filepath,
    )


if __name__ == "__main__":
    plac.call(distill)
