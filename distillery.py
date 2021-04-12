# CALTECH ARCHIVES AND SPECIAL COLLECTIONS DIGITAL OBJECT WORKFLOW

import base64
import hashlib
import json
import os
import pprint
import random
import string
import sys
from datetime import datetime
from pathlib import Path
from requests import HTTPError

import boto3
import botocore
import bottle
import sh
from asnake.client import ASnakeClient
from decouple import config
from jpylyzer import jpylyzer

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


@bottle.get("/")
def formview():
    return bottle.template("form_collection_id", error=None)


@bottle.post("/")
def formpost():
    collection_id = bottle.request.forms.get("collection_id").strip()
    # TODO refactor because main() is a long-running process \
    # and the form hangs on submission because the whole function must finish
    if collection_id:
        return main(collection_id)
    else:
        return bottle.template(
            "status", status="⚠️ <em>CollectionID</em> must not be empty."
        )


@bottle.error(200)
def problem(error):
    # NOTE this captures the abort() in a function that fails
    # TODO create a different template for environment errors
    return bottle.template("form_collection_id", error=error.body)


def main(collection_id, debug=False):

    if debug:
        if __debug__:
            set_debug(True)

    time_start = datetime.now()
    yield '<style type="text/css">* {white-space:pre-wrap;}</style>'

    # TODO move outside of main to validate ASPACE & AWS variables
    try:
        (
            SOURCE_DIRECTORY,
            COMPLETED_DIRECTORY,
            PRESERVATION_BUCKET,
        ) = validate_settings()
    except Exception as e:
        yield f"⚠️ there was a problem with the configuration settings\n"
        yield f"➡️ <em>{str(e)}</em>\n"
        yield "❌ exiting…\n"
        yield "<p>this issue must be resolved before continuing</a>"
        # TODO send notification to DLD
        sys.exit()

    # TODO refactor so that we can get an initial report on the results of both
    # the directory and the uri so that users can know if one or both of the
    # points of failure are messed up right away

    # TODO: change these function calls to try/except so that we \
    # don't have to change the function definitions for bottle

    try:
        collection_directory = get_collection_directory(SOURCE_DIRECTORY, collection_id)
        if collection_directory:
            yield f"✅ collection directory found on filesystem: {collection_directory}\n"
        # TODO report on contents of collection_directory
    except NotADirectoryError as e:
        yield f"⚠️ {str(e)}\n"
        yield "❌ exiting…\n"
        yield "<p><a href='/'>return to form</a>"
        sys.exit()

    collection_uri = get_collection_uri(collection_id)
    collection_data = get_collection_data(collection_uri)
    collection_data["tree"]["_resolved"] = get_collection_tree(collection_uri)
    yield f"✅ collection data gathered for {collection_id}\n"

    # Verify write permission on `COMPLETED_DIRECTORY` by saving collection metadata.
    # TODO how to check bucket write permission without writing?
    try:
        save_collection_metadata(collection_data, COMPLETED_DIRECTORY)
    except OSError as e:
        print(str(e))
        print(f"❌  unable to save file to {COMPLETED_DIRECTORY}\n")
        exit()

    # Send collection metadata to S3.
    try:
        s3_client.put_object(
            Bucket=PRESERVATION_BUCKET,
            Key=collection_id + "/" + collection_id + ".json",
            Body=json.dumps(collection_data, sort_keys=True, indent=4),
        )
        # print(f"✅ metadata sent to S3 for {collection_id}\n")
        yield f"✅ metadata sent to S3 for {collection_id}\n"
    except botocore.exceptions.ClientError as e:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
        if e.response["Error"]["Code"] == "InternalError":  # Generic error
            # We grab the message, request ID, and HTTP code to give to customer support
            print(f"Error Message: {e.response['Error']['Message']}")
            print(f"Request ID: {e.response['ResponseMetadata']['RequestId']}")
            print(f"HTTP Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
        else:
            raise e
    yield "done"
    return
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
        except RuntimeError as e:
            print(str(e))
            print("❌ unable to process folder metadata...")
            print(f"...skipping {folderpath}\n")
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
            print(f"✅ metadata sent to S3 for {folder_data['component_id']}\n")
        except botocore.exceptions.ClientError as e:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
            if e.response["Error"]["Code"] == "InternalError":  # Generic error
                # We grab the message, request ID, and HTTP code to give to customer support
                print(f"Error Message: {e.response['Error']['Message']}")
                print(f"Request ID: {e.response['ResponseMetadata']['RequestId']}")
                print(f"HTTP Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
                continue
            else:
                raise e

        # Set up list of TIFF paths for the current folder.
        filepaths = prepare_filepaths_list(folderpath)

        # We reverse the sort for use with pop() and so the components will be
        # ingested in the correct order for the digital object tree
        filepaths.sort(reverse=True)
        for f in range(len(filepaths)):
            filepath = filepaths.pop()
            print(
                f"▶️  {os.path.basename(filepath)} [images remaining: {filecounter}/{filecount}]"
            )
            filecounter -= 1
            try:
                aip_image_data = process_aip_image(
                    filepath, collection_data, folder_arrangement, folder_data
                )
            except RuntimeError as e:
                print(str(e))
                continue

            # Send AIP image to S3.
            # example success response:
            # {
            #     "ResponseMetadata": {
            #         "RequestId": "6BBE41DE8A1CABCE",
            #         "HostId": "c473fwfRMo+soCkOUwMsNZwR5fw0RIw2qcDVIXQOXVm1aGLV5clcL8JgBXojEJL99Umo4HYEzng=",
            #         "HTTPStatusCode": 200,
            #         "HTTPHeaders": {
            #             "x-amz-id-2": "c473fwfRMo+soCkOUwMsNZwR5fw0RIw2qcDVIXQOXVm1aGLV5clcL8JgBXojEJL99Umo4HYEzng=",
            #             "x-amz-request-id": "6BBE41DE8A1CABCE",
            #             "date": "Mon, 30 Nov 2020 22:58:33 GMT",
            #             "etag": "\"614bccea2760f37f41be65c62c41d66e\"",
            #             "content-length": "0",
            #             "server": "AmazonS3"
            #         },
            #         "RetryAttempts": 0
            #     },
            #     "ETag": "\"614bccea2760f37f41be65c62c41d66e\""
            # }
            try:
                aip_image_put_response = s3_client.put_object(
                    Bucket=PRESERVATION_BUCKET,
                    Key=aip_image_data["s3key"],
                    Body=open(aip_image_data["filepath"], "rb"),
                    ContentMD5=base64.b64encode(
                        aip_image_data["md5"].digest()
                    ).decode(),
                )
            except botocore.exceptions.ClientError as e:
                # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
                if e.response["Error"]["Code"] == "InternalError":  # Generic error
                    # We grab the message, request ID, and HTTP code to give to customer support
                    print(f"Error Message: {e.response['Error']['Message']}")
                    print(f"Request ID: {e.response['ResponseMetadata']['RequestId']}")
                    print(
                        f"HTTP Code: {e.response['ResponseMetadata']['HTTPStatusCode']}"
                    )
                    continue
                else:
                    raise e

            # Verify S3 ETag.
            if (
                aip_image_put_response["ETag"].strip('"')
                == aip_image_data["md5"].hexdigest()
            ):
                pass
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
            sh.magick.stream(
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
            f"❌ the ArchivesSpace record for {folder_data['component_id']} contains multiple digital objects"
        )
    if digital_object_count < 1:
        # returns new folder_data with digital object info included
        folder_data = create_digital_object(folder_data)
    return folder_data


def confirm_digital_object_id(folder_data):
    # returns folder_data always in case digital_object_id was updated
    for instance in folder_data["instances"]:
        # TODO(tk) confirm Archives policy disallows multiple digital objects
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
        "./properties/contiguousCodestreamBox/siz/rsiz"
    )
    aip_image_data["transformation"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestreamBox/cod/transformation"
    )
    aip_image_data["quantization"] = jpylyzer_xml.findtext(
        "./properties/contiguousCodestreamBox/qcd/qStyle"
    )
    aip_image_data["md5"] = hashlib.md5(open(aip_image_data["filepath"], "rb").read())
    return aip_image_data


def get_archival_object(id):
    response = asnake_client.get("/repositories/2/archival_objects/" + id)
    response.raise_for_status()
    return response.json()


def get_collection_directory(SOURCE_DIRECTORY, collection_id):
    if os.path.isdir(os.path.join(SOURCE_DIRECTORY, collection_id)):
        return os.path.join(SOURCE_DIRECTORY, collection_id)
    else:
        raise NotADirectoryError(
            f"missing or invalid collection directory: {os.path.join(SOURCE_DIRECTORY, collection_id)}"
        )


def get_collection_data(collection_uri):
    return asnake_client.get(collection_uri).json()


def get_collection_tree(collection_uri):
    return asnake_client.get(collection_uri + "/ordered_records").json()


def get_collection_uri(collection_id):
    search_results_json = asnake_client.get(
        '/repositories/2/search?page=1&page_size=1&type[]=resource&fields[]=uri&aq={"query":{"field":"identifier","value":"'
        + collection_id
        + '","jsonmodel_type":"field_query","negated":false,"literal":false}}'
    ).json()
    # TODO raise exception for multiple results
    # TODO friendly webform error if search field is empty
    if bool(search_results_json["results"]):
        return search_results_json["results"][0]["uri"]
    else:
        bottle.abort(
            200, f" ❌\t CollectionID not found in ArchivesSpace: {collection_id}"
        )


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
                    f" ⚠️\t missing collection data for {folder_data['component_id']}"
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
                                f" ⚠️\t Sub-Series record is missing component_id: {subseries['display_string']} {ancestor['ref']}"
                            )
            else:
                if __debug__:
                    print(
                        f"👀 series: {instance['sub_container']['top_container']['_resolved']['series']}"
                    )
                raise ValueError(
                    f" ⚠️\t missing series data for {folder_data['component_id']}"
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
        raise ValueError(f" ⚠️\t No records found with component_id: {component_id}")
    if len(find_by_id_response.json()["archival_objects"]) > 1:
        raise ValueError(
            f" ⚠️\t Multiple records found with component_id: {component_id}"
        )
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
            f"⚠️\tThe component_id cannot be determined from the directory name: {os.path.basename(folderpath)}"
        )
    collection_id = component_id_parts[0]
    if collection_id != os.path.basename(os.path.dirname(folderpath)):
        raise ValueError(
            f"⚠️\tThe directory name does not correspond to the collection_id: {os.path.basename(folderpath)}"
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
    # cut out only the checksum string for the pixel stream
    sip_image_signature = sh.cut(
        sh.sha512sum(
            sh.magick.stream(
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
    aip_image_conversion = sh.magick.convert(
        "-quiet", filepath, "-quality", "0", aip_image_path, _bg=True
    )
    file_parts = get_file_parts(filepath)
    # if __debug__: log('file_parts ⬇️'); print(json.dumps(file_parts, sort_keys=True, indent=4))
    xmp_dc = get_xmp_dc_metadata(
        folder_arrangement, file_parts, folder_data, collection_data
    )
    # print(json.dumps(xmp_dc, sort_keys=True, indent=4))
    aip_image_conversion.wait()
    write_xmp_metadata(aip_image_path, xmp_dc)
    # cut out only the checksum string for the pixel stream
    aip_image_signature = sh.cut(
        sh.sha512sum(
            sh.magick.stream(
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
            f"❌  image signatures did not match: {file_parts['image_id']}"
        )
    aip_image_s3key = get_s3_aip_image_key(
        get_s3_aip_folder_prefix(folder_arrangement, folder_data), file_parts
    )
    # if __debug__: log(f'🔑 aip_image_s3key: {aip_image_s3key}')
    # Add more values to `aip_image_data` dictionary.
    aip_image_data["component_id"] = file_parts["component_id"]
    aip_image_data["sequence"] = file_parts["sequence"]
    aip_image_data["s3key"] = aip_image_s3key
    # if __debug__: log('aip_image_data ⬇️'); print(json.dumps(aip_image_data, sort_keys=True, indent=4))
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


###

if __name__ == "__main__":
    bottle.run(host="localhost", port=1234, debug=True, reloader=True)
