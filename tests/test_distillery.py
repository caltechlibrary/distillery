import datetime
import glob
import os
import pytest
import shutil

from decouple import config
from playwright.sync_api import Page, expect


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    return {
        **browser_context_args,
        "http_credentials": {
            "username": config("DISTILLERY_BASIC_AUTH_USERNAME", default=""),
            "password": config("DISTILLERY_BASIC_AUTH_PASSWORD", default=""),
        },
    }


@pytest.fixture(autouse=True)
def distillery_0000_reset_files():
    for d in glob.glob(os.path.join(config("WORKING_ORIGINAL_FILES"), "*/")):
        shutil.move(d, config("INITIAL_ORIGINAL_FILES"))
    for d in glob.glob(os.path.join(config("STAGE_3_ORIGINAL_FILES"), "*/")):
        shutil.move(d, config("INITIAL_ORIGINAL_FILES"))
    for d in glob.glob(os.path.join(config("WORK_PRESERVATION_FILES"), "*/")):
        os.system(f"/bin/rm -r {d}")
    for d in glob.glob(os.path.join(config("COMPRESSED_ACCESS_FILES"), "*/")):
        os.system(f"/bin/rm -r {d}")
    return


@pytest.fixture
def asnake_client():
    from asnake.client import ASnakeClient

    asnake_client = ASnakeClient(
        baseurl=config("ASPACE_API_URL"),
        username=config("ASPACE_USERNAME"),
        password=config("ASPACE_PASSWORD"),
    )
    asnake_client.authorize()
    return asnake_client


@pytest.fixture
def s3_client():
    import boto3

    s3_client = boto3.client(
        "s3",
        region_name=config("DISTILLERY_AWS_REGION", default="us-west-2"),
        aws_access_key_id=config("DISTILLERY_AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=config("DISTILLERY_AWS_SECRET_ACCESS_KEY"),
    )
    return s3_client


# NOTE cleaning up previous tests and then setting up for new tests is not the
# recommended way to do Arrange and Cleanup in pytest, but it allows manually
# viewing the outcome of tests in ArchivesSpace


def delete_archivesspace_test_records(asnake_client, resource_identifer):
    """Delete any existing test records."""

    def delete_digital_objects(uri):
        result = asnake_client.get(uri, params={"resolve[]": "digital_object"}).json()
        for instance in result["instances"]:
            if instance.get("digital_object"):
                digital_object_delete_response = asnake_client.delete(
                    instance["digital_object"]["_resolved"]["uri"]
                )

    def recursive_delete(node, resource_ref):
        if node["child_count"] > 0:
            children = asnake_client.get(
                f'{resource_ref}/tree/waypoint?offset=0&parent_node={node["uri"]}'
            ).json()
            for child in children:
                delete_digital_objects(child["uri"])
                recursive_delete(child, resource_ref)

    resource_find_by_id_results = asnake_client.get(
        "/repositories/2/find_by_id/resources",
        params={"identifier[]": [f'["{resource_identifer}"]']},
    ).json()
    for result in resource_find_by_id_results["resources"]:
        delete_digital_objects(result["ref"])
        resource_tree = asnake_client.get(f'{result["ref"]}/tree/root').json()
        if resource_tree["waypoints"] > 1:
            raise Exception("Test resource has more than one waypoint.")
        resource_children = resource_tree["precomputed_waypoints"][""]["0"]
        for child in resource_children:
            delete_digital_objects(child["uri"])
            recursive_delete(child, result["ref"])
        asnake_client.delete(result["ref"])


def test_distillery_alchemist_date_output_x2edw(page: Page, asnake_client):
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, "DistilleryTEST-x2edw")
    # CREATE COLLECTION RECORD
    resource = {}
    # NOTE required
    resource["title"] = "_Minim exercitation enim nulla x2edw"
    resource["id_0"] = "DistilleryTEST-x2edw"
    resource["level"] = "collection"
    resource["finding_aid_language"] = "eng"
    resource["finding_aid_script"] = "Latn"
    resource["lang_materials"] = [
        {"language_and_script": {"language": "eng", "script": "Latn"}}
    ]
    resource["dates"] = [
        {
            "label": "creation",
            "date_type": "single",
            "begin": str(datetime.date.today()),
        }
    ]
    resource["extents"] = [{"portion": "whole", "number": "1", "extent_type": "boxes"}]
    # NOTE post
    resource_post_response = asnake_client.post(
        "/repositories/2/resources", json=resource
    )
    print(
        "🐞 resource_post_response:DistilleryTEST-x2edw",
        resource_post_response.json(),
    )
    # CREATE ITEM RECORD
    item = {}
    # NOTE required
    item["title"] = "_Velit quis et adipisicing commodo x2edw"
    item["level"] = "item"
    item["resource"] = {"ref": resource_post_response.json()["uri"]}
    # NOTE optional
    item["component_id"] = "item-x2edw"
    item["dates"] = [
        {
            "label": "digitized",
            "date_type": "single",
            "begin": "2022-02-22",
        },
        {
            "label": "creation",
            "date_type": "single",
            "begin": "1584-02-29",
        },
        {
            "label": "creation",
            "date_type": "inclusive",
            "begin": "1969-12-31",
            "end": "1970-01-01",
        },
        {
            "label": "creation",
            "date_type": "bulk",
            "begin": "1999-12-31",
            "end": "2000-01-01",
        },
        {
            "label": "creation",
            "date_type": "single",
            "expression": "ongoing into the future",
        },
    ]
    # NOTE post
    item_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=item
    )
    print("🐞 item_post_response:x2edw", item_post_response.json())
    # RUN ALCHEMIST PROCESS
    access_uri = f'{config("ACCESS_SITE_BASE_URL").rstrip("/")}/DistilleryTEST-x2edw/item-x2edw/index.html'
    page.goto(config("DISTILLERY_BASE_URL"))
    page.get_by_label("Collection ID").fill("DistilleryTEST-x2edw")
    page.get_by_text(
        "Public web access generate files & metadata and publish on the web"
    ).click()
    page.get_by_role("button", name="Validate").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        "✅ Validated metadata, files, and destinations for DistilleryTEST-x2edw."
    )
    page.get_by_role("button", name="Run").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        "✅ Processed metadata and files for DistilleryTEST-x2edw.", timeout=10000
    )
    # VALIDATE DIGITAL OBJECT
    results = asnake_client.get(
        "/repositories/2/find_by_id/digital_objects",
        params={"digital_object_id[]": "item-x2edw"},
    ).json()
    print("🐞 find_by_id/digital_objects:item-x2edw", results)
    assert len(results["digital_objects"]) == 1
    for result in results["digital_objects"]:
        digital_object = asnake_client.get(result["ref"]).json()
        print("🐞 digital_object", digital_object)
        assert digital_object["publish"] is True
        assert digital_object["file_versions"][0]["file_uri"] == access_uri
    # VALIDATE ALCHEMIST HTML
    page.goto(access_uri)
    expect(page).to_have_title("_Velit quis et adipisicing commodo x2edw")
    expect(page.locator("#dates")).to_have_text(
        "1584 February 29; 1969 December 31 to 1970 January 1; 1999 December 31 to 2000 January 1; ongoing into the future"
    )


def test_distillery_0001_setup_nonnumeric_sequence_gz36p(
    page: Page, asnake_client, s3_client
):
    """Sequence strings on TIFF files are alphanumeric not numeric."""
    # NOTE without page parameter test does not seem to run in order
    # DELETE ANY EXISTING TEST RECORDS
    resource_find_by_id_results = asnake_client.get(
        "/repositories/2/find_by_id/resources",
        params={"identifier[]": ['["DistilleryTEST-gz36p"]']},
    ).json()
    for resource in resource_find_by_id_results["resources"]:
        resource_tree = asnake_client.get(f'{resource["ref"]}/tree/root').json()
        item_uri = resource_tree["precomputed_waypoints"][""]["0"][0]["uri"]
        archival_object = asnake_client.get(
            item_uri, params={"resolve[]": "digital_object"}
        ).json()
        for instance in archival_object["instances"]:
            if instance.get("digital_object"):
                digital_object_delete_response = asnake_client.delete(
                    instance["digital_object"]["_resolved"]["uri"]
                )
        resource_delete_response = asnake_client.delete(resource["ref"])
    # CREATE RESOURCE RECORD
    resource = {}
    # required
    resource["title"] = "_DISTILLERY TEST RESOURCE gz36p"
    resource["id_0"] = "DistilleryTEST-gz36p"
    resource["level"] = "collection"
    resource["finding_aid_language"] = "eng"
    resource["finding_aid_script"] = "Latn"
    resource["lang_materials"] = [
        {"language_and_script": {"language": "eng", "script": "Latn"}}
    ]
    resource["dates"] = [
        {
            "label": "creation",
            "date_type": "single",
            "begin": str(datetime.date.today()),
        }
    ]
    resource["extents"] = [{"portion": "whole", "number": "1", "extent_type": "boxes"}]
    # post
    resource_post_response = asnake_client.post(
        "/repositories/2/resources", json=resource
    )
    print(
        "🐞 resource_post_response:DistilleryTEST-gz36p",
        resource_post_response.json(),
    )
    # CREATE ITEM RECORD
    item = {}
    # required
    item["title"] = "_DISTILLERY TEST ITEM gz36p"
    item["level"] = "item"
    item["resource"] = {"ref": resource_post_response.json()["uri"]}
    # optional
    item["component_id"] = "item-gz36p"
    # post
    item_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=item
    )
    # DELETE S3 OBJECTS
    s3_response = s3_client.list_objects_v2(
        Bucket=config("PRESERVATION_BUCKET"), Prefix="DistilleryTEST-gz36p"
    )
    print("🐞 s3_client.list_objects_v2", s3_response)
    if s3_response.get("Contents"):
        s3_keys = [{"Key": s3_object["Key"]} for s3_object in s3_response["Contents"]]
        s3_response = s3_client.delete_objects(
            Bucket=config("PRESERVATION_BUCKET"), Delete={"Objects": s3_keys}
        )


def test_distillery_0001_setup_access_nonnumeric_sequence_yw3ff(
    page: Page, asnake_client, s3_client
):
    """Sequence strings on TIFF files are alphanumeric not numeric.

    FILESYSTEM TREE:

    DistilleryTEST-yw3ff
    └── item-yw3ff
        ├── DistilleryTEST-yw3ff-item_C.tiff
        ├── DistilleryTEST-yw3ff-item_p000-p001.tiff
        └── DistilleryTEST-yw3ff-item_p002-p003.tiff

    1 directory, 3 files
    """
    # NOTE without page parameter test does not seem to run in order
    # DELETE ANY EXISTING TEST RECORDS
    resource_find_by_id_results = asnake_client.get(
        "/repositories/2/find_by_id/resources",
        params={"identifier[]": ['["DistilleryTEST-yw3ff"]']},
    ).json()
    for resource in resource_find_by_id_results["resources"]:
        resource_tree = asnake_client.get(f'{resource["ref"]}/tree/root').json()
        series_uri = resource_tree["precomputed_waypoints"][""]["0"][0]["uri"]
        series_waypoint = asnake_client.get(
            f'{resource["ref"]}/tree/waypoint?offset=0&parent_node={series_uri}'
        ).json()
        subseries_waypoint = asnake_client.get(
            f'{resource["ref"]}/tree/waypoint?offset=0&parent_node={series_waypoint[0]["uri"]}'
        ).json()
        archival_object = asnake_client.get(
            subseries_waypoint[0]["uri"], params={"resolve[]": "digital_object"}
        ).json()
        for instance in archival_object["instances"]:
            if instance.get("digital_object"):
                # delete digital object before deleting related archival object
                digital_object_delete_response = asnake_client.delete(
                    instance["digital_object"]["_resolved"]["uri"]
                )
        resource_delete_response = asnake_client.delete(resource["ref"])
    # CREATE COLLECTION RECORD
    resource = {}
    # required
    resource["title"] = "_DISTILLERY TEST COLLECTION yw3ff"
    resource["id_0"] = "DistilleryTEST-yw3ff"
    resource["level"] = "collection"
    resource["finding_aid_language"] = "eng"
    resource["finding_aid_script"] = "Latn"
    resource["lang_materials"] = [
        {"language_and_script": {"language": "eng", "script": "Latn"}}
    ]
    resource["dates"] = [
        {
            "label": "creation",
            "date_type": "single",
            "begin": str(datetime.date.today()),
        }
    ]
    resource["extents"] = [{"portion": "whole", "number": "1", "extent_type": "boxes"}]
    # post
    resource_post_response = asnake_client.post(
        "/repositories/2/resources", json=resource
    )
    print(
        "🐞 resource_post_response:DistilleryTEST-yw3ff",
        resource_post_response.json(),
    )
    # CREATE SERIES RECORD
    series = {}
    series["title"] = "_DISTILLERY TEST SERIES yw3ff"  # title or date required
    series["component_id"] = "DistilleryTEST-yw3ff-series"
    series["level"] = "series"  # required
    series["resource"] = {"ref": resource_post_response.json()["uri"]}  # required
    series_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=series
    )
    print("🐞 series_post_response", series_post_response.json())
    # CREATE SUBSERIES RECORD
    subseries = {}
    subseries["title"] = "_DISTILLERY TEST SUBSERIES yw3ff"  # title or date required
    subseries["component_id"] = "DistilleryTEST-yw3ff-subseries"
    subseries["level"] = "subseries"  # required
    subseries["resource"] = {"ref": resource_post_response.json()["uri"]}  # required
    subseries_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=subseries
    )
    print("🐞 subseries_post_response", subseries_post_response.json())
    # set subseries as a child of series
    subseries_parent_position_post_response = asnake_client.post(
        f'{subseries_post_response.json()["uri"]}/parent',
        params={"parent": series_post_response.json()["id"], "position": 0},
    )
    print(
        "🐞 subseries_parent_position_post_response",
        subseries_parent_position_post_response.json(),
    )
    # CREATE ITEM RECORD
    item = {}
    # required
    item["title"] = "_DISTILLERY TEST ITEM yw3ff"
    item["level"] = "item"
    item["resource"] = {"ref": resource_post_response.json()["uri"]}
    # optional
    item["component_id"] = "item-yw3ff"
    item["dates"] = [
        {
            "label": "creation",
            "date_type": "inclusive",
            "begin": "1999-12-31",
            "end": "2001-01-01",
        }
    ]
    item["notes"] = [
        {
            "jsonmodel_type": "note_multipart",
            "publish": True,
            "subnotes": [
                {
                    "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. (This is a Scope and Contents note set to published.)",
                    "jsonmodel_type": "note_text",
                    "publish": True,
                }
            ],
            "type": "scopecontent",
        },
        {
            "content": [
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. (This is an Abstract note set to unpublished.)"
            ],
            "jsonmodel_type": "note_singlepart",
            "publish": False,
            "type": "abstract",
        },
    ]
    # post
    item_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=item
    )
    # set item as a child of subseries
    item_parent_position_post_response = asnake_client.post(
        f'{item_post_response.json()["uri"]}/parent',
        params={
            "parent": subseries_post_response.json()["id"],
            "position": 1,
        },
    )
    print(
        "🐞 item_parent_position_post_response",
        item_parent_position_post_response.json(),
    )
    # DELETE S3 OBJECTS
    # NOTE deletion of relevant files is part of the production workflow


def test_distillery_landing(page: Page):
    page.goto(config("DISTILLERY_BASE_URL"))
    expect(page).to_have_title("Distillery")


def test_distillery_cloud_wrong_component_id_948vk(page: Page, asnake_client):
    """Corresponding directory name does not match component_id."""
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, "DistilleryTEST-948vk")
    # CREATE COLLECTION RECORD
    resource = {}
    # NOTE required
    resource["title"] = "_DISTILLERY TEST COLLECTION 948vk"
    resource["id_0"] = "DistilleryTEST-948vk"
    resource["level"] = "collection"
    resource["finding_aid_language"] = "eng"
    resource["finding_aid_script"] = "Latn"
    resource["lang_materials"] = [
        {"language_and_script": {"language": "eng", "script": "Latn"}}
    ]
    resource["dates"] = [
        {
            "label": "creation",
            "date_type": "single",
            "begin": str(datetime.date.today()),
        }
    ]
    resource["extents"] = [{"portion": "whole", "number": "1", "extent_type": "boxes"}]
    # NOTE post
    resource_post_response = asnake_client.post(
        "/repositories/2/resources", json=resource
    )
    print(
        "🐞 resource_post_response:DistilleryTEST-948vk",
        resource_post_response.json(),
    )
    # CREATE ITEM RECORD
    item = {}
    # NOTE required
    item["title"] = "_DISTILLERY TEST ITEM 948vk"
    item["level"] = "item"
    item["resource"] = {"ref": resource_post_response.json()["uri"]}
    # NOTE optional
    item["component_id"] = "distillery_test_item_948vk"
    # NOTE post
    item_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=item
    )
    print("🐞 item_post_response:948vk", item_post_response.json())
    # DISTILLERY CLOUD WORKFLOW
    page.goto(config("DISTILLERY_BASE_URL"))
    page.get_by_label("Collection ID").fill("DistilleryTEST-948vk")
    page.get_by_text(
        "Cloud preservation storage generate and send files to a remote storage provider"
    ).click()
    page.get_by_role("button", name="Validate").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        "❌ Something went wrong. View the details for more information.", timeout=30000
    )


def test_distillery_cloud_nonnumeric_sequence_gz36p(
    page: Page, asnake_client, s3_client
):
    page.goto(config("DISTILLERY_BASE_URL"))
    page.get_by_label("Collection ID").fill("DistilleryTEST-gz36p")
    page.get_by_text(
        "Cloud preservation storage generate and send files to a remote storage provider"
    ).click()
    page.get_by_role("button", name="Validate").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        "✅ Validated metadata, files, and destinations for DistilleryTEST-gz36p."
    )
    page.get_by_role("button", name="Run").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        "✅ Processed metadata and files for DistilleryTEST-gz36p.", timeout=60000
    )
    # get a list of s3 objects under this test prefix
    s3_response = s3_client.list_objects_v2(
        Bucket=config("PRESERVATION_BUCKET"), Prefix="DistilleryTEST-gz36p"
    )
    print("🐞 s3_client.list_objects_v2", s3_response)
    # ensure that the digital object components were created correctly
    results = asnake_client.get(
        "/repositories/2/find_by_id/digital_objects",
        params={"digital_object_id[]": "item-gz36p"},
    ).json()
    print("🐞 find_by_id/digital_objects:item-gz36p", results)
    assert len(results["digital_objects"]) == 1
    for digital_object in results["digital_objects"]:
        tree = asnake_client.get(f'{digital_object["ref"]}/tree/root').json()
        print(f'🐞 {digital_object["ref"]}/tree/root', tree)
        assert len(tree["precomputed_waypoints"][""]["0"]) > 0
        for waypoint in tree["precomputed_waypoints"][""]["0"]:
            # split the s3 key from the file_uri_summary and ensure it matches
            assert waypoint["file_uri_summary"].split(
                f's3://{config("PRESERVATION_BUCKET")}/'
            )[-1] in [s3_object["Key"] for s3_object in s3_response["Contents"]]
            # split the original filename from the s3 key and match the label
            assert waypoint["label"] in [
                s3_object["Key"].split("/")[-2] for s3_object in s3_response["Contents"]
            ]


def test_distillery_access_nonnumeric_sequence_yw3ff(page: Page, asnake_client):
    access_uri = f'{config("ACCESS_SITE_BASE_URL").rstrip("/")}/DistilleryTEST-yw3ff/item-yw3ff/index.html'
    page.goto(config("DISTILLERY_BASE_URL"))
    page.get_by_label("Collection ID").fill("DistilleryTEST-yw3ff")
    page.get_by_text(
        "Public web access generate files & metadata and publish on the web"
    ).click()
    page.get_by_role("button", name="Validate").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        "✅ Validated metadata, files, and destinations for DistilleryTEST-yw3ff."
    )
    page.get_by_role("button", name="Run").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        "✅ Processed metadata and files for DistilleryTEST-yw3ff.", timeout=30000
    )
    # VALIDATE DIGITAL OBJECT
    results = asnake_client.get(
        "/repositories/2/find_by_id/digital_objects",
        params={"digital_object_id[]": "item-yw3ff"},
    ).json()
    print("🐞 find_by_id/digital_objects:item-yw3ff", results)
    assert len(results["digital_objects"]) == 1
    for result in results["digital_objects"]:
        digital_object = asnake_client.get(result["ref"]).json()
        print("🐞 digital_object", digital_object)
        assert digital_object["publish"] is True
        assert digital_object["file_versions"][0]["file_uri"] == access_uri
    # VALIDATE ACCESS HTML
    page.goto(access_uri)
    expect(page).to_have_title("_DISTILLERY TEST ITEM yw3ff, 1999-12-31 - 2001-01-01")


def test_distillery_cloud_video_7b3px(page: Page, asnake_client, s3_client):
    """Test scenario with no TIFFs, but video and supplementary files."""
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, "DistilleryTEST-7b3px")
    # CREATE COLLECTION RECORD
    resource = {}
    # required
    resource["title"] = "_DISTILLERY TEST COLLECTION 7b3px"
    resource["id_0"] = "DistilleryTEST-7b3px"
    resource["level"] = "collection"
    resource["finding_aid_language"] = "eng"
    resource["finding_aid_script"] = "Latn"
    resource["lang_materials"] = [
        {"language_and_script": {"language": "eng", "script": "Latn"}}
    ]
    resource["dates"] = [
        {
            "label": "creation",
            "date_type": "single",
            "begin": str(datetime.date.today()),
        }
    ]
    resource["extents"] = [{"portion": "whole", "number": "1", "extent_type": "boxes"}]
    # post
    resource_post_response = asnake_client.post(
        "/repositories/2/resources", json=resource
    )
    print(
        "🐞 resource_post_response:DistilleryTEST-7b3px",
        resource_post_response.json(),
    )
    # CREATE SERIES RECORD
    series = {}
    series["title"] = "_DISTILLERY TEST SERIES 7b3px"  # title or date required
    series["component_id"] = "DistilleryTEST-7b3px-series"
    series["level"] = "series"  # required
    series["resource"] = {"ref": resource_post_response.json()["uri"]}  # required
    series_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=series
    )
    print("🐞 series_post_response", series_post_response.json())
    # CREATE SUBSERIES RECORD
    subseries = {}
    subseries["title"] = "_DISTILLERY TEST SUBSERIES 7b3px"  # title or date required
    subseries["component_id"] = "DistilleryTEST-7b3px-subseries"
    subseries["level"] = "subseries"  # required
    subseries["resource"] = {"ref": resource_post_response.json()["uri"]}  # required
    subseries_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=subseries
    )
    print("🐞 subseries_post_response", subseries_post_response.json())
    # set subseries as a child of series
    subseries_parent_position_post_response = asnake_client.post(
        f'{subseries_post_response.json()["uri"]}/parent',
        params={"parent": series_post_response.json()["id"], "position": 0},
    )
    print(
        "🐞 subseries_parent_position_post_response",
        subseries_parent_position_post_response.json(),
    )
    # CREATE ITEM RECORD
    item = {}
    # required
    item[
        "title"
    ] = "_DISTILLERY TEST ITEM dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt 7b3px"
    item["level"] = "item"
    item["resource"] = {"ref": resource_post_response.json()["uri"]}
    # optional
    item["component_id"] = "distillery_test_item_7b3px"
    item["dates"] = [
        {
            "label": "creation",
            "date_type": "single",
            "begin": "1999-12-31",
        }
    ]
    # post
    item_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=item
    )
    # set item as a child of subseries
    item_parent_position_post_response = asnake_client.post(
        f'{item_post_response.json()["uri"]}/parent',
        params={
            "parent": subseries_post_response.json()["id"],
            "position": 1,
        },
    )
    print(
        "🐞 item_parent_position_post_response",
        item_parent_position_post_response.json(),
    )
    # DELETE S3 OBJECTS
    s3_response = s3_client.list_objects_v2(
        Bucket=config("PRESERVATION_BUCKET"), Prefix="DistilleryTEST-7b3px"
    )
    print("🐞 s3_client.list_objects_v2", s3_response)
    if s3_response.get("Contents"):
        s3_keys = [{"Key": s3_object["Key"]} for s3_object in s3_response["Contents"]]
        s3_response = s3_client.delete_objects(
            Bucket=config("PRESERVATION_BUCKET"), Delete={"Objects": s3_keys}
        )
    # DISTILLERY CLOUD WORKFLOW
    page.goto(config("DISTILLERY_BASE_URL"))
    page.get_by_label("Collection ID").fill("DistilleryTEST-7b3px")
    page.get_by_text(
        "Cloud preservation storage generate and send files to a remote storage provider"
    ).click()
    page.get_by_role("button", name="Validate").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        "✅ Validated metadata, files, and destinations for DistilleryTEST-7b3px."
    )
    page.get_by_role("button", name="Run").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        "✅ Processed metadata and files for DistilleryTEST-7b3px.", timeout=60000
    )
    # get a list of s3 objects under this test prefix
    s3_response = s3_client.list_objects_v2(
        Bucket=config("PRESERVATION_BUCKET"), Prefix="DistilleryTEST-7b3px"
    )
    print("🐞 s3_client.list_objects_v2", s3_response)
    # ensure that the digital object components were created correctly
    results = asnake_client.get(
        "/repositories/2/find_by_id/digital_objects",
        params={"digital_object_id[]": "distillery_test_item_7b3px"},
    ).json()
    print("🐞 find_by_id/digital_objects", results)
    assert len(results["digital_objects"]) == 1
    for digital_object in results["digital_objects"]:
        tree = asnake_client.get(f'{digital_object["ref"]}/tree/root').json()
        print(f'🐞 {digital_object["ref"]}/tree/root', tree)
        assert len(tree["precomputed_waypoints"][""]["0"]) > 0
        for waypoint in tree["precomputed_waypoints"][""]["0"]:
            # split the s3 key from the file_uri_summary and ensure it matches
            assert waypoint["file_uri_summary"].split(
                f's3://{config("PRESERVATION_BUCKET")}/'
            )[-1] in [s3_object["Key"] for s3_object in s3_response["Contents"]]
            # split the original filename from the s3 key and match the label
            assert waypoint["label"] in [
                s3_object["Key"].split("/")[-2] for s3_object in s3_response["Contents"]
            ]
