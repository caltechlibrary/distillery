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
def set_timeout(page: Page):
    page.set_default_timeout(60000)
    return page


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


def create_archivesspace_test_resource(asnake_client, test_name, test_id):
    resource = {}
    # required
    resource["title"] = f"{test_name.capitalize()} Resource {test_id}"
    resource["id_0"] = f"DistilleryTEST-{test_id}"
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
    # optional
    resource["publish"] = True
    # post
    resource_post_response = asnake_client.post(
        "/repositories/2/resources", json=resource
    )
    return resource_post_response


def create_archivesspace_test_archival_object_item(
    asnake_client, test_name, test_id, resource_uri
):
    item = {}
    # required
    item["title"] = f"{test_name.capitalize()} Item {test_id}"
    item["level"] = "item"
    item["resource"] = {"ref": resource_uri}
    # optional
    item["component_id"] = f"item-{test_id}"
    item["publish"] = True
    # post
    item_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=item
    )
    return item_post_response


def create_archivesspace_test_archival_object_series(
    asnake_client, test_name, test_id, resource_uri
):
    series = {}
    # required
    series["title"] = f"{test_name.capitalize()} Series {test_id}"
    series["level"] = "series"
    series["resource"] = {"ref": resource_uri}
    # optional
    series["component_id"] = f"series-{test_id}"
    series["publish"] = True
    # post
    series_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=series
    )
    return series_post_response


def create_archivesspace_test_archival_object_subseries(
    asnake_client, test_name, test_id, resource_uri
):
    subseries = {}
    # required
    subseries["title"] = f"{test_name.capitalize()} Sub-Series {test_id}"
    subseries["level"] = "subseries"
    subseries["resource"] = {"ref": resource_uri}
    # optional
    subseries["component_id"] = f"subseries-{test_id}"
    subseries["publish"] = True
    # post
    subseries_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=subseries
    )
    return subseries_post_response


def run_distillery(page: Page, resource_identifier, destinations, outcome="success"):
    page.goto(config("DISTILLERY_BASE_URL"))
    page.get_by_label("Collection ID").fill(resource_identifier)
    for destination in destinations:
        page.locator(f'input[value="{destination}"]').click()
    page.get_by_role("button", name="Validate").click()
    page.get_by_text("Details").click()
    if outcome == "failure":
        expect(page.locator("p")).to_have_text(
            "❌ Something went wrong. View the details for more information.", timeout=60000
        )
        return page
    expect(page.locator("p")).to_have_text(
        f"✅ Validated metadata, files, and destinations for {resource_identifier}.", timeout=60000
    )
    page.get_by_role("button", name="Run").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        f"✅ Processed metadata and files for {resource_identifier}.", timeout=60000
    )


def format_alchemist_item_uri(test_id):
    return "/".join(
        [
            config("ACCESS_SITE_BASE_URL").rstrip("/"),
            f"DistilleryTEST-{test_id}",
            f"item-{test_id}",
            "index.html",
        ]
    )


def test_distillery_landing(page: Page):
    page.goto(config("DISTILLERY_BASE_URL"))
    expect(page).to_have_title("Distillery")


def test_distillery_access_unpublished_archival_object_sjex6(page: Page, asnake_client):
    test_name = "access unpublished archival object"
    test_id = "sjex6"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 archival_object_create_response:{test_id}",
        item_create_response.json(),
    )
    # CUSTOMIZE ARCHIVAL OBJECT ITEM RECORD
    # set publish to False
    item = asnake_client.get(item_create_response.json()["uri"]).json()
    item["publish"] = False
    item_update_response = asnake_client.post(item["uri"], json=item)
    print(
        f"🐞 item_update_response:{test_id}",
        item_update_response.json(),
    )
    # RUN DISTILLERY ACCESS WORKFLOW
    page = run_distillery(
        page, f"DistilleryTEST-{test_id}", ["access"], outcome="failure"
    )
    # TODO check contents of iframe


def test_distillery_access_unpublished_ancestor_jvycv(page: Page, asnake_client):
    test_name = "access unpublished ancestor"
    test_id = "jvycv"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CUSTOMIZE RESOURCE RECORD
    # set publish to False
    resource = asnake_client.get(resource_create_response.json()["uri"]).json()
    resource["publish"] = False
    resource_update_response = asnake_client.post(resource["uri"], json=resource)
    print(
        f"🐞 resource_update_response:{test_id}",
        resource_update_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 item_create_response:{test_id}",
        item_create_response.json(),
    )
    # RUN DISTILLERY ACCESS WORKFLOW
    page = run_distillery(
        page, f"DistilleryTEST-{test_id}", ["access"], outcome="failure"
    )
    # TODO check contents of iframe


def test_distillery_access_file_uri_v8v5r(page: Page, asnake_client):
    """Confirm file_uri for Alchemist item makes it to ArchivesSpace."""
    test_name = "access file uri"
    test_id = "v8v5r"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 item_create_response:{test_id}",
        item_create_response.json(),
    )
    # RUN ALCHEMIST PROCESS
    run_distillery(page, f"DistilleryTEST-{test_id}", ["access"])
    alchemist_item_uri = format_alchemist_item_uri(test_id)
    # VALIDATE DIGITAL OBJECT RECORD
    results = asnake_client.get(
        "/repositories/2/find_by_id/digital_objects",
        params={"digital_object_id[]": f"item-{test_id}"},
    ).json()
    print(f"🐞 find_by_id/digital_objects:item-{test_id}", results)
    assert len(results["digital_objects"]) == 1
    for result in results["digital_objects"]:
        digital_object = asnake_client.get(result["ref"]).json()
        print("🐞 digital_object", digital_object)
        assert digital_object["publish"] is True
        assert digital_object["file_versions"][0]["file_uri"] == alchemist_item_uri


def test_distillery_alchemist_date_output_x2edw(page: Page, asnake_client):
    test_name = "alchemist date output"
    test_id = "x2edw"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 item_create_response:{test_id}",
        item_create_response.json(),
    )
    # CUSTOMIZE ARCHIVAL OBJECT ITEM RECORD
    item = asnake_client.get(item_create_response.json()["uri"]).json()
    # add dates
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
    item_update_response = asnake_client.post(item["uri"], json=item)
    print(
        f"🐞 item_update_response:{test_id}",
        item_update_response.json(),
    )
    # RUN ALCHEMIST PROCESS
    run_distillery(page, f"DistilleryTEST-{test_id}", ["access"])
    alchemist_item_uri = format_alchemist_item_uri(test_id)
    # VALIDATE ALCHEMIST HTML
    page.goto(alchemist_item_uri)
    expect(page.locator("hgroup p:first-of-type")).to_have_text(
        "1584 February 29; 1969 December 31 to 1970 January 1; 1999 December 31 to 2000 January 1; ongoing into the future"
    )


def test_distillery_alchemist_extent_output_77cjj(page: Page, asnake_client):
    test_name = "alchemist extent output"
    test_id = "77cjj"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 item_create_response:{test_id}",
        item_create_response.json(),
    )
    # CUSTOMIZE ARCHIVAL OBJECT ITEM RECORD
    item = asnake_client.get(item_create_response.json()["uri"]).json()
    # add extents
    item["extents"] = [
        {
            "portion": "whole",
            "number": "1",
            "extent_type": "books",
        },
        {
            "portion": "part",
            "number": "2",
            "extent_type": "photographs",
        },
    ]
    item_update_response = asnake_client.post(item["uri"], json=item)
    print(
        f"🐞 item_update_response:{test_id}",
        item_update_response.json(),
    )
    # RUN ALCHEMIST PROCESS
    run_distillery(page, f"DistilleryTEST-{test_id}", ["access"])
    alchemist_item_uri = format_alchemist_item_uri(test_id)
    # VALIDATE ALCHEMIST HTML
    page.goto(alchemist_item_uri)
    expect(page.locator("#metadata")).to_contain_text("1 books", ignore_case=True)
    expect(page.locator("#metadata")).to_contain_text("2 photographs", ignore_case=True)


def test_distillery_alchemist_subject_output_28s3q(page: Page, asnake_client):
    test_name = "alchemist subject output"
    test_id = "28s3q"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 item_create_response:{test_id}",
        item_create_response.json(),
    )
    # CUSTOMIZE ARCHIVAL OBJECT ITEM RECORD
    item = asnake_client.get(item_create_response.json()["uri"]).json()
    # add subjects
    item["subjects"] = [
        {
            "ref": "/subjects/1",
        },
        {
            "ref": "/subjects/2",
        },
    ]
    item_update_response = asnake_client.post(item["uri"], json=item)
    print(
        f"🐞 item_update_response:{test_id}",
        item_update_response.json(),
    )
    # RUN ALCHEMIST PROCESS
    run_distillery(page, f"DistilleryTEST-{test_id}", ["access"])
    alchemist_item_uri = format_alchemist_item_uri(test_id)
    # VALIDATE ALCHEMIST HTML
    page.goto(alchemist_item_uri)
    expect(page.locator("#metadata")).to_contain_text("Commencement")
    expect(page.locator("#metadata")).to_contain_text("Conferences")


def test_distillery_alchemist_note_output_u8vvf(page: Page, asnake_client):
    test_name = "alchemist note output"
    test_id = "u8vvf"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 item_create_response:{test_id}",
        item_create_response.json(),
    )
    # CUSTOMIZE ARCHIVAL OBJECT ITEM RECORD
    item = asnake_client.get(item_create_response.json()["uri"]).json()
    # add notes
    item["notes"] = [
        {
            "jsonmodel_type": "note_singlepart",
            "type": "abstract",
            "content": [
                "Published note. One content item. Sint nulla ea nostrud est tempor non exercitation tempor ad consectetur nisi voluptate consequat."
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_singlepart",
            "type": "materialspec",
            "content": [
                "Published note. Multiple content items: One. Veniam enim ullamco non commodo enim ad incididunt quis.",
                "Published note. Multiple content items: Two. Enim tempor ea nulla voluptate incididunt voluptate.",
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_singlepart",
            "type": "abstract",
            "label": "Foo Note",
            "content": [
                "Published note. One content item. Laborum labore irure consequat dolore aute minim deserunt nostrud amet."
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_singlepart",
            "type": "abstract",
            "label": "Foo Note",
            "content": [
                "Published note. Multiple content items: One. Consequat cupidatat enim duis Lorem ipsum.",
                "Published note. Multiple content items: Two. Lorem ipsum velit cillum ex do officia pariatur pariatur duis est dolor.",
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_singlepart",
            "type": "abstract",
            "content": [
                "Unpublished note. One content item. Enim aute Lorem tempor exercitation enim adipisicing occaecat veniam ad duis excepteur culpa ut consectetur."
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_singlepart",
            "type": "abstract",
            "content": [
                "Unpublished note. Multiple content items: One. Consectetur cupidatat ea sunt sit enim minim officia ea ut tempor aute.",
                "Unpublished note. Multiple content items: Two. Cillum dolore amet dolor labore do deserunt adipisicing dolore in aliquip nulla.",
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. One published text subnote. Consequat nostrud ipsum irure reprehenderit qui veniam pariatur.",
                    "publish": True,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "odd",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. Multiple published text subnotes: One. Officia nisi nisi incididunt excepteur nisi.",
                    "publish": True,
                },
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. Multiple published text subnotes: Two. Laborum commodo exercitation deserunt velit.",
                    "publish": True,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. One published text subnote. Laboris ipsum cupidatat consequat velit.",
                    "publish": True,
                },
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. Multiple published text subnotes: One. Laborum anim laborum est laborum.",
                    "publish": True,
                },
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. Multiple published text subnotes: Two. Consectetur laborum laborum quis.",
                    "publish": True,
                },
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. One unpublished text subnote. Laborum cupidatat adipisicing cillum deserunt.",
                    "publish": False,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "label": "Baz Note",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. One unpublished text subnote. Aliqua consequat mollit reprehenderit pariatur exercitation nisi culpa incididunt.",
                    "publish": False,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. Multiple unpublished text subnotes: One. Aliquip culpa pariatur consequat.",
                    "publish": False,
                },
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. Multiple unpublished text subnotes: Two. Tempor exercitation sunt.",
                    "publish": False,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. One unpublished text subnote. Esse in proident.",
                    "publish": False,
                },
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. Multiple unpublished text subnotes: One. Laborum laborum sunt.",
                    "publish": False,
                },
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. Multiple unpublished text subnotes: Two. Sint adipisicing.",
                    "publish": False,
                },
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "label": "Bar Note",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. Mixed publication status text subnotes: Published. Laboris id cupidatat.",
                    "publish": True,
                },
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. Mixed publication status text subnotes: Unpublished. Nostrud anim dolore anim consequat quis sit laborum non.",
                    "publish": False,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. Mixed publication status text subnotes: Published. Est nostrud laboris id sint amet proident officia commodo ut sint amet sint dolore sunt.",
                    "publish": True,
                },
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. Mixed publication status text subnotes: Unpublished. Fugiat irure sunt magna nulla minim commodo dolor ea dolor aliquip enim magna fugiat.",
                    "publish": False,
                },
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "label": "Bar Note",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. One published text subnote. Minim aute nulla laborum ullamco do incididunt nostrud irure eiusmod laborum elit deserunt.",
                    "publish": True,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "label": "Foo Note",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. One published text subnote. Eiusmod irure laboris eu reprehenderit proident exercitation qui nulla irure amet.",
                    "publish": True,
                },
            ],
            "publish": True,
        },
    ]
    item_update_response = asnake_client.post(item["uri"], json=item)
    print(
        f"🐞 item_update_response:{test_id}",
        item_update_response.json(),
    )
    # RUN ALCHEMIST PROCESS
    run_distillery(page, f"DistilleryTEST-{test_id}", ["access"])
    alchemist_item_uri = format_alchemist_item_uri(test_id)
    # VALIDATE ALCHEMIST HTML
    page.goto(alchemist_item_uri)
    expect(page.locator("#metadata")).to_contain_text("Abstract")
    expect(page.locator("#metadata")).to_contain_text("Materials Specific Details")
    expect(page.locator("#metadata")).to_contain_text("Foo Note")
    expect(page.locator("#metadata")).to_contain_text("Scope and Contents")
    expect(page.locator("#metadata")).to_contain_text("General")
    expect(page.locator("#metadata")).to_contain_text("Bar Note")
    expect(page.locator("#metadata")).not_to_contain_text(
        "unpublished", ignore_case=True
    )


def test_distillery_alchemist_ancestors_2gj5n(page: Page, asnake_client):
    """Confirm ancestors display in Alchemist."""
    test_name = "alchemist ancestors"
    test_id = "2gj5n"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CREATE ARCHIVAL OBJECT SERIES RECORD
    series_create_response = create_archivesspace_test_archival_object_series(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 series_create_response:{test_id}",
        series_create_response.json(),
    )
    # CREATE ARCHIVAL OBJECT SUBSERIES RECORD
    subseries_create_response = create_archivesspace_test_archival_object_subseries(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 subseries_create_response:{test_id}",
        subseries_create_response.json(),
    )
    # set subseries as a child of series
    subseries_parent_position_post_response = asnake_client.post(
        f'{subseries_create_response.json()["uri"]}/parent',
        params={"parent": series_create_response.json()["id"], "position": 1},
    )
    print(
        "🐞 subseries_parent_position_post_response",
        subseries_parent_position_post_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 item_create_response:{test_id}",
        item_create_response.json(),
    )
    # set item as a child of subseries
    item_parent_position_post_response = asnake_client.post(
        f'{item_create_response.json()["uri"]}/parent',
        params={
            "parent": subseries_create_response.json()["id"],
            "position": 1,
        },
    )
    print(
        "🐞 item_parent_position_post_response",
        item_parent_position_post_response.json(),
    )
    # RUN ALCHEMIST PROCESS
    run_distillery(page, f"DistilleryTEST-{test_id}", ["access"])
    alchemist_item_uri = format_alchemist_item_uri(test_id)
    # VALIDATE ALCHEMIST ITEM
    page.goto(alchemist_item_uri)
    expect(page.locator("hgroup p:last-child")).to_have_text(
        f"{test_name.capitalize()} Resource {test_id}{test_name.capitalize()} Series {test_id}{test_name.capitalize()} Sub-Series {test_id}"
    )
    expect(page.locator("#metadata")).to_contain_text("Collection")
    expect(page.locator("#metadata")).to_contain_text("Series")
    expect(page.locator("#metadata")).to_contain_text("Sub-Series")


def test_distillery_alchemist_nonnumeric_sequence_yw3ff(page: Page, asnake_client):
    """Confirm non-numeric sequence strings make it to Alchemist."""
    test_name = "alchemist non-numeric sequence"
    test_id = "yw3ff"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 item_create_response:{test_id}",
        item_create_response.json(),
    )
    # RUN ALCHEMIST PROCESS
    run_distillery(page, f"DistilleryTEST-{test_id}", ["access"])
    alchemist_item_uri = format_alchemist_item_uri(test_id)
    # VALIDATE DIGITAL OBJECT RECORD
    results = asnake_client.get(
        "/repositories/2/find_by_id/digital_objects",
        params={"digital_object_id[]": f"item-{test_id}"},
    ).json()
    print(f"🐞 find_by_id/digital_objects:item-{test_id}", results)
    assert len(results["digital_objects"]) == 1
    for result in results["digital_objects"]:
        digital_object = asnake_client.get(result["ref"]).json()
        print("🐞 digital_object", digital_object)
        assert digital_object["publish"] is True
        assert digital_object["file_versions"][0]["file_uri"] == alchemist_item_uri
    # VALIDATE ALCHEMIST ITEM
    page.goto(alchemist_item_uri)
    expect(page.locator("#thumb-0")).to_have_text("C")
    expect(page.locator("#thumb-1")).to_have_text("p000-p001")
    expect(page.locator("#thumb-2")).to_have_text("p002-p003")


def test_distillery_alchemist_kitchen_sink_pd4s3(page: Page, asnake_client):
    """Attempt to test every ArchivesSpace field."""
    test_name = "kitchen sink"
    test_id = "pd4s3"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CUSTOMIZE ARCHIVAL OBJECT RESOURCE RECORD
    resource = asnake_client.get(resource_create_response.json()["uri"]).json()
    # add dates
    resource["dates"] = [
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
    resource_update_response = asnake_client.post(resource["uri"], json=resource)
    print(
        f"🐞 resource_update_response:{test_id}",
        resource_update_response.json(),
    )
    # CREATE ARCHIVAL OBJECT SERIES RECORD
    series_create_response = create_archivesspace_test_archival_object_series(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 series_create_response:{test_id}",
        series_create_response.json(),
    )
    # CUSTOMIZE ARCHIVAL OBJECT SERIES RECORD
    series = asnake_client.get(series_create_response.json()["uri"]).json()
    # add dates
    series["dates"] = [
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
    series_update_response = asnake_client.post(series["uri"], json=series)
    print(
        f"🐞 series_update_response:{test_id}",
        series_update_response.json(),
    )
    # CREATE ARCHIVAL OBJECT SUBSERIES RECORD
    subseries_create_response = create_archivesspace_test_archival_object_subseries(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 subseries_create_response:{test_id}",
        subseries_create_response.json(),
    )
    # set subseries as a child of series
    subseries_parent_position_post_response = asnake_client.post(
        f'{subseries_create_response.json()["uri"]}/parent',
        params={"parent": series_create_response.json()["id"], "position": 1},
    )
    print(
        "🐞 subseries_parent_position_post_response",
        subseries_parent_position_post_response.json(),
    )
    # CUSTOMIZE ARCHIVAL OBJECT SUBSERIES RECORD
    subseries = asnake_client.get(subseries_create_response.json()["uri"]).json()
    # add dates
    subseries["dates"] = [
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
    subseries_update_response = asnake_client.post(subseries["uri"], json=subseries)
    print(
        f"🐞 subseries_update_response:{test_id}",
        subseries_update_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 item_create_response:{test_id}",
        item_create_response.json(),
    )
    # set item as a child of subseries
    item_parent_position_post_response = asnake_client.post(
        f'{item_create_response.json()["uri"]}/parent',
        params={
            "parent": subseries_create_response.json()["id"],
            "position": 1,
        },
    )
    print(
        "🐞 item_parent_position_post_response",
        item_parent_position_post_response.json(),
    )
    # CUSTOMIZE ARCHIVAL OBJECT ITEM RECORD
    item = asnake_client.get(item_create_response.json()["uri"]).json()
    # add dates
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
    # add extents
    item["extents"] = [
        {
            "portion": "whole",
            "number": "1",
            "extent_type": "books",
        },
        {
            "portion": "part",
            "number": "2",
            "extent_type": "photographs",
        },
    ]
    # add subjects
    item["subjects"] = [
        {
            "ref": "/subjects/1",
        },
        {
            "ref": "/subjects/2",
        },
    ]
    # add notes
    item["notes"] = [
        {
            "jsonmodel_type": "note_singlepart",
            "type": "abstract",
            "content": [
                "Published note. One content item. Sint nulla ea nostrud est tempor non exercitation tempor ad consectetur nisi voluptate consequat."
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_singlepart",
            "type": "materialspec",
            "content": [
                "Published note. Multiple content items: One. Veniam enim ullamco non commodo enim ad incididunt quis.",
                "Published note. Multiple content items: Two. Enim tempor ea nulla voluptate incididunt voluptate.",
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_singlepart",
            "type": "abstract",
            "label": "Foo Note",
            "content": [
                "Published note. One content item. Laborum labore irure consequat dolore aute minim deserunt nostrud amet."
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_singlepart",
            "type": "abstract",
            "label": "Foo Note",
            "content": [
                "Published note. Multiple content items: One. Consequat cupidatat enim duis Lorem ipsum.",
                "Published note. Multiple content items: Two. Lorem ipsum velit cillum ex do officia pariatur pariatur duis est dolor.",
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_singlepart",
            "type": "abstract",
            "content": [
                "Unpublished note. One content item. Enim aute Lorem tempor exercitation enim adipisicing occaecat veniam ad duis excepteur culpa ut consectetur."
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_singlepart",
            "type": "abstract",
            "content": [
                "Unpublished note. Multiple content items: One. Consectetur cupidatat ea sunt sit enim minim officia ea ut tempor aute.",
                "Unpublished note. Multiple content items: Two. Cillum dolore amet dolor labore do deserunt adipisicing dolore in aliquip nulla.",
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. One published text subnote. Consequat nostrud ipsum irure reprehenderit qui veniam pariatur.",
                    "publish": True,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "odd",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. Multiple published text subnotes: One. Officia nisi nisi incididunt excepteur nisi.",
                    "publish": True,
                },
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. Multiple published text subnotes: Two. Laborum commodo exercitation deserunt velit.",
                    "publish": True,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. One published text subnote. Laboris ipsum cupidatat consequat velit.",
                    "publish": True,
                },
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. Multiple published text subnotes: One. Laborum anim laborum est laborum.",
                    "publish": True,
                },
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. Multiple published text subnotes: Two. Consectetur laborum laborum quis.",
                    "publish": True,
                },
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. One unpublished text subnote. Laborum cupidatat adipisicing cillum deserunt.",
                    "publish": False,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "label": "Baz Note",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. One unpublished text subnote. Aliqua consequat mollit reprehenderit pariatur exercitation nisi culpa incididunt.",
                    "publish": False,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. Multiple unpublished text subnotes: One. Aliquip culpa pariatur consequat.",
                    "publish": False,
                },
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. Multiple unpublished text subnotes: Two. Tempor exercitation sunt.",
                    "publish": False,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. One unpublished text subnote. Esse in proident.",
                    "publish": False,
                },
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. Multiple unpublished text subnotes: One. Laborum laborum sunt.",
                    "publish": False,
                },
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. Multiple unpublished text subnotes: Two. Sint adipisicing.",
                    "publish": False,
                },
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "label": "Bar Note",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. Mixed publication status text subnotes: Published. Laboris id cupidatat.",
                    "publish": True,
                },
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. Mixed publication status text subnotes: Unpublished. Nostrud anim dolore anim consequat quis sit laborum non.",
                    "publish": False,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. Mixed publication status text subnotes: Published. Est nostrud laboris id sint amet proident officia commodo ut sint amet sint dolore sunt.",
                    "publish": True,
                },
                {
                    "jsonmodel_type": "note_text",
                    "content": "Unpublished note. Mixed publication status text subnotes: Unpublished. Fugiat irure sunt magna nulla minim commodo dolor ea dolor aliquip enim magna fugiat.",
                    "publish": False,
                },
            ],
            "publish": False,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "label": "Bar Note",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. One published text subnote. Minim aute nulla laborum ullamco do incididunt nostrud irure eiusmod laborum elit deserunt.",
                    "publish": True,
                },
            ],
            "publish": True,
        },
        {
            "jsonmodel_type": "note_multipart",
            "type": "scopecontent",
            "label": "Foo Note",
            "subnotes": [
                {
                    "jsonmodel_type": "note_text",
                    "content": "Published note. One published text subnote. Eiusmod irure laboris eu reprehenderit proident exercitation qui nulla irure amet.",
                    "publish": True,
                },
            ],
            "publish": True,
        },
    ]
    item_update_response = asnake_client.post(item["uri"], json=item)
    print(
        f"🐞 item_update_response:{test_id}",
        item_update_response.json(),
    )
    # RUN ALCHEMIST PROCESS
    run_distillery(page, f"DistilleryTEST-{test_id}", ["access"])
    alchemist_item_uri = format_alchemist_item_uri(test_id)
    # VALIDATE ALCHEMIST ITEM
    page.goto(alchemist_item_uri)
    expect(page.locator("hgroup p:first-of-type")).to_have_text(
        "1584 February 29; 1969 December 31 to 1970 January 1; 1999 December 31 to 2000 January 1; ongoing into the future"
    )
    expect(page.locator("hgroup p:last-child")).to_have_text(
        f"{test_name.capitalize()} Resource {test_id}{test_name.capitalize()} Series {test_id}{test_name.capitalize()} Sub-Series {test_id}"
    )
    expect(page.locator("#metadata")).to_contain_text("Collection")
    expect(page.locator("#metadata")).to_contain_text("Series")
    expect(page.locator("#metadata")).to_contain_text("Sub-Series")
    expect(page.locator("#metadata")).to_contain_text("Commencement")
    expect(page.locator("#metadata")).to_contain_text("Conferences")
    expect(page.locator("#metadata")).to_contain_text("1 books", ignore_case=True)
    expect(page.locator("#metadata")).to_contain_text("2 photographs", ignore_case=True)
    expect(page.locator("#metadata")).to_contain_text("Abstract")
    expect(page.locator("#metadata")).to_contain_text("Materials Specific Details")
    expect(page.locator("#metadata")).to_contain_text("Foo Note")
    expect(page.locator("#metadata")).to_contain_text("Scope and Contents")
    expect(page.locator("#metadata")).to_contain_text("General")
    expect(page.locator("#metadata")).to_contain_text("Bar Note")
    expect(page.locator("#metadata")).not_to_contain_text(
        "unpublished", ignore_case=True
    )


def test_distillery_cloud_wrong_component_id_948vk(page: Page, asnake_client):
    """Corresponding directory name does not match component_id."""
    test_name = "cloud wrong component_id"
    test_id = "948vk"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 item_create_response:{test_id}",
        item_create_response.json(),
    )
    # RUN DISTILLERY CLOUD PROCESS
    page = run_distillery(
        page, f"DistilleryTEST-{test_id}", ["cloud"], outcome="failure"
    )
    # TODO check contents of iframe


def test_distillery_cloud_nonnumeric_sequence_gz36p(
    page: Page, asnake_client, s3_client
):
    """Confirm images with non-numeric sequence strings make it to S3 and ArchivesSpace."""
    test_name = "cloud non-numeric sequence"
    test_id = "gz36p"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 item_create_response:{test_id}",
        item_create_response.json(),
    )
    # DELETE S3 OBJECTS
    s3_response = s3_client.list_objects_v2(
        Bucket=config("PRESERVATION_BUCKET"), Prefix=f"DistilleryTEST-{test_id}"
    )
    print("🐞 s3_client.list_objects_v2", s3_response)
    if s3_response.get("Contents"):
        s3_keys = [{"Key": s3_object["Key"]} for s3_object in s3_response["Contents"]]
        s3_response = s3_client.delete_objects(
            Bucket=config("PRESERVATION_BUCKET"), Delete={"Objects": s3_keys}
        )
    # RUN DISTILLERY CLOUD PROCESS
    run_distillery(page, f"DistilleryTEST-{test_id}", ["cloud"])
    # VALIDATE S3 UPLOAD AND DIGITAL OBJECT RECORD
    # get a list of s3 objects under this collection prefix
    s3_response = s3_client.list_objects_v2(
        Bucket=config("PRESERVATION_BUCKET"), Prefix=f"DistilleryTEST-{test_id}"
    )
    print("🐞 s3_client.list_objects_v2", s3_response)
    # ensure that the digital object components were created correctly
    results = asnake_client.get(
        "/repositories/2/find_by_id/digital_objects",
        params={"digital_object_id[]": f"item-{test_id}"},
    ).json()
    print(f"🐞 find_by_id/digital_objects:item-{test_id}", results)
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


def test_distillery_cloud_nonimage_files_7b3px(page: Page, asnake_client, s3_client):
    """Confirm non-image files make it to S3 and ArchivesSpace."""
    test_name = "cloud non-image files"
    test_id = "7b3px"
    # DELETE ANY EXISTING TEST RECORDS
    delete_archivesspace_test_records(asnake_client, f"DistilleryTEST-{test_id}")
    # CREATE RESOURCE RECORD
    resource_create_response = create_archivesspace_test_resource(
        asnake_client, test_name, test_id
    )
    print(
        f"🐞 resource_create_response:{test_id}",
        resource_create_response.json(),
    )
    # CREATE ARCHIVAL OBJECT ITEM RECORD
    item_create_response = create_archivesspace_test_archival_object_item(
        asnake_client, test_name, test_id, resource_create_response.json()["uri"]
    )
    print(
        f"🐞 item_create_response:{test_id}",
        item_create_response.json(),
    )
    # DELETE S3 OBJECTS
    s3_response = s3_client.list_objects_v2(
        Bucket=config("PRESERVATION_BUCKET"), Prefix=f"DistilleryTEST-{test_id}"
    )
    print("🐞 s3_client.list_objects_v2", s3_response)
    if s3_response.get("Contents"):
        s3_keys = [{"Key": s3_object["Key"]} for s3_object in s3_response["Contents"]]
        s3_response = s3_client.delete_objects(
            Bucket=config("PRESERVATION_BUCKET"), Delete={"Objects": s3_keys}
        )
    # RUN DISTILLERY CLOUD PROCESS
    run_distillery(page, f"DistilleryTEST-{test_id}", ["cloud"])
    # get a list of s3 objects under this collection prefix
    s3_response = s3_client.list_objects_v2(
        Bucket=config("PRESERVATION_BUCKET"), Prefix=f"DistilleryTEST-{test_id}"
    )
    print("🐞 s3_client.list_objects_v2", s3_response)
    # ensure that the digital object components were created correctly
    results = asnake_client.get(
        "/repositories/2/find_by_id/digital_objects",
        params={"digital_object_id[]": f"item-{test_id}"},
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
