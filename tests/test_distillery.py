import datetime
import glob
import os
import pytest
import shutil
import subprocess

from asnake.client import ASnakeClient
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


def test_distillery_0000_reset_files(page: Page):
    # NOTE without page parameter test does not run in order
    for d in glob.glob(os.path.join(config("WORKING_ORIGINAL_FILES"), "*/")):
        shutil.move(d, config("INITIAL_ORIGINAL_FILES"))
    for d in glob.glob(os.path.join(config("STAGE_3_ORIGINAL_FILES"), "*/")):
        shutil.move(d, config("INITIAL_ORIGINAL_FILES"))
    for d in glob.glob(os.path.join(config("WORK_PRESERVATION_FILES"), "*/")):
        shutil.rmtree(d)
    for d in glob.glob(os.path.join(config("COMPRESSED_ACCESS_FILES"), "*/")):
        shutil.rmtree(d)


# NOTE cleaning up previous tests and then setting up for new tests is not the
# recommended way to do Arrange and Cleanup in pytest, but it allows manually
# viewing the outcome of tests in ArchivesSpace


def test_distillery_0001_setup_wrong_component_id_948vk(page: Page):
    """Corresponding directory name does not match component_id."""
    # NOTE without page parameter test does not run in order
    try:
        asnake_client = ASnakeClient(
            baseurl=config("ASPACE_API_URL"),
            username=config("ASPACE_USERNAME"),
            password=config("ASPACE_PASSWORD"),
        )
        asnake_client.authorize()
        # DELETE ANY EXISTING TEST RECORDS
        resource_948vk_find_by_id_results = asnake_client.get(
            "/repositories/2/find_by_id/resources",
            params={"identifier[]": ['["DistilleryTEST-948vk"]']},
        ).json()
        print("🐞 resource_948vk_find_by_id_results", resource_948vk_find_by_id_results)
        for resource in resource_948vk_find_by_id_results["resources"]:
            resource_948vk_delete_response = asnake_client.delete(resource["ref"])
            print(
                "🐞 resource_948vk_delete_response",
                resource_948vk_delete_response.json(),
            )
        # CREATE RESOURCE RECORD
        resource_948vk = {}
        resource_948vk["title"] = "_DISTILLERY TEST RESOURCE 948vk"  # required
        resource_948vk["id_0"] = "DistilleryTEST-948vk"  # required
        resource_948vk["level"] = "collection"  # required
        resource_948vk["finding_aid_language"] = "eng"  # required
        resource_948vk["finding_aid_script"] = "Latn"  # required
        resource_948vk["lang_materials"] = [
            {"language_and_script": {"language": "eng", "script": "Latn"}}
        ]  # required
        resource_948vk["dates"] = [
            {
                "label": "creation",
                "date_type": "single",
                "begin": str(datetime.date.today()),
            }
        ]  # required
        resource_948vk["extents"] = [
            {"portion": "whole", "number": "1", "extent_type": "boxes"}
        ]  # required
        resource_948vk_post_response = asnake_client.post(
            "/repositories/2/resources", json=resource_948vk
        )
        print("🐞 resource_948vk_post_response", resource_948vk_post_response.json())
        # CREATE ITEM RECORD
        item_948vk = {}
        item_948vk["title"] = "_DISTILLERY TEST ITEM 948vk"  # title or date required
        item_948vk["component_id"] = "item_948vk"
        item_948vk["level"] = "item"  # required
        item_948vk["resource"] = {
            "ref": resource_948vk_post_response.json()["uri"]
        }  # required
        item_948vk_post_response = asnake_client.post(
            "/repositories/2/archival_objects", json=item_948vk
        )
        print("🐞 item_948vk_post_response", item_948vk_post_response.json())
    except Exception:
        raise


def test_distillery_0001_setup_nonnumeric_sequence_gz36p(page: Page):
    """Sequence strings on TIFF files are alphanumeric not numeric."""
    # NOTE without page parameter test does not run in order
    try:
        asnake_client = ASnakeClient(
            baseurl=config("ASPACE_API_URL"),
            username=config("ASPACE_USERNAME"),
            password=config("ASPACE_PASSWORD"),
        )
        asnake_client.authorize()
        # DELETE ANY EXISTING TEST RECORDS
        resource_find_by_id_results = asnake_client.get(
            "/repositories/2/find_by_id/resources",
            params={"identifier[]": ['["DistilleryTEST-gz36p"]']},
        ).json()
        print("🐞 resource_find_by_id_results", resource_find_by_id_results)
        for resource in resource_find_by_id_results["resources"]:
            resource_tree = asnake_client.get(f'{resource["ref"]}/tree/root').json()
            print("🐞 resource_tree", resource_tree)
            print(
                "🐞 precomputed_waypoints",
                resource_tree["precomputed_waypoints"][""]["0"],
            )
            print(
                "🐞 item",
                resource_tree["precomputed_waypoints"][""]["0"][0]["uri"],
            )
            item_uri = resource_tree["precomputed_waypoints"][""]["0"][0]["uri"]
            print("🐞 item_uri", item_uri)
            archival_object = asnake_client.get(
                item_uri, params={"resolve[]": "digital_object"}
            ).json()
            print(f"🐞 archival_object", archival_object)
            for instance in archival_object["instances"]:
                if instance.get("digital_object"):
                    digital_object_delete_response = asnake_client.delete(
                        instance["digital_object"]["_resolved"]["uri"]
                    )
                    print(
                        f"🐞 digital_object_delete_response",
                        digital_object_delete_response.json(),
                    )
            resource_delete_response = asnake_client.delete(resource["ref"])
            print(
                "🐞 resource_delete_response",
                resource_delete_response.json(),
            )
        # CREATE RESOURCE RECORD
        resource_gz36p = {}
        resource_gz36p["title"] = "_DISTILLERY TEST RESOURCE gz36p"  # required
        resource_gz36p["id_0"] = "DistilleryTEST-gz36p"  # required
        resource_gz36p["level"] = "collection"  # required
        resource_gz36p["finding_aid_language"] = "eng"  # required
        resource_gz36p["finding_aid_script"] = "Latn"  # required
        resource_gz36p["lang_materials"] = [
            {"language_and_script": {"language": "eng", "script": "Latn"}}
        ]  # required
        resource_gz36p["dates"] = [
            {
                "label": "creation",
                "date_type": "single",
                "begin": str(datetime.date.today()),
            }
        ]  # required
        resource_gz36p["extents"] = [
            {"portion": "whole", "number": "1", "extent_type": "boxes"}
        ]  # required
        resource_gz36p_post_response = asnake_client.post(
            "/repositories/2/resources", json=resource_gz36p
        )
        print("🐞 resource_gz36p_post_response", resource_gz36p_post_response.json())
        # CREATE ITEM RECORD
        item_gz36p = {}
        item_gz36p["title"] = "_DISTILLERY TEST ITEM gz36p"  # title or date required
        item_gz36p["component_id"] = "item-gz36p"
        item_gz36p["level"] = "item"  # required
        item_gz36p["resource"] = {
            "ref": resource_gz36p_post_response.json()["uri"]
        }  # required
        item_gz36p_post_response = asnake_client.post(
            "/repositories/2/archival_objects", json=item_gz36p
        )
        print("🐞 item_gz36p_post_response", item_gz36p_post_response.json())
    except Exception:
        raise


def test_distillery_0001_setup(page: Page):
    # NOTE without page parameter test does not run in order

    try:
        asnake_client = ASnakeClient(
            baseurl=config("ASPACE_API_URL"),
            username=config("ASPACE_USERNAME"),
            password=config("ASPACE_PASSWORD"),
        )
        asnake_client.authorize()

        # DELETE ANY EXISTING TEST RECORDS
        resource_0001_find_by_id_results = asnake_client.get(
            "/repositories/2/find_by_id/resources",
            params={"identifier[]": ['["DistilleryTEST0001_collection"]']},
        ).json()
        print("🐞 resource_0001_find_by_id_results", resource_0001_find_by_id_results)
        # delete digital objects
        for resource in resource_0001_find_by_id_results["resources"]:
            resource_0001_tree = asnake_client.get(
                f'{resource["ref"]}/tree/root'
            ).json()
            print("🐞 resource_0001_tree", resource_0001_tree)
            print(
                "🐞 precomputed_waypoints",
                resource_0001_tree["precomputed_waypoints"][""]["0"],
            )
            print(
                "🐞 series",
                resource_0001_tree["precomputed_waypoints"][""]["0"][0]["uri"],
            )
            series_uri = resource_0001_tree["precomputed_waypoints"][""]["0"][0]["uri"]
            print("🐞 series_uri", series_uri)
            series_0001_slice = asnake_client.get(
                f'{resource["ref"]}/tree/waypoint?offset=0&parent_node={series_uri}'
            ).json()
            print("🐞 series_0001_slice", series_0001_slice)
            subseries_0001_slice = asnake_client.get(
                f'{resource["ref"]}/tree/waypoint?offset=0&parent_node={series_0001_slice[0]["uri"]}'
            ).json()
            print("🐞 subseries_0001_slice", subseries_0001_slice)
            for child in subseries_0001_slice:
                archival_object = asnake_client.get(
                    child["uri"], params={"resolve[]": "digital_object"}
                ).json()
                print(f"🐞 archival_object", archival_object)
                for instance in archival_object["instances"]:
                    if instance.get("digital_object"):
                        digital_object_delete_response = asnake_client.delete(
                            instance["digital_object"]["_resolved"]["uri"]
                        )
                        print(
                            f"🐞 digital_object_delete_response",
                            digital_object_delete_response.json(),
                        )
        # delete resources
        for resource in resource_0001_find_by_id_results["resources"]:
            resource_0001_delete_response = asnake_client.delete(resource["ref"])
            print(
                "🐞 resource_0001_delete_response", resource_0001_delete_response.json()
            )

        # CREATE A RESOURCE
        resource_0001 = {}
        resource_0001["title"] = "0001 DISTILLERY TEST RESOURCE"  # required
        resource_0001["id_0"] = "DistilleryTEST0001_collection"  # required
        resource_0001["level"] = "collection"  # required
        resource_0001["finding_aid_language"] = "eng"  # required
        resource_0001["finding_aid_script"] = "Latn"  # required
        resource_0001["lang_materials"] = [
            {"language_and_script": {"language": "eng", "script": "Latn"}}
        ]  # required
        resource_0001["dates"] = [
            {
                "label": "creation",
                "date_type": "single",
                "begin": str(datetime.date.today()),
            }
        ]  # required
        resource_0001["extents"] = [
            {"portion": "whole", "number": "1", "extent_type": "boxes"}
        ]  # required
        resource_0001_post_response = asnake_client.post(
            "/repositories/2/resources", json=resource_0001
        )
        print("🐞 resource_0001_post_response", resource_0001_post_response.json())

        # CREATE ARCHIVAL OBJECT HIERARCHY
        series_0001 = {}
        series_0001["title"] = "0001 DISTILLERY TEST SERIES"  # title or date required
        series_0001["component_id"] = "DistilleryTEST0001_series"
        series_0001["level"] = "series"  # required
        series_0001["resource"] = {
            "ref": resource_0001_post_response.json()["uri"]
        }  # required
        series_0001_post_response = asnake_client.post(
            "/repositories/2/archival_objects", json=series_0001
        )
        print("🐞 series_0001_post_response", series_0001_post_response.json())

        subseries_0001 = {}
        subseries_0001[
            "title"
        ] = "0001 DISTILLERY TEST SUB-SERIES"  # title or date required
        subseries_0001["component_id"] = "DistilleryTEST0001_subseries"
        subseries_0001["level"] = "subseries"  # required
        subseries_0001["resource"] = {
            "ref": resource_0001_post_response.json()["uri"]
        }  # required
        subseries_0001_post_response = asnake_client.post(
            "/repositories/2/archival_objects", json=subseries_0001
        )
        print("🐞 subseries_0001_post_response", subseries_0001_post_response.json())
        subseries_0001_parent_position_post_response = asnake_client.post(
            f'{subseries_0001_post_response.json()["uri"]}/parent',
            params={"parent": series_0001_post_response.json()["id"], "position": 0},
        )
        print(
            "🐞 subseries_0001_parent_position_post_response",
            subseries_0001_parent_position_post_response.json(),
        )

        items_0001 = ["item1", "item2", "item3"]
        for i in items_0001:
            item = {}
            item[
                "title"
            ] = f"0001 DISTILLERY TEST {i}".upper()  # title or date required
            item["component_id"] = f"DistilleryTEST0001_{i}"
            item["level"] = "item"  # required
            item["resource"] = {
                "ref": resource_0001_post_response.json()["uri"]
            }  # required
            item_post_response = asnake_client.post(
                "/repositories/2/archival_objects", json=item
            )
            print(f"🐞 {i}_post_response", item_post_response.json())
            item_parent_position_post_response = asnake_client.post(
                f'{item_post_response.json()["uri"]}/parent',
                params={
                    "parent": subseries_0001_post_response.json()["id"],
                    "position": 1,
                },
            )
            print(
                f"🐞 {i}_parent_position_post_response",
                item_parent_position_post_response.json(),
            )
    except Exception:
        raise


def test_distillery_landing(page: Page):
    page.goto(config("BASE_URL"))
    expect(page).to_have_title("Distillery")


def test_distillery_cloud(page: Page):
    page.goto(config("BASE_URL"))
    page.get_by_label("Collection ID").fill("DistilleryTEST0001_collection")
    page.get_by_text(
        "Cloud preservation storage generate and send files to a remote storage provider"
    ).click()
    page.get_by_role("button", name="Validate").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        "✅ Validated metadata, files, and destinations for DistilleryTEST0001_collection."
    )
    page.get_by_role("button", name="Run").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        "✅ Processed metadata and files for DistilleryTEST0001_collection.",
        timeout=60000,
    )


def test_distillery_cloud_wrong_component_id_948vk(page: Page):
    page.goto(config("BASE_URL"))
    page.get_by_label("Collection ID").fill("DistilleryTEST-948vk")
    page.get_by_text(
        "Cloud preservation storage generate and send files to a remote storage provider"
    ).click()
    page.get_by_role("button", name="Validate").click()
    page.get_by_text("Details").click()
    expect(page.locator("p")).to_have_text(
        "❌ Something went wrong. View the details for more information."
    )


def test_distillery_cloud_nonnumeric_sequence_gz36p(page: Page):
    page.goto(config("BASE_URL"))
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
    # TODO: test that the digital object component was created correctly
    asnake_client = ASnakeClient(
        baseurl=config("ASPACE_API_URL"),
        username=config("ASPACE_USERNAME"),
        password=config("ASPACE_PASSWORD"),
    )
    asnake_client.authorize()
    digital_object_find_by_id_results = asnake_client.get(
        "/repositories/2/find_by_id/digital_objects",
        params={"digital_object_id[]": "item-gz36p"},
    ).json()
    print("🐞 digital_object_find_by_id_results", digital_object_find_by_id_results)
    assert len(digital_object_find_by_id_results["digital_objects"]) == 1
    for digital_object in digital_object_find_by_id_results["digital_objects"]:
        digital_object_tree = asnake_client.get(
            f'{digital_object["ref"]}/tree/root'
        ).json()
        print("🐞 digital_object_tree", digital_object_tree)
        assert len(digital_object_tree["precomputed_waypoints"][""]["0"]) > 0
        print(
            "🐞 precomputed_waypoints",
            digital_object_tree["precomputed_waypoints"][""]["0"],
        )
        print(
            "🐞 component",
            digital_object_tree["precomputed_waypoints"][""]["0"][0]["uri"],
        )
        component_uri = digital_object_tree["precomputed_waypoints"][""]["0"][0]["uri"]
        print("🐞 component_uri", component_uri)
        digital_object_component = asnake_client.get(component_uri).json()
        print(f"🐞 digital_object_component", digital_object_component)
