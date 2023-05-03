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


def test_distillery_0000_reset_db(page: Page):
    subprocess.run(
        [
            "scp",
            "-i",
            config("ARCHIVESSPACE_SSH_KEY"),
            "-P",
            config("ARCHIVESSPACE_SSH_PORT"),
            f"{os.path.dirname(__file__)}/assets/reset.sh",
            f'{config("ARCHIVESSPACE_SSH_USER")}@{config("ARCHIVESSPACE_SSH_HOST")}:/tmp/reset.sh',
        ],
        check=True,
    )
    subprocess.run(
        [
            "ssh",
            "-t",
            "-i",
            config("ARCHIVESSPACE_SSH_KEY"),
            f'{config("ARCHIVESSPACE_SSH_USER")}@{config("ARCHIVESSPACE_SSH_HOST")}',
            f'-p{config("ARCHIVESSPACE_SSH_PORT")}',
            "sudo",
            "/bin/sh",
            "/tmp/reset.sh",
            f'{config("ARCHIVESSPACE_RESET_DB")}',
        ],
        check=True,
    )


def test_distillery_0000_reset_files(page: Page):
    for d in glob.glob(os.path.join(config("WORKING_ORIGINAL_FILES"), "*/")):
        shutil.move(d, config("INITIAL_ORIGINAL_FILES"))
    for d in glob.glob(os.path.join(config("STAGE_3_ORIGINAL_FILES"), "*/")):
        shutil.move(d, config("INITIAL_ORIGINAL_FILES"))
    for d in glob.glob(os.path.join(config("WORK_PRESERVATION_FILES"), "*/")):
        shutil.rmtree(d)
    for d in glob.glob(os.path.join(config("COMPRESSED_ACCESS_FILES"), "*/")):
        shutil.rmtree(d)


def test_distillery_0001_setup(page: Page):
    # NOTE without page parameter test does not run first

    asnake_client = ASnakeClient(
        baseurl=config("ASPACE_API_URL"),
        username=config("ASPACE_USERNAME"),
        password=config("ASPACE_PASSWORD"),
    )
    asnake_client.authorize()

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
    subseries_0001_parent_position_post_response = asnake_client.post(
        f'{subseries_0001_post_response.json()["uri"]}/parent',
        params={"parent": series_0001_post_response.json()["id"], "position": 0},
    )
    item_0001_1 = {}
    item_0001_1["title"] = "0001 DISTILLERY TEST ITEM 1"  # title or date required
    item_0001_1["component_id"] = "DistilleryTEST0001_item1"
    item_0001_1["level"] = "item"  # required
    item_0001_1["resource"] = {
        "ref": resource_0001_post_response.json()["uri"]
    }  # required
    item_0001_1_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=item_0001_1
    )
    # NOTE position 0 for all causes later items to be first;
    # position 1 for all causes later items to be last
    item_0001_1_parent_position_post_response = asnake_client.post(
        f'{item_0001_1_post_response.json()["uri"]}/parent',
        params={"parent": subseries_0001_post_response.json()["id"], "position": 1},
    )
    item_0001_2 = {}
    item_0001_2["title"] = "0001 DISTILLERY TEST ITEM 2"  # title or date required
    item_0001_2["component_id"] = "DistilleryTEST0001_item2"
    item_0001_2["level"] = "item"  # required
    item_0001_2["resource"] = {
        "ref": resource_0001_post_response.json()["uri"]
    }  # required
    item_0001_2_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=item_0001_2
    )
    # NOTE position 0 for all causes later items to be first;
    # position 1 for all causes later items to be last
    item_0001_2_parent_position_post_response = asnake_client.post(
        f'{item_0001_2_post_response.json()["uri"]}/parent',
        params={"parent": subseries_0001_post_response.json()["id"], "position": 1},
    )
    item_0001_3 = {}
    item_0001_3["title"] = "0001 DISTILLERY TEST ITEM 3"  # title or date required
    item_0001_3["component_id"] = "DistilleryTEST0001_item3"
    item_0001_3["level"] = "item"  # required
    item_0001_3["resource"] = {
        "ref": resource_0001_post_response.json()["uri"]
    }  # required
    item_0001_3_post_response = asnake_client.post(
        "/repositories/2/archival_objects", json=item_0001_3
    )
    # NOTE position 0 for all causes later items to be first;
    # position 1 for all causes later items to be last
    item_0001_3_parent_position_post_response = asnake_client.post(
        f'{item_0001_3_post_response.json()["uri"]}/parent',
        params={"parent": subseries_0001_post_response.json()["id"], "position": 1},
    )


def test_distillery_landing(page: Page):
    page.goto(config("BASE_URL"))
    expect(page).to_have_title("Distillery")
