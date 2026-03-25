from pathlib import Path

import distillery


def test_get_archival_object_directory_prefix_sanitizes_title_and_trailing_slash():
    arrangement = {
        "collection_id": "EXAMPLE:123",
        "archival_object_title": "Field Notes, Box #1/2",
    }
    archival_object = {"component_id": "aspace_abcdef"}

    prefix = distillery.get_archival_object_directory_prefix(arrangement, archival_object)

    assert prefix == "EXAMPLE:123/aspace_abcdef--Field-Notes--Box--1-2/"


def test_get_archival_object_datafile_key_uses_component_id_and_json_extension():
    archival_object = {"component_id": "aspace_1234"}

    datafile_key = distillery.get_archival_object_datafile_key("COLL/aspace_1234--Title/", archival_object)

    assert datafile_key == "COLL/aspace_1234--Title/aspace_1234.json"


def test_get_digital_object_component_file_key_builds_expected_path():
    file_parts = {
        "filename": "EXAMPLE_0001.tiff",
        "crockford_id": "ab12_cd34",
        "extension": "tiff",
        "filepath": str(Path("/tmp/EXAMPLE_0001.tiff")),
        "filestem": "EXAMPLE_0001",
        "sequence": "0001",
    }

    file_key = distillery.get_digital_object_component_file_key(
        "EXAMPLE:123/aspace_abcdef--Field-Notes/", file_parts
    )

    assert file_key == (
        "EXAMPLE:123/aspace_abcdef--Field-Notes/"
        "EXAMPLE_0001.tiff/ab12_cd34.tiff"
    )
