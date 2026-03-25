from pathlib import Path
import hashlib
import json
import tempfile

import pytest

import distillery


def make_validation_results(**overrides):
    base = {
        "eligible_archival_objects": {"aspace_1": {}},
        "ineligible_archival_objects": [],
        "nested_directories": [],
        "empty_directories": [],
        "file_count": 1,
        "source_path": None,
    }
    base.update(overrides)
    return base


def test_get_crockford_characters_default_length_is_four():
    value = distillery.get_crockford_characters()

    assert len(value) == 4


def test_get_crockford_characters_uses_allowed_charset():
    value = distillery.get_crockford_characters(12)
    allowed = set("abcdefghjkmnpqrstvwxyz0123456789")

    assert len(value) == 12
    assert set(value).issubset(allowed)


def test_get_crockford_id_has_expected_shape_and_charset():
    value = distillery.get_crockford_id()
    allowed = set("abcdefghjkmnpqrstvwxyz0123456789")

    assert len(value) == 9
    assert value[4] == "_"
    assert set(value.replace("_", "")).issubset(allowed)


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


def test_get_digital_object_component_file_key_preserves_prefix_and_extension_variant():
    file_parts = {
        "filename": "OH_Acosta_A_0007.wav",
        "crockford_id": "wxyz_9876",
        "extension": "wav",
        "filepath": str(Path("/tmp/OH_Acosta_A_0007.wav")),
        "filestem": "OH_Acosta_A_0007",
        "sequence": "0007",
    }

    file_key = distillery.get_digital_object_component_file_key(
        "CALT-001/aspace_777--Interview-Audio/", file_parts
    )

    assert file_key == (
        "CALT-001/aspace_777--Interview-Audio/"
        "OH_Acosta_A_0007.wav/wxyz_9876.wav"
    )


def test_get_file_parts_standard_filename_shape():
    filepath = Path("/tmp/EXAMPLE_02_0B_056_07_0001.tiff")

    parts = distillery.get_file_parts(filepath)

    assert parts["filepath"] == "/tmp/EXAMPLE_02_0B_056_07_0001.tiff"
    assert parts["filename"] == "EXAMPLE_02_0B_056_07_0001.tiff"
    assert parts["filestem"] == "EXAMPLE_02_0B_056_07_0001"
    assert parts["extension"] == "tiff"
    assert parts["sequence"] == "0001"
    assert len(parts["crockford_id"]) == 9
    assert parts["crockford_id"][4] == "_"


def test_get_file_parts_multidot_filename_current_behavior():
    filepath = Path("/tmp/interview.audio.master.wav")

    parts = distillery.get_file_parts(filepath)

    assert parts["filename"] == "interview.audio.master.wav"
    assert parts["filestem"] == "interview"
    assert parts["extension"] == "wav"
    assert parts["sequence"] == "interview"


def test_chunk_hash_returns_expected_md5_for_known_bytes(tmp_path):
    filepath = tmp_path / "known.bin"
    filepath.write_bytes(b"hello world")

    digest = distillery.chunk_hash(filepath).hexdigest()

    assert digest == "5eb63bbbe01eeed093cb22bb8f5acdc3"


def test_construct_file_version_builds_expected_payload(tmp_path):
    build_root = tmp_path / "build"
    filepath = build_root / "COLL" / "aspace_123" / "file.tiff"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_bytes(b"abc")

    variables = {
        "file_uri_scheme": "s3",
        "file_uri_host": "my-bucket",
        "preservation_file_info": {
            "filepath": filepath,
            "md5": hashlib.md5(b"abc"),
            "filesize": filepath.stat().st_size,
        },
    }
    build_directory = tempfile.TemporaryDirectory(dir=tmp_path)
    build_directory.name = str(build_root.resolve())

    file_version = distillery.construct_file_version(variables, build_directory)

    assert file_version == {
        "checksum_method": "md5",
        "checksum": "900150983cd24fb0d6963f7d28e17f72",
        "file_size_bytes": 3,
        "file_uri": "s3://my-bucket/COLL/aspace_123/file.tiff",
    }


def test_construct_file_version_coerces_string_filesize_to_int(tmp_path):
    build_root = tmp_path / "build"
    filepath = build_root / "COLL" / "aspace_123" / "file.tiff"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_bytes(b"abc")

    variables = {
        "file_uri_scheme": "s3",
        "file_uri_host": "my-bucket",
        "preservation_file_info": {
            "filepath": filepath,
            "md5": hashlib.md5(b"abc"),
            "filesize": "3",
        },
    }
    build_directory = tempfile.TemporaryDirectory(dir=tmp_path)
    build_directory.name = str(build_root.resolve())

    file_version = distillery.construct_file_version(variables, build_directory)

    assert file_version["file_size_bytes"] == 3


def test_construct_file_version_uses_relative_nested_path_in_file_uri(tmp_path):
    build_root = tmp_path / "build"
    filepath = build_root / "collection-001" / "aspace_xyz" / "preservation" / "master.wav"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_bytes(b"hello")

    variables = {
        "file_uri_scheme": "s3",
        "file_uri_host": "archive-bucket",
        "preservation_file_info": {
            "filepath": filepath,
            "md5": hashlib.md5(b"hello"),
            "filesize": 5,
        },
    }
    build_directory = tempfile.TemporaryDirectory(dir=tmp_path)
    build_directory.name = str(build_root.resolve())

    file_version = distillery.construct_file_version(variables, build_directory)

    assert (
        file_version["file_uri"]
        == "s3://archive-bucket/collection-001/aspace_xyz/preservation/master.wav"
    )


def test_save_collection_datafile_writes_json_and_returns_expected_key(tmp_path):
    collection_data = {
        "id_0": "CALT-001",
        "title": "Test Collection",
        "uri": "/repositories/2/resources/123",
    }

    key = distillery.save_collection_datafile(collection_data, str(tmp_path))

    assert key == "CALT-001/CALT-001.json"
    written_path = tmp_path / key
    assert written_path.exists()
    assert json.loads(written_path.read_text()) == collection_data


def test_save_archival_object_datafile_writes_json_and_returns_expected_key(tmp_path):
    arrangement = {
        "collection_id": "CALT-001",
        "archival_object_title": "Interview Tape #1",
    }
    archival_object = {
        "component_id": "aspace_abc123",
        "title": "Interview Tape 1",
    }

    key = distillery.save_archival_object_datafile(
        arrangement, archival_object, str(tmp_path)
    )

    assert key == "CALT-001/aspace_abc123--Interview-Tape--1/aspace_abc123.json"
    written_path = tmp_path / key
    assert written_path.exists()
    assert json.loads(written_path.read_text()) == archival_object


def test_validate_digital_files_for_preservation_raises_when_no_files_found():
    validation_results = make_validation_results(file_count=0)

    with pytest.raises(FileNotFoundError, match="NO FILES FOUND"):
        distillery.validate_digital_files_for_preservation(validation_results)


def test_validate_digital_files_for_preservation_raises_for_ineligible_archival_objects():
    validation_results = make_validation_results(
        ineligible_archival_objects=["aspace_bad_1"]
    )

    with pytest.raises(RuntimeError, match="INELIGIBLE ARCHIVAL OBJECTS FOUND"):
        distillery.validate_digital_files_for_preservation(validation_results)


def test_validate_digital_files_for_preservation_raises_for_nested_directories():
    validation_results = make_validation_results(nested_directories=["nested/dir"])

    with pytest.raises(RuntimeError, match="NESTED DIRECTORIES FOUND"):
        distillery.validate_digital_files_for_preservation(validation_results)


def test_validate_digital_files_for_preservation_raises_when_space_is_insufficient(
    tmp_path, monkeypatch
):
    source_path = tmp_path / "source"
    source_path.mkdir()
    # Create a 10-byte top-level file entry.
    (source_path / "aspace_1").write_bytes(b"0123456789")

    class DummyDiskUsage:
        total = 100
        used = 95
        free = 10

    monkeypatch.setattr(distillery.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(distillery.shutil, "disk_usage", lambda _p: DummyDiskUsage())

    validation_results = make_validation_results(source_path=source_path)

    with pytest.raises(RuntimeError, match="INSUFFICIENT LOCAL SPACE"):
        distillery.validate_digital_files_for_preservation(validation_results)


def test_validate_digital_files_for_preservation_raises_for_empty_directories():
    validation_results = make_validation_results(empty_directories=["empty_dir"])

    with pytest.raises(RuntimeError, match="EMPTY DIRECTORIES FOUND"):
        distillery.validate_digital_files_for_preservation(validation_results)


def test_validate_digital_files_for_preservation_happy_path_no_exception(
    tmp_path, monkeypatch
):
    source_path = tmp_path / "source"
    source_path.mkdir()
    (source_path / "aspace_1").write_bytes(b"abc")

    class DummyDiskUsage:
        total = 10 * (1024**3)
        used = 1 * (1024**3)
        free = 9 * (1024**3)

    monkeypatch.setattr(distillery.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(distillery.shutil, "disk_usage", lambda _p: DummyDiskUsage())

    validation_results = make_validation_results(source_path=source_path)

    distillery.validate_digital_files_for_preservation(validation_results)


def test_ensure_digital_object_creates_when_none_exist(monkeypatch):
    archival_object = {"component_id": "aspace_1", "instances": []}

    def fake_create_digital_object(obj):
        obj["instances"].append({"digital_object": {"ref": "/digital_objects/1"}})
        return "/digital_objects/1", obj

    monkeypatch.setattr(distillery.cascflow, "create_digital_object", fake_create_digital_object)

    digital_object_uri, returned_archival_object = distillery.ensure_digital_object(
        archival_object
    )

    assert digital_object_uri == "/digital_objects/1"
    assert returned_archival_object["instances"][0]["digital_object"]["ref"] == "/digital_objects/1"


def test_ensure_digital_object_returns_existing_when_one_exists():
    archival_object = {
        "component_id": "aspace_1",
        "instances": [{"digital_object": {"ref": "/digital_objects/99"}}],
    }

    digital_object_uri, returned_archival_object = distillery.ensure_digital_object(
        archival_object
    )

    assert digital_object_uri == "/digital_objects/99"
    assert returned_archival_object == archival_object


def test_ensure_digital_object_raises_when_multiple_exist():
    archival_object = {
        "component_id": "aspace_1",
        "instances": [
            {"digital_object": {"ref": "/digital_objects/1"}},
            {"digital_object": {"ref": "/digital_objects/2"}},
        ],
    }

    with pytest.raises(ValueError, match="MULTIPLE DIGITAL OBJECTS FOUND"):
        distillery.ensure_digital_object(archival_object)
