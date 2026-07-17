from pathlib import Path
import hashlib
import json

import pytest

import arkminter
import cascflow
import distillery


def make_settings_validation_results(**overrides):
    base = {"results": [], "missing": [], "invalid": []}
    base.update(overrides)
    return base


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


def test_chunk_hashes_returns_expected_digests_for_known_bytes(tmp_path):
    filepath = tmp_path / "known.bin"
    filepath.write_bytes(b"hello world")

    digests = distillery.chunk_hashes(filepath)

    assert digests == {
        "sha256": "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
        "md5": "5eb63bbbe01eeed093cb22bb8f5acdc3",
    }


def test_chunk_hashes_supports_a_subset_of_algorithms(tmp_path):
    filepath = tmp_path / "known.bin"
    filepath.write_bytes(b"hello world")

    digests = distillery.chunk_hashes(filepath, algorithms=("md5",))

    assert digests == {"md5": "5eb63bbbe01eeed093cb22bb8f5acdc3"}


def make_preservation_mount_config(mount_parent, **overrides):
    values = {
        "PRESERVATION_MOUNT_PARENT": str(mount_parent),
        "RELATIVE_OCFL_STORAGE_DIRECTORY": "ocfl-storage",
        "RELATIVE_OCFL_STAGING_DIRECTORY": "ocfl-staging",
    }
    values.update(overrides)
    return lambda key, **kwargs: values.get(key, kwargs.get("default"))


def test_ocfl_storage_root_combines_mount_parent_and_relative_directory(monkeypatch):
    monkeypatch.setattr(distillery, "config", make_preservation_mount_config("/mnt"))

    storage_root = distillery.ocfl_storage_root()

    assert storage_root == Path("/mnt/ocfl-storage")


def test_ocfl_staging_root_combines_mount_parent_and_relative_directory(monkeypatch):
    monkeypatch.setattr(distillery, "config", make_preservation_mount_config("/mnt"))

    staging_root = distillery.ocfl_staging_root()

    assert staging_root == Path("/mnt/ocfl-staging")


def test_ocfl_object_root_combines_storage_root_and_extension_path(monkeypatch):
    monkeypatch.setattr(distillery, "config", make_preservation_mount_config("/mnt"))

    object_root = distillery.ocfl_object_root("https://n2t.net/ark:99999/b3xq7z")

    assert object_root == Path("/mnt/ocfl-storage/b3x/q7z/b3xq7z")


def test_load_ocfl_inventory_builds_fresh_shell_when_missing(tmp_path):
    object_root = tmp_path / "obj"

    inventory = distillery.load_ocfl_inventory(object_root, "id1")

    assert inventory["id"] == "id1"
    assert inventory["head"] is None
    assert inventory["digestAlgorithm"] == "sha256"


def test_load_ocfl_inventory_loads_existing_file(tmp_path):
    object_root = tmp_path / "obj"
    object_root.mkdir()
    existing = {"id": "id1", "head": "v0001"}
    object_root.joinpath("inventory.json").write_text(json.dumps(existing))

    inventory = distillery.load_ocfl_inventory(object_root, "id1")

    assert inventory == existing


def test_write_ocfl_inventory_files_writes_inventory_and_sidecar(tmp_path):
    directory = tmp_path / "somewhere"
    directory.mkdir()
    inventory = cascflow.ocfl.build_inventory("id1")
    inventory = cascflow.ocfl.add_version(
        inventory, {"a.txt": "digest_a"}, created="t1"
    )

    distillery.write_ocfl_inventory_files(directory, inventory)

    assert json.loads(directory.joinpath("inventory.json").read_text()) == inventory
    sidecar = directory.joinpath("inventory.json.sha256").read_text()
    assert sidecar.endswith(" inventory.json\n")


def test_write_current_ocfl_inventory_writes_declaration_and_cleans_up_staging(
    tmp_path,
):
    object_root = tmp_path / "obj"
    staging_root = tmp_path / "staging"
    staging_root.mkdir()
    inventory = cascflow.ocfl.build_inventory("id1")
    inventory = cascflow.ocfl.add_version(
        inventory, {"a.txt": "digest_a"}, created="t1"
    )

    distillery.write_current_ocfl_inventory(object_root, inventory, staging_root)

    assert object_root.joinpath("0=ocfl_object_1.1").read_text() == "ocfl_object_1.1\n"
    assert json.loads(object_root.joinpath("inventory.json").read_text()) == inventory
    assert object_root.joinpath("inventory.json.sha256").exists()
    # the temporary staging directory used internally is cleaned up
    assert list(staging_root.iterdir()) == []


def test_write_current_ocfl_inventory_replaces_on_a_later_call(tmp_path):
    object_root = tmp_path / "obj"
    staging_root = tmp_path / "staging"
    staging_root.mkdir()
    v1 = cascflow.ocfl.add_version(
        cascflow.ocfl.build_inventory("id1"), {"a.txt": "digest_a"}, created="t1"
    )
    v2 = cascflow.ocfl.add_version(
        v1, {"a.txt": "digest_a", "b.txt": "d2"}, created="t2"
    )

    distillery.write_current_ocfl_inventory(object_root, v1, staging_root)
    distillery.write_current_ocfl_inventory(object_root, v2, staging_root)

    assert json.loads(object_root.joinpath("inventory.json").read_text()) == v2
    # the declaration file isn't rewritten/duplicated on the later call
    assert object_root.joinpath("0=ocfl_object_1.1").read_text() == "ocfl_object_1.1\n"


def test_build_version_files_maps_filenames_to_digests_and_sources(tmp_path):
    file1 = tmp_path / "a.txt"
    file2 = tmp_path / "b.txt"
    file1.write_bytes(b"aaa")
    file2.write_bytes(b"bbb")
    archival_object = {"component_id": "aspace_1", "title": "Test"}

    files, fixity, sources, ao_bytes = distillery.build_version_files(
        archival_object, [file1, file2]
    )

    assert set(files.keys()) == {"a.txt", "b.txt", "archival_object.json"}
    assert files["a.txt"] == hashlib.sha256(b"aaa").hexdigest()
    assert fixity["md5"]["a.txt"] == hashlib.md5(b"aaa").hexdigest()
    assert sources["a.txt"] == file1
    assert ao_bytes == json.dumps(archival_object, indent=4).encode()
    assert files["archival_object.json"] == hashlib.sha256(ao_bytes).hexdigest()


def test_write_ocfl_version_writes_content_and_inventory(tmp_path, monkeypatch):
    mount_parent = tmp_path / "mount"
    monkeypatch.setattr(
        distillery, "config", make_preservation_mount_config(mount_parent)
    )
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    filepath = source_dir / "photo.tif"
    filepath.write_bytes(b"photo-bytes")
    archival_object = {
        "component_id": "aspace_1",
        "external_ark_url": "https://n2t.net/ark:99999/b3xq7z",
    }

    inventory = distillery.write_ocfl_version(archival_object, [filepath])

    assert inventory["head"] == "v0001"
    object_root = mount_parent / "ocfl-storage" / "b3x" / "q7z" / "b3xq7z"
    assert (
        object_root / "v0001" / "content" / "photo.tif"
    ).read_bytes() == b"photo-bytes"
    assert (object_root / "v0001" / "content" / "archival_object.json").exists()
    assert (object_root / "inventory.json").exists()
    # the temporary staging directory used internally is cleaned up
    assert list((mount_parent / "ocfl-staging").iterdir()) == []


def test_write_ocfl_version_does_not_recopy_unchanged_content_on_second_version(
    tmp_path, monkeypatch
):
    mount_parent = tmp_path / "mount"
    monkeypatch.setattr(
        distillery, "config", make_preservation_mount_config(mount_parent)
    )
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    photo = source_dir / "photo.tif"
    photo.write_bytes(b"photo-bytes")
    audio = source_dir / "audio.wav"
    audio.write_bytes(b"audio-bytes")
    archival_object = {
        "component_id": "aspace_1",
        "external_ark_url": "https://n2t.net/ark:99999/b3xq7z",
    }

    distillery.write_ocfl_version(archival_object, [photo])
    inventory = distillery.write_ocfl_version(archival_object, [photo, audio])

    object_root = mount_parent / "ocfl-storage" / "b3x" / "q7z" / "b3xq7z"
    assert inventory["head"] == "v0002"
    # photo.tif's bytes (and archival_object.json's, since it's unchanged
    # too) were already stored in v0001 -- v0002 shouldn't duplicate them
    assert not (object_root / "v0002" / "content" / "photo.tif").exists()
    assert not (object_root / "v0002" / "content" / "archival_object.json").exists()
    assert (
        object_root / "v0002" / "content" / "audio.wav"
    ).read_bytes() == b"audio-bytes"


def test_construct_inventory_file_version_points_at_ark_relative_inventory_url():
    archival_object = {"external_ark_url": "https://n2t.net/ark:99999/b3xq7z"}

    file_version = distillery.construct_inventory_file_version(archival_object)

    assert file_version == {
        "file_uri": "https://n2t.net/ark:99999/b3xq7z/inventory.json",
        "publish": False,
    }


def test_build_inventory_manifest_note_lists_current_version_state():
    inventory = cascflow.ocfl.build_inventory("id1")
    inventory = cascflow.ocfl.add_version(
        inventory, {"b.txt": "digest_b", "a.txt": "digest_a"}, created="t1"
    )

    note = distillery.build_inventory_manifest_note(inventory)

    assert note["jsonmodel_type"] == "note_digital_object"
    assert note["label"] == "OCFL Inventory"
    assert note["publish"] is False
    content = note["content"][0]
    assert "v0001" in content
    assert "a.txt (sha256: digest_a)" in content
    assert "b.txt (sha256: digest_b)" in content


def test_update_inventory_manifest_note_replaces_prior_ocfl_note_only():
    digital_object = {
        "notes": [
            {
                "jsonmodel_type": "note_digital_object",
                "label": "OCFL Inventory",
                "content": ["old"],
            },
            {
                "jsonmodel_type": "note_digital_object",
                "label": "Other Note",
                "content": ["keep me"],
            },
        ]
    }
    inventory = cascflow.ocfl.build_inventory("id1")
    inventory = cascflow.ocfl.add_version(
        inventory, {"a.txt": "digest_a"}, created="t1"
    )

    result = distillery.update_inventory_manifest_note(digital_object, inventory)

    ocfl_notes = [n for n in result["notes"] if n["label"] == "OCFL Inventory"]
    other_notes = [n for n in result["notes"] if n["label"] == "Other Note"]
    assert len(ocfl_notes) == 1
    assert "old" not in ocfl_notes[0]["content"][0]
    assert len(other_notes) == 1


def test_process_archival_object_preservation_files_writes_version_and_updates_as(
    tmp_path, monkeypatch
):
    storage_root = tmp_path / "ocfl_root"
    monkeypatch.setattr(distillery, "config", lambda key, **kwargs: str(storage_root))
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    filepath = source_dir / "photo.tif"
    filepath.write_bytes(b"photo-bytes")
    archival_object = {
        "component_id": "aspace_1",
        "external_ark_url": "https://n2t.net/ark:99999/b3xq7z",
        "instances": [{"digital_object": {"ref": "/digital_objects/1"}}],
    }

    save_calls = []
    monkeypatch.setattr(
        distillery.cascflow,
        "save_digital_object_file_versions",
        lambda ao, fvs: save_calls.append((ao, fvs)),
    )

    digital_object_state = {"uri": "/digital_objects/1", "notes": []}
    post_calls = []

    def fake_archivesspace_get(uri):
        class Resp:
            def json(self):
                return digital_object_state

        return Resp()

    def fake_archivesspace_post(uri, obj):
        post_calls.append((uri, obj))

    monkeypatch.setattr(
        distillery.cascflow, "archivesspace_get", fake_archivesspace_get
    )
    monkeypatch.setattr(
        distillery.cascflow, "archivesspace_post", fake_archivesspace_post
    )

    distillery.process_archival_object_preservation_files(
        archival_object, "/digital_objects/1", [filepath]
    )

    assert len(save_calls) == 1
    assert (
        save_calls[0][1][0]["file_uri"]
        == "https://n2t.net/ark:99999/b3xq7z/inventory.json"
    )
    assert len(post_calls) == 1
    posted_uri, posted_digital_object = post_calls[0]
    assert posted_uri == "/digital_objects/1"
    ocfl_notes = [
        n for n in posted_digital_object["notes"] if n["label"] == "OCFL Inventory"
    ]
    assert len(ocfl_notes) == 1


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


def test_validate_digital_files_for_preservation_raises_for_reserved_filename(
    tmp_path,
):
    source_path = tmp_path / "source"
    aspace_dir = source_path / "aspace_1"
    aspace_dir.mkdir(parents=True)
    (aspace_dir / "archival_object.json").write_text("{}")

    validation_results = make_validation_results(source_path=source_path)

    with pytest.raises(RuntimeError, match="RESERVED FILENAME"):
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

    monkeypatch.setattr(distillery, "config", lambda key, **kwargs: str(tmp_path))
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

    monkeypatch.setattr(distillery, "config", lambda key, **kwargs: str(tmp_path))
    monkeypatch.setattr(distillery.shutil, "disk_usage", lambda _p: DummyDiskUsage())

    validation_results = make_validation_results(source_path=source_path)

    distillery.validate_digital_files_for_preservation(validation_results)


def test_validate_settings_for_preservation_raises_for_missing_settings():
    settings_validation_results = make_settings_validation_results(
        missing=[
            {
                "key": "RESOLVER_BUCKET",
                "kind": "string",
                "status": "missing",
                "detail": "RESOLVER_BUCKET is not set",
            }
        ]
    )

    with pytest.raises(RuntimeError, match="SETTINGS VALIDATION FAILED"):
        distillery.validate_settings_for_preservation(settings_validation_results)


def test_validate_settings_for_preservation_raises_for_invalid_settings():
    settings_validation_results = make_settings_validation_results(
        invalid=[
            {
                "key": "PRESERVATION_MOUNT_PARENT",
                "kind": "directory",
                "status": "invalid",
                "detail": "PRESERVATION_MOUNT_PARENT is not a directory",
            }
        ]
    )

    with pytest.raises(RuntimeError, match="SETTINGS VALIDATION FAILED"):
        distillery.validate_settings_for_preservation(settings_validation_results)


def test_validate_settings_for_preservation_happy_path_no_exception():
    settings_validation_results = make_settings_validation_results()

    distillery.validate_settings_for_preservation(settings_validation_results)


def test_ensure_digital_object_creates_when_none_exist(monkeypatch):
    archival_object = {"component_id": "aspace_1", "instances": []}

    def fake_create_digital_object(obj):
        obj["instances"].append({"digital_object": {"ref": "/digital_objects/1"}})
        return "/digital_objects/1", obj

    monkeypatch.setattr(
        distillery.cascflow, "create_digital_object", fake_create_digital_object
    )

    digital_object_uri, returned_archival_object = distillery.ensure_digital_object(
        archival_object
    )

    assert digital_object_uri == "/digital_objects/1"
    assert (
        returned_archival_object["instances"][0]["digital_object"]["ref"]
        == "/digital_objects/1"
    )


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


def test_mint_ark_for_archival_object_skips_when_nma_or_naan_not_configured(
    monkeypatch,
):
    monkeypatch.setattr(distillery, "config", lambda key, **kwargs: kwargs.get("default"))

    archival_object = {"component_id": "aspace_1"}

    assert distillery.mint_ark_for_archival_object(archival_object) is None
    assert "external_ark_url" not in archival_object


def test_mint_ark_for_archival_object_skips_when_existing_ark_is_valid(monkeypatch):
    def fake_config(key, **kwargs):
        values = {"OUR_NMA": "resolver.example.org", "OUR_NAAN": "99999"}
        return values.get(key, kwargs.get("default"))

    monkeypatch.setattr(distillery, "config", fake_config)

    archival_object = {
        "component_id": "aspace_1",
        "external_ark_url": "https://resolver.example.org/ark:/99999/abc123x",
    }

    assert distillery.mint_ark_for_archival_object(archival_object) is None
    # Untouched -- an already-valid ARK is left as-is, not re-minted
    assert (
        archival_object["external_ark_url"]
        == "https://resolver.example.org/ark:/99999/abc123x"
    )


def test_mint_ark_for_archival_object_raises_when_s3_connection_fails(monkeypatch):
    def fake_config(key, **kwargs):
        values = {"OUR_NMA": "resolver.example.org", "OUR_NAAN": "99999"}
        return values.get(key, kwargs.get("default"))

    monkeypatch.setattr(distillery, "config", fake_config)
    monkeypatch.setattr(distillery.cascflow, "establish_s3_connection", lambda: None)

    archival_object = {"component_id": "aspace_1"}

    with pytest.raises(RuntimeError, match="Failed to establish S3 connection"):
        distillery.mint_ark_for_archival_object(archival_object)


def test_mint_ark_for_archival_object_mints_and_sets_external_ark_url(monkeypatch):
    def fake_config(key, **kwargs):
        values = {
            "OUR_NMA": "resolver.example.org",
            "OUR_NAAN": "99999",
            "ARCHIVESSPACE_STAFF_URL": "https://archivesspace.example.org",
            "RESOLVER_BUCKET": "resolver-bucket",
        }
        return values.get(key, kwargs.get("default"))

    # distillery's own config() calls, and cascflow's internal config() calls
    # (used inside establish_s3_connection()) are two separate lookups --
    # both need patching for this to run hermetically. See get_our_ark's
    # equivalent test in alchemist for the same reasoning.
    monkeypatch.setattr(distillery, "config", fake_config)
    monkeypatch.setattr(distillery.cascflow.cascflow, "_config", fake_config)

    mint_calls = []

    def fake_mint_ark(**kwargs):
        mint_calls.append(kwargs)
        return "ark:99999/abc123x"

    monkeypatch.setattr(arkminter, "mint_ark", fake_mint_ark)

    archival_object = {
        "component_id": "aspace_1",
        "title": "Test AO",
        "uri": "/repositories/2/archival_objects/1",
    }

    minted = distillery.mint_ark_for_archival_object(archival_object)

    assert minted == "ark:99999/abc123x"
    assert (
        archival_object["external_ark_url"]
        == "https://resolver.example.org/ark:99999/abc123x"
    )
    assert len(mint_calls) == 1
    assert mint_calls[0]["bucket_name"] == "resolver-bucket"
    assert mint_calls[0]["redirect_target"] == (
        "https://archivesspace.example.org"
        "/resolve/readonly?uri=/repositories/2/archival_objects/1"
    )
    assert mint_calls[0]["nma"] == "resolver.example.org"
    assert mint_calls[0]["naan"] == "99999"
    assert mint_calls[0]["title"] == "Test AO"
    assert mint_calls[0]["s3_client"] is not None
