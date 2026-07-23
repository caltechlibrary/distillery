# file: distillery.py
# DIGITAL OBJECT PRESERVATION WORKFLOW

import argparse
import datetime
import hashlib
import json
import logging
import os
import shutil
import tempfile

from pathlib import Path
from decouple import Config, Csv, RepositoryIni  # pypi: python-decouple

import cascflow

parser = argparse.ArgumentParser(
    description="Digital object preservation workflow for Caltech Archives."
)
parser.add_argument(
    "--settings",
    default=None,
    help="Path to settings.ini file (optional). Defaults to working directory.",
)
subparsers = parser.add_subparsers(dest="command", required=True)

# subcommand: validate
validate_parser = subparsers.add_parser(
    "validate", help="Validate connections, files, and data."
)
validate_parser.add_argument(
    "--context", required=True, help="String. Supply a volume name."
)

# subcommand: execute
execute_parser = subparsers.add_parser(
    "execute", help="Execute the preservation process."
)
execute_parser.add_argument(
    "--context", required=True, help="String. Supply a volume name."
)

args = None
config = cascflow.config
logger = logging.getLogger("distillery")

DISTILLERY_SETTINGS_SPEC = [
    {"key": "ARCHIVESSPACE_API_URL", "kind": "url"},
    {"key": "ARCHIVESSPACE_USERNAME", "kind": "string"},
    {"key": "ARCHIVESSPACE_PASSWORD", "kind": "string"},
    {"key": "ARCHIVESSPACE_STAFF_URL", "kind": "url"},
    {"key": "ARCHIVESSPACE_REPOSITORY_ID", "kind": "int", "required": False},
    {"key": "AWS_ACCESS_KEY_ID", "kind": "string"},
    {"key": "AWS_SECRET_ACCESS_KEY", "kind": "string"},
    {"key": "RESOLVER_BUCKET", "kind": "string"},
    {"key": "OUR_NMA", "kind": "url"},
    {"key": "OUR_NAAN", "kind": "string"},
    {"key": "OUR_SHOULDER", "kind": "string", "required": False},
    {"key": "ARK_BLADE_LENGTH", "kind": "int", "required": False},
    {"key": "ABSOLUTE_MOUNT_PARENT", "kind": "directory"},
    {"key": "RELATIVE_SOURCE_DIRECTORY", "kind": "string"},
    {"key": "RELATIVE_BATCH_DIRECTORY", "kind": "string"},
    {"key": "FILES_TO_REMOVE", "kind": "csv", "required": False},
    {"key": "PRESERVATION_MOUNT_PARENT", "kind": "directory"},
    {"key": "RELATIVE_OCFL_STORAGE_DIRECTORY", "kind": "string"},
    {"key": "RELATIVE_OCFL_STAGING_DIRECTORY", "kind": "string"},
]

# OCFL Extension 0007 (N Tuple Omit Prefix Storage Layout) configuration.
# Fixed architectural choices, not per-environment settings -- see
# OCFL-PRESERVATION-PLAN.md §3. OCFL_VERSION_PADDING in particular must
# never change for an object once its first version exists.
# OCFL_NUMBER_OF_TUPLES must stay >= 2 -- the inventory-mirroring Lambda's
# version-copy detection (cdk/functions/mirror_inventory_to_resolver.py,
# assigned_name_for_key()) relies on this to distinguish a version copy
# from the current inventory.json; enforced in
# validate_settings_for_preservation() below.
OCFL_DELIMITER = "/"
OCFL_TUPLE_SIZE = 3
OCFL_NUMBER_OF_TUPLES = 2
OCFL_VERSION_PADDING = 4
OCFL_DIGEST_ALGORITHM = "sha256"


def mint_ark_for_archival_object(archival_object: dict) -> str | None:
    import arkminter

    nma = config("OUR_NMA", default="").strip()
    naan = config("OUR_NAAN", default="").strip()
    if not nma or not naan:
        logger.warning(
            "⚠️ OUR_NMA OR OUR_NAAN NOT CONFIGURED; SKIPPING ARK MINTING: %s",
            archival_object.get("component_id", ""),
        )
        return None

    # Validate existing ARK (returns None if doesn't exist or is invalid)
    existing_ark = arkminter.get_existing_our_ark(archival_object, naan)
    if existing_ark:
        logger.info(
            "⊘ SKIPPING ARK MINTING (EXISTING ARK FOUND): %s → %s",
            archival_object.get("component_id", ""),
            existing_ark,
        )
        return None

    s3_client = cascflow.establish_s3_connection()
    if s3_client is None:
        raise RuntimeError("Failed to establish S3 connection")

    redirect_location = (
        config("ARCHIVESSPACE_STAFF_URL").rstrip("/")
        + f"/resolve/readonly?uri={archival_object['uri']}"
    )

    # Mint stub ARK with minimal metadata (title only)
    minted_ark = arkminter.mint_ark(
        bucket_name=config("RESOLVER_BUCKET"),
        redirect_target=redirect_location,
        nma=nma,
        naan=naan,
        title=str(archival_object.get("title", "")),
        shoulder=config("OUR_SHOULDER", default=""),
        blade_length=config("ARK_BLADE_LENGTH", default=6, cast=int),
        s3_client=s3_client,
    )

    # Set external_ark_url on archival_object
    archival_object["external_ark_url"] = f"https://{nma}/{minted_ark}"

    logger.info(f"✳️ ARK MINTED: {minted_ark}")
    return minted_ark


def ensure_digital_object(archival_object):
    """Ensure a digital object exists for the archival object."""
    digital_object_count = len(
        [i for i in archival_object["instances"] if "digital_object" in i.keys()]
    )
    logger.debug(f"🐞 DIGITAL OBJECT COUNT: {digital_object_count}")
    if digital_object_count > 1:
        raise ValueError(
            "❌ MULTIPLE DIGITAL OBJECTS FOUND: {}".format(
                archival_object["component_id"]
            )
        )
    elif digital_object_count < 1:
        digital_object_uri, archival_object = cascflow.create_digital_object(
            archival_object
        )
    else:
        digital_object_uri = archival_object["instances"][0]["digital_object"]["ref"]

    # returns new archival_object with digital_object instance included
    return digital_object_uri, archival_object


def chunk_hashes(path: Path, algorithms=("sha256", "md5")) -> dict:
    """Compute one or more hex digests for a file in a single read pass."""
    hashers = {algorithm: hashlib.new(algorithm) for algorithm in algorithms}
    with open(path, "rb") as f:
        # read the file in 8MB chunks to avoid memory issues with large files
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            for hasher in hashers.values():
                hasher.update(chunk)
    return {algorithm: hasher.hexdigest() for algorithm, hasher in hashers.items()}


def ocfl_storage_root() -> Path:
    """Return the OCFL storage root -- what the sync tool watches to mirror to
    S3 (see OCFL-PRESERVATION-PLAN.md §2)."""
    return Path(config("PRESERVATION_MOUNT_PARENT")).joinpath(
        config("RELATIVE_OCFL_STORAGE_DIRECTORY")
    )


def ocfl_staging_root() -> Path:
    """Return the staging directory used to build OCFL content before an
    atomic rename into ocfl_storage_root() -- a sibling on the same
    filesystem (required for os.rename() to be atomic), not watched by
    the sync tool."""
    return Path(config("PRESERVATION_MOUNT_PARENT")).joinpath(
        config("RELATIVE_OCFL_STAGING_DIRECTORY")
    )


def ocfl_object_root(object_id: str) -> Path:
    """Return the local OCFL storage path for an OCFL object id."""
    return ocfl_storage_root().joinpath(
        cascflow.ocfl.object_root_path(
            object_id,
            delimiter=OCFL_DELIMITER,
            tuple_size=OCFL_TUPLE_SIZE,
            number_of_tuples=OCFL_NUMBER_OF_TUPLES,
        )
    )


def load_ocfl_inventory(object_root: Path, object_id: str) -> dict:
    """Load an object's current inventory.json, or start a fresh shell."""
    inventory_path = object_root.joinpath("inventory.json")
    if inventory_path.exists():
        return json.loads(inventory_path.read_text())
    return cascflow.ocfl.build_inventory(
        object_id, digest_algorithm=OCFL_DIGEST_ALGORITHM
    )


def write_ocfl_inventory_files(directory: Path, inventory: dict) -> None:
    """Write inventory.json and its digest sidecar into `directory`."""
    inventory_bytes = json.dumps(inventory, indent=2).encode()
    inventory_digest = hashlib.new(
        inventory["digestAlgorithm"], inventory_bytes
    ).hexdigest()
    sidecar_name = f"inventory.json.{inventory['digestAlgorithm']}"
    directory.joinpath("inventory.json").write_bytes(inventory_bytes)
    directory.joinpath(sidecar_name).write_text(f"{inventory_digest} inventory.json\n")


def write_current_ocfl_inventory(
    object_root: Path, inventory: dict, staging_root: Path
) -> None:
    """Atomically replace the object root's current inventory.json (+
    sidecar), and ensure the OCFL object declaration file exists.

    Stages the new inventory in a temporary directory under
    `staging_root` (same filesystem as `object_root`) and os.rename()s
    each file into place, so nothing watching the storage root ever sees
    a partially-written file (see OCFL-PRESERVATION-PLAN.md §2).
    """
    staging_dir = Path(tempfile.mkdtemp(dir=staging_root))
    try:
        write_ocfl_inventory_files(staging_dir, inventory)
        sidecar_name = f"inventory.json.{inventory['digestAlgorithm']}"
        object_root.mkdir(parents=True, exist_ok=True)
        os.rename(
            staging_dir.joinpath("inventory.json"),
            object_root.joinpath("inventory.json"),
        )
        os.rename(
            staging_dir.joinpath(sidecar_name), object_root.joinpath(sidecar_name)
        )
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)

    declaration_path = object_root.joinpath("0=ocfl_object_1.1")
    if not declaration_path.exists():
        declaration_path.write_text("ocfl_object_1.1\n")


def build_metadata_filename(archival_object: dict) -> str:
    """Return the OCFL logical path for archival_object's metadata snapshot,
    namespaced by its Component Unique Identifier so it can never collide
    across archival objects sharing the same OCFL storage root."""
    return f"_metadata-{archival_object['component_id']}.json"


def build_version_files(archival_object: dict, filepaths: list) -> tuple:
    """Return (files, fixity, source_by_logical_path, archival_object_bytes)
    describing one new OCFL version for archival_object's preservation
    files plus a metadata snapshot (see build_metadata_filename()).

    `files` maps logical path -> sha256 digest (the OCFL manifest digest).
    `fixity` maps "md5" -> {logical path: md5 digest} (see
    OCFL-PRESERVATION-PLAN.md §2, "Digest algorithm"). `filepaths` always
    come from a single directory listing (cascflow.execute()), so two
    entries sharing a filename can't occur -- filesystems already guarantee
    that (see §4).
    """
    files = {}
    fixity_md5 = {}
    source_by_logical_path = {}

    for filepath in filepaths:
        digests = chunk_hashes(filepath)
        files[filepath.name] = digests["sha256"]
        fixity_md5[filepath.name] = digests["md5"]
        source_by_logical_path[filepath.name] = filepath

    archival_object_bytes = json.dumps(archival_object, indent=4).encode()
    files[build_metadata_filename(archival_object)] = hashlib.sha256(
        archival_object_bytes
    ).hexdigest()

    return files, {"md5": fixity_md5}, source_by_logical_path, archival_object_bytes


def ark_base_compact_name(external_ark_url: str) -> str:
    """Return the ARK Base Compact Name (e.g. "ark:99999/b3xq7z") from a
    full resolvable ARK URL (e.g. "https://n2t.net/ark:99999/b3xq7z")."""
    return "ark:" + external_ark_url.split("ark:", 1)[1]


def resolve_ocfl_user() -> tuple:
    """Resolve the (name, address) to attribute a new OCFL version to.

    GITHUB_ACTOR is always present as a default environment variable when
    running under GitHub Actions -- used directly as both the triggering
    user's name and the base of their GitHub profile URL. Falls back to
    identifying the pipeline itself when run from the command line.
    """
    actor = os.environ.get("GITHUB_ACTOR", "").strip()
    if actor:
        return actor, f"https://github.com/{actor}"
    return "distillery", "https://github.com/caltechlibrary/distillery"


def write_ocfl_version(archival_object: dict, filepaths: list) -> dict:
    """Add a new OCFL version for archival_object's preservation files plus
    a metadata snapshot; write it under the OCFL storage root.

    Builds the new version's content in a temporary staging directory
    (ocfl_staging_root()), then os.rename()s the whole thing into place in
    one atomic step -- anything watching the storage root (the sync tool)
    only ever sees a complete version directory appear, never a partial
    one (see OCFL-PRESERVATION-PLAN.md §2).

    Returns the updated inventory dict (already written to disk).
    """
    object_id = ark_base_compact_name(archival_object["external_ark_url"])
    object_root = ocfl_object_root(object_id)
    inventory = load_ocfl_inventory(object_root, object_id)
    existing_digests = set(inventory["manifest"].keys())

    files, fixity, source_by_logical_path, archival_object_bytes = build_version_files(
        archival_object, filepaths
    )
    metadata_filename = build_metadata_filename(archival_object)
    user_name, user_address = resolve_ocfl_user()

    inventory = cascflow.ocfl.add_version(
        inventory,
        files=files,
        created=datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        user_name=user_name,
        user_address=user_address,
        version_padding=OCFL_VERSION_PADDING,
        fixity=fixity,
    )
    version_id = inventory["head"]

    staging_root = ocfl_staging_root()
    staging_root.mkdir(parents=True, exist_ok=True)
    staging_dir = Path(tempfile.mkdtemp(dir=staging_root))
    try:
        content_dir = staging_dir.joinpath("content")
        for logical_path, digest in files.items():
            if digest in existing_digests:
                continue
            destination = content_dir.joinpath(logical_path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if logical_path == metadata_filename:
                destination.write_bytes(archival_object_bytes)
            else:
                shutil.copy2(source_by_logical_path[logical_path], destination)
        write_ocfl_inventory_files(staging_dir, inventory)

        object_root.mkdir(parents=True, exist_ok=True)
        os.rename(staging_dir, object_root.joinpath(version_id))
    except BaseException:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    write_current_ocfl_inventory(object_root, inventory, staging_root)
    logger.info(f"☑️  OCFL VERSION WRITTEN: {object_root}/{version_id}")
    return inventory


def construct_inventory_file_version(archival_object: dict) -> dict:
    """Build the ArchivesSpace file_version pointing at this object's
    inventory.json, once mirrored to the public resolver bucket by the
    Lambda described in OCFL-PRESERVATION-PLAN.md §6 Phase 0. There's an
    inherent lag between this record existing and that URL resolving --
    acceptable since it's unpublished/staff-only (§5)."""
    return {
        "file_uri": f"{archival_object['external_ark_url']}/inventory.json",
        "publish": False,
    }


def build_inventory_manifest_note(inventory: dict) -> dict:
    """Build the unpublished ArchivesSpace note summarizing an object's
    current OCFL inventory contents (see OCFL-PRESERVATION-PLAN.md §5)."""
    head = inventory["head"]
    state = inventory["versions"][head]["state"]
    lines = [f"OCFL object inventory ({head}):"]
    for digest, logical_paths in sorted(state.items()):
        for logical_path in sorted(logical_paths):
            lines.append(f"{logical_path} ({inventory['digestAlgorithm']}: {digest})")
    return {
        "jsonmodel_type": "note_digital_object",
        "type": "note",
        "label": "OCFL Inventory",
        "content": ["\n".join(lines)],
        "publish": False,
    }


def update_inventory_manifest_note(digital_object: dict, inventory: dict) -> dict:
    """Replace (or add) the digital_object's OCFL inventory manifest note.

    Regenerated/overwritten on every update, not appended (§2)."""
    note = build_inventory_manifest_note(inventory)
    notes = [
        n
        for n in digital_object.get("notes", [])
        if not (
            n.get("jsonmodel_type") == "note_digital_object"
            and n.get("label") == "OCFL Inventory"
        )
    ]
    notes.append(note)
    digital_object["notes"] = notes
    return digital_object


def process_archival_object_preservation_files(
    archival_object: dict, digital_object_uri: str, filepaths: list
) -> None:
    """Write a new OCFL version for archival_object's preservation files,
    then update ArchivesSpace: a single file_version pointing at the
    object's (eventual) public inventory.json URL, plus a regenerated
    unpublished manifest note (see OCFL-PRESERVATION-PLAN.md §5)."""
    inventory = write_ocfl_version(archival_object, filepaths)

    file_version = construct_inventory_file_version(archival_object)
    cascflow.save_digital_object_file_versions(archival_object, [file_version])

    # re-fetch fresh so the note update POSTs a current lock_version, since
    # save_digital_object_file_versions already POSTed the digital_object once
    digital_object = cascflow.archivesspace_get(digital_object_uri).json()
    digital_object = update_inventory_manifest_note(digital_object, inventory)
    cascflow.archivesspace_post(digital_object_uri, digital_object)
    logger.info(f"☑️  DIGITAL OBJECT NOTE UPDATED: {digital_object_uri}")


def validate_settings_for_preservation(settings_validation_results: dict) -> None:
    missing = settings_validation_results.get("missing", [])
    invalid = settings_validation_results.get("invalid", [])
    if missing:
        message = "❌ REQUIRED SETTINGS NOT SET"
        logger.error(message)
        for setting in missing:
            logger.error(f"❌ {setting['key']}: {setting['detail']}")
    if invalid:
        message = "❌ INVALID SETTINGS VALUES"
        logger.error(message)
        for setting in invalid:
            logger.error(f"❌ {setting['key']}: {setting['detail']}")
    if missing or invalid:
        raise RuntimeError("❌ SETTINGS VALIDATION FAILED")

    if OCFL_NUMBER_OF_TUPLES < 2:
        message = (
            "❌ OCFL_NUMBER_OF_TUPLES MUST BE >= 2 -- the inventory-mirroring "
            "Lambda's version-copy detection (cdk/functions/"
            "mirror_inventory_to_resolver.py) can't distinguish a version "
            "copy from the current inventory.json when an assigned name "
            "happens to spell a version directory name."
        )
        logger.error(message)
        raise RuntimeError(message)

    logger.debug("✅ ALL REQUIRED SETTINGS ARE SET AND USABLE")


def validate_digital_files_for_preservation(validation_results: dict) -> None:
    # Distillery writes a _metadata-{component_id}.json snapshot into each
    # OCFL version (OCFL-PRESERVATION-PLAN.md §4); a source file with that
    # exact name would silently collide with it. Relies on each top-level
    # source entry's own name (entry.stem) being byte-identical to its
    # component_id -- true today (both come from the same ArchivesSpace
    # lookup), but nothing enforces it explicitly.
    def validate_no_reserved_filename_conflicts(source_path: Path) -> None:
        conflicts = sorted(
            str(filepath)
            for entry in source_path.iterdir()
            if entry.is_dir()
            for filepath in entry.rglob(f"_metadata-{entry.stem}.json")
        )
        if conflicts:
            message = "❌ RESERVED FILENAME '_metadata-*.json' FOUND IN SOURCE FILES"
            logger.error(message)
            for conflict in conflicts:
                logger.error(f"❌ {conflict}")
            raise RuntimeError(message)

    # Return total bytes for one top-level archival-object entry.
    def get_entry_size_bytes(entry: Path) -> int:
        if entry.is_file():
            return entry.stat().st_size
        if entry.is_dir():
            return sum(
                filepath.stat().st_size
                for filepath in entry.rglob("*")
                if filepath.is_file()
            )
        return 0

    # Compare each top-level entry size against free bytes on the temp directory mount,
    # since that is where the build_directory will be created during execute.
    def validate_available_storage_for_archival_objects(source_path: Path) -> None:
        # Warn/fail thresholds based on ratio-to-free-space and projected free-space floors.
        warning_threshold_ratio = 0.80
        fail_threshold_ratio = 0.90
        staging_overhead_ratio = 1.05
        warning_free_floor_bytes = 20 * (1024**3)
        fail_free_floor_bytes = 5 * (1024**3)
        preservation_mount = config("PRESERVATION_MOUNT_PARENT")
        free_bytes = shutil.disk_usage(preservation_mount).free
        logger.info(
            f"💽 AVAILABLE DISK SPACE ON PRESERVATION_MOUNT_PARENT ({preservation_mount}): {free_bytes / (1024**3):.2f} GiB"
        )

        oversized_entries = []
        near_capacity_entries = []

        for entry in sorted(source_path.iterdir(), key=lambda p: p.name):
            if not entry.exists():
                continue
            entry_size_bytes = get_entry_size_bytes(entry)
            if entry_size_bytes == 0:
                continue
            usage_ratio = (
                entry_size_bytes / free_bytes if free_bytes > 0 else float("inf")
            )
            # Account for staging/log overhead when estimating post-copy free space.
            projected_bytes_needed = int(entry_size_bytes * staging_overhead_ratio)
            projected_free_bytes = free_bytes - projected_bytes_needed
            if (
                usage_ratio >= fail_threshold_ratio
                or projected_free_bytes < fail_free_floor_bytes
            ):
                oversized_entries.append(
                    (entry.name, entry_size_bytes, usage_ratio, projected_free_bytes)
                )
            elif (
                usage_ratio >= warning_threshold_ratio
                or projected_free_bytes < warning_free_floor_bytes
            ):
                near_capacity_entries.append(
                    (entry.name, entry_size_bytes, usage_ratio, projected_free_bytes)
                )

        for (
            entry_name,
            entry_size_bytes,
            usage_ratio,
            projected_free_bytes,
        ) in near_capacity_entries:
            logger.warning(
                "⚠️ ARCHIVAL OBJECT APPROACHES AVAILABLE SPACE: "
                f"{entry_name} ({entry_size_bytes / (1024**3):.2f} GiB, "
                f"{usage_ratio * 100:.1f}% of free space, "
                f"projected free: {projected_free_bytes / (1024**3):.2f} GiB)"
            )

        if oversized_entries:
            message = "❌ INSUFFICIENT LOCAL SPACE FOR ONE OR MORE ARCHIVAL OBJECTS"
            logger.error(message)
            for (
                entry_name,
                entry_size_bytes,
                usage_ratio,
                projected_free_bytes,
            ) in oversized_entries:
                logger.error(
                    f"❌ {entry_name} requires {entry_size_bytes / (1024**3):.2f} GiB "
                    f"({usage_ratio * 100:.1f}% of free space, "
                    f"projected free: {projected_free_bytes / (1024**3):.2f} GiB)"
                )
            raise RuntimeError(message)

    eligible_archival_objects = validation_results.get("eligible_archival_objects", {})
    ineligible_archival_objects = validation_results.get(
        "ineligible_archival_objects", []
    )
    nested_directories = validation_results.get("nested_directories", [])
    empty_directories = validation_results.get("empty_directories", [])
    file_count = validation_results.get("file_count", 0)
    source_path = validation_results.get("source_path")
    ## messaging
    if eligible_archival_objects:
        logger.info(
            f"{next(iter(eligible_archival_objects))}{', …' if len(eligible_archival_objects) > 1 else ''}"
        )
        for component_id in eligible_archival_objects.keys():
            logger.info(f"🉑 {component_id}")
        logger.info(f"🗂 ARCHIVAL OBJECT COUNT: {len(eligible_archival_objects)}")
        logger.info(f"📄 FILE COUNT: {file_count}")
    ## validation and messaging
    if file_count == 0:
        message = "❌ NO FILES FOUND"
        logger.error(message)
        raise FileNotFoundError(message)
    if ineligible_archival_objects:
        message = "❌ INELIGIBLE ARCHIVAL OBJECTS FOUND"
        logger.error(message)
        for ineligible_archival_object in ineligible_archival_objects:
            logger.error(f"❌ {ineligible_archival_object['detail']}")
        raise RuntimeError(message)
    if nested_directories:
        message = "❌ NESTED DIRECTORIES FOUND"
        logger.error(message)
        for nested_directory in nested_directories:
            logger.error(f"❌ {nested_directory}")
        raise RuntimeError(message)
    if empty_directories:
        message = "❌ EMPTY DIRECTORIES FOUND"
        logger.error(message)
        for empty_directory in empty_directories:
            logger.error(f"❌ {empty_directory}")
        raise RuntimeError(message)
    if isinstance(source_path, Path):
        validate_no_reserved_filename_conflicts(source_path)
        validate_available_storage_for_archival_objects(source_path)


def main():
    global args, config, logger
    args = parser.parse_args()
    # set up settings path and logging
    if args.settings is not None:
        settings_path = Path(args.settings).expanduser().resolve()
    else:
        settings_path = Path(__file__).parent.joinpath("settings.ini").resolve()
    if not settings_path.exists():
        raise FileNotFoundError(f"Settings file not found: {settings_path}")
    # configure cascflow to use our settings.ini file
    config_instance = Config(RepositoryIni(settings_path.as_posix()))
    cascflow.set_config(config_instance)
    # reassign module-level config to use the cascflow config instance with our settings
    config = cascflow.config

    cascflow.setup_logging(str(settings_path))
    logger = logging.getLogger("distillery")
    # handle the commands
    if args.command == "validate":
        settings_validation_results = cascflow.validate_settings(
            DISTILLERY_SETTINGS_SPEC
        )
        validate_settings_for_preservation(settings_validation_results)
        validation_results = cascflow.validate_digital_files(args.context)
        validate_digital_files_for_preservation(validation_results)
    elif args.command == "execute":
        batch_set_id = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
        for (
            batch_directory,
            stage_2_path_obj,
            filepaths,
            archival_object,
            arrangement,
        ) in cascflow.execute(args.context, batch_set_id, "preservation"):
            # NOTE returned archival_object will include any created digital_object instance
            digital_object_uri, archival_object = ensure_digital_object(archival_object)
            # Mint ARK (or skip if already exists); this also sets external_ark_url on archival_object
            mint_ark_for_archival_object(archival_object)
            # POST archival_object back to ArchivesSpace to persist external_ark_url if it was set
            if archival_object.get("external_ark_url"):
                cascflow.archivesspace_post(archival_object["uri"], archival_object)
                logger.debug(
                    f"☑️  ARCHIVAL OBJECT UPDATED WITH external_ark_url: {archival_object['uri']}"
                )
            files_to_remove = config("FILES_TO_REMOVE", default="", cast=Csv())
            if not isinstance(files_to_remove, list):
                files_to_remove = []
            preservation_filepaths = sorted(
                (
                    filepath
                    for filepath in filepaths
                    if filepath.name not in files_to_remove
                ),
                key=lambda filepath: filepath.name,
            )
            logger.debug(f"🐞 PRESERVATION FILEPATHS: {preservation_filepaths}")
            if preservation_filepaths:
                process_archival_object_preservation_files(
                    archival_object, digital_object_uri, preservation_filepaths
                )
            cascflow.move_to_stage_3(stage_2_path_obj, batch_directory)
    else:
        parser.print_help()


if __name__ == "__main__":
    try:
        main()
    except Exception as exception:
        logger.error(f"❌ UNEXPECTED ERROR: {exception}")
        raise
