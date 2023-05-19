# file: alchemist.py
# RENDER AND PUBLISH ACCESS PAGES AND ASSETS

import json
import logging
import os
import subprocess
import tempfile

from pathlib import Path

import boto3
import botocore
import jinja2  # pypi: Jinja2
import sh

from decouple import config

import distillery

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    Path(Path(__file__).resolve().parent).joinpath("settings.ini"),
    disable_existing_loggers=False,
)
logger = logging.getLogger(__name__)

s3_client = boto3.client(
    "s3",
    aws_access_key_id=config("DISTILLERY_AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=config("DISTILLERY_AWS_SECRET_ACCESS_KEY"),
)


class AccessPlatform:
    def __init__(self, collection_id, collection_data):
        self.collection_id = collection_id
        self.collection_data = collection_data
        self.build_directory = tempfile.TemporaryDirectory()

    def collection_structure_processing(self):
        # TODO build html metadata/thumbnail page?
        logger.info(f"🐛 COLLECTION STRUCTURE PROCESSING: {self.collection_id}")

    def archival_object_level_processing(self, variables):
        logger.info(f"🐛 ARCHIVAL OBJECT LEVEL PROCESSING: {self.collection_id}")
        logger.info(f"🐛 variables.keys(): {variables.keys()}")
        generate_archival_object_page(self.build_directory, variables)
        upload_archival_object_page(self.build_directory, variables)
        generate_iiif_manifest(self.build_directory, variables)
        upload_iiif_manifest(self.build_directory, variables)

    def create_access_file(self, variables):
        # TODO adapt for different file types
        # TODO create the Pyramid TIFF for iiif-serverless
        logger.info(f"🐛 CREATE ACCESS FILE: {self.collection_id}")
        logger.info(f"🐛 variables.keys(): {variables.keys()}")
        logger.info(
            f"🐛 variables['original_image_path']: {variables['original_image_path']}"
        )
        create_pyramid_tiff(self.build_directory, variables)
        return

    def transfer_derivative_files(self, variables):
        logger.info(f"🐛 TRANSFER DERIVATIVE FILES: {self.collection_id}")
        logger.info(f"🐛 variables.keys(): {variables.keys()}")
        publish_access_files(self.build_directory, variables)

    def ingest_derivative_files(self, variables):
        logger.info(f"🐛 INGEST DERIVATIVE FILES: {self.collection_id}")
        logger.info(f"🐛 variables.keys(): {variables.keys()}")

    def loop_over_derivative_structure(self, variables):
        logger.info(f"🐛 LOOP OVER DERIVATIVE STRUCTURE: {self.collection_id}")
        logger.info(f"🐛 variables.keys(): {variables.keys()}")
        create_digital_object_file_versions(self.build_directory, variables)


def validate_connection():
    try:
        response = s3_client.put_object(
            Bucket=config("ACCESS_BUCKET"), Key=".distillery"
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            logger.info(f'☁️  S3 BUCKET WRITABLE: {config("ACCESS_BUCKET")}')
            return True
        else:
            logger.error(f'❌ S3 BUCKET NOT WRITABLE: {config("ACCESS_BUCKET")}')
            logger.error(f"❌ S3 BUCKET RESPONSE: {response}")
            return False
    except botocore.exceptions.ClientError as error:
        logger.error(f"❌ S3 ERROR: {error.response}")
        return False


def generate_archival_object_page(build_directory, variables):
    try:
        logger.info(
            f'🐛 GENERATE ARCHIVAL OBJECT PAGE: {variables["archival_object"]["component_id"]}'
        )
        # logger.info(f"🐛 VARIABLES.keys(): {variables.keys()}")
        # dict_keys(['filepaths', 'archival_object', 'folder_arrangement'])
        # logger.info(f'🐛 variables["archival_object"].keys(): {variables["archival_object"].keys()}')
        # dict_keys(['lock_version', 'position', 'publish', 'ref_id',
        # 'component_id', 'title', 'display_string', 'restrictions_apply',
        # 'created_by', 'last_modified_by', 'create_time', 'system_mtime',
        # 'user_mtime', 'suppressed', 'is_slug_auto', 'level', 'jsonmodel_type',
        # 'external_ids', 'subjects', 'linked_events', 'extents', 'lang_materials',
        # 'dates', 'external_documents', 'rights_statements', 'linked_agents',
        # 'import_previous_arks', 'ancestors', 'instances', 'notes',
        # 'accession_links', 'uri', 'repository', 'resource', 'parent',
        # 'has_unpublished_ancestor'])
        logger.info(
            f'🐛 variables["archival_object"]["dates"]: {variables["archival_object"]["dates"]}'
        )
        logger.info(
            f'🐛 variables["archival_object"]["notes"]: {variables["archival_object"]["notes"]}'
        )
        logger.info(
            f'🐛 variables["archival_object"]["uri"]: {variables["archival_object"]["uri"]}'
        )
        environment = jinja2.Environment(
            loader=jinja2.FileSystemLoader(f"{os.path.dirname(__file__)}/templates")
        )
        template = environment.get_template("alchemist/archival_object.tpl")
        logger.info(f"🐛 TEMPLATE: {template}")
        logger.info(f"🐛 BUILD DIRECTORY: {build_directory.name}")
        collection_directory = Path(build_directory.name).joinpath(
            variables["folder_arrangement"]["collection_id"]
        )
        logger.info(f"🐛 COLLECTION DIRECTORY: {collection_directory}")
        iiif_manifest_url = "/".join(
            [
                config("ACCESS_SITE_BASE_URL").rstrip("/"),
                variables["folder_arrangement"]["collection_id"],
                variables["archival_object"]["component_id"],
                "manifest.json",
            ]
        )
        abstract_notes = []
        for note in variables["archival_object"]["notes"]:
            if note["type"] == "abstract" and note["publish"]:
                for content in note["content"]:
                    abstract_notes.append(content["content"])
        scopecontent_notes = []
        for note in variables["archival_object"]["notes"]:
            if note["type"] == "scopecontent" and note["publish"]:
                for subnote in note["subnotes"]:
                    if subnote["jsonmodel_type"] == "note_text" and subnote["publish"]:
                        scopecontent_notes.append(subnote["content"])
        archival_object_page_key = (
            Path(variables["folder_arrangement"]["collection_id"])
            .joinpath(f'{variables["archival_object"]["component_id"]}', "index.html")
            .as_posix()
        )
        archival_object_page_file = (
            Path(build_directory.name)
            .joinpath(
                archival_object_page_key,
            )
            .as_posix()
        )
        Path(archival_object_page_file).parent.mkdir(parents=True, exist_ok=True)
        with open(
            archival_object_page_file,
            "w",
        ) as f:
            # supply data to template placeholders
            f.write(
                template.render(
                    title=variables["archival_object"]["title"],
                    collection=variables["folder_arrangement"].get(
                        "collection_display"
                    ),
                    series=variables["folder_arrangement"].get("series_display"),
                    subseries=variables["folder_arrangement"].get("subseries_display"),
                    dates=variables["archival_object"].get("dates", []),
                    abstract_notes=abstract_notes,
                    scopecontent_notes=scopecontent_notes,
                    archivesspace_url="/".join(
                        [
                            config("ASPACE_PUBLIC_URL").rstrip("/"),
                            variables["archival_object"]["uri"],
                        ]
                    ),
                    iiif_manifest_url=iiif_manifest_url,
                    iiif_manifest_json=json.dumps({"manifest": f"{iiif_manifest_url}"}),
                )
            )
        logger.info(
            f"🐛 ARCHIVAL OBJECT PAGE FILE GENERATED: {archival_object_page_file}"
        )
    except Exception as e:
        import traceback

        logger.error(f"❌ EXCEPTION: {e}")
        logger.error(f"❌ TRACEBACK: {traceback.format_exc()}")
        raise


def upload_archival_object_page(build_directory, variables):
    try:
        # TODO generalize with upload_iiif_manifest
        logger.info(
            f'🐛 UPLOAD ARCHIVAL OBJECT PAGE: {variables["folder_arrangement"]["collection_id"]}/{variables["archival_object"]["component_id"]}/index.html'
        )
        logger.info(f"🐛 BUILD DIRECTORY: {build_directory.name}")
        archival_object_page_key = (
            Path(variables["folder_arrangement"]["collection_id"])
            .joinpath(f'{variables["archival_object"]["component_id"]}', "index.html")
            .as_posix()
        )
        archival_object_page_file = (
            Path(build_directory.name)
            .joinpath(
                archival_object_page_key,
            )
            .as_posix()
        )
        logger.info(
            f"🐛 ARCHIVAL OBJECT PAGE FILE EXISTS: {Path(archival_object_page_file).exists()}"
        )
        # TODO add config("ACCESS_BUCKET") to variables for fewer calls to decouple
        try:
            response = s3_client.upload_file(
                archival_object_page_file,
                config("ACCESS_BUCKET"),
                archival_object_page_key,
                ExtraArgs={"ContentType": "text/html"},
            )
            logger.info(f"🐛 RESPONSE: {response}")
        except Exception as e:
            logger.error(f"❌ EXCEPTION: {str(e)}")
    except Exception as e:
        import traceback

        logger.error(f"❌ EXCEPTION: {e}")
        logger.error(f"❌ TRACEBACK: {traceback.format_exc()}")
        raise


def get_thumbnail_url(variables):
    thumbnail_file = Path(sorted(variables["filepaths"])[0])
    thumbnail_id = "%2F".join(
        [
            thumbnail_file.parent.parent.name,
            thumbnail_file.parent.name,
            thumbnail_file.stem,
        ]
    )
    return "/".join(
        [
            config("ACCESS_IIIF_ENDPOINT").rstrip("/"),
            thumbnail_id,
            "full",
            "200,",
            "0",
            "default.jpg",
        ]
    )


def generate_iiif_manifest(build_directory, variables):
    try:
        manifest = {
            "@context": "http://iiif.io/api/presentation/2/context.json",
            "@type": "sc:Manifest",
            "@id": "/".join(
                [
                    config("ACCESS_SITE_BASE_URL").strip("/"),
                    variables["folder_arrangement"]["collection_id"],
                    variables["archival_object"]["component_id"],
                    "/manifest.json",
                ]
            ),
            "label": variables["archival_object"]["display_string"],
            "thumbnail": {
                "@id": get_thumbnail_url(variables),
                "service": {
                    "@context": "http://iiif.io/api/image/2/context.json",
                    "@id": get_thumbnail_url(variables).rsplit("/", maxsplit=4)[0],
                    "profile": "http://iiif.io/api/image/2/level1.json",
                },
            },
            "sequences": [{"@type": "sc:Sequence", "canvases": []}],
        }
        for filepath in sorted(variables["filepaths"]):
            # create canvas metadata
            # HACK the binaries for `vips` and `vipsheader` should be in the same place
            width = (
                os.popen(f'{config("WORK_VIPS_CMD")}header -f width {filepath}')
                .read()
                .strip()
            )
            height = (
                os.popen(f'{config("WORK_VIPS_CMD")}header -f height {filepath}')
                .read()
                .strip()
            )
            canvas_id = "/".join(
                [
                    config("ACCESS_SITE_BASE_URL").strip("/"),
                    variables["folder_arrangement"]["collection_id"],
                    variables["archival_object"]["component_id"],
                    "canvas",
                    f'{Path(filepath).stem}',
                ]
            )
            escaped_identifier = "/".join(
                [
                    variables["folder_arrangement"]["collection_id"],
                    variables["archival_object"]["component_id"],
                    f'{Path(filepath).stem}',
                ]
            )
            service_id = (
                config("ACCESS_IIIF_ENDPOINT").strip("/") + "/" + escaped_identifier
            )
            resource_id = service_id + "/full/max/0/default.jpg"
            canvas = {
                "@type": "sc:Canvas",
                "@id": canvas_id,
                "label": Path(filepath).stem.split("_")[-1].lstrip("0"),
                "width": width,
                "height": height,
                "images": [
                    {
                        "@type": "oa:Annotation",
                        "motivation": "sc:painting",
                        "on": canvas_id,
                        "resource": {
                            "@type": "dctypes:Image",
                            "@id": resource_id,
                            "service": {
                                "@context": "http://iiif.io/api/image/2/context.json",
                                "@id": service_id,
                                "profile": "http://iiif.io/api/image/2/level2.json",
                            },  # optional?
                        },
                    }
                ],
            }
            # add canvas to sequences
            manifest["sequences"][0]["canvases"].append(canvas)

        # save manifest file
        manifest_file = Path(build_directory.name).joinpath(
            variables["folder_arrangement"]["collection_id"],
            variables["archival_object"]["component_id"],
            "manifest.json",
        )
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        with open(
            manifest_file,
            "w",
        ) as f:
            f.write(json.dumps(manifest, indent=4))
    except Exception as error:
        import traceback

        logger.error(f"❌ ERROR: {error}")
        logger.error(f"❌ TRACEBACK: {traceback.format_exc()}")
        raise


def upload_iiif_manifest(build_directory, variables):
    try:
        # TODO generalize with upload_archival_object_page
        logger.info(
            f'🐛 UPLOAD IIIF MANIFEST: {variables["folder_arrangement"]["collection_id"]}/{variables["archival_object"]["component_id"]}/manifest.json'
        )
        logger.info(f"🐛 BUILD DIRECTORY: {build_directory.name}")
        manifest_key = (
            Path(variables["folder_arrangement"]["collection_id"])
            .joinpath(
                variables["archival_object"]["component_id"],
                "manifest.json",
            )
            .as_posix()
        )
        manifest_file = Path(build_directory.name).joinpath(manifest_key).as_posix()
        logger.info(f"🐛 IIIF MANIFEST EXISTS: {Path(manifest_file).exists()}")
        # TODO add config("ACCESS_BUCKET") to variables for fewer calls to decouple
        try:
            response = s3_client.upload_file(
                manifest_file,
                config("ACCESS_BUCKET"),
                manifest_key,
                ExtraArgs={"ContentType": "application/json"},
            )
            logger.info(f"🐛 RESPONSE: {response}")
        except Exception as e:
            logger.error(f"❌ EXCEPTION: {str(e)}")
    except Exception as e:
        import traceback

        logger.error(f"❌ EXCEPTION: {e}")
        logger.error(f"❌ TRACEBACK: {traceback.format_exc()}")
        raise


def create_pyramid_tiff(build_directory, variables):
    try:
        pyramid_tiff_key = "/".join(
            [
                variables["folder_arrangement"]["collection_id"],
                variables["archival_object"]["component_id"],
                f'{Path(variables["original_image_path"]).stem}.ptif',
            ]
        )
        pyramid_tiff_file = (
            Path(build_directory.name).joinpath(pyramid_tiff_key).as_posix()
        )
        output = subprocess.run(
            [
                config("WORK_VIPS_CMD"),
                "tiffsave",
                variables["original_image_path"],
                pyramid_tiff_file,
                "--tile",
                "--pyramid",
                "--compression",
                "jpeg",
                "--tile-width",
                "256",
                "--tile-height",
                "256",
            ],
            capture_output=True,
            text=True,
        ).stdout
    except Exception as e:
        import traceback

        logger.error(f"❌ INSIDE create_pyramid_tiff(): {e}")
        logger.error(f"❌ TRACEBACK: {traceback.format_exc()}")
        raise


def publish_access_files(build_directory, variables):
    logger.info(f"🐛 PUBLISH ACCESS FILES: {variables['filepaths']}")

    def sync_output(line):
        logger.info(f"🐛 SYNC OUTPUT: {line}")

    try:
        s5cmd_cmd = sh.Command(config("WORK_S5CMD_CMD"))
        # Sync each archival object directory separately to avoid deleting files
        # that are not in the build directory.
        for child in Path(
            f'{build_directory.name}/{variables["folder_arrangement"]["collection_id"]}'
        ).iterdir():
            if child.is_dir():
                sync = s5cmd_cmd(
                    "sync",
                    "--delete",
                    f"{child.as_posix()}/*",
                    f's3://{config("ACCESS_BUCKET")}/{variables["folder_arrangement"]["collection_id"]}/{variables["archival_object"]["component_id"]}/',
                    _env={
                        "AWS_ACCESS_KEY_ID": config("DISTILLERY_AWS_ACCESS_KEY_ID"),
                        "AWS_SECRET_ACCESS_KEY": config(
                            "DISTILLERY_AWS_SECRET_ACCESS_KEY"
                        ),
                    },
                    _out=sync_output,
                    _err=sync_output,
                    _bg=True,
                )
                sync.wait()
    except Exception as e:
        import traceback

        logger.error(f"❌ EXCEPTION: {e}")
        logger.error(f"❌ TRACEBACK: {traceback.format_exc()}")
        raise


def create_digital_object_file_versions(build_directory, variables):

    collection_directory = Path(build_directory.name).joinpath(
        variables["folder_arrangement"]["collection_id"]
    )

    for archival_object_directory in collection_directory.iterdir():

        if not archival_object_directory.is_dir():
            continue

        archival_object_page_url = "/".join(
            [
                config("ACCESS_SITE_BASE_URL").strip("/"),
                variables["folder_arrangement"]["collection_id"],
                variables["archival_object"]["component_id"],
                "index.html",
            ]
        )

        file_versions = [
            {
                "file_uri": archival_object_page_url,
                "jsonmodel_type": "file_version",
                "publish": True,
            },
            {
                "file_uri": get_thumbnail_url(variables),
                "jsonmodel_type": "file_version",
                "publish": True,
                "xlink_show_attribute": "embed",
            },
        ]

        # load existing or create new digital_object with component_id
        variables["archival_object"] = distillery.load_digital_object(
            variables["archival_object"]
        )

        for instance in variables["archival_object"]["instances"]:
            if "digital_object" in instance.keys():
                # ASSUMPTION: only one digital_object exists per archival_object
                # TODO handle multiple digital_objects per archival_object
                if instance["digital_object"]["_resolved"]["file_versions"]:
                    # TODO decide what to do with existing file_versions; unpublish? delete?
                    logger.warning(
                        f'🔥 EXISTING DIGITAL_OBJECT FILE_VERSIONS FOUND: {variables["archival_object"]["component_id"]}: {instance["digital_object"]["ref"]}'
                    )
                else:
                    digital_object = instance["digital_object"]["_resolved"]

        # NOTE this will fail if there are existing file_versions
        digital_object["file_versions"] = file_versions
        digital_object["publish"] = True

        digital_object_post_response = distillery.update_digital_object(
            digital_object["uri"], digital_object
        ).json()
