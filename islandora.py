# PUBLISH ACCESS FILES AND METADATA TO ISLANDORA
# generate image files and metadata for display

# NOTE: be sure that the islandora_batch module is patched on the Islandora server;
# publishing the correct file_uri in ArchivesSpace for digital objects relies on a
# patched islandora_batch module so PIDs can be specified for bookCModel objects
# SEE https://github.com/caltechlibrary/islandora_batch/commit/9968d30e68f3a12b03a071b45ada4d20a6c6b04b


import json
import logging
import mimetypes
import os
import shutil

from datetime import datetime
from pathlib import Path

import sh
from decouple import config
from lxml import etree
from lxml.builder import ElementMaker
from requests import HTTPError

import distillery

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.ini"),
    disable_existing_loggers=False,
)
logger = logging.getLogger("islandora")
validation_logger = logging.getLogger("validation")

islandora_server = sh.ssh.bake(
    f"-i",
    f"{config('ISLANDORA_SSH_KEY')}",
    f"{config('ISLANDORA_SSH_USER')}@{config('ISLANDORA_SSH_HOST')}",
    f"-p{config('ISLANDORA_SSH_PORT')}",
)

# TODO normalize collection_id (limit to allowed characters)
# function islandora_is_valid_pid($pid) {
#   return drupal_strlen(trim($pid)) <= 64 && preg_match('/^([A-Za-z0-9]|-|\.)+:(([A-Za-z0-9])|-|\.|~|_|(%[0-9A-F]{2}))+$/', trim($pid));
# }
# https://regexr.com/
class AccessPlatform:
    def __init__(self, collection_id, collection_data):
        self.collection_id = collection_id
        self.collection_data = collection_data

    def collection_structure_processing(self):
        logger.info(f"🐞 COLLECTION STRUCTURE PROCESSING: {self.collection_id}")
        (
            self.islandora_staging_files,
            self.islandora_collection_pid,
        ) = islandora_collection_structure_processing(
            self.collection_id, self.collection_data
        )

    def archival_object_level_processing(self, variables):
        logger.info(f"🐞 ARCHIVAL OBJECT LEVEL PROCESSING: {self.collection_id}")

    def create_access_files(self, variables):
        logger.info(f"🐞 CREATE ACCESS FILES: {self.collection_id}")

    def transfer_derivative_files(self, variables):
        logger.info(f"🐞 TRANSFER DERIVATIVE FILES: {self.collection_id}")
        islandora_transfer_derivative_files(self.islandora_staging_files)

    def ingest_derivative_files(self, variables):
        logger.info(f"🐞 INGEST DERIVATIVE FILES: {self.collection_id}")
        islandora_ingest_derivative_files(variables)

    def loop_over_derivative_structure(self, variables):
        logger.info(f"🐞 LOOP OVER DERIVATIVE STRUCTURE: {self.collection_id}")
        islandora_loop_over_derivative_structure(variables)


def islandora_collection_structure_processing(collection_id, collection_data):
    """ Set the directory for the Islandora collection files."""
    # NOTE: The parent directory name is formatted for use as a PID:
    # https://github.com/mjordan/islandora_batch_with_derivs#preserving-existing-pids-and-relationships
    islandora_collection_metadata_directory = os.path.join(
        config("COMPRESSED_ACCESS_FILES"),
        "collections",
        f"caltech+{collection_id}",  # NOTE hardcoded namespace
    )

    # Construct the MODS XML for the collection.
    collection_mods_xml = construct_collection_mods_xml(collection_data)

    # Save the MODS.xml file for the collection.
    collection_mods_xml_path = save_xml_file(
        os.path.join(
            islandora_collection_metadata_directory,
            "MODS.xml",
        ),
        collection_mods_xml,
    )
    logger.info(f"☑️  ISLANDORA COLLECTION MODS SAVED: {collection_mods_xml_path}")

    # Construct the COLLECTION_POLICY XML for the collection.
    collection_policy_xml = construct_collection_policy_xml(collection_data)

    # Save a COLLECTION_POLICY.xml file for the collection.
    collection_policy_xml_path = save_xml_file(
        os.path.join(
            islandora_collection_metadata_directory,
            "COLLECTION_POLICY.xml",
        ),
        collection_policy_xml,
    )
    logger.info(f"☑️  ISLANDORA COLLECTION POLICY SAVED: {collection_policy_xml_path}")

    # Create the temporary staging directory.
    islandora_staging_files = islandora_server.mktemp("--directory").strip()
    validation_logger.info(f"ISLANDORA: {islandora_staging_files}")
    # Upload the collections directory.
    upload_to_islandora_server(
        os.path.join(config("COMPRESSED_ACCESS_FILES"), "collections"),
        islandora_staging_files,
    )

    # Retrieve an existing or new collection PID from Islandora.
    try:
        # Run a Solr query via drush for the expected collection PID.
        # NOTE: Using DC fields for simpler syntax; a Solr query string example
        # using fedora fields would be:
        # f"--solr_query='PID:caltech\:{collection_id} AND RELS_EXT_hasModel_uri_s:info\:fedora\/islandora\:collectionCModel'",
        idcrudfp = islandora_server(
            "drush",
            f"--root={config('ISLANDORA_WEBROOT')}",
            "islandora_datastream_crud_fetch_pids",
            f'--solr_query="dc.identifier:caltech\:{collection_id} AND dc.type:Collection"',
        )
        islandora_collection_pid = idcrudfp.strip()
        logger.info(
            f'☑️  EXISTING ISLANDORA COLLECTION FOUND: {config("ISLANDORA_URL").rstrip("/")}/islandora/object/{islandora_collection_pid}'
        )
        return islandora_staging_files, islandora_collection_pid
    except sh.ErrorReturnCode as e:
        # Drush exits with a non-zero status when no PIDs are found, which is
        # interpreted as an error by sh.
        if "Sorry, no PIDS were found." in str(e.stderr, "utf-8"):
            # Create a new collection because the identifier was not found.
            islandora_collection_pid = create_islandora_collection(
                islandora_staging_files
            )
            return islandora_staging_files, islandora_collection_pid
        else:
            raise


def archival_object_level_processing(variables):
    # logger.debug("🦕 archival_object_level_processing()")
    # logger.debug("\n".join(["🐞 variables.keys():", *variables.keys()]))

    # Construct the MODS XML for the book.
    book_mods_xml = construct_book_mods_xml(variables)

    # Save the MODS.xml file for the book.
    book_mods_xml_path = save_xml_file(
        os.path.join(
            config("COMPRESSED_ACCESS_FILES"),
            "books",
            f'{variables["collection_id"]}+{variables["folder_data"]["component_id"]}',
            "MODS.xml",
        ),
        book_mods_xml,
    )
    logger.info(f"☑️  ISLANDORA BOOK MODS SAVED: {book_mods_xml_path}")


def create_access_files(variables):
    # logger.debug("🦕 create_access_files()")
    # logger.debug("\n".join(["🐞 variables.keys():", *variables.keys()]))
    if variables["filepaths_count_initial"] > 1:
        # we have some kind of multi-file compound object
        sequence = str(
            variables["filepaths_count_initial"] - len(variables["filepaths_popped"])
        ).zfill(4)
        type, encoding = mimetypes.guess_type(variables["original_image_path"])
        if type == "image/tiff":
            try:
                datastream_paths = generate_islandora_page_datastreams(
                    variables["original_image_path"],
                    sequence,
                    config("COMPRESSED_ACCESS_FILES"),
                    variables["collection_data"],
                    variables["folder_arrangement"],
                    variables["folder_data"],
                )
                # copy first page thumbnail to book-level thumbnail
                if sequence == "0001":
                    shutil.copyfile(
                        datastream_paths["tn_path"],
                        os.path.join(
                            config("COMPRESSED_ACCESS_FILES"),
                            "books",
                            f'{variables["collection_id"]}+{variables["folder_data"]["component_id"]}',
                            "TN.jpg",
                        ),
                    )
            except sh.ErrorReturnCode as e:
                logger.warning(
                    f'⚠️  DATASTREAM GENERATION FAILED: {Path(variables["original_image_path"]).name}'
                )
                logger.debug(f"🐞 EXCEPTION:\n{str(e)}")
                return
        else:
            logger.debug(f"🐞 MIMETYPE NOT ACCOUNTED FOR: {type}")
    else:
        logger.debug(f'🐞 SINGLE FILE IN DIRECTORY: {variables["original_image_path"]}')


def islandora_ingest_derivative_files(variables):
    # TODO determine object types
    # TODO check for existing book PIDs; script crashes if book PID exists
    add_books_to_islandora_collection(variables)


def add_books_to_islandora_collection(variables):
    # NOTE: be sure that the islandora_batch module is patched on the Islandora server;
    # publishing the correct file_uri in ArchivesSpace for digital objects relies on a
    # patched islandora_batch module so PIDs can be specified for bookCModel objects
    # SEE https://github.com/caltechlibrary/islandora_batch/commit/9968d30e68f3a12b03a071b45ada4d20a6c6b04b
    ibbp = islandora_server(
        "drush",
        "--user=1",
        f"--root={config('ISLANDORA_WEBROOT')}",
        "islandora_book_batch_preprocess",
        "--output_set_id=TRUE",
        f'--parent={variables["islandora_collection_pid"]}',
        f'--scan_target={variables["islandora_staging_files"]}/books',
        "--type=directory",
    )
    # ibbp contains a trailing newline
    ingest_set = ibbp.strip()
    ibi = islandora_server(
        "drush",
        "--user=1",
        f"--root={config('ISLANDORA_WEBROOT')}",
        "islandora_batch_ingest",
        f"--ingest_set={ingest_set}",
    )
    # ibi is formatted like (actual newlines inserted here for readability):
    # b'Ingested CollectionID:302.                                                  [ok]
    # \nIngested CollectionID:303.                                                  [ok]
    # \nIngested CollectionID:304.                                                  [ok]
    # \nIngested CollectionID:301.                                                  [ok]
    # \nProcessing complete; review the queue for some additional               [status]
    # \ninformation.
    # \n'
    logger.debug(
        "\n".join(["OUTPUT: drush islandora_batch_ingest", ibi.stderr.decode().strip()])
    )
    for line in ibi.stderr.decode().splitlines():
        if line.split(":")[0].split()[-1] == variables["collection_id"]:
            validation_logger.info(
                f'ISLANDORA: {config("ISLANDORA_URL").rstrip("/")}/islandora/object/{line.split(".")[0].split()[-1]}'
            )
    logger.info(
        f'☑️  ISLANDORA BATCH INGEST COMPLETE: {config("ISLANDORA_URL").rstrip("/")}/admin/reports/islandora_batch_queue/{ingest_set}'
    )


def islandora_loop_over_derivative_structure(variables):
    for book_directory in Path(
        f'{config("COMPRESSED_ACCESS_FILES").rstrip("/")}/books'
    ).iterdir():
        if not book_directory.is_dir():
            continue

        book_pid = book_directory.name.replace("+", ":")

        # update ArchivesSpace digital object

        # NOTE: the file_uri value used below relies on a patched islandora_batch module
        # which allows PIDs to be specified for bookCModel objects
        # SEE https://github.com/caltechlibrary/islandora_batch/commit/9968d30e68f3a12b03a071b45ada4d20a6c6b04b

        file_versions = [
            {
                "file_uri": f'{config("ISLANDORA_URL").rstrip("/")}/islandora/object/{book_pid}',
                "jsonmodel_type": "file_version",
                "publish": True,
            },
            {
                "file_uri": f'{config("ISLANDORA_URL").rstrip("/")}/islandora/object/{book_pid}/datastream/TN/view',
                "jsonmodel_type": "file_version",
                "publish": True,
                "xlink_show_attribute": "embed",
            },
        ]

        variables["folder_data"] = distillery.get_folder_data(book_pid.split(":")[-1])
        # confirm existing or create digital_object with component_id
        variables["folder_data"] = distillery.confirm_digital_object(
            variables["folder_data"]
        )

        for instance in variables["folder_data"]["instances"]:
            if "digital_object" in instance.keys():
                if instance["digital_object"]["_resolved"]["file_versions"]:
                    raise RuntimeError(
                        f"⚠️ uh oh, digital_object file_versions for {variables['folder_data']['component_id']} has data: {instance['digital_object']['ref']}"
                    )
                else:
                    digital_object = instance["digital_object"]["_resolved"]

        digital_object["file_versions"] = file_versions

        digital_object_post_response = distillery.update_digital_object(
            digital_object["uri"], digital_object
        ).json()


def construct_book_mods_xml(variables):
    ns = "http://www.loc.gov/mods/v3"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"
    xsd = "http://www.loc.gov/standards/mods/v3/mods-3-7.xsd"
    E = ElementMaker(nsmap={None: ns, "xsi": xsi})
    book_mods_xml = E.mods()
    # https://stackoverflow.com/questions/48970040/lxml-elementmaker-attribute-formatting
    book_mods_xml.attrib["{{{pre}}}schemaLocation".format(pre=xsi)] = f"{ns} {xsd}"
    titleInfo = E.titleInfo()
    book_mods_xml.append(titleInfo)
    titleInfo.append(E.title(variables["folder_arrangement"]["folder_title"]))
    originInfo = E.originInfo()
    book_mods_xml.append(originInfo)
    for date in variables["folder_data"]["dates"]:
        if date["label"] == "creation":
            if date["date_type"] == "single":
                originInfo.append(E.dateCreated(date["begin"], encoding="w3cdtf"))
            if date["date_type"] == "inclusive":
                originInfo.append(
                    E.dateCreated(date["begin"], encoding="w3cdtf", point="start")
                )
                originInfo.append(
                    E.dateCreated(date["end"], encoding="w3cdtf", point="end")
                )
            if date["date_type"] == "bulk":
                pass
    physicalDescription = E.physicalDescription()
    book_mods_xml.append(physicalDescription)
    physicalDescription.append(
        E.extent(str(len(variables["filepaths"])), unit="images")
    )
    relatedItem = E.relatedItem(type="host")
    book_mods_xml.append(relatedItem)
    relatedItem_titleInfo = E.titleInfo()
    relatedItem_titleInfo.append(
        E.title(variables["folder_arrangement"]["collection_display"])
    )
    if "series_id" in variables["folder_arrangement"]:
        relatedItem_titleInfo.append(
            E.partNumber(f'Series {variables["folder_arrangement"]["series_id"]}')
        )
        relatedItem_titleInfo.append(
            E.partName(variables["folder_arrangement"]["series_display"])
        )
    if "subseries_id" in variables["folder_arrangement"]:
        relatedItem_titleInfo.append(
            E.partNumber(f'Subseries {variables["folder_arrangement"]["subseries_id"]}')
        )
        relatedItem_titleInfo.append(
            E.partName(variables["folder_arrangement"]["subseries_display"])
        )
    relatedItem.append(relatedItem_titleInfo)
    book_mods_xml.append(
        E.relatedItem(
            f'https://collections.archives.caltech.edu{variables["folder_data"]["uri"]}'
        )
    )
    book_mods_xml.append(
        E.identifier(variables["folder_data"]["component_id"], type="local")
    )
    for note in variables["collection_data"]["notes"]:
        if note["type"] == "userestrict":
            book_mods_xml.append(
                E.accessCondition(
                    note["subnotes"][0]["content"], type="use and reproduction"
                )
            )
    return book_mods_xml


def create_islandora_collection(islandora_staging_files):
    # Islandora Batch with Derivatives allows us to set a PID for our new collection.
    # https://github.com/mjordan/islandora_batch_with_derivs#preserving-existing-pids-and-relationships
    ibwd = islandora_server(
        "drush",
        "--user=1",
        f"--root={config('ISLANDORA_WEBROOT')}",
        "islandora_batch_with_derivs_preprocess",
        "--content_models=islandora:collectionCModel",
        "--key_datastream=MODS",
        "--namespace=caltech",  # TODO setting?
        "--parent=caltech:archives",  # TODO setting?
        f"--scan_target={islandora_staging_files}/collections",
        "--use_pids=TRUE",
    )
    # ibwd.stderr is formatted like:
    # b'SetId: 1234                                                                 [ok]\n'
    # we capture just the integer portion (1234)
    ingest_set = str(ibwd.stderr, "utf-8").split()[1]
    ibi = islandora_server(
        "drush",
        "--user=1",
        f"--root={config('ISLANDORA_WEBROOT')}",
        "islandora_batch_ingest",
        f"--ingest_set={ingest_set}",
    )
    # ibi.stderr is formatted like (actual newlines inserted here for readability):
    # b'Ingested caltech:ABC.                                                       [ok]
    # \nProcessing complete; review the queue for some additional               [status]
    # \ninformation.
    # \n'
    # we capture just the namespace:id portion (caltech:ABC)
    collection_pid = str(ibi.stderr, "utf-8").split()[1].strip(".")
    logger.info(f"☑️  ISLANDORA COLLECTION CREATED: {collection_pid}")
    validation_logger.info(
        f'ISLANDORA: {config("ISLANDORA_URL").rstrip("/")}/islandora/object/{collection_pid}'
    )
    return collection_pid


def construct_collection_mods_xml(collection_data):
    ns = "http://www.loc.gov/mods/v3"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"
    xsd = "http://www.loc.gov/standards/mods/v3/mods-3-7.xsd"
    E = ElementMaker(nsmap={None: ns, "xsi": xsi})
    modsxml = E.mods()
    # https://stackoverflow.com/questions/48970040/lxml-elementmaker-attribute-formatting
    modsxml.attrib["{{{pre}}}schemaLocation".format(pre=xsi)] = f"{ns} {xsd}"
    titleInfo = E.titleInfo()
    modsxml.append(titleInfo)
    titleInfo.append(E.title(collection_data["title"]))
    modsxml.append(E.typeOfResource(collection="yes"))
    return modsxml


def construct_collection_policy_xml(collection_data):
    ns = "http://www.islandora.ca"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"
    xsd = "http://syn.lib.umanitoba.ca/collection_policy.xsd"
    E = ElementMaker(nsmap={None: ns, "xsi": xsi})
    collection_policy_xml = E.collection_policy()
    # https://stackoverflow.com/questions/48970040/lxml-elementmaker-attribute-formatting
    collection_policy_xml.attrib[
        "{{{pre}}}schemaLocation".format(pre=xsi)
    ] = f"{ns} {xsd}"
    collection_policy_xml.attrib["name"] = ""
    content_models = E.content_models()
    collection_policy_xml.append(content_models)
    namespace = collection_data["id_0"]
    content_models.append(
        E.content_model(
            name="Islandora Page Content Model",
            namespace=namespace,
            pid="islandora:pageCModel",
            dsid="",
        )
    )
    content_models.append(
        E.content_model(
            name="Islandora Internet Archive Book Content Model",
            namespace=namespace,
            pid="islandora:bookCModel",
            dsid="",
        )
    )
    collection_policy_xml.append(E.relationship("isMemberOfCollection"))
    collection_policy_xml.append(E.search_terms())
    return collection_policy_xml


def create_page_mods_xml(xmp_dc):
    ns = "http://www.loc.gov/mods/v3"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"
    xsd = "http://www.loc.gov/standards/mods/v3/mods-3-7.xsd"
    E = ElementMaker(nsmap={None: ns, "xsi": xsi})
    modsxml = E.mods()
    # https://stackoverflow.com/questions/48970040/lxml-elementmaker-attribute-formatting
    modsxml.attrib["{{{pre}}}schemaLocation".format(pre=xsi)] = f"{ns} {xsd}"
    titleInfo = E.titleInfo()
    modsxml.append(titleInfo)
    titleInfo.append(E.title(xmp_dc["title"]))
    return modsxml


def generate_islandora_page_datastreams(
    filepath,
    page_sequence,
    COMPRESSED_ACCESS_FILES,
    collection_data,
    folder_arrangement,
    folder_data,
):
    magick_cmd = sh.Command(config("WORK_MAGICK_CMD"))
    magick_cmd.convert(
        "-quiet", filepath, "-compress", "None", "/tmp/uncompressed.tiff"
    )

    tesseract_cmd = sh.Command(config("WORK_TESSERACT_CMD"))
    ocr_generation = tesseract_cmd(
        "/tmp/uncompressed.tiff", "/tmp/tesseract", "txt", "hocr", _bg=True
    )

    page_datastreams_directory = os.path.join(
        COMPRESSED_ACCESS_FILES,
        "books",
        f"{collection_data['id_0']}+{folder_data['component_id']}",
        page_sequence,
    )
    os.makedirs(page_datastreams_directory, exist_ok=True)

    jpg_path = os.path.join(page_datastreams_directory, "JPG.jpg")
    jpg_conversion = magick_cmd.convert(
        "-quiet", filepath, "-quality", "75", "-resize", "600x800", jpg_path, _bg=True
    )

    tn_path = os.path.join(page_datastreams_directory, "TN.jpg")
    tn_conversion = magick_cmd.convert(
        "-quiet", filepath, "-quality", "75", "-thumbnail", "200x200", tn_path, _bg=True
    )

    obj_path = os.path.join(page_datastreams_directory, "OBJ.jp2")
    kdu_compress_cmd = sh.Command(config("WORK_KDU_COMPRESS_CMD"))
    obj_conversion = kdu_compress_cmd(
        "-i",
        "/tmp/uncompressed.tiff",
        "-o",
        obj_path,
        "-rate",
        "0.5",
        "Clayers=1",
        "Clevels=7",
        "Cprecincts={256,256},{256,256},{256,256},{128,128},{128,128},{64,64},{64,64},{32,32},{16,16}",
        "Corder=RPCL",
        "ORGgen_plt=yes",
        "ORGtparts=R",
        "Cblk={32,32}",
        "Cuse_sop=yes",
        _bg=True,
    )

    file_parts = distillery.get_file_parts(filepath)
    xmp_dc = distillery.get_xmp_dc_metadata(
        folder_arrangement, file_parts, folder_data, collection_data
    )
    modsxml = create_page_mods_xml(xmp_dc)
    mods_path = os.path.join(page_datastreams_directory, "MODS.xml")
    save_xml_file(mods_path, modsxml)

    obj_conversion.wait()
    distillery.write_xmp_metadata(obj_path, xmp_dc)

    jp2_path = os.path.join(page_datastreams_directory, "JP2.jp2")
    shutil.copyfile(obj_path, jp2_path)

    tn_conversion.wait()
    distillery.write_xmp_metadata(tn_path, xmp_dc)

    jpg_conversion.wait()
    distillery.write_xmp_metadata(jpg_path, xmp_dc)

    datastream_paths = {
        "jp2_path": jp2_path,
        "jpg_path": jpg_path,
        "mods_path": mods_path,
        "obj_path": obj_path,
        "tn_path": tn_path,
    }

    try:
        ocr_generation.wait(timeout=10)
        datastream_paths["ocr_path"] = os.path.join(
            page_datastreams_directory, "OCR.txt"
        )
        shutil.move("/tmp/tesseract.txt", datastream_paths["ocr_path"])
        datastream_paths["hocr_path"] = os.path.join(
            page_datastreams_directory, "HOCR.html"
        )
        shutil.move("/tmp/tesseract.hocr", datastream_paths["hocr_path"])
    except sh.TimeoutException:
        logger.warning(f" 🕑\t Tesseract timeout: {os.path.basename(filepath)}")

    os.remove("/tmp/uncompressed.tiff")

    logger.info(
        f"☑️  ISLANDORA PAGE DATASTREAMS GENERATED: {page_datastreams_directory}"
    )
    return datastream_paths


def process_folder_metadata(folderpath):
    try:
        folder_data = distillery.get_folder_data(
            distillery.normalize_directory_component_id(folderpath)
        )
    except ValueError as e:
        raise RuntimeError(str(e))

    try:
        folder_arrangement = distillery.get_folder_arrangement(folder_data)
    except HTTPError as e:
        raise RuntimeError(str(e))

    return folder_arrangement, folder_data


def save_xml_file(destination_filepath, xml):
    os.makedirs(os.path.dirname(destination_filepath), exist_ok=True)
    with open(destination_filepath, "w") as f:
        f.write(etree.tostring(xml, encoding="unicode", pretty_print=True))
    return destination_filepath


def islandora_transfer_derivative_files(islandora_staging_files):
    """Transfer ISLANDORA_ACCESS_FILES directory to Islandora server."""
    # Calculate whether the directory will fit on the server.
    access_files_bytes = distillery.get_directory_bytes(
        config("COMPRESSED_ACCESS_FILES")
    )
    logger.info(f"🔢 BYTECOUNT OF ISLANDORA ACCESS FILES: {access_files_bytes}")
    # NOTE output from islandora_server connection is a string formatted like:
    # `52701552640 3822366720`
    server_bytes = islandora_server(
        f'{config("ISLANDORA_PYTHON3_CMD")} -c \'import shutil; total, used, free = shutil.disk_usage("{islandora_staging_files}"); print(total, free)\'',
    ).strip()
    # convert the string to a tuple and get the parts
    server_total_bytes = tuple(map(int, server_bytes.split(" ")))[0]
    server_free_bytes = tuple(map(int, server_bytes.split(" ")))[1]
    logger.info(f"🔢 FREE BYTES ON ISLANDORA SERVER: {server_free_bytes}")
    server_capacity_buffer = server_total_bytes * 0.01  # reserve 1% for tape index
    if not server_free_bytes - access_files_bytes > server_capacity_buffer:
        message = f"❌ the islandora server does not have capacity for staging this set of access files: {islandora_staging_files}"
        logger.error(message)
        raise RuntimeError(message)
    # Copy ISLANDORA_ACCESS_FILES to Islandora server using rsync.
    # NOTE this is only for staging files prior to ingest
    upload_to_islandora_server(
        os.path.join(config("COMPRESSED_ACCESS_FILES"), "books"),
        islandora_staging_files,
    )


def upload_to_islandora_server(source_directory, variables, islandora_staging_files):
    """Copy files via rsync for staging."""
    rsync_cmd = sh.Command(config("WORK_RSYNC_CMD"))
    rsync_cmd(
        "-az",
        "-e",
        f"ssh -i {config('ISLANDORA_SSH_KEY')} -p{config('ISLANDORA_SSH_PORT')}",
        source_directory,
        f'{config("ISLANDORA_SSH_USER")}@{config("ISLANDORA_SSH_HOST")}:{variables["islandora_staging_files"]}',
    )
    logger.info(
        f'☑️  ISLANDORA STAGING FILES UPLOADED: {variables["islandora_staging_files"]}'
    )


def islandora_server_connection_is_successful():
    islandora_server_connection = islandora_server()
    if islandora_server_connection.exit_code == 0:
        logger.info(f"🦕 ISLANDORA SERVER CONNECTION SUCCESSFUL: {islandora_server}")
        return True


def validate_settings():
    WORKING_ORIGINAL_FILES = Path(
        os.path.expanduser(config("WORKING_ORIGINAL_FILES"))
    ).resolve(
        strict=True
    )  # NOTE do not create missing `WORKING_ORIGINAL_FILES` directory
    COMPRESSED_ACCESS_FILES = Path(
        os.path.expanduser(config("COMPRESSED_ACCESS_FILES"))
    ).resolve(
        strict=True
    )  # NOTE do not create missing `COMPRESSED_ACCESS_FILES` directory
    return WORKING_ORIGINAL_FILES, COMPRESSED_ACCESS_FILES
