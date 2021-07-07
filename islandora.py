# PUBLISH ACCESS FILES AND METADATA TO ISLANDORA
# generate image files and metadata for display

# Islandora 7
# steps:
# - MODS XML, folder level
# -- distill:get_folder_arrangement()
# -- distill:get_folder_data()
# - MODS XML, page level
# -- distill:get_folder_data()
# - Lossy JPEG 2000 as OBJ datastream
# - Lossy JPEG 2000 as JP2 datastream
# - JPG
# - TN
# - OCR?
# - HOCR?
# - PDF?
# - RELS-INT

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from shutil import copyfile

import plac
import sh
from decouple import config
from lxml import etree
from lxml.builder import ElementMaker
from requests import HTTPError

import distill  # TODO sh logs end up in distillery.log; why?

# logging.config.fileConfig(
#     os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.ini"),
#     disable_existing_loggers=False,
# )
# # TODO need to understand more about naming the logger with __name__ and avoiding the
# # problem(?) with it looking for a logger named __main__
# # maybe we need a __main__.py file that calls distill.py and islandora.py?
# logger = logging.getLogger("islandora")
# logger.info("🦕 islandora")

islandora_server = sh.ssh.bake(
    f"{config('ISLANDORA_SSH_USER')}@{config('ISLANDORA_SSH_HOST')}",
    f"-p{config('ISLANDORA_SSH_PORT')}",
)

# TODO normalize collection_id (limit to allowed characters)
# function islandora_is_valid_pid($pid) {
#   return drupal_strlen(trim($pid)) <= 64 && preg_match('/^([A-Za-z0-9]|-|\.)+:(([A-Za-z0-9])|-|\.|~|_|(%[0-9A-F]{2}))+$/', trim($pid));
# }
# https://regexr.com/
def main(collection_id: "the Collection ID from ArchivesSpace"):

    # NOTE we have to assume that PROCESSING_FILES is set correctly
    stream_path = Path(config("PROCESSING_FILES")).joinpath(
        f"{collection_id}-processing"
    )
    with open(stream_path, "a") as stream:
        # NOTE specific emoji used to indicate start of script for event listener
        # SEE distillery.py:stream()
        stream.write(f"🟢\n")

    try:
        (
            STAGE_2_ORIGINAL_FILES,
            COMPRESSED_ACCESS_FILES,
        ) = validate_settings()
    except Exception as e:
        message = (
            "❌ There was a problem with the settings for the processing script.\n"
        )
        with open(stream_path, "a") as f:
            f.write(message)
        # logger.error(message, exc_info=True)
        raise

    with open(stream_path, "a") as f:
        f.write(f"📅 {datetime.now()}\n🦕 islandora processing\n🗄 {collection_id}\n")

    try:
        collection_directory = distill.get_collection_directory(
            STAGE_2_ORIGINAL_FILES, collection_id
        )
        if collection_directory:
            with open(stream_path, "a") as f:
                f.write(
                    f"✅ Collection directory for {collection_id} found on filesystem: {collection_directory}\n"
                )
        # TODO report on contents of collection_directory
    except NotADirectoryError as e:
        message = f"❌ No valid directory for {collection_id} was found on filesystem: {os.path.join(STAGE_2_ORIGINAL_FILES, collection_id)}\n"
        with open(stream_path, "a") as f:
            f.write(message)
        # logger.error(message, exc_info=True)
        raise

    try:
        collection_uri = distill.get_collection_uri(collection_id)
        if collection_uri:
            with open(stream_path, "a") as f:
                f.write(
                    f"✅ Collection URI for {collection_id} found in ArchivesSpace: {collection_uri}\n"
                )
    except ValueError as e:
        message = (
            f"❌ No collection URI for {collection_id} was found in ArchivesSpace.\n"
        )
        with open(stream_path, "a") as f:
            f.write(message)
        # logger.error(message, exc_info=True)
        raise
    except HTTPError as e:
        message = f"❌ There was a problem with the connection to ArchivesSpace."
        with open(stream_path, "a") as f:
            f.write(message)
        # logger.error(message, exc_info=True)
        raise

    try:
        collection_data = distill.get_collection_data(collection_uri)
        if collection_data:
            with open(stream_path, "a") as f:
                f.write(
                    f"✅ Collection data for {collection_id} retrieved from ArchivesSpace.\n"
                )
        # TODO report on contents of collection_data
    except RuntimeError as e:
        message = (
            f"❌ No collection data for {collection_id} retrieved from ArchivesSpace.\n"
        )
        with open(stream_path, "a") as f:
            f.write(message)
        # logger.error(message, exc_info=True)
        raise
    except HTTPError as e:
        message = f"❌ There was a problem with the connection to ArchivesSpace.\n"
        with open(stream_path, "a") as f:
            f.write(message)
        # logger.error(message, exc_info=True)
        raise

    # Set the directory for the Islandora collection files.
    # NOTE: The parent directory name is formatted for use as a PID:
    # https://github.com/mjordan/islandora_batch_with_derivs#preserving-existing-pids-and-relationships
    islandora_collection_metadata_directory = os.path.join(
        COMPRESSED_ACCESS_FILES,
        "collections",
        f"caltech+{collection_data['id_0']}", # NOTE hardcoded namespace
    )

    # Set up the MODS XML for the collection.
    collection_mods_xml = create_collection_mods_xml(collection_data)

    # Save the MODS.xml file for the collection.
    save_xml_file(
        os.path.join(
            islandora_collection_metadata_directory,
            "MODS.xml",
        ),
        collection_mods_xml,
    )

    # Set up the COLLECTION_POLICY XML for the collection.
    collection_policy_xml = create_collection_policy_xml(collection_data)

    # Save a COLLECTION_POLICY.xml file for the collection.
    save_xml_file(
        os.path.join(
            islandora_collection_metadata_directory,
            "COLLECTION_POLICY.xml",
        ),
        collection_policy_xml,
    )

    folders, filecount = distill.prepare_folder_list(collection_directory)
    filecounter = 0

    # Loop over folders list.
    folders.sort(reverse=True)
    for _ in range(len(folders)):
        # Using pop() (and/or range(len()) above) maybe helps to be sure that
        # if folder metadata fails to process properly, it and its images are
        # skipped completely and the script moves on to the next folder.
        folderpath = folders.pop()

        # Avoid processing folder when there are no files.
        filepaths = distill.prepare_filepaths_list(folderpath)
        if not filepaths:
            continue

        try:
            folder_arrangement, folder_data = process_folder_metadata(folderpath)
        except RuntimeError as e:
            print(str(e))
            print(" ❌\t Unable to process folder metadata...")
            print(f" \t ...skipping {folderpath}\n")
            # TODO increment file counter by the count of files in this folder
            continue

        # Set up the MODS XML for the book.
        modsxml = create_book_mods_xml(
            collection_data, folder_arrangement, folder_data, filepaths
        )

        # Save the MODS.xml file for the book.
        save_xml_file(
            os.path.join(
                COMPRESSED_ACCESS_FILES,
                "books",
                folder_data["component_id"],
                "MODS.xml",
            ),
            modsxml,
        )

        # TODO need a thumbnail for the book

        # Loop over filepaths list inside this folder.
        filepaths.sort(reverse=True)
        filecount_folder = len(filepaths)
        for f in range(filecount_folder):
            filepath = filepaths.pop()
            filecounter += 1
            if __debug__:
                print(
                    f" ▶️\t {os.path.basename(filepath)} [image {filecounter}/{filecount}]"
                )
            page_sequence = str(filecount_folder - len(filepaths)).zfill(4)
            try:
                (
                    hocr_path,
                    jp2_path,
                    jpg_path,
                    mods_path,
                    obj_path,
                    ocr_path,
                    tn_path,
                ) = generate_islandora_page_datastreams(
                    filepath,
                    page_sequence,
                    COMPRESSED_ACCESS_FILES,
                    collection_data,
                    folder_arrangement,
                    folder_data,
                )
                # copy first page thumbnail to book-level thumbnail
                if filecounter == 1:
                    copyfile(
                        tn_path,
                        os.path.join(
                            COMPRESSED_ACCESS_FILES,
                            "books",
                            folder_data["component_id"],
                            "TN.jpg",
                        ),
                    )
            except sh.ErrorReturnCode as e:
                print(str(e))
                continue
            except RuntimeError as e:
                print(str(e))
                continue

    # upload staging files to Islandora server
    try:
        islandora_staging_files = upload_to_islandora_server().strip()
    except Exception as e:
        # TODO log something
        raise e

    # retrieve a collection pid from Islandora
    try:
        # run a solr query via drush for the expected collection pid
        # NOTE: using dc fields for simpler syntax; a solr query string example using
        # fedora fields would be:
        # f"--solr_query='PID:caltech\:{collection_id} AND RELS_EXT_hasModel_uri_s:info\:fedora\/islandora\:collectionCModel'",
        idcrudfp = islandora_server(
            "drush",
            f"--root={config('ISLANDORA_WEBROOT')}",
            "islandora_datastream_crud_fetch_pids",
            f"--solr_query='dc.identifier:caltech\:{collection_id} AND dc.type:Collection'",
        )
        islandora_collection_pid = idcrudfp.strip()
    except sh.ErrorReturnCode as e:
        # drush exits with a non-zero status when no PIDs are found,
        # which is interpreted as an error
        # TODO how to structure this condition? it seems wrong to call a function inside here
        if "Sorry, no PIDS were found." in str(e.stderr, "utf-8"):
            # create a new collection because the identifier was not found
            islandora_collection_pid = create_islandora_collection(
                islandora_staging_files
            )
        else:
            raise e

    # add “books” to Islandora collection
    # TODO return something?
    add_books_to_islandora_collection(islandora_collection_pid, islandora_staging_files)

    # TODO write to ArchivesSpace digital object


def add_books_to_islandora_collection(
    islandora_collection_pid, islandora_staging_files
):
    ibbp = islandora_server(
        "drush",
        "--user=1",
        f"--root={config('ISLANDORA_WEBROOT')}",
        "islandora_book_batch_preprocess",
        "--output_set_id=TRUE",
        f"--parent={islandora_collection_pid}",
        f"--scan_target={islandora_staging_files}/books",
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
    # ibi is formatted like:
    # b'Ingested HBF:302.                                                           [ok]\nIngested HBF:303.                                                           [ok]\nIngested HBF:304.                                                           [ok]\nIngested HBF:301.                                                           [ok]\nProcessing complete; review the queue for some additional               [status]\ninformation.\n'
    print(str(ibi.stderr, "utf-8"))  # TODO log this
    # TODO what should return?


def create_book_mods_xml(collection_data, folder_arrangement, folder_data, filepaths):
    ns = "http://www.loc.gov/mods/v3"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"
    xsd = "http://www.loc.gov/standards/mods/v3/mods-3-7.xsd"
    E = ElementMaker(nsmap={None: ns, "xsi": xsi})
    modsxml = E.mods()
    # https://stackoverflow.com/questions/48970040/lxml-elementmaker-attribute-formatting
    modsxml.attrib["{{{pre}}}schemaLocation".format(pre=xsi)] = f"{ns} {xsd}"
    titleInfo = E.titleInfo()
    modsxml.append(titleInfo)
    titleInfo.append(E.title(folder_arrangement["folder_title"]))
    originInfo = E.originInfo()
    modsxml.append(originInfo)
    for date in folder_data["dates"]:
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
    modsxml.append(physicalDescription)
    physicalDescription.append(E.extent(str(len(filepaths)), unit="images"))
    relatedItem = E.relatedItem(type="host")
    modsxml.append(relatedItem)
    relatedItem_titleInfo = E.titleInfo()
    relatedItem_titleInfo.append(E.title(folder_arrangement["collection_display"]))
    if "series_id" in folder_arrangement:
        relatedItem_titleInfo.append(
            E.partNumber(f"Series {folder_arrangement['series_id']}")
        )
        relatedItem_titleInfo.append(E.partName(folder_arrangement["series_display"]))
    if "subseries_id" in folder_arrangement:
        relatedItem_titleInfo.append(
            E.partNumber(f"Subseries {folder_arrangement['subseries_id']}")
        )
        relatedItem_titleInfo.append(
            E.partName(folder_arrangement["subseries_display"])
        )
    relatedItem.append(relatedItem_titleInfo)
    modsxml.append(
        E.relatedItem(f"https://collections.archives.caltech.edu{folder_data['uri']}")
    )
    modsxml.append(E.identifier(folder_data["component_id"], type="local"))
    for note in collection_data["notes"]:
        if note["type"] == "userestrict":
            modsxml.append(
                E.accessCondition(
                    note["subnotes"][0]["content"], type="use and reproduction"
                )
            )
    return modsxml


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
        "--parent=islandora:root",  # TODO setting?
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
    # ibi.stderr is formatted like:
    # b'Ingested caltech:ABC.                                                       [ok]\nProcessing complete; review the queue for some additional               [status]\ninformation.\n'
    # we capture just the namespace:id portion (caltech:ABC)
    collection_pid = str(ibi.stderr, "utf-8").split()[1].strip(".")
    return collection_pid


def create_collection_mods_xml(collection_data):
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


def create_collection_policy_xml(collection_data):
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
    page_start = datetime.now()

    magick_cmd = sh.Command(config("MAGICK_CMD"))
    magick_cmd.convert(
        "-quiet", filepath, "-compress", "None", "/tmp/uncompressed.tiff"
    )

    tesseract_cmd = sh.Command(config("TESSERACT_CMD"))
    ocr_generation = tesseract_cmd(
        "/tmp/uncompressed.tiff", "/tmp/tesseract", "txt", "hocr", _bg=True
    )

    page_datastreams_directory = os.path.join(
        COMPRESSED_ACCESS_FILES, "books", folder_data["component_id"], page_sequence
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
    kdu_compress_cmd = sh.Command(config("KDU_COMPRESS_CMD"))
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

    file_parts = distill.get_file_parts(filepath)
    xmp_dc = distill.get_xmp_dc_metadata(
        folder_arrangement, file_parts, folder_data, collection_data
    )
    modsxml = create_page_mods_xml(xmp_dc)
    mods_path = os.path.join(page_datastreams_directory, "MODS.xml")
    save_xml_file(mods_path, modsxml)

    obj_conversion.wait()
    distill.write_xmp_metadata(obj_path, xmp_dc)

    jp2_path = os.path.join(page_datastreams_directory, "JP2.jp2")
    copyfile(obj_path, jp2_path)

    tn_conversion.wait()
    distill.write_xmp_metadata(tn_path, xmp_dc)

    jpg_conversion.wait()
    distill.write_xmp_metadata(jpg_path, xmp_dc)

    try:
        ocr_generation.wait(timeout=10)
        ocr_path = os.path.join(page_datastreams_directory, "OCR.txt")
        os.rename("/tmp/tesseract.txt", ocr_path)
        hocr_path = os.path.join(page_datastreams_directory, "HOCR.html")
        os.rename("/tmp/tesseract.hocr", hocr_path)
    except sh.TimeoutException:
        print(f" 🕑\t Tesseract timeout: {os.path.basename(filepath)}")

    os.remove("/tmp/uncompressed.tiff")

    print(
        f" ⏳\t {os.path.basename(filepath)} processing time: {datetime.now() - page_start}"
    )

    return (hocr_path, jp2_path, jpg_path, mods_path, obj_path, ocr_path, tn_path)


def process_folder_metadata(folderpath):
    try:
        folder_data = distill.get_folder_data(
            distill.normalize_directory_component_id(folderpath)
        )
    except ValueError as e:
        raise RuntimeError(str(e))

    try:
        folder_arrangement = distill.get_folder_arrangement(folder_data)
    except HTTPError as e:
        raise RuntimeError(str(e))

    return folder_arrangement, folder_data


def save_xml_file(destination_filepath, xml):
    os.makedirs(os.path.dirname(destination_filepath), exist_ok=True)
    with open(destination_filepath, "w") as f:
        f.write(etree.tostring(xml, encoding="unicode", pretty_print=True))


def upload_to_islandora_server():
    # TODO try/except?
    islandora_staging_files = islandora_server("mktemp", "-d")
    sh.rsync(
        "-az",
        "-e",
        f"ssh -p{config('ISLANDORA_SSH_PORT')}",
        f"{config('COMPRESSED_ACCESS_FILES')}/",
        f"{config('ISLANDORA_SSH_USER')}@{config('ISLANDORA_SSH_HOST')}:{islandora_staging_files}",
    )
    return islandora_staging_files


def validate_settings():
    STAGE_2_ORIGINAL_FILES = Path(
        os.path.expanduser(config("STAGE_2_ORIGINAL_FILES"))
    ).resolve(
        strict=True
    )  # NOTE do not create missing `STAGE_2_ORIGINAL_FILES` directory
    COMPRESSED_ACCESS_FILES = Path(
        os.path.expanduser(config("COMPRESSED_ACCESS_FILES"))
    ).resolve(
        strict=True
    )  # NOTE do not create missing `COMPRESSED_ACCESS_FILES` directory
    return STAGE_2_ORIGINAL_FILES, COMPRESSED_ACCESS_FILES


if __name__ == "__main__":
    plac.call(main)
