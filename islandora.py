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

import distill
import json
import os
import plac
import sh
from datetime import datetime
from lxml import etree
from lxml.builder import ElementMaker
from pathlib import Path
from requests import HTTPError
from shutil import copyfile

from decouple import config


@plac.annotations(
    collection_id=("the collection identifier from ArchivesSpace"),
)
def main(collection_id):

    try:
        (
            UNCOMPRESSED_SOURCE_FILES,
            COMPRESSED_ACCESS_FILES,
        ) = validate_settings()
    except Exception as e:
        # # different emoji to indicate start of script for event listener
        # message = (
        #     "⛔️ There was a problem with the settings for the processing script.\n"
        # )
        # with open(stream_path, "a") as f:
        #     f.write(message)
        # # delete the stream file, otherwise it will continue trying to process
        # stream_path.unlink(missing_ok=True)
        # logging.error(message, exc_info=True)
        raise

    collection_directory = distill.get_collection_directory(
        UNCOMPRESSED_SOURCE_FILES, collection_id
    )
    collection_uri = distill.get_collection_uri(collection_id)
    collection_data = distill.get_collection_data(collection_uri)

    # Set the directory for the Islandora collection files.
    # NOTE: The parent directory name is formatted for use as a PID:
    # https://github.com/mjordan/islandora_batch_with_derivs#preserving-existing-pids-and-relationships
    islandora_collection_metadata_directory = os.path.join(
        COMPRESSED_ACCESS_FILES,
        "collections",
        f"caltech+{collection_data['id_0']}",
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
    filecounter = filecount

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

        # Loop over filepaths list inside this folder.
        filepaths.sort(reverse=True)
        filecount_folder = len(filepaths)
        for f in range(filecount_folder):
            filepath = filepaths.pop()
            if __debug__:
                print(
                    # TODO use ascending count, both folder and total
                    f" ▶️\t {os.path.basename(filepath)} [images remaining: {filecounter}/{filecount}]"
                )
            filecounter -= 1
            page_sequence = str(filecount_folder - len(filepaths)).zfill(4)
            try:
                generate_islandora_page_datastreams(
                    filepath,
                    page_sequence,
                    COMPRESSED_ACCESS_FILES,
                    collection_data,
                    folder_arrangement,
                    folder_data,
                )
            except RuntimeError as e:
                print(str(e))
                continue


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

    sh.magick.convert("-quiet", filepath, "-compress", "None", "/tmp/uncompressed.tiff")

    ocr_generation = sh.tesseract(
        "/tmp/uncompressed.tiff", "/tmp/tesseract", "txt", "hocr", _bg=True
    )

    page_datastreams_directory = os.path.join(
        COMPRESSED_ACCESS_FILES, "books", folder_data["component_id"], page_sequence
    )
    os.makedirs(page_datastreams_directory, exist_ok=True)

    jpg_path = os.path.join(page_datastreams_directory, "JPG.jpg")
    jpg_conversion = sh.magick.convert(
        "-quiet", filepath, "-quality", "75", "-resize", "600x800", jpg_path, _bg=True
    )

    tn_path = os.path.join(page_datastreams_directory, "TN.jpg")
    tn_conversion = sh.magick.convert(
        "-quiet", filepath, "-quality", "75", "-thumbnail", "200x200", tn_path, _bg=True
    )

    obj_path = os.path.join(page_datastreams_directory, "OBJ.jp2")
    obj_conversion = sh.kdu_compress(
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


def validate_settings():
    UNCOMPRESSED_SOURCE_FILES = Path(
        os.path.expanduser(config("UNCOMPRESSED_SOURCE_FILES"))
    ).resolve(
        strict=True
    )  # NOTE do not create missing `UNCOMPRESSED_SOURCE_FILES` directory
    COMPRESSED_ACCESS_FILES = Path(
        os.path.expanduser(config("COMPRESSED_ACCESS_FILES"))
    ).resolve(
        strict=True
    )  # NOTE do not create missing `COMPRESSED_ACCESS_FILES` directory
    return UNCOMPRESSED_SOURCE_FILES, COMPRESSED_ACCESS_FILES


if __name__ == "__main__":
    plac.call(main)
