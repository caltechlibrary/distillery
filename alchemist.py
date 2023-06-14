# file: alchemist.py
# RENDER AND PUBLISH ACCESS PAGES AND ASSETS

import json
import logging
import os
import subprocess
import tempfile

from pathlib import Path

import arrow
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

linked_agent_archival_record_relators = {
    "abr": "Abridger",
    "acp": "Art copyist",
    "act": "Actor",
    "adi": "Art director",
    "adp": "Adapter",
    "aft": "Author of afterword, colophon, etc.",
    "anl": "Analyst",
    "anm": "Animator",
    "ann": "Annotator",
    "ant": "Bibliographic antecedent",
    "ape": "Appellee",
    "apl": "Appellant",
    "app": "Applicant",
    "aqt": "Author in quotations or text abstracts",
    "arc": "Architect",
    "ard": "Artistic director",
    "arr": "Arranger",
    "art": "Artist",
    "asg": "Assignee",
    "asn": "Associated name",
    "ato": "Autographer",
    "att": "Attributed name",
    "auc": "Auctioneer",
    "aud": "Author of dialog",
    "aui": "Author of introduction, etc.",
    "aus": "Author of screenplay, etc.",
    "aut": "Author",
    "bdd": "Binding designer",
    "bjd": "Bookjacket designer",
    "bkd": "Book designer",
    "bkp": "Book producer",
    "blw": "Blurb writer",
    "bnd": "Binder",
    "bpd": "Bookplate designer",
    "brd": "Broadcaster",
    "brl": "Braille embosser",
    "bsl": "Bookseller",
    "cas": "Caster",
    "ccp": "Conceptor",
    "chr": "Choreographer",
    "clb": "Collaborator",
    "cli": "Client",
    "cll": "Calligrapher",
    "clr": "Colorist",
    "clt": "Collotyper",
    "cmm": "Commentator",
    "cmp": "Composer",
    "cmt": "Compositor",
    "cnd": "Conductor",
    "cng": "Cinematographer",
    "cns": "Censor",
    "coe": "Contestant-appellee",
    "col": "Collector",
    "com": "Compiler",
    "con": "Conservator",
    "cor": "Collection registrar",
    "cos": "Contestant",
    "cot": "Contestant-appellant",
    "cou": "Court governed",
    "cov": "Cover designer",
    "cpc": "Copyright claimant",
    "cpe": "Complainant-appellee",
    "cph": "Copyright holder",
    "cpl": "Complainant",
    "cpt": "Complainant-appellant",
    "cre": "Creator",
    "crp": "Correspondent",
    "crr": "Corrector",
    "crt": "Court reporter",
    "csl": "Consultant",
    "csp": "Consultant to a project",
    "cst": "Costume designer",
    "ctb": "Contributor",
    "cte": "Contestee-appellee",
    "ctg": "Cartographer",
    "ctr": "Contractor",
    "cts": "Contestee",
    "ctt": "Contestee-appellant",
    "cur": "Curator of an exhibition",
    "cwt": "Commentator for written text",
    "dbp": "Distribution place",
    "dfd": "Defendant",
    "dfe": "Defendant-appellee",
    "dft": "Defendant-appellant",
    "dgg": "Degree grantor",
    "dgs": "Degree supervisor",
    "dis": "Dissertant",
    "dln": "Delineator",
    "dnc": "Dancer",
    "dnr": "Donor",
    "dpc": "Depicted",
    "dpt": "Depositor",
    "drm": "Draftsman",
    "drt": "Director",
    "dsr": "Designer",
    "dst": "Distributor",
    "dtc": "Data contributor",
    "dte": "Dedicatee",
    "dtm": "Data manager",
    "dto": "Dedicator",
    "dub": "Dubious author",
    "edc": "Editor of compilation",
    "edm": "Editor of moving image work",
    "edt": "Editor",
    "egr": "Engraver",
    "elg": "Electrician",
    "elt": "Electrotyper",
    "eng": "Engineer",
    "enj": "Enacting jurisdiction",
    "etr": "Etcher",
    "evp": "Event place",
    "exp": "Appraiser",
    "fac": "Facsimilist",
    "fds": "Film distributor",
    "fld": "Field director",
    "flm": "Film editor",
    "fmd": "Film director",
    "fmk": "Filmmaker",
    "fmo": "Former owner",
    "fmp": "Film producer",
    "fnd": "Funder",
    "fpy": "First party",
    "frg": "Forger",
    "gis": "Geographic information specialist",
    "grt": "Graphic technician",
    "his": "Host institution",
    "hnr": "Honoree",
    "hst": "Host",
    "ill": "Illustrator",
    "ilu": "Illuminator",
    "ins": "Inscriber",
    "inv": "Inventor",
    "isb": "Issuing body",
    "itr": "Instrumentalist",
    "ive": "Interviewee",
    "ivr": "Interviewer",
    "jud": "Judge",
    "jug": "Jurisdiction governed",
    "lbr": "Laboratory",
    "lbt": "Librettist",
    "ldr": "Laboratory director",
    "led": "Lead",
    "lee": "Libelee-appellee",
    "lel": "Libelee",
    "len": "Lender",
    "let": "Libelee-appellant",
    "lgd": "Lighting designer",
    "lie": "Libelant-appellee",
    "lil": "Libelant",
    "lit": "Libelant-appellant",
    "lsa": "Landscape architect",
    "lse": "Licensee",
    "lso": "Licensor",
    "ltg": "Lithographer",
    "lyr": "Lyricist",
    "mcp": "Music copyist",
    "mdc": "Metadata contact",
    "med": "Medium",
    "mfp": "Manufacture place",
    "mfr": "Manufacturer",
    "mod": "Moderator",
    "mon": "Monitor",
    "mrb": "Marbler",
    "mrk": "Markup editor",
    "msd": "Musical director",
    "mte": "Metal-engraver",
    "mtk": "Minute taker",
    "mus": "Musician",
    "nrt": "Narrator",
    "opn": "Opponent",
    "org": "Originator",
    "orm": "Organizer of meeting",
    "osp": "Onscreen presenter",
    "oth": "Other",
    "own": "Owner",
    "pan": "Panelist",
    "pat": "Patron",
    "pbd": "Publishing director",
    "pbl": "Publisher",
    "pdr": "Project director",
    "pfr": "Proofreader",
    "pht": "Photographer",
    "plt": "Platemaker",
    "pma": "Permitting agency",
    "pmn": "Production manager",
    "pop": "Printer of plates",
    "ppm": "Papermaker",
    "ppt": "Puppeteer",
    "pra": "Praeses",
    "prc": "Process contact",
    "prd": "Production personnel",
    "pre": "Presenter",
    "prf": "Performer",
    "prg": "Programmer",
    "prm": "Printmaker",
    "prn": "Production company",
    "pro": "Producer",
    "prp": "Production place",
    "prs": "Production designer",
    "prt": "Printer",
    "prv": "Provider",
    "pta": "Patent applicant",
    "pte": "Plaintiff-appellee",
    "ptf": "Plaintiff",
    "pth": "Patentee",
    "ptt": "Plaintiff-appellant",
    "pup": "Publication place",
    "rbr": "Rubricator",
    "rcd": "Recordist",
    "rce": "Recording engineer",
    "rcp": "Recipient",
    "rdd": "Radio director",
    "red": "Redaktor",
    "ren": "Renderer",
    "res": "Researcher",
    "rev": "Reviewer",
    "rpc": "Radio producer",
    "rps": "Repository",
    "rpt": "Reporter",
    "rpy": "Responsible party",
    "rse": "Respondent-appellee",
    "rsg": "Restager",
    "rsp": "Respondent",
    "rsr": "Restorationist",
    "rst": "Respondent-appellant",
    "rth": "Research team head",
    "rtm": "Research team member",
    "sad": "Scientific advisor",
    "sce": "Scenarist",
    "scl": "Sculptor",
    "scr": "Scribe",
    "sds": "Sound designer",
    "sec": "Secretary",
    "sgd": "Stage director",
    "sgn": "Signer",
    "sht": "Supporting host",
    "sll": "Seller",
    "sng": "Singer",
    "spk": "Speaker",
    "spn": "Sponsor",
    "spy": "Second party",
    "srv": "Surveyor",
    "std": "Set designer",
    "stg": "Setting",
    "stl": "Storyteller",
    "stm": "Stage manager",
    "stn": "Standards body",
    "str": "Stereotyper",
    "tcd": "Technical director",
    "tch": "Teacher",
    "ths": "Thesis advisor",
    "tld": "Television director",
    "tlp": "Television producer",
    "trc": "Transcriber",
    "trl": "Translator",
    "tyd": "Type designer",
    "tyg": "Typographer",
    "uvp": "University place",
    "vac": "Voice actor",
    "vdg": "Videographer",
    "voc": "Vocalist",
    "wac": "Writer of added commentary",
    "wal": "Writer of added lyrics",
    "wam": "Writer of accompanying material",
    "wat": "Writer of added text",
    "wdc": "Woodcutter",
    "wde": "Wood engraver",
    "win": "Writer of introduction",
    "wit": "Witness",
    "wpr": "Writer of preface",
    "wst": "Writer of supplementary textual content",
}


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
        environment = jinja2.Environment(
            loader=jinja2.FileSystemLoader(f"{os.path.dirname(__file__)}/templates"),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        template = environment.get_template("alchemist/archival_object.tpl")
        logger.info(f"🐛 TEMPLATE: {template}")
        logger.info(f"🐛 BUILD DIRECTORY: {build_directory.name}")
        collection_directory = Path(build_directory.name).joinpath(
            variables["arrangement"]["collection_id"]
        )
        logger.info(f"🐛 COLLECTION DIRECTORY: {collection_directory}")
        iiif_manifest_url = "/".join(
            [
                config("ACCESS_SITE_BASE_URL").rstrip("/"),
                variables["arrangement"]["collection_id"],
                variables["archival_object"]["component_id"],
                "manifest.json",
            ]
        )
        if variables["arrangement"].get("series_title"):
            series_display = variables["arrangement"]["series_title"]
        else:
            series_display = variables["arrangement"].get("series_display")
        if variables["arrangement"].get("subseries_title"):
            subseries_display = variables["arrangement"]["subseries_title"]
        else:
            subseries_display = variables["arrangement"].get("subseries_display")
        creators_display = format_archival_object_creators_display(
            variables["archival_object"]
        )
        dates_display = format_archival_object_dates_display(
            variables["archival_object"]
        )
        extents_display = format_archival_object_extents_display(
            variables["archival_object"]
        )
        subjects_display = format_archival_object_subjects_display(
            variables["archival_object"]
        )
        notes_display = format_archival_object_notes_display(
            variables["archival_object"]
        )
        archival_object_page_key = (
            Path(variables["arrangement"]["collection_id"])
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
                    collection=variables["arrangement"].get("collection_title"),
                    series=series_display,
                    subseries=subseries_display,
                    dates=dates_display,
                    creators=creators_display,
                    extents=extents_display,
                    subjects=subjects_display,
                    notes=notes_display,
                    archivesspace_public_url=config("ASPACE_PUBLIC_URL"),
                    archival_object_uri=variables["archival_object"]["uri"],
                    iiif_manifest_url=iiif_manifest_url,
                    iiif_manifest_json=json.dumps({"manifest": f"{iiif_manifest_url}"}),
                )
            )
        logger.info(
            f"🐛 ARCHIVAL OBJECT PAGE FILE GENERATED: {archival_object_page_file}"
        )
    except Exception as e:
        logger.exception(e)
        raise


def upload_archival_object_page(build_directory, variables):
    try:
        # TODO generalize with upload_iiif_manifest
        logger.info(
            f'🐛 UPLOAD ARCHIVAL OBJECT PAGE: {variables["arrangement"]["collection_id"]}/{variables["archival_object"]["component_id"]}/index.html'
        )
        logger.info(f"🐛 BUILD DIRECTORY: {build_directory.name}")
        archival_object_page_key = (
            Path(variables["arrangement"]["collection_id"])
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
        logger.exception(e)
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
    metadata = []
    if variables["arrangement"].get("collection_title"):
        metadata.append(
            {
                "label": "Collection",
                "value": variables["arrangement"]["collection_title"],
            }
        )
    if variables["arrangement"].get("series_title"):
        metadata.append(
            {
                "label": "Series",
                "value": variables["arrangement"]["series_title"],
            }
        )
    elif variables["arrangement"].get("series_display"):
        metadata.append(
            {
                "label": "Series",
                "value": variables["arrangement"]["series_display"],
            }
        )
    if variables["arrangement"].get("subseries_title"):
        metadata.append(
            {
                "label": "Sub-Series",
                "value": variables["arrangement"]["subseries_title"],
            }
        )
    elif variables["arrangement"].get("subseries_display"):
        metadata.append(
            {
                "label": "Sub-Series",
                "value": variables["arrangement"]["subseries_display"],
            }
        )
    dates = format_archival_object_dates_display(variables["archival_object"])
    if dates:
        metadata.append({"label": "Dates", "value": dates})
    creators = format_archival_object_creators_display(variables["archival_object"])
    if creators:
        metadata.append({"label": "Creators", "value": creators})
    extents = format_archival_object_extents_display(variables["archival_object"])
    if extents:
        metadata.append({"label": "Extents", "value": extents})
    subjects = format_archival_object_subjects_display(variables["archival_object"])
    if subjects:
        metadata.append({"label": "Subjects", "value": subjects})
    notes = format_archival_object_notes_display(variables["archival_object"])
    description = ""
    attribution = ""
    if notes:
        for note_label, note_contents in notes.items():
            if note_contents:
                metadata.append({"label": note_label, "value": note_contents})
            if note_label == "Scope and Contents":
                description = note_contents
            if note_label == "Conditions Governing Use":
                # TODO check for note upwards in the hierarchy
                # TODO validate against Restriction End date
                attribution = note_contents
    try:
        manifest = {
            "@context": "http://iiif.io/api/presentation/2/context.json",
            "@type": "sc:Manifest",
            "@id": "/".join(
                [
                    config("ACCESS_SITE_BASE_URL").strip("/"),
                    variables["arrangement"]["collection_id"],
                    variables["archival_object"]["component_id"],
                    "/manifest.json",
                ]
            ),
            "label": variables["archival_object"]["title"],
        }
        # maintain order of keys
        if description:
            manifest["description"] = description
        manifest.update(
            {
                "thumbnail": {
                    "@id": get_thumbnail_url(variables),
                    "service": {
                        "@context": "http://iiif.io/api/image/2/context.json",
                        "@id": get_thumbnail_url(variables).rsplit("/", maxsplit=4)[0],
                        "profile": "http://iiif.io/api/image/2/level1.json",
                    },
                },
                "metadata": metadata,
            }
        )
        if attribution:
            manifest["attribution"] = attribution
        manifest.update(
            {
                "sequences": [{"@type": "sc:Sequence", "canvases": []}],
            }
        )
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
                    variables["arrangement"]["collection_id"],
                    variables["archival_object"]["component_id"],
                    "canvas",
                    f"{Path(filepath).stem}",
                ]
            )
            escaped_identifier = "/".join(
                [
                    variables["arrangement"]["collection_id"],
                    variables["archival_object"]["component_id"],
                    f"{Path(filepath).stem}",
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
            variables["arrangement"]["collection_id"],
            variables["archival_object"]["component_id"],
            "manifest.json",
        )
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        with open(
            manifest_file,
            "w",
        ) as f:
            f.write(json.dumps(manifest, indent=4))
    except Exception as e:
        logger.exception(e)
        raise


def upload_iiif_manifest(build_directory, variables):
    try:
        # TODO generalize with upload_archival_object_page
        logger.info(
            f'🐛 UPLOAD IIIF MANIFEST: {variables["arrangement"]["collection_id"]}/{variables["archival_object"]["component_id"]}/manifest.json'
        )
        logger.info(f"🐛 BUILD DIRECTORY: {build_directory.name}")
        manifest_key = (
            Path(variables["arrangement"]["collection_id"])
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
        logger.exception(e)
        raise


def create_pyramid_tiff(build_directory, variables):
    try:
        pyramid_tiff_key = "/".join(
            [
                variables["arrangement"]["collection_id"],
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
        logger.exception(e)
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
            f'{build_directory.name}/{variables["arrangement"]["collection_id"]}'
        ).iterdir():
            if child.is_dir():
                sync = s5cmd_cmd(
                    "sync",
                    "--delete",
                    f"{child.as_posix()}/*",
                    f's3://{config("ACCESS_BUCKET")}/{variables["arrangement"]["collection_id"]}/{variables["archival_object"]["component_id"]}/',
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
    except:
        logger.exception("‼️")
        raise


def create_digital_object_file_versions(build_directory, variables):
    try:

        collection_directory = Path(build_directory.name).joinpath(
            variables["arrangement"]["collection_id"]
        )
        logger.debug(f"🐞 COLLECTION_DIRECTORY: {collection_directory}")

        for archival_object_directory in collection_directory.iterdir():

            if not archival_object_directory.is_dir():
                continue
            logger.debug(f"🐞 ARCHIVAL_OBJECT_DIRECTORY: {archival_object_directory}")

            variables["archival_object"] = distillery.find_archival_object(
                archival_object_directory.name
            )
            logger.debug(
                f'🐞 ARCHIVAL_OBJECT: {variables["archival_object"]["component_id"]}'
            )

            archival_object_page_url = "/".join(
                [
                    config("ACCESS_SITE_BASE_URL").strip("/"),
                    variables["arrangement"]["collection_id"],
                    variables["archival_object"]["component_id"],
                    "index.html",
                ]
            )
            logger.debug(f"🐞 ARCHIVAL_OBJECT_PAGE_URL: {archival_object_page_url}")

            variables["filepaths"] = [
                f.absolute()
                for f in archival_object_directory.iterdir()
                if f.is_file() and f.name not in [".DS_Store", "Thumbs.db"]
            ]
            logger.debug(f'🐞 FILEPATHS[0]: {sorted(variables["filepaths"])[0]}')

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

            digital_object_count = len(
                [
                    i
                    for i in variables["archival_object"]["instances"]
                    if "digital_object" in i.keys()
                ]
            )
            logger.debug(f"🐞 DIGITAL OBJECT COUNT: {digital_object_count}")
            if digital_object_count > 1:
                raise ValueError(
                    f'❌ MULTIPLE DIGITAL OBJECTS FOUND: {variables["archival_object"]["component_id"]}'
                )
            elif digital_object_count == 1:
                distillery.add_digital_object_file_versions(
                    variables["archival_object"], file_versions
                )
            elif digital_object_count < 1:
                # returns new archival_object with digital_object instance included
                (
                    digital_object_uri,
                    variables["archival_object"],
                ) = distillery.create_digital_object(variables["archival_object"])
                distillery.add_digital_object_file_versions(
                    variables["archival_object"], file_versions
                )
    except:
        logger.exception("‼️")
        raise


def format_archival_object_creators_display(archival_object):
    # TODO check for creators higher up in the hierarchy
    creators_display = []
    for linked_agent in archival_object["linked_agents"]:
        if linked_agent["_resolved"]["publish"] and linked_agent["role"] == "creator":
            sort_name = linked_agent["_resolved"]["display_name"]["sort_name"]
            if linked_agent["relator"]:
                sort_name += f' [{linked_agent_archival_record_relators[linked_agent["relator"]]}]'
            creators_display.append(sort_name)
    return creators_display


def format_archival_object_dates_display(archival_object):
    dates_display = []
    for date in archival_object["dates"]:
        if date["label"] == "creation":
            if date.get("end"):
                dates_display.append(
                    f'{arrow.get(date["begin"]).format("YYYY MMMM D")} to {arrow.get(date["end"]).format("YYYY MMMM D")}'
                )
            elif date.get("begin"):
                dates_display.append(arrow.get(date["begin"]).format("YYYY MMMM D"))
            else:
                dates_display.append(date["expression"])
    return dates_display


def format_archival_object_extents_display(archival_object):
    extents_display = []
    for extent in archival_object["extents"]:
        extents_display.append(f'{extent["number"].strip()} {extent["extent_type"]}')
    return extents_display


def format_archival_object_subjects_display(archival_object):
    subjects_display = []
    for subject in archival_object["subjects"]:
        if subject["_resolved"]["publish"]:
            subjects_display.append(subject["_resolved"]["title"])
    # TODO check for subjects higher up in the hierarchy
    for linked_agent in archival_object["linked_agents"]:
        if linked_agent["_resolved"]["publish"] and linked_agent["role"] == "subject":
            sort_name = linked_agent["_resolved"]["display_name"]["sort_name"]
            if linked_agent["relator"]:
                sort_name += f' [{linked_agent_archival_record_relators[linked_agent["relator"]]}]'
            subjects_display.append(sort_name)
    return subjects_display


def format_archival_object_notes_display(archival_object):
    singlepart_note_types = {
        "abstract": "Abstract",
        "materialspec": "Materials Specific Details",
        "physdesc": "Physical Description",
        "physfacet": "Physical Facet",
        "physloc": "Physical Location",
    }
    multipart_note_types = {
        "accessrestrict": "Conditions Governing Access",
        "accruals": "Accruals",
        "acqinfo": "Immediate Source of Acquisition",
        "altformavail": "Existence and Location of Copies",
        "appraisal": "Appraisal",
        "arrangement": "Arrangement",
        "bioghist": "Biographical / Historical",
        "custodhist": "Custodial History",
        "dimensions": "Dimensions",
        "fileplan": "File Plan",
        "legalstatus": "Legal Status",
        "odd": "General",
        "originalsloc": "Existence and Location of Originals",
        "otherfindaid": "Other Finding Aids",
        "phystech": "Physical Characteristics and Technical Requirements",
        "prefercite": "Preferred Citation",
        "processinfo": "Processing Information",
        "relatedmaterial": "Related Materials",
        "scopecontent": "Scope and Contents",
        "separatedmaterial": "Separated Materials",
        "userestrict": "Conditions Governing Use",
    }
    notes_display = {}
    for note in archival_object["notes"]:
        if note["publish"]:
            if note["jsonmodel_type"] == "note_singlepart":
                if note["type"] in singlepart_note_types.keys():
                    if note.get("label"):
                        note_label = note["label"]
                    else:
                        note_label = singlepart_note_types[note["type"]]
                    if note_label not in notes_display.keys():
                        notes_display[note_label] = []
                    for content in note["content"]:
                        # group all same-type or -label notes together
                        notes_display[note_label].append(content)
            elif note["jsonmodel_type"] == "note_multipart":
                if note["type"] in multipart_note_types.keys():
                    if note.get("label"):
                        note_label = note["label"]
                    else:
                        note_label = multipart_note_types[note["type"]]
                    if note_label not in notes_display.keys():
                        # NOTE this could end up remaining empty
                        notes_display[note_label] = []
                    for subnote in note["subnotes"]:
                        if (
                            subnote["publish"]
                            and subnote["jsonmodel_type"] == "note_text"
                        ):
                            # group all same-type or -label notes together
                            notes_display[note_label].append(subnote["content"])
                        # TODO elif subnote["jsonmodel_type"] == "note_chronology":
                        # TODO elif subnote["jsonmodel_type"] == "note_definedlist":
                        # TODO elif subnote["jsonmodel_type"] == "note_orderedlist":
            # TODO elif note["jsonmodel_type"] == "note_bibliography":
            # TODO elif note["jsonmodel_type"] == "note_index":
    return notes_display
