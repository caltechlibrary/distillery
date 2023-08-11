# file: alchemist.py
# RENDER AND PUBLISH ACCESS PAGES AND ASSETS

import json
import logging
import os
import subprocess
import tempfile
import time

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

rights_notice_html = '<p>These digitized collections are accessible for purposes of education and research. Due to the nature of archival collections, archivists at the Caltech Archives and Special Collections are not always able to identify copyright and rights of privacy, publicity, or trademark. We are eager to <a href="mailto:archives@caltech.edu">hear from any rights holders</a>, so that we may obtain accurate information. Upon request, we’ll remove material from public view while we address a rights issue.</p>'


class AccessPlatform:
    def __init__(self):
        self.build_directory = tempfile.TemporaryDirectory()

    def collection_structure_processing(self):
        # TODO build html metadata/thumbnail page?
        logger.debug("🐞 EMPTY METHOD")

    def archival_object_level_processing(self, variables):
        logger.info(f'ℹ️  {variables["archival_object"]["component_id"]}')
        generate_archival_object_page(self.build_directory, variables)
        generate_iiif_manifest(self.build_directory, variables)

    def create_access_file(self, variables):
        # TODO adapt for different file types
        logger.debug(
            f'🐞 variables["original_image_path"]: {variables["original_image_path"]}'
        )
        create_pyramid_tiff(self.build_directory, variables)
        return

    def transfer_archival_object_derivative_files(self, variables):
        logger.info(f'ℹ️  {variables["archival_object"]["component_id"]}')
        publish_archival_object_access_files(self.build_directory, variables)

    def loop_over_derivative_structure(self, variables):
        try:
            create_digital_object_file_versions(self.build_directory, variables)
        except:
            logger.exception("‼️")
            raise

    def regenerate_all(self, variables):
        collection_prefixes = []
        archival_object_prefixes = []
        paginator = s3_client.get_paginator("list_objects_v2")
        for result in paginator.paginate(Bucket=config("ACCESS_BUCKET"), Delimiter="/"):
            for prefix in result.get("CommonPrefixes"):
                # store collection_id/
                collection_prefixes.append(prefix.get("Prefix"))
                print(prefix.get("Prefix"))
        for collection_prefix in collection_prefixes:
            paginator = s3_client.get_paginator("list_objects_v2")
            for result in paginator.paginate(
                Bucket=config("ACCESS_BUCKET"), Delimiter="/", Prefix=collection_prefix
            ):
                for prefix in result.get("CommonPrefixes"):
                    # store collection_id/component_id/
                    archival_object_prefixes.append(prefix.get("Prefix"))
                    print(prefix.get("Prefix"))
        return archival_object_prefixes


def invalidate_cloudfront_path(path="/*", caller_reference=str(time.time())):
    cloudfront_client = boto3.client(
        "cloudfront",
        aws_access_key_id=config("DISTILLERY_AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=config("DISTILLERY_AWS_SECRET_ACCESS_KEY"),
    )
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudfront/client/create_invalidation.html
    response = cloudfront_client.create_invalidation(
        DistributionId=config("ALCHEMIST_CLOUDFRONT_DISTRIBUTION_ID"),
        InvalidationBatch={
            "Paths": {
                "Quantity": 1,
                "Items": [path],
            },
            "CallerReference": caller_reference,
        },
    )
    logger.debug(f"🐞 CLOUDFRONT INVALIDATION RESPONSE: {str(response)}")
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudfront/waiter/InvalidationCompleted.html
    waiter = cloudfront_client.get_waiter("invalidation_completed")
    logger.debug("🐞 WAITING ON CLOUDFRONT INVALIDATION")
    waiter.wait(
        DistributionId=config("ALCHEMIST_CLOUDFRONT_DISTRIBUTION_ID"),
        Id=response["Invalidation"]["Id"],
    )
    logger.debug("🐞 CLOUDFRONT INVALIDATION COMPLETE")


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
        logger.debug(f"🐞 BUILD_DIRECTORY.NAME: {build_directory.name}")
        environment = jinja2.Environment(
            loader=jinja2.FileSystemLoader(f"{os.path.dirname(__file__)}/templates"),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        template = environment.get_template("alchemist/archival_object.tpl")
        iiif_manifest_url = "/".join(
            [
                config("ACCESS_SITE_BASE_URL").rstrip("/"),
                config("ALCHEMIST_URL_PATH_PREFIX"),
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
        creators = format_archival_object_creators_display(variables["archival_object"])
        dates_display = format_archival_object_dates_display(
            variables["archival_object"]
        )
        extents_display = format_archival_object_extents_display(
            variables["archival_object"]
        )
        subjects = format_archival_object_subjects_display(variables["archival_object"])
        notes_display = format_archival_object_notes_display(
            variables["archival_object"]
        )
        archival_object_page_key = (
            Path(config("ALCHEMIST_URL_PATH_PREFIX"))
            .joinpath(
                variables["arrangement"]["collection_id"],
                variables["archival_object"]["component_id"],
                "index.html",
            )
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
                    collection_uri=variables["arrangement"]["collection_uri"],
                    series=series_display,
                    series_uri=variables["arrangement"]["series_uri"],
                    subseries=subseries_display,
                    subseries_uri=variables["arrangement"]["subseries_uri"],
                    dates=dates_display,
                    creators=creators,
                    extents=extents_display,
                    subjects=subjects,
                    notes=notes_display,
                    archivesspace_public_url=config("ASPACE_PUBLIC_URL"),
                    archival_object_uri=variables["archival_object"]["uri"],
                    iiif_manifest_url=iiif_manifest_url,
                    iiif_manifest_json=json.dumps({"manifest": f"{iiif_manifest_url}"}),
                    rights=rights_notice_html,
                )
            )
        logger.info(
            f"✨ ARCHIVAL OBJECT PAGE FILE GENERATED: {archival_object_page_file}"
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
            Path(config("ALCHEMIST_URL_PATH_PREFIX"))
            .joinpath(
                variables["arrangement"]["collection_id"],
                variables["archival_object"]["component_id"],
                "index.html",
            )
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
    thumbnail_id = "/".join(
        [
            config("ALCHEMIST_URL_PATH_PREFIX"),
            variables["arrangement"]["collection_id"],
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
        metadata.append({"label": "Creators", "value": list(creators.values())})
    extents = format_archival_object_extents_display(variables["archival_object"])
    if extents:
        metadata.append({"label": "Extents", "value": extents})
    subjects = format_archival_object_subjects_display(variables["archival_object"])
    if subjects:
        metadata.append({"label": "Subjects", "value": list(subjects.values())})
    notes = format_archival_object_notes_display(variables["archival_object"])
    description = ""
    attribution = rights_notice_html
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
        if variables.get("alchemist_regenerate"):
            manifest_key = (
                Path(config("ALCHEMIST_URL_PATH_PREFIX"))
                .joinpath(
                    variables["arrangement"]["collection_id"],
                    variables["archival_object"]["component_id"],
                    "manifest.json",
                )
                .as_posix()
            )
            response = s3_client.get_object(
                Bucket=config("ACCESS_BUCKET"),
                Key=manifest_key,
            )
            manifest = json.loads(response["Body"].read())
        else:
            manifest = {
                "@context": "http://iiif.io/api/presentation/2/context.json",
                "@type": "sc:Manifest",
                "@id": "/".join(
                    [
                        config("ACCESS_SITE_BASE_URL").rstrip("/"),
                        config("ALCHEMIST_URL_PATH_PREFIX"),
                        variables["arrangement"]["collection_id"],
                        variables["archival_object"]["component_id"],
                        "manifest.json",
                    ]
                ),
            }
        # maintain order of keys
        manifest["label"] = variables["archival_object"]["title"]
        if description:
            manifest["description"] = description
        if not variables.get("alchemist_regenerate"):
            manifest.update(
                {
                    "thumbnail": {
                        "@id": get_thumbnail_url(variables),
                        "service": {
                            "@context": "http://iiif.io/api/image/2/context.json",
                            "@id": get_thumbnail_url(variables).rsplit("/", maxsplit=4)[
                                0
                            ],
                            "profile": "http://iiif.io/api/image/2/level1.json",
                        },
                    },
                }
            )
        manifest["metadata"] = metadata
        if attribution:
            manifest["attribution"] = attribution
        if not variables.get("alchemist_regenerate"):
            manifest.update(
                {
                    "sequences": [{"@type": "sc:Sequence", "canvases": []}],
                }
            )
            for filepath in sorted(variables["filepaths"]):
                # create canvas metadata
                dimensions = (
                    os.popen(
                        f'{config("WORK_MAGICK_CMD")} identify -format "%w*%h" {filepath}'
                    )
                    .read()
                    .strip()
                    .split("*")
                )
                canvas_id = "/".join(
                    [
                        config("ACCESS_SITE_BASE_URL").rstrip("/"),
                        config("ALCHEMIST_URL_PATH_PREFIX"),
                        variables["arrangement"]["collection_id"],
                        variables["archival_object"]["component_id"],
                        "canvas",
                        f"{Path(filepath).stem}",
                    ]
                )
                escaped_identifier = "/".join(
                    [
                        config("ALCHEMIST_URL_PATH_PREFIX"),
                        variables["arrangement"]["collection_id"],
                        variables["archival_object"]["component_id"],
                        f"{Path(filepath).stem}",
                    ]
                )
                service_id = "/".join(
                    [config("ACCESS_IIIF_ENDPOINT").rstrip("/"), escaped_identifier]
                )
                resource_id = service_id + "/full/max/0/default.jpg"
                canvas = {
                    "@type": "sc:Canvas",
                    "@id": canvas_id,
                    "label": Path(filepath).stem.split("_")[-1].lstrip("0"),
                    "width": dimensions[0],
                    "height": dimensions[1],
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
                if variables["thumbnail_label"] == "filename":
                    canvas["label"] = Path(filepath).stem
                logger.debug(f"🐞 CANVAS: {canvas}")
                # add canvas to sequences
                manifest["sequences"][0]["canvases"].append(canvas)

        # save manifest file
        manifest_file = Path(build_directory.name).joinpath(
            config("ALCHEMIST_URL_PATH_PREFIX"),
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
            Path(config("ALCHEMIST_URL_PATH_PREFIX"))
            .joinpath(
                variables["arrangement"]["collection_id"],
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
        # NOTE vips has problems with JP2 source images
        if (
            os.popen(
                '{} identify -format "%m" {}'.format(
                    config("WORK_MAGICK_CMD"), variables["original_image_path"]
                )
            )
            .read()
            .strip()
            == "JP2"
        ):
            vips_source_image = (
                Path(build_directory.name).joinpath("uncompressed.tiff").as_posix()
            )
            magick_output = subprocess.run(
                [
                    config("WORK_MAGICK_CMD"),
                    "convert",
                    "-quiet",
                    variables["original_image_path"],
                    "-compress",
                    "None",
                    vips_source_image,
                ],
                capture_output=True,
                text=True,
            ).stdout
        else:
            vips_source_image = variables["original_image_path"]
        pyramid_tiff_key = "/".join(
            [
                config("ALCHEMIST_URL_PATH_PREFIX"),
                variables["arrangement"]["collection_id"],
                variables["archival_object"]["component_id"],
                f'{Path(variables["original_image_path"]).stem}.ptif',
            ]
        )
        pyramid_tiff_file = (
            Path(build_directory.name).joinpath(pyramid_tiff_key).as_posix()
        )
        vips_output = subprocess.run(
            [
                config("WORK_VIPS_CMD"),
                "tiffsave",
                vips_source_image,
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


def publish_archival_object_access_files(build_directory, variables):
    # NOTE working on variables["archival_object"]["component_id"]

    archival_object_access_path = "/".join(
        [
            config("ALCHEMIST_URL_PATH_PREFIX"),
            variables["arrangement"]["collection_id"],
            variables["archival_object"]["component_id"],
        ]
    )

    def sync_output(line):
        logger.debug(f"🐞 S5CMD SYNC OUTPUT: {line}")

    # TODO think about how to DRY this out;
    # we need one version with a --delete flag and one without
    if variables.get("alchemist_regenerate"):
        try:
            s5cmd_cmd = sh.Command(config("WORK_S5CMD_CMD"))
            sync = s5cmd_cmd(
                "sync",
                f"{build_directory.name}/{archival_object_access_path}/*",
                f's3://{config("ACCESS_BUCKET")}/{archival_object_access_path}/',
                _env={
                    "AWS_ACCESS_KEY_ID": config("DISTILLERY_AWS_ACCESS_KEY_ID"),
                    "AWS_SECRET_ACCESS_KEY": config("DISTILLERY_AWS_SECRET_ACCESS_KEY"),
                },
                _out=sync_output,
                _err=sync_output,
                _bg=True,
            )
            sync.wait()
        except:
            logger.exception("‼️")
            raise
    else:
        try:
            s5cmd_cmd = sh.Command(config("WORK_S5CMD_CMD"))
            sync = s5cmd_cmd(
                "sync",
                "--delete",
                f"{build_directory.name}/{archival_object_access_path}/*",
                f's3://{config("ACCESS_BUCKET")}/{archival_object_access_path}/',
                _env={
                    "AWS_ACCESS_KEY_ID": config("DISTILLERY_AWS_ACCESS_KEY_ID"),
                    "AWS_SECRET_ACCESS_KEY": config("DISTILLERY_AWS_SECRET_ACCESS_KEY"),
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
    # TODO no loop over collection directory; we are acting on an archival_object_directory
    logger.debug(f'🐞 ARCHIVAL_OBJECT: {variables["archival_object"]["component_id"]}')
    logger.debug(f"🐞 BUILD_DIRECTORY: {build_directory}")

    archival_object_directory = (
        Path(build_directory.name)
        .joinpath(
            config("ALCHEMIST_URL_PATH_PREFIX"),
            variables["arrangement"]["collection_id"],
            variables["archival_object"]["component_id"],
        )
        .resolve(strict=True)
    )
    logger.debug(f"🐞 ARCHIVAL_OBJECT_DIRECTORY: {archival_object_directory}")

    archival_object_page_url = "/".join(
        [
            config("ACCESS_SITE_BASE_URL").rstrip("/"),
            config("ALCHEMIST_URL_PATH_PREFIX"),
            variables["arrangement"]["collection_id"],
            variables["archival_object"]["component_id"],
        ]
    )
    logger.debug(f"🐞 ARCHIVAL_OBJECT_PAGE_URL: {archival_object_page_url}")

    variables["filepaths"] = [
        f.absolute()
        for f in archival_object_directory.iterdir()
        if f.is_file() and f.name.endswith(".ptif")
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
            "use_statement": "image-thumbnail",
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
        distillery.save_digital_object_file_versions(
            variables["archival_object"],
            file_versions,
            variables["file_versions_op"],
        )
    elif digital_object_count < 1:
        # returns new archival_object with digital_object instance included
        (
            digital_object_uri,
            variables["archival_object"],
        ) = distillery.create_digital_object(variables["archival_object"])
        distillery.save_digital_object_file_versions(
            variables["archival_object"],
            file_versions,
            variables["file_versions_op"],
        )


def format_archival_object_creators_display(archival_object):
    # TODO check for creators higher up in the hierarchy
    creators = {}
    for linked_agent in archival_object["linked_agents"]:
        if linked_agent["_resolved"]["publish"] and linked_agent["role"] == "creator":
            sort_name = linked_agent["_resolved"]["display_name"]["sort_name"]
            if linked_agent["relator"]:
                sort_name += f' [{linked_agent_archival_record_relators[linked_agent["relator"]]}]'
            creators[linked_agent["ref"]] = sort_name
    return creators


def format_archival_object_dates_display(archival_object):
    # NOTE begin and end could be: YYYY, YYYY-MM, YYYY-MM-DD
    months = {
        "01": "January",
        "02": "February",
        "03": "March",
        "04": "April",
        "05": "May",
        "06": "June",
        "07": "July",
        "08": "August",
        "09": "September",
        "10": "October",
        "11": "November",
        "12": "December",
    }
    dates_display = []
    for date in archival_object["dates"]:
        if date["label"] == "creation":
            # NOTE filter(None, ...) removes empty strings from the list
            if date.get("end"):
                if date["begin"][:4] == date["end"][:4]:
                    # NOTE exclude the end year per DACS: 1975 March-August
                    dates_display.append(
                        "{} to {}".format(
                            " ".join(
                                filter(
                                    None,
                                    [
                                        date.get("begin")[:4],
                                        months.get(date.get("begin")[5:7], ""),
                                        date.get("begin")[8:10].lstrip("0"),
                                    ],
                                )
                            ),
                            " ".join(
                                filter(
                                    None,
                                    [
                                        months.get(date.get("end")[5:7], ""),
                                        date.get("end")[8:10].lstrip("0"),
                                    ],
                                )
                            ),
                        )
                    )
                else:
                    dates_display.append(
                        "{} to {}".format(
                            " ".join(
                                filter(
                                    None,
                                    [
                                        date.get("begin")[:4],
                                        months.get(date.get("begin")[5:7], ""),
                                        date.get("begin")[8:10].lstrip("0"),
                                    ],
                                )
                            ),
                            " ".join(
                                filter(
                                    None,
                                    [
                                        date.get("end")[:4],
                                        months.get(date.get("end")[5:7], ""),
                                        date.get("end")[8:10].lstrip("0"),
                                    ],
                                )
                            ),
                        )
                    )
            elif date.get("begin"):
                dates_display.append(
                    " ".join(
                        filter(
                            None,
                            [
                                date.get("begin")[:4],
                                months[date.get("begin")[5:7]],
                                date.get("begin")[8:10].lstrip("0"),
                            ],
                        )
                    )
                )
            else:
                dates_display.append(date["expression"])
    return dates_display


def format_archival_object_extents_display(archival_object):
    extents_display = []
    for extent in archival_object["extents"]:
        extents_display.append(f'{extent["number"].strip()} {extent["extent_type"]}')
    return extents_display


def format_archival_object_subjects_display(archival_object):
    subjects = {}
    for subject in archival_object["subjects"]:
        if subject["_resolved"]["publish"]:
            subjects[subject["ref"]] = subject["_resolved"]["title"]
    # TODO check for subjects higher up in the hierarchy
    for linked_agent in archival_object["linked_agents"]:
        if linked_agent["_resolved"]["publish"] and linked_agent["role"] == "subject":
            sort_name = linked_agent["_resolved"]["display_name"]["sort_name"]
            if linked_agent["relator"]:
                sort_name += f' [{linked_agent_archival_record_relators[linked_agent["relator"]]}]'
            subjects[linked_agent["ref"]] = sort_name
    return subjects


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
