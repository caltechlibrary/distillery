import json
import logging
import os
import sh
import shutil
import tempfile

from pathlib import Path

from decouple import config  # pypi: python-decouple

import distillery

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    Path(__file__)
    .resolve()
    .parent.joinpath("settings.ini"),
)
logger = logging.getLogger("oralhistories")


def main(
    docxfile: ("Word file to convert to Markdown", "option", "w"),  # type: ignore
    publish: ("Initiate publishing to web", "flag", "p"),  # type: ignore
):
    if docxfile:
        repo_dir = clone_git_repository()
        transcript_dir = Path(repo_dir).joinpath("transcripts", Path(docxfile).stem)
        os.makedirs(transcript_dir, exist_ok=True)
        metadata = create_metadata_file(transcript_dir)
        convert_word_to_markdown(docxfile, transcript_dir)
        push_markdown_file(transcript_dir)
        digital_object_uri = create_digital_object(metadata)
        create_digital_object_component(
            digital_object_uri,
            "Transcript Markdown",
            f'{metadata["component_id"]}-TS.md',
        )
        # cleanup
        shutil.rmtree(repo_dir)
    if publish:
        repo_dir = clone_git_repository()
        aws_cmd = sh.Command(config("WORK_AWS_CMD"))
        s3sync_output = aws_cmd(
            "s3",
            "sync",
            f'{Path(repo_dir).joinpath("transcripts")}',
            f's3://{config("OH_S3_BUCKET")}',
            "--exclude",
            "*.md",
            "--delete",
            "--no-progress",
            _env={
                "AWS_ACCESS_KEY_ID": config("AWS_ACCESS_KEY"),
                "AWS_SECRET_ACCESS_KEY": config("AWS_SECRET_KEY"),
            },
        )
        for line in s3sync_output.splitlines():
            logger.info(f"line: {line}")
            if line.split()[0] == "upload:":
                # look for the digital_object ending in -TS
                digital_object_uri = distillery.find_digital_object(
                    f'{line.split()[1].split("/")[-2]}-TS'
                )
                if not digital_object_uri:
                    logger.warning(
                        f'⚠️  DIGITAL OBJECT NOT FOUND: {line.split()[1].split("/")[-2]}-TS'
                    )
                    continue
                digital_object = distillery.archivessnake_get(digital_object_uri).json()
                # add file_version to digital_object
                if line.split()[-1].split(".")[-1] == "html":
                    file_versions = [
                        file_version["file_uri"]
                        for file_version in digital_object["file_versions"]
                    ]
                    if line.split()[-1] not in file_versions:
                        # TODO change file_uri from s3 scheme
                        file_version = {"file_uri": line.split()[-1], "publish": True}
                        digital_object["publish"] = True
                        digital_object["file_versions"].append(file_version)
                        distillery.archivessnake_post(
                            digital_object_uri, digital_object
                        )
                else:
                    # look for an existing digital_object_component
                    digital_object_component_uri = find_digital_object_component(
                        f'{line.split()[-1].split("/")[-1]}'
                    )
                    if digital_object_component_uri:
                        logger.info(
                            f'ℹ️  EXISTING DIGITAL OBJECT COMPONENT FOUND: {line.split()[-1].split("/")[-1]}'
                        )
                        # no updates needed for existing record
                        continue
                    # create new digital_object_component
                    create_digital_object_component(
                        digital_object_uri,
                        f'Transcript Asset *-{line.split()[-1].split("/")[-1].split("-")[-1]}',
                        line.split()[-1].split("/")[-1],
                    )


def find_digital_object_component(digital_object_component_component_id):
    response = distillery.archivessnake_get(
        f"/repositories/2/find_by_id/digital_object_components?component_id[]={digital_object_component_component_id}"
    )
    response.raise_for_status()
    if len(response.json()["digital_object_components"]) < 1:
        return None
    if len(response.json()["digital_object_components"]) > 1:
        raise ValueError(
            f"❌ MULTIPLE DIGITAL OBJECT COMPONENTS FOUND WITH COMPONENT ID: {digital_object_component_component_id}"
        )
    return response.json()["digital_object_components"][0]["ref"]


def create_digital_object(metadata):
    digital_object = {}
    digital_object["digital_object_id"] = f'{metadata["component_id"]}-TS'  # required
    digital_object["title"] = f'{metadata["title"]} Transcript'  # required
    # TODO handle error upstream if digital_object_id already exists
    digital_object_post_response = distillery.archivessnake_post(
        "/repositories/2/digital_objects", digital_object
    )
    logger.info(
        f'✳️  DIGITAL OBJECT CREATED: {digital_object_post_response.json()["uri"]}'
    )
    # set up a digital object instance to add to the archival object
    digital_object_instance = {
        "instance_type": "digital_object",
        "digital_object": {"ref": digital_object_post_response.json()["uri"]},
    }
    # get archival object
    archival_object_get_response = distillery.archivessnake_get(
        metadata["archival_object_uri"]
    )
    archival_object = archival_object_get_response.json()
    # add digital object instance to archival object
    archival_object["instances"].append(digital_object_instance)
    # post updated archival object
    archival_object_post_response = distillery.archivessnake_post(
        metadata["archival_object_uri"], archival_object
    )
    logger.info(
        f'☑️  ARCHIVAL OBJECT UPDATED: {archival_object_post_response.json()["uri"]}'
    )
    return digital_object_post_response.json()["uri"]


def create_digital_object_component(digital_object_uri, label, filename):
    digital_object_component = {"digital_object": {"ref": digital_object_uri}}
    digital_object_component["label"] = label
    # TODO unsure about -TS in filename; TBD
    digital_object_component["component_id"] = filename
    digital_object_component["file_versions"] = [
        {
            "file_uri": f'https://github.com/{config("OH_REPO")}/blob/main/transcripts/{"-".join([filename.split("-")[0], filename.split("-")[1], filename.split("-")[2]])}/{filename}'
        }
    ]
    response = distillery.archivessnake_post(
        "/repositories/2/digital_object_components", digital_object_component
    )
    logger.info(f'✳️  DIGITAL OBJECT COMPONENT CREATED: {response.json()["uri"]}')


def clone_git_repository():
    repo_dir = tempfile.mkdtemp()
    git_cmd = sh.Command(config("WORK_GIT_CMD"))
    # use a specific ssh identity_file when cloning this repository
    git_cmd(
        "clone",
        f'git@github.com:{config("OH_REPO")}.git',
        "--depth",
        "1",
        repo_dir,
        _env={"GIT_SSH_COMMAND": f'ssh -i {config("OH_REPO_SSH_KEY")}'},
    )
    logger.info(f"☑️  GIT REPOSITORY CLONED TO TEMPORARY DIRECTORY: {repo_dir}")
    # set the ssh identity_file to use with this repository
    git_cmd(
        "-C",
        repo_dir,
        "config",
        "core.sshCommand",
        f'ssh -i {config("OH_REPO_SSH_KEY")}',
    )
    return repo_dir


def create_metadata_file(transcript_dir):
    archival_object = distillery.get_folder_data(transcript_dir.stem)
    metadata = {"title": archival_object["title"]}
    metadata["component_id"] = archival_object["component_id"]
    metadata["archival_object_uri"] = archival_object["uri"]
    if archival_object.get("dates"):
        metadata["dates"] = []
        for date in archival_object["dates"]:
            if date["date_type"] == "single":
                metadata["dates"].append(date["begin"])
    if archival_object.get("linked_agents"):
        for linked_agent in archival_object["linked_agents"]:
            if linked_agent.get("relator") == "ive":
                agent = distillery.archivessnake_get(linked_agent["ref"]).json()
                # TODO allow for other than inverted names
                if agent["display_name"]["name_order"] == "inverted":
                    metadata[
                        "interviewee"
                    ] = f'{agent["display_name"]["rest_of_name"]} {agent["display_name"]["primary_name"]}'
            if linked_agent.get("relator") == "ivr":
                agent = distillery.archivessnake_get(linked_agent["ref"]).json()
                # TODO allow for other than inverted names
                if agent["display_name"]["name_order"] == "inverted":
                    metadata[
                        "interviewer"
                    ] = f'{agent["display_name"]["rest_of_name"]} {agent["display_name"]["primary_name"]}'
    if archival_object.get("notes"):
        for note in archival_object["notes"]:
            if note["type"] == "abstract":
                # NOTE only using the first abstract content field
                metadata["abstract"] = note["content"][0].replace(r"\\n", r"\n")
    with open(transcript_dir.joinpath("metadata.json"), "w") as f:
        f.write(json.dumps(metadata))
    logger.info(
        f'☑️  METADATA FILE CREATED: {transcript_dir.joinpath("metadata.json")}'
    )
    return metadata


def convert_word_to_markdown(docxfile, transcript_dir):
    # TODO account for _closed versions
    pandoc_cmd = sh.Command(config("WORK_PANDOC_CMD"))
    pandoc_cmd(
        "--standalone",
        "--table-of-contents",
        f'--metadata-file={transcript_dir.joinpath("metadata.json")}',
        f'--output={transcript_dir.joinpath(f"{transcript_dir.stem}-TS.md")}',
        docxfile,
    )
    logger.info(
        f'☑️  WORD FILE CONVERTED TO MARKDOWN: {transcript_dir.joinpath(f"{transcript_dir.stem}-TS.md")}'
    )


def push_markdown_file(transcript_dir):
    git_cmd = sh.Command(config("WORK_GIT_CMD"))
    git_cmd(
        "-C",
        transcript_dir.parent.parent,
        "add",
        transcript_dir.joinpath(f"{transcript_dir.stem}-TS.md"),
    )
    diff = git_cmd(
        "-C",
        transcript_dir.parent.parent,
        "diff-index",
        "HEAD",
        "--",
    )
    if diff:
        if config("OH_REPO_GIT_EMAIL", default="") and config(
            "OH_REPO_GIT_NAME", default=""
        ):
            git_cmd(
                "-C",
                transcript_dir.parent.parent,
                "config",
                "user.email",
                config("OH_REPO_GIT_EMAIL"),
            )
            git_cmd(
                "-C",
                transcript_dir.parent.parent,
                "config",
                "user.name",
                config("OH_REPO_GIT_NAME"),
            )
        git_cmd(
            "-C",
            transcript_dir.parent.parent,
            "commit",
            "-m",
            f"add {transcript_dir.stem}-TS.md converted from docx",
        )
        git_cmd("-C", transcript_dir.parent.parent, "push", "origin", "main")
        logger.info(
            f'☑️  TRANSCRIPT PUSHED TO GITHUB: https://github.com/{config("OH_REPO")}/blob/main/{transcript_dir.stem}/{transcript_dir.stem}-TS.md'
        )
    else:
        logger.warning(f"⚠️  NO CHANGES DETECTED: {transcript_dir.stem}-TS.md")


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
