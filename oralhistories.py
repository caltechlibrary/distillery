import json
import logging
import os
import sh
import shutil
import tempfile
import urllib.parse

from datetime import datetime
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
    component_id: ("Component Unique Identifier from ArchivesSpace", "option", "i"),  # type: ignore
    update: ("Update metadata from ArchivesSpace", "flag", "u"),  # type: ignore
    publish: ("Initiate publishing to web", "flag", "p"),  # type: ignore
):
    # update workflow files
    repo_dir = clone_git_repository()
    shutil.copytree(
        Path(__file__).parent.joinpath("oralhistories"),
        Path(repo_dir).joinpath(".github", "workflows"),
        dirs_exist_ok=True,
    )
    git_cmd = sh.Command(config("WORK_GIT_CMD"))
    git_cmd(
        "-C",
        repo_dir,
        "add",
        "-A",
    )
    if docxfile:
        transcript_dir = Path(repo_dir).joinpath("transcripts", Path(docxfile).stem)
        os.makedirs(transcript_dir, exist_ok=True)
        metadata = create_metadata_file(transcript_dir)
        convert_word_to_markdown(docxfile, transcript_dir)
        os.remove(transcript_dir.joinpath("metadata.json"))
        push_markdown_file(transcript_dir)
        digital_object_uri = create_digital_object(metadata)
        create_digital_object_component(
            digital_object_uri,
            "Markdown",
            metadata["component_id"],
            f'{metadata["component_id"]}.md',
        )
    if publish:
        if component_id:
            # publish a single record
            transcript_source = Path(repo_dir).joinpath("transcripts", component_id)
            bucket_destination = (
                f's3://{config("ORALHISTORIES_BUCKET")}/{component_id}/'
            )
            s3sync_output = publish_transcripts(transcript_source, bucket_destination)
        else:
            # publish all records (example case: interviewer name change)
            transcript_source = Path(repo_dir).joinpath("transcripts")
            bucket_destination = f's3://{config("ORALHISTORIES_BUCKET")}'
            s3sync_output = publish_transcripts(transcript_source, bucket_destination)
        # tag latest commit as published
        tagname = f'published/{datetime.now().strftime("%Y-%m-%d.%H%M%S")}'
        git_cmd("-C", repo_dir, "tag", tagname)
        git_cmd("-C", repo_dir, "push", "origin", tagname)
        # update ArchivesSpace records
        for line in s3sync_output.splitlines():
            logger.info(f"line: {line}")
            # look for the digital_object
            digital_object_uri = distillery.find_digital_object(
                f'{line.split("/")[-2]}'
            )
            if not digital_object_uri:
                logger.warning(f'⚠️  DIGITAL OBJECT NOT FOUND: {line.split("/")[-2]}')
                continue
            digital_object = distillery.archivessnake_get(digital_object_uri).json()
            if line.split()[0] == "upload:":
                if line.split(".")[-1] == "html":
                    # add file_version to digital_object
                    file_versions = [
                        file_version["file_uri"]
                        for file_version in digital_object["file_versions"]
                    ]
                    if line.split()[-1] not in file_versions:
                        base_url = f'https://{config("ORALHISTORIES_BUCKET")}.s3.us-west-2.amazonaws.com'
                        file_uri = (
                            f'{base_url}/{line.split()[-1].split("/", maxsplit=3)[-1]}'
                        )
                        file_version = {"file_uri": file_uri, "publish": True}
                        digital_object["publish"] = True
                        digital_object["file_versions"].append(file_version)
                        distillery.archivessnake_post(
                            digital_object_uri, digital_object
                        )
                        logger.info(
                            f"☑️  DIGITAL OBJECT FILE VERSION ADDED: {line.split()[-1]}"
                        )
                        logger.info(
                            f'☑️  DIGITAL OBJECT PUBLISHED: {line.split()[-1].split("/")[-2]}'
                        )
                    else:
                        logger.info(
                            f"ℹ️  EXISTING DIGITAL OBJECT FILE VERSION FOUND: {line.split()[-1]}"
                        )
                    # set a redirect in the resolver
                    set_resolver_redirect(
                        f'archives/{line.split("/")[-1].split(".")[0]}', file_uri
                    )
                else:
                    # look for an existing digital_object_component
                    digital_object_component_uri = find_digital_object_component(
                        f'{line.split("/")[-1]}'
                    )
                    if digital_object_component_uri:
                        logger.info(
                            f'ℹ️  EXISTING DIGITAL OBJECT COMPONENT FOUND: {line.split("/")[-1]}'
                        )
                        # no updates needed for existing record
                        continue
                    # create new digital_object_component
                    if line.split(".")[-1] == "pdf":
                        label = "PDF Asset"
                    else:
                        label = f'{line.rsplit(".")[-1].upper()} Asset: {line.split("/")[-1].rsplit(".", maxsplit=1)[0].split("-", maxsplit=3)[-1]}'
                    create_digital_object_component(
                        digital_object_uri,
                        label,
                        line.split("/")[-2],
                        line.split("/")[-1],
                    )
            if line.split()[0] == "delete:":
                if line.split(".")[-1] == "html":
                    # remove file_version from digital_object
                    file_versions = [
                        file_version
                        for file_version in digital_object["file_versions"]
                        if not (file_version["file_uri"] == line.split()[-1])
                    ]
                    digital_object["publish"] = False
                    digital_object["file_versions"] = file_versions
                    distillery.archivessnake_post(digital_object_uri, digital_object)
                    logger.info(
                        f'☑️  DIGITAL OBJECT UNPUBLISHED: {line.split("/")[-2]}'
                    )
                    logger.info(
                        f"🔥 DIGITAL OBJECT FILE VERSION DELETED: {line.split()[-1]}"
                    )
                    # TODO determine if resolver entry should be deleted
                else:
                    # look for an existing digital_object_component
                    digital_object_component_uri = find_digital_object_component(
                        f'{line.split("/")[-1]}'
                    )
                    if digital_object_component_uri:
                        # delete the digital_object_component
                        distillery.archivessnake_delete(digital_object_component_uri)
                        logger.info(
                            f'🔥 DIGITAL OBJECT COMPONENT DELETED: {line.split("/")[-1]}'
                        )
    if update:
        if component_id:
            # update a single record
            transcript_dir = Path(repo_dir).joinpath("transcripts", component_id)
            update_markdown_metadata(transcript_dir)
        else:
            # update all records (example case: interviewer name change)
            transcript_directories = [
                i
                for i in Path(repo_dir).joinpath("transcripts").iterdir()
                if i.is_dir()
            ]
            for transcript_dir in transcript_directories:
                update_markdown_metadata(transcript_dir)
        add_commit_push(repo_dir, component_id, update)
    # cleanup
    shutil.rmtree(repo_dir)


def add_commit_push(repo_dir, component_id="", update=False):
    git_cmd = sh.Command(config("WORK_GIT_CMD"))
    git_cmd(
        "-C",
        repo_dir,
        "add",
        "-A",
    )
    diff = git_cmd(
        "-C",
        repo_dir,
        "diff-index",
        "HEAD",
        "--",
    )
    if diff:
        if config("ORALHISTORIES_GIT_USER_EMAIL", default="") and config(
            "ORALHISTORIES_GIT_USER_NAME", default=""
        ):
            git_cmd(
                "-C",
                repo_dir,
                "config",
                "user.email",
                config("ORALHISTORIES_GIT_USER_EMAIL"),
            )
            git_cmd(
                "-C",
                repo_dir,
                "config",
                "user.name",
                config("ORALHISTORIES_GIT_USER_NAME"),
            )
        if component_id:
            if update:
                commit_msg = f"update {component_id}.md metadata"
            else:
                commit_msg = f"add {component_id}.md converted from docx"
        elif ".github/workflows/" in diff and "transcripts/" not in diff:
            commit_msg = "update workflow files"
        else:
            commit_msg = "bulk update metadata"
        git_cmd(
            "-C",
            repo_dir,
            "commit",
            "-m",
            commit_msg,
        )
        git_cmd("-C", repo_dir, "push", "origin", "main")
        hash = git_cmd(
            "-C",
            repo_dir,
            "log",
            "--max-count=1",
            "--format=format:'%H'",
            _tty_out=False,
        ).strip("'")
        logger.info(
            f'☑️  CHANGES PUSHED TO GITHUB: https://github.com/{config("ORALHISTORIES_GITHUB_REPO")}/commit/{hash}'
        )
    else:
        logger.warning(f"⚠️  NO CHANGES DETECTED")


def update_markdown_metadata(transcript_dir):
    create_metadata_file(transcript_dir)
    # TODO account for _closed versions
    pandoc_cmd = sh.Command(config("WORK_PANDOC_CMD"))
    # create a fragment without metadata but with table of contents
    pandoc_cmd(
        "--from",
        "markdown",
        "--to",
        "markdown",
        f'--output={transcript_dir.joinpath(f"fragment.md")}',
        transcript_dir.joinpath(f"{transcript_dir.stem}.md"),
    )
    # add updated metadata to markdown fragment
    pandoc_cmd(
        "--standalone",
        f'--metadata-file={transcript_dir.joinpath("metadata.json")}',
        "--from",
        "markdown",
        "--to",
        "markdown",
        f'--output={transcript_dir.joinpath(f"{transcript_dir.stem}.md")}',
        transcript_dir.joinpath(f"fragment.md"),
    )
    os.remove(transcript_dir.joinpath("metadata.json"))
    os.remove(transcript_dir.joinpath("fragment.md"))
    logger.info(
        f'☑️  MARKDOWN METADATA UPDATED: {transcript_dir.joinpath(f"{transcript_dir.stem}.md")}'
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
    digital_object["digital_object_id"] = f'{metadata["component_id"]}'  # required
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


def create_digital_object_component(digital_object_uri, label, fileparent, filename):
    digital_object_component = {"digital_object": {"ref": digital_object_uri}}
    digital_object_component["label"] = label
    digital_object_component["component_id"] = filename
    digital_object_component["file_versions"] = [
        {
            "file_uri": f'https://github.com/{config("ORALHISTORIES_GITHUB_REPO")}/blob/main/transcripts/{fileparent}/{urllib.parse.quote(filename)}'
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
        f'git@github.com:{config("ORALHISTORIES_GITHUB_REPO")}.git',
        "--depth",
        "1",
        repo_dir,
        _env={"GIT_SSH_COMMAND": f'ssh -i {config("ORALHISTORIES_GITHUB_SSH_KEY")}'},
    )
    logger.info(f"☑️  GIT REPOSITORY CLONED TO TEMPORARY DIRECTORY: {repo_dir}")
    # set the ssh identity_file to use with this repository
    git_cmd(
        "-C",
        repo_dir,
        "config",
        "core.sshCommand",
        f'ssh -i {config("ORALHISTORIES_GITHUB_SSH_KEY")}',
    )
    return repo_dir


def create_metadata_file(transcript_dir):
    """Create a metadata.json file and return a dictionary.

    :param transcript_dir: Path to the directory containing the
        transcript.
    :type transcript_dir: pathlib.Path
    :return: Contents of the metadata.json file.
    :rtype: dict
    """

    archival_object = distillery.get_folder_data(transcript_dir.stem)
    metadata = {"title": archival_object["title"]}
    metadata["component_id"] = archival_object["component_id"]
    metadata["archival_object_uri"] = archival_object["uri"]
    metadata["bucket"] = config("ORALHISTORIES_BUCKET")
    metadata["archivesspace_public_url"] = config("ASPACE_PUBLIC_URL").rstrip("/")
    if archival_object.get("dates"):
        dates = {}
        for date in archival_object["dates"]:
            if date["date_type"] == "single":
                # key: YYYY-MM-DD, value: Month D, YYYY
                dates[date["begin"]] = (
                    datetime.strptime(date["begin"], "%Y-%m-%d")
                    .strftime("%B %d, %Y")
                    .replace(" 0", " ")
                )
            else:
                logger.warning(
                    f'⚠️  NON-SINGLE DATE TYPE FOUND: {archival_object["component_id"]}'
                )
        if int(sorted(dates)[-1][:4]) - int(sorted(dates)[0][:4]) == 0:
            metadata["date_summary"] = sorted(dates)[0][:4]
        else:
            metadata[
                "date_summary"
            ] = f"{sorted(dates)[0][:4]} to {sorted(dates)[-1][:4]}"
        # sort dates by key (YYYY-MM-DD) and get values (Month D, YYYY)
        metadata["dates"] = [value for key, value in sorted(dates.items())]
    if archival_object.get("linked_agents"):
        for linked_agent in archival_object["linked_agents"]:
            if linked_agent.get("relator") == "ive":
                agent = distillery.archivessnake_get(linked_agent["ref"]).json()
                # TODO [allow for other than inverted names](https://github.com/caltechlibrary/distillery/issues/24)
                if agent["display_name"]["name_order"] == "inverted":
                    metadata[
                        "interviewee"
                    ] = f'{agent["display_name"]["rest_of_name"]} {agent["display_name"]["primary_name"]}'
            if linked_agent.get("relator") == "ivr":
                agent = distillery.archivessnake_get(linked_agent["ref"]).json()
                # TODO [allow for other than inverted names](https://github.com/caltechlibrary/distillery/issues/24)
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
        f'--metadata-file={transcript_dir.joinpath("metadata.json")}',
        f'--output={transcript_dir.joinpath(f"{transcript_dir.stem}.md")}',
        docxfile,
    )
    logger.info(
        f'☑️  WORD FILE CONVERTED TO MARKDOWN: {transcript_dir.joinpath(f"{transcript_dir.stem}.md")}'
    )


def publish_transcripts(transcript_source, bucket_destination):
    # publish transcript files to S3
    aws_cmd = sh.Command(config("WORK_AWS_CMD"))
    return aws_cmd(
        "s3",
        "sync",
        transcript_source,
        bucket_destination,
        "--exclude",
        "*.md",
        "--delete",
        "--no-progress",
        _env={
            "AWS_ACCESS_KEY_ID": config("DISTILLERY_AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": config("DISTILLERY_AWS_SECRET_ACCESS_KEY"),
        },
    )


def set_resolver_redirect(resolver_id, redirect_url):
    """Create or update a redirect entry in the S3 bucket."""
    aws_cmd = sh.Command(config("WORK_AWS_CMD"))
    logger.info(
        f'☑️  RESOLVER REDIRECT SET: s3://{config("RESOLVER_BUCKET")}/{resolver_id} ➡️  {redirect_url}'
    )
    return aws_cmd(
        "s3api",
        "put-object",
        "--acl",
        "public-read",
        "--bucket",
        config("RESOLVER_BUCKET"),
        "--key",
        resolver_id,
        "--website-redirect-location",
        redirect_url,
        _env={
            "AWS_ACCESS_KEY_ID": config("DISTILLERY_AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": config("DISTILLERY_AWS_SECRET_ACCESS_KEY"),
        },
    )


def push_markdown_file(transcript_dir):
    git_cmd = sh.Command(config("WORK_GIT_CMD"))
    git_cmd(
        "-C",
        transcript_dir.parent.parent,
        "add",
        transcript_dir.joinpath(f"{transcript_dir.stem}.md"),
    )
    diff = git_cmd(
        "-C",
        transcript_dir.parent.parent,
        "diff-index",
        "HEAD",
        "--",
    )
    if diff:
        if config("ORALHISTORIES_GIT_USER_EMAIL", default="") and config(
            "ORALHISTORIES_GIT_USER_NAME", default=""
        ):
            git_cmd(
                "-C",
                transcript_dir.parent.parent,
                "config",
                "user.email",
                config("ORALHISTORIES_GIT_USER_EMAIL"),
            )
            git_cmd(
                "-C",
                transcript_dir.parent.parent,
                "config",
                "user.name",
                config("ORALHISTORIES_GIT_USER_NAME"),
            )
        git_cmd(
            "-C",
            transcript_dir.parent.parent,
            "commit",
            "-m",
            f"add {transcript_dir.stem}.md converted from docx",
        )
        git_cmd("-C", transcript_dir.parent.parent, "push", "origin", "main")
        logger.info(
            f'☑️  TRANSCRIPT PUSHED TO GITHUB: https://github.com/{config("ORALHISTORIES_GITHUB_REPO")}/blob/main/{transcript_dir.stem}/{transcript_dir.stem}.md'
        )
    else:
        logger.warning(f"⚠️  NO CHANGES DETECTED: {transcript_dir.stem}.md")


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
