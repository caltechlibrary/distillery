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
        create_metadata_file(transcript_dir)
        convert_word_to_markdown(docxfile, transcript_dir)
        push_markdown_file(transcript_dir)
        # cleanup
        shutil.rmtree(repo_dir)
    if publish:
        repo_dir = clone_git_repository()
        aws_cmd = sh.Command(config("WORK_AWS_CMD"))
        aws_cmd(
            "s3",
            "sync",
            f'{Path(repo_dir).joinpath("transcripts")}',
            f's3://{config("OH_S3_BUCKET")}',
            "--exclude",
            "*.md",
            _env={
                "AWS_ACCESS_KEY_ID": config("AWS_ACCESS_KEY"),
                "AWS_SECRET_ACCESS_KEY": config("AWS_SECRET_KEY"),
            },
        )


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


def convert_word_to_markdown(docxfile, transcript_dir):
    # TODO account for _closed versions
    pandoc_cmd = sh.Command(config("WORK_PANDOC_CMD"))
    pandoc_cmd(
        "--standalone",
        "--table-of-contents",
        f'--metadata-file={transcript_dir.joinpath("metadata.json")}',
        f'--output={transcript_dir.joinpath(f"{transcript_dir.stem}.md")}',
        docxfile,
    )
    logger.info(
        f'☑️  WORD FILE CONVERTED TO MARKDOWN: {transcript_dir.joinpath(f"{transcript_dir.stem}.md")}'
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
        if config("OH_REPO_GIT_EMAIL") and config("OH_REPO_GIT_NAME"):
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
            f"add {transcript_dir.stem}.md converted from docx",
        )
        git_cmd("-C", transcript_dir.parent.parent, "push", "origin", "main")
        logger.info(
            f'☑️  TRANSCRIPT PUSHED TO GITHUB: https://github.com/{config("OH_REPO")}/blob/main/{transcript_dir.stem}/{transcript_dir.stem}.md'
        )
    else:
        logger.warning(f"⚠️  NO CHANGES DETECTED: {transcript_dir.stem}.md")


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
