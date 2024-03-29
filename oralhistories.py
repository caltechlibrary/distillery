import json
import logging
import os
import sh
import shutil
import tempfile
import urllib.parse

import markdown
import rpyc

from datetime import datetime
from pathlib import Path

from decouple import config  # pypi: python-decouple
from markdown_link_attr_modifier import (
    LinkAttrModifierExtension,
)  # pypi: markdown-link-attr-modifier

import distillery

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    Path(__file__)
    .resolve()
    .parent.joinpath("settings.ini")
)
logger = logging.getLogger("oralhistories")


@rpyc.service
class OralHistoriesService(rpyc.Service):
    @rpyc.exposed
    def run(self, component_id="", update=False, publish=False, logfile=""):
        if component_id:
            self.status_logger = logging.getLogger(component_id)
        else:
            self.status_logger = logging.getLogger("_")
        self.status_logger.setLevel(logging.INFO)
        status_handler = logging.FileHandler(logfile)
        status_handler.setLevel(logging.INFO)
        status_handler.setFormatter(StatusFormatter("%(message)s"))
        self.status_logger.addHandler(status_handler)

        self.tmp_oralhistories_repository = self.clone_oralhistories_repository()
        # update github workflow files
        self.copy_github_workflow_changes()
        self.add_commit_push()
        # ASSUMPTION: a DOCX file is provided
        if component_id and not update and not publish:
            self.status_logger.info(f"☑️  received **{component_id}.docx** file")
            self.component_id = component_id
            self.archival_object = distillery.find_archival_object(self.component_id)
            self.metadata = self.create_metadata(archival_object=self.archival_object)
            self.transcript_directory = self.create_metadata_file()
            self.convert_word_to_markdown()
            os.remove(self.transcript_directory.joinpath("metadata.json"))
            os.remove(
                f'{Path(config("ORALHISTORIES_WORK_UPLOADS")).joinpath(f"{self.component_id}.docx")}'
            )
            self.add_commit_push(self.component_id)
            self.status_logger.info(
                f'☑️  pushed [**{component_id}.md** file](https://github.com/{config("ORALHISTORIES_GITHUB_REPO")}/blob/main/transcripts/{component_id}/{component_id}.md) to GitHub'
            )
            self.digital_object_uri = self.create_digital_object()
            self.create_digital_object_component(
                "Markdown", self.component_id, f"{self.component_id}.md"
            )
            self.status_logger.info(
                f'☑️  created [**{component_id}** Digital Object record]({config("ASPACE_STAFF_URL")}/resolve/readonly?uri={self.digital_object_uri}) in ArchivesSpace'
            )
        if publish:
            if component_id:
                # publish a single record
                self.transcript_source_directory = Path(
                    self.tmp_oralhistories_repository
                ).joinpath("transcripts", component_id)
                self.bucket_destination = "s3://{}/{}/{}".format(
                    config("ORALHISTORIES_BUCKET"),
                    config("ORALHISTORIES_URL_PATH_PREFIX"),
                    component_id,
                )
                s3sync_output = self.publish_transcripts()
                self.status_logger.info(
                    "☑️  published [**{}** transcript]({}/{}/{}) to the web".format(
                        component_id,
                        config("ALCHEMIST_BASE_URL").rstrip("/"),
                        config("ORALHISTORIES_URL_PATH_PREFIX"),
                        component_id,
                    )
                )
            else:
                # (re)publish all records (example case: interviewer name change)
                self.transcript_source_directory = Path(
                    self.tmp_oralhistories_repository
                ).joinpath("transcripts")
                self.bucket_destination = f's3://{config("ORALHISTORIES_BUCKET")}'
                s3sync_output = self.publish_transcripts()
                self.status_logger.info("☑️ (re)published all transcripts to the web")
            # tag latest commit as published
            tagname = f'published/{datetime.now().strftime("%Y-%m-%d.%H%M%S")}'
            git_cmd = sh.Command(config("WORK_GIT_CMD"))
            git_cmd("-C", self.tmp_oralhistories_repository, "tag", tagname)
            git_cmd("-C", self.tmp_oralhistories_repository, "push", "origin", tagname)
            # update ArchivesSpace records
            for line in s3sync_output.splitlines():
                logger.info(f"line: {line}")
                # look for the digital_object
                self.digital_object_uri = distillery.find_digital_object(
                    f'{line.split("/")[-2]}'
                )
                if not self.digital_object_uri:
                    logger.warning(
                        f'⚠️  DIGITAL OBJECT NOT FOUND: {line.split("/")[-2]}'
                    )
                    continue
                digital_object = distillery.archivessnake_get(
                    self.digital_object_uri
                ).json()
                base_url = "/".join(
                    [
                        config("ALCHEMIST_BASE_URL").rstrip("/"),
                        config("ORALHISTORIES_URL_PATH_PREFIX"),
                    ]
                )
                file_uri = "/".join([base_url, line.split("/")[-2]])
                if line.split()[0] == "upload:":
                    if line.split("/")[-1] == "index.html":
                        # add file_version to digital_object
                        file_versions = [
                            file_version["file_uri"]
                            for file_version in digital_object["file_versions"]
                        ]
                        if file_uri not in file_versions:
                            file_version = {"file_uri": file_uri, "publish": True}
                            digital_object["publish"] = True
                            digital_object["file_versions"].append(file_version)
                            distillery.archivessnake_post(
                                self.digital_object_uri, digital_object
                            )
                            logger.info(
                                f"☑️  DIGITAL OBJECT FILE VERSION ADDED: {file_uri}"
                            )
                            logger.info(
                                f'☑️  DIGITAL OBJECT PUBLISHED: {line.split()[-1].split("/")[-2]}'
                            )
                        else:
                            logger.info(
                                f"ℹ️  EXISTING DIGITAL OBJECT FILE VERSION FOUND: {file_uri}"
                            )
                        # set a redirect in the resolver
                        if config("RESOLVER_BUCKET", default=""):
                            self.set_resolver_redirect(
                                "{}:{}".format(
                                    config("RESOLVER_ORALHISTORIES_URL_PATH_PREFIX"),
                                    f'{line.split("/")[-2]}',
                                ),
                                file_uri,
                            )
                    else:
                        # look for an existing digital_object_component
                        digital_object_component_uri = (
                            self.find_digital_object_component(f'{line.split("/")[-1]}')
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
                        self.create_digital_object_component(
                            label, line.split("/")[-2], line.split("/")[-1]
                        )
                if line.split()[0] == "delete:":
                    if line.split(".")[-1] == "html":
                        # remove file_version from digital_object
                        file_versions = [
                            file_version
                            for file_version in digital_object["file_versions"]
                            if not (file_version["file_uri"] == file_uri)
                        ]
                        digital_object["publish"] = False
                        digital_object["file_versions"] = file_versions
                        distillery.archivessnake_post(
                            self.digital_object_uri, digital_object
                        )
                        logger.info(
                            f'☑️  DIGITAL OBJECT UNPUBLISHED: {line.split("/")[-2]}'
                        )
                        logger.info(
                            f"🔥 DIGITAL OBJECT FILE VERSION DELETED: {file_uri}"
                        )
                        # TODO determine if resolver entry should be deleted
                    else:
                        # look for an existing digital_object_component
                        digital_object_component_uri = (
                            self.find_digital_object_component(f'{line.split("/")[-1]}')
                        )
                        if digital_object_component_uri:
                            # delete the digital_object_component
                            distillery.archivessnake_delete(
                                digital_object_component_uri
                            )
                            logger.info(
                                f'🔥 DIGITAL OBJECT COMPONENT DELETED: {line.split("/")[-1]}'
                            )
            if component_id:
                if config("RESOLVER_BUCKET", default=""):
                    self.status_logger.info(
                        "☑️ created [**{}** persistant URL entry]({}/{}:{}) in resolver".format(
                            component_id,
                            config("RESOLVER_SERVICE_ENDPOINT").rstrip("/"),
                            config("RESOLVER_ORALHISTORIES_URL_PATH_PREFIX"),
                            component_id,
                        )
                    )
                self.status_logger.info(
                    "☑️ published [**{}** Digital Object record]({}/resolve/readonly?uri={}) in ArchivesSpace".format(
                        component_id,
                        config("ASPACE_STAFF_URL").rstrip("/"),
                        self.digital_object_uri,
                    )
                )
            else:
                if config("RESOLVER_BUCKET", default=""):
                    self.status_logger.info(f"☑️ (re)published all resolver links")
                self.status_logger.info(
                    "☑️ (re)published all Digital Object records in ArchivesSpace"
                )
        if update:
            if component_id:
                # update a single record
                self.component_id = component_id
                self.transcript_directory = Path(
                    self.tmp_oralhistories_repository
                ).joinpath("transcripts", component_id)
                self.update_markdown_metadata()
            else:
                # update all records (example case: interviewer name change)
                transcript_directories = [
                    i
                    for i in Path(self.tmp_oralhistories_repository)
                    .joinpath("transcripts")
                    .iterdir()
                    if i.is_dir()
                ]
                for self.transcript_directory in transcript_directories:
                    self.component_id = self.transcript_directory.name
                    self.update_markdown_metadata()
            # NOTE use component_id instead of self.component_id because
            # component_id can be an empty string when updating all records
            self.add_commit_push(component_id, update)
            if component_id:
                self.status_logger.info(
                    f'☑️ updated [**{component_id}** metadata](https://github.com/{config("ORALHISTORIES_GITHUB_REPO")}/blob/main/transcripts/{component_id}/{component_id}.md) in GitHub'
                )
            else:
                self.status_logger.info("☑️ updated all metadata in GitHub")
        # cleanup
        shutil.rmtree(self.tmp_oralhistories_repository)

        # send the character that stops javascript reloading in the web ui
        self.status_logger.info("🏁")

    def clone_oralhistories_repository(self):
        tmp_oralhistories_repository = tempfile.mkdtemp()
        git_cmd = sh.Command(config("WORK_GIT_CMD"))
        # use a specific ssh identity_file when cloning this repository
        git_cmd(
            "clone",
            f'git@github.com:{config("ORALHISTORIES_GITHUB_REPO")}.git',
            "--depth",
            "1",
            tmp_oralhistories_repository,
            _env={
                "GIT_SSH_COMMAND": f'ssh -i {config("ORALHISTORIES_GITHUB_SSH_KEY")}'
            },
        )
        logger.info(
            f"☑️  GIT REPOSITORY CLONED TO TEMPORARY DIRECTORY: {tmp_oralhistories_repository}"
        )
        # set the ssh identity_file to use with this repository
        git_cmd(
            "-C",
            tmp_oralhistories_repository,
            "config",
            "core.sshCommand",
            f'ssh -i {config("ORALHISTORIES_GITHUB_SSH_KEY")}',
        )
        return tmp_oralhistories_repository

    def copy_github_workflow_changes(self):
        shutil.copytree(
            Path(__file__).parent.joinpath("oralhistories"),
            Path(self.tmp_oralhistories_repository).joinpath(".github", "workflows"),
            dirs_exist_ok=True,
        )

    def add_commit_push(self, component_id="", update=False):
        git_cmd = sh.Command(config("WORK_GIT_CMD"))
        git_cmd("-C", self.tmp_oralhistories_repository, "add", "-A")
        diff = git_cmd(
            "-C", self.tmp_oralhistories_repository, "diff-index", "HEAD", "--"
        )
        if diff:
            git_cmd(
                "-C",
                self.tmp_oralhistories_repository,
                "config",
                "user.email",
                config("ORALHISTORIES_GIT_USER_EMAIL"),
            )
            git_cmd(
                "-C",
                self.tmp_oralhistories_repository,
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
            git_cmd("-C", self.tmp_oralhistories_repository, "commit", "-m", commit_msg)
            git_cmd("-C", self.tmp_oralhistories_repository, "push", "origin", "main")
            hash = git_cmd(
                "-C",
                self.tmp_oralhistories_repository,
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

    def create_metadata(self, archival_object):
        metadata = {"title": archival_object["title"]}
        metadata["component_id"] = archival_object["component_id"]
        metadata["archival_object_uri"] = archival_object["uri"]
        metadata["resolver_url"] = "{}/{}:{}".format(
            config("RESOLVER_SERVICE_ENDPOINT").rstrip("/"),
            config("RESOLVER_ORALHISTORIES_URL_PATH_PREFIX"),
            archival_object["component_id"],
        )
        metadata["archivesspace_public_url"] = config("ASPACE_PUBLIC_URL").rstrip("/")
        if archival_object.get("dates"):
            dates = {}
            for date in archival_object["dates"]:
                if date["label"] == "creation":
                    if date["date_type"] == "single":
                        # key: ISO 8601 date, value: formatted date
                        if len(date["begin"]) == 4:
                            dates[date["begin"]] = date["begin"]
                        elif len(date["begin"]) == 7:
                            dates[date["begin"]] = datetime.strptime(
                                date["begin"], "%Y-%m"
                            ).strftime("%B %Y")
                        elif len(date["begin"]) == 10:
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
                    # NOTE [add a newline to the end of the JSON string literal](https://github.com/jgm/pandoc/issues/8502)
                    metadata["abstract"] = f'{note["content"][0].strip()}\n'
        return metadata

    def create_metadata_file(self):
        transcript_directory = Path(self.tmp_oralhistories_repository).joinpath(
            "transcripts", self.component_id
        )
        os.makedirs(transcript_directory, exist_ok=True)
        with open(transcript_directory.joinpath("metadata.json"), "w") as f:
            f.write(json.dumps(self.metadata))
        logger.info(
            f'☑️  METADATA FILE CREATED: {transcript_directory.joinpath("metadata.json")}'
        )
        return transcript_directory

    def convert_word_to_markdown(self):
        # TODO account for _closed versions
        pandoc_cmd = sh.Command(config("WORK_PANDOC_CMD"))
        pandoc_cmd(
            "--standalone",
            f'--metadata-file={self.transcript_directory.joinpath("metadata.json")}',
            # explicitly set the title so Word Document metadata is not used
            "--metadata",
            f'title={self.metadata["title"]}',
            f'--output={self.transcript_directory.joinpath(f"{self.component_id}.md")}',
            f'{Path(config("ORALHISTORIES_WORK_UPLOADS")).joinpath(f"{self.component_id}.docx")}',
        )
        logger.info(
            f'☑️  WORD FILE CONVERTED TO MARKDOWN: {self.transcript_directory.joinpath(f"{self.component_id}.md")}'
        )

    def create_digital_object(self):
        digital_object = {}
        digital_object[
            "digital_object_id"
        ] = f'{self.metadata["component_id"]}'  # required
        digital_object["title"] = f'{self.metadata["title"]} Transcript'  # required
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
            self.metadata["archival_object_uri"]
        )
        archival_object = archival_object_get_response.json()
        # add digital object instance to archival object
        archival_object["instances"].append(digital_object_instance)
        # post updated archival object
        archival_object_post_response = distillery.archivessnake_post(
            self.metadata["archival_object_uri"], archival_object
        )
        logger.info(
            f'☑️  ARCHIVAL OBJECT UPDATED: {archival_object_post_response.json()["uri"]}'
        )
        return digital_object_post_response.json()["uri"]

    def create_digital_object_component(self, label, fileparent, filename):
        digital_object_component = {"digital_object": {"ref": self.digital_object_uri}}
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

    def publish_transcripts(self):
        # publish transcript files to S3
        aws_cmd = sh.Command(config("WORK_AWS_CMD"))
        try:
            s3sync_output = aws_cmd(
                "s3",
                "sync",
                self.transcript_source_directory,
                self.bucket_destination,
                "--exclude",
                "*.md",
                "--delete",
                "--no-progress",
                _env={
                    "AWS_ACCESS_KEY_ID": config("DISTILLERY_AWS_ACCESS_KEY_ID"),
                    "AWS_SECRET_ACCESS_KEY": config("DISTILLERY_AWS_SECRET_ACCESS_KEY"),
                },
            )
            return s3sync_output
        except Exception as e:
            message = f'❌  S3 SYNC ERROR: {str(e.stderr, "utf-8")}'
            logger.error(message)
            self.status_logger.error(message)
            raise e

    def set_resolver_redirect(self, resolver_id, redirect_url):
        """Create or update a redirect entry in the S3 bucket."""
        aws_cmd = sh.Command(config("WORK_AWS_CMD"))
        logger.info(
            f'☑️  RESOLVER REDIRECT SET: s3://{config("RESOLVER_BUCKET")}/{resolver_id} ➡️  {redirect_url}'
        )
        # TODO reveal errors from the aws_cmd
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

    def find_digital_object_component(self, digital_object_component_component_id):
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

    def update_markdown_metadata(self):
        self.archival_object = distillery.find_archival_object(self.component_id)
        self.metadata = self.create_metadata(archival_object=self.archival_object)
        self.create_metadata_file()
        # TODO account for _closed versions
        pandoc_cmd = sh.Command(config("WORK_PANDOC_CMD"))
        # create a fragment without metadata but with table of contents
        pandoc_cmd(
            "--from",
            "markdown",
            "--to",
            "markdown",
            f'--output={self.transcript_directory.joinpath(f"fragment.md")}',
            self.transcript_directory.joinpath(f"{self.transcript_directory.name}.md"),
        )
        # add updated metadata to markdown fragment
        pandoc_cmd(
            "--standalone",
            f'--metadata-file={self.transcript_directory.joinpath("metadata.json")}',
            "--from",
            "markdown",
            "--to",
            "markdown",
            f'--output={self.transcript_directory.joinpath(f"{self.transcript_directory.name}.md")}',
            self.transcript_directory.joinpath(f"fragment.md"),
        )
        os.remove(self.transcript_directory.joinpath("metadata.json"))
        os.remove(self.transcript_directory.joinpath("fragment.md"))
        logger.info(
            f'☑️  MARKDOWN METADATA UPDATED: {self.transcript_directory.joinpath(f"{self.transcript_directory.name}.md")}'
        )


class StatusFormatter(logging.Formatter):
    def format(self, record):
        """Output markdown status messages as HTML5."""
        return markdown.markdown(
            super().format(record),
            output_format="html5",
            extensions=[LinkAttrModifierExtension(new_tab="on")],
        )


if __name__ == "__main__":
    # fmt: off
    from rpyc.utils.server import ThreadedServer
    ThreadedServer(OralHistoriesService, port=config("ORALHISTORIES_RPYC_PORT")).start()
