import json
import os
import pathlib
import sh
import shutil
import tempfile

from decouple import config  # pypi: python-decouple

import distillery


def main(
    docxfile: ("Word file to convert to Markdown", "option", "w"),  # type: ignore
):
    if docxfile:
        repodir = clone_git_repository()
        component_id = get_component_id_from_filename(docxfile)
        os.makedirs(pathlib.Path(repodir).joinpath(component_id), exist_ok=True)
        create_metadata_file(component_id, repodir)
        convert_word_to_markdown(docxfile, repodir)
        push_markdown_file(component_id, repodir)
        # cleanup
        shutil.rmtree(repodir)


def clone_git_repository():
    if not shutil.which("git"):
        raise RuntimeError("git executable not found")
    repodir = tempfile.mkdtemp()
    sh.git(
        "clone",
        f'git@github.com:{config("OH_REPO")}.git',
        "--depth",
        "1",
        repodir,
    )
    return repodir


def get_component_id_from_filename(filepath):
    # TODO account for _closed versions
    component_id = pathlib.Path(filepath).stem
    return component_id


def create_metadata_file(component_id, repodir):
    # NOTE we are creating an interstitial text file containing the YAML
    # metadata block for the final markdown file so we can control the
    # order of the metadata fields
    pathlib.Path(repodir).joinpath(component_id, "touch.txt").touch()
    metadata_template = (
        "---"
        "\n"
        "$if(title)$title: $title$$endif$"
        "\n"
        "$if(interviewee)$interviewee: $interviewee$$endif$"
        "\n"
        "$if(interviewer)$interviewer: $interviewer$$endif$"
        "\n"
        "$if(dates)$dates:"
        "\n"
        "$for(dates)$"
        "  - $dates$"
        "\n"
        "$endfor$"
        "$endif$"
        "$if(abstract)$abstract: $^$$abstract$$endif$"
        "\n"
        "---"
        "\n"
    )
    with open(pathlib.Path(repodir).joinpath(component_id, "metadata.tpl"), "w") as f:
        f.write(metadata_template)
    archival_object = distillery.get_folder_data(component_id)
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
                metadata["abstract"] = note["content"][0]
    with open(
        pathlib.Path(repodir).joinpath(component_id, f"{component_id}.json"), "w"
    ) as f:
        f.write(json.dumps(metadata))
    if not shutil.which("pandoc"):
        raise RuntimeError("pandoc executable not found")
    sh.pandoc(
        f'--metadata-file={pathlib.Path(repodir).joinpath(component_id, f"{component_id}.json")}',
        f'--template={pathlib.Path(repodir).joinpath(component_id, "metadata.tpl")}',
        f'--output={pathlib.Path(repodir).joinpath(component_id, "metadata.txt")}',
        pathlib.Path(repodir).joinpath(component_id, "touch.txt"),
    )


def convert_word_to_markdown(docxfile, repodir):
    component_id = get_component_id_from_filename(docxfile)
    # TODO account for _closed versions
    sh.pandoc(
        "--standalone",
        "--table-of-contents",
        f'--output={pathlib.Path(repodir).joinpath(component_id, "docx.md")}',
        docxfile,
    )
    # NOTE we concatenate the interstitial metadata and transcript
    with open(
        f'{pathlib.Path(repodir).joinpath(component_id, f"{component_id}.md")}', "w"
    ) as outfile:
        with open(
            pathlib.Path(repodir).joinpath(component_id, "metadata.txt"), "r"
        ) as metadata:
            shutil.copyfileobj(metadata, outfile)
            outfile.write("\n")
        with open(
            pathlib.Path(repodir).joinpath(component_id, "docx.md"), "r"
        ) as markdown:
            shutil.copyfileobj(markdown, outfile)


def push_markdown_file(component_id, repodir):
    if not shutil.which("git"):
        raise RuntimeError("git executable not found")
    sh.git(
        "-C",
        repodir,
        "add",
        pathlib.Path(repodir).joinpath(component_id, f"{component_id}.md"),
    )
    diff = sh.git(
        "-C",
        repodir,
        "diff-index",
        "HEAD",
        "--",
    )
    if diff:
        sh.git(
            "-C", repodir, "commit", "-m", f"add {component_id}.md converted from docx"
        )
        sh.git("-C", repodir, "push", "origin", "main")


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
