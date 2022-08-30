import os
import pathlib
import sh
import shutil
import tempfile

from decouple import config  # pypi: python-decouple


def main(
    docxfile: ("Word file to convert to Markdown", "option", "w"),  # type: ignore
):
    if docxfile:
        repodir = clone_git_repository()
        component_id = convert_word_to_markdown(docxfile, repodir)
        push_markdown_file(component_id, repodir)


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


def convert_word_to_markdown(docxfile, repodir):
    if not shutil.which("pandoc"):
        raise RuntimeError("pandoc executable not found")
    component_id = pathlib.Path(docxfile).stem
    mdfilename = f"{component_id}.md"
    os.makedirs(pathlib.Path(repodir).joinpath(component_id), exist_ok=True)
    sh.pandoc(
        "--standalone",
        "--table-of-contents",
        f'--output={pathlib.Path(repodir).joinpath(component_id, f"{component_id}.md")}',
        docxfile,
    )
    return component_id


def push_markdown_file(component_id, repodir):
    if not shutil.which("git"):
        raise RuntimeError("git executable not found")
    sh.git(
        "-C",
        repodir,
        "add",
        pathlib.Path(repodir).joinpath(component_id, f"{component_id}.md"),
    )
    sh.git("-C", repodir, "commit", "-m", f"add {component_id}.md converted from docx")
    sh.git("-C", repodir, "push", "origin", "main")


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
