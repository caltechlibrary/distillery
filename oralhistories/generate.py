import os
import subprocess
import sys


def main(file):
    print(f"🐞 file: {file}")
    if not os.path.exists(file):
        return
    # TODO think through handling of _closed files
    if file.startswith("transcripts/"):
        # file_segments[1] will be component_id
        file_segments = file.split("/")
        # skip when build artifacts already exist
        if os.path.isfile(f"build/{file_segments[1]}/index.html"):
            print(f"🐞 file exists: build/{file_segments[1]}/index.html")
            return
        # remove published files when markdown transcript is deleted
        if file.endswith(".md") and not os.path.isfile(file):
            print(f"🐞 file deleted: {file}")
            os.remove(f"index.html")
            os.remove(f"{os.path.splitext(file)[0]}.pdf")
            return
        generate_files(file_segments[1])


def generate_files(identifier):
    os.makedirs(f"build/{identifier}", exist_ok=True)
    # copy assets for build
    subprocess.run(
        [
            "rsync",
            "-a",
            "--exclude",
            "*.md",
            "--exclude",
            "*.html",
            "--exclude",
            "*.pdf",
            f"transcripts/{identifier}/",
            f"build/{identifier}/",
        ]
    )
    # create intermediate html for pagedjs
    subprocess.run(
        [
            "pandoc",
            "--standalone",
            "--shift-heading-level-by=1",
            "--table-of-contents",
            "--from=markdown",
            "--to=html",
            "--template=.github/workflows/templates/pdf.html",
            f"--output=build/{identifier}/tmp.html",
            f"transcripts/{identifier}/{identifier}.md",
        ]
    )
    print(f"🐞 file generated: build/{identifier}/tmp.html")
    # create pdf with pagedjs
    subprocess.run(
        [
            "pagedjs-cli",
            f"build/{identifier}/tmp.html",
            "--output",
            f"transcripts/{identifier}/{identifier}.pdf",
        ]
    )
    print(f"🐞 file generated: build/{identifier}/{identifier}.pdf")
    os.remove(f"build/{identifier}/tmp.html")
    print(f"🐞 file deleted: build/{identifier}/tmp.html")
    # generate html
    subprocess.run(
        [
            "pandoc",
            "--standalone",
            "--shift-heading-level-by=1",
            "--table-of-contents",
            "--from=markdown",
            "--to=html",
            "--template=.github/workflows/templates/web.html",
            "--variable=pdf-size:{}".format(
                round(
                    os.path.getsize(f"transcripts/{identifier}/{identifier}.pdf")
                    / (1024 * 1024),
                    2,
                )
            ),
            f"--output=build/{identifier}/index.html",
            f"transcripts/{identifier}/{identifier}.md",
        ]
    )
    print(f"🐞 file generated: build/{identifier}/index.html")


if __name__ == "__main__":
    main(sys.argv[1])
