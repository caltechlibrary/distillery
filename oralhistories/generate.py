import os
import subprocess
import sys


def main(file):
    print(f"🐞 file: {file}")
    # TODO think through handling of _closed files
    if file.startswith("transcripts/"):
        # file_segments[1] will be component_id
        file_segments = file.split("/")
        if os.path.isfile(f"build/{file_segments[1]}/{file_segments[1]}.html"):
            print(f"🐞 file exists: build/{file_segments[1]}/{file_segments[1]}.html")
            return
        if file.endswith(".md") and not os.path.isfile(file):
            print(f"🐞 file deleted: {file}")
            os.remove(f"{os.path.splitext(file)[0]}.html")
            os.remove(f"{os.path.splitext(file)[0]}.pdf")
            return
        generate_files(file_segments[1])


def generate_files(identifier):
    os.makedirs(f"build/{identifier}", exist_ok=True)
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
            f"--output=build/{identifier}/{identifier}.html",
            f"transcripts/{identifier}/{identifier}.md",
        ]
    )
    print(f"🐞 file generated: build/{identifier}/{identifier}.html")
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


if __name__ == "__main__":
    main(sys.argv[1])
