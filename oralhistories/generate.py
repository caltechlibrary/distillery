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
        os.makedirs(f"build/{file_segments[1]}", exist_ok=True)
        subprocess.run(
            [
                "pandoc",
                "--standalone",
                "--shift-heading-level-by=1",
                "--table-of-contents",
                "--from=markdown",
                "--to=html",
                "--template=.github/workflows/templates/web.html",
                f"--output=build/{file_segments[1]}/{file_segments[1]}.html",
                f"transcripts/{file_segments[1]}/{file_segments[1]}.md",
            ]
        )
        print(f"🐞 file generated: build/{file_segments[1]}/{file_segments[1]}.html")
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
                f"--output=build/{file_segments[1]}/tmp.html",
                f"transcripts/{file_segments[1]}/{file_segments[1]}.md",
            ]
        )
        print(f"🐞 file generated: build/{file_segments[1]}/tmp.html")
        # create pdf with pagedjs
        subprocess.run(
            [
                "pagedjs-cli",
                f"build/{file_segments[1]}/tmp.html",
                "--output",
                f"transcripts/{file_segments[1]}/{file_segments[1]}.pdf",
            ]
        )
        print(f"🐞 file generated: build/{file_segments[1]}/{file_segments[1]}.pdf")
        os.remove(f"build/{file_segments[1]}/tmp.html")
        print(f"🐞 file deleted: build/{file_segments[1]}/tmp.html")


if __name__ == "__main__":
    main(sys.argv[1])
