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
        os.makedirs(f"build/{file_segments[1]}", exist_ok=True)
        subprocess.run(
            [
                "pandoc",
                "--standalone",
                "--from=markdown",
                "--to=html",
                "--template=.github/workflows/templates/default.html",
                f"--output=build/{file_segments[1]}/{file_segments[1]}.html",
                f"transcripts/{file_segments[1]}/{file_segments[1]}.md",
            ]
        )
        print(f"🐞 file generated: build/{file_segments[1]}/{file_segments[1]}.html")


if __name__ == "__main__":
    main(sys.argv[1])
