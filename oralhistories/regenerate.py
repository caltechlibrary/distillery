import os
import sys

import generate


def main(identifier):
    print(f"🐞 identifier: {identifier}")
    if os.path.isfile(f"transcripts/{identifier}/{identifier}.md"):
        generate.generate_files(identifier)


if __name__ == "__main__":
    main(sys.argv[1])
