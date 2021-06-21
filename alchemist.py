# NOTE: this file is intended to be run via cron every minute
# configure /path/to/python3 in `settings.ini`

# TODO notification of errors from this file

import os
import shutil
import subprocess
from datetime import datetime
from glob import glob

from decouple import config

for f in glob(os.path.join(config("PROCESSING_FILES"), "*-init-*")):
    # using rsplit() in case the collection_id contains a - (hyphen) character
    collection_id = os.path.basename(f).rsplit("-", 2)[0]
    PYTHON_CMD = config("PYTHON_CMD")
    print(f"📅 {datetime.now()}")
    print(f"🗄 {collection_id}")
    if os.path.basename(f).split("-")[-1] == "report":
        pass
    elif os.path.basename(f).split("-")[-1] == "preservation":
        # delete the init file
        os.remove(f)
        subprocess.run(
            [
                PYTHON_CMD,
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "distill.py"),
                collection_id,
            ]
        )
    elif os.path.basename(f).split("-")[-1] == "access":
        # delete the init file
        os.remove(f)
        # move the `collection_id` directory into `STAGE_2_ORIGINAL_FILES`
        # NOTE shutil.move() in Python < 3.9 needs strings as arguments
        try:
            shutil.move(
                str(os.path.join(config("STAGE_1_ORIGINAL_FILES"), collection_id)),
                str(config("STAGE_2_ORIGINAL_FILES")),
            )
        except Exception as e:
            # TODO log a problem and notify
            raise
        subprocess.run(
            [
                PYTHON_CMD,
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    f"{config('ACCESS_PLATFORM')}.py",
                ),
                collection_id,
            ]
        )
    elif os.path.basename(f).split("-")[-1] == "preservation_access":
        # delete the init file
        os.remove(f)
        subprocess.run(
            [
                PYTHON_CMD,
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "distill.py"),
                collection_id,
            ]
        )
        subprocess.run(
            [
                PYTHON_CMD,
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    f"{config('ACCESS_PLATFORM')}.py",
                ),
                collection_id,
            ]
        )
    else:
        # TODO log a message that an unknown file was found
        pass
