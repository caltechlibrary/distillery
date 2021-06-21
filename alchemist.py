# NOTE: this file is intended to be run via cron every minute
# configure /path/to/python3 in `settings.ini`

# TODO notification of errors from this file

import os
from datetime import datetime
from glob import glob
from subprocess import run

from decouple import config

for f in glob(os.path.join(config("PROCESSING_FILES"), "*-init-*")):
    # using rsplit() in case the collection_id contains a - (hyphen) character
    collection_id = os.path.basename(f).rsplit("-", 2)[0]
    PYTHON_CMD = config("PYTHON_CMD")
    print(f"📅 {datetime.now()}")
    print(f"🗄 {collection_id}")
    run(
        [
            PYTHON_CMD,
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "distill.py"),
            collection_id,
        ]
    )
