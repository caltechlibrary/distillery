# NOTE: this file is intended to be run via cron every minute
# configure python path in `settings.ini`

import os
from datetime import datetime
from glob import glob
from subprocess import run

from decouple import config

for f in glob(os.path.join(config("STATUS_FILES_DIR"), "*-processing")):
    collection_id = os.path.basename(f).split("-")[0]
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
