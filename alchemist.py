# NOTE: this file is intended to be run via cron every minute
# configure /path/to/python3 in `settings.ini`

import logging
import logging.config
import os
import shutil
import subprocess
from datetime import datetime
from glob import glob

from decouple import config

# NOTE the following logs deliberate output from this file as well as output from the
# subprocesses; errors from running this file are output wherever the initiating process
# (cron/launchd) sends them
logging.config.fileConfig(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.ini"),
    disable_existing_loggers=False,
)
logger = logging.getLogger("alchemist")

for f in glob(os.path.join(config("PROCESSING_FILES"), "*-init-*")):
    # using rsplit() in case the collection_id contains a - (hyphen) character
    collection_id = os.path.basename(f).rsplit("-", 2)[0]
    PYTHON_CMD = config("PYTHON_CMD")

    logger.info(f"📅 {datetime.now()} begin")
    logger.info(f"🗄 {collection_id}")

    if os.path.basename(f).split("-")[-1] == "report":
        logger.info("⚗️ report processing")
        pass
    elif os.path.basename(f).split("-")[-1] == "preservation":
        logger.info("⚗️ preservation processing")
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
        logger.info("⚗️ access processing")
        # delete the init file
        os.remove(f)
        # move the `collection_id` directory into `STAGE_2_ORIGINAL_FILES`
        # NOTE shutil.move() in Python < 3.9 needs strings as arguments
        try:
            shutil.move(
                # TODO need to rethink file locations; some files may be explicitly not
                # sent to islandora, so islandora.py should not look for files in the
                # same place; probably need two different locations: one for when
                # originals are not to be published and one for when they may be
                str(os.path.join(config("STAGE_1_ORIGINAL_FILES"), collection_id)),
                str(config("STAGE_2_ORIGINAL_FILES")),
            )
        except Exception as e:
            # TODO log a problem and notify
            logger.error(
                f"❌ unable to move directory: {str(os.path.join(config('STAGE_1_ORIGINAL_FILES'), collection_id))}"
            )
            logger.error(e)
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
        logger.info("⚗️ preservation & access processing")
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

    logger.info(f"📆 {datetime.now()} end")
