# NOTE: this file is intended to be run via cron every minute

import logging
import logging.config
import os
import shutil
import subprocess
import sys
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

    logger.info(f"📅 {datetime.now()} begin")
    logger.info(f"🗄 {collection_id}")

    if os.path.basename(f).split("-")[-1] == "report":
        logger.info("⚗️ report processing")
        pass
    elif os.path.basename(f).split("-")[-1] == "preservation":
        logger.info("⚗️ preservation processing")
        # delete the init file
        os.remove(f)
        try:
            subprocess.run(
                [
                    sys.executable,
                    os.path.join(
                        os.path.dirname(os.path.abspath(__file__)), "distill.py"
                    ),
                    collection_id,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=True,
            )
        except BaseException as e:
            logger.error(f"❌ {e.stdout.decode('utf-8')}")
    elif os.path.basename(f).split("-")[-1] == "access":
        logger.info("⚗️ access processing")
        # delete the init file
        os.remove(f)
        # move the `collection_id` directory into `STAGE_2_ORIGINAL_FILES`
        # NOTE shutil.move() in Python < 3.9 needs strings as arguments
        try:
            # TODO need to rethink file locations; some files may be explicitly not
            # sent to islandora, so islandora.py should not look for files in the
            # same place; probably need two different locations: one for when
            # originals are not to be published and one for when they may be
            shutil.copytree(
                str(os.path.join(config("STAGE_1_ORIGINAL_FILES"), collection_id)),
                str(config("STAGE_2_ORIGINAL_FILES")),
                dirs_exist_ok=True
            )
            shutil.rmtree(
                str(os.path.join(config("STAGE_1_ORIGINAL_FILES"), collection_id))
            )
            # shutil.move(
            #     str(os.path.join(config("STAGE_1_ORIGINAL_FILES"), collection_id)),
            #     str(config("STAGE_2_ORIGINAL_FILES")),
            # )
        # except FileNotFoundError as e:
        #     # NOTE FileNotFoundError is not caught with BaseException
        #     logger.error(f"❌ {e}")
        #     # we re-raise the exception because we cannot continue without the files
        #     raise
        except BaseException as e:
            logger.error(f"❌ {e}")
            raise
        try:
            subprocess.run(
                [
                    sys.executable,
                    os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        f"{config('ACCESS_PLATFORM')}.py",
                    ),
                    collection_id,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=True,
            )
        except BaseException as e:
            logger.error(f"❌ {e.stdout.decode('utf-8')}")
    elif os.path.basename(f).split("-")[-1] == "preservation_access":
        logger.info("⚗️ preservation & access processing")
        # delete the init file
        os.remove(f)
        try:
            subprocess.run(
                [
                    sys.executable,
                    os.path.join(
                        os.path.dirname(os.path.abspath(__file__)), "distill.py"
                    ),
                    collection_id,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=True,
            )
        except BaseException as e:
            logger.error(f"❌ {e.stdout.decode('utf-8')}")
        try:
            subprocess.run(
                [
                    sys.executable,
                    os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        f"{config('ACCESS_PLATFORM')}.py",
                    ),
                    collection_id,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=True,
            )
        except BaseException as e:
            logger.error(f"❌ {e.stdout.decode('utf-8')}")
    else:
        # TODO log a message that an unknown file was found
        pass

    logger.info(f"📆 {datetime.now()} end")
