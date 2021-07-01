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

# NOTE the following logs explicit output from this file as well as output from the
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

    # NOTE we assume that PROCESSING_FILES is set correctly
    stream_path = os.path.join(
        config("PROCESSING_FILES"), f"{collection_id}-processing"
    )

    # move the `collection_id` directory into `STAGE_2_ORIGINAL_FILES`
    # NOTE shutil.move() in Python < 3.9 needs strings as arguments
    try:
        if os.path.isdir(os.path.join(config("STAGE_2_ORIGINAL_FILES"), collection_id)):
            # NOTE using copy+rm in order to not destroy an existing destination structure
            shutil.copytree(
                str(os.path.join(config("STAGE_1_ORIGINAL_FILES"), collection_id)),
                str(config("STAGE_2_ORIGINAL_FILES")),
                dirs_exist_ok=True,
            )
            shutil.rmtree(
                str(os.path.join(config("STAGE_1_ORIGINAL_FILES"), collection_id))
            )
        else:
            shutil.move(
                str(os.path.join(config("STAGE_1_ORIGINAL_FILES"), collection_id)),
                str(config("STAGE_2_ORIGINAL_FILES")),
            )
    except FileNotFoundError as e:
        # delete the init file to stop loop
        os.remove(f)
        # NOTE specific emoji used to indicate start of script for event listener
        message = f"⛔️ {collection_id} directory not found in {config('STAGE_1_ORIGINAL_FILES')}\n"
        with open(stream_path, "a") as stream:
            stream.write(message)
        logger.error(f"❌ {e}")
        # remove the stream file because it only contains one error message
        os.remove(stream_path)
        # we re-raise the exception because we cannot continue without the files
        raise
    except BaseException as e:
        # delete the init file to stop loop
        os.remove(f)
        # NOTE specific emoji used to indicate start of script for event listener
        message = "⛔️ unable to move the source files for processing\n"
        with open(stream_path, "a") as stream:
            stream.write(message)
        logger.error(f"❌ {e}")
        # remove the stream file because it only contains one error message
        os.remove(stream_path)
        # we re-raise the exception because we cannot continue without the files
        raise

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
        # TODO check for ACCESS_PLATFORM first
        logger.info("⚗️ access processing")
        # delete the init file
        os.remove(f)
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
        # TODO check for ACCESS_PLATFORM first
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

    # move the `collection_id` directory into `STAGE_3_ORIGINAL_FILES`
    # NOTE shutil.move() in Python < 3.9 needs strings as arguments
    try:
        if os.path.isdir(os.path.join(config("STAGE_3_ORIGINAL_FILES"), collection_id)):
            # NOTE using copy+rm in order to not destroy an existing destination structure
            shutil.copytree(
                str(os.path.join(config("STAGE_2_ORIGINAL_FILES"), collection_id)),
                str(os.path.join(config("STAGE_3_ORIGINAL_FILES"), collection_id)),
                dirs_exist_ok=True,
            )
            shutil.rmtree(
                str(os.path.join(config("STAGE_2_ORIGINAL_FILES"), collection_id))
            )
        else:
            shutil.move(
                str(os.path.join(config("STAGE_2_ORIGINAL_FILES"), collection_id)),
                str(config("STAGE_3_ORIGINAL_FILES")),
            )
    except BaseException as e:
        message = "❌ unable to move the processed files from the processing directory\n"
        with open(stream_path, "a") as stream:
            stream.write(message)
        logger.error(f"❌ {e}")

    # move the `*-processing` file to `STAGE_3_ORIGINAL_FILES`
    try:
        shutil.move(
            str(stream_path),
            str(
                os.path.join(
                    config("STAGE_3_ORIGINAL_FILES"),
                    f"{collection_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}.log",
                )
            ),
        )
    except BaseException as e:
        message = "❌ unable to move the processing log from the processing directory\n"
        with open(stream_path, "a") as stream:
            stream.write(message)
        logger.error(f"❌ {e}")

    logger.info(f"📆 {datetime.now()} end")
