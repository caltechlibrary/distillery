# file: alchemist.py
# NOTE: this file is intended to be run every minute (via cron/launchd)

import logging
import logging.config
import os
import shutil
import subprocess
import sys
from datetime import datetime
from glob import glob
from pathlib import Path
from time import sleep

from decouple import config, UndefinedValueError

# NOTE: the following configuration is for explicit output from this file as well as
# output from the subprocesses; any errors from running this file are output wherever
# the initiating process (cron/launchd) sends them
logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.ini"),
    disable_existing_loggers=False,
)
logger = logging.getLogger("alchemist")

# the main loop which checks for files in the STATUS_FILES directory
# NOTE: preview and process files are created in web.py
for f in glob(
    os.path.join(
        f'{config("WORK_NAS_APPS_MOUNTPOINT")}/{config("NAS_STATUS_FILES_RELATIVE_PATH")}',
        "*-*-*",
    )
):
    # NOTE using rsplit() in case collection_id contains a - (hyphen)
    collection_id = os.path.basename(f).rsplit("-", 2)[0]
    step = os.path.basename(f).rsplit("-", 2)[1]

    # set up list of flags
    flags = os.path.basename(f).rsplit("-", 1)[-1].split("_")
    for i, flag in enumerate(flags):
        flags[i] = f"--{flag}"

    # delete the init file to stop future initiation with the same file
    os.remove(f)

    if step == "preview":
        module = "preview"
    elif step == "process":
        module = "distillery"
    else:
        continue

    stream_path = Path(config("WORK_NAS_APPS_MOUNTPOINT")).joinpath(
        config("NAS_STATUS_FILES_RELATIVE_PATH"), f"{collection_id}-processing"
    )

    # if stream_path.is_file():
    #     # move the `*-processing` file to `STAGE_3_ORIGINAL_FILES`
    #     # NOTE shutil.move() in Python < 3.9 needs strings as arguments
    #     shutil.move(
    #         str(stream_path),
    #         str(
    #             os.path.join(
    #                 config("STAGE_3_ORIGINAL_FILES"),
    #                 f"{collection_id}-{os.path.getmtime(stream_path)}.log",
    #             )
    #         ),
    #     )

    # # recreate the stream_path file
    # stream_path.touch()

    with open(stream_path, "a") as stream:
        # NOTE specific emoji used to indicate start of script for event listener
        # SEE web.py:stream()
        stream.write(f"🟢\n")

    logger.info(f"📅 {datetime.now()} begin")
    logger.info(f"🗄  {collection_id}")

    try:
        command = [
            sys.executable,
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                f"{module}.py",
            ),
            collection_id,
        ]
        command.extend(flags)
        # TODO get errors and output from the subprocess somewhere
        result = subprocess.run(
            command,
            # stdout=subprocess.PIPE,
            # stderr=subprocess.STDOUT,
            capture_output=True,
            # check=True,
        )
        # from pprint import pprint; pprint(result)  # DEBUG
    except BaseException as e:
        logger.error(f"❌ {e}")
        raise

    if step == "process":
        # move the `collection_id` directory into `STAGE_3_ORIGINAL_FILES`
        # NOTE shutil.move() in Python < 3.9 needs strings as arguments
        try:
            if os.path.isdir(
                os.path.join(config("STAGE_3_ORIGINAL_FILES"), collection_id)
            ):
                # NOTE using copy+rm in order to not destroy an existing destination structure
                shutil.copytree(
                    str(os.path.join(config("WORKING_ORIGINAL_FILES"), collection_id)),
                    str(os.path.join(config("STAGE_3_ORIGINAL_FILES"), collection_id)),
                    dirs_exist_ok=True,
                )
                shutil.rmtree(
                    str(os.path.join(config("WORKING_ORIGINAL_FILES"), collection_id))
                )
            else:
                shutil.move(
                    str(os.path.join(config("WORKING_ORIGINAL_FILES"), collection_id)),
                    str(config("STAGE_3_ORIGINAL_FILES")),
                )
        except BaseException as e:
            message = (
                "❌ unable to move the processed files from the processing directory\n"
            )
            with open(stream_path, "a") as stream:
                stream.write(message)
            logger.error(f"❌ {e}")
            raise

    with open(stream_path, "a") as stream:
        # NOTE specific emoji used for event listener in web.py:stream()
        # NOTE extra line feed required for tail to see the change
        stream.write("🟡\n\n")

    logger.info(f"📆 {datetime.now()} end")
