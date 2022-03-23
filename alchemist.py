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
    # disable_existing_loggers=False,
)
logger = logging.getLogger("alchemist")

# the main loop which checks for init files in the STATUS_FILES directory
# NOTE: init files are created in distillery.py
for f in glob(
    os.path.join(
        f'{config("WORK_NAS_APPS_MOUNTPOINT")}/{config("NAS_STATUS_FILES_RELATIVE_PATH")}',
        "*-init-*",
    )
):
    # using rsplit() in case the collection_id contains a - (hyphen) character
    collection_id = os.path.basename(f).rsplit("-", 2)[0]

    # set up list of flags
    flags = os.path.basename(f).rsplit("-", 1)[-1].split("_")
    for i, flag in enumerate(flags):
        flags[i] = f"--{flag}"

    # delete the init file to stop future initiation with the same file
    os.remove(f)

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
        # SEE distillery.py:stream()
        stream.write(f"🟢\n")

    logger.info(f"📅 {datetime.now()} begin")
    logger.info(f"🗄  {collection_id}")

    # move the `collection_id` directory into `WORKING_ORIGINAL_FILES`
    try:
        # make a list of directory names to check against
        entries = []
        for entry in os.scandir(config("INITIAL_ORIGINAL_FILES")):
            if entry.is_dir:
                entries.append(entry.name)
        # check that collection_id case matches directory name
        if collection_id in entries:
            # NOTE using copy+rm in order to not destroy an existing destination structure
            shutil.copytree(
                str(os.path.join(config("INITIAL_ORIGINAL_FILES"), collection_id)),
                str(os.path.join(config("WORKING_ORIGINAL_FILES"), collection_id)),
                dirs_exist_ok=True,
            )
            shutil.rmtree(
                str(os.path.join(config("INITIAL_ORIGINAL_FILES"), collection_id))
            )
        else:
            message = f"❌ no directory name matching {collection_id} in {config('INITIAL_ORIGINAL_FILES')}\n"
            with open(stream_path, "a") as stream:
                stream.write(message)
            raise NotADirectoryError(message)
    except FileNotFoundError as e:
        message = f"❌ {collection_id} directory not found in {config('INITIAL_ORIGINAL_FILES')}\n"
        with open(stream_path, "a") as stream:
            stream.write(message)
        logger.error(f"❌ {e}")
        # remove the stream file because it only contains one error message
        os.remove(stream_path)
        # we re-raise the exception because we cannot continue without the files
        raise
    except BaseException as e:
        message = "❌ unable to move the source files for processing\n"
        with open(stream_path, "a") as stream:
            stream.write(message)
        logger.error(f"❌ {e}")
        # remove the stream file because it only contains one error message
        os.remove(stream_path)
        # we re-raise the exception because we cannot continue without the files
        raise

    # check independently for each flag option; the different processing scripts
    # are each responsible for checking the list of flag options that were
    # passed in order to know what other processes will have run
    # TODO NOT SURE THIS IS THE BEST WAY; MAYBE ALL LOGIC ABOUT WHAT TO RUN IN
    # WHAT ORDER SHOULD ALL HAPPEN IN THIS FILE

    # NOTE the order of these conditions matters for certain processing scripts
    if "--report" in flags:
        logger.info("⚗️  processing report")
        pass
    if "--cloud" in flags:
        logger.info("⚗️  processing cloud preservation files")
        # validate CLOUD_PLATFORM
        try:
            config("CLOUD_PLATFORM")
        except UndefinedValueError as e:
            message = "❌ CLOUD_PLATFORM not defined in settings file\n"
            with open(stream_path, "a") as stream:
                stream.write(message)
            logger.error(f"❌ {e}")
            raise
        try:
            command = [
                sys.executable,
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    f'{config("CLOUD_PLATFORM")}.py',
                ),
                collection_id,
            ]
            command.extend(flags)
            subprocess.run(
                command,
                # stdout=subprocess.PIPE,
                # stderr=subprocess.STDOUT,
                check=True,
            )
        except BaseException as e:
            logger.error(f"❌ {e}")
            raise
    if "--onsite" in flags:
        logger.info("⚗️  processing onsite preservation files")
        # validate ONSITE_MEDIUM
        try:
            config("ONSITE_MEDIUM")
        except UndefinedValueError as e:
            message = "❌ ONSITE_MEDIUM not defined in settings file\n"
            with open(stream_path, "a") as stream:
                stream.write(message)
            logger.error(f"❌ {e}")
            raise
        try:
            command = [
                sys.executable,
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    # f"{config('ONSITE_MEDIUM')}.py",
                    "distill.py",
                ),
                collection_id,
            ]
            command.extend(flags)
            # TODO get errors and output from the subprocess somewhere
            subprocess.run(
                command,
                # stdout=subprocess.PIPE,
                # stderr=subprocess.STDOUT,
                check=True,
            )
        except BaseException as e:
            logger.error(f"❌ {e}")
            raise
    if "--access" in flags:
        logger.info("⚗️  processing access files")
        # validate ACCESS_PLATFORM
        try:
            config("ACCESS_PLATFORM")
        except UndefinedValueError as e:
            message = "❌ ACCESS_PLATFORM not defined in settings file\n"
            with open(stream_path, "a") as stream:
                stream.write(message)
            logger.error(f"❌ {e}")
            raise
        try:
            command = [
                sys.executable,
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    f"{config('ACCESS_PLATFORM')}.py",
                ),
                collection_id,
            ]
            command.extend(flags)
            subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=True,
            )
        except BaseException as e:
            logger.error(f"❌ {e}")
            raise

    # move the `collection_id` directory into `STAGE_3_ORIGINAL_FILES`
    # NOTE shutil.move() in Python < 3.9 needs strings as arguments
    try:
        if os.path.isdir(os.path.join(config("STAGE_3_ORIGINAL_FILES"), collection_id)):
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
        message = "❌ unable to move the processed files from the processing directory\n"
        with open(stream_path, "a") as stream:
            stream.write(message)
        logger.error(f"❌ {e}")
        raise

    with open(stream_path, "a") as stream:
        # NOTE specific emoji used for event listener in distillery.py:stream()
        stream.write("🔴")

    logger.info(f"📆 {datetime.now()} end")
