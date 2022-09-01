# file: alchemist.py
# NOTE: this file is intended to be run every minute (via cron/launchd)

import logging
import logging.config
import os
import shutil
import subprocess
import sys
import tempfile

from datetime import datetime
from glob import glob
from pathlib import Path

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
# NOTE: files are created in web.py with names like
# - CollectionID-preview-cloud_onsite_access
# - AnotherID-process-onsite
# - NameID-OH-1999.docx (Oral Histories IDs happen to fit the *-*-* scheme)
# TODO confirm Oral Histories ID scheme and/or make glob more robust
for f in glob(
    os.path.join(
        f'{config("WORK_NAS_APPS_MOUNTPOINT")}/{config("NAS_STATUS_FILES_RELATIVE_PATH")}',
        "*-*-*",
    )
):
    # logger.debug("🟣")
    # logger.info("🔵")
    # logger.warning("🟡")
    # logger.error("🔴")
    # logger.critical("🆘")

    logger.info(f"⚗️  FILE DETECTED: {f}")

    if Path(f).suffix in [".docx"]:
        # move the file to stop future initiation with the same file
        docxfile = shutil.move(f, tempfile.mkdtemp())
        logger.info(f"➡️  FILE MOVED: {docxfile}")
        try:
            command = [
                sys.executable,
                str(Path(__file__).parent.resolve().joinpath("oralhistories.py")),
                "--docxfile",
                docxfile,
            ]
            logger.info(f'🚰 RUNNING COMMAND: {" ".join(command)}')
            result = subprocess.run(
                command,
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            # If check is true, and the process exits with a non-zero exit code,
            # a CalledProcessError exception will be raised. Attributes of that
            # exception hold the arguments, the exit code, and stdout and stderr
            # if they were captured.
            logger.error(f"❌ {e}")
            logger.error(f"❌ {e.returncode}")
            logger.error(f"❌ {e.cmd}")
            logger.error(f"❌ {e.output}")
            logger.error(f"❌ {e.stdout}")
            logger.error(f"❌ {e.stderr}")
            raise
        except BaseException as e:
            logger.error(f"❌ {e}")
            raise
        # cleanup
        os.remove(str(Path(docxfile).parent))
        continue

    # NOTE using rsplit() in case collection_id contains a - (hyphen)
    collection_id = os.path.basename(f).rsplit("-", 2)[0]
    step = os.path.basename(f).rsplit("-", 2)[1]

    # set up list of flags
    flags = os.path.basename(f).rsplit("-", 1)[-1].split("_")
    for i, flag in enumerate(flags):
        flags[i] = f"--{flag}"

    # delete the init file to stop future initiation with the same file
    os.remove(f)
    logger.info(f"🗑  FILE REMOVED: {f}")

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
        logger.info(f'🚰 RUNNING COMMAND: {" ".join(command)}')
        # TODO get errors and output from the subprocess somewhere
        result = subprocess.run(
            command,
            # stdout=subprocess.PIPE,
            # stderr=subprocess.STDOUT,
            capture_output=True,
            check=True,
        )
        # from pprint import pprint; pprint(result)  # DEBUG
    except subprocess.CalledProcessError as e:
        # If check is true, and the process exits with a non-zero exit code, a
        # CalledProcessError exception will be raised. Attributes of that
        # exception hold the arguments, the exit code, and stdout and stderr if
        # they were captured.
        logger.error(f"❌ {e}")
        logger.error(f"❌ {e.returncode}")
        logger.error(f"❌ {e.cmd}")
        logger.error(f"❌ {e.output}")
        logger.error(f"❌ {e.stdout}")
        logger.error(f"❌ {e.stderr}")
        raise
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
            logger.info(f'📂 FILES MOVED TO: {config("STAGE_3_ORIGINAL_FILES")}')
        except BaseException as e:
            message = (
                "❌ unable to move the processed files from the processing directory\n"
            )
            with open(stream_path, "a") as stream:
                stream.write(message)
            logger.error(f"❌ {e}")
            raise

        try:
            command = [
                sys.executable,
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "validate.py",
                ),
            ]
            logger.info(
                f'🧾 VALIDATING: {Path(config("WORK_NAS_APPS_MOUNTPOINT")).joinpath(config("NAS_LOG_FILES_RELATIVE_PATH"), "validation.log")}'
            )
            subprocess.run(
                command,
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ {e}")
            logger.error(f"❌ {e.returncode}")
            logger.error(f"❌ {e.cmd}")
            logger.error(f"❌ {e.output}")
            logger.error(f"❌ {e.stdout}")
            logger.error(f"❌ {e.stderr}")
            raise
        except BaseException as e:
            logger.error(f"❌ {e}")
            raise

    with open(stream_path, "a") as stream:
        # NOTE specific emoji used for event listener in web.py:stream()
        # NOTE extra line feed required for tail to see the change
        stream.write("🟡\n\n")

    if step == "preview":
        logger.info(f"⏯  FINISHED: {collection_id} {step.upper()}")
    elif step == "process":
        logger.info(f"🥃 FINISHED: {collection_id} {step.upper()}")
