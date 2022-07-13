import logging
import logging.config
import os

from asnake.client import ASnakeClient
from decouple import config
from pathlib import Path

import tape

logging.config.fileConfig(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.ini"),
)
logger = logging.getLogger("validate")

asnake_client = ASnakeClient(
    baseurl=config("ASPACE_API_URL"),
    username=config("ASPACE_USERNAME"),
    password=config("ASPACE_PASSWORD"),
)
asnake_client.authorize()


def main():
    validation_log = Path(config("WORK_NAS_APPS_MOUNTPOINT")).joinpath(
        config("NAS_LOG_FILES_RELATIVE_PATH"), "validation.log"
    )
    with open(validation_log) as f:
        log = f.read()
    if "🔮" in log:
        last_run = log.split("🔮")[-1]
    else:
        logger.warning("🔮 INDICATOR NOT FOUND")
        return
    archivesspace_log = []
    tape_log = []
    for line in last_run.splitlines():
        if "ARCHIVESSPACE" in line:
            archivesspace_log.append(line.strip().split()[-1])
        elif "TAPE" in line:
            tape_log.append(line.strip().split()[-1])
    mounted_tape_indicator = tape.get_tape_indicator()
    for archivesspace_uri in archivesspace_log:
        record = asnake_client.get(archivesspace_uri).json()
        # TODO additional jsonmodel_types
        if record["jsonmodel_type"] == "digital_object_component":
            for file_version in record["file_versions"]:
                file_uri = file_version["file_uri"]
                if file_uri.startswith("file:///LTO/"):
                    if file_uri.split("/", 5)[-1] in tape_log:
                        if (
                            file_uri.split("file:///LTO/")[-1].split("/")[0]
                            == mounted_tape_indicator
                        ):
                            if tape.tape_server(
                                f'find {config("TAPE_LTO_MOUNTPOINT")} -type f -name {file_uri.split("/")[-1]}',
                            ):
                                logger.info(f"✅ FOUND ON TAPE: {file_uri}")
                                tape_log.remove(file_uri.split("/", 5)[-1])
                    else:
                        logger.warning(f"‼️  NOT FOUND IN TAPE_LOG: {file_uri}")
        else:
            logger.info(f'🈳 JSONMODEL_TYPE: {record["jsonmodel_type"]}')
    for tape_uri in tape_log:
        if not tape_uri.endswith(".json"):
            logger.warning(f"‼️  NOT FOUND IN ARCHIVESSPACE: {tape_uri}")


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
