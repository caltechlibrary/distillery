import os
import shutil

from glob import glob

import sh

from decouple import config


# remove directories from INITIAL_ORIGINAL_FILES
for d in glob(os.path.join(config("INITIAL_ORIGINAL_FILES"), "*/")):
    print(f"🔥 deleting {d}")
    shutil.rmtree(d)

# remove directories from WORKING_ORIGINAL_FILES
for d in glob(os.path.join(config("WORKING_ORIGINAL_FILES"), "*/")):
    print(f"🔥 deleting {d}")
    shutil.rmtree(d)

# remove directories from STAGE_3_ORIGINAL_FILES
for d in glob(os.path.join(config("STAGE_3_ORIGINAL_FILES"), "*/")):
    print(f"🔥 deleting {d}")
    shutil.rmtree(d)

# remove directories from %(ENV)s_PRESERVATION_FILES
for d in glob(
    os.path.join(
        f'{config("WORK_PRESERVATION_FILES")}',
        "*/",
    )
):
    print(f"🔥 deleting {d}")
    shutil.rmtree(d)

# remove directories from COMPRESSED_ACCESS_FILES
for d in glob(os.path.join(config("COMPRESSED_ACCESS_FILES"), "*/")):
    print(f"🔥 deleting {d}")
    shutil.rmtree(d)

# copy test data to INITIAL_ORIGINAL_FILES
for d in glob(os.path.join(config("ENV_RESET_TEST_DATA"), "*/")):
    print(f"📁 copying {d}")
    shutil.copytree(
        d.rstrip("/"),
        os.path.join(config("INITIAL_ORIGINAL_FILES"), os.path.basename(d.rstrip("/"))),
    )

# reset ArchivesSpace db
print("🔄 resetting ArchivesSpace database")
archivesspace_server = sh.ssh.bake(
    f"-A",  # enable agent forwarding
    f"-t",  # allow sudo commands
    f"-i",
    f"{config('ARCHIVESSPACE_SSH_KEY')}",
    f"{config('ARCHIVESSPACE_SSH_USER')}@{config('ARCHIVESSPACE_SSH_HOST')}",
    f"-p{config('ARCHIVESSPACE_SSH_PORT')}",
)
# ASSUMPTION command is three parts like `sudo /bin/bash /path/to/script`
archivesspace_server(
    f'{config("ENV_ARCHIVESSPACE_RESET_CMD").split(maxsplit=2)[0]}',
    f'{config("ENV_ARCHIVESSPACE_RESET_CMD").split(maxsplit=2)[1]}',
    f'{config("ENV_ARCHIVESSPACE_RESET_CMD").split(maxsplit=2)[2]}',
    _fg=True,
)
