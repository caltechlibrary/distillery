import os
import shutil
import sys
from datetime import datetime, timedelta
from glob import glob

import sh
from asnake.client import ASnakeClient
from decouple import config
from requests import HTTPError

if len(sys.argv) < 1:
    backup_file_date = (datetime.now() - timedelta(1)).strftime("%Y-%m-%d")
else:
    # TODO ensure argument is a date
    backup_file_date = sys.argv[1]

# delete created ArchivesSpace objects
asnake_client = ASnakeClient(
    baseurl=config("ASPACE_API_URL"),
    username=config("ASPACE_USERNAME"),
    password=config("ASPACE_PASSWORD"),
)
asnake_client.authorize()
# loop over URI paths, checking digital_object_components before digital_objects
for uri in [
    "/repositories/2/digital_object_components/",
    "/repositories/2/digital_objects/",
]:
    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(config("LOG_FILE"))), "archivesspace.log"
        )
    ) as log:
        lines = log.readlines()
        for line in lines:
            if line.startswith(uri):
                try:
                    print(f"🔥 deleting {line.strip()}")
                    delete_response = asnake_client.delete(line.strip())
                    delete_response.raise_for_status()
                except HTTPError as e:
                    print(f"⚠️ {e}")

# remove directories from STAGE_1_ORIGINAL_FILES
for d in glob(os.path.join(config("STAGE_1_ORIGINAL_FILES"), "*/")):
    print(f"🔥 deleting {d}")
    shutil.rmtree(d)

# remove directories from STAGE_2_ORIGINAL_FILES
for d in glob(os.path.join(config("STAGE_2_ORIGINAL_FILES"), "*/")):
    print(f"🔥 deleting {d}")
    shutil.rmtree(d)

# remove directories from STAGE_3_ORIGINAL_FILES
for d in glob(os.path.join(config("STAGE_3_ORIGINAL_FILES"), "*/")):
    print(f"🔥 deleting {d}")
    shutil.rmtree(d)

# remove directories from LOSSLESS_PRESERVATION_FILES
for d in glob(os.path.join(config("LOSSLESS_PRESERVATION_FILES"), "*/")):
    print(f"🔥 deleting {d}")
    shutil.rmtree(d)

# remove directories from COMPRESSED_ACCESS_FILES
for d in glob(os.path.join(config("COMPRESSED_ACCESS_FILES"), "*/")):
    print(f"🔥 deleting {d}")
    shutil.rmtree(d)

# copy test data to STAGE_1_ORIGINAL_FILES and store collection identifiers for later
collections = []
for d in glob(
    os.path.join(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "_data"), "*/"
    )
):
    print(f"📁 copying {d}")
    collections.append(os.path.basename(d.rstrip("/")))
    shutil.copytree(
        d.rstrip("/"),
        os.path.join(config("STAGE_1_ORIGINAL_FILES"), os.path.basename(d.rstrip("/"))),
    )

# move logs
for f in glob(os.path.join(os.path.dirname(os.path.abspath(config("LOG_FILE"))), "*")):
    if os.path.getsize(f) > 0:
        print(f"📄 moving {f}")
        shutil.move(
            f,
            os.path.join(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "_data"),
                f"reset-{datetime.now().strftime('%Y%m%d%H%M%S')}-{os.path.basename(f)}",
            ),
        )

islandora_server = sh.ssh.bake(
    f"-i",
    f"{config('ISLANDORA_SSH_KEY')}",
    f"{config('ISLANDORA_SSH_USER')}@{config('ISLANDORA_SSH_HOST')}",
    f"-p{config('ISLANDORA_SSH_PORT')}",
)

# store all the existing Islandora PIDs from the collections we are testing
collection_pids = []
book_pids = []
for c in collections:
    try:
        collection_pids.append(f"caltech:{c}")
        idcrudfp = islandora_server(
            "drush",
            "--user=1",
            f"--root={config('ISLANDORA_WEBROOT')}",
            "islandora_datastream_crud_fetch_pids",
            f"--collection=caltech:{c}",
        )
        book_pids.extend(idcrudfp.split())
    except sh.ErrorReturnCode as e:
        # drush exits with a non-zero status when no PIDs are found,
        # which is interpreted as an error
        if "Sorry, no PIDS were found." not in str(e.stderr, "utf-8"):
            raise e
print(collection_pids)
print(book_pids)

page_pids = []
for pid in book_pids:
    try:
        idcrudfp = islandora_server(
            "drush",
            "--user=1",
            f"--root={config('ISLANDORA_WEBROOT')}",
            "islandora_datastream_crud_fetch_pids",
            f"--is_member_of={pid}",
        )
        page_pids.extend(idcrudfp.split())
    except sh.ErrorReturnCode as e:
        # drush exits with a non-zero status when no PIDs are found,
        # which is interpreted as an error
        if "Sorry, no PIDS were found." not in str(e.stderr, "utf-8"):
            raise e
print(page_pids)

pids = collection_pids + book_pids + page_pids
print(pids)

# enable the Islandora REST module
islandora_server(
    "drush",
    "--user=1",
    f"--root={config('ISLANDORA_WEBROOT')}",
    "pm-enable",
    "islandora_rest",
    "--yes",
)
print("✅ enabled Islandora REST module")

# use Islandora REST to delete each object, including the collection objects
# use python requests: https://gist.github.com/NanoDano/cf368a7374a6963677c9
import requests

login = {
    "name": f"{config('ISLANDORA_USERNAME')}",
    "pass": f"{config('ISLANDORA_PASSWORD')}",
    "form_id": "user_login",
    "op": "Log in",
}

url = "http://localhost:8000"

session = requests.Session()
response = session.post(f"{url}/user", data=login)

# print(response.text)
# print(response.headers)
# print(session.cookies) # The session cookie is stored for subsequent requests

# describe_response = session.get(f"{url}/islandora/rest/v1/object/islandora:root")
# print(describe_response.text)

for pid in pids:
    print(f"🔥 deleting {pid}")
    delete_response = session.delete(f"{url}/islandora/rest/v1/object/{pid}")

# clear the Islandora Batch queue
islandora_server(
    "drush",
    "--user=1",
    f"--root={config('ISLANDORA_WEBROOT')}",
    "islandora_batch_cleanup_processed_sets",
    f"--time={int(datetime.now().timestamp())}",
)
print("✅ cleaned-up Islandora Batch processed sets")

# reset ArchivesSpace db
print("🔄 resetting ArchivesSpace database")
archivesspace_server = sh.ssh.bake(
    f"-A",  # enable agent forwarding
    f"-i",
    f"{config('ARCHIVESSPACE_SSH_KEY')}",
    f"{config('ARCHIVESSPACE_SSH_USER')}@{config('ARCHIVESSPACE_SSH_HOST')}",
    f"-p{config('ARCHIVESSPACE_SSH_PORT')}",
)
archivesspace_server(
    "/bin/bash",
    "/home/vagrant/shared/stop-load_db-start.sh",
    backup_file_date,
    _fg=True,
)
