# NOTE this script must be run from an account with SSH access to Islandora

import os
import datetime

import sh
from decouple import config

islandora_server = sh.ssh.bake(
    f"-i",
    f"{config('ISLANDORA_SSH_KEY')}",
    f"{config('ISLANDORA_SSH_USER')}@{config('ISLANDORA_SSH_HOST')}",
    f"-p{config('ISLANDORA_SSH_PORT')}",
)


def main(
    collection_id: "the Collection ID from ArchivesSpace",  # type: ignore
):

    # get all the Islandora PIDs from the collections we are testing
    collection_pids = []
    book_pids = []
    try:
        collection_pids.append(f"caltech:{collection_id}")
        idcrudfp = islandora_server(
            "drush",
            "--user=1",
            f"--root={config('ISLANDORA_WEBROOT')}",
            "islandora_datastream_crud_fetch_pids",
            f"--collection=caltech:{collection_id}",
        )
        book_pids.extend(idcrudfp.split())
    except sh.ErrorReturnCode as e:
        # drush exits with a non-zero status when no PIDs are found,
        # which is interpreted as an error
        if "Sorry, no PIDS were found." not in str(e.stderr, "utf-8"):
            raise
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

    session = requests.Session()
    response = session.post(f'{config("ISLANDORA_URL").rstrip("/")}/user', data=login)

    # print(response.text)
    # print(response.headers)
    # print(session.cookies) # The session cookie is stored for subsequent requests

    # describe_response = session.get(f"{url}/islandora/rest/v1/object/islandora:root")
    # print(describe_response.text)

    for pid in pids:
        print(f"🔥 deleting {pid}")
        delete_response = session.delete(
            f'{config("ISLANDORA_URL").rstrip("/")}/islandora/rest/v1/object/{pid}'
        )

    # clear the Islandora Batch queue
    islandora_server(
        "drush",
        "--user=1",
        f"--root={config('ISLANDORA_WEBROOT')}",
        "islandora_batch_cleanup_processed_sets",
        f"--time={int(datetime.datetime.now().timestamp())}",
    )
    print("✅ cleaned-up Islandora Batch processed sets")

    # find and remove temporary staging files
    islandora_staging_files = (
        islandora_server.find("/tmp", "-name", f"caltech+{collection_id}")
        .strip()
        .rsplit("/", 2)[0]
    )
    print(f"🔥 deleting islandora_staging_files: {islandora_staging_files}")
    os.remove(islandora_staging_files)


if __name__ == "__main__":
    # fmt: off
    import plac; plac.call(main)
