# CALTECH ARCHIVES AND SPECIAL COLLECTIONS
# digital object preservation workflow

# bottlepy web application; see distillery.py for processing functionality

# bottle requires gevent.monkey.patch_all()
# fmt: off
from gevent import monkey; monkey.patch_all()
# fmt: on

import logging.config
import os
import shutil

from csv import DictReader
from pathlib import Path

import rpyc
import tailer
from bottle import (
    abort,
    default_app,
    error,
    get,
    post,
    request,
    response,
    template,
)
from decouple import config
import bottle

bottle.TEMPLATES.clear()
bottle.TEMPLATE_PATH.insert(0, str(Path(__file__).parent.resolve().joinpath("views")))

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    Path(__file__).resolve().parent.joinpath("settings.ini"),
    disable_existing_loggers=True,
)
logger = logging.getLogger("web")


@error(403)
def error403(error):
    return "Please contact Archives & Special Collections or Digital Library Development if you should have access."


@get("/")
def form_collection_id():
    # we pass the user dictionary to the template
    return template(
        "form", base_url=config("BASE_URL").rstrip("/"), user=authorize_user()
    )


@post("/preview")
def preview():
    # strip any invisibles
    collection_id = request.forms.get("collection_id").strip()
    # NOTE from form as multiple values
    processes = "_".join(request.forms.getall("processes"))
    # write the preview file for alchemist.py to find
    Path(config("WEB_NAS_APPS_MOUNTPOINT")).joinpath(
        config("NAS_STATUS_FILES_RELATIVE_PATH"), f"{collection_id}-preview-{processes}"
    ).touch()
    # set stream file path
    stream_path = Path(config("WEB_NAS_APPS_MOUNTPOINT")).joinpath(
        config("NAS_STATUS_FILES_RELATIVE_PATH"), f"{collection_id}-processing"
    )
    # move any existing stream file to logs directory
    if stream_path.is_file():
        # NOTE shutil.move() in Python < 3.9 needs strings as arguments
        shutil.move(
            str(stream_path),
            str(
                os.path.join(
                    config("WEB_NAS_APPS_MOUNTPOINT"),
                    config("NAS_LOG_FILES_RELATIVE_PATH"),
                    f"{collection_id}-{os.path.getmtime(stream_path)}.log",
                )
            ),
        )
    # create a new file for the event stream
    # NOTE this file seemingly must be create here instead of in alchemist.py
    # because nothing shows up in the stream when the file is created there
    stream_path.touch()
    return template(
        "preview",
        base_url=config("BASE_URL").rstrip("/"),
        collection_id=collection_id,
        processes=processes,
    )


@post("/distilling")
def begin_processing():
    collection_id = request.forms.get("collection_id")
    processes = request.forms.get("processes")
    # write the process file for alchemist.py to find
    Path(config("WEB_NAS_APPS_MOUNTPOINT")).joinpath(
        config("NAS_STATUS_FILES_RELATIVE_PATH"), f"{collection_id}-process-{processes}"
    ).touch()
    return template(
        "distilling",
        base_url=config("BASE_URL").rstrip("/"),
        collection_id=collection_id,
    )


@get("/distill/<collection_id>")
def stream(collection_id):
    # using server-sent events that work with javascript in preview.tpl
    response.content_type = "text/event-stream"
    response.cache_control = "no-cache"

    with open(
        Path(
            f'{config("WEB_NAS_APPS_MOUNTPOINT")}/{config("NAS_STATUS_FILES_RELATIVE_PATH")}'
        ).joinpath(f"{collection_id}-processing"),
        encoding="utf-8",
    ) as f:
        for line in tailer.follow(f):
            # the event stream format starts with "data: " and ends with "\n\n"
            # https://web.archive.org/web/20210701185847/https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format
            if line.startswith("🟢"):
                # we send an event field targeting a specific listener without data
                yield f"event: init\n"
            elif line.startswith("🟡"):
                yield f"event: done\n"
            else:
                yield f"data: {line}\n\n"


@bottle.route("/oralhistories")
def oralhistories_form():
    return bottle.template(
        "oralhistories",
        distillery_base_url=config("BASE_URL").rstrip("/"),
        user=authorize_user(),
        archivesspace_staff_url=config("ASPACE_STAFF_URL"),
        github_repo=config("ORALHISTORIES_GITHUB_REPO"),
        s3_bucket=config("ORALHISTORIES_BUCKET"),
    )


@bottle.route("/oralhistories", method="POST")
def oralhistories_post():
    oralhistories_work_server_connection = rpyc.connect(
        config("WORK_HOSTNAME"), config("ORALHISTORIES_RPYC_PORT")
    )
    if bottle.request.forms.get("upload"):
        upload = bottle.request.files.get("file")
        if Path(upload.filename).suffix in [".docx"]:
            # TODO avoid nasty Error: 500 by checking for existing file
            upload.save(
                config("ORALHISTORIES_WEB_UPLOADS")
            )  # appends upload.filename automatically
            # asynchronously run process on WORK server
            oralhistories_run = rpyc.async_(
                oralhistories_work_server_connection.root.run
            )
            async_result = oralhistories_run(component_id=Path(upload.filename).stem)
            return bottle.template(
                "oralhistories_post",
                distillery_base_url=config("BASE_URL").rstrip("/"),
                github_repo=config("ORALHISTORIES_GITHUB_REPO"),
                archivesspace_staff_url=config("ASPACE_STAFF_URL"),
                user=authorize_user(),
                component_id=Path(upload.filename).stem,
                op="upload",
            )
        else:
            return bottle.template(
                "oralhistories_post",
                distillery_base_url=config("BASE_URL").rstrip("/"),
                user=authorize_user(),
                component_id="error",
                op="upload",
            )
    if bottle.request.forms.get("publish"):
        # asynchronously run process on WORK server
        oralhistories_run = rpyc.async_(oralhistories_work_server_connection.root.run)
        if bottle.request.forms.get("component_id_publish"):
            async_result = oralhistories_run(
                component_id=request.forms.get("component_id_publish"), publish=True
            )
            return bottle.template(
                "oralhistories_post",
                distillery_base_url=config("BASE_URL").rstrip("/"),
                archivesspace_staff_url=config("ASPACE_STAFF_URL"),
                user=authorize_user(),
                component_id=bottle.request.forms.get("component_id_publish"),
                op="publish",
                oralhistories_public_base_url=config("ORALHISTORIES_PUBLIC_BASE_URL"),
                resolver_base_url=config("RESOLVER_BASE_URL"),
            )
        else:
            async_result = oralhistories_run(publish=True)
            return bottle.template(
                "oralhistories_post",
                distillery_base_url=config("BASE_URL").rstrip("/"),
                user=authorize_user(),
                component_id="all",
                op="publish",
            )
    if bottle.request.forms.get("update"):
        # asynchronously run process on WORK server
        oralhistories_run = rpyc.async_(oralhistories_work_server_connection.root.run)
        if request.forms.get("component_id_update"):
            async_result = oralhistories_run(
                component_id=request.forms.get("component_id_update"), update=True
            )
            return bottle.template(
                "oralhistories_post",
                distillery_base_url=config("BASE_URL").rstrip("/"),
                github_repo=config("ORALHISTORIES_GITHUB_REPO"),
                user=authorize_user(),
                component_id=bottle.request.forms.get("component_id_update"),
                op="update",
            )
        else:
            async_result = oralhistories_run(update=True)
            return bottle.template(
                "oralhistories_post",
                distillery_base_url=config("BASE_URL").rstrip("/"),
                github_repo=config("ORALHISTORIES_GITHUB_REPO"),
                user=authorize_user(),
                component_id="all",
                op="update",
            )


def authorize_user():
    if debug_user:
        # debug_user is set when running bottle locally
        return debug_user
    elif request.environ.get("REMOTE_USER", None):
        # REMOTE_USER is an email address in Shibboleth
        email_address = request.environ["REMOTE_USER"]
        # we check the username against our authorized users.csv file
        with open(Path(__file__).parent.resolve().joinpath("users.csv")) as csvfile:
            users = DictReader(csvfile)
            for user in users:
                if user["email_address"] == email_address:
                    return user
        abort(403)
    else:
        abort(403)


if __name__ == "__main__":
    # supply a user when running bottle locally
    debug_user = {
        "email_address": "hello@example.com",
        "display_name": "World",
    }
    bottle.run(
        host=config("LOCALHOST"),
        port=config("LOCALPORT"),
        server="gevent",
        reloader=True,
        debug=True,
    )
else:
    # NOTE this code will run when using mod_wsgi
    # fmt: off

    # change working directory so relative paths (and template lookup) work again
    import os
    os.chdir(os.path.dirname(__file__))

    # set the variable to avoid a NameError
    debug_user = None

    # attach Bottle to Apache using mod_wsgi
    application = default_app()
