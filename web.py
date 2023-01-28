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
import time

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


@bottle.route("/")
def distillery_get():
    step = "collecting"
    return bottle.template(
        "distillery",
        distillery_base_url=config("BASE_URL").rstrip("/"),
        user=authorize_user(),
        step=step,
    )


@bottle.route("/", method="POST")
def distillery_post():
    distillery_work_server_connection = rpyc.connect(
        config("WORK_HOSTNAME"), config("DISTILLERY_RPYC_PORT")
    )
    # TODO sanitize collection_id
    collection_id = bottle.request.forms.get("collection_id").strip()
    timestamp = str(int(time.time()))
    Path(config("WEB_STATUS_FILES")).joinpath(
        f"{collection_id}.{timestamp}.log"
    ).touch()
    # NOTE using getall() wraps single and multiple values in a list;
    # allows reuse of the same field name; only strings seem to work with rpyc
    # TODO sanitize destinations
    destinations = "_".join(bottle.request.forms.getall("destinations"))
    if bottle.request.forms.get("step") == "validating":
        step = "validating"
        # asynchronously validate on WORK server
        distillery_validate = rpyc.async_(
            distillery_work_server_connection.root.validate
        )
        async_result = distillery_validate(collection_id, destinations, timestamp)
    if bottle.request.forms.get("step") == "running":
        step = "running"
        # asynchronously run on WORK server
        distillery_run = rpyc.async_(distillery_work_server_connection.root.run)
        async_result = distillery_run(collection_id, destinations, timestamp)
    return bottle.template(
        "distillery",
        distillery_base_url=config("BASE_URL").rstrip("/"),
        user=authorize_user(),
        step=step,
        collection_id=collection_id,
        timestamp=timestamp,
        destinations=destinations,
    )


@bottle.route("/log/<collection_id>/<timestamp>")
def log(collection_id, timestamp):
    with open(
        Path(config("WEB_STATUS_FILES")).joinpath(f"{collection_id}.{timestamp}.log"),
        encoding="utf-8",
    ) as f:
        return bottle.template("distillery_log", log=f.readlines())


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
    timestamp = str(int(time.time()))
    if bottle.request.forms.get("upload"):
        op = "UPLOAD"
        upload = bottle.request.files.get("file")
        if Path(upload.filename).suffix in [".docx"]:
            component_id = Path(upload.filename).stem
            # TODO ensure WEB_STATUS_FILES exists
            logfile = Path(config("WEB_STATUS_FILES")).joinpath(
                f"{component_id}.{timestamp}.{op}.log"
            )
            logfile.touch()
            upload.save(
                config("ORALHISTORIES_WEB_UPLOADS"), overwrite=True
            )  # appends upload.filename automatically
            # asynchronously run process on WORK server
            oralhistories_run = rpyc.async_(
                oralhistories_work_server_connection.root.run
            )
            async_result = oralhistories_run(
                component_id=component_id, logfile=str(logfile)
            )
            return bottle.template(
                "oralhistories_post",
                distillery_base_url=config("BASE_URL").rstrip("/"),
                github_repo=config("ORALHISTORIES_GITHUB_REPO"),
                archivesspace_staff_url=config("ASPACE_STAFF_URL"),
                user=authorize_user(),
                component_id=component_id,
                timestamp=timestamp,
                op=op,
            )
        else:
            return "❌ ERROR: only .docx files are accepted"
    if bottle.request.forms.get("publish"):
        op = "PUBLISH"
        # asynchronously run process on WORK server
        oralhistories_run = rpyc.async_(oralhistories_work_server_connection.root.run)
        if bottle.request.forms.get("component_id_publish"):
            component_id = bottle.request.forms.get("component_id_publish")
            logfile = Path(config("WEB_STATUS_FILES")).joinpath(
                f"{component_id}.{timestamp}.{op}.log"
            )
            logfile.touch()
            async_result = oralhistories_run(
                component_id=component_id, publish=True, logfile=str(logfile)
            )
            return bottle.template(
                "oralhistories_post",
                distillery_base_url=config("BASE_URL").rstrip("/"),
                archivesspace_staff_url=config("ASPACE_STAFF_URL"),
                user=authorize_user(),
                component_id=component_id,
                timestamp=timestamp,
                op=op,
                oralhistories_public_base_url=config("ORALHISTORIES_PUBLIC_BASE_URL"),
                resolver_base_url=config("RESOLVER_BASE_URL"),
            )
        else:
            logfile = Path(config("WEB_STATUS_FILES")).joinpath(
                f"_.{timestamp}.{op}.log"
            )
            logfile.touch()
            async_result = oralhistories_run(publish=True, logfile=str(logfile))
            return bottle.template(
                "oralhistories_post",
                distillery_base_url=config("BASE_URL").rstrip("/"),
                user=authorize_user(),
                component_id="_",
                timestamp=timestamp,
                op=op,
            )
    if bottle.request.forms.get("update"):
        op = "UPDATE"
        # asynchronously run process on WORK server
        oralhistories_run = rpyc.async_(oralhistories_work_server_connection.root.run)
        if bottle.request.forms.get("component_id_update"):
            component_id = bottle.request.forms.get("component_id_update")
            logfile = Path(config("WEB_STATUS_FILES")).joinpath(
                f"{component_id}.{timestamp}.{op}.log"
            )
            logfile.touch()
            async_result = oralhistories_run(
                component_id=component_id, update=True, logfile=str(logfile)
            )
            return bottle.template(
                "oralhistories_post",
                distillery_base_url=config("BASE_URL").rstrip("/"),
                github_repo=config("ORALHISTORIES_GITHUB_REPO"),
                user=authorize_user(),
                component_id=component_id,
                timestamp=timestamp,
                op=op,
            )
        else:
            logfile = Path(config("WEB_STATUS_FILES")).joinpath(
                f"_.{timestamp}.{op}.log"
            )
            logfile.touch()
            async_result = oralhistories_run(update=True, logfile=str(logfile))
            return bottle.template(
                "oralhistories_post",
                distillery_base_url=config("BASE_URL").rstrip("/"),
                github_repo=config("ORALHISTORIES_GITHUB_REPO"),
                user=authorize_user(),
                component_id="_",
                timestamp=timestamp,
                op=op,
            )


@bottle.route("/oralhistories/log/<component_id>/<timestamp>/<op>")
def oralhistories_log(component_id, timestamp, op):
    with open(
        Path(config("WEB_STATUS_FILES")).joinpath(
            f"{component_id}.{timestamp}.{op}.log"
        ),
        encoding="utf-8",
    ) as f:
        return bottle.template("oralhistories_log", log=f.readlines())


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
