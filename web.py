# CALTECH ARCHIVES AND SPECIAL COLLECTIONS
# digital object preservation workflow

# bottlepy web application; see distillery.py for processing functionality

# bottle requires gevent.monkey.patch_all()
# fmt: off
from gevent import monkey; monkey.patch_all()
# fmt: on

import json
import logging.config
import os
import time

from csv import DictReader
from pathlib import Path

import bottle
import rpyc

from decouple import config

bottle.TEMPLATES.clear()
bottle.TEMPLATE_PATH.insert(0, str(Path(__file__).parent.resolve().joinpath("views")))

logging.config.fileConfig(
    # set the logging configuration in the settings.ini file
    Path(__file__).resolve().parent.joinpath("settings.ini"),
    disable_existing_loggers=True,
)
logger = logging.getLogger("web")


@bottle.error(403)
def error403(error):
    return "Please contact Archives & Special Collections or Digital Library Development if you should have access."


@bottle.route("/")
def distillery_form():
    step = "collecting"
    return bottle.template(
        "distillery_form",
        distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
        user=authorize_user(),
        step=step,
    )


@bottle.route("/validate", method="POST")
def distillery_validate():
    try:
        distillery_work_server_connection = rpyc.connect(
            config("WORK_HOSTNAME"), config("DISTILLERY_RPYC_PORT")
        )
    except ConnectionRefusedError:
        return f'<h1>Connection Refused</h1><p>There was a problem connecting to <code>{config("WORK_HOSTNAME")}</code>. Please contact {config("DISTILLERY_DEVELOPER_CONTACT", default="Digital Library Development")} for assistance.</p>'
    except:
        raise
    destinations = {}
    batch_set_id = str(time.time())
    # NOTE using getall() wraps single and multiple values in a list
    for destination in bottle.request.forms.getall("destinations"):
        if destination in ["cloud", "onsite", "access"]:
            destinations[destination] = True
    if destinations.get("access"):
        destinations["access"] = {}
        destinations["access"]["file_versions_op"] = bottle.request.forms.get(
            "file_versions_op"
        )
        destinations["access"]["thumbnail_label"] = bottle.request.forms.get(
            "thumbnail_label"
        )
    # asynchronously validate on WORK server
    distillery_validate = rpyc.async_(distillery_work_server_connection.root.validate)
    async_result = distillery_validate(json.dumps(destinations), batch_set_id)
    return bottle.template(
        "distillery_validate",
        distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
        user=authorize_user(),
        destinations=json.dumps(destinations),
        batch_set_id=batch_set_id,
    )


@bottle.route("/validate/log/<batch_set_id>")
def distillery_validate_log(batch_set_id):
    with open(
        Path(config("WEB_STATUS_FILES")).joinpath(f"{batch_set_id}.validate.log"),
        encoding="utf-8",
    ) as f:
        return bottle.template("distillery_log", log=f.readlines())


@bottle.route("/run", method="POST")
def distillery_run():
    try:
        distillery_work_server_connection = rpyc.connect(
            config("WORK_HOSTNAME"), config("DISTILLERY_RPYC_PORT")
        )
    except ConnectionRefusedError:
        return f'<h1>Connection Refused</h1><p>There was a problem connecting to <code>{config("WORK_HOSTNAME")}</code>. Please contact {config("DISTILLERY_DEVELOPER_CONTACT", default="Digital Library Development")} for assistance.</p>'
    except:
        raise
    destinations = json.loads(bottle.request.forms.get("destinations"))
    batch_set_id = bottle.request.forms.get("batch_set_id")
    # asynchronously run on WORK server
    distillery_run = rpyc.async_(distillery_work_server_connection.root.run)
    async_result = distillery_run(json.dumps(destinations), batch_set_id)
    return bottle.template(
        "distillery_run",
        distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
        user=authorize_user(),
        destinations=json.dumps(destinations),
        batch_set_id=batch_set_id,
    )


@bottle.route("/run/log/<batch_set_id>")
def distillery_run_log(batch_set_id):
    with open(
        Path(config("WEB_STATUS_FILES")).joinpath(f"{batch_set_id}.run.log"),
        encoding="utf-8",
    ) as f:
        return bottle.template("distillery_log", log=f.readlines())


@bottle.route("/alchemist")
def alchemist_form():
    return bottle.template(
        "alchemist_form",
        distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
        user=authorize_user(),
    )


@bottle.route("/alchemist/regenerate", method="POST")
def alchemist_regenerate():
    try:
        distillery_work_server_connection = rpyc.connect(
            config("WORK_HOSTNAME"), config("DISTILLERY_RPYC_PORT")
        )
    except ConnectionRefusedError:
        return f'<h1>Connection Refused</h1><p>There was a problem connecting to <code>{config("WORK_HOSTNAME")}</code>. Please contact {config("DISTILLERY_DEVELOPER_CONTACT", default="Digital Library Development")} for assistance.</p>'
    except:
        raise
    timestamp = str(int(time.time()))
    # asynchronously run on WORK server
    distillery_alchemist_regenerate = rpyc.async_(
        distillery_work_server_connection.root.alchemist_regenerate
    )
    if bottle.request.forms.get("component_id"):
        component_id = bottle.request.forms.get("component_id")
        logfile = Path(config("WEB_STATUS_FILES")).joinpath(
            f"{component_id}.{timestamp}.alchemist_regenerate.log"
        )
        logfile.touch()
        async_result = distillery_alchemist_regenerate(
            component_id=component_id, logfile=str(logfile)
        )
        return bottle.template(
            "alchemist_regenerate",
            distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
            archivesspace_staff_url=config("ASPACE_STAFF_URL"),
            user=authorize_user(),
            component_id=component_id,
            timestamp=timestamp,
        )
    elif bottle.request.forms.get("collection_id"):
        collection_id = bottle.request.forms.get("collection_id")
        logfile = Path(config("WEB_STATUS_FILES")).joinpath(
            f"{collection_id}.{timestamp}.alchemist_regenerate.log"
        )
        logfile.touch()
        async_result = distillery_alchemist_regenerate(
            collection_id=collection_id, logfile=str(logfile)
        )
        return bottle.template(
            "alchemist_regenerate",
            distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
            archivesspace_staff_url=config("ASPACE_STAFF_URL"),
            user=authorize_user(),
            component_id=collection_id,
            timestamp=timestamp,
        )
    else:
        logfile = Path(config("WEB_STATUS_FILES")).joinpath(
            f"_.{timestamp}.alchemist_regenerate.log"
        )
        logfile.touch()
        async_result = distillery_alchemist_regenerate(logfile=str(logfile))
        return bottle.template(
            "alchemist_regenerate",
            distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
            user=authorize_user(),
            component_id="_",
            timestamp=timestamp,
        )


@bottle.route("/alchemist/regenerate/log/<component_id>/<timestamp>")
def alchemist_regenerate_log(component_id, timestamp):
    with open(
        Path(config("WEB_STATUS_FILES")).joinpath(
            f"{component_id}.{timestamp}.alchemist_regenerate.log"
        ),
        encoding="utf-8",
    ) as f:
        return bottle.template("alchemist_regenerate_log", log=f.readlines())


@bottle.route("/oralhistories")
def oralhistories_form():
    return bottle.template(
        "oralhistories",
        distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
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
                distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
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
                distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
                archivesspace_staff_url=config("ASPACE_STAFF_URL"),
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
            async_result = oralhistories_run(publish=True, logfile=str(logfile))
            return bottle.template(
                "oralhistories_post",
                distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
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
                distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
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
                distillery_base_url=config("DISTILLERY_BASE_URL").rstrip("/"),
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
    elif bottle.request.environ.get("REMOTE_USER", None):
        # REMOTE_USER is an email address in Shibboleth
        email_address = bottle.request.environ["REMOTE_USER"]
        # we check the username against our authorized users.csv file
        with open(Path(__file__).parent.resolve().joinpath("users.csv")) as csvfile:
            users = DictReader(csvfile)
            for user in users:
                if user["email_address"] == email_address:
                    return user
        bottle.abort(403)
    else:
        bottle.abort(403)


if __name__ == "__main__":
    # supply a user when running bottle locally
    debug_user = {"email_address": "hello@example.com", "display_name": "World"}
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
    application = bottle.default_app()
