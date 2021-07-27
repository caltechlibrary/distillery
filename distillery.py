# CALTECH ARCHIVES AND SPECIAL COLLECTIONS
# digital object preservation workflow

# bottlepy web application; see distill.py for processing functionality

# bottle requires gevent.monkey.patch_all()
# fmt: off
from gevent import monkey; monkey.patch_all()
# fmt: on

from csv import DictReader
from pathlib import Path

import tailer
from bottle import (
    abort,
    default_app,
    error,
    get,
    post,
    request,
    response,
    run,
    template,
)
from decouple import config


@error(403)
def error403(error):
    return "Please contact Archives & Special Collections or Digital Library Development if you should have access."


@get("/")
def form_collection_id():
    # we pass the user dictionary to the template
    return template("form", user=authorize_user())


@post("/distilling")
def begin_processing():
    collection_id = request.forms.get("collection_id").strip()
    process = request.forms.get("process").strip()
    # write a file for alchemist.sh to find
    Path(config("PROCESSING_FILES")).joinpath(f"{collection_id}-init-{process}").touch()
    # write a file for the event stream
    Path(config("PROCESSING_FILES")).joinpath(f"{collection_id}-processing").touch()
    return template("distilling", collection_id=collection_id)


@get("/distill/<collection_id>")
def stream(collection_id):
    # using server-sent events that work with javascript in distilling.tpl
    response.content_type = "text/event-stream"
    response.cache_control = "no-cache"

    with open(
        Path(config("PROCESSING_FILES")).joinpath(f"{collection_id}-processing")
    ) as f:
        for line in tailer.follow(f):
            # the event stream format starts with "data: " and ends with "\n\n"
            # https://web.archive.org/web/20210701185847/https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format
            if line.startswith("🟢"):
                # we send an event field targeting a specific listener without data
                yield f"event: init\n"
            else:
                yield f"data: {line}\n\n"


def authorize_user():
    if debug_user:
        # debug_user is set when running bottle locally
        return debug_user
    elif request.environ.get("REMOTE_USER", None):
        # REMOTE_USER will be set by basic auth and shibboleth
        username = request.environ["REMOTE_USER"]
        # we check the username against our authorized users.csv file
        with open(Path(__file__).parent.resolve().joinpath("users.csv")) as csvfile:
            users = DictReader(csvfile)
            for user in users:
                if user["username"] == username:
                    print(user)
                    return user
        abort(403)
    else:
        abort(403)


if __name__ == "__main__":
    # supply a user when running bottle locally
    debug_user = {
        "username": "hello",
        "display_name": "World!",
        "email_address": "hello@example.com",
    }
    run(host="localhost", port=1234, server="gevent", reloader=True, debug=True)
else:
    # fmt: off

    # change working directory so relative paths (and template lookup) work again
    import os
    os.chdir(os.path.dirname(__file__))

    # set the variable to avoid a NameError
    debug_user = None

    # bottle requires gevent.monkey.patch_all()
    from gevent import monkey
    monkey.patch_all()

    # for attaching Bottle to Apache using mod_wsgi
    application = default_app()

    # fmt: on
