# CALTECH ARCHIVES AND SPECIAL COLLECTIONS
# digital object preservation workflow

# bottlepy web application; see distill.py for processing functionality

# bottle requires gevent.monkey.patch_all()
# fmt: off
from gevent import monkey; monkey.patch_all()
# fmt: on

from pathlib import Path

import tailer
from bottle import default_app, error, get, post, request, response, run, template
from decouple import config


@error(403)
def error403(error):
    return "Please contact Archives & Special Collections or Digital Library Development if you should have access."


@get("/")
def form_collection_id():
    return template("form")


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


if __name__ == "__main__":
    run(host="localhost", port=1234, server="gevent", reloader=True, debug=True)
else:
    # for attaching Bottle to Apache using mod_wsgi
    application = default_app()
