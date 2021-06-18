# CALTECH ARCHIVES AND SPECIAL COLLECTIONS
# digital object preservation workflow

# bottlepy web application; see distill.py for processing functionality

# bottle requires gevent.monkey.patch_all()
# fmt: off
from gevent import monkey; monkey.patch_all()
# fmt: on

from pathlib import Path

import tailer
from bottle import get, post, request, response, run, template
from decouple import config


@get("/")
def form_collection_id():
    return template("form")


@post("/distilling")
def begin_processing():
    collection_id = request.forms.get("collection_id").strip()
    # write a file to a shared status directory for alchemist.sh to find
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
            if line.startswith("📅") or line.startswith("⛔️"):
                # we send an event field targeting a specific listener
                yield f"event: start\ndata: {line}\n\n"
            else:
                yield f"data: {line}\n\n"
            print(line)


if __name__ == "__main__":
    run(host="localhost", port=1234, server="gevent", reloader=True, debug=True)
