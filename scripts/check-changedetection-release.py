"""Capture real read-only API responses from a pinned release and generated state."""
import ast
import json
import secrets
import socket
import sys
import tempfile
import threading
import time
import uuid
from importlib.metadata import distribution, version
from types import SimpleNamespace

RELEASE = "0.70.4"
if version("changedetection.io") != RELEASE:
    raise SystemExit("Expected changedetection.io " + RELEASE)

blocked = []


def isolate(event, args):
    socket_activity = event.startswith("socket.") and event != "socket.gethostname"
    launch = event in {"subprocess.Popen", "os.fork", "os.exec", "os.posix_spawn", "os.system"}
    if socket_activity or launch:
        blocked.append((event, args[1] if event == "socket.__new__" else None))
        raise RuntimeError("Sockets and subprocesses are disabled for this check")


sys.addaudithook(isolate)
# Some urllib3 builds probe IPv6 support at import. The guard blocks socket creation.
import urllib3
assert blocked in ([], [("socket.__new__", socket.AF_INET6)]), blocked
probes = len(blocked)
blocked.clear()

from loguru import logger
logger.remove()
from flask import Flask
from flask_restful import Api
from werkzeug.routing import UnicodeConverter
from changedetectionio.api import Watch, WatchHistory, WatchSingleHistory, WatchHistoryDiff
from changedetectionio.model.Watch import model


def release_routes():
    # Read the installed routing table without importing the full application.
    wiring = distribution("changedetection.io").locate_file("changedetectionio/flask_app.py")
    routes = {}
    for node in ast.walk(ast.parse(wiring.read_text(encoding="utf-8"))):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "add_resource" and len(node.args) >= 2
                and isinstance(node.args[0], ast.Name) and isinstance(node.args[1], ast.Constant)):
            routes[node.args[0].id] = node.args[1].value
    return routes


def capture(directory):
    key = secrets.token_hex(32)
    settings = {"application": {"api_access_token_enabled": True, "api_access_token": key}}
    data = {"settings": settings, "watching": {}}
    store = SimpleNamespace(data=data, lock=threading.RLock())
    stamp = int(time.time()) - 2
    first, second = str(stamp), str(stamp + 1)
    watch_id = str(uuid.uuid4())
    initial = {"uuid": watch_id, "last_checked": stamp + 1, "url": "http://127.0.0.1/"}
    watch = model(datastore_path=directory, **{"__datastore": data, "default": initial})
    data["watching"][watch_id] = watch

    app = Flask(__name__)
    app.testing = True
    # All requests use the generated valid UUID. The handlers consume the string form.
    app.url_map.converters["uuid_str"] = UnicodeConverter
    api = Api(app)
    routes = release_routes()
    for resource in [Watch, WatchHistory, WatchSingleHistory, WatchHistoryDiff]:
        extra = {"update_q": None} if resource is Watch else {}
        api.add_resource(resource, routes[resource.__name__], methods=["GET"],
                         resource_class_kwargs={"datastore": store, **extra})
    client = app.test_client()
    prefix = "/api/v1/watch/" + watch_id

    def read(suffix, responses=None, mime="application/json", status=200, authenticated=True):
        response = client.get(prefix + suffix, headers={"x-api-key": key} if authenticated else {})
        assert (response.status_code, response.mimetype) == (status, mime), suffix
        body = response.get_data(as_text=True)
        if responses is not None:
            responses[suffix] = {"status": status, "type": response.content_type, "body": body}
        return json.loads(body) if mime == "application/json" else body

    def point_history(responses, keys):
        info = read("", responses)
        assert info["uuid"] == watch_id and info["processor"] == "text_json_diff"
        assert info["paused"] is False and info["last_error"] is False
        assert info["last_checked"] == stamp + 1
        assert info["last_changed"] == (int(keys[-1]) if len(keys) > 1 else 0)
        assert info["history_n"] == len(keys)
        assert set(read("/history", responses)) == set(keys)

    read("", status=403, authenticated=False)
    one, two = {}, {}
    watch.save_history_blob("waiting", first, "first")
    point_history(one, [first])
    assert read("/history/latest", one, "text/plain") == "waiting"
    assert read("/history/" + first, one, "text/plain") == "waiting"
    watch.save_history_blob("available", second, "second")
    point_history(two, [first, second])
    assert read("/history/" + first, two, "text/plain") == "waiting"
    assert read("/history/" + second, two, "text/plain") == "available"
    assert read("/history/latest", two, "text/plain") == "available"
    diff = read("/difference/" + first + "/" + second + "?format=text", two, "text/plain")
    assert "waiting" in diff and "available" in diff
    read("/history/1", status=404)
    read("/difference/1/" + second + "?format=text", status=404)
    assert not blocked, blocked
    return {
        "release": RELEASE, "watchId": watch_id, "now": (stamp + 1) * 1000,
        "first": first, "second": second, "one": one, "two": two, "blockedImportProbes": probes,
    }


with tempfile.TemporaryDirectory(prefix="watch-release-state-") as temp:
    print(json.dumps(capture(temp)))
