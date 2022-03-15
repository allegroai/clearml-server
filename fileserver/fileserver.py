""" A Simple file server for uploading and downloading files """
import json
import mimetypes
import os
from argparse import ArgumentParser
from pathlib import Path

from boltons.iterutils import first
from flask import Flask, request, send_from_directory, abort, Response
from flask_compress import Compress
from flask_cors import CORS
from werkzeug.exceptions import NotFound
from werkzeug.security import safe_join

from config import config
from utils import get_env_bool

DEFAULT_UPLOAD_FOLDER = "/mnt/fileserver"

app = Flask(__name__)
CORS(app, **config.get("fileserver.cors"))

if get_env_bool("CLEARML_COMPRESS_RESP", default=True):
    Compress(app)

app.config["UPLOAD_FOLDER"] = first(
    (os.environ.get(f"{prefix}_UPLOAD_FOLDER") for prefix in ("CLEARML", "TRAINS")),
    default=DEFAULT_UPLOAD_FOLDER,
)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = config.get(
    "fileserver.download.cache_timeout_sec", 5 * 60
)


@app.before_request
def before_request():
    if request.content_encoding:
        return f"Content encoding is not supported ({request.content_encoding})", 415


@app.after_request
def after_request(response):
    response.headers["server"] = config.get(
        "fileserver.response.headers.server", "clearml"
    )
    return response


@app.route("/", methods=["POST"])
def upload():
    results = []
    for filename, file in request.files.items():
        if not filename:
            continue
        file_path = filename.lstrip(os.sep)
        safe_path = safe_join(app.config["UPLOAD_FOLDER"], file_path)
        if safe_path is None:
            raise NotFound()
        target = Path(safe_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        file.save(str(target))
        results.append(file_path)
    return json.dumps(results), 200


@app.route("/<path:path>", methods=["GET"])
def download(path):
    as_attachment = "download" in request.args

    _, encoding = mimetypes.guess_type(os.path.basename(path))
    mimetype = "application/octet-stream" if encoding == "gzip" else None

    response = send_from_directory(
        app.config["UPLOAD_FOLDER"],
        path,
        as_attachment=as_attachment,
        mimetype=mimetype,
    )
    if config.get("fileserver.download.disable_browser_caching", False):
        headers = response.headers
        headers["Pragma-directive"] = "no-cache"
        headers["Cache-directive"] = "no-cache"
        headers["Cache-control"] = "no-cache"
        headers["Pragma"] = "no-cache"
        headers["Expires"] = "0"
    return response


@app.route("/<path:path>", methods=["DELETE"])
def delete(path):
    real_path = Path(safe_join(os.fspath(app.config["UPLOAD_FOLDER"]), os.fspath(path)))
    if not real_path.exists() or not real_path.is_file():
        abort(Response(f"File {str(path)} not found", 404))

    real_path.unlink()
    return json.dumps(str(path)), 200


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--port", "-p", type=int, default=8081, help="Port (default %(default)d)"
    )
    parser.add_argument(
        "--ip", "-i", type=str, default="0.0.0.0", help="Address (default %(default)s)"
    )
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument(
        "--upload-folder",
        "-u",
        help=f"Upload folder (default {DEFAULT_UPLOAD_FOLDER})",
    )
    args = parser.parse_args()

    if args.upload_folder is not None:
        app.config["UPLOAD_FOLDER"] = args.upload_folder

    app.run(debug=args.debug, host=args.ip, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
