""" A Simple file server for uploading and downloading files """
import json
import os
from argparse import ArgumentParser
from pathlib import Path

from boltons.iterutils import first
from flask import Flask, request, send_from_directory, safe_join, abort, Response
from flask._compat import fspath
from flask_compress import Compress
from flask_cors import CORS

from config import config

DEFAULT_UPLOAD_FOLDER = "/mnt/fileserver"

app = Flask(__name__)
CORS(app, **config.get("fileserver.cors"))
Compress(app)

app.config["UPLOAD_FOLDER"] = first(
    (os.environ.get(f"{prefix}_UPLOAD_FOLDER") for prefix in ("CLEARML", "TRAINS")),
    default=DEFAULT_UPLOAD_FOLDER,
)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = config.get(
    "fileserver.download.cache_timeout_sec", 5 * 60
)


@app.route("/", methods=["POST"])
def upload():
    results = []
    for filename, file in request.files.items():
        if not filename:
            continue
        file_path = filename.lstrip(os.sep)
        target = Path(safe_join(app.config["UPLOAD_FOLDER"], file_path))
        target.parent.mkdir(parents=True, exist_ok=True)
        file.save(str(target))
        results.append(file_path)
    return json.dumps(results), 200


@app.route("/<path:path>", methods=["GET"])
def download(path):
    as_attachment = "download" in request.args
    response = send_from_directory(
        app.config["UPLOAD_FOLDER"], path, as_attachment=as_attachment
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
    path = Path(
        safe_join(
            fspath(app.config["UPLOAD_FOLDER"]),
            fspath(path)
        )
    )
    if not path.exists() or path.is_file():
        abort(Response(f"File {str(path)} not found", 404))

    path.unlink()
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
