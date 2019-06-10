""" A Simple file server for uploading and downloading files """
import json
import logging.config
import os
from argparse import ArgumentParser
from pathlib import Path

from flask import Flask, request, send_from_directory, safe_join
from pyhocon import ConfigFactory

logging.config.dictConfig(ConfigFactory.parse_file("logging.conf"))

app = Flask(__name__)


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
    return (json.dumps(results), 200)


@app.route("/<path:path>", methods=["GET"])
def download(path):
    return send_from_directory(app.config["UPLOAD_FOLDER"], path)


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
        default="/mnt/fileserver",
        help="Upload folder (default %(default)s)",
    )
    args = parser.parse_args()

    app.config["UPLOAD_FOLDER"] = args.upload_folder

    app.run(debug=args.debug, host=args.ip, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
