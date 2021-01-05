from argparse import ArgumentParser

from flask import Flask

from apiserver.config_repo import config
from apiserver.server_init.app_sequence import AppSequence
from apiserver.server_init.request_handlers import RequestHandlers

app = Flask(__name__, static_url_path="/static")
AppSequence(app).start(request_handlers=RequestHandlers())


# =================== MAIN =======================
if __name__ == "__main__":
    p = ArgumentParser(description=__doc__)
    p.add_argument(
        "--port", "-p", type=int, default=config.get("apiserver.listen.port")
    )
    p.add_argument("--ip", "-i", type=str, default=config.get("apiserver.listen.ip"))
    p.add_argument(
        "--debug", action="store_true", default=config.get("apiserver.debug")
    )
    p.add_argument(
        "--watch", action="store_true", default=config.get("apiserver.watch")
    )
    args = p.parse_args()

    # logging.info("Starting API Server at %s:%s and env '%s'" % (args.ip, args.port, config.env))

    app.run(
        debug=args.debug,
        host=args.ip,
        port=args.port,
        threaded=True,
        use_reloader=args.watch,
    )
