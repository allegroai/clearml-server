import json
from argparse import ArgumentParser
from operator import attrgetter
from os.path import join

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    send_from_directory,
    flash,
    make_response,
    send_file,
    session,
)
from flask_compress import Compress
from flask_login import (
    LoginManager,
    login_required,
    logout_user,
    login_user,
    current_user,
)

from config import config
from user import (
    UserFactory,
    AUTH_TOKEN_COOKIE_KEY,
    CreateUserError,
)

log = config.logger(__file__)

log.info("################ Web Server initializing #####################")

app = Flask(__name__)
app.config.from_mapping(config.get("webserver.flask"))
app.config.update(
    SECRET_KEY=config["secure.http.session_secret.webserver"],
    COMPRESS_MIMETYPES=[
        "text/html",
        "text/css",
        "text/xml",
        "application/json",
        "application/javascript",
        "text/javascript",
    ],
    COMPRESS_LEVEL=9,
)

login_manager = LoginManager(app)
login_manager.login_view = "login"

Compress(app)


def _secure_url_for(endpoint, external=True, **values):
    if not config.get("webserver.redirect_to_https", False):
        return url_for(endpoint, _external=external, **values)
    return url_for(endpoint, _external=external, _scheme="https", **values)


@login_manager.user_loader
def load_user(id):
    return UserFactory.get_class().get(id)


@login_manager.unauthorized_handler
def handle_needs_login():
    session.pop("_flashes", None)
    flash("You have to be logged in to access this page.")
    return redirect(
        _secure_url_for(".login", next=request.endpoint, **request.view_args)
    )


@app.route("/create_user", methods=["GET", "POST"])
def create_user():
    data = request.args or request.form
    name = data["name"].strip()

    try:
        user = UserFactory.get_class().create_by_name(name)
    except CreateUserError as e:
        session.pop("_flashes", None)
        message = e.args[0]
        if "value combination" in message.lower():
            message = f"Failed creating user {name}"
        flash(message)
        return redirect(_secure_url_for(".login"))

    return _complete_user_login(user)


@app.route("/login/<user_id>")
def login_by_id(user_id):
    if current_user.get_id() == user_id and current_user.is_authenticated:
        return redirect(_secure_url_for(".login"))

    try:
        user = load_user(user_id)
    except Exception as e:
        # Some callback issue, try again.
        # For example, the user tried to reload the callback page and the provider token was already redeemed.
        session.pop("_flashes", None)
        if e.args:
            flash(e.args[0])
        else:
            flash(repr(e))
        return redirect(_secure_url_for(".login"))

    args = dict(request.args)
    endpoint = args.pop("next", ".index")

    return _complete_user_login(user, endpoint, **request.args)


def _complete_user_login(user, endpoint=".index", **kwargs):
    login_user(user, True)
    response = redirect(_secure_url_for(endpoint, **kwargs))
    set_response_cookie(response, user)
    return response


def set_response_cookie(response, user, copy_request=None):
    if copy_request and AUTH_TOKEN_COOKIE_KEY in copy_request.cookies:
        token = request.cookies[AUTH_TOKEN_COOKIE_KEY]
    else:
        token = user.token
    response.set_cookie(
        AUTH_TOKEN_COOKIE_KEY,
        token,
        **config.get("webserver.auth.cookies"),
    )


@app.route("/logout")
def logout():
    logout_user()
    return redirect(_secure_url_for(".login"))


@app.route("/static/network/")
@login_required
def send_netron():
    return send_from_directory("static", join("network", "index.html"))


@app.route("/static/<path:path>")
@login_required
def send_static(path):
    return send_from_directory("static", path)


@app.route("/webapp_conf.js")
def webapp_conf():
    webapp_conf = _get_webapp_conf()
    response = make_response(f"SM_CONFIG={json.dumps(webapp_conf)}")
    response.content_type = "text/javascript"
    return response


def _get_webapp_conf():
    webapp_conf = config.get("webapp")

    webapp_build = None
    webapp_path = "pages/webapp"
    try:
        with open(f"{webapp_path}/assets/build.json") as data_file:
            webapp_build = json.load(data_file)
        webapp_build["branch"] = webapp_build["branch"].replace("@", "")
    except Exception:
        webapp_build = {}
    backend_path = "."
    try:
        with open(f"{backend_path}/static/build.json") as data_file:
            backend_build = json.load(data_file)
    except Exception:
        backend_build = {}
    webapp_conf.put("backend_build", backend_build)
    webapp_conf.put("webapp_build", webapp_build)
    webapp_conf.put("env", config.env)

    return webapp_conf


@app.route("/login")
def login():
    users = sorted((user.user_data for user in UserFactory.get_class().get_all()), key=attrgetter("name"))
    response = make_response(render_template("login.html", users=users))
    # make sure to clear out session token cookie
    response.set_cookie(
        AUTH_TOKEN_COOKIE_KEY, "", expires=0, **config.get("webserver.auth.cookies")
    )
    return response


def _serve_webapp(path=None):
    if not path:
        response = make_response(send_file("pages/webapp/index.html"))
    else:
        try:
            response = make_response(send_from_directory("pages/webapp", path))
        except Exception:
            response = make_response(send_file("pages/webapp/index.html"))
    set_response_cookie(response, current_user, request)
    response.headers["X-Trains-Environment"] = "oss"
    return response


@app.route("/favicon.ico")
def favicon():
    return send_from_directory("static", "favicon.ico")


@app.route("/")
def index():
    if not current_user.is_authenticated:
        return redirect(_secure_url_for(".login"))
    return _serve_webapp()


@app.route("/<path:path>")
@login_required
def webapp(path=None):
    return _serve_webapp(path)


def parse_args():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--port", "-p", type=int, default=config.get("webserver.listen.port")
    )
    parser.add_argument("--ip", "-i", type=str, default=config.get("webserver.listen.ip"))
    parser.add_argument(
        "--debug", action="store_true", default=config.get("webserver.debug")
    )
    return parser.parse_args()


def main():
    args = parse_args()
    app.run(debug=args.debug, host=args.ip, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
