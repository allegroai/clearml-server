import requests
from furl import furl
from requests.auth import HTTPBasicAuth

from config import config


class SimpleSession:
    def __init__(self):
        self.host = config["hosts.api_server"]
        if not self.host.startswith("http"):
            self.host = f"http://{self.host}"

        self.key = config.get("secure.credentials.webserver.user_key")
        self.secret = config.get("secure.credentials.webserver.user_secret")

        self.auth = HTTPBasicAuth(self.key, self.secret)

    def send_request(self, endpoint, json=None):
        url = furl(self.host).set(path=endpoint)
        return requests.get(str(url), json=json, auth=self.auth)
