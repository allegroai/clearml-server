import secrets
import string


def get_random_string(length):
    """
    Create a random crypto-safe sequence of 'length' or more characters
    Possible characters: alphanumeric, '-' and '_'
    Make sure that it starts from alphanumeric for better compatibility with yaml files
    """
    token = secrets.token_urlsafe(length)
    for _ in range(10):
        if not (token.startswith("-") or token.startswith("_")):
            break
        token = secrets.token_urlsafe(length)

    return token


def get_client_id(
    length: int = 30, allowed_chars: str = string.ascii_uppercase + string.digits
) -> str:
    """
    Create a random client id composed of 'length' upper case characters or digits
    """
    return "".join(secrets.choice(allowed_chars) for _ in range(length))


def get_secret_key(length: int = 50) -> str:
    """
    Create a random secret key
    """
    return get_random_string(length)


if __name__ == "__main__":
    print(get_client_id())
    print(get_secret_key())
