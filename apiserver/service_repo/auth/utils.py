import random
import string

sys_random = random.SystemRandom()


def get_random_string(
    length: int = 12, allowed_chars: str = string.ascii_letters + string.digits
) -> str:
    """
    Returns a securely generated random string.

    The default length of 12 with the a-z, A-Z, 0-9 character set returns
    a 71-bit value. log_2((26+26+10)^12) =~ 71 bits.

    Taken from the django.utils.crypto module.
    """
    return "".join(sys_random.choice(allowed_chars) for _ in range(length))


def get_client_id(length: int = 20) -> str:
    """
    Create a random secret key.

    Taken from the Django project.
    """
    chars = string.ascii_uppercase + string.digits
    return get_random_string(length, chars)


def get_secret_key(length: int = 50) -> str:
    """
    Create a random secret key.

    Taken from the Django project.
    NOTE: asterisk is not supported due to issues with environment variables containing
     asterisks (in case the secret key is stored in an environment variable)
    """
    chars = string.ascii_letters + string.digits
    return get_random_string(length, chars)
