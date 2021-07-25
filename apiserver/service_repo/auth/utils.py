import random
sys_random = random.SystemRandom()


def get_random_string(length=12, allowed_chars='abcdefghijklmnopqrstuvwxyz'
                                               'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'):
    """
    Returns a securely generated random string.

    The default length of 12 with the a-z, A-Z, 0-9 character set returns
    a 71-bit value. log_2((26+26+10)^12) =~ 71 bits.

    Taken from the django.utils.crypto module.
    """
    return ''.join(sys_random.choice(allowed_chars) for _ in range(length))


def get_client_id(length=20):
    """
    Create a random secret key.

    Taken from the Django project.
    """
    chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return get_random_string(length, chars)


def get_secret_key(length=50):
    """
    Create a random secret key.

    Taken from the Django project.
    NOTE: asterisk is not supported due to issues with environment variables containing
     asterisks (in case the secret key is stored in an environment variable)
    """
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return get_random_string(length, chars)
