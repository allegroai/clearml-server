if __name__ == '__main__':
    from pathlib import Path
    from apiserver.apierrors import _error_codes
    from apiserver.apierrors.autogen import generate

    generate(Path(__file__).parent.parent, _error_codes)
