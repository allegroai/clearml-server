if __name__ == '__main__':
    from pathlib import Path
    from apierrors import _error_codes
    from apierrors.autogen import generate

    generate(Path(__file__).parent.parent, _error_codes)
