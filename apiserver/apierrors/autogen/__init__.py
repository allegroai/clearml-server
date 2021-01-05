def generate(path, error_codes):
    from .generator import Generator
    from pathlib import Path
    Generator(Path(path) / 'errors', format_pep8=False).make_errors(error_codes)
