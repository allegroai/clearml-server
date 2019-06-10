from semantic_version import Version


class PartialVersion(Version):
    def __init__(self, version_string: str):
        assert isinstance(version_string, str)
        super().__init__(version_string, partial=True)
