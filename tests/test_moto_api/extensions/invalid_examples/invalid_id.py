from moto.extensions import AbstractExtension


class InvalidIdentifier(AbstractExtension):
    #  we should return a non-empty string
    def extension_id(self):
        return ""
