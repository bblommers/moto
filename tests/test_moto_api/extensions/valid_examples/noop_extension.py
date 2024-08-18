from moto.extensions import AbstractExtension


class NoOpExtension(AbstractExtension):
    def extension_id(self):
        return "noop"
