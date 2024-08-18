from moto.extensions import AbstractExtension


class NoOpExtensionWithInit(AbstractExtension):
    def __init__(self):
        self.my_id = "noop with init"

    def extension_id(self):
        return self.my_id
