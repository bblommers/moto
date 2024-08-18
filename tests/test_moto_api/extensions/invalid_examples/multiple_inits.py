from moto.extensions import AbstractExtension


class NoOpExtensionWithMultipleInit(AbstractExtension):
    def __init__(self):
        self.my_id = "noop with init"

    def __init__(self):
        self.my_id = "second init"

    def extension_id(self):
        return self.my_id
