from moto.extensions import AbstractExtension


class SomeClass:
    pass


class OtherClass:
    pass


class NoOpExtension(AbstractExtension):
    def extension_id(self):
        return "noop with classes"
