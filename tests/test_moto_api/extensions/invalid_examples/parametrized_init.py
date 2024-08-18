from moto.extensions import AbstractExtension


class ParametrizedExtension(AbstractExtension):
    def __init__(self, name: str):
        self.my_id = "parametrized"

    def extension_id(self):
        return self.my_id
