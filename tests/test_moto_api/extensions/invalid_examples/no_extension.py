from moto.extensions import AbstractExtension


# AbstractExtension is imported, but we forgot to extend our class
class NoExtension:
    def extension_id(self):
        return "no"
