from moto.extensions import AbstractExtension


class NoIdentifier(AbstractExtension):
    # Typo - we should implement `extension_id` instead
    def extension_id_with_typo(self):
        return "typo"
