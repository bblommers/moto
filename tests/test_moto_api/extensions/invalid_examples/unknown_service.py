from typing import List

from moto.extensions import AbstractExtension


class UnknownService(AbstractExtension):
    def invoke_on(self) -> List[str]:
        return ["Unknown:GetParameters"]

    def extension_id(self):
        return "extension with unknown service"
