from typing import List

from moto.extensions import AbstractExtension


class EnrichSSMParameterExtension(AbstractExtension):
    def invoke_on(self) -> List[str]:
        return ["ssm:GetParameters"]

    def extension_id(self):
        return "ssm"
