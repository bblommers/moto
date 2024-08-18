from typing import List


class AbstractExtension:
    def extension_id(self) -> str:
        """
        Unique (non-empty) name of this extension. Required.
        """
        raise NotImplementedError

    def invoke_on(self) -> List[str]:
        """
        List of services/features this Extension should be invoked on.
        Example: ['EC2:RunInstances', 'SSM:GetParameter']
        """
        return []

    def request_hook(self, request):
        pass

    def response_hook(self, status, headers, body):
        pass
