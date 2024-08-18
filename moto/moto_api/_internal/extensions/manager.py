from typing import List, Tuple

from moto.extensions import AbstractExtension


class ExtensionManager:
    def __init__(self):
        self.extensions: List[Tuple[int, AbstractExtension]] = []

    def add_extension(self, index, extension: AbstractExtension):
        self.extensions.append((index, extension))
        self.extensions = sorted(self.extensions, key=lambda x: x[0])

    def process_request(self, service_feature: str, request):
        for _, extension in self.extensions:
            if service_feature in extension.invoke_on():
                result = extension.request_hook(request)
                if result:
                    return result

    def process_response(self, service_feature: str, status, headers, body):
        for _, extension in self.extensions:
            if service_feature in extension.invoke_on():
                result = extension.response_hook(status, headers, body)
                if result:
                    return result
