import re
from io import BytesIO
from botocore.awsrequest import AWSResponse

import moto.backend_index as backend_index
from typing import Any, Union


class MockRawResponse(BytesIO):
    def __init__(self, response_input: Union[str, bytes]):
        if isinstance(response_input, str):
            response_input = response_input.encode("utf-8")
        super().__init__(response_input)

    def stream(self, **kwargs: Any) -> Any:  # pylint: disable=unused-argument
        contents = self.read()
        while contents:
            yield contents
            contents = self.read()


class BotocoreStubOnDemand:
    def __init__(self) -> None:
        self.enabled = False
        self.loaded_backends = {}

    def reset(self) -> None:
        print("BotocoreStubOnDemand.reset()")
        #self.methods.clear()
        #for k in self.loaded_backends.values():
        #    k.reset()
        import moto.backends as backends
        # TODO: we should have a BackendManager
        # Everytime we need a Backend, we say
        # backend_manager.ec2(region, account)
        #
        # Resetting can then be done using
        # b.reset() for b in backend_manager.loaded_backends()

    def __call__(self, event_name: str, request: Any, **kwargs: Any) -> AWSResponse:
        if not self.enabled:
            return None

        for service, pattern in backend_index.backend_url_patterns:
            if pattern.match(request.url):

                import moto.backends as backends
                from moto.core import BackendDict, DEFAULT_ACCOUNT_ID
                from moto.core.exceptions import HTTPException
                from .utils import convert_flask_to_responses_response
                backend_dict = backends.get_backend(service)
                print(backend_dict)

                if isinstance(backend_dict, BackendDict):
                    if "us-east-1" in backend_dict[DEFAULT_ACCOUNT_ID]:
                        backend = backend_dict[DEFAULT_ACCOUNT_ID]["us-east-1"]
                    else:
                        backend = backend_dict[DEFAULT_ACCOUNT_ID]["global"]
                else:
                    backend = backend_dict["global"]
                self.loaded_backends[service] = backend

                for header, value in request.headers.items():
                    if isinstance(value, bytes):
                        request.headers[header] = value.decode("utf-8")

                for url, method_to_execute in backend.urls.items():
                    if re.compile(url).match(request.url):
                        from moto.moto_api import recorder
                        try:
                            recorder._record_request(request)
                            status, headers, body = method_to_execute(request, request.url, request.headers)
                        except HTTPException as e:
                            status = e.code  # type: ignore[assignment]
                            headers = e.get_headers()  # type: ignore[assignment]
                            body = e.get_body()

                        raw_response = MockRawResponse(body)
                        return AWSResponse(request.url, status, headers, raw_response)

                return AWSResponse(request.url, 404, {}, "Not yet implemented")
        return "now what"
