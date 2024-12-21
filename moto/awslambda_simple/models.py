from typing import Any, Optional

from moto.awslambda.models import LambdaBackend
from moto.core.base_backend import BackendDict


class LambdaSimpleBackend(LambdaBackend):
    """
    Implements a Lambda-Backend that does not use Docker containers, will always succeed.
    Annotate your tests with `@mock_aws(config={"lambda": {"use_docker": False}}) to use this Lambda-implementation.
    """

    def __init__(self, region_name: str, account_id: str):
        super().__init__(region_name, account_id)
        self.lambda_simple_results_queue: list[str] = []

    # pylint: disable=unused-argument
    def invoke(
        self,
        function_name: str,
        qualifier: str | None,
        body: Any,
        headers: Any,
        response_headers: Any,
    ) -> Optional[str | bytes]:
        default_result = body or "Simple Lambda happy path OK"
        if self.lambda_simple_results_queue:
            default_result = self.lambda_simple_results_queue.pop(0)
        return str.encode(default_result)


lambda_simple_backends = BackendDict(LambdaSimpleBackend, "lambda")
