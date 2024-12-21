from typing import Any

from moto.core.exceptions import JsonRESTError


class LogsClientError(JsonRESTError):
    code = 400


class ResourceNotFoundException(LogsClientError):
    def __init__(self, msg: str | None = None):
        self.code = 400
        super().__init__(
            "ResourceNotFoundException",
            msg or "The specified log group does not exist.",
        )


class InvalidParameterException(LogsClientError):
    def __init__(
        self,
        msg: str | None = None,
        constraint: str | None = None,
        parameter: str | None = None,
        value: Any = None,
    ):
        self.code = 400
        if constraint:
            msg = f"1 validation error detected: Value '{value}' at '{parameter}' failed to satisfy constraint: {constraint}"
        super().__init__(
            "InvalidParameterException", msg or "A parameter is specified incorrectly."
        )


class ResourceAlreadyExistsException(LogsClientError):
    def __init__(self) -> None:
        self.code = 400
        super().__init__(
            "ResourceAlreadyExistsException", "The specified log group already exists"
        )


class LimitExceededException(LogsClientError):
    def __init__(self) -> None:
        self.code = 400
        super().__init__("LimitExceededException", "Resource limit exceeded.")
