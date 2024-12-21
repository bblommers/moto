from typing import Any


class BadSegmentException(Exception):
    def __init__(
        self,
        seg_id: str | None = None,
        code: str | None = None,
        message: str | None = None,
    ):
        self.id = seg_id
        self.code = code
        self.message = message

    def __repr__(self) -> str:
        return f"<BadSegment {self.id}-{self.code}-{self.message}>"

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.id is not None:
            result["Id"] = self.id
        if self.code is not None:
            result["ErrorCode"] = self.code
        if self.message is not None:
            result["Message"] = self.message

        return result
