from typing import Any, SupportsFloat


def parse_expression(
    expression: str, results: list[dict[str, Any]]
) -> tuple[list[SupportsFloat], list[str]]:
    values: list[SupportsFloat] = []
    timestamps: list[str] = []
    for result in results:
        if result.get("id") == expression:
            values.extend(result["vals"])
            timestamps.extend(result["timestamps"])
    return values, timestamps
