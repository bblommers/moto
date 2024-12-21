from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from py_partiql_parser import QueryMetadata


def query(
    statement: str, source_data: dict[str, str], parameters: list[dict[str, Any]]
) -> tuple[
    list[dict[str, Any]],
    dict[str, list[tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]]],
]:
    from py_partiql_parser import DynamoDBStatementParser

    return DynamoDBStatementParser(source_data).parse(statement, parameters)


def get_query_metadata(statement: str) -> "QueryMetadata":
    from py_partiql_parser import DynamoDBStatementParser

    return DynamoDBStatementParser.get_query_metadata(query=statement)
