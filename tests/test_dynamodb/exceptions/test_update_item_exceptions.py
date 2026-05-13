import botocore
import pytest
from botocore.exceptions import ClientError

from tests.test_dynamodb import dynamodb_aws_verified


@pytest.mark.aws_verified
@dynamodb_aws_verified(create_table=False)
def test_update_item_with_empty_table_name(table_name=None):
    session = botocore.session.Session()
    config = botocore.client.Config(parameter_validation=False)
    client = session.create_client("dynamodb", region_name="us-east-1", config=config)

    # check using wrong name for sort key throws exception
    with pytest.raises(ClientError) as exc:
        client.update_item(
            TableName="",
            Key={"pk": {"S": "foo"}},
            UpdateExpression="ADD stringset :emptySet",
            ExpressionAttributeValues={":emptySet": {"SS": ()}},
        )
    err = exc.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert (
        err["Message"]
        == "1 validation error detected: Value '' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 1"
    )


@pytest.mark.aws_verified
@dynamodb_aws_verified(create_table=False)
def test_update_item_with_invalid_table_name(table_name=None):
    session = botocore.session.Session()
    config = botocore.client.Config(parameter_validation=False)
    client = session.create_client("dynamodb", region_name="us-east-1", config=config)

    # check using wrong name for sort key throws exception
    with pytest.raises(ClientError) as exc:
        client.update_item(
            TableName="x!",
            Key={"pk": {"S": "foo"}},
            UpdateExpression="ADD stringset :emptySet",
            ExpressionAttributeValues={":emptySet": {"SS": ()}},
        )
    err = exc.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert (
        err["Message"]
        == "2 validation errors detected: Value 'x!' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3; Value 'x!' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
    )
