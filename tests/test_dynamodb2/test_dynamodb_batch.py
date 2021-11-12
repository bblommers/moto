import boto3
import pytest
import re

from botocore.exceptions import ClientError
from moto import mock_dynamodb2

# Test all things related to batch - batch_get, batch_write, transact_get, transact_write


@mock_dynamodb2
def test_batch_items_returns_all():
    dynamodb = _create_user_table()
    returned_items = dynamodb.batch_get_item(
        RequestItems={
            "users": {
                "Keys": [
                    {"username": {"S": "user0"}},
                    {"username": {"S": "user1"}},
                    {"username": {"S": "user2"}},
                    {"username": {"S": "user3"}},
                ],
                "ConsistentRead": True,
            }
        }
    )["Responses"]["users"]
    assert len(returned_items) == 3
    assert [item["username"]["S"] for item in returned_items] == [
        "user1",
        "user2",
        "user3",
    ]


@mock_dynamodb2
def test_batch_items_throws_exception_when_requesting_100_items_for_single_table():
    dynamodb = _create_user_table()
    with pytest.raises(ClientError) as ex:
        dynamodb.batch_get_item(
            RequestItems={
                "users": {
                    "Keys": [
                        {"username": {"S": "user" + str(i)}} for i in range(0, 104)
                    ],
                    "ConsistentRead": True,
                }
            }
        )
    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    msg = ex.value.response["Error"]["Message"]
    msg.should.contain("1 validation error detected: Value")
    msg.should.contain(
        "at 'requestItems.users.member.keys' failed to satisfy constraint: Member must have length less than or equal to 100"
    )


@mock_dynamodb2
def test_batch_items_throws_exception_when_requesting_100_items_across_all_tables():
    dynamodb = _create_user_table()
    with pytest.raises(ClientError) as ex:
        dynamodb.batch_get_item(
            RequestItems={
                "users": {
                    "Keys": [
                        {"username": {"S": "user" + str(i)}} for i in range(0, 75)
                    ],
                    "ConsistentRead": True,
                },
                "users2": {
                    "Keys": [
                        {"username": {"S": "user" + str(i)}} for i in range(0, 75)
                    ],
                    "ConsistentRead": True,
                },
            }
        )
    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["Error"]["Message"].should.equal(
        "Too many items requested for the BatchGetItem call"
    )


@mock_dynamodb2
def test_batch_items_with_basic_projection_expression():
    dynamodb = _create_user_table()
    returned_items = dynamodb.batch_get_item(
        RequestItems={
            "users": {
                "Keys": [
                    {"username": {"S": "user0"}},
                    {"username": {"S": "user1"}},
                    {"username": {"S": "user2"}},
                    {"username": {"S": "user3"}},
                ],
                "ConsistentRead": True,
                "ProjectionExpression": "username",
            }
        }
    )["Responses"]["users"]

    returned_items.should.have.length_of(3)
    [item["username"]["S"] for item in returned_items].should.be.equal(
        ["user1", "user2", "user3"]
    )
    [item.get("foo") for item in returned_items].should.be.equal([None, None, None])

    # The projection expression should not remove data from storage
    returned_items = dynamodb.batch_get_item(
        RequestItems={
            "users": {
                "Keys": [
                    {"username": {"S": "user0"}},
                    {"username": {"S": "user1"}},
                    {"username": {"S": "user2"}},
                    {"username": {"S": "user3"}},
                ],
                "ConsistentRead": True,
            }
        }
    )["Responses"]["users"]

    [item["username"]["S"] for item in returned_items].should.be.equal(
        ["user1", "user2", "user3"]
    )
    [item["foo"]["S"] for item in returned_items].should.be.equal(["bar", "bar", "bar"])


@mock_dynamodb2
def test_batch_items_with_basic_projection_expression_and_attr_expression_names():
    dynamodb = _create_user_table()
    returned_items = dynamodb.batch_get_item(
        RequestItems={
            "users": {
                "Keys": [
                    {"username": {"S": "user0"}},
                    {"username": {"S": "user1"}},
                    {"username": {"S": "user2"}},
                    {"username": {"S": "user3"}},
                ],
                "ConsistentRead": True,
                "ProjectionExpression": "#rl",
                "ExpressionAttributeNames": {"#rl": "username"},
            }
        }
    )["Responses"]["users"]

    returned_items.should.have.length_of(3)
    [item["username"]["S"] for item in returned_items].should.be.equal(
        ["user1", "user2", "user3"]
    )
    [item.get("foo") for item in returned_items].should.be.equal([None, None, None])


@mock_dynamodb2
def test_batch_items_should_throw_exception_for_duplicate_request():
    client = _create_user_table()
    with pytest.raises(ClientError) as ex:
        client.batch_get_item(
            RequestItems={
                "users": {
                    "Keys": [
                        {"username": {"S": "user0"}},
                        {"username": {"S": "user0"}},
                    ],
                    "ConsistentRead": True,
                }
            }
        )
    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["Error"]["Message"].should.equal(
        "Provided list of item keys contains duplicates"
    )


@mock_dynamodb2
def test_batch_write_item():
    conn = boto3.resource("dynamodb", region_name="us-west-2")
    tables = [f"table-{i}" for i in range(3)]
    for name in tables:
        conn.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

    conn.batch_write_item(
        RequestItems={
            tables[0]: [{"PutRequest": {"Item": {"id": "0"}}}],
            tables[1]: [{"PutRequest": {"Item": {"id": "1"}}}],
            tables[2]: [{"PutRequest": {"Item": {"id": "2"}}}],
        }
    )

    for idx, name in enumerate(tables):
        table = conn.Table(f"table-{idx}")
        res = table.get_item(Key={"id": str(idx)})
        assert res["Item"].should.equal({"id": str(idx)})
        scan = table.scan()
        assert scan["Count"].should.equal(1)

    conn.batch_write_item(
        RequestItems={
            tables[0]: [{"DeleteRequest": {"Key": {"id": "0"}}}],
            tables[1]: [{"DeleteRequest": {"Key": {"id": "1"}}}],
            tables[2]: [{"DeleteRequest": {"Key": {"id": "2"}}}],
        }
    )

    for idx, name in enumerate(tables):
        table = conn.Table(f"table-{idx}")
        scan = table.scan()
        assert scan["Count"].should.equal(0)


def _create_user_table():
    client = boto3.client("dynamodb", region_name="us-east-1")
    client.create_table(
        TableName="users",
        KeySchema=[{"AttributeName": "username", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "username", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    client.put_item(
        TableName="users", Item={"username": {"S": "user1"}, "foo": {"S": "bar"}}
    )
    client.put_item(
        TableName="users", Item={"username": {"S": "user2"}, "foo": {"S": "bar"}}
    )
    client.put_item(
        TableName="users", Item={"username": {"S": "user3"}, "foo": {"S": "bar"}}
    )
    return client


@mock_dynamodb2
def test_invalid_transact_get_items():
    _create_multiple_tables()

    client = boto3.client("dynamodb", region_name="us-east-1")

    with pytest.raises(ClientError) as ex:
        client.transact_get_items(
            TransactItems=[
                {"Get": {"Key": {"id": {"S": "1"}}, "TableName": "test1"}}
                for i in range(26)
            ]
        )

    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Message"].should.match(
        r"failed to satisfy constraint: Member must have length less than or equal to 25",
        re.I,
    )

    with pytest.raises(ClientError) as ex:
        client.transact_get_items(
            TransactItems=[
                {"Get": {"Key": {"id": {"S": "1"},}, "TableName": "test1",}},
                {"Get": {"Key": {"id": {"S": "1"},}, "TableName": "non_exists_table",}},
            ]
        )

    ex.value.response["Error"]["Code"].should.equal("ResourceNotFoundException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Message"].should.equal("Requested resource not found")


@mock_dynamodb2
def test_transact_get_items_from_single_table():
    _create_multiple_tables()

    client = boto3.client("dynamodb", region_name="us-east-1")
    res = client.transact_get_items(
        TransactItems=[
            {
                "Get": {
                    "Key": {"id": {"S": "1"}, "sort_key": {"S": "1"}},
                    "TableName": "test1",
                }
            },
            {
                "Get": {
                    "Key": {"id": {"S": "non_exists_key"}, "sort_key": {"S": "2"}},
                    "TableName": "test1",
                }
            },
        ]
    )
    res["Responses"][0]["Item"].should.equal({"id": {"S": "1"}, "sort_key": {"S": "1"}})
    len(res["Responses"]).should.equal(2)
    res["Responses"][1].should.equal({})


@mock_dynamodb2
def test_transact_get_items_from_multiple_tables():
    _create_multiple_tables()

    client = boto3.client("dynamodb", region_name="us-east-1")
    res = client.transact_get_items(
        TransactItems=[
            {
                "Get": {
                    "Key": {"id": {"S": "1"}, "sort_key": {"S": "1"}},
                    "TableName": "test1",
                }
            },
            {
                "Get": {
                    "Key": {"id": {"S": "1"}, "sort_key": {"S": "2"}},
                    "TableName": "test1",
                }
            },
            {
                "Get": {
                    "Key": {"id": {"S": "1"}, "sort_key": {"S": "1"}},
                    "TableName": "test2",
                }
            },
        ]
    )

    res["Responses"][0]["Item"].should.equal({"id": {"S": "1"}, "sort_key": {"S": "1"}})
    res["Responses"][1]["Item"].should.equal({"id": {"S": "1"}, "sort_key": {"S": "2"}})
    res["Responses"][2]["Item"].should.equal({"id": {"S": "1"}, "sort_key": {"S": "1"}})


@mock_dynamodb2
def test_transact_get_items_with_consumed_capacity():
    _create_multiple_tables()

    client = boto3.client("dynamodb", region_name="us-east-1")
    res = client.transact_get_items(
        TransactItems=[
            {
                "Get": {
                    "Key": {"id": {"S": "1"}, "sort_key": {"S": "1"}},
                    "TableName": "test1",
                }
            },
            {
                "Get": {
                    "Key": {"id": {"S": "1"}, "sort_key": {"S": "2"}},
                    "TableName": "test1",
                }
            },
            {
                "Get": {
                    "Key": {"id": {"S": "1"}, "sort_key": {"S": "1"}},
                    "TableName": "test2",
                }
            },
        ],
        ReturnConsumedCapacity="TOTAL",
    )

    res["ConsumedCapacity"][0].should.equal(
        {"TableName": "test1", "CapacityUnits": 4.0, "ReadCapacityUnits": 4.0}
    )
    res["ConsumedCapacity"][1].should.equal(
        {"TableName": "test2", "CapacityUnits": 2.0, "ReadCapacityUnits": 2.0}
    )


@mock_dynamodb2
def test_transact_get_items_with_consumed_capacity_indexes():
    _create_multiple_tables()
    client = boto3.client("dynamodb", region_name="us-east-1")

    res = client.transact_get_items(
        TransactItems=[
            {
                "Get": {
                    "Key": {"id": {"S": "1"}, "sort_key": {"S": "1"}},
                    "TableName": "test1",
                }
            },
            {
                "Get": {
                    "Key": {"id": {"S": "1"}, "sort_key": {"S": "2"}},
                    "TableName": "test1",
                }
            },
            {
                "Get": {
                    "Key": {"id": {"S": "1"}, "sort_key": {"S": "1"}},
                    "TableName": "test2",
                }
            },
        ],
        ReturnConsumedCapacity="INDEXES",
    )

    res["ConsumedCapacity"][0].should.equal(
        {
            "TableName": "test1",
            "CapacityUnits": 4.0,
            "ReadCapacityUnits": 4.0,
            "Table": {"CapacityUnits": 4.0, "ReadCapacityUnits": 4.0,},
        }
    )

    res["ConsumedCapacity"][1].should.equal(
        {
            "TableName": "test2",
            "CapacityUnits": 2.0,
            "ReadCapacityUnits": 2.0,
            "Table": {"CapacityUnits": 2.0, "ReadCapacityUnits": 2.0,},
        }
    )


@mock_dynamodb2
def test_transact_get_items_should_return_empty_map_for_non_existent_item():
    client = boto3.client("dynamodb", region_name="us-west-2")
    table_name = "test-table"
    key_schema = [{"AttributeName": "id", "KeyType": "HASH"}]
    attribute_definitions = [{"AttributeName": "id", "AttributeType": "S"}]
    client.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    item = {"id": {"S": "1"}}
    client.put_item(TableName=table_name, Item=item)
    items = client.transact_get_items(
        TransactItems=[
            {"Get": {"Key": {"id": {"S": "1"}}, "TableName": table_name}},
            {"Get": {"Key": {"id": {"S": "2"}}, "TableName": table_name}},
        ]
    ).get("Responses", [])
    items.should.have.length_of(2)
    items[0].should.equal({"Item": item})
    items[1].should.equal({})


def _create_multiple_tables():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test1",
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "sort_key", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "sort_key", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table1 = dynamodb.Table("test1")
    table1.put_item(
        Item={"id": "1", "sort_key": "1",}
    )
    table1.put_item(
        Item={"id": "1", "sort_key": "2",}
    )
    dynamodb.create_table(
        TableName="test2",
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "sort_key", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "sort_key", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table2 = dynamodb.Table("test2")
    table2.put_item(
        Item={"id": "1", "sort_key": "1",}
    )


@mock_dynamodb2
def test_transact_write_items_put():
    table_schema = {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"},],
    }
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    # Put multiple items
    dynamodb.transact_write_items(
        TransactItems=[
            {
                "Put": {
                    "Item": {"id": {"S": "foo{}".format(str(i))}, "foo": {"S": "bar"},},
                    "TableName": "test-table",
                }
            }
            for i in range(0, 5)
        ]
    )
    # Assert all are present
    items = dynamodb.scan(TableName="test-table")["Items"]
    items.should.have.length_of(5)


@mock_dynamodb2
def test_transact_write_items_put_conditional_expressions():
    table_schema = {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"},],
    }
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    dynamodb.put_item(
        TableName="test-table", Item={"id": {"S": "foo2"},},
    )
    # Put multiple items
    with pytest.raises(ClientError) as ex:
        dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "Item": {
                            "id": {"S": "foo{}".format(str(i))},
                            "foo": {"S": "bar"},
                        },
                        "TableName": "test-table",
                        "ConditionExpression": "#i <> :i",
                        "ExpressionAttributeNames": {"#i": "id"},
                        "ExpressionAttributeValues": {
                            ":i": {
                                "S": "foo2"
                            }  # This item already exist, so the ConditionExpression should fail
                        },
                    }
                }
                for i in range(0, 5)
            ]
        )
    # Assert the exception is correct
    ex.value.response["Error"]["Code"].should.equal("TransactionCanceledException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    # Assert all are present
    items = dynamodb.scan(TableName="test-table")["Items"]
    items.should.have.length_of(1)
    items[0].should.equal({"id": {"S": "foo2"}})


@mock_dynamodb2
def test_transact_write_items_conditioncheck_passes():
    table_schema = {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"},],
    }
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    # Insert an item without email address
    dynamodb.put_item(
        TableName="test-table", Item={"id": {"S": "foo"},},
    )
    # Put an email address, after verifying it doesn't exist yet
    dynamodb.transact_write_items(
        TransactItems=[
            {
                "ConditionCheck": {
                    "Key": {"id": {"S": "foo"}},
                    "TableName": "test-table",
                    "ConditionExpression": "attribute_not_exists(#e)",
                    "ExpressionAttributeNames": {"#e": "email_address"},
                }
            },
            {
                "Put": {
                    "Item": {
                        "id": {"S": "foo"},
                        "email_address": {"S": "test@moto.com"},
                    },
                    "TableName": "test-table",
                }
            },
        ]
    )
    # Assert all are present
    items = dynamodb.scan(TableName="test-table")["Items"]
    items.should.have.length_of(1)
    items[0].should.equal({"email_address": {"S": "test@moto.com"}, "id": {"S": "foo"}})


@mock_dynamodb2
def test_transact_write_items_conditioncheck_fails():
    table_schema = {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"},],
    }
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    # Insert an item with email address
    dynamodb.put_item(
        TableName="test-table",
        Item={"id": {"S": "foo"}, "email_address": {"S": "test@moto.com"}},
    )
    # Try to put an email address, but verify whether it exists
    # ConditionCheck should fail
    with pytest.raises(ClientError) as ex:
        dynamodb.transact_write_items(
            TransactItems=[
                {
                    "ConditionCheck": {
                        "Key": {"id": {"S": "foo"}},
                        "TableName": "test-table",
                        "ConditionExpression": "attribute_not_exists(#e)",
                        "ExpressionAttributeNames": {"#e": "email_address"},
                    }
                },
                {
                    "Put": {
                        "Item": {
                            "id": {"S": "foo"},
                            "email_address": {"S": "update@moto.com"},
                        },
                        "TableName": "test-table",
                    }
                },
            ]
        )
    # Assert the exception is correct
    ex.value.response["Error"]["Code"].should.equal("TransactionCanceledException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)

    # Assert the original email address is still present
    items = dynamodb.scan(TableName="test-table")["Items"]
    items.should.have.length_of(1)
    items[0].should.equal({"email_address": {"S": "test@moto.com"}, "id": {"S": "foo"}})


@mock_dynamodb2
def test_transact_write_items_delete():
    table_schema = {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"},],
    }
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    # Insert an item
    dynamodb.put_item(
        TableName="test-table", Item={"id": {"S": "foo"},},
    )
    # Delete the item
    dynamodb.transact_write_items(
        TransactItems=[
            {"Delete": {"Key": {"id": {"S": "foo"}}, "TableName": "test-table",}}
        ]
    )
    # Assert the item is deleted
    items = dynamodb.scan(TableName="test-table")["Items"]
    items.should.have.length_of(0)


@mock_dynamodb2
def test_transact_write_items_delete_with_successful_condition_expression():
    table_schema = {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"},],
    }
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    # Insert an item without email address
    dynamodb.put_item(
        TableName="test-table", Item={"id": {"S": "foo"},},
    )
    # ConditionExpression will pass - no email address has been specified yet
    dynamodb.transact_write_items(
        TransactItems=[
            {
                "Delete": {
                    "Key": {"id": {"S": "foo"},},
                    "TableName": "test-table",
                    "ConditionExpression": "attribute_not_exists(#e)",
                    "ExpressionAttributeNames": {"#e": "email_address"},
                }
            }
        ]
    )
    # Assert the item is deleted
    items = dynamodb.scan(TableName="test-table")["Items"]
    items.should.have.length_of(0)


@mock_dynamodb2
def test_transact_write_items_delete_with_failed_condition_expression():
    table_schema = {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"},],
    }
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    # Insert an item with email address
    dynamodb.put_item(
        TableName="test-table",
        Item={"id": {"S": "foo"}, "email_address": {"S": "test@moto.com"}},
    )
    # Try to delete an item that does not have an email address
    # ConditionCheck should fail
    with pytest.raises(ClientError) as ex:
        dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Delete": {
                        "Key": {"id": {"S": "foo"},},
                        "TableName": "test-table",
                        "ConditionExpression": "attribute_not_exists(#e)",
                        "ExpressionAttributeNames": {"#e": "email_address"},
                    }
                }
            ]
        )
    # Assert the exception is correct
    ex.value.response["Error"]["Code"].should.equal("TransactionCanceledException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    # Assert the original item is still present
    items = dynamodb.scan(TableName="test-table")["Items"]
    items.should.have.length_of(1)
    items[0].should.equal({"email_address": {"S": "test@moto.com"}, "id": {"S": "foo"}})


@mock_dynamodb2
def test_transact_write_items_update():
    table_schema = {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"},],
    }
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    # Insert an item
    dynamodb.put_item(TableName="test-table", Item={"id": {"S": "foo"}})
    # Update the item
    dynamodb.transact_write_items(
        TransactItems=[
            {
                "Update": {
                    "Key": {"id": {"S": "foo"}},
                    "TableName": "test-table",
                    "UpdateExpression": "SET #e = :v",
                    "ExpressionAttributeNames": {"#e": "email_address"},
                    "ExpressionAttributeValues": {":v": {"S": "test@moto.com"}},
                }
            }
        ]
    )
    # Assert the item is updated
    items = dynamodb.scan(TableName="test-table")["Items"]
    items.should.have.length_of(1)
    items[0].should.equal({"id": {"S": "foo"}, "email_address": {"S": "test@moto.com"}})


@mock_dynamodb2
def test_transact_write_items_update_with_failed_condition_expression():
    table_schema = {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"},],
    }
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    # Insert an item with email address
    dynamodb.put_item(
        TableName="test-table",
        Item={"id": {"S": "foo"}, "email_address": {"S": "test@moto.com"}},
    )
    # Try to update an item that does not have an email address
    # ConditionCheck should fail
    with pytest.raises(ClientError) as ex:
        dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "Key": {"id": {"S": "foo"}},
                        "TableName": "test-table",
                        "UpdateExpression": "SET #e = :v",
                        "ConditionExpression": "attribute_not_exists(#e)",
                        "ExpressionAttributeNames": {"#e": "email_address"},
                        "ExpressionAttributeValues": {":v": {"S": "update@moto.com"}},
                    }
                }
            ]
        )
    # Assert the exception is correct
    ex.value.response["Error"]["Code"].should.equal("TransactionCanceledException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    # Assert the original item is still present
    items = dynamodb.scan(TableName="test-table")["Items"]
    items.should.have.length_of(1)
    items[0].should.equal({"email_address": {"S": "test@moto.com"}, "id": {"S": "foo"}})


@mock_dynamodb2
def test_transact_write_items_fails_with_transaction_canceled_exception():
    table_schema = {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"},],
    }
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    # Insert one item
    dynamodb.put_item(TableName="test-table", Item={"id": {"S": "foo"}})
    # Update two items, the one that exists and another that doesn't
    with pytest.raises(ClientError) as ex:
        dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "Key": {"id": {"S": "foo"}},
                        "TableName": "test-table",
                        "UpdateExpression": "SET #k = :v",
                        "ConditionExpression": "attribute_exists(id)",
                        "ExpressionAttributeNames": {"#k": "key"},
                        "ExpressionAttributeValues": {":v": {"S": "value"}},
                    }
                },
                {
                    "Update": {
                        "Key": {"id": {"S": "doesnotexist"}},
                        "TableName": "test-table",
                        "UpdateExpression": "SET #e = :v",
                        "ConditionExpression": "attribute_exists(id)",
                        "ExpressionAttributeNames": {"#e": "key"},
                        "ExpressionAttributeValues": {":v": {"S": "value"}},
                    }
                },
            ]
        )
    ex.value.response["Error"]["Code"].should.equal("TransactionCanceledException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Message"].should.equal(
        "Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed]"
    )
