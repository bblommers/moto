import boto3
import pytest

from botocore.exceptions import ClientError
from moto import mock_dynamodb2

# Test complex UpdateExpressions


# https://github.com/spulec/moto/issues/1342
@mock_dynamodb2
def test_update_item_on_map():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    client = boto3.client("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    dynamodb.create_table(
        TableName="users",
        KeySchema=[
            {"AttributeName": "forum_name", "KeyType": "HASH"},
            {"AttributeName": "subject", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "forum_name", "AttributeType": "S"},
            {"AttributeName": "subject", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table = dynamodb.Table("users")

    table.put_item(
        Item={
            "forum_name": "the-key",
            "subject": "123",
            "body": {"nested": {"data": "test"}},
        }
    )

    resp = table.scan()
    resp["Items"][0]["body"].should.equal({"nested": {"data": "test"}})

    # Nonexistent nested attributes are supported for existing top-level attributes.
    table.update_item(
        Key={"forum_name": "the-key", "subject": "123"},
        UpdateExpression="SET body.#nested.#data = :tb",
        ExpressionAttributeNames={"#nested": "nested", "#data": "data",},
        ExpressionAttributeValues={":tb": "new_value"},
    )
    # Running this against AWS DDB gives an exception so make sure it also fails.:
    with pytest.raises(client.exceptions.ClientError):
        # botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the UpdateItem
        # operation: The document path provided in the update expression is invalid for update
        table.update_item(
            Key={"forum_name": "the-key", "subject": "123"},
            UpdateExpression="SET body.#nested.#nonexistentnested.#data = :tb2",
            ExpressionAttributeNames={
                "#nested": "nested",
                "#nonexistentnested": "nonexistentnested",
                "#data": "data",
            },
            ExpressionAttributeValues={":tb2": "other_value"},
        )

    table.update_item(
        Key={"forum_name": "the-key", "subject": "123"},
        UpdateExpression="SET body.#nested.#nonexistentnested = :tb2",
        ExpressionAttributeNames={
            "#nested": "nested",
            "#nonexistentnested": "nonexistentnested",
        },
        ExpressionAttributeValues={":tb2": {"data": "other_value"}},
    )

    resp = table.scan()
    resp["Items"][0]["body"].should.equal(
        {"nested": {"data": "new_value", "nonexistentnested": {"data": "other_value"}}}
    )

    # Test nested value for a nonexistent attribute throws a ClientError.
    with pytest.raises(client.exceptions.ClientError):
        table.update_item(
            Key={"forum_name": "the-key", "subject": "123"},
            UpdateExpression="SET nonexistent.#nested = :tb",
            ExpressionAttributeNames={"#nested": "nested"},
            ExpressionAttributeValues={":tb": "new_value"},
        )


# https://github.com/spulec/moto/issues/1358
@mock_dynamodb2
def test_update_if_not_exists():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    dynamodb.create_table(
        TableName="users",
        KeySchema=[
            {"AttributeName": "forum_name", "KeyType": "HASH"},
            {"AttributeName": "subject", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "forum_name", "AttributeType": "S"},
            {"AttributeName": "subject", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table = dynamodb.Table("users")

    table.put_item(Item={"forum_name": "the-key", "subject": "123"})

    table.update_item(
        Key={"forum_name": "the-key", "subject": "123"},
        # if_not_exists without space
        UpdateExpression="SET created_at=if_not_exists(created_at,:created_at)",
        ExpressionAttributeValues={":created_at": 123},
    )

    resp = table.scan()
    assert resp["Items"][0]["created_at"] == 123

    table.update_item(
        Key={"forum_name": "the-key", "subject": "123"},
        # if_not_exists with space
        UpdateExpression="SET created_at = if_not_exists (created_at, :created_at)",
        ExpressionAttributeValues={":created_at": 456},
    )

    resp = table.scan()
    # Still the original value
    assert resp["Items"][0]["created_at"] == 123


@mock_dynamodb2
def test_update_list_index__set_existing_nested_index():
    table_name = "test_list_index_access"
    client = create_table_with_list(table_name)
    client.put_item(
        TableName=table_name,
        Item={
            "id": {"S": "foo2"},
            "itemmap": {
                "M": {"itemlist": {"L": [{"S": "bar1"}, {"S": "bar2"}, {"S": "bar3"}]}}
            },
        },
    )
    client.update_item(
        TableName=table_name,
        Key={"id": {"S": "foo2"}},
        UpdateExpression="set itemmap.itemlist[1]=:Item",
        ExpressionAttributeValues={":Item": {"S": "bar2_update"}},
    )
    #
    result = client.get_item(TableName=table_name, Key={"id": {"S": "foo2"}})["Item"]
    result["id"].should.equal({"S": "foo2"})
    result["itemmap"]["M"]["itemlist"]["L"].should.equal(
        [{"S": "bar1"}, {"S": "bar2_update"}, {"S": "bar3"}]
    )


@mock_dynamodb2
def test_update_list_index__set_index_out_of_range():
    table_name = "test_list_index_access"
    client = create_table_with_list(table_name)
    client.put_item(
        TableName=table_name,
        Item={
            "id": {"S": "foo"},
            "itemlist": {"L": [{"S": "bar1"}, {"S": "bar2"}, {"S": "bar3"}]},
        },
    )
    client.update_item(
        TableName=table_name,
        Key={"id": {"S": "foo"}},
        UpdateExpression="set itemlist[10]=:Item",
        ExpressionAttributeValues={":Item": {"S": "bar10"}},
    )
    #
    result = client.get_item(TableName=table_name, Key={"id": {"S": "foo"}})["Item"]
    assert result["id"] == {"S": "foo"}
    assert result["itemlist"] == {
        "L": [{"S": "bar1"}, {"S": "bar2"}, {"S": "bar3"}, {"S": "bar10"}]
    }


@mock_dynamodb2
def test_update_list_index__set_nested_index_out_of_range():
    table_name = "test_list_index_access"
    client = create_table_with_list(table_name)
    client.put_item(
        TableName=table_name,
        Item={
            "id": {"S": "foo2"},
            "itemmap": {
                "M": {"itemlist": {"L": [{"S": "bar1"}, {"S": "bar2"}, {"S": "bar3"}]}}
            },
        },
    )
    client.update_item(
        TableName=table_name,
        Key={"id": {"S": "foo2"}},
        UpdateExpression="set itemmap.itemlist[10]=:Item",
        ExpressionAttributeValues={":Item": {"S": "bar10"}},
    )
    #
    result = client.get_item(TableName=table_name, Key={"id": {"S": "foo2"}})["Item"]
    assert result["id"] == {"S": "foo2"}
    assert result["itemmap"]["M"]["itemlist"]["L"] == [
        {"S": "bar1"},
        {"S": "bar2"},
        {"S": "bar3"},
        {"S": "bar10"},
    ]


@mock_dynamodb2
def test_update_list_index__set_double_nested_index():
    table_name = "test_list_index_access"
    client = create_table_with_list(table_name)
    client.put_item(
        TableName=table_name,
        Item={
            "id": {"S": "foo2"},
            "itemmap": {
                "M": {
                    "itemlist": {
                        "L": [
                            {"M": {"foo": {"S": "bar11"}, "foos": {"S": "bar12"}}},
                            {"M": {"foo": {"S": "bar21"}, "foos": {"S": "bar21"}}},
                        ]
                    }
                }
            },
        },
    )
    client.update_item(
        TableName=table_name,
        Key={"id": {"S": "foo2"}},
        UpdateExpression="set itemmap.itemlist[1].foos=:Item",
        ExpressionAttributeValues={":Item": {"S": "bar22"}},
    )
    #
    result = client.get_item(TableName=table_name, Key={"id": {"S": "foo2"}})["Item"]
    assert result["id"] == {"S": "foo2"}
    len(result["itemmap"]["M"]["itemlist"]["L"]).should.equal(2)
    result["itemmap"]["M"]["itemlist"]["L"][0].should.equal(
        {"M": {"foo": {"S": "bar11"}, "foos": {"S": "bar12"}}}
    )  # unchanged
    result["itemmap"]["M"]["itemlist"]["L"][1].should.equal(
        {"M": {"foo": {"S": "bar21"}, "foos": {"S": "bar22"}}}
    )  # updated


@mock_dynamodb2
def test_update_list_index__set_index_of_a_string():
    table_name = "test_list_index_access"
    client = create_table_with_list(table_name)
    client.put_item(
        TableName=table_name, Item={"id": {"S": "foo2"}, "itemstr": {"S": "somestring"}}
    )
    with pytest.raises(ClientError) as ex:
        client.update_item(
            TableName=table_name,
            Key={"id": {"S": "foo2"}},
            UpdateExpression="set itemstr[1]=:Item",
            ExpressionAttributeValues={":Item": {"S": "string_update"}},
        )
        client.get_item(TableName=table_name, Key={"id": {"S": "foo2"}})["Item"]

    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["Error"]["Message"].should.equal(
        "The document path provided in the update expression is invalid for update"
    )


@mock_dynamodb2
def test_remove_top_level_attribute():
    table_name = "test_remove"
    client = create_table_with_list(table_name)
    client.put_item(
        TableName=table_name, Item={"id": {"S": "foo"}, "item": {"S": "bar"}}
    )
    client.update_item(
        TableName=table_name,
        Key={"id": {"S": "foo"}},
        UpdateExpression="REMOVE #i",
        ExpressionAttributeNames={"#i": "item"},
    )
    #
    result = client.get_item(TableName=table_name, Key={"id": {"S": "foo"}})["Item"]
    result.should.equal({"id": {"S": "foo"}})


@mock_dynamodb2
def test_remove_top_level_attribute_non_existent():
    """
    Remove statements do not require attribute to exist they silently pass
    """
    table_name = "test_remove"
    client = create_table_with_list(table_name)
    ddb_item = {"id": {"S": "foo"}, "item": {"S": "bar"}}
    client.put_item(TableName=table_name, Item=ddb_item)
    client.update_item(
        TableName=table_name,
        Key={"id": {"S": "foo"}},
        UpdateExpression="REMOVE non_existent_attribute",
        ExpressionAttributeNames={"#i": "item"},
    )
    result = client.get_item(TableName=table_name, Key={"id": {"S": "foo"}})["Item"]
    result.should.equal(ddb_item)


@mock_dynamodb2
def test_remove_list_index__remove_existing_index():
    table_name = "test_list_index_access"
    client = create_table_with_list(table_name)
    client.put_item(
        TableName=table_name,
        Item={
            "id": {"S": "foo"},
            "itemlist": {"L": [{"S": "bar1"}, {"S": "bar2"}, {"S": "bar3"}]},
        },
    )
    client.update_item(
        TableName=table_name,
        Key={"id": {"S": "foo"}},
        UpdateExpression="REMOVE itemlist[1]",
    )
    #
    result = client.get_item(TableName=table_name, Key={"id": {"S": "foo"}})["Item"]
    result["id"].should.equal({"S": "foo"})
    result["itemlist"].should.equal({"L": [{"S": "bar1"}, {"S": "bar3"}]})


@mock_dynamodb2
def test_remove_list_index__remove_existing_nested_index():
    table_name = "test_list_index_access"
    client = create_table_with_list(table_name)
    client.put_item(
        TableName=table_name,
        Item={
            "id": {"S": "foo2"},
            "itemmap": {"M": {"itemlist": {"L": [{"S": "bar1"}, {"S": "bar2"}]}}},
        },
    )
    client.update_item(
        TableName=table_name,
        Key={"id": {"S": "foo2"}},
        UpdateExpression="REMOVE itemmap.itemlist[1]",
    )
    #
    result = client.get_item(TableName=table_name, Key={"id": {"S": "foo2"}})["Item"]
    result["id"].should.equal({"S": "foo2"})
    result["itemmap"]["M"]["itemlist"]["L"].should.equal([{"S": "bar1"}])


@mock_dynamodb2
def test_remove_list_index__remove_existing_double_nested_index():
    table_name = "test_list_index_access"
    client = create_table_with_list(table_name)
    client.put_item(
        TableName=table_name,
        Item={
            "id": {"S": "foo2"},
            "itemmap": {
                "M": {
                    "itemlist": {
                        "L": [
                            {"M": {"foo00": {"S": "bar1"}, "foo01": {"S": "bar2"}}},
                            {"M": {"foo10": {"S": "bar1"}, "foo11": {"S": "bar2"}}},
                        ]
                    }
                }
            },
        },
    )
    client.update_item(
        TableName=table_name,
        Key={"id": {"S": "foo2"}},
        UpdateExpression="REMOVE itemmap.itemlist[1].foo10",
    )
    #
    result = client.get_item(TableName=table_name, Key={"id": {"S": "foo2"}})["Item"]
    assert result["id"] == {"S": "foo2"}
    assert result["itemmap"]["M"]["itemlist"]["L"][0]["M"].should.equal(
        {"foo00": {"S": "bar1"}, "foo01": {"S": "bar2"}}
    )  # untouched
    assert result["itemmap"]["M"]["itemlist"]["L"][1]["M"].should.equal(
        {"foo11": {"S": "bar2"}}
    )  # changed


@mock_dynamodb2
def test_remove_list_index__remove_index_out_of_range():
    table_name = "test_list_index_access"
    client = create_table_with_list(table_name)
    client.put_item(
        TableName=table_name,
        Item={
            "id": {"S": "foo"},
            "itemlist": {"L": [{"S": "bar1"}, {"S": "bar2"}, {"S": "bar3"}]},
        },
    )
    client.update_item(
        TableName=table_name,
        Key={"id": {"S": "foo"}},
        UpdateExpression="REMOVE itemlist[10]",
    )
    #
    result = client.get_item(TableName=table_name, Key={"id": {"S": "foo"}})["Item"]
    assert result["id"] == {"S": "foo"}
    assert result["itemlist"] == {"L": [{"S": "bar1"}, {"S": "bar2"}, {"S": "bar3"}]}


@mock_dynamodb2
def test_update_list_index__set_existing_index():
    table_name = "test_list_index_access"
    client = create_table_with_list(table_name)
    client.put_item(
        TableName=table_name,
        Item={
            "id": {"S": "foo"},
            "itemlist": {"L": [{"S": "bar1"}, {"S": "bar2"}, {"S": "bar3"}]},
        },
    )
    client.update_item(
        TableName=table_name,
        Key={"id": {"S": "foo"}},
        UpdateExpression="set itemlist[1]=:Item",
        ExpressionAttributeValues={":Item": {"S": "bar2_update"}},
    )
    #
    result = client.get_item(TableName=table_name, Key={"id": {"S": "foo"}})["Item"]
    result["id"].should.equal({"S": "foo"})
    result["itemlist"].should.equal(
        {"L": [{"S": "bar1"}, {"S": "bar2_update"}, {"S": "bar3"}]}
    )


def create_table_with_list(table_name):
    client = boto3.client("dynamodb", region_name="us-east-1")
    client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return client


@mock_dynamodb2
def test_update_supports_list_append():
    # Verify whether the list_append operation works as expected
    client = boto3.client("dynamodb", region_name="us-east-1")

    client.create_table(
        AttributeDefinitions=[{"AttributeName": "SHA256", "AttributeType": "S"}],
        TableName="TestTable",
        KeySchema=[{"AttributeName": "SHA256", "KeyType": "HASH"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    client.put_item(
        TableName="TestTable",
        Item={"SHA256": {"S": "sha-of-file"}, "crontab": {"L": [{"S": "bar1"}]}},
    )

    # Update item using list_append expression
    updated_item = client.update_item(
        TableName="TestTable",
        Key={"SHA256": {"S": "sha-of-file"}},
        UpdateExpression="SET crontab = list_append(crontab, :i)",
        ExpressionAttributeValues={":i": {"L": [{"S": "bar2"}]}},
        ReturnValues="UPDATED_NEW",
    )

    # Verify updated item is correct
    updated_item["Attributes"].should.equal(
        {"crontab": {"L": [{"S": "bar1"}, {"S": "bar2"}]}}
    )
    # Verify item is appended to the existing list
    result = client.get_item(
        TableName="TestTable", Key={"SHA256": {"S": "sha-of-file"}}
    )["Item"]
    result.should.equal(
        {
            "SHA256": {"S": "sha-of-file"},
            "crontab": {"L": [{"S": "bar1"}, {"S": "bar2"}]},
        }
    )


@mock_dynamodb2
def test_update_supports_nested_list_append():
    # Verify whether we can append a list that's inside a map
    client = boto3.client("dynamodb", region_name="us-east-1")

    client.create_table(
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        TableName="TestTable",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    client.put_item(
        TableName="TestTable",
        Item={
            "id": {"S": "nested_list_append"},
            "a": {"M": {"b": {"L": [{"S": "bar1"}]}}},
        },
    )

    # Update item using list_append expression
    updated_item = client.update_item(
        TableName="TestTable",
        Key={"id": {"S": "nested_list_append"}},
        UpdateExpression="SET a.#b = list_append(a.#b, :i)",
        ExpressionAttributeValues={":i": {"L": [{"S": "bar2"}]}},
        ExpressionAttributeNames={"#b": "b"},
        ReturnValues="UPDATED_NEW",
    )

    # Verify updated item is correct
    updated_item["Attributes"].should.equal(
        {"a": {"M": {"b": {"L": [{"S": "bar1"}, {"S": "bar2"}]}}}}
    )
    result = client.get_item(
        TableName="TestTable", Key={"id": {"S": "nested_list_append"}}
    )["Item"]
    result.should.equal(
        {
            "id": {"S": "nested_list_append"},
            "a": {"M": {"b": {"L": [{"S": "bar1"}, {"S": "bar2"}]}}},
        }
    )


@mock_dynamodb2
def test_update_supports_multiple_levels_nested_list_append():
    # Verify whether we can append a list that's inside a map that's inside a map  (Inception!)
    client = boto3.client("dynamodb", region_name="us-east-1")

    client.create_table(
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        TableName="TestTable",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    client.put_item(
        TableName="TestTable",
        Item={
            "id": {"S": "nested_list_append"},
            "a": {"M": {"b": {"M": {"c": {"L": [{"S": "bar1"}]}}}}},
        },
    )

    # Update item using list_append expression
    updated_item = client.update_item(
        TableName="TestTable",
        Key={"id": {"S": "nested_list_append"}},
        UpdateExpression="SET a.#b.c = list_append(a.#b.#c, :i)",
        ExpressionAttributeValues={":i": {"L": [{"S": "bar2"}]}},
        ExpressionAttributeNames={"#b": "b", "#c": "c"},
        ReturnValues="UPDATED_NEW",
    )

    # Verify updated item is correct
    updated_item["Attributes"].should.equal(
        {"a": {"M": {"b": {"M": {"c": {"L": [{"S": "bar1"}, {"S": "bar2"}]}}}}}}
    )
    # Verify item is appended to the existing list
    result = client.get_item(
        TableName="TestTable", Key={"id": {"S": "nested_list_append"}}
    )["Item"]
    result.should.equal(
        {
            "id": {"S": "nested_list_append"},
            "a": {"M": {"b": {"M": {"c": {"L": [{"S": "bar1"}, {"S": "bar2"}]}}}}},
        }
    )


@mock_dynamodb2
def test_update_supports_nested_list_append_onto_another_list():
    # Verify whether we can take the contents of one list, and use that to fill another list
    # Note that the contents of the other list is completely overwritten
    client = boto3.client("dynamodb", region_name="us-east-1")

    client.create_table(
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        TableName="TestTable",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    client.put_item(
        TableName="TestTable",
        Item={
            "id": {"S": "list_append_another"},
            "a": {"M": {"b": {"L": [{"S": "bar1"}]}, "c": {"L": [{"S": "car1"}]}}},
        },
    )

    # Update item using list_append expression
    updated_item = client.update_item(
        TableName="TestTable",
        Key={"id": {"S": "list_append_another"}},
        UpdateExpression="SET a.#c = list_append(a.#b, :i)",
        ExpressionAttributeValues={":i": {"L": [{"S": "bar2"}]}},
        ExpressionAttributeNames={"#b": "b", "#c": "c"},
        ReturnValues="UPDATED_NEW",
    )

    # Verify updated item is correct
    updated_item["Attributes"].should.equal(
        {"a": {"M": {"c": {"L": [{"S": "bar1"}, {"S": "bar2"}]}}}}
    )
    # Verify item is appended to the existing list
    result = client.get_item(
        TableName="TestTable", Key={"id": {"S": "list_append_another"}}
    )["Item"]
    result.should.equal(
        {
            "id": {"S": "list_append_another"},
            "a": {
                "M": {
                    "b": {"L": [{"S": "bar1"}]},
                    "c": {"L": [{"S": "bar1"}, {"S": "bar2"}]},
                }
            },
        }
    )


@mock_dynamodb2
def test_update_supports_list_append_maps():
    client = boto3.client("dynamodb", region_name="us-west-1")
    client.create_table(
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "rid", "AttributeType": "S"},
        ],
        TableName="TestTable",
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "rid", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    client.put_item(
        TableName="TestTable",
        Item={
            "id": {"S": "nested_list_append"},
            "rid": {"S": "range_key"},
            "a": {"L": [{"M": {"b": {"S": "bar1"}}}]},
        },
    )

    # Update item using list_append expression
    updated_item = client.update_item(
        TableName="TestTable",
        Key={"id": {"S": "nested_list_append"}, "rid": {"S": "range_key"}},
        UpdateExpression="SET a = list_append(a, :i)",
        ExpressionAttributeValues={":i": {"L": [{"M": {"b": {"S": "bar2"}}}]}},
        ReturnValues="UPDATED_NEW",
    )

    # Verify updated item is correct
    updated_item["Attributes"].should.equal(
        {"a": {"L": [{"M": {"b": {"S": "bar1"}}}, {"M": {"b": {"S": "bar2"}}}]}}
    )
    # Verify item is appended to the existing list
    result = client.query(
        TableName="TestTable",
        KeyConditionExpression="id = :i AND begins_with(rid, :r)",
        ExpressionAttributeValues={
            ":i": {"S": "nested_list_append"},
            ":r": {"S": "range_key"},
        },
    )["Items"]
    result.should.equal(
        [
            {
                "a": {"L": [{"M": {"b": {"S": "bar1"}}}, {"M": {"b": {"S": "bar2"}}}]},
                "rid": {"S": "range_key"},
                "id": {"S": "nested_list_append"},
            }
        ]
    )


@mock_dynamodb2
def test_update_supports_nested_update_if_nested_value_not_exists():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    name = "TestTable"

    dynamodb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "user_id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    table = dynamodb.Table(name)
    table.put_item(
        Item={"user_id": "1234", "friends": {"5678": {"name": "friend_5678"}},},
    )
    table.update_item(
        Key={"user_id": "1234"},
        ExpressionAttributeNames={"#friends": "friends", "#friendid": "0000",},
        ExpressionAttributeValues={":friend": {"name": "friend_0000"},},
        UpdateExpression="SET #friends.#friendid = :friend",
        ReturnValues="UPDATED_NEW",
    )
    item = table.get_item(Key={"user_id": "1234"})["Item"]
    assert item == {
        "user_id": "1234",
        "friends": {"5678": {"name": "friend_5678"}, "0000": {"name": "friend_0000"},},
    }


@mock_dynamodb2
def test_update_supports_list_append_with_nested_if_not_exists_operation():
    dynamo = boto3.resource("dynamodb", region_name="us-west-1")
    table_name = "test"

    dynamo.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "Id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "Id", "KeyType": "HASH"}],
        ProvisionedThroughput={"ReadCapacityUnits": 20, "WriteCapacityUnits": 20},
    )

    table = dynamo.Table(table_name)

    table.put_item(Item={"Id": "item-id", "nest1": {"nest2": {}}})
    updated_item = table.update_item(
        Key={"Id": "item-id"},
        UpdateExpression="SET nest1.nest2.event_history = list_append(if_not_exists(nest1.nest2.event_history, :empty_list), :new_value)",
        ExpressionAttributeValues={":empty_list": [], ":new_value": ["some_value"]},
        ReturnValues="UPDATED_NEW",
    )

    # Verify updated item is correct
    updated_item["Attributes"].should.equal(
        {"nest1": {"nest2": {"event_history": ["some_value"]}}}
    )

    table.get_item(Key={"Id": "item-id"})["Item"].should.equal(
        {"Id": "item-id", "nest1": {"nest2": {"event_history": ["some_value"]}}}
    )


@mock_dynamodb2
def test_update_supports_list_append_with_nested_if_not_exists_operation_and_property_already_exists():
    dynamo = boto3.resource("dynamodb", region_name="us-west-1")
    table_name = "test"

    dynamo.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "Id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "Id", "KeyType": "HASH"}],
        ProvisionedThroughput={"ReadCapacityUnits": 20, "WriteCapacityUnits": 20},
    )

    table = dynamo.Table(table_name)

    table.put_item(Item={"Id": "item-id", "event_history": ["other_value"]})
    updated_item = table.update_item(
        Key={"Id": "item-id"},
        UpdateExpression="SET event_history = list_append(if_not_exists(event_history, :empty_list), :new_value)",
        ExpressionAttributeValues={":empty_list": [], ":new_value": ["some_value"]},
        ReturnValues="UPDATED_NEW",
    )

    # Verify updated item is correct
    updated_item["Attributes"].should.equal(
        {"event_history": ["other_value", "some_value"]}
    )

    table.get_item(Key={"Id": "item-id"})["Item"].should.equal(
        {"Id": "item-id", "event_history": ["other_value", "some_value"]}
    )


@mock_dynamodb2
def test_update_item_if_original_value_is_none():
    dynamo = boto3.resource("dynamodb", region_name="eu-central-1")
    dynamo.create_table(
        AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
        TableName="origin-rbu-dev",
        KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    table = dynamo.Table("origin-rbu-dev")
    table.put_item(Item={"job_id": "a", "job_name": None})
    table.update_item(
        Key={"job_id": "a"},
        UpdateExpression="SET job_name = :output",
        ExpressionAttributeValues={":output": "updated"},
    )
    table.scan()["Items"][0]["job_name"].should.equal("updated")


@mock_dynamodb2
def test_update_nested_item_if_original_value_is_none():
    dynamo = boto3.resource("dynamodb", region_name="eu-central-1")
    dynamo.create_table(
        AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
        TableName="origin-rbu-dev",
        KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    table = dynamo.Table("origin-rbu-dev")
    table.put_item(Item={"job_id": "a", "job_details": {"job_name": None}})
    updated_item = table.update_item(
        Key={"job_id": "a"},
        UpdateExpression="SET job_details.job_name = :output",
        ExpressionAttributeValues={":output": "updated"},
        ReturnValues="UPDATED_NEW",
    )

    # Verify updated item is correct
    updated_item["Attributes"].should.equal({"job_details": {"job_name": "updated"}})

    table.scan()["Items"][0]["job_details"]["job_name"].should.equal("updated")


@mock_dynamodb2
def test_allow_update_to_item_with_different_type():
    dynamo = boto3.resource("dynamodb", region_name="eu-central-1")
    dynamo.create_table(
        AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
        TableName="origin-rbu-dev",
        KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    table = dynamo.Table("origin-rbu-dev")
    table.put_item(Item={"job_id": "a", "job_details": {"job_name": {"nested": "yes"}}})
    table.put_item(Item={"job_id": "b", "job_details": {"job_name": {"nested": "yes"}}})
    updated_item = table.update_item(
        Key={"job_id": "a"},
        UpdateExpression="SET job_details.job_name = :output",
        ExpressionAttributeValues={":output": "updated"},
        ReturnValues="UPDATED_NEW",
    )

    # Verify updated item is correct
    updated_item["Attributes"].should.equal({"job_details": {"job_name": "updated"}})

    table.get_item(Key={"job_id": "a"})["Item"]["job_details"][
        "job_name"
    ].should.be.equal("updated")
    table.get_item(Key={"job_id": "b"})["Item"]["job_details"][
        "job_name"
    ].should.be.equal({"nested": "yes"})


@mock_dynamodb2
def test_update_expression_with_numeric_literal_instead_of_value():
    """
    DynamoDB requires literals to be passed in as values. If they are put literally in the expression a token error will
    be raised
    """
    dynamodb = boto3.client("dynamodb", region_name="eu-west-1")

    dynamodb.create_table(
        TableName="moto-test",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
    )

    try:
        dynamodb.update_item(
            TableName="moto-test",
            Key={"id": {"S": "1"}},
            UpdateExpression="SET MyStr = myNum + 1",
        )
        assert False, "Validation exception not thrown"
    except dynamodb.exceptions.ClientError as e:
        assert_raise_syntax_error(e, "1", "+ 1")


@mock_dynamodb2
def test_update_expression_with_multiple_set_clauses_must_be_comma_separated():
    """
    An UpdateExpression can have multiple set clauses but if they are passed in without the separating comma.
    """
    dynamodb = boto3.client("dynamodb", region_name="eu-west-1")

    dynamodb.create_table(
        TableName="moto-test",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
    )

    try:
        dynamodb.update_item(
            TableName="moto-test",
            Key={"id": {"S": "1"}},
            UpdateExpression="SET MyStr = myNum Mystr2 myNum2",
        )
        assert False, "Validation exception not thrown"
    except dynamodb.exceptions.ClientError as e:
        assert_raise_syntax_error(e, "Mystr2", "myNum Mystr2 myNum2")


# https://github.com/spulec/moto/issues/2806
# https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
#       #DDB-UpdateItem-request-UpdateExpression
@mock_dynamodb2
def test_update_item_with_attribute_in_right_hand_side_and_operation():
    dynamodb = create_simple_table_and_return_client()

    dynamodb.update_item(
        TableName="moto-test",
        Key={"id": {"S": "1"}},
        UpdateExpression="SET myNum = myNum+:val",
        ExpressionAttributeValues={":val": {"N": "3"}},
    )

    result = dynamodb.get_item(TableName="moto-test", Key={"id": {"S": "1"}})
    assert result["Item"]["myNum"]["N"] == "4"

    dynamodb.update_item(
        TableName="moto-test",
        Key={"id": {"S": "1"}},
        UpdateExpression="SET myNum = myNum - :val",
        ExpressionAttributeValues={":val": {"N": "1"}},
    )
    result = dynamodb.get_item(TableName="moto-test", Key={"id": {"S": "1"}})
    assert result["Item"]["myNum"]["N"] == "3"


@mock_dynamodb2
def test_non_existing_attribute_should_raise_exception():
    """
    Does error message get correctly raised if attribute is referenced but it does not exist for the item.
    """
    dynamodb = create_simple_table_and_return_client()

    try:
        dynamodb.update_item(
            TableName="moto-test",
            Key={"id": {"S": "1"}},
            UpdateExpression="SET MyStr = no_attr + MyStr",
        )
        assert False, "Validation exception not thrown"
    except dynamodb.exceptions.ClientError as e:
        assert_correct_client_error(
            e,
            "ValidationException",
            "The provided expression refers to an attribute that does not exist in the item",
        )


@mock_dynamodb2
def test_update_expression_with_plus_in_attribute_name():
    """
    Does error message get correctly raised if attribute contains a plus and is passed in without an AttributeName. And
    lhs & rhs are not attribute IDs by themselve.
    """
    dynamodb = create_simple_table_and_return_client()

    dynamodb.put_item(
        TableName="moto-test",
        Item={"id": {"S": "1"}, "my+Num": {"S": "1"}, "MyStr": {"S": "aaa"},},
    )
    try:
        dynamodb.update_item(
            TableName="moto-test",
            Key={"id": {"S": "1"}},
            UpdateExpression="SET MyStr = my+Num",
        )
        assert False, "Validation exception not thrown"
    except dynamodb.exceptions.ClientError as e:
        assert_correct_client_error(
            e,
            "ValidationException",
            "The provided expression refers to an attribute that does not exist in the item",
        )


@mock_dynamodb2
def test_update_expression_with_minus_in_attribute_name():
    """
    Does error message get correctly raised if attribute contains a minus and is passed in without an AttributeName. And
    lhs & rhs are not attribute IDs by themselve.
    """
    dynamodb = create_simple_table_and_return_client()

    dynamodb.put_item(
        TableName="moto-test",
        Item={"id": {"S": "1"}, "my-Num": {"S": "1"}, "MyStr": {"S": "aaa"},},
    )
    try:
        dynamodb.update_item(
            TableName="moto-test",
            Key={"id": {"S": "1"}},
            UpdateExpression="SET MyStr = my-Num",
        )
        assert False, "Validation exception not thrown"
    except dynamodb.exceptions.ClientError as e:
        assert_correct_client_error(
            e,
            "ValidationException",
            "The provided expression refers to an attribute that does not exist in the item",
        )


@mock_dynamodb2
def test_update_expression_with_space_in_attribute_name():
    """
    Does error message get correctly raised if attribute contains a space and is passed in without an AttributeName. And
    lhs & rhs are not attribute IDs by themselves.
    """
    dynamodb = create_simple_table_and_return_client()

    dynamodb.put_item(
        TableName="moto-test",
        Item={"id": {"S": "1"}, "my Num": {"S": "1"}, "MyStr": {"S": "aaa"},},
    )

    try:
        dynamodb.update_item(
            TableName="moto-test",
            Key={"id": {"S": "1"}},
            UpdateExpression="SET MyStr = my Num",
        )
        assert False, "Validation exception not thrown"
    except dynamodb.exceptions.ClientError as e:
        assert_raise_syntax_error(e, "Num", "my Num")


@mock_dynamodb2
def test_summing_up_2_strings_raises_exception():
    """
    Update set supports different DynamoDB types but some operations are not supported. For example summing up 2 strings
    raises an exception.  It results in ClientError with code ValidationException:
        Saying An operand in the update expression has an incorrect data type
    """
    dynamodb = create_simple_table_and_return_client()

    try:
        dynamodb.update_item(
            TableName="moto-test",
            Key={"id": {"S": "1"}},
            UpdateExpression="SET MyStr = MyStr + MyStr",
        )
        assert False, "Validation exception not thrown"
    except dynamodb.exceptions.ClientError as e:
        assert_correct_client_error(
            e,
            "ValidationException",
            "An operand in the update expression has an incorrect data type",
        )


# https://github.com/spulec/moto/issues/2806
@mock_dynamodb2
def test_update_item_with_attribute_in_right_hand_side():
    """
    After tokenization and building expression make sure referenced attributes are replaced with their current value
    """
    dynamodb = create_simple_table_and_return_client()

    # Make sure there are 2 values
    dynamodb.put_item(
        TableName="moto-test",
        Item={"id": {"S": "1"}, "myVal1": {"S": "Value1"}, "myVal2": {"S": "Value2"}},
    )

    dynamodb.update_item(
        TableName="moto-test",
        Key={"id": {"S": "1"}},
        UpdateExpression="SET myVal1 = myVal2",
    )

    result = dynamodb.get_item(TableName="moto-test", Key={"id": {"S": "1"}})
    assert result["Item"]["myVal1"]["S"] == result["Item"]["myVal2"]["S"] == "Value2"


@mock_dynamodb2
def test_multiple_updates():
    dynamodb = create_simple_table_and_return_client()
    dynamodb.put_item(
        TableName="moto-test",
        Item={"id": {"S": "1"}, "myNum": {"N": "1"}, "path": {"N": "6"}},
    )
    dynamodb.update_item(
        TableName="moto-test",
        Key={"id": {"S": "1"}},
        UpdateExpression="SET myNum = #p + :val, newAttr = myNum",
        ExpressionAttributeValues={":val": {"N": "1"}},
        ExpressionAttributeNames={"#p": "path"},
    )
    result = dynamodb.get_item(TableName="moto-test", Key={"id": {"S": "1"}})["Item"]
    expected_result = {
        "myNum": {"N": "7"},
        "newAttr": {"N": "1"},
        "path": {"N": "6"},
        "id": {"S": "1"},
    }
    assert result == expected_result


@mock_dynamodb2
def test_update_item_atomic_counter():
    table = "table_t"
    ddb_mock = boto3.client("dynamodb", region_name="eu-west-3")
    ddb_mock.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "t_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "t_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    key = {"t_id": {"S": "item1"}}

    ddb_mock.put_item(
        TableName=table,
        Item={"t_id": {"S": "item1"}, "n_i": {"N": "5"}, "n_f": {"N": "5.3"}},
    )

    ddb_mock.update_item(
        TableName=table,
        Key=key,
        UpdateExpression="set n_i = n_i + :inc1, n_f = n_f + :inc2",
        ExpressionAttributeValues={":inc1": {"N": "1.2"}, ":inc2": {"N": "0.05"}},
    )
    updated_item = ddb_mock.get_item(TableName=table, Key=key)["Item"]
    updated_item["n_i"]["N"].should.equal("6.2")
    updated_item["n_f"]["N"].should.equal("5.35")


@mock_dynamodb2
def test_update_item_atomic_counter_return_values():
    table = "table_t"
    ddb_mock = boto3.client("dynamodb", region_name="eu-west-3")
    ddb_mock.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "t_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "t_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    key = {"t_id": {"S": "item1"}}

    ddb_mock.put_item(TableName=table, Item={"t_id": {"S": "item1"}, "v": {"N": "5"}})

    response = ddb_mock.update_item(
        TableName=table,
        Key=key,
        UpdateExpression="set v = v + :inc",
        ExpressionAttributeValues={":inc": {"N": "1"}},
        ReturnValues="UPDATED_OLD",
    )
    assert (
        "v" in response["Attributes"]
    ), "v has been updated, and should be returned here"
    response["Attributes"]["v"]["N"].should.equal("5")

    # second update
    response = ddb_mock.update_item(
        TableName=table,
        Key=key,
        UpdateExpression="set v = v + :inc",
        ExpressionAttributeValues={":inc": {"N": "1"}},
        ReturnValues="UPDATED_OLD",
    )
    assert (
        "v" in response["Attributes"]
    ), "v has been updated, and should be returned here"
    response["Attributes"]["v"]["N"].should.equal("6")

    # third update
    response = ddb_mock.update_item(
        TableName=table,
        Key=key,
        UpdateExpression="set v = v + :inc",
        ExpressionAttributeValues={":inc": {"N": "1"}},
        ReturnValues="UPDATED_NEW",
    )
    assert (
        "v" in response["Attributes"]
    ), "v has been updated, and should be returned here"
    response["Attributes"]["v"]["N"].should.equal("8")


@mock_dynamodb2
def test_update_item_atomic_counter_from_zero():
    table = "table_t"
    ddb_mock = boto3.client("dynamodb", region_name="eu-west-1")
    ddb_mock.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "t_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "t_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    key = {"t_id": {"S": "item1"}}

    ddb_mock.put_item(
        TableName=table, Item=key,
    )

    ddb_mock.update_item(
        TableName=table,
        Key=key,
        UpdateExpression="add n_i :inc1, n_f :inc2",
        ExpressionAttributeValues={":inc1": {"N": "1.2"}, ":inc2": {"N": "-0.5"}},
    )
    updated_item = ddb_mock.get_item(TableName=table, Key=key)["Item"]
    assert updated_item["n_i"]["N"] == "1.2"
    assert updated_item["n_f"]["N"] == "-0.5"


@mock_dynamodb2
def test_update_item_add_to_non_existent_set():
    table = "table_t"
    ddb_mock = boto3.client("dynamodb", region_name="eu-west-1")
    ddb_mock.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "t_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "t_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    key = {"t_id": {"S": "item1"}}
    ddb_mock.put_item(
        TableName=table, Item=key,
    )

    ddb_mock.update_item(
        TableName=table,
        Key=key,
        UpdateExpression="add s_i :s1",
        ExpressionAttributeValues={":s1": {"SS": ["hello"]}},
    )
    updated_item = ddb_mock.get_item(TableName=table, Key=key)["Item"]
    assert updated_item["s_i"]["SS"] == ["hello"]


@mock_dynamodb2
def test_update_item_add_to_non_existent_number_set():
    table = "table_t"
    ddb_mock = boto3.client("dynamodb", region_name="eu-west-1")
    ddb_mock.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "t_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "t_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    key = {"t_id": {"S": "item1"}}
    ddb_mock.put_item(
        TableName=table, Item=key,
    )

    ddb_mock.update_item(
        TableName=table,
        Key=key,
        UpdateExpression="add s_i :s1",
        ExpressionAttributeValues={":s1": {"NS": ["3"]}},
    )
    updated_item = ddb_mock.get_item(TableName=table, Key=key)["Item"]
    assert updated_item["s_i"]["NS"] == ["3"]


def assert_correct_client_error(
    client_error, code, message_template, message_values=None, braces=None
):
    """
    Assert whether a client_error is as expected. Allow for a list of values to be passed into the message

    Args:
        client_error(ClientError): The ClientError exception that was raised
        code(str): The code for the error (e.g. ValidationException)
        message_template(str): Error message template. if message_values is not None then this template has a {values}
            as placeholder. For example:
            'Value provided in ExpressionAttributeValues unused in expressions: keys: {values}'
        message_values(list of str|None): The values that are passed in the error message
        braces(list of str|None): List of length 2 with opening and closing brace for the values. By default it will be
                                  surrounded by curly brackets
    """
    braces = braces or ["{", "}"]
    assert client_error.response["Error"]["Code"] == code
    if message_values is not None:
        values_string = "{open_brace}(?P<values>.*){close_brace}".format(
            open_brace=braces[0], close_brace=braces[1]
        )
        re_msg = re.compile(message_template.format(values=values_string))
        match_result = re_msg.match(client_error.response["Error"]["Message"])
        assert match_result is not None
        values_string = match_result.groupdict()["values"]
        values = [key for key in values_string.split(", ")]
        assert len(message_values) == len(values)
        for value in message_values:
            assert value in values
    else:
        assert client_error.response["Error"]["Message"] == message_template


def assert_raise_syntax_error(client_error, token, near):
    """
    Assert whether a client_error is as expected Syntax error. Syntax error looks like: `syntax_error_template`

    Args:
        client_error(ClientError): The ClientError exception that was raised
        token(str): The token that ws unexpected
        near(str): The part in the expression that shows where the error occurs it generally has the preceding token the
        optional separation and the problematic token.
    """
    syntax_error_template = (
        'Invalid UpdateExpression: Syntax error; token: "{token}", near: "{near}"'
    )
    expected_syntax_error = syntax_error_template.format(token=token, near=near)
    assert client_error.response["Error"]["Code"] == "ValidationException"
    assert expected_syntax_error == client_error.response["Error"]["Message"]


def create_simple_table_and_return_client():
    dynamodb = boto3.client("dynamodb", region_name="eu-west-1")
    dynamodb.create_table(
        TableName="moto-test",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"},],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    dynamodb.put_item(
        TableName="moto-test",
        Item={"id": {"S": "1"}, "myNum": {"N": "1"}, "MyStr": {"S": "1"},},
    )
    return dynamodb
