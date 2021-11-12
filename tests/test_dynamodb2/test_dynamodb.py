from __future__ import print_function

import uuid
from datetime import datetime
from decimal import Decimal

import boto
import boto3
from boto3.dynamodb.conditions import Attr, Key
import sure  # noqa # pylint: disable=unused-import
from moto import mock_dynamodb2, mock_dynamodb2_deprecated
from moto.dynamodb2 import dynamodb_backend2, dynamodb_backends2
from boto.exception import JSONResponseError
from botocore.exceptions import ClientError
from tests.helpers import requires_boto_gte

from moto.dynamodb2.limits import HASH_KEY_MAX_LENGTH, RANGE_KEY_MAX_LENGTH

import pytest

try:
    import boto.dynamodb2
except ImportError:
    print("This boto version is not supported")


@requires_boto_gte("2.9")
# Has boto3 equivalent
@mock_dynamodb2_deprecated
def test_list_tables():
    name = "TestTable"
    # Should make tables properly with boto
    dynamodb_backend2.create_table(
        name,
        schema=[
            {"KeyType": "HASH", "AttributeName": "forum_name"},
            {"KeyType": "RANGE", "AttributeName": "subject"},
        ],
    )
    conn = boto.dynamodb2.connect_to_region(
        "us-east-1", aws_access_key_id="ak", aws_secret_access_key="sk"
    )
    assert conn.list_tables()["TableNames"] == [name]


@mock_dynamodb2
@pytest.mark.parametrize(
    "names",
    [[], ["TestTable"], ["TestTable1", "TestTable2"]],
    ids=["no-table", "one-table", "multiple-tables"],
)
def test_list_tables_boto3(names):
    conn = boto3.client("dynamodb", region_name="us-west-2")
    for name in names:
        conn.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
    conn.list_tables()["TableNames"].should.equal(names)


@requires_boto_gte("2.9")
# Has boto3 equivalent
@mock_dynamodb2_deprecated
def test_list_tables_layer_1():
    # Should make tables properly with boto
    dynamodb_backend2.create_table(
        "test_1", schema=[{"KeyType": "HASH", "AttributeName": "name"}]
    )
    dynamodb_backend2.create_table(
        "test_2", schema=[{"KeyType": "HASH", "AttributeName": "name"}]
    )
    conn = boto.dynamodb2.connect_to_region(
        "us-east-1", aws_access_key_id="ak", aws_secret_access_key="sk"
    )

    res = conn.list_tables(limit=1)
    expected = {"TableNames": ["test_1"], "LastEvaluatedTableName": "test_1"}
    res.should.equal(expected)

    res = conn.list_tables(limit=1, exclusive_start_table_name="test_1")
    expected = {"TableNames": ["test_2"]}
    res.should.equal(expected)


@mock_dynamodb2
def test_list_tables_paginated():
    conn = boto3.client("dynamodb", region_name="us-west-2")
    for name in ["name1", "name2", "name3"]:
        conn.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )

    res = conn.list_tables(Limit=2)
    res.should.have.key("TableNames").equal(["name1", "name2"])
    res.should.have.key("LastEvaluatedTableName").equal("name2")

    res = conn.list_tables(Limit=1, ExclusiveStartTableName="name1")
    res.should.have.key("TableNames").equal(["name2"])
    res.should.have.key("LastEvaluatedTableName").equal("name2")

    res = conn.list_tables(ExclusiveStartTableName="name1")
    res.should.have.key("TableNames").equal(["name2", "name3"])
    res.shouldnt.have.key("LastEvaluatedTableName")


@requires_boto_gte("2.9")
# Has boto3 equivalent
@mock_dynamodb2_deprecated
def test_describe_missing_table():
    conn = boto.dynamodb2.connect_to_region(
        "us-west-2", aws_access_key_id="ak", aws_secret_access_key="sk"
    )
    with pytest.raises(JSONResponseError):
        conn.describe_table("messages")


@mock_dynamodb2
def test_describe_missing_table_boto3():
    conn = boto3.client("dynamodb", region_name="us-west-2")
    with pytest.raises(ClientError) as ex:
        conn.describe_table(TableName="messages")
    ex.value.response["Error"]["Code"].should.equal("ResourceNotFoundException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Message"].should.equal("Requested resource not found")


@requires_boto_gte("2.9")
@mock_dynamodb2
def test_list_table_tags():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table_description = conn.describe_table(TableName=name)
    arn = table_description["Table"]["TableArn"]

    # Tag table
    tags = [
        {"Key": "TestTag", "Value": "TestValue"},
        {"Key": "TestTag2", "Value": "TestValue2"},
    ]
    conn.tag_resource(ResourceArn=arn, Tags=tags)

    # Check tags
    resp = conn.list_tags_of_resource(ResourceArn=arn)
    assert resp["Tags"] == tags

    # Remove 1 tag
    conn.untag_resource(ResourceArn=arn, TagKeys=["TestTag"])

    # Check tags
    resp = conn.list_tags_of_resource(ResourceArn=arn)
    assert resp["Tags"] == [{"Key": "TestTag2", "Value": "TestValue2"}]


@requires_boto_gte("2.9")
@mock_dynamodb2
def test_list_table_tags_empty():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table_description = conn.describe_table(TableName=name)
    arn = table_description["Table"]["TableArn"]
    resp = conn.list_tags_of_resource(ResourceArn=arn)
    assert resp["Tags"] == []


@requires_boto_gte("2.9")
@mock_dynamodb2
def test_list_table_tags_paginated():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table_description = conn.describe_table(TableName=name)
    arn = table_description["Table"]["TableArn"]
    for i in range(11):
        tags = [{"Key": "TestTag%d" % i, "Value": "TestValue"}]
        conn.tag_resource(ResourceArn=arn, Tags=tags)
    resp = conn.list_tags_of_resource(ResourceArn=arn)
    assert len(resp["Tags"]) == 10
    assert "NextToken" in resp.keys()
    resp2 = conn.list_tags_of_resource(ResourceArn=arn, NextToken=resp["NextToken"])
    assert len(resp2["Tags"]) == 1
    assert "NextToken" not in resp2.keys()


@requires_boto_gte("2.9")
@mock_dynamodb2
def test_list_not_found_table_tags():
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    arn = "DymmyArn"
    try:
        conn.list_tags_of_resource(ResourceArn=arn)
    except ClientError as exception:
        assert exception.response["Error"]["Code"] == "ResourceNotFoundException"


@mock_dynamodb2
def test_item_add_empty_string_hash_key_exception():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    with pytest.raises(ClientError) as ex:
        conn.put_item(
            TableName=name,
            Item={
                "forum_name": {"S": ""},
                "subject": {"S": "Check this out!"},
                "Body": {"S": "http://url_to_lolcat.gif"},
                "SentBy": {"S": "someone@somewhere.edu"},
                "ReceivedTime": {"S": "12/9/2011 11:36:03 PM"},
            },
        )

    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Message"].should.equal(
        "One or more parameter values were invalid: An AttributeValue may not contain an empty string"
    )


@mock_dynamodb2
def test_item_add_empty_string_range_key_exception():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    conn.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "forum_name", "KeyType": "HASH"},
            {"AttributeName": "ReceivedTime", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "forum_name", "AttributeType": "S"},
            {"AttributeName": "ReceivedTime", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    with pytest.raises(ClientError) as ex:
        conn.put_item(
            TableName=name,
            Item={
                "forum_name": {"S": "LOLCat Forum"},
                "subject": {"S": "Check this out!"},
                "Body": {"S": "http://url_to_lolcat.gif"},
                "SentBy": {"S": "someone@somewhere.edu"},
                "ReceivedTime": {"S": ""},
            },
        )

    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Message"].should.equal(
        "One or more parameter values were invalid: An AttributeValue may not contain an empty string"
    )


@mock_dynamodb2
def test_item_add_empty_string_attr_no_exception():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    conn.put_item(
        TableName=name,
        Item={
            "forum_name": {"S": "LOLCat Forum"},
            "subject": {"S": "Check this out!"},
            "Body": {"S": "http://url_to_lolcat.gif"},
            "SentBy": {"S": ""},
            "ReceivedTime": {"S": "12/9/2011 11:36:03 PM"},
        },
    )


@mock_dynamodb2
def test_update_item_with_empty_string_attr_no_exception():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    conn.put_item(
        TableName=name,
        Item={
            "forum_name": {"S": "LOLCat Forum"},
            "subject": {"S": "Check this out!"},
            "Body": {"S": "http://url_to_lolcat.gif"},
            "SentBy": {"S": "test"},
            "ReceivedTime": {"S": "12/9/2011 11:36:03 PM"},
        },
    )

    conn.update_item(
        TableName=name,
        Key={"forum_name": {"S": "LOLCat Forum"}},
        UpdateExpression="set Body=:Body",
        ExpressionAttributeValues={":Body": {"S": ""}},
    )


@mock_dynamodb2
def test_item_add_long_string_hash_key_exception():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    conn.put_item(
        TableName=name,
        Item={
            "forum_name": {"S": "x" * HASH_KEY_MAX_LENGTH},
            "subject": {"S": "Check this out!"},
            "Body": {"S": "http://url_to_lolcat.gif"},
            "SentBy": {"S": "test"},
            "ReceivedTime": {"S": "12/9/2011 11:36:03 PM"},
        },
    )

    with pytest.raises(ClientError) as ex:
        conn.put_item(
            TableName=name,
            Item={
                "forum_name": {"S": "x" * (HASH_KEY_MAX_LENGTH + 1)},
                "subject": {"S": "Check this out!"},
                "Body": {"S": "http://url_to_lolcat.gif"},
                "SentBy": {"S": "test"},
                "ReceivedTime": {"S": "12/9/2011 11:36:03 PM"},
            },
        )
    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    # deliberately no space between "of" and "2048"
    ex.value.response["Error"]["Message"].should.equal(
        "One or more parameter values were invalid: Size of hashkey has exceeded the maximum size limit of2048 bytes"
    )


@mock_dynamodb2
def test_item_add_long_string_nonascii_hash_key_exception():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    emoji_b = b"\xf0\x9f\x98\x83"  # smile emoji
    emoji = emoji_b.decode("utf-8")  # 1 character, but 4 bytes
    short_enough = emoji * int(HASH_KEY_MAX_LENGTH / len(emoji.encode("utf-8")))
    too_long = "x" + short_enough

    conn.put_item(
        TableName=name,
        Item={
            "forum_name": {"S": short_enough},
            "subject": {"S": "Check this out!"},
            "Body": {"S": "http://url_to_lolcat.gif"},
            "SentBy": {"S": "test"},
            "ReceivedTime": {"S": "12/9/2011 11:36:03 PM"},
        },
    )

    with pytest.raises(ClientError) as ex:
        conn.put_item(
            TableName=name,
            Item={
                "forum_name": {"S": too_long},
                "subject": {"S": "Check this out!"},
                "Body": {"S": "http://url_to_lolcat.gif"},
                "SentBy": {"S": "test"},
                "ReceivedTime": {"S": "12/9/2011 11:36:03 PM"},
            },
        )

    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    # deliberately no space between "of" and "2048"
    ex.value.response["Error"]["Message"].should.equal(
        "One or more parameter values were invalid: Size of hashkey has exceeded the maximum size limit of2048 bytes"
    )


@mock_dynamodb2
def test_item_add_long_string_range_key_exception():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    conn.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "forum_name", "KeyType": "HASH"},
            {"AttributeName": "ReceivedTime", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "forum_name", "AttributeType": "S"},
            {"AttributeName": "ReceivedTime", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    conn.put_item(
        TableName=name,
        Item={
            "forum_name": {"S": "LOLCat Forum"},
            "subject": {"S": "Check this out!"},
            "Body": {"S": "http://url_to_lolcat.gif"},
            "SentBy": {"S": "someone@somewhere.edu"},
            "ReceivedTime": {"S": "x" * RANGE_KEY_MAX_LENGTH},
        },
    )

    with pytest.raises(ClientError) as ex:
        conn.put_item(
            TableName=name,
            Item={
                "forum_name": {"S": "LOLCat Forum"},
                "subject": {"S": "Check this out!"},
                "Body": {"S": "http://url_to_lolcat.gif"},
                "SentBy": {"S": "someone@somewhere.edu"},
                "ReceivedTime": {"S": "x" * (RANGE_KEY_MAX_LENGTH + 1)},
            },
        )

    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Message"].should.equal(
        "One or more parameter values were invalid: Aggregated size of all range keys has exceeded the size limit of 1024 bytes"
    )


@mock_dynamodb2
def test_update_item_with_long_string_hash_key_exception():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    conn.update_item(
        TableName=name,
        Key={
            "forum_name": {"S": "x" * HASH_KEY_MAX_LENGTH},
            "ReceivedTime": {"S": "12/9/2011 11:36:03 PM"},
        },
        UpdateExpression="set body=:New",
        ExpressionAttributeValues={":New": {"S": "hello"}},
    )

    with pytest.raises(ClientError) as ex:
        conn.update_item(
            TableName=name,
            Key={
                "forum_name": {"S": "x" * (HASH_KEY_MAX_LENGTH + 1)},
                "ReceivedTime": {"S": "12/9/2011 11:36:03 PM"},
            },
            UpdateExpression="set body=:New",
            ExpressionAttributeValues={":New": {"S": "hello"}},
        )

    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    # deliberately no space between "of" and "2048"
    ex.value.response["Error"]["Message"].should.equal(
        "One or more parameter values were invalid: Size of hashkey has exceeded the maximum size limit of2048 bytes"
    )


@mock_dynamodb2
def test_update_item_with_long_string_range_key_exception():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    conn.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "forum_name", "KeyType": "HASH"},
            {"AttributeName": "ReceivedTime", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "forum_name", "AttributeType": "S"},
            {"AttributeName": "ReceivedTime", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    conn.update_item(
        TableName=name,
        Key={
            "forum_name": {"S": "Lolcat Forum"},
            "ReceivedTime": {"S": "x" * RANGE_KEY_MAX_LENGTH},
        },
        UpdateExpression="set body=:New",
        ExpressionAttributeValues={":New": {"S": "hello"}},
    )

    with pytest.raises(ClientError) as ex:
        conn.update_item(
            TableName=name,
            Key={
                "forum_name": {"S": "Lolcat Forum"},
                "ReceivedTime": {"S": "x" * (RANGE_KEY_MAX_LENGTH + 1)},
            },
            UpdateExpression="set body=:New",
            ExpressionAttributeValues={":New": {"S": "hello"}},
        )

    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    # deliberately no space between "of" and "2048"
    ex.value.response["Error"]["Message"].should.equal(
        "One or more parameter values were invalid: Aggregated size of all range keys has exceeded the size limit of 1024 bytes"
    )


@requires_boto_gte("2.9")
@mock_dynamodb2
def test_query_invalid_table():
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )
    try:
        conn.query(
            TableName="invalid_table",
            KeyConditionExpression="index1 = :partitionkeyval",
            ExpressionAttributeValues={":partitionkeyval": {"S": "test"}},
        )
    except ClientError as exception:
        assert exception.response["Error"]["Code"] == "ResourceNotFoundException"


@requires_boto_gte("2.9")
@mock_dynamodb2
def test_put_item_with_special_chars():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )

    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    conn.put_item(
        TableName=name,
        Item={
            "forum_name": {"S": "LOLCat Forum"},
            "subject": {"S": "Check this out!"},
            "Body": {"S": "http://url_to_lolcat.gif"},
            "SentBy": {"S": "test"},
            "ReceivedTime": {"S": "12/9/2011 11:36:03 PM"},
            '"': {"S": "foo"},
        },
    )


@requires_boto_gte("2.9")
@mock_dynamodb2
def test_put_item_with_streams():
    name = "TestTable"
    conn = boto3.client(
        "dynamodb",
        region_name="us-west-2",
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
    )

    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    conn.put_item(
        TableName=name,
        Item={
            "forum_name": {"S": "LOLCat Forum"},
            "subject": {"S": "Check this out!"},
            "Body": {"S": "http://url_to_lolcat.gif"},
            "SentBy": {"S": "test"},
            "Data": {"M": {"Key1": {"S": "Value1"}, "Key2": {"S": "Value2"}}},
        },
    )

    result = conn.get_item(TableName=name, Key={"forum_name": {"S": "LOLCat Forum"}})

    result["Item"].should.be.equal(
        {
            "forum_name": {"S": "LOLCat Forum"},
            "subject": {"S": "Check this out!"},
            "Body": {"S": "http://url_to_lolcat.gif"},
            "SentBy": {"S": "test"},
            "Data": {"M": {"Key1": {"S": "Value1"}, "Key2": {"S": "Value2"}}},
        }
    )
    table = dynamodb_backends2["us-west-2"].get_table(name)
    if not table:
        # There is no way to access stream data over the API, so this part can't run in server-tests mode.
        return
    len(table.stream_shard.items).should.be.equal(1)
    stream_record = table.stream_shard.items[0].record
    stream_record["eventName"].should.be.equal("INSERT")
    stream_record["dynamodb"]["SizeBytes"].should.be.equal(447)


@mock_dynamodb2
def test_basic_projection_expression_using_get_item():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    table = dynamodb.create_table(
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
        Item={"forum_name": "the-key", "subject": "123", "body": "some test message"}
    )

    table.put_item(
        Item={
            "forum_name": "not-the-key",
            "subject": "123",
            "body": "some other test message",
        }
    )
    result = table.get_item(
        Key={"forum_name": "the-key", "subject": "123"},
        ProjectionExpression="body, subject",
    )

    result["Item"].should.be.equal({"subject": "123", "body": "some test message"})

    # The projection expression should not remove data from storage
    result = table.get_item(Key={"forum_name": "the-key", "subject": "123"})

    result["Item"].should.be.equal(
        {"forum_name": "the-key", "subject": "123", "body": "some test message"}
    )


@mock_dynamodb2
def test_basic_projection_expressions_using_scan():
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

    table.put_item(
        Item={"forum_name": "the-key", "subject": "123", "body": "some test message"}
    )

    table.put_item(
        Item={
            "forum_name": "not-the-key",
            "subject": "123",
            "body": "some other test message",
        }
    )
    # Test a scan returning all items
    results = table.scan(
        FilterExpression=Key("forum_name").eq("the-key"),
        ProjectionExpression="body, subject",
    )

    assert "body" in results["Items"][0]
    assert results["Items"][0]["body"] == "some test message"
    assert "subject" in results["Items"][0]

    table.put_item(
        Item={
            "forum_name": "the-key",
            "subject": "1234",
            "body": "yet another test message",
        }
    )

    results = table.scan(
        FilterExpression=Key("forum_name").eq("the-key"), ProjectionExpression="body"
    )

    bodies = [item["body"] for item in results["Items"]]
    bodies.should.contain("some test message")
    bodies.should.contain("yet another test message")
    assert "subject" not in results["Items"][0]
    assert "forum_name" not in results["Items"][0]
    assert "subject" not in results["Items"][1]
    assert "forum_name" not in results["Items"][1]

    # The projection expression should not remove data from storage
    results = table.query(KeyConditionExpression=Key("forum_name").eq("the-key"))
    assert "subject" in results["Items"][0]
    assert "body" in results["Items"][1]
    assert "forum_name" in results["Items"][1]


@mock_dynamodb2
def test_nested_projection_expression_using_get_item():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    dynamodb.create_table(
        TableName="users",
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table = dynamodb.Table("users")
    table.put_item(
        Item={
            "forum_name": "key1",
            "nested": {
                "level1": {"id": "id1", "att": "irrelevant"},
                "level2": {"id": "id2", "include": "all"},
                "level3": {"id": "irrelevant"},
            },
            "foo": "bar",
        }
    )
    table.put_item(
        Item={
            "forum_name": "key2",
            "nested": {"id": "id2", "incode": "code2"},
            "foo": "bar",
        }
    )

    # Test a get_item returning all items
    result = table.get_item(
        Key={"forum_name": "key1"},
        ProjectionExpression="nested.level1.id, nested.level2",
    )["Item"]
    result.should.equal(
        {"nested": {"level1": {"id": "id1"}, "level2": {"id": "id2", "include": "all"}}}
    )
    # Assert actual data has not been deleted
    result = table.get_item(Key={"forum_name": "key1"})["Item"]
    result.should.equal(
        {
            "foo": "bar",
            "forum_name": "key1",
            "nested": {
                "level1": {"id": "id1", "att": "irrelevant"},
                "level2": {"id": "id2", "include": "all"},
                "level3": {"id": "irrelevant"},
            },
        }
    )


@mock_dynamodb2
def test_basic_projection_expressions_using_query():
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
    table.put_item(
        Item={"forum_name": "the-key", "subject": "123", "body": "some test message"}
    )
    table.put_item(
        Item={
            "forum_name": "not-the-key",
            "subject": "123",
            "body": "some other test message",
        }
    )

    # Test a query returning all items
    result = table.query(
        KeyConditionExpression=Key("forum_name").eq("the-key"),
        ProjectionExpression="body, subject",
    )["Items"][0]

    assert "body" in result
    assert result["body"] == "some test message"
    assert "subject" in result
    assert "forum_name" not in result

    table.put_item(
        Item={
            "forum_name": "the-key",
            "subject": "1234",
            "body": "yet another test message",
        }
    )

    items = table.query(
        KeyConditionExpression=Key("forum_name").eq("the-key"),
        ProjectionExpression="body",
    )["Items"]

    assert "body" in items[0]
    assert "subject" not in items[0]
    assert items[0]["body"] == "some test message"
    assert "body" in items[1]
    assert "subject" not in items[1]
    assert items[1]["body"] == "yet another test message"

    # The projection expression should not remove data from storage
    items = table.query(KeyConditionExpression=Key("forum_name").eq("the-key"))["Items"]
    assert "subject" in items[0]
    assert "body" in items[1]
    assert "forum_name" in items[1]


@mock_dynamodb2
def test_nested_projection_expression_using_query():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    dynamodb.create_table(
        TableName="users",
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table = dynamodb.Table("users")
    table.put_item(
        Item={
            "forum_name": "key1",
            "nested": {
                "level1": {"id": "id1", "att": "irrelevant"},
                "level2": {"id": "id2", "include": "all"},
                "level3": {"id": "irrelevant"},
            },
            "foo": "bar",
        }
    )
    table.put_item(
        Item={
            "forum_name": "key2",
            "nested": {"id": "id2", "incode": "code2"},
            "foo": "bar",
        }
    )

    # Test a query returning all items
    result = table.query(
        KeyConditionExpression=Key("forum_name").eq("key1"),
        ProjectionExpression="nested.level1.id, nested.level2",
    )["Items"][0]

    assert "nested" in result
    result["nested"].should.equal(
        {"level1": {"id": "id1"}, "level2": {"id": "id2", "include": "all"}}
    )
    assert "foo" not in result
    # Assert actual data has not been deleted
    result = table.query(KeyConditionExpression=Key("forum_name").eq("key1"))["Items"][
        0
    ]
    result.should.equal(
        {
            "foo": "bar",
            "forum_name": "key1",
            "nested": {
                "level1": {"id": "id1", "att": "irrelevant"},
                "level2": {"id": "id2", "include": "all"},
                "level3": {"id": "irrelevant"},
            },
        }
    )


@mock_dynamodb2
def test_nested_projection_expression_using_scan():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    dynamodb.create_table(
        TableName="users",
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table = dynamodb.Table("users")
    table.put_item(
        Item={
            "forum_name": "key1",
            "nested": {
                "level1": {"id": "id1", "att": "irrelevant"},
                "level2": {"id": "id2", "include": "all"},
                "level3": {"id": "irrelevant"},
            },
            "foo": "bar",
        }
    )
    table.put_item(
        Item={
            "forum_name": "key2",
            "nested": {"id": "id2", "incode": "code2"},
            "foo": "bar",
        }
    )

    # Test a scan
    results = table.scan(
        FilterExpression=Key("forum_name").eq("key1"),
        ProjectionExpression="nested.level1.id, nested.level2",
    )["Items"]
    results.should.equal(
        [
            {
                "nested": {
                    "level1": {"id": "id1"},
                    "level2": {"include": "all", "id": "id2"},
                }
            }
        ]
    )
    # Assert original data is still there
    results = table.scan(FilterExpression=Key("forum_name").eq("key1"))["Items"]
    results.should.equal(
        [
            {
                "forum_name": "key1",
                "foo": "bar",
                "nested": {
                    "level1": {"att": "irrelevant", "id": "id1"},
                    "level2": {"include": "all", "id": "id2"},
                    "level3": {"id": "irrelevant"},
                },
            }
        ]
    )


@mock_dynamodb2
def test_basic_projection_expression_using_get_item_with_attr_expression_names():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    table = dynamodb.create_table(
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
            "body": "some test message",
            "attachment": "something",
        }
    )

    table.put_item(
        Item={
            "forum_name": "not-the-key",
            "subject": "123",
            "body": "some other test message",
            "attachment": "something",
        }
    )
    result = table.get_item(
        Key={"forum_name": "the-key", "subject": "123"},
        ProjectionExpression="#rl, #rt, subject",
        ExpressionAttributeNames={"#rl": "body", "#rt": "attachment"},
    )

    result["Item"].should.be.equal(
        {"subject": "123", "body": "some test message", "attachment": "something"}
    )


@mock_dynamodb2
def test_basic_projection_expressions_using_query_with_attr_expression_names():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    table = dynamodb.create_table(
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
            "body": "some test message",
            "attachment": "something",
        }
    )

    table.put_item(
        Item={
            "forum_name": "not-the-key",
            "subject": "123",
            "body": "some other test message",
            "attachment": "something",
        }
    )
    # Test a query returning all items

    results = table.query(
        KeyConditionExpression=Key("forum_name").eq("the-key"),
        ProjectionExpression="#rl, #rt, subject",
        ExpressionAttributeNames={"#rl": "body", "#rt": "attachment"},
    )

    assert "body" in results["Items"][0]
    assert results["Items"][0]["body"] == "some test message"
    assert "subject" in results["Items"][0]
    assert results["Items"][0]["subject"] == "123"
    assert "attachment" in results["Items"][0]
    assert results["Items"][0]["attachment"] == "something"


@mock_dynamodb2
def test_nested_projection_expression_using_get_item_with_attr_expression():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    dynamodb.create_table(
        TableName="users",
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table = dynamodb.Table("users")
    table.put_item(
        Item={
            "forum_name": "key1",
            "nested": {
                "level1": {"id": "id1", "att": "irrelevant"},
                "level2": {"id": "id2", "include": "all"},
                "level3": {"id": "irrelevant"},
            },
            "foo": "bar",
        }
    )
    table.put_item(
        Item={
            "forum_name": "key2",
            "nested": {"id": "id2", "incode": "code2"},
            "foo": "bar",
        }
    )

    # Test a get_item returning all items
    result = table.get_item(
        Key={"forum_name": "key1"},
        ProjectionExpression="#nst.level1.id, #nst.#lvl2",
        ExpressionAttributeNames={"#nst": "nested", "#lvl2": "level2"},
    )["Item"]
    result.should.equal(
        {"nested": {"level1": {"id": "id1"}, "level2": {"id": "id2", "include": "all"}}}
    )
    # Assert actual data has not been deleted
    result = table.get_item(Key={"forum_name": "key1"})["Item"]
    result.should.equal(
        {
            "foo": "bar",
            "forum_name": "key1",
            "nested": {
                "level1": {"id": "id1", "att": "irrelevant"},
                "level2": {"id": "id2", "include": "all"},
                "level3": {"id": "irrelevant"},
            },
        }
    )


@mock_dynamodb2
def test_nested_projection_expression_using_query_with_attr_expression_names():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    dynamodb.create_table(
        TableName="users",
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table = dynamodb.Table("users")
    table.put_item(
        Item={
            "forum_name": "key1",
            "nested": {
                "level1": {"id": "id1", "att": "irrelevant"},
                "level2": {"id": "id2", "include": "all"},
                "level3": {"id": "irrelevant"},
            },
            "foo": "bar",
        }
    )
    table.put_item(
        Item={
            "forum_name": "key2",
            "nested": {"id": "id2", "incode": "code2"},
            "foo": "bar",
        }
    )

    # Test a query returning all items
    result = table.query(
        KeyConditionExpression=Key("forum_name").eq("key1"),
        ProjectionExpression="#nst.level1.id, #nst.#lvl2",
        ExpressionAttributeNames={"#nst": "nested", "#lvl2": "level2"},
    )["Items"][0]

    assert "nested" in result
    result["nested"].should.equal(
        {"level1": {"id": "id1"}, "level2": {"id": "id2", "include": "all"}}
    )
    assert "foo" not in result
    # Assert actual data has not been deleted
    result = table.query(KeyConditionExpression=Key("forum_name").eq("key1"))["Items"][
        0
    ]
    result.should.equal(
        {
            "foo": "bar",
            "forum_name": "key1",
            "nested": {
                "level1": {"id": "id1", "att": "irrelevant"},
                "level2": {"id": "id2", "include": "all"},
                "level3": {"id": "irrelevant"},
            },
        }
    )


@mock_dynamodb2
def test_basic_projection_expressions_using_scan_with_attr_expression_names():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    table = dynamodb.create_table(
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
            "body": "some test message",
            "attachment": "something",
        }
    )

    table.put_item(
        Item={
            "forum_name": "not-the-key",
            "subject": "123",
            "body": "some other test message",
            "attachment": "something",
        }
    )
    # Test a scan returning all items

    results = table.scan(
        FilterExpression=Key("forum_name").eq("the-key"),
        ProjectionExpression="#rl, #rt, subject",
        ExpressionAttributeNames={"#rl": "body", "#rt": "attachment"},
    )

    assert "body" in results["Items"][0]
    assert "attachment" in results["Items"][0]
    assert "subject" in results["Items"][0]
    assert "form_name" not in results["Items"][0]

    # Test without a FilterExpression
    results = table.scan(
        ProjectionExpression="#rl, #rt, subject",
        ExpressionAttributeNames={"#rl": "body", "#rt": "attachment"},
    )

    assert "body" in results["Items"][0]
    assert "attachment" in results["Items"][0]
    assert "subject" in results["Items"][0]
    assert "form_name" not in results["Items"][0]


@mock_dynamodb2
def test_nested_projection_expression_using_scan_with_attr_expression_names():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    dynamodb.create_table(
        TableName="users",
        KeySchema=[{"AttributeName": "forum_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table = dynamodb.Table("users")
    table.put_item(
        Item={
            "forum_name": "key1",
            "nested": {
                "level1": {"id": "id1", "att": "irrelevant"},
                "level2": {"id": "id2", "include": "all"},
                "level3": {"id": "irrelevant"},
            },
            "foo": "bar",
        }
    )
    table.put_item(
        Item={
            "forum_name": "key2",
            "nested": {"id": "id2", "incode": "code2"},
            "foo": "bar",
        }
    )

    # Test a scan
    results = table.scan(
        FilterExpression=Key("forum_name").eq("key1"),
        ProjectionExpression="nested.level1.id, nested.level2",
        ExpressionAttributeNames={"#nst": "nested", "#lvl2": "level2"},
    )["Items"]
    results.should.equal(
        [
            {
                "nested": {
                    "level1": {"id": "id1"},
                    "level2": {"include": "all", "id": "id2"},
                }
            }
        ]
    )
    # Assert original data is still there
    results = table.scan(FilterExpression=Key("forum_name").eq("key1"))["Items"]
    results.should.equal(
        [
            {
                "forum_name": "key1",
                "foo": "bar",
                "nested": {
                    "level1": {"att": "irrelevant", "id": "id1"},
                    "level2": {"include": "all", "id": "id2"},
                    "level3": {"id": "irrelevant"},
                },
            }
        ]
    )


@mock_dynamodb2
def test_put_empty_item():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        AttributeDefinitions=[{"AttributeName": "structure_id", "AttributeType": "S"},],
        TableName="test",
        KeySchema=[{"AttributeName": "structure_id", "KeyType": "HASH"},],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )
    table = dynamodb.Table("test")

    with pytest.raises(ClientError) as ex:
        table.put_item(Item={})
    ex.value.response["Error"]["Message"].should.equal(
        "One or more parameter values were invalid: Missing the key structure_id in the item"
    )
    ex.value.response["Error"]["Code"].should.equal("ValidationException")


@mock_dynamodb2
def test_put_item_nonexisting_hash_key():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        AttributeDefinitions=[{"AttributeName": "structure_id", "AttributeType": "S"},],
        TableName="test",
        KeySchema=[{"AttributeName": "structure_id", "KeyType": "HASH"},],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )
    table = dynamodb.Table("test")

    with pytest.raises(ClientError) as ex:
        table.put_item(Item={"a_terribly_misguided_id_attribute": "abcdef"})
    ex.value.response["Error"]["Message"].should.equal(
        "One or more parameter values were invalid: Missing the key structure_id in the item"
    )
    ex.value.response["Error"]["Code"].should.equal("ValidationException")


@mock_dynamodb2
def test_put_item_nonexisting_range_key():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        AttributeDefinitions=[
            {"AttributeName": "structure_id", "AttributeType": "S"},
            {"AttributeName": "added_at", "AttributeType": "N"},
        ],
        TableName="test",
        KeySchema=[
            {"AttributeName": "structure_id", "KeyType": "HASH"},
            {"AttributeName": "added_at", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )
    table = dynamodb.Table("test")

    with pytest.raises(ClientError) as ex:
        table.put_item(Item={"structure_id": "abcdef"})
    ex.value.response["Error"]["Message"].should.equal(
        "One or more parameter values were invalid: Missing the key added_at in the item"
    )
    ex.value.response["Error"]["Code"].should.equal("ValidationException")


@mock_dynamodb2
def test_query_filter():
    client = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )
    client.put_item(
        TableName="test1",
        Item={
            "client": {"S": "client1"},
            "app": {"S": "app1"},
            "nested": {
                "M": {
                    "version": {"S": "version1"},
                    "contents": {"L": [{"S": "value1"}, {"S": "value2"}]},
                }
            },
        },
    )
    client.put_item(
        TableName="test1",
        Item={
            "client": {"S": "client1"},
            "app": {"S": "app2"},
            "nested": {
                "M": {
                    "version": {"S": "version2"},
                    "contents": {"L": [{"S": "value1"}, {"S": "value2"}]},
                }
            },
        },
    )

    table = dynamodb.Table("test1")
    response = table.query(KeyConditionExpression=Key("client").eq("client1"))
    assert response["Count"] == 2

    response = table.query(
        KeyConditionExpression=Key("client").eq("client1"),
        FilterExpression=Attr("app").eq("app2"),
    )
    assert response["Count"] == 1
    assert response["Items"][0]["app"] == "app2"
    response = table.query(
        KeyConditionExpression=Key("client").eq("client1"),
        FilterExpression=Attr("app").contains("app"),
    )
    assert response["Count"] == 2

    response = table.query(
        KeyConditionExpression=Key("client").eq("client1"),
        FilterExpression=Attr("nested.version").contains("version"),
    )
    assert response["Count"] == 2

    response = table.query(
        KeyConditionExpression=Key("client").eq("client1"),
        FilterExpression=Attr("nested.contents[0]").eq("value1"),
    )
    assert response["Count"] == 2


@mock_dynamodb2
def test_query_filter_overlapping_expression_prefixes():
    client = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )

    client.put_item(
        TableName="test1",
        Item={
            "client": {"S": "client1"},
            "app": {"S": "app1"},
            "nested": {
                "M": {
                    "version": {"S": "version1"},
                    "contents": {"L": [{"S": "value1"}, {"S": "value2"}]},
                }
            },
        },
    )

    table = dynamodb.Table("test1")
    response = table.query(
        KeyConditionExpression=Key("client").eq("client1") & Key("app").eq("app1"),
        ProjectionExpression="#1, #10, nested",
        ExpressionAttributeNames={"#1": "client", "#10": "app"},
    )

    assert response["Count"] == 1
    assert response["Items"][0] == {
        "client": "client1",
        "app": "app1",
        "nested": {"version": "version1", "contents": ["value1", "value2"]},
    }


@mock_dynamodb2
def test_scan_filter():
    client = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )
    client.put_item(
        TableName="test1", Item={"client": {"S": "client1"}, "app": {"S": "app1"}}
    )

    table = dynamodb.Table("test1")
    response = table.scan(FilterExpression=Attr("app").eq("app2"))
    assert response["Count"] == 0

    response = table.scan(FilterExpression=Attr("app").eq("app1"))
    assert response["Count"] == 1

    response = table.scan(FilterExpression=Attr("app").ne("app2"))
    assert response["Count"] == 1

    response = table.scan(FilterExpression=Attr("app").ne("app1"))
    assert response["Count"] == 0


@mock_dynamodb2
def test_scan_filter2():
    client = boto3.client("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )
    client.put_item(
        TableName="test1", Item={"client": {"S": "client1"}, "app": {"N": "1"}}
    )

    response = client.scan(
        TableName="test1",
        Select="ALL_ATTRIBUTES",
        FilterExpression="#tb >= :dt",
        ExpressionAttributeNames={"#tb": "app"},
        ExpressionAttributeValues={":dt": {"N": str(1)}},
    )
    assert response["Count"] == 1


@mock_dynamodb2
def test_scan_filter3():
    client = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )
    client.put_item(
        TableName="test1",
        Item={"client": {"S": "client1"}, "app": {"N": "1"}, "active": {"BOOL": True}},
    )

    table = dynamodb.Table("test1")
    response = table.scan(FilterExpression=Attr("active").eq(True))
    assert response["Count"] == 1

    response = table.scan(FilterExpression=Attr("active").ne(True))
    assert response["Count"] == 0

    response = table.scan(FilterExpression=Attr("active").ne(False))
    assert response["Count"] == 1

    response = table.scan(FilterExpression=Attr("app").ne(1))
    assert response["Count"] == 0

    response = table.scan(FilterExpression=Attr("app").ne(2))
    assert response["Count"] == 1


@mock_dynamodb2
def test_scan_filter4():
    client = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )

    table = dynamodb.Table("test1")
    response = table.scan(
        FilterExpression=Attr("epoch_ts").lt(7) & Attr("fanout_ts").not_exists()
    )
    # Just testing
    assert response["Count"] == 0


@mock_dynamodb2
def test_scan_filter_should_not_return_non_existing_attributes():
    table_name = "my-table"
    item = {"partitionKey": "pk-2", "my-attr": 42}
    # Create table
    res = boto3.resource("dynamodb", region_name="us-east-1")
    res.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "partitionKey", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "partitionKey", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table = res.Table(table_name)
    # Insert items
    table.put_item(Item={"partitionKey": "pk-1"})
    table.put_item(Item=item)
    # Verify a few operations
    # Assert we only find the item that has this attribute
    table.scan(FilterExpression=Attr("my-attr").lt(43))["Items"].should.equal([item])
    table.scan(FilterExpression=Attr("my-attr").lte(42))["Items"].should.equal([item])
    table.scan(FilterExpression=Attr("my-attr").gte(42))["Items"].should.equal([item])
    table.scan(FilterExpression=Attr("my-attr").gt(41))["Items"].should.equal([item])
    # Sanity check that we can't find the item if the FE is wrong
    table.scan(FilterExpression=Attr("my-attr").gt(43))["Items"].should.equal([])


@mock_dynamodb2
def test_bad_scan_filter():
    client = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )
    table = dynamodb.Table("test1")

    # Bad expression
    try:
        table.scan(FilterExpression="client test")
    except ClientError as err:
        err.response["Error"]["Code"].should.equal("ValidationError")
    else:
        raise RuntimeError("Should have raised ResourceInUseException")


@mock_dynamodb2
def test_create_table_pay_per_request():
    client = boto3.client("dynamodb", region_name="us-east-1")
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )


@mock_dynamodb2
def test_create_table_error_pay_per_request_with_provisioned_param():
    client = boto3.client("dynamodb", region_name="us-east-1")

    try:
        client.create_table(
            TableName="test1",
            AttributeDefinitions=[
                {"AttributeName": "client", "AttributeType": "S"},
                {"AttributeName": "app", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "client", "KeyType": "HASH"},
                {"AttributeName": "app", "KeyType": "RANGE"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
            BillingMode="PAY_PER_REQUEST",
        )
    except ClientError as err:
        err.response["Error"]["Code"].should.equal("ValidationException")


@mock_dynamodb2
def test_duplicate_create():
    client = boto3.client("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )

    try:
        client.create_table(
            TableName="test1",
            AttributeDefinitions=[
                {"AttributeName": "client", "AttributeType": "S"},
                {"AttributeName": "app", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "client", "KeyType": "HASH"},
                {"AttributeName": "app", "KeyType": "RANGE"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
        )
    except ClientError as err:
        err.response["Error"]["Code"].should.equal("ResourceInUseException")
    else:
        raise RuntimeError("Should have raised ResourceInUseException")


@mock_dynamodb2
def test_delete_table():
    client = boto3.client("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )

    client.delete_table(TableName="test1")

    resp = client.list_tables()
    len(resp["TableNames"]).should.equal(0)

    try:
        client.delete_table(TableName="test1")
    except ClientError as err:
        err.response["Error"]["Code"].should.equal("ResourceNotFoundException")
    else:
        raise RuntimeError("Should have raised ResourceNotFoundException")


@mock_dynamodb2
def test_delete_item():
    client = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )
    client.put_item(
        TableName="test1", Item={"client": {"S": "client1"}, "app": {"S": "app1"}}
    )
    client.put_item(
        TableName="test1", Item={"client": {"S": "client1"}, "app": {"S": "app2"}}
    )

    table = dynamodb.Table("test1")
    response = table.scan()
    assert response["Count"] == 2

    # Test ReturnValues validation
    with pytest.raises(ClientError) as ex:
        table.delete_item(
            Key={"client": "client1", "app": "app1"}, ReturnValues="ALL_NEW"
        )
    err = ex.value.response["Error"]
    err["Code"].should.equal("ValidationException")
    err["Message"].should.equal("Return values set to invalid value")

    # Test deletion and returning old value
    response = table.delete_item(
        Key={"client": "client1", "app": "app1"}, ReturnValues="ALL_OLD"
    )
    response["Attributes"].should.contain("client")
    response["Attributes"].should.contain("app")

    response = table.scan()
    assert response["Count"] == 1

    # Test deletion returning nothing
    response = table.delete_item(Key={"client": "client1", "app": "app2"})
    len(response["Attributes"]).should.equal(0)

    response = table.scan()
    assert response["Count"] == 0


@mock_dynamodb2
def test_describe_limits():
    client = boto3.client("dynamodb", region_name="eu-central-1")
    resp = client.describe_limits()

    resp["AccountMaxReadCapacityUnits"].should.equal(20000)
    resp["AccountMaxWriteCapacityUnits"].should.equal(20000)
    resp["TableMaxWriteCapacityUnits"].should.equal(10000)
    resp["TableMaxReadCapacityUnits"].should.equal(10000)


@mock_dynamodb2
def test_set_ttl():
    client = boto3.client("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )

    client.update_time_to_live(
        TableName="test1",
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expire"},
    )

    resp = client.describe_time_to_live(TableName="test1")
    resp["TimeToLiveDescription"]["TimeToLiveStatus"].should.equal("ENABLED")
    resp["TimeToLiveDescription"]["AttributeName"].should.equal("expire")

    client.update_time_to_live(
        TableName="test1",
        TimeToLiveSpecification={"Enabled": False, "AttributeName": "expire"},
    )

    resp = client.describe_time_to_live(TableName="test1")
    resp["TimeToLiveDescription"]["TimeToLiveStatus"].should.equal("DISABLED")


@mock_dynamodb2
def test_describe_continuous_backups():
    # given
    client = boto3.client("dynamodb", region_name="us-east-1")
    table_name = client.create_table(
        TableName="test",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )["TableDescription"]["TableName"]

    # when
    response = client.describe_continuous_backups(TableName=table_name)

    # then
    response["ContinuousBackupsDescription"].should.equal(
        {
            "ContinuousBackupsStatus": "ENABLED",
            "PointInTimeRecoveryDescription": {"PointInTimeRecoveryStatus": "DISABLED"},
        }
    )


@mock_dynamodb2
def test_describe_continuous_backups_errors():
    # given
    client = boto3.client("dynamodb", region_name="us-east-1")

    # when
    with pytest.raises(Exception) as e:
        client.describe_continuous_backups(TableName="not-existing-table")

    # then
    ex = e.value
    ex.operation_name.should.equal("DescribeContinuousBackups")
    ex.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.response["Error"]["Code"].should.contain("TableNotFoundException")
    ex.response["Error"]["Message"].should.equal("Table not found: not-existing-table")


@mock_dynamodb2
def test_update_continuous_backups():
    # given
    client = boto3.client("dynamodb", region_name="us-east-1")
    table_name = client.create_table(
        TableName="test",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )["TableDescription"]["TableName"]

    # when
    response = client.update_continuous_backups(
        TableName=table_name,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
    )

    # then
    response["ContinuousBackupsDescription"]["ContinuousBackupsStatus"].should.equal(
        "ENABLED"
    )
    point_in_time = response["ContinuousBackupsDescription"][
        "PointInTimeRecoveryDescription"
    ]
    earliest_datetime = point_in_time["EarliestRestorableDateTime"]
    earliest_datetime.should.be.a(datetime)
    latest_datetime = point_in_time["LatestRestorableDateTime"]
    latest_datetime.should.be.a(datetime)
    point_in_time["PointInTimeRecoveryStatus"].should.equal("ENABLED")

    # when
    # a second update should not change anything
    response = client.update_continuous_backups(
        TableName=table_name,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
    )

    # then
    response["ContinuousBackupsDescription"]["ContinuousBackupsStatus"].should.equal(
        "ENABLED"
    )
    point_in_time = response["ContinuousBackupsDescription"][
        "PointInTimeRecoveryDescription"
    ]
    point_in_time["EarliestRestorableDateTime"].should.equal(earliest_datetime)
    point_in_time["LatestRestorableDateTime"].should.equal(latest_datetime)
    point_in_time["PointInTimeRecoveryStatus"].should.equal("ENABLED")

    # when
    response = client.update_continuous_backups(
        TableName=table_name,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": False},
    )

    # then
    response["ContinuousBackupsDescription"].should.equal(
        {
            "ContinuousBackupsStatus": "ENABLED",
            "PointInTimeRecoveryDescription": {"PointInTimeRecoveryStatus": "DISABLED"},
        }
    )


@mock_dynamodb2
def test_update_continuous_backups_errors():
    # given
    client = boto3.client("dynamodb", region_name="us-east-1")

    # when
    with pytest.raises(Exception) as e:
        client.update_continuous_backups(
            TableName="not-existing-table",
            PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
        )

    # then
    ex = e.value
    ex.operation_name.should.equal("UpdateContinuousBackups")
    ex.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.response["Error"]["Code"].should.contain("TableNotFoundException")
    ex.response["Error"]["Message"].should.equal("Table not found: not-existing-table")


# https://github.com/spulec/moto/issues/1043
@mock_dynamodb2
def test_query_missing_expr_names():
    client = boto3.client("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    client.create_table(
        TableName="test1",
        AttributeDefinitions=[
            {"AttributeName": "client", "AttributeType": "S"},
            {"AttributeName": "app", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "client", "KeyType": "HASH"},
            {"AttributeName": "app", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )
    client.put_item(
        TableName="test1", Item={"client": {"S": "test1"}, "app": {"S": "test1"}}
    )
    client.put_item(
        TableName="test1", Item={"client": {"S": "test2"}, "app": {"S": "test2"}}
    )

    resp = client.query(
        TableName="test1",
        KeyConditionExpression="client=:client",
        ExpressionAttributeValues={":client": {"S": "test1"}},
    )

    resp["Count"].should.equal(1)
    resp["Items"][0]["client"]["S"].should.equal("test1")

    resp = client.query(
        TableName="test1",
        KeyConditionExpression=":name=test2",
        ExpressionAttributeNames={":name": "client"},
    )

    resp["Count"].should.equal(1)
    resp["Items"][0]["client"]["S"].should.equal("test2")


# https://github.com/spulec/moto/issues/2328
@mock_dynamodb2
def test_update_item_with_list():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    dynamodb.create_table(
        TableName="Table",
        KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    table = dynamodb.Table("Table")
    table.update_item(
        Key={"key": "the-key"},
        AttributeUpdates={"list": {"Value": [1, 2], "Action": "PUT"}},
    )

    resp = table.get_item(Key={"key": "the-key"})
    resp["Item"].should.equal({"key": "the-key", "list": [1, 2]})


# https://github.com/spulec/moto/issues/2328
@mock_dynamodb2
def test_update_item_with_no_action_passed_with_list():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    dynamodb.create_table(
        TableName="Table",
        KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    table = dynamodb.Table("Table")
    table.update_item(
        Key={"key": "the-key"},
        # Do not pass 'Action' key, in order to check that the
        # parameter's default value will be used.
        AttributeUpdates={"list": {"Value": [1, 2]}},
    )

    resp = table.get_item(Key={"key": "the-key"})
    resp["Item"].should.equal({"key": "the-key", "list": [1, 2]})


# https://github.com/spulec/moto/issues/1937
@mock_dynamodb2
def test_update_return_attributes():
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")

    dynamodb.create_table(
        TableName="moto-test",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )

    def update(col, to, rv):
        return dynamodb.update_item(
            TableName="moto-test",
            Key={"id": {"S": "foo"}},
            AttributeUpdates={col: {"Value": {"S": to}, "Action": "PUT"}},
            ReturnValues=rv,
        )

    r = update("col1", "val1", "ALL_NEW")
    assert r["Attributes"] == {"id": {"S": "foo"}, "col1": {"S": "val1"}}

    r = update("col1", "val2", "ALL_OLD")
    assert r["Attributes"] == {"id": {"S": "foo"}, "col1": {"S": "val1"}}

    r = update("col2", "val3", "UPDATED_NEW")
    assert r["Attributes"] == {"col2": {"S": "val3"}}

    r = update("col2", "val4", "UPDATED_OLD")
    assert r["Attributes"] == {"col2": {"S": "val3"}}

    r = update("col1", "val5", "NONE")
    assert r["Attributes"] == {}

    with pytest.raises(ClientError) as ex:
        update("col1", "val6", "WRONG")
    err = ex.value.response["Error"]
    err["Code"].should.equal("ValidationException")
    err["Message"].should.equal("Return values set to invalid value")


# https://github.com/spulec/moto/issues/3448
@mock_dynamodb2
def test_update_return_updated_new_attributes_when_same():
    dynamo_client = boto3.resource("dynamodb", region_name="us-east-1")
    dynamo_client.create_table(
        TableName="moto-test",
        KeySchema=[{"AttributeName": "HashKey1", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "HashKey1", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )

    dynamodb_table = dynamo_client.Table("moto-test")
    dynamodb_table.put_item(
        Item={"HashKey1": "HashKeyValue1", "listValuedAttribute1": ["a", "b"]}
    )

    def update(col, to, rv):
        return dynamodb_table.update_item(
            TableName="moto-test",
            Key={"HashKey1": "HashKeyValue1"},
            UpdateExpression="SET listValuedAttribute1=:" + col,
            ExpressionAttributeValues={":" + col: to},
            ReturnValues=rv,
        )

    r = update("a", ["a", "c"], "UPDATED_NEW")
    assert r["Attributes"] == {"listValuedAttribute1": ["a", "c"]}

    r = update("a", {"a", "c"}, "UPDATED_NEW")
    assert r["Attributes"] == {"listValuedAttribute1": {"a", "c"}}

    r = update("a", {1, 2}, "UPDATED_NEW")
    assert r["Attributes"] == {"listValuedAttribute1": {1, 2}}

    with pytest.raises(ClientError) as ex:
        update("a", ["a", "c"], "WRONG")
    err = ex.value.response["Error"]
    err["Code"].should.equal("ValidationException")
    err["Message"].should.equal("Return values set to invalid value")


@mock_dynamodb2
def test_put_return_attributes():
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")

    dynamodb.create_table(
        TableName="moto-test",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )

    r = dynamodb.put_item(
        TableName="moto-test",
        Item={"id": {"S": "foo"}, "col1": {"S": "val1"}},
        ReturnValues="NONE",
    )
    assert "Attributes" not in r

    r = dynamodb.put_item(
        TableName="moto-test",
        Item={"id": {"S": "foo"}, "col1": {"S": "val2"}},
        ReturnValues="ALL_OLD",
    )
    assert r["Attributes"] == {"id": {"S": "foo"}, "col1": {"S": "val1"}}

    with pytest.raises(ClientError) as ex:
        dynamodb.put_item(
            TableName="moto-test",
            Item={"id": {"S": "foo"}, "col1": {"S": "val3"}},
            ReturnValues="ALL_NEW",
        )
    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Message"].should.equal(
        "Return values set to invalid value"
    )


@mock_dynamodb2
def test_query_global_secondary_index_when_created_via_update_table_resource():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create the DynamoDB table.
    dynamodb.create_table(
        TableName="users",
        KeySchema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "user_id", "AttributeType": "N"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table = dynamodb.Table("users")
    table.update(
        AttributeDefinitions=[{"AttributeName": "forum_name", "AttributeType": "S"}],
        GlobalSecondaryIndexUpdates=[
            {
                "Create": {
                    "IndexName": "forum_name_index",
                    "KeySchema": [{"AttributeName": "forum_name", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5,
                    },
                }
            }
        ],
    )

    next_user_id = 1
    for my_forum_name in ["cats", "dogs"]:
        for my_subject in [
            "my pet is the cutest",
            "wow look at what my pet did",
            "don't you love my pet?",
        ]:
            table.put_item(
                Item={
                    "user_id": next_user_id,
                    "forum_name": my_forum_name,
                    "subject": my_subject,
                }
            )
            next_user_id += 1

    # get all the cat users
    forum_only_query_response = table.query(
        IndexName="forum_name_index",
        Select="ALL_ATTRIBUTES",
        KeyConditionExpression=Key("forum_name").eq("cats"),
    )
    forum_only_items = forum_only_query_response["Items"]
    assert len(forum_only_items) == 3
    for item in forum_only_items:
        assert item["forum_name"] == "cats"

    # query all cat users with a particular subject
    forum_and_subject_query_results = table.query(
        IndexName="forum_name_index",
        Select="ALL_ATTRIBUTES",
        KeyConditionExpression=Key("forum_name").eq("cats"),
        FilterExpression=Attr("subject").eq("my pet is the cutest"),
    )
    forum_and_subject_items = forum_and_subject_query_results["Items"]
    assert len(forum_and_subject_items) == 1
    assert forum_and_subject_items[0] == {
        "user_id": Decimal("1"),
        "forum_name": "cats",
        "subject": "my pet is the cutest",
    }


@mock_dynamodb2
def test_dynamodb_streams_1():
    conn = boto3.client("dynamodb", region_name="us-east-1")

    resp = conn.create_table(
        TableName="test-streams",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
    )

    assert "StreamSpecification" in resp["TableDescription"]
    assert resp["TableDescription"]["StreamSpecification"] == {
        "StreamEnabled": True,
        "StreamViewType": "NEW_AND_OLD_IMAGES",
    }
    assert "LatestStreamLabel" in resp["TableDescription"]
    assert "LatestStreamArn" in resp["TableDescription"]

    resp = conn.delete_table(TableName="test-streams")

    assert "StreamSpecification" in resp["TableDescription"]


@mock_dynamodb2
def test_dynamodb_streams_2():
    conn = boto3.client("dynamodb", region_name="us-east-1")

    resp = conn.create_table(
        TableName="test-stream-update",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )

    assert "StreamSpecification" not in resp["TableDescription"]

    resp = conn.update_table(
        TableName="test-stream-update",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
    )

    assert "StreamSpecification" in resp["TableDescription"]
    assert resp["TableDescription"]["StreamSpecification"] == {
        "StreamEnabled": True,
        "StreamViewType": "NEW_IMAGE",
    }
    assert "LatestStreamLabel" in resp["TableDescription"]
    assert "LatestStreamArn" in resp["TableDescription"]


@mock_dynamodb2
def test_query_gsi_with_range_key():
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "gsi_hash_key", "AttributeType": "S"},
            {"AttributeName": "gsi_range_key", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        GlobalSecondaryIndexes=[
            {
                "IndexName": "test_gsi",
                "KeySchema": [
                    {"AttributeName": "gsi_hash_key", "KeyType": "HASH"},
                    {"AttributeName": "gsi_range_key", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1,
                },
            }
        ],
    )

    dynamodb.put_item(
        TableName="test",
        Item={
            "id": {"S": "test1"},
            "gsi_hash_key": {"S": "key1"},
            "gsi_range_key": {"S": "range1"},
        },
    )
    dynamodb.put_item(
        TableName="test", Item={"id": {"S": "test2"}, "gsi_hash_key": {"S": "key1"}}
    )

    res = dynamodb.query(
        TableName="test",
        IndexName="test_gsi",
        KeyConditionExpression="gsi_hash_key = :gsi_hash_key and gsi_range_key = :gsi_range_key",
        ExpressionAttributeValues={
            ":gsi_hash_key": {"S": "key1"},
            ":gsi_range_key": {"S": "range1"},
        },
    )
    res.should.have.key("Count").equal(1)
    res.should.have.key("Items")
    res["Items"][0].should.equal(
        {
            "id": {"S": "test1"},
            "gsi_hash_key": {"S": "key1"},
            "gsi_range_key": {"S": "range1"},
        }
    )


@mock_dynamodb2
def test_scan_by_non_exists_index():
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")

    dynamodb.create_table(
        TableName="test",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "gsi_col", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        GlobalSecondaryIndexes=[
            {
                "IndexName": "test_gsi",
                "KeySchema": [{"AttributeName": "gsi_col", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1,
                },
            }
        ],
    )

    with pytest.raises(ClientError) as ex:
        dynamodb.scan(TableName="test", IndexName="non_exists_index")

    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Message"].should.equal(
        "The table does not have the specified index: non_exists_index"
    )


@mock_dynamodb2
def test_query_by_non_exists_index():
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")

    dynamodb.create_table(
        TableName="test",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "gsi_col", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        GlobalSecondaryIndexes=[
            {
                "IndexName": "test_gsi",
                "KeySchema": [{"AttributeName": "gsi_col", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1,
                },
            }
        ],
    )

    with pytest.raises(ClientError) as ex:
        dynamodb.query(
            TableName="test",
            IndexName="non_exists_index",
            KeyConditionExpression="CarModel=M",
        )

    ex.value.response["Error"]["Code"].should.equal("ResourceNotFoundException")
    ex.value.response["Error"]["Message"].should.equal(
        "Invalid index: non_exists_index for table: test. Available indexes are: test_gsi"
    )


@mock_dynamodb2
def test_index_with_unknown_attributes_should_fail():
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")

    expected_exception = (
        "Some index key attributes are not defined in AttributeDefinitions."
    )

    with pytest.raises(ClientError) as ex:
        dynamodb.create_table(
            AttributeDefinitions=[
                {"AttributeName": "customer_nr", "AttributeType": "S"},
                {"AttributeName": "last_name", "AttributeType": "S"},
            ],
            TableName="table_with_missing_attribute_definitions",
            KeySchema=[
                {"AttributeName": "customer_nr", "KeyType": "HASH"},
                {"AttributeName": "last_name", "KeyType": "RANGE"},
            ],
            LocalSecondaryIndexes=[
                {
                    "IndexName": "indexthataddsanadditionalattribute",
                    "KeySchema": [
                        {"AttributeName": "customer_nr", "KeyType": "HASH"},
                        {"AttributeName": "postcode", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )

    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["Error"]["Message"].should.contain(expected_exception)


@mock_dynamodb2
def test_sorted_query_with_numerical_sort_key():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="CarCollection",
        KeySchema=[
            {"AttributeName": "CarModel", "KeyType": "HASH"},
            {"AttributeName": "CarPrice", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "CarModel", "AttributeType": "S"},
            {"AttributeName": "CarPrice", "AttributeType": "N"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )

    def create_item(price):
        return {"CarModel": "M", "CarPrice": price}

    table = dynamodb.Table("CarCollection")
    items = list(map(create_item, [2, 1, 10, 3]))
    for item in items:
        table.put_item(Item=item)

    response = table.query(KeyConditionExpression=Key("CarModel").eq("M"))

    response_items = response["Items"]
    assert len(items) == len(response_items)
    assert all(isinstance(item["CarPrice"], Decimal) for item in response_items)
    response_prices = [item["CarPrice"] for item in response_items]
    expected_prices = [Decimal(item["CarPrice"]) for item in items]
    expected_prices.sort()
    assert (
        expected_prices == response_prices
    ), "result items are not sorted by numerical value"


# https://github.com/spulec/moto/issues/1874
@mock_dynamodb2
def test_item_size_is_under_400KB():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    client = boto3.client("dynamodb", region_name="us-east-1")

    dynamodb.create_table(
        TableName="moto-test",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    table = dynamodb.Table("moto-test")

    large_item = "x" * 410 * 1000
    assert_failure_due_to_item_size(
        func=client.put_item,
        TableName="moto-test",
        Item={"id": {"S": "foo"}, "cont": {"S": large_item}},
    )
    assert_failure_due_to_item_size(
        func=table.put_item, Item={"id": "bar", "cont": large_item}
    )
    assert_failure_due_to_item_size_to_update(
        func=client.update_item,
        TableName="moto-test",
        Key={"id": {"S": "foo2"}},
        UpdateExpression="set cont=:Item",
        ExpressionAttributeValues={":Item": {"S": large_item}},
    )
    # Assert op fails when updating a nested item
    assert_failure_due_to_item_size(
        func=table.put_item, Item={"id": "bar", "itemlist": [{"cont": large_item}]}
    )
    assert_failure_due_to_item_size(
        func=client.put_item,
        TableName="moto-test",
        Item={
            "id": {"S": "foo"},
            "itemlist": {"L": [{"M": {"item1": {"S": large_item}}}]},
        },
    )


def assert_failure_due_to_item_size(func, **kwargs):
    with pytest.raises(ClientError) as ex:
        func(**kwargs)
    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["Error"]["Message"].should.equal(
        "Item size has exceeded the maximum allowed size"
    )


def assert_failure_due_to_item_size_to_update(func, **kwargs):
    with pytest.raises(ClientError) as ex:
        func(**kwargs)
    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["Error"]["Message"].should.equal(
        "Item size to update has exceeded the maximum allowed size"
    )


@mock_dynamodb2
# https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression
def test_hash_key_cannot_use_begins_with_operations():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.create_table(
        TableName="test-table",
        KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )

    items = [
        {"key": "prefix-$LATEST", "value": "$LATEST"},
        {"key": "prefix-DEV", "value": "DEV"},
        {"key": "prefix-PROD", "value": "PROD"},
    ]

    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

    table = dynamodb.Table("test-table")
    with pytest.raises(ClientError) as ex:
        table.query(KeyConditionExpression=Key("key").begins_with("prefix-"))
    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["Error"]["Message"].should.equal(
        "Query key condition not supported"
    )


@mock_dynamodb2
def test_update_supports_complex_expression_attribute_values():
    client = boto3.client("dynamodb", region_name="us-east-1")

    client.create_table(
        AttributeDefinitions=[{"AttributeName": "SHA256", "AttributeType": "S"}],
        TableName="TestTable",
        KeySchema=[{"AttributeName": "SHA256", "KeyType": "HASH"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    client.update_item(
        TableName="TestTable",
        Key={"SHA256": {"S": "sha-of-file"}},
        UpdateExpression=(
            "SET MD5 = :md5," "MyStringSet = :string_set," "MyMap = :map"
        ),
        ExpressionAttributeValues={
            ":md5": {"S": "md5-of-file"},
            ":string_set": {"SS": ["string1", "string2"]},
            ":map": {"M": {"EntryKey": {"SS": ["thing1", "thing2"]}}},
        },
    )
    result = client.get_item(
        TableName="TestTable", Key={"SHA256": {"S": "sha-of-file"}}
    )["Item"]
    result.should.equal(
        {
            "MyStringSet": {"SS": ["string1", "string2"]},
            "MyMap": {"M": {"EntryKey": {"SS": ["thing1", "thing2"]}}},
            "SHA256": {"S": "sha-of-file"},
            "MD5": {"S": "md5-of-file"},
        }
    )


@mock_dynamodb2
def test_query_catches_when_no_filters():
    dynamo = boto3.resource("dynamodb", region_name="eu-central-1")
    dynamo.create_table(
        AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
        TableName="origin-rbu-dev",
        KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    table = dynamo.Table("origin-rbu-dev")

    with pytest.raises(ClientError) as ex:
        table.query(TableName="original-rbu-dev")

    ex.value.response["Error"]["Code"].should.equal("ValidationException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Message"].should.equal(
        "Either KeyConditions or QueryFilter should be present"
    )


@mock_dynamodb2
def test_gsi_verify_negative_number_order():
    table_schema = {
        "KeySchema": [{"AttributeName": "partitionKey", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "GSI-K1",
                "KeySchema": [
                    {"AttributeName": "gsiK1PartitionKey", "KeyType": "HASH"},
                    {"AttributeName": "gsiK1SortKey", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY",},
            }
        ],
        "AttributeDefinitions": [
            {"AttributeName": "partitionKey", "AttributeType": "S"},
            {"AttributeName": "gsiK1PartitionKey", "AttributeType": "S"},
            {"AttributeName": "gsiK1SortKey", "AttributeType": "N"},
        ],
    }

    item1 = {
        "partitionKey": "pk-1",
        "gsiK1PartitionKey": "gsi-k1",
        "gsiK1SortKey": Decimal("-0.6"),
    }

    item2 = {
        "partitionKey": "pk-2",
        "gsiK1PartitionKey": "gsi-k1",
        "gsiK1SortKey": Decimal("-0.7"),
    }

    item3 = {
        "partitionKey": "pk-3",
        "gsiK1PartitionKey": "gsi-k1",
        "gsiK1SortKey": Decimal("0.7"),
    }

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    table = dynamodb.Table("test-table")
    table.put_item(Item=item3)
    table.put_item(Item=item1)
    table.put_item(Item=item2)

    resp = table.query(
        KeyConditionExpression=Key("gsiK1PartitionKey").eq("gsi-k1"),
        IndexName="GSI-K1",
    )
    # Items should be ordered with the lowest number first
    [float(item["gsiK1SortKey"]) for item in resp["Items"]].should.equal(
        [-0.7, -0.6, 0.7]
    )


@mock_dynamodb2
def test_dynamodb_max_1mb_limit():
    ddb = boto3.resource("dynamodb", region_name="eu-west-1")

    table_name = "populated-mock-table"
    table = ddb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "partition_key", "KeyType": "HASH"},
            {"AttributeName": "sort_key", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "partition_key", "AttributeType": "S"},
            {"AttributeName": "sort_key", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    # Populate the table
    items = [
        {
            "partition_key": "partition_key_val",  # size=30
            "sort_key": "sort_key_value____" + str(i),  # size=30
        }
        for i in range(10000, 29999)
    ]
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

    response = table.query(
        KeyConditionExpression=Key("partition_key").eq("partition_key_val")
    )
    # We shouldn't get everything back - the total result set is well over 1MB
    len(items).should.be.greater_than(response["Count"])
    response["LastEvaluatedKey"].shouldnt.be(None)


@mock_dynamodb2
def test_list_tables_exclusive_start_table_name_empty():
    client = boto3.client("dynamodb", region_name="us-east-1")

    resp = client.list_tables(Limit=1, ExclusiveStartTableName="whatever")

    len(resp["TableNames"]).should.equal(0)


@mock_dynamodb2
def test_gsi_projection_type_keys_only():
    table_schema = {
        "KeySchema": [{"AttributeName": "partitionKey", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "GSI-K1",
                "KeySchema": [
                    {"AttributeName": "gsiK1PartitionKey", "KeyType": "HASH"},
                    {"AttributeName": "gsiK1SortKey", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY",},
            }
        ],
        "AttributeDefinitions": [
            {"AttributeName": "partitionKey", "AttributeType": "S"},
            {"AttributeName": "gsiK1PartitionKey", "AttributeType": "S"},
            {"AttributeName": "gsiK1SortKey", "AttributeType": "S"},
        ],
    }

    item = {
        "partitionKey": "pk-1",
        "gsiK1PartitionKey": "gsi-pk",
        "gsiK1SortKey": "gsi-sk",
        "someAttribute": "lore ipsum",
    }

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    table = dynamodb.Table("test-table")
    table.put_item(Item=item)

    items = table.query(
        KeyConditionExpression=Key("gsiK1PartitionKey").eq("gsi-pk"),
        IndexName="GSI-K1",
    )["Items"]
    items.should.have.length_of(1)
    # Item should only include GSI Keys and Table Keys, as per the ProjectionType
    items[0].should.equal(
        {
            "gsiK1PartitionKey": "gsi-pk",
            "gsiK1SortKey": "gsi-sk",
            "partitionKey": "pk-1",
        }
    )


@mock_dynamodb2
def test_gsi_projection_type_include():
    table_schema = {
        "KeySchema": [{"AttributeName": "partitionKey", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "GSI-INC",
                "KeySchema": [
                    {"AttributeName": "gsiK1PartitionKey", "KeyType": "HASH"},
                    {"AttributeName": "gsiK1SortKey", "KeyType": "RANGE"},
                ],
                "Projection": {
                    "ProjectionType": "INCLUDE",
                    "NonKeyAttributes": ["projectedAttribute"],
                },
            }
        ],
        "AttributeDefinitions": [
            {"AttributeName": "partitionKey", "AttributeType": "S"},
            {"AttributeName": "gsiK1PartitionKey", "AttributeType": "S"},
            {"AttributeName": "gsiK1SortKey", "AttributeType": "S"},
        ],
    }

    item = {
        "partitionKey": "pk-1",
        "gsiK1PartitionKey": "gsi-pk",
        "gsiK1SortKey": "gsi-sk",
        "projectedAttribute": "lore ipsum",
        "nonProjectedAttribute": "dolor sit amet",
    }

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    table = dynamodb.Table("test-table")
    table.put_item(Item=item)

    items = table.query(
        KeyConditionExpression=Key("gsiK1PartitionKey").eq("gsi-pk"),
        IndexName="GSI-INC",
    )["Items"]
    items.should.have.length_of(1)
    # Item should only include keys and additionally projected attributes only
    items[0].should.equal(
        {
            "gsiK1PartitionKey": "gsi-pk",
            "gsiK1SortKey": "gsi-sk",
            "partitionKey": "pk-1",
            "projectedAttribute": "lore ipsum",
        }
    )


@mock_dynamodb2
def test_lsi_projection_type_keys_only():
    table_schema = {
        "KeySchema": [
            {"AttributeName": "partitionKey", "KeyType": "HASH"},
            {"AttributeName": "sortKey", "KeyType": "RANGE"},
        ],
        "LocalSecondaryIndexes": [
            {
                "IndexName": "LSI",
                "KeySchema": [
                    {"AttributeName": "partitionKey", "KeyType": "HASH"},
                    {"AttributeName": "lsiK1SortKey", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY",},
            }
        ],
        "AttributeDefinitions": [
            {"AttributeName": "partitionKey", "AttributeType": "S"},
            {"AttributeName": "sortKey", "AttributeType": "S"},
            {"AttributeName": "lsiK1SortKey", "AttributeType": "S"},
        ],
    }

    item = {
        "partitionKey": "pk-1",
        "sortKey": "sk-1",
        "lsiK1SortKey": "lsi-sk",
        "someAttribute": "lore ipsum",
    }

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table", BillingMode="PAY_PER_REQUEST", **table_schema
    )
    table = dynamodb.Table("test-table")
    table.put_item(Item=item)

    items = table.query(
        KeyConditionExpression=Key("partitionKey").eq("pk-1"), IndexName="LSI",
    )["Items"]
    items.should.have.length_of(1)
    # Item should only include GSI Keys and Table Keys, as per the ProjectionType
    items[0].should.equal(
        {"partitionKey": "pk-1", "sortKey": "sk-1", "lsiK1SortKey": "lsi-sk"}
    )


@mock_dynamodb2
@pytest.mark.parametrize(
    "attr_name",
    ["orders", "#placeholder"],
    ids=["use attribute name", "use expression attribute name"],
)
def test_set_attribute_is_dropped_if_empty_after_update_expression(attr_name):
    table_name, item_key, set_item = "test-table", "test-id", "test-data"
    expression_attribute_names = {"#placeholder": "orders"}
    client = boto3.client("dynamodb", region_name="us-east-1")
    client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "customer", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "customer", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    client.update_item(
        TableName=table_name,
        Key={"customer": {"S": item_key}},
        UpdateExpression="ADD {} :order".format(attr_name),
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues={":order": {"SS": [set_item]}},
    )
    resp = client.scan(TableName=table_name, ProjectionExpression="customer, orders")
    item = resp["Items"][0]
    item.should.have.key("customer")
    item.should.have.key("orders")

    client.update_item(
        TableName=table_name,
        Key={"customer": {"S": item_key}},
        UpdateExpression="DELETE {} :order".format(attr_name),
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues={":order": {"SS": [set_item]}},
    )
    resp = client.scan(TableName=table_name, ProjectionExpression="customer, orders")
    item = resp["Items"][0]
    item.should.have.key("customer")
    item.should_not.have.key("orders")


@mock_dynamodb2
def test_dynamodb_update_item_fails_on_string_sets():
    dynamodb = boto3.resource("dynamodb", region_name="eu-west-1")
    client = boto3.client("dynamodb", region_name="eu-west-1")

    table = dynamodb.create_table(
        TableName="test",
        KeySchema=[{"AttributeName": "record_id", "KeyType": "HASH"},],
        AttributeDefinitions=[{"AttributeName": "record_id", "AttributeType": "S"},],
        BillingMode="PAY_PER_REQUEST",
    )
    table.meta.client.get_waiter("table_exists").wait(TableName="test")
    attribute = {"test_field": {"Value": {"SS": ["test1", "test2"],}, "Action": "PUT",}}

    client.update_item(
        TableName="test",
        Key={"record_id": {"S": "testrecord"}},
        AttributeUpdates=attribute,
    )


@mock_dynamodb2
def test_update_item_add_to_list_using_legacy_attribute_updates():
    resource = boto3.resource("dynamodb", region_name="us-west-2")
    resource.create_table(
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        TableName="TestTable",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table = resource.Table("TestTable")
    table.wait_until_exists()
    table.put_item(Item={"id": "list_add", "attr": ["a", "b", "c"]},)

    table.update_item(
        TableName="TestTable",
        Key={"id": "list_add"},
        AttributeUpdates={"attr": {"Action": "ADD", "Value": ["d", "e"]}},
    )

    resp = table.get_item(Key={"id": "list_add"})
    resp["Item"]["attr"].should.equal(["a", "b", "c", "d", "e"])


@mock_dynamodb2
def test_get_item_for_non_existent_table_raises_error():
    client = boto3.client("dynamodb", "us-east-1")
    with pytest.raises(ClientError) as ex:
        client.get_item(TableName="non-existent", Key={"site-id": {"S": "foo"}})
    ex.value.response["Error"]["Code"].should.equal("ResourceNotFoundException")
    ex.value.response["Error"]["Message"].should.equal("Requested resource not found")


@mock_dynamodb2
def test_error_when_providing_expression_and_nonexpression_params():
    client = boto3.client("dynamodb", "eu-central-1")
    table_name = "testtable"
    client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pkey", "KeyType": "HASH"},],
        AttributeDefinitions=[{"AttributeName": "pkey", "AttributeType": "S"},],
        BillingMode="PAY_PER_REQUEST",
    )

    with pytest.raises(ClientError) as ex:
        client.update_item(
            TableName=table_name,
            Key={"pkey": {"S": "testrecord"}},
            AttributeUpdates={
                "test_field": {"Value": {"SS": ["test1", "test2"],}, "Action": "PUT"}
            },
            UpdateExpression="DELETE orders :order",
            ExpressionAttributeValues={":order": {"SS": ["item"]}},
        )
    err = ex.value.response["Error"]
    err["Code"].should.equal("ValidationException")
    err["Message"].should.equal(
        "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributeUpdates} Expression parameters: {UpdateExpression}"
    )


@mock_dynamodb2
def test_attribute_item_delete():
    name = "TestTable"
    conn = boto3.client("dynamodb", region_name="eu-west-1")
    conn.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "name", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "name", "KeyType": "HASH"}],
    )

    item_name = "foo"
    conn.put_item(
        TableName=name, Item={"name": {"S": item_name}, "extra": {"S": "bar"}}
    )

    conn.update_item(
        TableName=name,
        Key={"name": {"S": item_name}},
        AttributeUpdates={"extra": {"Action": "DELETE"}},
    )
    items = conn.scan(TableName=name)["Items"]
    items.should.equal([{"name": {"S": "foo"}}])


@mock_dynamodb2
def test_gsi_key_can_be_updated():
    name = "TestTable"
    conn = boto3.client("dynamodb", region_name="eu-west-2")
    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "main_key", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "main_key", "AttributeType": "S"},
            {"AttributeName": "index_key", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        GlobalSecondaryIndexes=[
            {
                "IndexName": "test_index",
                "KeySchema": [{"AttributeName": "index_key", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL",},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1,
                },
            }
        ],
    )

    conn.put_item(
        TableName=name,
        Item={
            "main_key": {"S": "testkey1"},
            "extra_data": {"S": "testdata"},
            "index_key": {"S": "indexkey1"},
        },
    )

    conn.update_item(
        TableName=name,
        Key={"main_key": {"S": "testkey1"}},
        UpdateExpression="set index_key=:new_index_key",
        ExpressionAttributeValues={":new_index_key": {"S": "new_value"}},
    )

    item = conn.scan(TableName=name)["Items"][0]
    item["index_key"].should.equal({"S": "new_value"})
    item["main_key"].should.equal({"S": "testkey1"})


@mock_dynamodb2
def test_gsi_key_cannot_be_empty():
    name = "TestTable"
    conn = boto3.client("dynamodb", region_name="eu-west-2")
    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "main_key", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "main_key", "AttributeType": "S"},
            {"AttributeName": "index_key", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        GlobalSecondaryIndexes=[
            {
                "IndexName": "test_index",
                "KeySchema": [{"AttributeName": "index_key", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL",},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1,
                },
            }
        ],
    )

    conn.put_item(
        TableName=name,
        Item={
            "main_key": {"S": "testkey1"},
            "extra_data": {"S": "testdata"},
            "index_key": {"S": "indexkey1"},
        },
    )

    with pytest.raises(ClientError) as ex:
        conn.update_item(
            TableName=name,
            Key={"main_key": {"S": "testkey1"}},
            UpdateExpression="set index_key=:new_index_key",
            ExpressionAttributeValues={":new_index_key": {"S": ""}},
        )
    err = ex.value.response["Error"]
    err["Code"].should.equal("ValidationException")
    err["Message"].should.equal(
        "One or more parameter values are not valid. The update expression attempted to update a secondary index key to a value that is not supported. The AttributeValue for a key attribute cannot contain an empty string value."
    )


@mock_dynamodb2
def test_create_backup_for_non_existent_table_raises_error():
    client = boto3.client("dynamodb", "us-east-1")
    with pytest.raises(ClientError) as ex:
        client.create_backup(TableName="non-existent", BackupName="backup")
    error = ex.value.response["Error"]
    error["Code"].should.equal("TableNotFoundException")
    error["Message"].should.equal("Table not found: non-existent")


@mock_dynamodb2
def test_create_backup():
    client = boto3.client("dynamodb", "us-east-1")
    table_name = "test-table"
    client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    backup_name = "backup-test-table"
    resp = client.create_backup(TableName=table_name, BackupName=backup_name)
    details = resp.get("BackupDetails")
    details.should.have.key("BackupArn").should.contain(table_name)
    details.should.have.key("BackupName").should.equal(backup_name)
    details.should.have.key("BackupSizeBytes").should.be.a(int)
    details.should.have.key("BackupStatus")
    details.should.have.key("BackupType").should.equal("USER")
    details.should.have.key("BackupCreationDateTime").should.be.a(datetime)


@mock_dynamodb2
def test_create_multiple_backups_with_same_name():
    client = boto3.client("dynamodb", "us-east-1")
    table_name = "test-table"
    client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    backup_name = "backup-test-table"
    backup_arns = []
    for _ in range(4):
        backup = client.create_backup(TableName=table_name, BackupName=backup_name).get(
            "BackupDetails"
        )
        backup["BackupName"].should.equal(backup_name)
        backup_arns.should_not.contain(backup["BackupArn"])
        backup_arns.append(backup["BackupArn"])


@mock_dynamodb2
def test_describe_backup_for_non_existent_backup_raises_error():
    client = boto3.client("dynamodb", "us-east-1")
    non_existent_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/table-name/backup/01623095754481-2cfcd6f9"
    with pytest.raises(ClientError) as ex:
        client.describe_backup(BackupArn=non_existent_arn)
    error = ex.value.response["Error"]
    error["Code"].should.equal("BackupNotFoundException")
    error["Message"].should.equal("Backup not found: {}".format(non_existent_arn))


@mock_dynamodb2
def test_describe_backup():
    client = boto3.client("dynamodb", "us-east-1")
    table_name = "test-table"
    table = client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    ).get("TableDescription")
    backup_name = "backup-test-table"
    backup_arn = (
        client.create_backup(TableName=table_name, BackupName=backup_name)
        .get("BackupDetails")
        .get("BackupArn")
    )
    resp = client.describe_backup(BackupArn=backup_arn)
    description = resp.get("BackupDescription")
    details = description.get("BackupDetails")
    details.should.have.key("BackupArn").should.contain(table_name)
    details.should.have.key("BackupName").should.equal(backup_name)
    details.should.have.key("BackupSizeBytes").should.be.a(int)
    details.should.have.key("BackupStatus")
    details.should.have.key("BackupType").should.equal("USER")
    details.should.have.key("BackupCreationDateTime").should.be.a(datetime)
    source = description.get("SourceTableDetails")
    source.should.have.key("TableName").should.equal(table_name)
    source.should.have.key("TableArn").should.equal(table["TableArn"])
    source.should.have.key("TableSizeBytes").should.be.a(int)
    source.should.have.key("KeySchema").should.equal(table["KeySchema"])
    source.should.have.key("TableCreationDateTime").should.equal(
        table["CreationDateTime"]
    )
    source.should.have.key("ProvisionedThroughput").should.be.a(dict)
    source.should.have.key("ItemCount").should.equal(table["ItemCount"])


@mock_dynamodb2
def test_list_backups_for_non_existent_table():
    client = boto3.client("dynamodb", "us-east-1")
    resp = client.list_backups(TableName="non-existent")
    resp["BackupSummaries"].should.have.length_of(0)


@mock_dynamodb2
def test_list_backups():
    client = boto3.client("dynamodb", "us-east-1")
    table_names = ["test-table-1", "test-table-2"]
    backup_names = ["backup-1", "backup-2"]
    for table_name in table_names:
        client.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        for backup_name in backup_names:
            client.create_backup(TableName=table_name, BackupName=backup_name)
    resp = client.list_backups(BackupType="USER")
    resp["BackupSummaries"].should.have.length_of(4)
    for table_name in table_names:
        resp = client.list_backups(TableName=table_name)
        resp["BackupSummaries"].should.have.length_of(2)
        for summary in resp["BackupSummaries"]:
            summary.should.have.key("TableName").should.equal(table_name)
            summary.should.have.key("TableArn").should.contain(table_name)
            summary.should.have.key("BackupName").should.be.within(backup_names)
            summary.should.have.key("BackupArn")
            summary.should.have.key("BackupCreationDateTime").should.be.a(datetime)
            summary.should.have.key("BackupStatus")
            summary.should.have.key("BackupType").should.be.within(["USER", "SYSTEM"])
            summary.should.have.key("BackupSizeBytes").should.be.a(int)


@mock_dynamodb2
def test_restore_table_from_non_existent_backup_raises_error():
    client = boto3.client("dynamodb", "us-east-1")
    non_existent_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/table-name/backup/01623095754481-2cfcd6f9"
    with pytest.raises(ClientError) as ex:
        client.restore_table_from_backup(
            TargetTableName="from-backup", BackupArn=non_existent_arn
        )
    error = ex.value.response["Error"]
    error["Code"].should.equal("BackupNotFoundException")
    error["Message"].should.equal("Backup not found: {}".format(non_existent_arn))


@mock_dynamodb2
def test_restore_table_from_backup_raises_error_when_table_already_exists():
    client = boto3.client("dynamodb", "us-east-1")
    table_name = "test-table"
    client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    resp = client.create_backup(TableName=table_name, BackupName="backup")
    backup = resp.get("BackupDetails")
    with pytest.raises(ClientError) as ex:
        client.restore_table_from_backup(
            TargetTableName=table_name, BackupArn=backup["BackupArn"]
        )
    error = ex.value.response["Error"]
    error["Code"].should.equal("TableAlreadyExistsException")
    error["Message"].should.equal("Table already exists: {}".format(table_name))


@mock_dynamodb2
def test_restore_table_from_backup():
    client = boto3.client("dynamodb", "us-east-1")
    table_name = "test-table"
    resp = client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table = resp.get("TableDescription")
    for i in range(5):
        client.put_item(TableName=table_name, Item={"id": {"S": "item %d" % i}})

    backup_arn = (
        client.create_backup(TableName=table_name, BackupName="backup")
        .get("BackupDetails")
        .get("BackupArn")
    )

    restored_table_name = "restored-from-backup"
    restored = client.restore_table_from_backup(
        TargetTableName=restored_table_name, BackupArn=backup_arn
    ).get("TableDescription")
    restored.should.have.key("AttributeDefinitions").should.equal(
        table["AttributeDefinitions"]
    )
    restored.should.have.key("TableName").should.equal(restored_table_name)
    restored.should.have.key("KeySchema").should.equal(table["KeySchema"])
    restored.should.have.key("TableStatus")
    restored.should.have.key("ItemCount").should.equal(5)
    restored.should.have.key("TableArn").should.contain(restored_table_name)
    restored.should.have.key("RestoreSummary").should.be.a(dict)
    summary = restored.get("RestoreSummary")
    summary.should.have.key("SourceBackupArn").should.equal(backup_arn)
    summary.should.have.key("SourceTableArn").should.equal(table["TableArn"])
    summary.should.have.key("RestoreDateTime").should.be.a(datetime)
    summary.should.have.key("RestoreInProgress").should.equal(False)


@mock_dynamodb2
def test_delete_non_existent_backup_raises_error():
    client = boto3.client("dynamodb", "us-east-1")
    non_existent_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/table-name/backup/01623095754481-2cfcd6f9"
    with pytest.raises(ClientError) as ex:
        client.delete_backup(BackupArn=non_existent_arn)
    error = ex.value.response["Error"]
    error["Code"].should.equal("BackupNotFoundException")
    error["Message"].should.equal("Backup not found: {}".format(non_existent_arn))


@mock_dynamodb2
def test_delete_backup():
    client = boto3.client("dynamodb", "us-east-1")
    table_name = "test-table-1"
    backup_names = ["backup-1", "backup-2"]
    client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    for backup_name in backup_names:
        client.create_backup(TableName=table_name, BackupName=backup_name)
    resp = client.list_backups(TableName=table_name, BackupType="USER")
    resp["BackupSummaries"].should.have.length_of(2)
    backup_to_delete = resp["BackupSummaries"][0]["BackupArn"]
    backup_deleted = client.delete_backup(BackupArn=backup_to_delete).get(
        "BackupDescription"
    )
    backup_deleted.should.have.key("SourceTableDetails")
    backup_deleted.should.have.key("BackupDetails")
    details = backup_deleted["BackupDetails"]
    details.should.have.key("BackupArn").should.equal(backup_to_delete)
    details.should.have.key("BackupName").should.be.within(backup_names)
    details.should.have.key("BackupStatus").should.equal("DELETED")
    resp = client.list_backups(TableName=table_name, BackupType="USER")
    resp["BackupSummaries"].should.have.length_of(1)


@mock_dynamodb2
def test_source_and_restored_table_items_are_not_linked():
    client = boto3.client("dynamodb", "us-east-1")

    def add_guids_to_table(table, num_items):
        guids = []
        for _ in range(num_items):
            guid = str(uuid.uuid4())
            client.put_item(TableName=table, Item={"id": {"S": guid}})
            guids.append(guid)
        return guids

    source_table_name = "source-table"
    client.create_table(
        TableName=source_table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    guids_original = add_guids_to_table(source_table_name, 5)

    backup_arn = (
        client.create_backup(TableName=source_table_name, BackupName="backup")
        .get("BackupDetails")
        .get("BackupArn")
    )
    guids_added_after_backup = add_guids_to_table(source_table_name, 5)

    restored_table_name = "restored-from-backup"
    client.restore_table_from_backup(
        TargetTableName=restored_table_name, BackupArn=backup_arn
    )
    guids_added_after_restore = add_guids_to_table(restored_table_name, 5)

    source_table_items = client.scan(TableName=source_table_name)
    source_table_items.should.have.key("Count").should.equal(10)
    source_table_guids = [x["id"]["S"] for x in source_table_items["Items"]]
    set(source_table_guids).should.equal(
        set(guids_original) | set(guids_added_after_backup)
    )

    restored_table_items = client.scan(TableName=restored_table_name)
    restored_table_items.should.have.key("Count").should.equal(10)
    restored_table_guids = [x["id"]["S"] for x in restored_table_items["Items"]]
    set(restored_table_guids).should.equal(
        set(guids_original) | set(guids_added_after_restore)
    )


@mock_dynamodb2
@pytest.mark.parametrize("region", ["eu-central-1", "ap-south-1"])
def test_describe_endpoints(region):
    client = boto3.client("dynamodb", region)
    res = client.describe_endpoints()["Endpoints"]
    res.should.equal(
        [
            {
                "Address": "dynamodb.{}.amazonaws.com".format(region),
                "CachePeriodInMinutes": 1440,
            },
        ]
    )


@mock_dynamodb2
def test_update_non_existing_item_raises_error_and_does_not_contain_item_afterwards():
    """
    https://github.com/spulec/moto/issues/3729
    Exception is raised, but item was persisted anyway
    Happened because we would create a placeholder, before validating/executing the UpdateExpression
    :return:
    """
    name = "TestTable"
    conn = boto3.client("dynamodb", region_name="us-west-2")
    hkey = "primary_partition_key"
    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": hkey, "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": hkey, "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    update_expression = {
        "Key": {hkey: "some_identification_string"},
        "UpdateExpression": "set #AA.#AB = :aa",
        "ExpressionAttributeValues": {":aa": "abc"},
        "ExpressionAttributeNames": {"#AA": "some_dict", "#AB": "key1"},
        "ConditionExpression": "attribute_not_exists(#AA.#AB)",
    }
    table = boto3.resource("dynamodb", region_name="us-west-2").Table(name)
    with pytest.raises(ClientError) as err:
        table.update_item(**update_expression)
    err.value.response["Error"]["Code"].should.equal("ValidationException")

    conn.scan(TableName=name)["Items"].should.have.length_of(0)


@mock_dynamodb2
def test_gsi_lastevaluatedkey():
    # github.com/spulec/moto/issues/3968
    conn = boto3.resource("dynamodb", region_name="us-west-2")
    name = "test-table"
    table = conn.Table(name)

    conn.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "main_key", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "main_key", "AttributeType": "S"},
            {"AttributeName": "index_key", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        GlobalSecondaryIndexes=[
            {
                "IndexName": "test_index",
                "KeySchema": [{"AttributeName": "index_key", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL",},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1,
                },
            }
        ],
    )

    table.put_item(
        Item={
            "main_key": "testkey1",
            "extra_data": "testdata",
            "index_key": "indexkey",
        },
    )
    table.put_item(
        Item={
            "main_key": "testkey2",
            "extra_data": "testdata",
            "index_key": "indexkey",
        },
    )

    response = table.query(
        Limit=1,
        KeyConditionExpression=Key("index_key").eq("indexkey"),
        IndexName="test_index",
    )

    items = response["Items"]
    items.should.have.length_of(1)
    items[0].should.equal(
        {"main_key": "testkey1", "extra_data": "testdata", "index_key": "indexkey"}
    )

    last_evaluated_key = response["LastEvaluatedKey"]
    last_evaluated_key.should.have.length_of(2)
    last_evaluated_key.should.equal({"main_key": "testkey1", "index_key": "indexkey"})


@mock_dynamodb2
def test_begins_with():
    conn = boto3.client("dynamodb", region_name="us-east-1")
    name = "test-table"
    conn.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="THROUGHPUT",
    )
    for sk in ["sk1", "sk11", "sk2", "sk21", "sk3", "sk31"]:
        conn.put_item(TableName=name, Item={"pk": {"S": "pk"}, "sk": {"S": sk}})
    # Verify standard begins-with
    items = conn.query(
        TableName=name,
        KeyConditionExpression="pk = :pkval and begins_with(sk, :skval)",
        ExpressionAttributeValues={":pkval": {"S": "pk"}, ":skval": {"S": "sk1"}},
    )["Items"]
    items.should.have.length_of(2)
    # Verify begins-with with spaces everywhere
    items = conn.query(
        TableName="test-table",
        KeyConditionExpression="pk = :pkval and begins_with ( sk, :skval )",
        ExpressionAttributeValues={":pkval": {"S": "pk"}, ":skval": {"S": "sk1"}},
    )["Items"]
    items.should.have.length_of(2)
    # Verify begins-with with no spaces
    items = conn.query(
        TableName="test-table",
        KeyConditionExpression="pk = :pkval and begins_with(sk,:skval)",
        ExpressionAttributeValues={":pkval": {"S": "pk"}, ":skval": {"S": "sk1"}},
    )["Items"]
    items.should.have.length_of(2)
    # Verify begins-with without parentheses
    with pytest.raises(ClientError) as exc:
        conn.query(
            TableName="test-table",
            KeyConditionExpression="pk = :pkval and begins_with sk,:skval",
            ExpressionAttributeValues={":pkval": {"S": "pk"}, ":skval": {"S": "sk1"}},
        )
    err = exc.value.response["Error"]
    err["Code"].should.equal("ValidationException")
    err["Message"].should.contain("Invalid KeyConditionExpression: Syntax error")
