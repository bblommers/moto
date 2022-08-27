import boto3
import pytest
import sure  # noqa # pylint: disable=unused-import
from botocore.exceptions import ClientError
from moto import mock_apigateway
from uuid import uuid4


@mock_apigateway
def test_api_key_value_min_length():
    region_name = "us-east-1"
    client = boto3.client("apigateway", region_name=region_name)

    apikey_value = "12345"
    apikey_name = "TESTKEY1"
    payload = {"value": apikey_value, "name": apikey_name}

    with pytest.raises(ClientError) as e:
        client.create_api_key(**payload)
    ex = e.value
    ex.operation_name.should.equal("CreateApiKey")
    ex.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.response["Error"]["Code"].should.contain("BadRequestException")
    ex.response["Error"]["Message"].should.equal(
        "API Key value should be at least 20 characters"
    )


@mock_apigateway
def test_get_api_key_include_value():
    region_name = "us-west-2"
    client = boto3.client("apigateway", region_name=region_name)

    apikey_value = "01234567890123456789"
    apikey_name = str(uuid4())
    response = client.create_api_key(value=apikey_value, name=apikey_name)
    api_key_id_one = response["id"]

    response = client.get_api_key(apiKey=api_key_id_one)
    response.should_not.have.key("value")

    response = client.get_api_key(apiKey=api_key_id_one, includeValue=True)
    response.should.have.key("value")

    response = client.get_api_key(apiKey=api_key_id_one, includeValue=False)
    response.should_not.have.key("value")

    response = client.get_api_key(apiKey=api_key_id_one, includeValue=True)
    response.should.have.key("value")


@mock_apigateway
def test_get_api_keys_include_values():
    region_name = "us-west-2"
    client = boto3.client("apigateway", region_name=region_name)

    apikey_value = str(uuid4())
    apikey_name = str(uuid4())
    apikey_value2 = str(uuid4())
    apikey_name2 = str(uuid4())
    client.create_api_key(value=apikey_value, name=apikey_name)
    client.create_api_key(value=apikey_value2, name=apikey_name2)

    keys = client.get_api_keys()["items"]
    for api_key in keys:
        api_key.should_not.have.key("value")

    keys = client.get_api_keys(includeValues=True)["items"]
    for api_key in keys:
        api_key.should.have.key("value")

    keys = client.get_api_keys(includeValues=False)["items"]
    for api_key in keys:
        api_key.should_not.have.key("value")


@mock_apigateway
def test_create_api_key():
    region_name = "us-west-2"
    client = boto3.client("apigateway", region_name=region_name)

    apikey_value = str(uuid4())
    apikey_name = str(uuid4())
    payload = {"value": apikey_value, "name": apikey_name}

    response = client.create_api_key(**payload)
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(201)
    response["name"].should.equal(apikey_name)
    response["value"].should.equal(apikey_value)
    response["enabled"].should.equal(False)
    response["stageKeys"].should.equal([])

    keys = client.get_api_keys()["items"]
    key_ids = [k["id"] for k in keys]
    key_ids.should.contain(response["id"])


@mock_apigateway
def test_create_api_key_twice():
    region_name = "us-west-2"
    client = boto3.client("apigateway", region_name=region_name)

    apikey_value = str(uuid4())
    apikey_name = str(uuid4())
    payload = {"value": apikey_value, "name": apikey_name}

    client.create_api_key(**payload)
    with pytest.raises(ClientError) as ex:
        client.create_api_key(**payload)
    ex.value.response["Error"]["Code"].should.equal("ConflictException")


@mock_apigateway
def test_api_keys():
    region_name = "us-west-2"
    client = boto3.client("apigateway", region_name=region_name)

    apikey_value = str(uuid4())
    apikey_name = str(uuid4())
    payload = {
        "value": apikey_value,
        "name": apikey_name,
        "tags": {"tag1": "test_tag1", "tag2": "1"},
    }
    response = client.create_api_key(**payload)
    apikey_id = response["id"]
    apikey = client.get_api_key(apiKey=response["id"], includeValue=True)
    apikey["name"].should.equal(apikey_name)
    apikey["value"].should.equal(apikey_value)
    apikey["tags"]["tag1"].should.equal("test_tag1")
    apikey["tags"]["tag2"].should.equal("1")

    patch_operations = [
        {"op": "replace", "path": "/name", "value": "TESTKEY3_CHANGE"},
        {"op": "replace", "path": "/customerId", "value": "12345"},
        {"op": "replace", "path": "/description", "value": "APIKEY UPDATE TEST"},
        {"op": "replace", "path": "/enabled", "value": "false"},
    ]
    response = client.update_api_key(apiKey=apikey_id, patchOperations=patch_operations)
    response["name"].should.equal("TESTKEY3_CHANGE")
    response["customerId"].should.equal("12345")
    response["description"].should.equal("APIKEY UPDATE TEST")
    response["enabled"].should.equal(False)

    updated_api_key = client.get_api_key(apiKey=apikey_id)
    updated_api_key["name"].should.equal("TESTKEY3_CHANGE")
    updated_api_key["customerId"].should.equal("12345")
    updated_api_key["description"].should.equal("APIKEY UPDATE TEST")
    updated_api_key["enabled"].should.equal(False)

    payload = {"name": apikey_name}
    apikey_id2 = client.create_api_key(**payload)["id"]

    response = client.delete_api_key(apiKey=apikey_id)
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(202)

    keys = client.get_api_keys()["items"]
    key_ids = [k["id"] for k in keys]
    key_ids.should.contain(apikey_id2)
    key_ids.shouldnt.contain(apikey_id)


@mock_apigateway
def test_get_api_key_unknown_apikey():
    client = boto3.client("apigateway", region_name="us-east-1")
    with pytest.raises(ClientError) as ex:
        client.get_api_key(apiKey="unknown")

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid API Key identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
