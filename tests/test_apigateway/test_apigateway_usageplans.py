import boto3
import sure  # noqa # pylint: disable=unused-import
import pytest

from botocore.exceptions import ClientError
from moto import mock_apigateway


@mock_apigateway
def test_usage_plans():
    region_name = "us-west-2"
    client = boto3.client("apigateway", region_name=region_name)

    # # Try to get info about a non existing usage
    with pytest.raises(ClientError) as ex:
        client.get_usage_plan(usagePlanId="not_existing")
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["Error"]["Message"].should.equal(
        "Invalid Usage Plan ID specified"
    )

    usage_plan_name = "TEST-PLAN"
    payload = {"name": usage_plan_name}
    response = client.create_usage_plan(**payload)
    usage_plan = client.get_usage_plan(usagePlanId=response["id"])
    usage_plan["name"].should.equal(usage_plan_name)
    usage_plan["apiStages"].should.equal([])

    payload = {
        "name": "TEST-PLAN-2",
        "description": "Description",
        "quota": {"limit": 10, "period": "DAY", "offset": 0},
        "throttle": {"rateLimit": 2, "burstLimit": 1},
        "apiStages": [{"apiId": "foo", "stage": "bar"}],
        "tags": {"tag_key": "tag_value"},
    }
    response = client.create_usage_plan(**payload)
    usage_plan_id = response["id"]
    usage_plan = client.get_usage_plan(usagePlanId=usage_plan_id)

    # The payload should remain unchanged
    for key, value in payload.items():
        usage_plan.should.have.key(key).which.should.equal(value)

    # Status code should be 200
    usage_plan["ResponseMetadata"].should.have.key("HTTPStatusCode").which.should.equal(
        200
    )

    # An Id should've been generated
    usage_plan.should.have.key("id").which.should_not.be.none

    response = client.get_usage_plans()
    len(response["items"]).should.equal(2)

    client.delete_usage_plan(usagePlanId=usage_plan_id)

    response = client.get_usage_plans()
    len(response["items"]).should.equal(1)


@mock_apigateway
def test_update_usage_plan():
    region_name = "us-west-2"
    client = boto3.client("apigateway", region_name=region_name)

    payload = {
        "name": "TEST-PLAN-2",
        "description": "Description",
        "quota": {"limit": 10, "period": "DAY", "offset": 0},
        "throttle": {"rateLimit": 2, "burstLimit": 1},
        "apiStages": [{"apiId": "foo", "stage": "bar"}],
        "tags": {"tag_key": "tag_value"},
    }
    response = client.create_usage_plan(**payload)
    usage_plan_id = response["id"]
    response = client.update_usage_plan(
        usagePlanId=usage_plan_id,
        patchOperations=[
            {"op": "replace", "path": "/quota/limit", "value": "1000"},
            {"op": "replace", "path": "/quota/period", "value": "MONTH"},
            {"op": "replace", "path": "/throttle/rateLimit", "value": "500"},
            {"op": "replace", "path": "/throttle/burstLimit", "value": "1500"},
            {"op": "replace", "path": "/name", "value": "new-name"},
            {"op": "replace", "path": "/description", "value": "new-description"},
            {"op": "replace", "path": "/productCode", "value": "new-productionCode"},
        ],
    )
    response["quota"]["limit"].should.equal(1000)
    response["quota"]["period"].should.equal("MONTH")
    response["name"].should.equal("new-name")
    response["description"].should.equal("new-description")
    response["productCode"].should.equal("new-productionCode")


@mock_apigateway
def test_usage_plan_keys():
    region_name = "us-west-2"
    client = boto3.client("apigateway", region_name=region_name)
    usage_plan_id = "test"

    # Create an API key so we can use it
    key_name = "test-api-key"
    response = client.create_api_key(name=key_name)
    key_id = response["id"]
    key_value = response["value"]

    # Get current plan keys (expect none)
    response = client.get_usage_plan_keys(usagePlanId=usage_plan_id)
    len(response["items"]).should.equal(0)

    # Create usage plan key
    key_type = "API_KEY"
    payload = {"usagePlanId": usage_plan_id, "keyId": key_id, "keyType": key_type}
    response = client.create_usage_plan_key(**payload)
    response["ResponseMetadata"]["HTTPStatusCode"].should.equals(201)
    usage_plan_key_id = response["id"]

    # Get current plan keys (expect 1)
    response = client.get_usage_plan_keys(usagePlanId=usage_plan_id)
    len(response["items"]).should.equal(1)

    # Get a single usage plan key and check it matches the created one
    usage_plan_key = client.get_usage_plan_key(
        usagePlanId=usage_plan_id, keyId=usage_plan_key_id
    )
    usage_plan_key["name"].should.equal(key_name)
    usage_plan_key["id"].should.equal(key_id)
    usage_plan_key["type"].should.equal(key_type)
    usage_plan_key["value"].should.equal(key_value)

    # Delete usage plan key
    client.delete_usage_plan_key(usagePlanId=usage_plan_id, keyId=key_id)

    # Get current plan keys (expect none)
    response = client.get_usage_plan_keys(usagePlanId=usage_plan_id)
    len(response["items"]).should.equal(0)

    # Try to get info about a non existing api key
    with pytest.raises(ClientError) as ex:
        client.get_usage_plan_key(usagePlanId=usage_plan_id, keyId="not_existing_key")
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["Error"]["Message"].should.equal(
        "Invalid API Key identifier specified"
    )

    # Try to get info about an existing api key that has not jet added to a valid usage plan
    with pytest.raises(ClientError) as ex:
        client.get_usage_plan_key(usagePlanId=usage_plan_id, keyId=key_id)
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["Error"]["Message"].should.equal(
        "Invalid Usage Plan ID specified"
    )

    # Try to get info about an existing api key that has not jet added to a valid usage plan
    with pytest.raises(ClientError) as ex:
        client.get_usage_plan_key(usagePlanId="not_existing_plan_id", keyId=key_id)
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["Error"]["Message"].should.equal(
        "Invalid Usage Plan ID specified"
    )


@mock_apigateway
def test_create_usage_plan_key_non_existent_api_key():
    region_name = "us-west-2"
    client = boto3.client("apigateway", region_name=region_name)
    usage_plan_id = "test"

    # Attempt to create a usage plan key for a API key that doesn't exists
    payload = {
        "usagePlanId": usage_plan_id,
        "keyId": "non-existent",
        "keyType": "API_KEY",
    }
    client.create_usage_plan_key.when.called_with(**payload).should.throw(ClientError)


@mock_apigateway
def test_get_usage_plans_using_key_id():
    region_name = "us-west-2"
    client = boto3.client("apigateway", region_name=region_name)

    # Create 2 Usage Plans
    # one will be attached to an API Key, the other will remain unattached
    attached_plan = client.create_usage_plan(name="Attached")
    unattached_plan = client.create_usage_plan(name="Unattached")

    # Create an API key
    # to attach to the usage plan
    key_name = "test-api-key"
    response = client.create_api_key(name=key_name)
    key_id = response["id"]

    # Create a Usage Plan Key
    # Attached the Usage Plan and API Key
    key_type = "API_KEY"
    payload = {"usagePlanId": attached_plan["id"], "keyId": key_id, "keyType": key_type}
    client.create_usage_plan_key(**payload)

    # All usage plans should be returned when keyId is not included
    all_plans = client.get_usage_plans()["items"]
    all_ids = [p["id"] for p in all_plans]
    all_ids.should.contain(attached_plan["id"])
    all_ids.should.contain(unattached_plan["id"])

    # Only the usage plan attached to the given api key are included
    only_plans_with_key = client.get_usage_plans(keyId=key_id)
    len(only_plans_with_key["items"]).should.equal(1)
    only_plans_with_key["items"][0]["name"].should.equal(attached_plan["name"])
    only_plans_with_key["items"][0]["id"].should.equal(attached_plan["id"])
