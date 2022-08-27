import boto3
import json
import sure  # noqa # pylint: disable=unused-import
import pytest

from botocore.exceptions import ClientError
from freezegun import freeze_time
from moto import mock_apigateway, mock_cognitoidp
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID
from . import create_method_integration


@freeze_time("2015-01-01")
@mock_apigateway
def test_create_and_get_rest_api():
    client = boto3.client("apigateway", region_name="us-west-2")

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    response = client.get_rest_api(restApiId=api_id)

    response.pop("ResponseMetadata")
    response.pop("createdDate")
    response.should.equal(
        {
            "id": api_id,
            "name": "my_api",
            "description": "this is my api",
            "version": "V1",
            "binaryMediaTypes": [],
            "apiKeySource": "HEADER",
            "endpointConfiguration": {"types": ["EDGE"]},
            "tags": {},
            "disableExecuteApiEndpoint": False,
        }
    )


@mock_apigateway
def test_update_rest_api():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    patchOperations = [
        {"op": "replace", "path": "/name", "value": "new-name"},
        {"op": "replace", "path": "/description", "value": "new-description"},
        {"op": "replace", "path": "/apiKeySource", "value": "AUTHORIZER"},
        {"op": "replace", "path": "/binaryMediaTypes", "value": "image/jpeg"},
        {"op": "replace", "path": "/disableExecuteApiEndpoint", "value": "True"},
    ]

    response = client.update_rest_api(restApiId=api_id, patchOperations=patchOperations)
    response.pop("ResponseMetadata")
    response.pop("createdDate")
    response.pop("binaryMediaTypes")
    response.should.equal(
        {
            "id": api_id,
            "name": "new-name",
            "version": "V1",
            "description": "new-description",
            "apiKeySource": "AUTHORIZER",
            "endpointConfiguration": {"types": ["EDGE"]},
            "tags": {},
            "disableExecuteApiEndpoint": True,
        }
    )
    # should fail with wrong apikeysoruce
    patchOperations = [
        {"op": "replace", "path": "/apiKeySource", "value": "Wrong-value-AUTHORIZER"}
    ]
    with pytest.raises(ClientError) as ex:
        response = client.update_rest_api(
            restApiId=api_id, patchOperations=patchOperations
        )

    ex.value.response["Error"]["Message"].should.equal(
        "1 validation error detected: Value 'Wrong-value-AUTHORIZER' at 'createRestApiInput.apiKeySource' failed to satisfy constraint: Member must satisfy enum value set: [AUTHORIZER, HEADER]"
    )
    ex.value.response["Error"]["Code"].should.equal("ValidationException")


@mock_apigateway
def test_update_rest_api_invalid_api_id():
    client = boto3.client("apigateway", region_name="us-west-2")
    patchOperations = [
        {"op": "replace", "path": "/apiKeySource", "value": "AUTHORIZER"}
    ]
    with pytest.raises(ClientError) as ex:
        client.update_rest_api(restApiId="api_id", patchOperations=patchOperations)
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")


@mock_apigateway
def test_update_rest_api_operation_add_remove():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    patchOperations = [
        {"op": "add", "path": "/binaryMediaTypes", "value": "image/png"},
        {"op": "add", "path": "/binaryMediaTypes", "value": "image/jpeg"},
    ]
    response = client.update_rest_api(restApiId=api_id, patchOperations=patchOperations)
    response["binaryMediaTypes"].should.equal(["image/png", "image/jpeg"])
    response["description"].should.equal("this is my api")
    patchOperations = [
        {"op": "remove", "path": "/binaryMediaTypes", "value": "image/png"},
        {"op": "remove", "path": "/description"},
    ]
    response = client.update_rest_api(restApiId=api_id, patchOperations=patchOperations)
    response["binaryMediaTypes"].should.equal(["image/jpeg"])
    response["description"].should.equal("")


@mock_apigateway
def test_list_and_delete_apis():
    client = boto3.client("apigateway", region_name="us-west-2")

    api_id1 = client.create_rest_api(name="my_api")["id"]
    api_id2 = client.create_rest_api(name="my_api2")["id"]

    apis = client.get_rest_apis()["items"]
    api_ids = [a["id"] for a in apis]
    api_ids.should.contain(api_id1)
    api_ids.should.contain(api_id2)

    client.delete_rest_api(restApiId=api_id1)

    apis = client.get_rest_apis()["items"]
    api_ids = [a["id"] for a in apis]
    api_ids.should.contain(api_id2)
    api_ids.shouldnt.contain(api_id1)


@mock_apigateway
def test_create_rest_api_with_tags():
    client = boto3.client("apigateway", region_name="us-west-2")

    response = client.create_rest_api(
        name="my_api", description="this is my api", tags={"MY_TAG1": "MY_VALUE1"}
    )
    api_id = response["id"]

    response = client.get_rest_api(restApiId=api_id)

    assert "tags" in response
    response["tags"].should.equal({"MY_TAG1": "MY_VALUE1"})


@mock_apigateway
def test_create_rest_api_with_policy():
    client = boto3.client("apigateway", region_name="us-west-2")

    policy = '{"Version": "2012-10-17","Statement": []}'
    response = client.create_rest_api(
        name="my_api", description="this is my api", policy=policy
    )
    api_id = response["id"]

    response = client.get_rest_api(restApiId=api_id)

    assert "policy" in response
    response["policy"].should.equal(policy)


@mock_apigateway
def test_create_rest_api_invalid_apikeysource():
    client = boto3.client("apigateway", region_name="us-west-2")

    with pytest.raises(ClientError) as ex:
        client.create_rest_api(
            name="my_api",
            description="this is my api",
            apiKeySource="not a valid api key source",
        )
    ex.value.response["Error"]["Code"].should.equal("ValidationException")


@mock_apigateway
def test_create_rest_api_valid_apikeysources():
    client = boto3.client("apigateway", region_name="us-west-2")

    # 1. test creating rest api with HEADER apiKeySource
    response = client.create_rest_api(
        name="my_api", description="this is my api", apiKeySource="HEADER"
    )
    api_id = response["id"]

    response = client.get_rest_api(restApiId=api_id)
    response["apiKeySource"].should.equal("HEADER")

    # 2. test creating rest api with AUTHORIZER apiKeySource
    response = client.create_rest_api(
        name="my_api2", description="this is my api", apiKeySource="AUTHORIZER"
    )
    api_id = response["id"]

    response = client.get_rest_api(restApiId=api_id)
    response["apiKeySource"].should.equal("AUTHORIZER")


@mock_apigateway
def test_create_rest_api_invalid_endpointconfiguration():
    client = boto3.client("apigateway", region_name="us-west-2")

    with pytest.raises(ClientError) as ex:
        client.create_rest_api(
            name="my_api",
            description="this is my api",
            endpointConfiguration={"types": ["INVALID"]},
        )
    ex.value.response["Error"]["Code"].should.equal("ValidationException")


@mock_apigateway
def test_create_rest_api_valid_endpointconfigurations():
    client = boto3.client("apigateway", region_name="us-west-2")

    # 1. test creating rest api with PRIVATE endpointConfiguration
    response = client.create_rest_api(
        name="my_api",
        description="this is my api",
        endpointConfiguration={"types": ["PRIVATE"]},
    )
    api_id = response["id"]

    response = client.get_rest_api(restApiId=api_id)
    response["endpointConfiguration"].should.equal({"types": ["PRIVATE"]})

    # 2. test creating rest api with REGIONAL endpointConfiguration
    response = client.create_rest_api(
        name="my_api2",
        description="this is my api",
        endpointConfiguration={"types": ["REGIONAL"]},
    )
    api_id = response["id"]

    response = client.get_rest_api(restApiId=api_id)
    response["endpointConfiguration"].should.equal({"types": ["REGIONAL"]})

    # 3. test creating rest api with EDGE endpointConfiguration
    response = client.create_rest_api(
        name="my_api3",
        description="this is my api",
        endpointConfiguration={"types": ["EDGE"]},
    )
    api_id = response["id"]

    response = client.get_rest_api(restApiId=api_id)
    response["endpointConfiguration"].should.equal({"types": ["EDGE"]})


@mock_apigateway
def test_create_resource__validate_name():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]

    invalid_names = ["/users", "users/", "users/{user_id}", "us{er", "us+er"]
    valid_names = ["users", "{user_id}", "{proxy+}", "user_09", "good-dog"]
    # All invalid names should throw an exception
    for name in invalid_names:
        with pytest.raises(ClientError) as ex:
            client.create_resource(restApiId=api_id, parentId=root_id, pathPart=name)
        ex.value.response["Error"]["Code"].should.equal("BadRequestException")
        ex.value.response["Error"]["Message"].should.equal(
            "Resource's path part only allow a-zA-Z0-9._- and curly braces at the beginning and the end and an optional plus sign before the closing brace."
        )
    # All valid names  should go through
    for name in valid_names:
        client.create_resource(restApiId=api_id, parentId=root_id, pathPart=name)


@mock_apigateway
def test_create_resource():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]

    root_resource = client.get_resource(restApiId=api_id, resourceId=root_id)
    # this is hard to match against, so remove it
    root_resource["ResponseMetadata"].pop("HTTPHeaders", None)
    root_resource["ResponseMetadata"].pop("RetryAttempts", None)
    root_resource.should.equal(
        {"path": "/", "id": root_id, "ResponseMetadata": {"HTTPStatusCode": 200}}
    )

    client.create_resource(restApiId=api_id, parentId=root_id, pathPart="users")

    resources = client.get_resources(restApiId=api_id)["items"]
    len(resources).should.equal(2)
    non_root_resource = [resource for resource in resources if resource["path"] != "/"][
        0
    ]

    client.delete_resource(restApiId=api_id, resourceId=non_root_resource["id"])

    len(client.get_resources(restApiId=api_id)["items"]).should.equal(1)


@mock_apigateway
def test_child_resource():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]

    response = client.create_resource(
        restApiId=api_id, parentId=root_id, pathPart="users"
    )
    users_id = response["id"]

    response = client.create_resource(
        restApiId=api_id, parentId=users_id, pathPart="tags"
    )
    tags_id = response["id"]

    child_resource = client.get_resource(restApiId=api_id, resourceId=tags_id)
    # this is hard to match against, so remove it
    child_resource["ResponseMetadata"].pop("HTTPHeaders", None)
    child_resource["ResponseMetadata"].pop("RetryAttempts", None)
    child_resource.should.equal(
        {
            "path": "/users/tags",
            "pathPart": "tags",
            "parentId": users_id,
            "id": tags_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
    )


@mock_apigateway
def test_create_method():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]

    client.put_method(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="none"
    )

    response = client.get_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")

    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {
            "httpMethod": "GET",
            "authorizationType": "none",
            "apiKeyRequired": False,
            "methodResponses": {},
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
    )


@mock_apigateway
def test_create_method_apikeyrequired():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]

    client.put_method(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="GET",
        authorizationType="none",
        apiKeyRequired=True,
    )

    response = client.get_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")

    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {
            "httpMethod": "GET",
            "authorizationType": "none",
            "apiKeyRequired": True,
            "methodResponses": {},
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
    )


@mock_apigateway
def test_create_method_response():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]

    client.put_method(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="none"
    )

    response = client.get_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")

    response = client.put_method_response(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
    )
    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {"ResponseMetadata": {"HTTPStatusCode": 201}, "statusCode": "200"}
    )

    response = client.get_method_response(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
    )
    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {"ResponseMetadata": {"HTTPStatusCode": 200}, "statusCode": "200"}
    )

    response = client.delete_method_response(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
    )
    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal({"ResponseMetadata": {"HTTPStatusCode": 204}})


@mock_apigateway
def test_get_method_unknown_resource_id():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    with pytest.raises(ClientError) as ex:
        client.get_method(restApiId=api_id, resourceId="sth", httpMethod="GET")
    err = ex.value.response["Error"]
    err["Code"].should.equal("NotFoundException")
    err["Message"].should.equal("Invalid resource identifier specified")


@mock_apigateway
def test_delete_method():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]

    client.put_method(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="none"
    )

    client.get_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")

    client.delete_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")

    with pytest.raises(ClientError) as ex:
        client.get_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    err = ex.value.response["Error"]
    err["Code"].should.equal("NotFoundException")
    err["Message"].should.equal("Invalid Method identifier specified")


@mock_apigateway
def test_integrations():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]

    client.put_method(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="none"
    )

    client.put_method_response(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
    )

    response = client.put_integration(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="GET",
        type="HTTP",
        passthroughBehavior="WHEN_NO_TEMPLATES",
        uri="http://httpbin.org/robots.txt",
        integrationHttpMethod="POST",
        requestParameters={"integration.request.header.X-Custom": "'Custom'"},
    )
    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {
            "ResponseMetadata": {"HTTPStatusCode": 201},
            "httpMethod": "POST",
            "type": "HTTP",
            "uri": "http://httpbin.org/robots.txt",
            "passthroughBehavior": "WHEN_NO_TEMPLATES",
            "cacheKeyParameters": [],
            "requestParameters": {"integration.request.header.X-Custom": "'Custom'"},
        }
    )

    response = client.get_integration(
        restApiId=api_id, resourceId=root_id, httpMethod="GET"
    )
    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "httpMethod": "POST",
            "type": "HTTP",
            "uri": "http://httpbin.org/robots.txt",
            "passthroughBehavior": "WHEN_NO_TEMPLATES",
            "cacheKeyParameters": [],
            "requestParameters": {"integration.request.header.X-Custom": "'Custom'"},
        }
    )

    response = client.get_resource(restApiId=api_id, resourceId=root_id)
    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response["resourceMethods"]["GET"]["httpMethod"].should.equal("GET")
    response["resourceMethods"]["GET"]["authorizationType"].should.equal("none")
    response["resourceMethods"]["GET"]["methodIntegration"].should.equal(
        {
            "httpMethod": "POST",
            "type": "HTTP",
            "uri": "http://httpbin.org/robots.txt",
            "cacheKeyParameters": [],
            "passthroughBehavior": "WHEN_NO_TEMPLATES",
            "requestParameters": {"integration.request.header.X-Custom": "'Custom'"},
        }
    )

    client.delete_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET")

    response = client.get_resource(restApiId=api_id, resourceId=root_id)
    response["resourceMethods"]["GET"].shouldnt.contain("methodIntegration")

    # Create a new integration with a requestTemplates config

    client.put_method(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="POST",
        authorizationType="none",
    )

    templates = {
        # example based on
        # http://docs.aws.amazon.com/apigateway/latest/developerguide/api-as-kinesis-proxy-export-swagger-with-extensions.html
        "application/json": '{\n    "StreamName": "$input.params(\'stream-name\')",\n    "Records": []\n}'
    }
    test_uri = "http://example.com/foobar.txt"
    response = client.put_integration(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="POST",
        type="HTTP",
        uri=test_uri,
        requestTemplates=templates,
        passthroughBehavior="WHEN_NO_MATCH",
        integrationHttpMethod="POST",
        timeoutInMillis=29000,
    )

    response = client.get_integration(
        restApiId=api_id, resourceId=root_id, httpMethod="POST"
    )
    response["uri"].should.equal(test_uri)
    response["requestTemplates"].should.equal(templates)
    response["passthroughBehavior"].should.equal("WHEN_NO_MATCH")
    response.should.have.key("timeoutInMillis").equals(29000)


@mock_apigateway
def test_integration_response():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]

    client.put_method(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="none"
    )

    client.put_method_response(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
    )

    client.put_integration(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="GET",
        type="HTTP",
        uri="http://httpbin.org/robots.txt",
        integrationHttpMethod="POST",
    )

    response = client.put_integration_response(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="foobar",
        responseTemplates={},
    )

    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {
            "statusCode": "200",
            "selectionPattern": "foobar",
            "ResponseMetadata": {"HTTPStatusCode": 201},
            "responseTemplates": {},  # Note: TF compatibility
        }
    )

    response = client.get_integration_response(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
    )
    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {
            "statusCode": "200",
            "selectionPattern": "foobar",
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "responseTemplates": {},  # Note: TF compatibility
        }
    )

    response = client.get_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response["methodIntegration"]["integrationResponses"].should.equal(
        {
            "200": {
                "responseTemplates": {},  # Note: TF compatibility
                "selectionPattern": "foobar",
                "statusCode": "200",
            }
        }
    )

    response = client.delete_integration_response(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
    )

    response = client.get_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    response["methodIntegration"]["integrationResponses"].should.equal({})

    # adding a new method and perfomring put intergration with contentHandling as CONVERT_TO_BINARY
    client.put_method(
        restApiId=api_id, resourceId=root_id, httpMethod="PUT", authorizationType="none"
    )

    client.put_method_response(
        restApiId=api_id, resourceId=root_id, httpMethod="PUT", statusCode="200"
    )

    client.put_integration(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="PUT",
        type="HTTP",
        uri="http://httpbin.org/robots.txt",
        integrationHttpMethod="POST",
    )

    response = client.put_integration_response(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="PUT",
        statusCode="200",
        selectionPattern="foobar",
        responseTemplates={},
        contentHandling="CONVERT_TO_BINARY",
    )

    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {
            "statusCode": "200",
            "selectionPattern": "foobar",
            "ResponseMetadata": {"HTTPStatusCode": 201},
            "responseTemplates": {},  # Note: TF compatibility
            "contentHandling": "CONVERT_TO_BINARY",
        }
    )

    response = client.get_integration_response(
        restApiId=api_id, resourceId=root_id, httpMethod="PUT", statusCode="200"
    )
    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {
            "statusCode": "200",
            "selectionPattern": "foobar",
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "responseTemplates": {},  # Note: TF compatibility
            "contentHandling": "CONVERT_TO_BINARY",
        }
    )


@mock_apigateway
@mock_cognitoidp
def test_update_authorizer_configuration():
    client = boto3.client("apigateway", region_name="us-west-2")
    authorizer_name = "my_authorizer"
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    cognito_client = boto3.client("cognito-idp", region_name="us-west-2")
    user_pool_arn = cognito_client.create_user_pool(PoolName="my_cognito_pool")[
        "UserPool"
    ]["Arn"]

    response = client.create_authorizer(
        restApiId=api_id,
        name=authorizer_name,
        type="COGNITO_USER_POOLS",
        providerARNs=[user_pool_arn],
        identitySource="method.request.header.Authorization",
    )
    authorizer_id = response["id"]

    response = client.get_authorizer(restApiId=api_id, authorizerId=authorizer_id)
    # createdDate is hard to match against, remove it
    response.pop("createdDate", None)
    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {
            "id": authorizer_id,
            "name": authorizer_name,
            "type": "COGNITO_USER_POOLS",
            "providerARNs": [user_pool_arn],
            "identitySource": "method.request.header.Authorization",
            "authorizerResultTtlInSeconds": 300,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
    )

    client.update_authorizer(
        restApiId=api_id,
        authorizerId=authorizer_id,
        patchOperations=[{"op": "replace", "path": "/type", "value": "TOKEN"}],
    )

    authorizer = client.get_authorizer(restApiId=api_id, authorizerId=authorizer_id)

    authorizer.should.have.key("type").which.should.equal("TOKEN")

    client.update_authorizer(
        restApiId=api_id,
        authorizerId=authorizer_id,
        patchOperations=[{"op": "replace", "path": "/type", "value": "REQUEST"}],
    )

    authorizer = client.get_authorizer(restApiId=api_id, authorizerId=authorizer_id)

    authorizer.should.have.key("type").which.should.equal("REQUEST")

    # TODO: implement mult-update tests

    try:
        client.update_authorizer(
            restApiId=api_id,
            authorizerId=authorizer_id,
            patchOperations=[
                {"op": "add", "path": "/notasetting", "value": "eu-west-1"}
            ],
        )
        assert False.should.be.ok  # Fail, should not be here
    except Exception:
        assert True.should.be.ok


@mock_apigateway
def test_non_existent_authorizer():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    client.get_authorizer.when.called_with(
        restApiId=api_id, authorizerId="xxx"
    ).should.throw(ClientError)


@mock_apigateway
@mock_cognitoidp
def test_create_authorizer():
    client = boto3.client("apigateway", region_name="us-west-2")
    authorizer_name = "my_authorizer"
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    cognito_client = boto3.client("cognito-idp", region_name="us-west-2")
    user_pool_arn = cognito_client.create_user_pool(PoolName="my_cognito_pool")[
        "UserPool"
    ]["Arn"]

    response = client.create_authorizer(
        restApiId=api_id,
        name=authorizer_name,
        type="COGNITO_USER_POOLS",
        providerARNs=[user_pool_arn],
        identitySource="method.request.header.Authorization",
    )
    authorizer_id = response["id"]

    response = client.get_authorizer(restApiId=api_id, authorizerId=authorizer_id)
    # createdDate is hard to match against, remove it
    response.pop("createdDate", None)
    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {
            "id": authorizer_id,
            "name": authorizer_name,
            "type": "COGNITO_USER_POOLS",
            "providerARNs": [user_pool_arn],
            "identitySource": "method.request.header.Authorization",
            "authorizerResultTtlInSeconds": 300,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
    )

    authorizer_name2 = "my_authorizer2"
    response = client.create_authorizer(
        restApiId=api_id,
        name=authorizer_name2,
        type="COGNITO_USER_POOLS",
        providerARNs=[user_pool_arn],
        identitySource="method.request.header.Authorization",
    )
    authorizer_id2 = response["id"]

    response = client.get_authorizers(restApiId=api_id)

    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)

    response["items"][0]["id"].should.match(
        r"{0}|{1}".format(authorizer_id2, authorizer_id)
    )
    response["items"][1]["id"].should.match(
        r"{0}|{1}".format(authorizer_id2, authorizer_id)
    )

    new_authorizer_name_with_vars = "authorizer_with_vars"
    response = client.create_authorizer(
        restApiId=api_id,
        name=new_authorizer_name_with_vars,
        type="COGNITO_USER_POOLS",
        providerARNs=[user_pool_arn],
        identitySource="method.request.header.Authorization",
    )
    authorizer_id3 = response["id"]

    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)

    response.should.equal(
        {
            "name": new_authorizer_name_with_vars,
            "id": authorizer_id3,
            "type": "COGNITO_USER_POOLS",
            "providerARNs": [user_pool_arn],
            "identitySource": "method.request.header.Authorization",
            "authorizerResultTtlInSeconds": 300,
            "ResponseMetadata": {"HTTPStatusCode": 201},
        }
    )

    stage = client.get_authorizer(restApiId=api_id, authorizerId=authorizer_id3)
    stage["name"].should.equal(new_authorizer_name_with_vars)
    stage["id"].should.equal(authorizer_id3)
    stage["type"].should.equal("COGNITO_USER_POOLS")
    stage["providerARNs"].should.equal([user_pool_arn])
    stage["identitySource"].should.equal("method.request.header.Authorization")
    stage["authorizerResultTtlInSeconds"].should.equal(300)


@mock_apigateway
@mock_cognitoidp
def test_delete_authorizer():
    client = boto3.client("apigateway", region_name="us-west-2")
    authorizer_name = "my_authorizer"
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    cognito_client = boto3.client("cognito-idp", region_name="us-west-2")
    user_pool_arn = cognito_client.create_user_pool(PoolName="my_cognito_pool")[
        "UserPool"
    ]["Arn"]

    response = client.create_authorizer(
        restApiId=api_id,
        name=authorizer_name,
        type="COGNITO_USER_POOLS",
        providerARNs=[user_pool_arn],
        identitySource="method.request.header.Authorization",
    )
    authorizer_id = response["id"]

    response = client.get_authorizer(restApiId=api_id, authorizerId=authorizer_id)
    # createdDate is hard to match against, remove it
    response.pop("createdDate", None)
    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {
            "id": authorizer_id,
            "name": authorizer_name,
            "type": "COGNITO_USER_POOLS",
            "providerARNs": [user_pool_arn],
            "identitySource": "method.request.header.Authorization",
            "authorizerResultTtlInSeconds": 300,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
    )

    authorizer_name2 = "my_authorizer2"
    response = client.create_authorizer(
        restApiId=api_id,
        name=authorizer_name2,
        type="COGNITO_USER_POOLS",
        providerARNs=[user_pool_arn],
        identitySource="method.request.header.Authorization",
    )
    authorizer_id2 = response["id"]

    authorizers = client.get_authorizers(restApiId=api_id)["items"]
    sorted([authorizer["name"] for authorizer in authorizers]).should.equal(
        sorted([authorizer_name2, authorizer_name])
    )
    # delete stage
    response = client.delete_authorizer(restApiId=api_id, authorizerId=authorizer_id2)
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(202)
    # verify other stage still exists
    authorizers = client.get_authorizers(restApiId=api_id)["items"]
    sorted([authorizer["name"] for authorizer in authorizers]).should.equal(
        sorted([authorizer_name])
    )


@mock_apigateway
def test_put_integration_response_with_response_template():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]

    client.put_method(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE"
    )
    client.put_method_response(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
    )
    client.put_integration(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="GET",
        type="HTTP",
        uri="http://httpbin.org/robots.txt",
        integrationHttpMethod="POST",
    )

    client.put_integration_response(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="foobar",
        responseTemplates={"application/json": json.dumps({"data": "test"})},
    )

    response = client.get_integration_response(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
    )

    # this is hard to match against, so remove it
    response["ResponseMetadata"].pop("HTTPHeaders", None)
    response["ResponseMetadata"].pop("RetryAttempts", None)
    response.should.equal(
        {
            "statusCode": "200",
            "selectionPattern": "foobar",
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "responseTemplates": {"application/json": json.dumps({"data": "test"})},
        }
    )


@mock_apigateway
def test_put_integration_response_but_integration_not_found():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]

    client.put_method(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE"
    )
    client.put_method_response(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
    )

    with pytest.raises(ClientError) as ex:
        client.put_integration_response(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            statusCode="200",
            selectionPattern="foobar",
            responseTemplates={"application/json": json.dumps({"data": "test"})},
        )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)


@mock_apigateway
def test_put_integration_validation():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    resources = client.get_resources(restApiId=api_id)
    root_id = [resource for resource in resources["items"] if resource["path"] == "/"][
        0
    ]["id"]

    client.put_method(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE"
    )
    client.put_method_response(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
    )

    http_types = ["HTTP", "HTTP_PROXY"]
    aws_types = ["AWS", "AWS_PROXY"]
    types_requiring_integration_method = http_types + aws_types
    types_not_requiring_integration_method = ["MOCK"]

    for _type in types_requiring_integration_method:
        # Ensure that integrations of these types fail if no integrationHttpMethod is provided
        with pytest.raises(ClientError) as ex:
            client.put_integration(
                restApiId=api_id,
                resourceId=root_id,
                httpMethod="GET",
                type=_type,
                uri="http://httpbin.org/robots.txt",
            )
        ex.value.response["Error"]["Code"].should.equal("BadRequestException")
        ex.value.response["Error"]["Message"].should.equal(
            "Enumeration value for HttpMethod must be non-empty"
        )
    for _type in types_not_requiring_integration_method:
        # Ensure that integrations of these types do not need the integrationHttpMethod
        client.put_integration(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            type=_type,
            uri="http://httpbin.org/robots.txt",
        )
    for _type in http_types:
        # Ensure that it works fine when providing the integrationHttpMethod-argument
        client.put_integration(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            type=_type,
            uri="http://httpbin.org/robots.txt",
            integrationHttpMethod="POST",
        )
    for _type in ["AWS"]:
        # Ensure that it works fine when providing the integrationHttpMethod + credentials
        client.put_integration(
            restApiId=api_id,
            resourceId=root_id,
            credentials="arn:aws:iam::{}:role/service-role/testfunction-role-oe783psq".format(
                ACCOUNT_ID
            ),
            httpMethod="GET",
            type=_type,
            uri="arn:aws:apigateway:us-west-2:s3:path/b/k",
            integrationHttpMethod="POST",
        )
    for _type in aws_types:
        # Ensure that credentials are not required when URI points to a Lambda stream
        client.put_integration(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            type=_type,
            uri="arn:aws:apigateway:eu-west-1:lambda:path/2015-03-31/functions/arn:aws:lambda:eu-west-1:012345678901:function:MyLambda/invocations",
            integrationHttpMethod="POST",
        )
    for _type in ["AWS_PROXY"]:
        # Ensure that aws_proxy does not support S3
        with pytest.raises(ClientError) as ex:
            client.put_integration(
                restApiId=api_id,
                resourceId=root_id,
                credentials="arn:aws:iam::{}:role/service-role/testfunction-role-oe783psq".format(
                    ACCOUNT_ID
                ),
                httpMethod="GET",
                type=_type,
                uri="arn:aws:apigateway:us-west-2:s3:path/b/k",
                integrationHttpMethod="POST",
            )
        ex.value.response["Error"]["Code"].should.equal("BadRequestException")
        ex.value.response["Error"]["Message"].should.equal(
            "Integrations of type 'AWS_PROXY' currently only supports Lambda function and Firehose stream invocations."
        )
    for _type in aws_types:
        # Ensure that the Role ARN is for the current account
        with pytest.raises(ClientError) as ex:
            client.put_integration(
                restApiId=api_id,
                resourceId=root_id,
                credentials="arn:aws:iam::000000000000:role/service-role/testrole",
                httpMethod="GET",
                type=_type,
                uri="arn:aws:apigateway:us-west-2:s3:path/b/k",
                integrationHttpMethod="POST",
            )
        ex.value.response["Error"]["Code"].should.equal("AccessDeniedException")
        ex.value.response["Error"]["Message"].should.equal(
            "Cross-account pass role is not allowed."
        )
    for _type in ["AWS"]:
        # Ensure that the Role ARN is specified for aws integrations
        with pytest.raises(ClientError) as ex:
            client.put_integration(
                restApiId=api_id,
                resourceId=root_id,
                httpMethod="GET",
                type=_type,
                uri="arn:aws:apigateway:us-west-2:s3:path/b/k",
                integrationHttpMethod="POST",
            )
        ex.value.response["Error"]["Code"].should.equal("BadRequestException")
        ex.value.response["Error"]["Message"].should.equal(
            "Role ARN must be specified for AWS integrations"
        )
    for _type in http_types:
        # Ensure that the URI is valid HTTP
        with pytest.raises(ClientError) as ex:
            client.put_integration(
                restApiId=api_id,
                resourceId=root_id,
                httpMethod="GET",
                type=_type,
                uri="non-valid-http",
                integrationHttpMethod="POST",
            )
        ex.value.response["Error"]["Code"].should.equal("BadRequestException")
        ex.value.response["Error"]["Message"].should.equal(
            "Invalid HTTP endpoint specified for URI"
        )
    for _type in aws_types:
        # Ensure that the URI is an ARN
        with pytest.raises(ClientError) as ex:
            client.put_integration(
                restApiId=api_id,
                resourceId=root_id,
                httpMethod="GET",
                type=_type,
                uri="non-valid-arn",
                integrationHttpMethod="POST",
            )
        ex.value.response["Error"]["Code"].should.equal("BadRequestException")
        ex.value.response["Error"]["Message"].should.equal(
            "Invalid ARN specified in the request"
        )
    for _type in aws_types:
        # Ensure that the URI is a valid ARN
        with pytest.raises(ClientError) as ex:
            client.put_integration(
                restApiId=api_id,
                resourceId=root_id,
                httpMethod="GET",
                type=_type,
                uri="arn:aws:iam::0000000000:role/service-role/asdf",
                integrationHttpMethod="POST",
            )
        ex.value.response["Error"]["Code"].should.equal("BadRequestException")
        ex.value.response["Error"]["Message"].should.equal(
            "AWS ARN for integration must contain path or action"
        )


@mock_apigateway
def test_create_domain_names():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = "testDomain"
    test_certificate_name = "test.certificate"
    test_certificate_private_key = "testPrivateKey"
    # success case with valid params
    response = client.create_domain_name(
        domainName=domain_name,
        certificateName=test_certificate_name,
        certificatePrivateKey=test_certificate_private_key,
    )
    response["domainName"].should.equal(domain_name)
    response["certificateName"].should.equal(test_certificate_name)
    # without domain name it should throw BadRequestException
    with pytest.raises(ClientError) as ex:
        client.create_domain_name(domainName="")

    ex.value.response["Error"]["Message"].should.equal("No Domain Name specified")
    ex.value.response["Error"]["Code"].should.equal("BadRequestException")


@mock_apigateway
def test_get_domain_names():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = "testDomain"
    test_certificate_name = "test.certificate"
    response = client.create_domain_name(
        domainName=domain_name, certificateName=test_certificate_name
    )

    response["domainName"].should.equal(domain_name)
    response["certificateName"].should.equal(test_certificate_name)
    response["domainNameStatus"].should.equal("AVAILABLE")
    # after adding a new domain name
    result = client.get_domain_names()
    result["items"][0]["domainName"].should.equal(domain_name)
    result["items"][0]["certificateName"].should.equal(test_certificate_name)
    result["items"][0]["domainNameStatus"].should.equal("AVAILABLE")


@mock_apigateway
def test_get_domain_name():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = "testDomain"
    # adding a domain name
    client.create_domain_name(domainName=domain_name)
    # retrieving the data of added domain name.
    result = client.get_domain_name(domainName=domain_name)
    result["domainName"].should.equal(domain_name)
    result["domainNameStatus"].should.equal("AVAILABLE")


@mock_apigateway
def test_create_model():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    rest_api_id = response["id"]
    dummy_rest_api_id = "a12b3c4d"
    model_name = "testModel"
    description = "test model"
    content_type = "application/json"
    # success case with valid params
    response = client.create_model(
        restApiId=rest_api_id,
        name=model_name,
        description=description,
        contentType=content_type,
    )
    response["name"].should.equal(model_name)
    response["description"].should.equal(description)

    # with an invalid rest_api_id it should throw NotFoundException
    with pytest.raises(ClientError) as ex:
        client.create_model(
            restApiId=dummy_rest_api_id,
            name=model_name,
            description=description,
            contentType=content_type,
        )
    ex.value.response["Error"]["Message"].should.equal("Invalid Rest API Id specified")
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")

    with pytest.raises(ClientError) as ex:
        client.create_model(
            restApiId=rest_api_id,
            name="",
            description=description,
            contentType=content_type,
        )

    ex.value.response["Error"]["Message"].should.equal("No Model Name specified")
    ex.value.response["Error"]["Code"].should.equal("BadRequestException")


@mock_apigateway
def test_get_api_models():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    rest_api_id = response["id"]
    model_name = "testModel"
    description = "test model"
    content_type = "application/json"
    # when no models are present
    result = client.get_models(restApiId=rest_api_id)
    result["items"].should.equal([])
    # add a model
    client.create_model(
        restApiId=rest_api_id,
        name=model_name,
        description=description,
        contentType=content_type,
    )
    # get models after adding
    result = client.get_models(restApiId=rest_api_id)
    result["items"][0]["name"] = model_name
    result["items"][0]["description"] = description


@mock_apigateway
def test_get_model_by_name():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    rest_api_id = response["id"]
    dummy_rest_api_id = "a12b3c4d"
    model_name = "testModel"
    description = "test model"
    content_type = "application/json"
    # add a model
    client.create_model(
        restApiId=rest_api_id,
        name=model_name,
        description=description,
        contentType=content_type,
    )
    # get models after adding
    result = client.get_model(restApiId=rest_api_id, modelName=model_name)
    result["name"] = model_name
    result["description"] = description

    with pytest.raises(ClientError) as ex:
        client.get_model(restApiId=dummy_rest_api_id, modelName=model_name)
    ex.value.response["Error"]["Message"].should.equal("Invalid Rest API Id specified")
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")


@mock_apigateway
def test_get_model_with_invalid_name():
    client = boto3.client("apigateway", region_name="us-west-2")
    response = client.create_rest_api(name="my_api", description="this is my api")
    rest_api_id = response["id"]
    # test with an invalid model name
    with pytest.raises(ClientError) as ex:
        client.get_model(restApiId=rest_api_id, modelName="fake")
    ex.value.response["Error"]["Message"].should.equal("Invalid Model Name specified")
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")


@mock_apigateway
def test_get_integration_response_unknown_response():
    region_name = "us-west-2"
    client = boto3.client("apigateway", region_name=region_name)
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    root_id = create_method_integration(client, api_id)
    client.get_integration_response(
        restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
    )
    with pytest.raises(ClientError) as ex:
        client.get_integration_response(
            restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="300"
        )

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid Response status code specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")


@mock_apigateway
def test_get_domain_name_unknown_domainname():
    client = boto3.client("apigateway", region_name="us-east-1")
    with pytest.raises(ClientError) as ex:
        client.get_domain_name(domainName="www.google.com")

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid domain name identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")


@mock_apigateway
def test_update_domain_name_unknown_domainname():
    client = boto3.client("apigateway", region_name="us-east-1")
    with pytest.raises(ClientError) as ex:
        client.update_domain_name(domainName="www.google.fr", patchOperations=[])

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid domain name identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")


@mock_apigateway
def test_delete_domain_name_unknown_domainname():
    client = boto3.client("apigateway", region_name="us-east-1")
    with pytest.raises(ClientError) as ex:
        client.delete_domain_name(domainName="www.google.com")

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid domain name identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
