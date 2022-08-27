import boto3
import sure  # noqa # pylint: disable=unused-import
import pytest

from botocore.exceptions import ClientError
from moto import mock_apigateway
from uuid import uuid4
from . import create_method_integration


@mock_apigateway
def test_create_base_path_mapping():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    client.create_domain_name(
        domainName=domain_name,
        certificateName="test.certificate",
        certificatePrivateKey="testPrivateKey",
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    stage_name = "dev"
    create_method_integration(client, api_id)
    client.create_deployment(
        restApiId=api_id, stageName=stage_name, description="1.0.1"
    )

    response = client.create_base_path_mapping(domainName=domain_name, restApiId=api_id)

    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(201)
    response["basePath"].should.equal("(none)")
    response["restApiId"].should.equal(api_id)
    response.should_not.have.key("stage")

    response = client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id, stage=stage_name
    )
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(201)
    response["basePath"].should.equal("(none)")
    response["restApiId"].should.equal(api_id)
    response["stage"].should.equal(stage_name)

    response = client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id, stage=stage_name, basePath="v1"
    )
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(201)
    response["basePath"].should.equal("v1")
    response["restApiId"].should.equal(api_id)
    response["stage"].should.equal(stage_name)


@mock_apigateway
def test_create_base_path_mapping_with_unknown_api():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    client.create_domain_name(
        domainName=domain_name,
        certificateName="test.certificate",
        certificatePrivateKey="testPrivateKey",
    )

    with pytest.raises(ClientError) as ex:
        client.create_base_path_mapping(
            domainName=domain_name, restApiId="none-exists-api"
        )

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid REST API identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("BadRequestException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)


@mock_apigateway
def test_create_base_path_mapping_with_invalid_base_path():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    client.create_domain_name(
        domainName=domain_name,
        certificateName="test.certificate",
        certificatePrivateKey="testPrivateKey",
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    stage_name = "dev"
    create_method_integration(client, api_id)
    client.create_deployment(
        restApiId=api_id, stageName=stage_name, description="1.0.1"
    )

    with pytest.raises(ClientError) as ex:
        client.create_base_path_mapping(
            domainName=domain_name, restApiId=api_id, basePath="/v1"
        )

    ex.value.response["Error"]["Message"].should.equal(
        "API Gateway V1 doesn't support the slash character (/) in base path mappings. "
        "To create a multi-level base path mapping, use API Gateway V2."
    )
    ex.value.response["Error"]["Code"].should.equal("BadRequestException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)


@mock_apigateway
def test_create_base_path_mapping_with_unknown_stage():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    client.create_domain_name(
        domainName=domain_name,
        certificateName="test.certificate",
        certificatePrivateKey="testPrivateKey",
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    stage_name = "dev"
    create_method_integration(client, api_id)
    client.create_deployment(
        restApiId=api_id, stageName=stage_name, description="1.0.1"
    )

    with pytest.raises(ClientError) as ex:
        client.create_base_path_mapping(
            domainName=domain_name, restApiId=api_id, stage="unknown-stage"
        )

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid stage identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("BadRequestException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)


@mock_apigateway
def test_create_base_path_mapping_with_duplicate_base_path():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    client.create_domain_name(
        domainName=domain_name,
        certificateName="test.certificate",
        certificatePrivateKey="testPrivateKey",
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    base_path = "v1"
    client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id, basePath=base_path
    )
    with pytest.raises(ClientError) as ex:
        client.create_base_path_mapping(
            domainName=domain_name, restApiId=api_id, basePath=base_path
        )

    ex.value.response["Error"]["Message"].should.equal(
        "Base path already exists for this domain name"
    )
    ex.value.response["Error"]["Code"].should.equal("ConflictException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(409)


@mock_apigateway
def test_get_base_path_mappings():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    test_certificate_name = "test.certificate"
    client.create_domain_name(
        domainName=domain_name, certificateName=test_certificate_name
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    stage_name = "dev"
    create_method_integration(client, api_id)
    client.create_deployment(
        restApiId=api_id, stageName=stage_name, description="1.0.1"
    )

    client.create_base_path_mapping(domainName=domain_name, restApiId=api_id)
    client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id, basePath="v1"
    )
    client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id, basePath="v2", stage=stage_name
    )

    response = client.get_base_path_mappings(domainName=domain_name)
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(200)

    items = response["items"]

    items[0]["basePath"].should.equal("(none)")
    items[0]["restApiId"].should.equal(api_id)
    items[0].should_not.have.key("stage")

    items[1]["basePath"].should.equal("v1")
    items[1]["restApiId"].should.equal(api_id)
    items[1].should_not.have.key("stage")

    items[2]["basePath"].should.equal("v2")
    items[2]["restApiId"].should.equal(api_id)
    items[2]["stage"].should.equal(stage_name)


@mock_apigateway
def test_get_base_path_mappings_with_unknown_domain():
    client = boto3.client("apigateway", region_name="us-west-2")

    with pytest.raises(ClientError) as ex:
        client.get_base_path_mappings(domainName="unknown-domain")

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid domain name identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)


@mock_apigateway
def test_get_base_path_mapping():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    test_certificate_name = "test.certificate"
    client.create_domain_name(
        domainName=domain_name, certificateName=test_certificate_name
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    stage_name = "dev"
    create_method_integration(client, api_id)
    client.create_deployment(
        restApiId=api_id, stageName=stage_name, description="1.0.1"
    )

    client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id, stage=stage_name
    )

    response = client.get_base_path_mapping(domainName=domain_name, basePath="(none)")
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(200)
    response["basePath"].should.equal("(none)")
    response["restApiId"].should.equal(api_id)
    response["stage"].should.equal(stage_name)


@mock_apigateway
def test_get_base_path_mapping_with_unknown_domain():
    client = boto3.client("apigateway", region_name="us-west-2")

    with pytest.raises(ClientError) as ex:
        client.get_base_path_mapping(domainName="unknown-domain", basePath="v1")

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid domain name identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)


@mock_apigateway
def test_get_base_path_mapping_with_unknown_base_path():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    test_certificate_name = "test.certificate"
    client.create_domain_name(
        domainName=domain_name, certificateName=test_certificate_name
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id, basePath="v1"
    )

    with pytest.raises(ClientError) as ex:
        client.get_base_path_mapping(domainName=domain_name, basePath="unknown")

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid base path mapping identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)


@mock_apigateway
def test_delete_base_path_mapping():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    test_certificate_name = "test.certificate"
    client.create_domain_name(
        domainName=domain_name, certificateName=test_certificate_name
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    base_path = "v1"
    client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id, basePath=base_path
    )

    client.get_base_path_mapping(domainName=domain_name, basePath=base_path)
    response = client.delete_base_path_mapping(
        domainName=domain_name, basePath=base_path
    )
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(202)

    with pytest.raises(ClientError) as ex:
        client.get_base_path_mapping(domainName=domain_name, basePath=base_path)

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid base path mapping identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)


@mock_apigateway
def test_delete_base_path_mapping_with_unknown_domain():
    client = boto3.client("apigateway", region_name="us-west-2")

    with pytest.raises(ClientError) as ex:
        client.delete_base_path_mapping(domainName="unknown-domain", basePath="v1")

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid domain name identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)


@mock_apigateway
def test_delete_base_path_mapping_with_unknown_base_path():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    test_certificate_name = "test.certificate"
    client.create_domain_name(
        domainName=domain_name, certificateName=test_certificate_name
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id, basePath="v1"
    )

    with pytest.raises(ClientError) as ex:
        client.delete_base_path_mapping(domainName=domain_name, basePath="unknown")

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid base path mapping identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)


@mock_apigateway
def test_update_path_mapping():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    test_certificate_name = "test.certificate"
    client.create_domain_name(
        domainName=domain_name, certificateName=test_certificate_name
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]

    stage_name = "dev"

    client.create_base_path_mapping(domainName=domain_name, restApiId=api_id)

    response = client.create_rest_api(
        name="new_my_api", description="this is new my api"
    )
    new_api_id = response["id"]
    create_method_integration(client, new_api_id)
    client.create_deployment(
        restApiId=new_api_id, stageName=stage_name, description="1.0.1"
    )

    base_path = "v1"
    patch_operations = [
        {"op": "replace", "path": "/stage", "value": stage_name},
        {"op": "replace", "path": "/basePath", "value": base_path},
        {"op": "replace", "path": "/restapiId", "value": new_api_id},
    ]
    response = client.update_base_path_mapping(
        domainName=domain_name, basePath="(none)", patchOperations=patch_operations
    )
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(200)
    response["basePath"].should.equal(base_path)
    response["restApiId"].should.equal(new_api_id)
    response["stage"].should.equal(stage_name)


@mock_apigateway
def test_update_path_mapping_with_unknown_domain():

    client = boto3.client("apigateway", region_name="us-west-2")
    with pytest.raises(ClientError) as ex:
        client.update_base_path_mapping(
            domainName="unknown-domain", basePath="(none)", patchOperations=[]
        )

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid domain name identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)


@mock_apigateway
def test_update_path_mapping_with_unknown_base_path():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    test_certificate_name = "test.certificate"
    client.create_domain_name(
        domainName=domain_name, certificateName=test_certificate_name
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id, basePath="v1"
    )

    with pytest.raises(ClientError) as ex:
        client.update_base_path_mapping(
            domainName=domain_name, basePath="unknown", patchOperations=[]
        )

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid base path mapping identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("NotFoundException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)


@mock_apigateway
def test_update_path_mapping_to_same_base_path():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    test_certificate_name = "test.certificate"
    client.create_domain_name(
        domainName=domain_name, certificateName=test_certificate_name
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id_1 = response["id"]
    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id_2 = response["id"]

    client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id_1, basePath="v1"
    )
    client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id_2, basePath="v2"
    )

    response = client.get_base_path_mappings(domainName=domain_name)
    items = response["items"]
    len(items).should.equal(2)

    patch_operations = [
        {"op": "replace", "path": "/basePath", "value": "v2"},
    ]
    response = client.update_base_path_mapping(
        domainName=domain_name, basePath="v1", patchOperations=patch_operations
    )
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(200)
    response["basePath"].should.equal("v2")
    response["restApiId"].should.equal(api_id_1)

    response = client.get_base_path_mappings(domainName=domain_name)
    items = response["items"]
    len(items).should.equal(1)
    items[0]["basePath"].should.equal("v2")
    items[0]["restApiId"].should.equal(api_id_1)
    items[0].should_not.have.key("stage")


@mock_apigateway
def test_update_path_mapping_with_unknown_api():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    test_certificate_name = "test.certificate"
    client.create_domain_name(
        domainName=domain_name, certificateName=test_certificate_name
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    base_path = "v1"
    client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id, basePath=base_path
    )

    with pytest.raises(ClientError) as ex:
        client.update_base_path_mapping(
            domainName=domain_name,
            basePath=base_path,
            patchOperations=[
                {"op": "replace", "path": "/restapiId", "value": "unknown"},
            ],
        )

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid REST API identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("BadRequestException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)


@mock_apigateway
def test_update_path_mapping_with_unknown_stage():
    client = boto3.client("apigateway", region_name="us-west-2")
    domain_name = str(uuid4())
    test_certificate_name = "test.certificate"
    client.create_domain_name(
        domainName=domain_name, certificateName=test_certificate_name
    )

    response = client.create_rest_api(name="my_api", description="this is my api")
    api_id = response["id"]
    base_path = "v1"
    client.create_base_path_mapping(
        domainName=domain_name, restApiId=api_id, basePath=base_path
    )

    with pytest.raises(ClientError) as ex:
        client.update_base_path_mapping(
            domainName=domain_name,
            basePath=base_path,
            patchOperations=[{"op": "replace", "path": "/stage", "value": "unknown"}],
        )

    ex.value.response["Error"]["Message"].should.equal(
        "Invalid stage identifier specified"
    )
    ex.value.response["Error"]["Code"].should.equal("BadRequestException")
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
