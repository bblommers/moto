from botocore.exceptions import ClientError
import pytest
import boto3
import sure  # noqa # pylint: disable=unused-import

from moto import mock_athena
from uuid import uuid4


@mock_athena
def test_create_work_group():
    client = boto3.client("athena", region_name="us-east-1")

    name = str(uuid4())
    client.create_work_group(
        Name=name,
        Description="Test work group",
        Configuration={
            "ResultConfiguration": {
                "OutputLocation": "s3://bucket-name/prefix/",
                "EncryptionConfiguration": {
                    "EncryptionOption": "SSE_KMS",
                    "KmsKey": "aws:arn:kms:1233456789:us-east-1:key/number-1",
                },
            }
        },
        Tags=[],
    )

    with pytest.raises(ClientError) as exc:
        # The second time should throw an error
        client.create_work_group(
            Name=name,
            Description="duplicate",
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": "s3://bucket-name/prefix/",
                    "EncryptionConfiguration": {
                        "EncryptionOption": "SSE_KMS",
                        "KmsKey": "aws:arn:kms:1233456789:us-east-1:key/number-1",
                    },
                }
            },
        )
    err = exc.value.response["Error"]
    err["Code"].should.equal("InvalidRequestException")
    err["Message"].should.equal("WorkGroup already exists")

    # Then test the work group appears in the work group list
    groups = client.list_work_groups()["WorkGroups"]
    ours = [g for g in groups if g["Name"] == name]

    ours.should.have.length_of(1)
    work_group = ours[0]
    work_group["Name"].should.equal(name)
    work_group["Description"].should.equal("Test work group")
    work_group["State"].should.equal("ENABLED")


@mock_athena
def test_create_and_get_workgroup():
    client = boto3.client("athena", region_name="us-east-1")

    name = str(uuid4())
    create_basic_workgroup(client=client, name=name)

    work_group = client.get_work_group(WorkGroup=name)["WorkGroup"]
    del work_group["CreationTime"]  # Were not testing creationtime atm
    work_group.should.equal(
        {
            "Name": name,
            "State": "ENABLED",
            "Configuration": {
                "ResultConfiguration": {"OutputLocation": "s3://bucket-name/prefix/"}
            },
            "Description": "Test work group",
        }
    )


@mock_athena
def test_start_query_execution():
    client = boto3.client("athena", region_name="us-east-1")

    name = str(uuid4())
    create_basic_workgroup(client=client, name=name)
    response = client.start_query_execution(
        QueryString="query1",
        QueryExecutionContext={"Database": "string"},
        ResultConfiguration={"OutputLocation": "string"},
        WorkGroup=name,
    )
    assert "QueryExecutionId" in response

    sec_response = client.start_query_execution(
        QueryString="query2",
        QueryExecutionContext={"Database": "string"},
        ResultConfiguration={"OutputLocation": "string"},
    )
    assert "QueryExecutionId" in sec_response
    response["QueryExecutionId"].shouldnt.equal(sec_response["QueryExecutionId"])


@mock_athena
def test_start_query_validate_workgroup():
    client = boto3.client("athena", region_name="us-east-1")

    with pytest.raises(ClientError) as err:
        client.start_query_execution(
            QueryString="query1",
            QueryExecutionContext={"Database": "string"},
            ResultConfiguration={"OutputLocation": "string"},
            WorkGroup="unknown_workgroup",
        )
    err.value.response["Error"]["Code"].should.equal("InvalidRequestException")
    err.value.response["Error"]["Message"].should.equal("WorkGroup does not exist")


@mock_athena
def test_get_query_execution():
    client = boto3.client("athena", region_name="us-east-1")

    query = "SELECT stuff"
    location = "s3://bucket-name/prefix/"
    database = "database"
    # Start Query
    exex_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": location},
    )["QueryExecutionId"]
    #
    details = client.get_query_execution(QueryExecutionId=exex_id)["QueryExecution"]
    #
    details["QueryExecutionId"].should.equal(exex_id)
    details["Query"].should.equal(query)
    details["StatementType"].should.equal("DDL")
    details["ResultConfiguration"]["OutputLocation"].should.equal(location)
    details["QueryExecutionContext"]["Database"].should.equal(database)
    details["Status"]["State"].should.equal("QUEUED")
    details["Statistics"].should.equal(
        {
            "EngineExecutionTimeInMillis": 0,
            "DataScannedInBytes": 0,
            "TotalExecutionTimeInMillis": 0,
            "QueryQueueTimeInMillis": 0,
            "QueryPlanningTimeInMillis": 0,
            "ServiceProcessingTimeInMillis": 0,
        }
    )
    assert "WorkGroup" not in details


@mock_athena
def test_stop_query_execution():
    client = boto3.client("athena", region_name="us-east-1")

    query = "SELECT stuff"
    location = "s3://bucket-name/prefix/"
    database = "database"
    # Start Query
    exex_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": location},
    )["QueryExecutionId"]
    # Stop Query
    client.stop_query_execution(QueryExecutionId=exex_id)
    # Verify status
    details = client.get_query_execution(QueryExecutionId=exex_id)["QueryExecution"]
    #
    details["QueryExecutionId"].should.equal(exex_id)
    details["Status"]["State"].should.equal("CANCELLED")


@mock_athena
def test_create_named_query():
    client = boto3.client("athena", region_name="us-east-1")

    # craete named query
    res = client.create_named_query(
        Name="query-name", Database="target_db", QueryString="SELECT * FROM table1"
    )

    assert "NamedQueryId" in res


@mock_athena
def test_get_named_query():
    client = boto3.client("athena", region_name="us-east-1")
    query_name = "query-name"
    database = "target_db"
    query_string = "SELECT * FROM tbl1"
    description = "description of this query"
    # craete named query
    res_create = client.create_named_query(
        Name=query_name,
        Database=database,
        QueryString=query_string,
        Description=description,
    )
    query_id = res_create["NamedQueryId"]

    # get named query
    res_get = client.get_named_query(NamedQueryId=query_id)["NamedQuery"]
    res_get["Name"].should.equal(query_name)
    res_get["Description"].should.equal(description)
    res_get["Database"].should.equal(database)
    res_get["QueryString"].should.equal(query_string)
    res_get["NamedQueryId"].should.equal(query_id)


def create_basic_workgroup(client, name):
    client.create_work_group(
        Name=name,
        Description="Test work group",
        Configuration={
            "ResultConfiguration": {"OutputLocation": "s3://bucket-name/prefix/"}
        },
    )


@mock_athena
def test_create_data_catalog():
    client = boto3.client("athena", region_name="us-east-1")
    name = str(uuid4())
    client.create_data_catalog(
        Name=name,
        Type="GLUE",
        Description="Test data catalog",
        Parameters={"catalog-id": "AWS Test account ID"},
        Tags=[],
    )

    with pytest.raises(ClientError) as exc:
        # The second time should throw an error
        client.create_data_catalog(
            Name=name,
            Type="GLUE",
            Description="Test data catalog",
            Parameters={"catalog-id": "AWS Test account ID"},
            Tags=[],
        )
    err = exc.value.response["Error"]
    err["Code"].should.equal("InvalidRequestException")
    err["Message"].should.equal("DataCatalog already exists")

    # Then test the work group appears in the work group list
    catalogs = client.list_data_catalogs()["DataCatalogsSummary"]
    names = [c["CatalogName"] for c in catalogs]
    names.should.contain(name)


@mock_athena
def test_create_and_get_data_catalog():
    client = boto3.client("athena", region_name="us-east-1")

    name = str(uuid4())
    client.create_data_catalog(
        Name=name,
        Type="GLUE",
        Description="Test data catalog",
        Parameters={"catalog-id": "AWS Test account ID"},
        Tags=[],
    )

    data_catalog = client.get_data_catalog(Name=name)
    data_catalog["DataCatalog"].should.equal(
        {
            "Name": name,
            "Description": "Test data catalog",
            "Type": "GLUE",
            "Parameters": {"catalog-id": "AWS Test account ID"},
        }
    )
