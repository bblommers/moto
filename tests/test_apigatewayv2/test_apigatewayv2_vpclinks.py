import boto3
import pytest

from botocore.exceptions import ClientError
from moto import mock_apigatewayv2


@mock_apigatewayv2
def test_create_vpc_links():
    client = boto3.client("apigatewayv2", region_name="eu-west-1")

    resp = client.create_vpc_link(
        Name="vpcl",
        SecurityGroupIds=["sg1", "sg2"],
        SubnetIds=["sid1", "sid2"],
        Tags={"key1": "value1"},
    )

    resp.should.have.key("CreatedDate")
    resp.should.have.key("Name").equals("vpcl")
    resp.should.have.key("SecurityGroupIds").equals(["sg1", "sg2"])
    resp.should.have.key("SubnetIds").equals(["sid1", "sid2"])
    resp.should.have.key("Tags").equals({"key1": "value1"})
    resp.should.have.key("VpcLinkId")
    resp.should.have.key("VpcLinkStatus").equals("AVAILABLE")
    resp.should.have.key("VpcLinkVersion").equals("V2")


@mock_apigatewayv2
def test_get_vpc_link():
    client = boto3.client("apigatewayv2", region_name="eu-west-1")

    vpc_link_id = client.create_vpc_link(
        Name="vpcl",
        SecurityGroupIds=["sg1", "sg2"],
        SubnetIds=["sid1", "sid2"],
        Tags={"key1": "value1"},
    )["VpcLinkId"]

    resp = client.get_vpc_link(VpcLinkId=vpc_link_id)

    resp.should.have.key("CreatedDate")
    resp.should.have.key("Name").equals("vpcl")
    resp.should.have.key("SecurityGroupIds").equals(["sg1", "sg2"])
    resp.should.have.key("SubnetIds").equals(["sid1", "sid2"])
    resp.should.have.key("Tags").equals({"key1": "value1"})
    resp.should.have.key("VpcLinkId")
    resp.should.have.key("VpcLinkStatus").equals("AVAILABLE")
    resp.should.have.key("VpcLinkVersion").equals("V2")


@mock_apigatewayv2
def test_get_vpc_link_unknown():
    client = boto3.client("apigatewayv2", region_name="ap-southeast-1")

    with pytest.raises(ClientError) as exc:
        client.get_vpc_link(VpcLinkId="unknown")
    err = exc.value.response["Error"]
    err["Code"].should.equal("NotFoundException")
    err["Message"].should.equal("Invalid VpcLink identifier specified unknown")


@mock_apigatewayv2
def test_get_vpc_links():
    client = boto3.client("apigatewayv2", region_name="eu-west-1")

    vpc_link_id = client.create_vpc_link(
        Name="vpcl",
        SecurityGroupIds=["sg1", "sg2"],
        SubnetIds=["sid1", "sid2"],
        Tags={"key1": "value1"},
    )["VpcLinkId"]

    links = client.get_vpc_links()["Items"]
    [link["VpcLinkId"] for link in links].should.contain(vpc_link_id)

    vpc_link_id2 = client.create_vpc_link(
        Name="vpcl",
        SecurityGroupIds=["sg1", "sg2"],
        SubnetIds=["sid1", "sid2"],
        Tags={"key1": "value1"},
    )["VpcLinkId"]
    vpc_link_id.shouldnt.equal(vpc_link_id2)

    links = client.get_vpc_links()["Items"]
    [link["VpcLinkId"] for link in links].should.contain(vpc_link_id)
    [link["VpcLinkId"] for link in links].should.contain(vpc_link_id2)


@mock_apigatewayv2
def test_delete_vpc_link():
    client = boto3.client("apigatewayv2", region_name="eu-north-1")

    vpc_link_id = client.create_vpc_link(
        Name="vpcl",
        SecurityGroupIds=["sg1", "sg2"],
        SubnetIds=["sid1", "sid2"],
        Tags={"key1": "value1"},
    )["VpcLinkId"]

    client.delete_vpc_link(VpcLinkId=vpc_link_id)

    links = client.get_vpc_links()["Items"]
    [link["VpcLinkId"] for link in links].shouldnt.contain(vpc_link_id)


@mock_apigatewayv2
def test_update_vpc_link():
    client = boto3.client("apigatewayv2", region_name="eu-north-1")
    vpc_link_id = client.create_vpc_link(
        Name="vpcl",
        SecurityGroupIds=["sg1", "sg2"],
        SubnetIds=["sid1", "sid2"],
        Tags={"key1": "value1"},
    )["VpcLinkId"]

    resp = client.update_vpc_link(VpcLinkId=vpc_link_id, Name="vpcl2")

    resp.should.have.key("CreatedDate")
    resp.should.have.key("Name").equals("vpcl2")
    resp.should.have.key("SecurityGroupIds").equals(["sg1", "sg2"])
    resp.should.have.key("SubnetIds").equals(["sid1", "sid2"])
    resp.should.have.key("Tags").equals({"key1": "value1"})
    resp.should.have.key("VpcLinkId")
    resp.should.have.key("VpcLinkStatus").equals("AVAILABLE")
    resp.should.have.key("VpcLinkVersion").equals("V2")


@mock_apigatewayv2
def test_untag_vpc_link():
    client = boto3.client("apigatewayv2", region_name="eu-west-1")

    vpc_link_id = client.create_vpc_link(
        Name="vpcl",
        SecurityGroupIds=["sg1", "sg2"],
        SubnetIds=["sid1", "sid2"],
        Tags={"Key1": "value1", "key2": "val2"},
    )["VpcLinkId"]

    arn = f"arn:aws:apigateway:eu-west-1::/vpclinks/{vpc_link_id}"
    client.untag_resource(ResourceArn=arn, TagKeys=["Key1"])

    resp = client.get_vpc_link(VpcLinkId=vpc_link_id)

    resp.should.have.key("CreatedDate")
    resp.should.have.key("Name").equals("vpcl")
    resp.should.have.key("SecurityGroupIds").equals(["sg1", "sg2"])
    resp.should.have.key("SubnetIds").equals(["sid1", "sid2"])
    resp.should.have.key("Tags").equals({"key2": "val2"})
    resp.should.have.key("VpcLinkId")
    resp.should.have.key("VpcLinkStatus").equals("AVAILABLE")
    resp.should.have.key("VpcLinkVersion").equals("V2")
