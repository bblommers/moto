import pytest
from botocore.exceptions import ClientError

from moto import mock_aws

from .test_elbv2 import create_load_balancer

create_condition = {"Field": "host-header", "Values": ["example.com"]}
default_action = {
    "FixedResponseConfig": {"StatusCode": "200", "ContentType": "text/plain"},
    "Type": "fixed-response",
}


@mock_aws
def test_create_listener_rule_with_tags():
    response, _, _, _, _, elbv2 = create_load_balancer()

    load_balancer_arn = response.get("LoadBalancers")[0].get("LoadBalancerArn")

    listener_arn = elbv2.create_listener(
        LoadBalancerArn=load_balancer_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[],
    )["Listeners"][0]["ListenerArn"]
    rule_arn = elbv2.create_rule(
        ListenerArn=listener_arn,
        Priority=100,
        Conditions=[create_condition],
        Actions=[default_action],
        Tags=[{"Key": "k1", "Value": "v1"}],
    )["Rules"][0]["RuleArn"]

    # Ensure the tags persisted
    response = elbv2.describe_tags(ResourceArns=[rule_arn])
    tags = {d["Key"]: d["Value"] for d in response["TagDescriptions"][0]["Tags"]}
    assert tags == {"k1": "v1"}


@mock_aws
def test_listener_rule_add_remove_tags():
    response, _, _, _, _, elbv2 = create_load_balancer()

    load_balancer_arn = response.get("LoadBalancers")[0].get("LoadBalancerArn")

    listener_arn = elbv2.create_listener(
        LoadBalancerArn=load_balancer_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[],
        Tags=[{"Key": "k1", "Value": "v1"}],
    )["Listeners"][0]["ListenerArn"]
    rule_arn = elbv2.create_rule(
        ListenerArn=listener_arn,
        Priority=100,
        Conditions=[create_condition],
        Actions=[default_action],
    )["Rules"][0]["RuleArn"]

    elbv2.add_tags(ResourceArns=[rule_arn], Tags=[{"Key": "a", "Value": "b"}])

    tags = elbv2.describe_tags(ResourceArns=[rule_arn])["TagDescriptions"][0]["Tags"]
    assert tags == [{"Key": "a", "Value": "b"}]

    elbv2.add_tags(
        ResourceArns=[rule_arn],
        Tags=[
            {"Key": "a", "Value": "b"},
            {"Key": "b", "Value": "b"},
            {"Key": "c", "Value": "b"},
        ],
    )

    tags = elbv2.describe_tags(ResourceArns=[rule_arn])["TagDescriptions"][0]["Tags"]
    assert tags == [
        {"Key": "a", "Value": "b"},
        {"Key": "b", "Value": "b"},
        {"Key": "c", "Value": "b"},
    ]

    elbv2.remove_tags(ResourceArns=[rule_arn], TagKeys=["a"])

    tags = elbv2.describe_tags(ResourceArns=[rule_arn])["TagDescriptions"][0]["Tags"]
    assert tags == [
        {"Key": "b", "Value": "b"},
        {"Key": "c", "Value": "b"},
    ]


@mock_aws
def test_remove_tags_to_invalid_listener_rule():
    response, _, _, _, _, elbv2 = create_load_balancer()
    load_balancer_arn = response.get("LoadBalancers")[0].get("LoadBalancerArn")

    listener_arn = elbv2.create_listener(
        LoadBalancerArn=load_balancer_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[],
    )["Listeners"][0]["ListenerArn"]
    rule_arn = elbv2.create_rule(
        ListenerArn=listener_arn,
        Priority=100,
        Conditions=[create_condition],
        Actions=[default_action],
        Tags=[{"Key": "k1", "Value": "v1"}],
    )["Rules"][0]["RuleArn"]

    # add a random string in the ARN to make the resource non-existent
    BAD_ARN = rule_arn + "randostring"

    # test for exceptions on remove tags
    with pytest.raises(ClientError) as err:
        elbv2.remove_tags(ResourceArns=[BAD_ARN], TagKeys=["a"])

    assert err.value.response["Error"]["Code"] == "RuleNotFound"
    assert (
        err.value.response["Error"]["Message"] == "The specified rule does not exist."
    )
