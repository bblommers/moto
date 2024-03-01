from random import randint
from uuid import uuid4

import boto3

from moto import mock_aws


@mock_aws
def test_security_group_egress_tags():
    ec2 = boto3.resource("ec2", region_name="us-east-1")
    vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")

    client = boto3.client("ec2", region_name="us-east-1")
    sg_name = str(uuid4())[0:6]
    sg = ec2.create_security_group(GroupName=sg_name, Description="TSG", VpcId=vpc.id)

    ip_permissions = [
        {
            "IpProtocol": "tcp",
            "FromPort": (randint(0, 65535)),
            "ToPort": (randint(0, 65535)),
            "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Ip test"}],
        },
    ]

    tag_val = str(uuid4())
    rule = client.authorize_security_group_egress(
        GroupId=sg.id,
        IpPermissions=ip_permissions,
        TagSpecifications=[
            {
                "ResourceType": "security-group-rule",
                "Tags": [
                    {"Key": "Automation", "Value": "Lambda"},
                    {"Key": "Partner", "Value": tag_val},
                ],
            }
        ],
    )["SecurityGroupRules"][0]
    rule_id = rule["SecurityGroupRuleId"]
    assert rule["Tags"] == [
        {"Key": "Automation", "Value": "Lambda"},
        {"Key": "Partner", "Value": tag_val},
    ]

    rules = client.describe_security_group_rules(
        Filters=[{"Name": "tag:Partner", "Values": ["unknown"]}]
    )
    assert len(rules["SecurityGroupRules"]) == 0

    rules = client.describe_security_group_rules(
        Filters=[{"Name": "tag:Partner", "Values": [tag_val]}]
    )
    assert len(rules["SecurityGroupRules"]) == 1
    assert rules["SecurityGroupRules"][0]["SecurityGroupRuleId"] == rule_id

    rules = client.describe_security_group_rules(
        Filters=[{"Name": "security-group-rule-id", "Values": [rule_id]}]
    )
    assert len(rules["SecurityGroupRules"]) == 1
    assert rules["SecurityGroupRules"][0]["SecurityGroupRuleId"] == rule_id


@mock_aws
def test_security_group_ingress_tags():
    ec2 = boto3.resource("ec2", region_name="us-east-1")
    vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")

    client = boto3.client("ec2", region_name="us-east-1")
    sg_name = str(uuid4())[0:6]
    sg = ec2.create_security_group(GroupName=sg_name, Description="TSG", VpcId=vpc.id)

    ip_permissions = [
        {
            "IpProtocol": "tcp",
            "FromPort": (randint(0, 65535)),
            "ToPort": (randint(0, 65535)),
            "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Ip test"}],
        },
    ]

    tag_val = str(uuid4())
    rule = client.authorize_security_group_ingress(
        GroupId=sg.id,
        IpPermissions=ip_permissions,
        TagSpecifications=[
            {
                "ResourceType": "security-group-rule",
                "Tags": [
                    {"Key": "Automation", "Value": "Lambda"},
                    {"Key": "Partner", "Value": tag_val},
                ],
            }
        ],
    )["SecurityGroupRules"][0]
    rule_id = rule["SecurityGroupRuleId"]
    assert rule["Tags"] == [
        {"Key": "Automation", "Value": "Lambda"},
        {"Key": "Partner", "Value": tag_val},
    ]

    rules = client.describe_security_group_rules(
        Filters=[{"Name": "tag:Partner", "Values": ["unknown"]}]
    )
    assert len(rules["SecurityGroupRules"]) == 0

    rules = client.describe_security_group_rules(
        Filters=[{"Name": "tag:Partner", "Values": [tag_val]}]
    )
    assert len(rules["SecurityGroupRules"]) == 1
    assert rules["SecurityGroupRules"][0]["SecurityGroupRuleId"] == rule_id

    rules = client.describe_security_group_rules(
        Filters=[{"Name": "security-group-rule-id", "Values": [rule_id]}]
    )
    assert len(rules["SecurityGroupRules"]) == 1
    assert rules["SecurityGroupRules"][0]["SecurityGroupRuleId"] == rule_id
