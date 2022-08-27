import boto3

from moto import mock_autoscaling, mock_ec2

from . import get_all_tags
from .utils import setup_networking
from tests import EXAMPLE_AMI_ID
from uuid import uuid4


@mock_autoscaling
def test_autoscaling_tags_update():
    lc_name = str(uuid4())
    asg_name = str(uuid4())
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    _ = client.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    _ = client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchConfigurationName=lc_name,
        MinSize=0,
        MaxSize=20,
        DesiredCapacity=5,
        Tags=[
            {
                "ResourceId": "test_asg",
                "Key": "test_key",
                "Value": "test_value",
                "PropagateAtLaunch": True,
            }
        ],
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    client.create_or_update_tags(
        Tags=[
            {
                "ResourceId": asg_name,
                "Key": "test_key",
                "Value": "updated_test_value",
                "PropagateAtLaunch": True,
            },
            {
                "ResourceId": asg_name,
                "Key": "test_key2",
                "Value": "test_value2",
                "PropagateAtLaunch": False,
            },
        ]
    )

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    response["AutoScalingGroups"][0]["Tags"].should.have.length_of(2)


@mock_autoscaling
@mock_ec2
def test_delete_tags_by_key():
    lc_name = str(uuid4())
    asg_name = str(uuid4())
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    tag_to_delete = {
        "ResourceId": asg_name,
        "ResourceType": "auto-scaling-group",
        "PropagateAtLaunch": True,
        "Key": "TestDeleteTagKey1",
        "Value": "TestTagValue1",
    }
    tag_to_keep = {
        "ResourceId": asg_name,
        "ResourceType": "auto-scaling-group",
        "PropagateAtLaunch": True,
        "Key": "TestTagKey1",
        "Value": "TestTagValue1",
    }
    client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        MinSize=1,
        MaxSize=2,
        LaunchConfigurationName=lc_name,
        Tags=[tag_to_delete, tag_to_keep],
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    client.delete_tags(
        Tags=[
            {
                "ResourceId": asg_name,
                "ResourceType": "auto-scaling-group",
                "PropagateAtLaunch": True,
                "Key": "TestDeleteTagKey1",
            }
        ]
    )
    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    group = response["AutoScalingGroups"][0]
    tags = group["Tags"]
    tags.should.contain(tag_to_keep)
    tags.should_not.contain(tag_to_delete)


@mock_autoscaling
def test_describe_tags_no_filter():
    subnet = setup_networking()["subnet1"]
    client = boto3.client("autoscaling", region_name="us-east-1")
    asg1, asg2 = create_asgs(client, subnet)

    response = get_all_tags(client)
    response.should.contain(
        {
            "ResourceId": asg1,
            "ResourceType": "auto-scaling-group",
            "Key": "test_key",
            "Value": "updated_test_value",
            "PropagateAtLaunch": True,
        }
    )
    response.should.contain(
        {
            "ResourceId": asg1,
            "ResourceType": "auto-scaling-group",
            "Key": "test_key2",
            "Value": "test_value2",
            "PropagateAtLaunch": False,
        }
    )
    response.should.contain(
        {
            "ResourceId": asg2,
            "ResourceType": "auto-scaling-group",
            "Key": "asg2tag1",
            "Value": "val",
            "PropagateAtLaunch": False,
        }
    )
    response.should.contain(
        {
            "ResourceId": asg2,
            "ResourceType": "auto-scaling-group",
            "Key": "asg2tag2",
            "Value": "diff",
            "PropagateAtLaunch": False,
        }
    )


@mock_autoscaling
def test_describe_tags_filter_by_name():
    subnet = setup_networking()["subnet1"]
    client = boto3.client("autoscaling", region_name="us-east-1")
    asg1, asg2 = create_asgs(client, subnet)

    response = client.describe_tags(
        Filters=[{"Name": "auto-scaling-group", "Values": [asg1]}]
    )
    response.should.have.key("Tags").length_of(2)
    response["Tags"].should.contain(
        {
            "ResourceId": asg1,
            "ResourceType": "auto-scaling-group",
            "Key": "test_key",
            "Value": "updated_test_value",
            "PropagateAtLaunch": True,
        }
    )
    response["Tags"].should.contain(
        {
            "ResourceId": asg1,
            "ResourceType": "auto-scaling-group",
            "Key": "test_key2",
            "Value": "test_value2",
            "PropagateAtLaunch": False,
        }
    )

    response = client.describe_tags(
        Filters=[{"Name": "auto-scaling-group", "Values": [asg1, asg2]}]
    )
    response.should.have.key("Tags").length_of(4)
    response["Tags"].should.contain(
        {
            "ResourceId": asg1,
            "ResourceType": "auto-scaling-group",
            "Key": "test_key",
            "Value": "updated_test_value",
            "PropagateAtLaunch": True,
        }
    )
    response["Tags"].should.contain(
        {
            "ResourceId": asg1,
            "ResourceType": "auto-scaling-group",
            "Key": "test_key2",
            "Value": "test_value2",
            "PropagateAtLaunch": False,
        }
    )
    response["Tags"].should.contain(
        {
            "ResourceId": asg2,
            "ResourceType": "auto-scaling-group",
            "Key": "asg2tag1",
            "Value": "val",
            "PropagateAtLaunch": False,
        }
    )
    response["Tags"].should.contain(
        {
            "ResourceId": asg2,
            "ResourceType": "auto-scaling-group",
            "Key": "asg2tag2",
            "Value": "diff",
            "PropagateAtLaunch": False,
        }
    )


@mock_autoscaling
def test_describe_tags_filter_by_propgateatlaunch():
    subnet = setup_networking()["subnet1"]
    client = boto3.client("autoscaling", region_name="us-east-1")
    asg1, _ = create_asgs(client, subnet)

    response = client.describe_tags(
        Filters=[{"Name": "propagate-at-launch", "Values": ["True"]}]
    )
    response["Tags"].should.contain(
        {
            "ResourceId": asg1,
            "ResourceType": "auto-scaling-group",
            "Key": "test_key",
            "Value": "updated_test_value",
            "PropagateAtLaunch": True,
        }
    )


def create_asgs(client, subnet):
    asg1 = str(uuid4())
    asg2 = str(uuid4())
    lc_name = str(uuid4())
    _ = client.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    client.create_auto_scaling_group(
        AutoScalingGroupName=asg1,
        LaunchConfigurationName=lc_name,
        MinSize=0,
        MaxSize=20,
        DesiredCapacity=5,
        VPCZoneIdentifier=subnet,
    )
    client.create_auto_scaling_group(
        AutoScalingGroupName=asg2,
        LaunchConfigurationName=lc_name,
        MinSize=0,
        MaxSize=20,
        DesiredCapacity=5,
        Tags=[
            {"Key": "asg2tag1", "Value": "val"},
            {"Key": "asg2tag2", "Value": "diff"},
        ],
        VPCZoneIdentifier=subnet,
    )
    client.create_or_update_tags(
        Tags=[
            {
                "ResourceId": asg1,
                "Key": "test_key",
                "Value": "updated_test_value",
                "PropagateAtLaunch": True,
            },
            {
                "ResourceId": asg1,
                "Key": "test_key2",
                "Value": "test_value2",
                "PropagateAtLaunch": False,
            },
        ]
    )
    return asg1, asg2
