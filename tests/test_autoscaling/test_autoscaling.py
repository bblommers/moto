import unittest

import boto3
import sure  # noqa # pylint: disable=unused-import
from botocore.exceptions import ClientError
import pytest

from moto import mock_autoscaling, mock_ec2
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID

from . import get_all_instances, get_all_groups
from .utils import setup_networking, setup_instance_with_networking
from tests import EXAMPLE_AMI_ID
from uuid import uuid4


@mock_autoscaling
def test_create_autoscaling_groups_defaults():
    """Test with the minimum inputs and check that all of the proper defaults
    are assigned for the other attributes"""

    mocked_networking = setup_networking()
    as_client = boto3.client("autoscaling", region_name="us-east-1")
    lc_name = str(uuid4())
    as_client.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )

    asg_name = str(uuid4())
    as_client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchConfigurationName=lc_name,
        MinSize=2,
        MaxSize=2,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    group = as_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])[
        "AutoScalingGroups"
    ][0]
    group["AutoScalingGroupName"].should.equal(asg_name)
    group["MaxSize"].should.equal(2)
    group["MinSize"].should.equal(2)
    group["LaunchConfigurationName"].should.equal(lc_name)

    # Defaults
    group["AvailabilityZones"].should.equal(["us-east-1a"])  # subnet1
    group["DesiredCapacity"].should.equal(2)
    group["VPCZoneIdentifier"].should.equal(mocked_networking["subnet1"])
    group["DefaultCooldown"].should.equal(300)
    group["HealthCheckGracePeriod"].should.equal(300)
    group["HealthCheckType"].should.equal("EC2")
    group["LoadBalancerNames"].should.equal([])
    group.shouldnt.have.key("PlacementGroup")
    group["TerminationPolicies"].should.equal(["Default"])
    group["Tags"].should.equal([])


@mock_autoscaling
def test_list_many_autoscaling_groups():
    mocked_networking = setup_networking()
    conn = boto3.client("autoscaling", region_name="us-east-1")
    lc_name = str(uuid4())
    asg_prefix = str(uuid4())[0:6]
    conn.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )

    for i in range(51):
        conn.create_auto_scaling_group(
            AutoScalingGroupName=f"{asg_prefix}{i}",
            MinSize=1,
            MaxSize=2,
            LaunchConfigurationName=lc_name,
            VPCZoneIdentifier=mocked_networking["subnet1"],
        )

    groups = get_all_groups(conn)

    group_names = [a["AutoScalingGroupName"] for a in groups]
    group_names.should.contain(f"{asg_prefix}0")
    group_names.should.contain(f"{asg_prefix}2")
    group_names.should.contain(f"{asg_prefix}10")


@mock_autoscaling
def test_list_many_scheduled_scaling_actions():
    conn = boto3.client("autoscaling", region_name="us-east-1")

    asg_name = str(uuid4())
    sa_prefix = str(uuid4())[0:6]
    for i in range(30):
        conn.put_scheduled_update_group_action(
            AutoScalingGroupName=asg_name,
            ScheduledActionName=f"{sa_prefix}-{i}",
            StartTime=f"2022-07-01T00:00:{i}Z",
            EndTime=f"2022-09-01T00:00:{i}Z",
            Recurrence="* * * * *",
            MinSize=i + 1,
            MaxSize=i + 5,
            DesiredCapacity=i + 3,
        )

    response = conn.describe_scheduled_actions(AutoScalingGroupName=asg_name)
    actions = response["ScheduledUpdateGroupActions"]
    actions.should.have.length_of(30)


@mock_autoscaling
def test_non_existing_group_name():
    conn = boto3.client("autoscaling", region_name="us-east-1")

    conn.put_scheduled_update_group_action(
        AutoScalingGroupName=(str(uuid4())),
        ScheduledActionName=(str(uuid4())),
        StartTime="2022-07-01T00:00:1Z",
        EndTime="2022-09-01T00:00:2Z",
        Recurrence="* * * * *",
        MinSize=1,
        MaxSize=5,
        DesiredCapacity=3,
    )

    response = conn.describe_scheduled_actions(AutoScalingGroupName="wrong_group")
    actions = response["ScheduledUpdateGroupActions"]
    # since there is no such group name, no actions have been returned
    actions.should.have.length_of(0)


@mock_autoscaling
def test_describe_scheduled_actions_returns_all_actions_when_no_argument_is_passed():
    conn = boto3.client("autoscaling", region_name="us-east-1")

    sa_prefix = str(uuid4())[0:6]
    for i in range(30):
        conn.put_scheduled_update_group_action(
            AutoScalingGroupName=str(uuid4()),
            ScheduledActionName=f"{sa_prefix}-{i}",
            StartTime=f"2022-07-01T00:00:{i}Z",
            EndTime=f"2022-09-01T00:00:{i}Z",
            Recurrence="* * * * *",
            MinSize=i + 1,
            MaxSize=i + 5,
            DesiredCapacity=i + 3,
        )

    for i in range(10):
        conn.put_scheduled_update_group_action(
            AutoScalingGroupName=str(uuid4()),
            ScheduledActionName=f"{sa_prefix}-4{i}",
            StartTime=f"2022-07-01T00:00:{i}Z",
            EndTime=f"2022-09-01T00:00:{i}Z",
            Recurrence="* * * * *",
            MinSize=i + 1,
            MaxSize=i + 5,
            DesiredCapacity=i + 3,
        )

    actions = conn.describe_scheduled_actions()["ScheduledUpdateGroupActions"]
    names = [a["ScheduledActionName"] for a in actions]
    names.should.contain(f"{sa_prefix}-0")
    names.should.contain(f"{sa_prefix}-20")
    names.should.contain(f"{sa_prefix}-46")


@mock_autoscaling
@mock_ec2
def test_propogate_tags():
    mocked_networking = setup_networking()
    conn = boto3.client("autoscaling", region_name="us-east-1")
    lc_name = str(uuid4())
    asg_name = str(uuid4())
    tag_key = str(uuid4())[0:6]
    conn.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )

    conn.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        MinSize=1,
        MaxSize=2,
        LaunchConfigurationName=lc_name,
        Tags=[
            {
                "ResourceId": asg_name,
                "ResourceType": "auto-scaling-group",
                "PropagateAtLaunch": True,
                "Key": tag_key,
                "Value": "TestTagValue1",
            }
        ],
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    ec2 = boto3.client("ec2", region_name="us-east-1")
    instances = ec2.describe_instances(
        Filters=[{"Name": f"tag:{tag_key}", "Values": ["TestTagValue1"]}]
    )

    tags = instances["Reservations"][0]["Instances"][0]["Tags"]
    tags.should.contain({"Value": "TestTagValue1", "Key": tag_key})
    tags.should.contain({"Value": asg_name, "Key": "aws:autoscaling:groupName"})


@mock_autoscaling
def test_autoscaling_group_delete():
    mocked_networking = setup_networking()
    as_client = boto3.client("autoscaling", region_name="us-east-1")
    lc_name = str(uuid4())
    asg_name = str(uuid4())
    as_client.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )

    as_client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchConfigurationName=lc_name,
        MinSize=2,
        MaxSize=2,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    as_client.delete_auto_scaling_group(AutoScalingGroupName=asg_name)
    groups = as_client.describe_auto_scaling_groups()["AutoScalingGroups"]
    names = [g["AutoScalingGroupName"] for g in groups]
    names.shouldnt.contain(asg_name)


@mock_autoscaling
def test_scheduled_action_delete():
    as_client = boto3.client("autoscaling", region_name="us-east-1")

    asg_name = str(uuid4())
    for i in range(3):
        as_client.put_scheduled_update_group_action(
            AutoScalingGroupName=asg_name,
            ScheduledActionName=f"my-scheduled-action-{i}",
            StartTime=f"2022-07-01T00:00:{i}Z",
            EndTime=f"2022-09-01T00:00:{i}Z",
            Recurrence="* * * * *",
            MinSize=i + 1,
            MaxSize=i + 5,
            DesiredCapacity=i + 3,
        )

    response = as_client.describe_scheduled_actions(AutoScalingGroupName=asg_name)
    actions = response["ScheduledUpdateGroupActions"]
    actions.should.have.length_of(3)

    as_client.delete_scheduled_action(
        AutoScalingGroupName=asg_name,
        ScheduledActionName="my-scheduled-action-2",
    )
    as_client.delete_scheduled_action(
        AutoScalingGroupName=asg_name,
        ScheduledActionName="my-scheduled-action-1",
    )
    response = as_client.describe_scheduled_actions(AutoScalingGroupName=asg_name)
    actions = response["ScheduledUpdateGroupActions"]
    actions.should.have.length_of(1)


@mock_autoscaling
def test_create_autoscaling_group():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_launch_configuration(
        LaunchConfigurationName="test_launch_configuration",
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    response = client.create_auto_scaling_group(
        AutoScalingGroupName="test_asg",
        LaunchConfigurationName="test_launch_configuration",
        MinSize=0,
        MaxSize=20,
        DesiredCapacity=5,
        Tags=[
            {
                "ResourceId": "test_asg",
                "ResourceType": "auto-scaling-group",
                "Key": "propogated-tag-key",
                "Value": "propagate-tag-value",
                "PropagateAtLaunch": True,
            },
            {
                "ResourceId": "test_asg",
                "ResourceType": "auto-scaling-group",
                "Key": "not-propogated-tag-key",
                "Value": "not-propagate-tag-value",
                "PropagateAtLaunch": False,
            },
        ],
        VPCZoneIdentifier=mocked_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=False,
    )
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(200)


@mock_autoscaling
def test_create_autoscaling_group_from_instance():
    asg_name = str(uuid4())
    image_id = EXAMPLE_AMI_ID
    instance_type = "t2.micro"

    mocked_instance_with_networking = setup_instance_with_networking(
        image_id, instance_type
    )
    client = boto3.client("autoscaling", region_name="us-east-1")
    response = client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        InstanceId=mocked_instance_with_networking["instance"],
        MinSize=1,
        MaxSize=3,
        DesiredCapacity=2,
        Tags=[
            {
                "ResourceId": asg_name,
                "ResourceType": "auto-scaling-group",
                "Key": "propogated-tag-key",
                "Value": "propagate-tag-value",
                "PropagateAtLaunch": True,
            },
            {
                "ResourceId": asg_name,
                "ResourceType": "auto-scaling-group",
                "Key": "not-propogated-tag-key",
                "Value": "not-propagate-tag-value",
                "PropagateAtLaunch": False,
            },
        ],
        VPCZoneIdentifier=mocked_instance_with_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=False,
    )
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(200)

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    lc_name = response["AutoScalingGroups"][0]["LaunchConfigurationName"]

    response = client.describe_launch_configurations(LaunchConfigurationNames=[lc_name])
    response["LaunchConfigurations"].should.have.length_of(1)
    launch_config = response["LaunchConfigurations"][0]
    launch_config["LaunchConfigurationName"].should.equal(asg_name)
    launch_config["ImageId"].should.equal(image_id)
    launch_config["InstanceType"].should.equal(instance_type)


@mock_autoscaling
def test_create_autoscaling_group_from_invalid_instance_id():
    invalid_instance_id = "invalid_instance"

    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    with pytest.raises(ClientError) as ex:
        client.create_auto_scaling_group(
            AutoScalingGroupName="test_asg",
            InstanceId=invalid_instance_id,
            MinSize=9,
            MaxSize=15,
            DesiredCapacity=12,
            VPCZoneIdentifier=mocked_networking["subnet1"],
            NewInstancesProtectedFromScaleIn=False,
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Code"].should.equal("ValidationError")
    ex.value.response["Error"]["Message"].should.equal(
        "Instance [{0}] is invalid.".format(invalid_instance_id)
    )


@mock_autoscaling
@mock_ec2
class TestCreateAutoScalingGroup_FromTemplate(unittest.TestCase):
    def setUp(self) -> None:
        self.mocked_networking = setup_networking()

        self.as_client = boto3.client("autoscaling", region_name="us-east-1")
        self.ec2_client = boto3.client("ec2", region_name="us-east-1")
        self.lt_name = str(uuid4())
        self.template = self.ec2_client.create_launch_template(
            LaunchTemplateName=self.lt_name,
            LaunchTemplateData={"ImageId": EXAMPLE_AMI_ID, "InstanceType": "t2.micro"},
        )["LaunchTemplate"]

    def test_create_autoscaling_group_from_template(self):
        response = self.as_client.create_auto_scaling_group(
            AutoScalingGroupName=str(uuid4()),
            LaunchTemplate={
                "LaunchTemplateId": self.template["LaunchTemplateId"],
                "Version": str(self.template["LatestVersionNumber"]),
            },
            MinSize=1,
            MaxSize=3,
            DesiredCapacity=2,
            VPCZoneIdentifier=self.mocked_networking["subnet1"],
            NewInstancesProtectedFromScaleIn=False,
        )
        response["ResponseMetadata"]["HTTPStatusCode"].should.equal(200)

    def test_create_auto_scaling_from_template_version__latest(self):
        asg_name = str(uuid4())
        self.as_client.create_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            DesiredCapacity=1,
            MinSize=1,
            MaxSize=1,
            LaunchTemplate={
                "LaunchTemplateName": self.lt_name,
                "Version": "$Latest",
            },
            AvailabilityZones=["us-east-1a"],
        )

        response = self.as_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )["AutoScalingGroups"][0]
        response.should.have.key("LaunchTemplate")
        response["LaunchTemplate"].should.have.key("LaunchTemplateName").equals(
            self.lt_name
        )
        response["LaunchTemplate"].should.have.key("Version").equals("$Latest")

    def test_create_auto_scaling_from_template_version__default(self):
        self.ec2_client.create_launch_template_version(
            LaunchTemplateName=self.lt_name,
            LaunchTemplateData={"ImageId": EXAMPLE_AMI_ID, "InstanceType": "t3.medium"},
            VersionDescription="v2",
        )
        as_name = str(uuid4())
        self.as_client.create_auto_scaling_group(
            AutoScalingGroupName=as_name,
            DesiredCapacity=1,
            MinSize=1,
            MaxSize=1,
            LaunchTemplate={
                "LaunchTemplateName": self.lt_name,
                "Version": "$Default",
            },
            AvailabilityZones=["us-east-1a"],
        )

        response = self.as_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[as_name]
        )["AutoScalingGroups"][0]
        response.should.have.key("LaunchTemplate")
        response["LaunchTemplate"].should.have.key("LaunchTemplateName").equals(
            self.lt_name
        )
        response["LaunchTemplate"].should.have.key("Version").equals("$Default")

    def test_create_auto_scaling_from_template_version__no_version(self):
        as_name = str(uuid4())
        self.as_client.create_auto_scaling_group(
            AutoScalingGroupName=as_name,
            DesiredCapacity=1,
            MinSize=1,
            MaxSize=1,
            LaunchTemplate={"LaunchTemplateName": self.lt_name},
            AvailabilityZones=["us-east-1a"],
        )

        response = self.as_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[as_name]
        )["AutoScalingGroups"][0]
        response.should.have.key("LaunchTemplate")
        # We never specified the version - this is what it defaults to
        response["LaunchTemplate"].should.have.key("Version").equals("$Default")

    def test_create_autoscaling_group_no_template_ref(self):
        with pytest.raises(ClientError) as ex:
            self.as_client.create_auto_scaling_group(
                AutoScalingGroupName=str(uuid4()),
                LaunchTemplate={"Version": str(self.template["LatestVersionNumber"])},
                MinSize=0,
                MaxSize=20,
                DesiredCapacity=5,
                VPCZoneIdentifier=self.mocked_networking["subnet1"],
                NewInstancesProtectedFromScaleIn=False,
            )
        ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
        ex.value.response["Error"]["Code"].should.equal("ValidationError")
        ex.value.response["Error"]["Message"].should.equal(
            "Valid requests must contain either launchTemplateId or LaunchTemplateName"
        )

    def test_create_autoscaling_group_multiple_template_ref(self):
        with pytest.raises(ClientError) as ex:
            self.as_client.create_auto_scaling_group(
                AutoScalingGroupName=str(uuid4()),
                LaunchTemplate={
                    "LaunchTemplateId": self.template["LaunchTemplateId"],
                    "LaunchTemplateName": self.template["LaunchTemplateName"],
                    "Version": str(self.template["LatestVersionNumber"]),
                },
                MinSize=0,
                MaxSize=20,
                DesiredCapacity=5,
                VPCZoneIdentifier=self.mocked_networking["subnet1"],
                NewInstancesProtectedFromScaleIn=False,
            )
        ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
        ex.value.response["Error"]["Code"].should.equal("ValidationError")
        ex.value.response["Error"]["Message"].should.equal(
            "Valid requests must contain either launchTemplateId or LaunchTemplateName"
        )


@mock_autoscaling
def test_create_autoscaling_group_no_launch_configuration():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    with pytest.raises(ClientError) as ex:
        client.create_auto_scaling_group(
            AutoScalingGroupName="test_asg",
            MinSize=0,
            MaxSize=20,
            DesiredCapacity=5,
            VPCZoneIdentifier=mocked_networking["subnet1"],
            NewInstancesProtectedFromScaleIn=False,
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Code"].should.equal("ValidationError")
    ex.value.response["Error"]["Message"].should.equal(
        "Valid requests must contain either LaunchTemplate, LaunchConfigurationName, "
        "InstanceId or MixedInstancesPolicy parameter."
    )


@mock_autoscaling
@mock_ec2
def test_create_autoscaling_group_multiple_launch_configurations():
    mocked_networking = setup_networking()

    ec2_client = boto3.client("ec2", region_name="us-east-1")
    lt_name = str(uuid4())
    lc_name = str(uuid4())
    template = ec2_client.create_launch_template(
        LaunchTemplateName=lt_name,
        LaunchTemplateData={"ImageId": EXAMPLE_AMI_ID, "InstanceType": "t2.micro"},
    )["LaunchTemplate"]
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )

    with pytest.raises(ClientError) as ex:
        client.create_auto_scaling_group(
            AutoScalingGroupName=str(uuid4()),
            LaunchConfigurationName=lc_name,
            LaunchTemplate={
                "LaunchTemplateId": template["LaunchTemplateId"],
                "Version": str(template["LatestVersionNumber"]),
            },
            MinSize=0,
            MaxSize=20,
            DesiredCapacity=5,
            VPCZoneIdentifier=mocked_networking["subnet1"],
            NewInstancesProtectedFromScaleIn=False,
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Code"].should.equal("ValidationError")
    ex.value.response["Error"]["Message"].should.equal(
        "Valid requests must contain either LaunchTemplate, LaunchConfigurationName, "
        "InstanceId or MixedInstancesPolicy parameter."
    )


@mock_autoscaling
def test_describe_autoscaling_groups_launch_config():
    mocked_networking = setup_networking(region_name="eu-north-1")
    client = boto3.client("autoscaling", region_name="eu-north-1")
    client.create_launch_configuration(
        LaunchConfigurationName="test_launch_configuration",
        InstanceType="t2.micro",
        ImageId=EXAMPLE_AMI_ID,
    )
    client.create_auto_scaling_group(
        AutoScalingGroupName="test_asg",
        LaunchConfigurationName="test_launch_configuration",
        MinSize=0,
        MaxSize=20,
        DesiredCapacity=5,
        VPCZoneIdentifier=mocked_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=True,
    )

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=["test_asg"])
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(200)
    group = response["AutoScalingGroups"][0]
    group["AutoScalingGroupARN"].should.match(
        f"arn:aws:autoscaling:eu-north-1:{ACCOUNT_ID}:autoScalingGroup:"
    )
    group["AutoScalingGroupName"].should.equal("test_asg")
    group["LaunchConfigurationName"].should.equal("test_launch_configuration")
    group.should_not.have.key("LaunchTemplate")
    group["AvailabilityZones"].should.equal(["eu-north-1a"])
    group["VPCZoneIdentifier"].should.equal(mocked_networking["subnet1"])
    group["NewInstancesProtectedFromScaleIn"].should.equal(True)
    for instance in group["Instances"]:
        instance["LaunchConfigurationName"].should.equal("test_launch_configuration")
        instance.should_not.have.key("LaunchTemplate")
        instance["AvailabilityZone"].should.equal("eu-north-1a")
        instance["ProtectedFromScaleIn"].should.equal(True)
        instance["InstanceType"].should.equal("t2.micro")


@mock_autoscaling
@mock_ec2
def test_describe_autoscaling_groups_launch_template():
    mocked_networking = setup_networking()
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    asg_name = str(uuid4())
    lg_name = str(uuid4())
    template = ec2_client.create_launch_template(
        LaunchTemplateName=lg_name,
        LaunchTemplateData={"ImageId": EXAMPLE_AMI_ID, "InstanceType": "t2.micro"},
    )["LaunchTemplate"]
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchTemplate={"LaunchTemplateName": lg_name, "Version": "1"},
        MinSize=0,
        MaxSize=20,
        DesiredCapacity=5,
        VPCZoneIdentifier=mocked_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=True,
    )
    expected_launch_template = {
        "LaunchTemplateId": template["LaunchTemplateId"],
        "LaunchTemplateName": lg_name,
        "Version": "1",
    }

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    response["ResponseMetadata"]["HTTPStatusCode"].should.equal(200)
    group = response["AutoScalingGroups"][0]
    group["AutoScalingGroupName"].should.equal(asg_name)
    group["LaunchTemplate"].should.equal(expected_launch_template)
    group.should_not.have.key("LaunchConfigurationName")
    group["AvailabilityZones"].should.equal(["us-east-1a"])
    group["VPCZoneIdentifier"].should.equal(mocked_networking["subnet1"])
    group["NewInstancesProtectedFromScaleIn"].should.equal(True)
    for instance in group["Instances"]:
        instance["LaunchTemplate"].should.equal(expected_launch_template)
        instance.should_not.have.key("LaunchConfigurationName")
        instance["AvailabilityZone"].should.equal("us-east-1a")
        instance["ProtectedFromScaleIn"].should.equal(True)
        instance["InstanceType"].should.equal("t2.micro")


@mock_autoscaling
def test_describe_autoscaling_instances_launch_config():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    asg_name = str(uuid4())
    lc_name = str(uuid4())
    client.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        InstanceType="t2.micro",
        ImageId=EXAMPLE_AMI_ID,
    )
    client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchConfigurationName=lc_name,
        MinSize=0,
        MaxSize=20,
        DesiredCapacity=5,
        VPCZoneIdentifier=mocked_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=True,
    )

    instances = get_all_instances(client)
    ours = [i for i in instances if i["AutoScalingGroupName"] == asg_name]
    len(ours).should.equal(5)
    for instance in ours:
        instance["LaunchConfigurationName"].should.equal(lc_name)
        instance.should_not.have.key("LaunchTemplate")
        instance["AutoScalingGroupName"].should.equal(asg_name)
        instance["AvailabilityZone"].should.equal("us-east-1a")
        instance["ProtectedFromScaleIn"].should.equal(True)
        instance["InstanceType"].should.equal("t2.micro")


@mock_autoscaling
@mock_ec2
def test_describe_autoscaling_instances_launch_template():
    mocked_networking = setup_networking()
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    asg_name = str(uuid4())
    lt_name = str(uuid4())
    template = ec2_client.create_launch_template(
        LaunchTemplateName=lt_name,
        LaunchTemplateData={"ImageId": EXAMPLE_AMI_ID, "InstanceType": "t2.micro"},
    )["LaunchTemplate"]
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchTemplate={"LaunchTemplateName": lt_name, "Version": "1"},
        MinSize=0,
        MaxSize=20,
        DesiredCapacity=5,
        VPCZoneIdentifier=mocked_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=True,
    )
    expected_launch_template = {
        "LaunchTemplateId": template["LaunchTemplateId"],
        "LaunchTemplateName": lt_name,
        "Version": "1",
    }

    instances = get_all_instances(client)
    ours = [i for i in instances if i["AutoScalingGroupName"] == asg_name]
    len(ours).should.equal(5)
    for instance in ours:
        instance["LaunchTemplate"].should.equal(expected_launch_template)
        instance.should_not.have.key("LaunchConfigurationName")
        instance["AutoScalingGroupName"].should.equal(asg_name)
        instance["AvailabilityZone"].should.equal("us-east-1a")
        instance["ProtectedFromScaleIn"].should.equal(True)
        instance["InstanceType"].should.equal("t2.micro")


@mock_autoscaling
def test_describe_autoscaling_instances_instanceid_filter():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    asg_name = str(uuid4())
    lc_name = str(uuid4())
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
        VPCZoneIdentifier=mocked_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=True,
    )

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    instance_ids = [
        instance["InstanceId"]
        for instance in response["AutoScalingGroups"][0]["Instances"]
    ]

    response = client.describe_auto_scaling_instances(
        InstanceIds=instance_ids[0:2]
    )  # Filter by first 2 of 5
    len(response["AutoScalingInstances"]).should.equal(2)
    for instance in response["AutoScalingInstances"]:
        instance["AutoScalingGroupName"].should.equal(asg_name)
        instance["AvailabilityZone"].should.equal("us-east-1a")
        instance["ProtectedFromScaleIn"].should.equal(True)


@mock_autoscaling
def test_update_autoscaling_group_launch_config():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_launch_configuration(
        LaunchConfigurationName="test_launch_configuration",
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    client.create_launch_configuration(
        LaunchConfigurationName="test_launch_configuration_new",
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    client.create_auto_scaling_group(
        AutoScalingGroupName="test_asg",
        LaunchConfigurationName="test_launch_configuration",
        MinSize=0,
        MaxSize=20,
        DesiredCapacity=5,
        VPCZoneIdentifier=mocked_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=True,
    )

    client.update_auto_scaling_group(
        AutoScalingGroupName="test_asg",
        LaunchConfigurationName="test_launch_configuration_new",
        MinSize=1,
        VPCZoneIdentifier="{subnet1},{subnet2}".format(
            subnet1=mocked_networking["subnet1"], subnet2=mocked_networking["subnet2"]
        ),
        NewInstancesProtectedFromScaleIn=False,
    )

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=["test_asg"])
    group = response["AutoScalingGroups"][0]
    group["LaunchConfigurationName"].should.equal("test_launch_configuration_new")
    group["MinSize"].should.equal(1)
    set(group["AvailabilityZones"]).should.equal({"us-east-1a", "us-east-1b"})
    group["NewInstancesProtectedFromScaleIn"].should.equal(False)


@mock_autoscaling
@mock_ec2
def test_update_autoscaling_group_launch_template():
    mocked_networking = setup_networking()
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    asg_name = str(uuid4())
    lc_Name = str(uuid4())
    lc_name2 = str(uuid4())
    ec2_client.create_launch_template(
        LaunchTemplateName=lc_Name,
        LaunchTemplateData={"ImageId": EXAMPLE_AMI_ID, "InstanceType": "t2.micro"},
    )
    template = ec2_client.create_launch_template(
        LaunchTemplateName=lc_name2,
        LaunchTemplateData={
            "ImageId": "ami-1ea5b10a3d8867db4",
            "InstanceType": "t2.micro",
        },
    )["LaunchTemplate"]
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchTemplate={"LaunchTemplateName": lc_Name, "Version": "1"},
        MinSize=0,
        MaxSize=20,
        DesiredCapacity=5,
        VPCZoneIdentifier=mocked_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=True,
    )

    client.update_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchTemplate={
            "LaunchTemplateName": lc_name2,
            "Version": "1",
        },
        MinSize=1,
        VPCZoneIdentifier="{subnet1},{subnet2}".format(
            subnet1=mocked_networking["subnet1"], subnet2=mocked_networking["subnet2"]
        ),
        NewInstancesProtectedFromScaleIn=False,
    )

    expected_launch_template = {
        "LaunchTemplateId": template["LaunchTemplateId"],
        "LaunchTemplateName": lc_name2,
        "Version": "1",
    }

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    group = response["AutoScalingGroups"][0]
    group["LaunchTemplate"].should.equal(expected_launch_template)
    group["MinSize"].should.equal(1)
    set(group["AvailabilityZones"]).should.equal({"us-east-1a", "us-east-1b"})
    group["NewInstancesProtectedFromScaleIn"].should.equal(False)


@mock_autoscaling
def test_update_autoscaling_group_min_size_desired_capacity_change():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")

    client.create_launch_configuration(
        LaunchConfigurationName="test_launch_configuration",
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    client.create_auto_scaling_group(
        AutoScalingGroupName="test_asg",
        LaunchConfigurationName="test_launch_configuration",
        MinSize=2,
        MaxSize=20,
        DesiredCapacity=3,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )
    client.update_auto_scaling_group(AutoScalingGroupName="test_asg", MinSize=5)
    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=["test_asg"])
    group = response["AutoScalingGroups"][0]
    group["DesiredCapacity"].should.equal(5)
    group["MinSize"].should.equal(5)
    group["Instances"].should.have.length_of(5)


@mock_autoscaling
def test_update_autoscaling_group_max_size_desired_capacity_change():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")

    client.create_launch_configuration(
        LaunchConfigurationName="test_launch_configuration",
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    client.create_auto_scaling_group(
        AutoScalingGroupName="test_asg",
        LaunchConfigurationName="test_launch_configuration",
        MinSize=2,
        MaxSize=20,
        DesiredCapacity=10,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )
    client.update_auto_scaling_group(AutoScalingGroupName="test_asg", MaxSize=5)
    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=["test_asg"])
    group = response["AutoScalingGroups"][0]
    group["DesiredCapacity"].should.equal(5)
    group["MaxSize"].should.equal(5)
    group["Instances"].should.have.length_of(5)


@mock_autoscaling
def test_autoscaling_describe_policies():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    asg_name = str(uuid4())
    lc_name = str(uuid4())
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
                "ResourceId": asg_name,
                "Key": "test_key",
                "Value": "test_value",
                "PropagateAtLaunch": True,
            }
        ],
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    client.put_scaling_policy(
        AutoScalingGroupName=asg_name,
        PolicyName="test_policy_down",
        PolicyType="SimpleScaling",
        MetricAggregationType="Minimum",
        AdjustmentType="PercentChangeInCapacity",
        ScalingAdjustment=-10,
        Cooldown=60,
        MinAdjustmentMagnitude=1,
    )
    client.put_scaling_policy(
        AutoScalingGroupName=asg_name,
        PolicyName="test_policy_up",
        PolicyType="SimpleScaling",
        AdjustmentType="PercentChangeInCapacity",
        ScalingAdjustment=10,
        Cooldown=60,
        MinAdjustmentMagnitude=1,
    )

    response = client.describe_policies(AutoScalingGroupName=asg_name)
    response["ScalingPolicies"].should.have.length_of(2)

    response = client.describe_policies(
        AutoScalingGroupName=asg_name, PolicyTypes=["StepScaling"]
    )
    response["ScalingPolicies"].should.have.length_of(0)

    response = client.describe_policies(
        AutoScalingGroupName=asg_name,
        PolicyNames=["test_policy_down"],
        PolicyTypes=["SimpleScaling"],
    )
    response["ScalingPolicies"].should.have.length_of(1)
    policy = response["ScalingPolicies"][0]
    policy["PolicyType"].should.equal("SimpleScaling")
    policy["MetricAggregationType"].should.equal("Minimum")
    policy["AdjustmentType"].should.equal("PercentChangeInCapacity")
    policy["ScalingAdjustment"].should.equal(-10)
    policy["Cooldown"].should.equal(60)
    policy["PolicyARN"].should.equal(
        f"arn:aws:autoscaling:us-east-1:{ACCOUNT_ID}:scalingPolicy:c322761b-3172-4d56-9a21-0ed9d6161d67:autoScalingGroupName/{asg_name}:policyName/test_policy_down"
    )
    policy["PolicyName"].should.equal("test_policy_down")
    policy.shouldnt.have.key("TargetTrackingConfiguration")


@mock_autoscaling
@mock_ec2
def test_create_autoscaling_policy_with_policytype__targettrackingscaling():
    mocked_networking = setup_networking(region_name="us-west-1")
    client = boto3.client("autoscaling", region_name="us-west-1")
    configuration_name = "test"
    asg_name = "asg_test"

    client.create_launch_configuration(
        LaunchConfigurationName=configuration_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="m1.small",
    )
    client.create_auto_scaling_group(
        LaunchConfigurationName=configuration_name,
        AutoScalingGroupName=asg_name,
        MinSize=1,
        MaxSize=2,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    client.put_scaling_policy(
        AutoScalingGroupName=asg_name,
        PolicyName=configuration_name,
        PolicyType="TargetTrackingScaling",
        EstimatedInstanceWarmup=100,
        TargetTrackingConfiguration={
            "PredefinedMetricSpecification": {
                "PredefinedMetricType": "ASGAverageNetworkIn",
            },
            "TargetValue": 1000000.0,
        },
    )

    resp = client.describe_policies(AutoScalingGroupName=asg_name)
    policy = resp["ScalingPolicies"][0]
    policy.should.have.key("PolicyName").equals(configuration_name)
    policy.should.have.key("PolicyARN").equals(
        f"arn:aws:autoscaling:us-west-1:{ACCOUNT_ID}:scalingPolicy:c322761b-3172-4d56-9a21-0ed9d6161d67:autoScalingGroupName/{asg_name}:policyName/{configuration_name}"
    )
    policy.should.have.key("PolicyType").equals("TargetTrackingScaling")
    policy.should.have.key("TargetTrackingConfiguration").should.equal(
        {
            "PredefinedMetricSpecification": {
                "PredefinedMetricType": "ASGAverageNetworkIn",
            },
            "TargetValue": 1000000.0,
        }
    )
    policy.shouldnt.have.key("ScalingAdjustment")
    policy.shouldnt.have.key("Cooldown")


@mock_autoscaling
@mock_ec2
def test_create_autoscaling_policy_with_policytype__stepscaling():
    mocked_networking = setup_networking(region_name="eu-west-1")
    client = boto3.client("autoscaling", region_name="eu-west-1")
    launch_config_name = "lg_name"
    asg_name = "asg_test"

    client.create_launch_configuration(
        LaunchConfigurationName=launch_config_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="m1.small",
    )
    client.create_auto_scaling_group(
        LaunchConfigurationName=launch_config_name,
        AutoScalingGroupName=asg_name,
        MinSize=1,
        MaxSize=2,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    client.put_scaling_policy(
        AutoScalingGroupName=asg_name,
        PolicyName=launch_config_name,
        PolicyType="StepScaling",
        StepAdjustments=[
            {
                "MetricIntervalLowerBound": 2,
                "MetricIntervalUpperBound": 8,
                "ScalingAdjustment": 1,
            }
        ],
    )

    resp = client.describe_policies(AutoScalingGroupName=asg_name)
    policy = resp["ScalingPolicies"][0]
    policy.should.have.key("PolicyName").equals(launch_config_name)
    policy.should.have.key("PolicyARN").equals(
        f"arn:aws:autoscaling:eu-west-1:{ACCOUNT_ID}:scalingPolicy:c322761b-3172-4d56-9a21-0ed9d6161d67:autoScalingGroupName/{asg_name}:policyName/{launch_config_name}"
    )
    policy.should.have.key("PolicyType").equals("StepScaling")
    policy.should.have.key("StepAdjustments").equal(
        [
            {
                "MetricIntervalLowerBound": 2,
                "MetricIntervalUpperBound": 8,
                "ScalingAdjustment": 1,
            }
        ]
    )
    policy.shouldnt.have.key("TargetTrackingConfiguration")
    policy.shouldnt.have.key("ScalingAdjustment")
    policy.shouldnt.have.key("Cooldown")


@mock_autoscaling
@mock_ec2
def test_create_autoscaling_policy_with_predictive_scaling_config():
    mocked_networking = setup_networking(region_name="eu-west-1")
    client = boto3.client("autoscaling", region_name="eu-west-1")
    launch_config_name = "lg_name"
    asg_name = "asg_test"

    client.create_launch_configuration(
        LaunchConfigurationName=launch_config_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="m1.small",
    )
    client.create_auto_scaling_group(
        LaunchConfigurationName=launch_config_name,
        AutoScalingGroupName=asg_name,
        MinSize=1,
        MaxSize=2,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    client.put_scaling_policy(
        AutoScalingGroupName=asg_name,
        PolicyName=launch_config_name,
        PolicyType="PredictiveScaling",
        PredictiveScalingConfiguration={
            "MetricSpecifications": [{"TargetValue": 5}],
            "SchedulingBufferTime": 7,
        },
    )

    resp = client.describe_policies(AutoScalingGroupName=asg_name)
    policy = resp["ScalingPolicies"][0]
    policy.should.have.key("PredictiveScalingConfiguration").equals(
        {"MetricSpecifications": [{"TargetValue": 5.0}], "SchedulingBufferTime": 7}
    )


@mock_autoscaling
@mock_ec2
def test_describe_instance_health():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    _ = client.create_launch_configuration(
        LaunchConfigurationName="test_launch_configuration",
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    client.create_auto_scaling_group(
        AutoScalingGroupName="test_asg",
        LaunchConfigurationName="test_launch_configuration",
        MinSize=2,
        MaxSize=4,
        DesiredCapacity=2,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=["test_asg"])

    instance1 = response["AutoScalingGroups"][0]["Instances"][0]
    instance1["HealthStatus"].should.equal("Healthy")


@mock_autoscaling
@mock_ec2
def test_set_instance_health():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    _ = client.create_launch_configuration(
        LaunchConfigurationName="test_launch_configuration",
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    client.create_auto_scaling_group(
        AutoScalingGroupName="test_asg",
        LaunchConfigurationName="test_launch_configuration",
        MinSize=2,
        MaxSize=4,
        DesiredCapacity=2,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=["test_asg"])

    instance1 = response["AutoScalingGroups"][0]["Instances"][0]
    instance1["HealthStatus"].should.equal("Healthy")

    client.set_instance_health(
        InstanceId=instance1["InstanceId"], HealthStatus="Unhealthy"
    )

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=["test_asg"])

    instance1 = response["AutoScalingGroups"][0]["Instances"][0]
    instance1["HealthStatus"].should.equal("Unhealthy")


@mock_autoscaling
def test_suspend_processes():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_launch_configuration(
        LaunchConfigurationName="lc", ImageId=EXAMPLE_AMI_ID, InstanceType="t2.medium"
    )
    asg_name = str(uuid4())
    client.create_auto_scaling_group(
        LaunchConfigurationName="lc",
        AutoScalingGroupName=asg_name,
        MinSize=1,
        MaxSize=1,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    # When we suspend the 'Launch' process on the ASG client
    client.suspend_processes(AutoScalingGroupName=asg_name, ScalingProcesses=["Launch"])

    res = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])

    # The 'Launch' process should, in fact, be suspended
    launch_suspended = False
    for proc in res["AutoScalingGroups"][0]["SuspendedProcesses"]:
        if proc.get("ProcessName") == "Launch":
            launch_suspended = True

    assert launch_suspended is True


@mock_autoscaling
def test_suspend_processes_all_by_default():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_launch_configuration(
        LaunchConfigurationName="lc", ImageId=EXAMPLE_AMI_ID, InstanceType="t2.medium"
    )
    asg_name = str(uuid4())
    client.create_auto_scaling_group(
        LaunchConfigurationName="lc",
        AutoScalingGroupName=asg_name,
        MinSize=1,
        MaxSize=1,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    # When we suspend with no processes specified
    client.suspend_processes(AutoScalingGroupName=asg_name)

    res = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])

    # All processes should be suspended
    all_proc_names = [
        "Launch",
        "Terminate",
        "AddToLoadBalancer",
        "AlarmNotification",
        "AZRebalance",
        "HealthCheck",
        "InstanceRefresh",
        "ReplaceUnhealthy",
        "ScheduledActions",
    ]
    suspended_proc_names = [
        proc["ProcessName"]
        for proc in res["AutoScalingGroups"][0]["SuspendedProcesses"]
    ]
    set(suspended_proc_names).should.equal(set(all_proc_names))


@mock_autoscaling
def test_suspend_additional_processes():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_launch_configuration(
        LaunchConfigurationName="lc", ImageId=EXAMPLE_AMI_ID, InstanceType="t2.medium"
    )
    asg_name = str(uuid4())
    client.create_auto_scaling_group(
        LaunchConfigurationName="lc",
        AutoScalingGroupName=asg_name,
        MinSize=1,
        MaxSize=1,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    # When we suspend the 'Launch' and 'Terminate' processes in separate calls
    client.suspend_processes(AutoScalingGroupName=asg_name, ScalingProcesses=["Launch"])
    client.suspend_processes(
        AutoScalingGroupName=asg_name, ScalingProcesses=["Terminate"]
    )

    res = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])

    # Both 'Launch' and 'Terminate' should be suspended
    launch_suspended = False
    terminate_suspended = False
    for proc in res["AutoScalingGroups"][0]["SuspendedProcesses"]:
        if proc.get("ProcessName") == "Launch":
            launch_suspended = True
        if proc.get("ProcessName") == "Terminate":
            terminate_suspended = True

    assert launch_suspended is True
    assert terminate_suspended is True


@mock_autoscaling
def test_resume_processes():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_launch_configuration(
        LaunchConfigurationName="lc", ImageId=EXAMPLE_AMI_ID, InstanceType="t2.medium"
    )
    asg_name = str(uuid4())
    client.create_auto_scaling_group(
        LaunchConfigurationName="lc",
        AutoScalingGroupName=asg_name,
        MinSize=1,
        MaxSize=1,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    # When we suspect 'Launch' and 'Termiate' process then resume 'Launch'
    client.suspend_processes(
        AutoScalingGroupName=asg_name, ScalingProcesses=["Launch", "Terminate"]
    )

    client.resume_processes(AutoScalingGroupName=asg_name, ScalingProcesses=["Launch"])

    res = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])

    # Only 'Terminate' should be suspended
    expected_suspended_processes = [
        {"ProcessName": "Terminate", "SuspensionReason": ""}
    ]
    res["AutoScalingGroups"][0]["SuspendedProcesses"].should.equal(
        expected_suspended_processes
    )


@mock_autoscaling
def test_resume_processes_all_by_default():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_launch_configuration(
        LaunchConfigurationName="lc", ImageId=EXAMPLE_AMI_ID, InstanceType="t2.medium"
    )
    asg_name = str(uuid4())
    client.create_auto_scaling_group(
        LaunchConfigurationName="lc",
        AutoScalingGroupName=asg_name,
        MinSize=1,
        MaxSize=1,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    # When we suspend two processes then resume with no process argument
    client.suspend_processes(
        AutoScalingGroupName=asg_name, ScalingProcesses=["Launch", "Terminate"]
    )

    client.resume_processes(AutoScalingGroupName=asg_name)

    res = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])

    # No processes should be suspended
    res["AutoScalingGroups"][0]["SuspendedProcesses"].should.equal([])


@mock_autoscaling
def test_set_instance_protection():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    asg_name = str(uuid4())
    lc_name = str(uuid4())
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
        VPCZoneIdentifier=mocked_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=False,
    )

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    instance_ids = [
        instance["InstanceId"]
        for instance in response["AutoScalingGroups"][0]["Instances"]
    ]
    protected = instance_ids[:3]

    _ = client.set_instance_protection(
        AutoScalingGroupName=asg_name,
        InstanceIds=protected,
        ProtectedFromScaleIn=True,
    )

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    for instance in response["AutoScalingGroups"][0]["Instances"]:
        instance["ProtectedFromScaleIn"].should.equal(
            instance["InstanceId"] in protected
        )


@mock_autoscaling
def test_set_desired_capacity_up():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    _ = client.create_launch_configuration(
        LaunchConfigurationName="test_launch_configuration",
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    _ = client.create_auto_scaling_group(
        AutoScalingGroupName="test_asg",
        LaunchConfigurationName="test_launch_configuration",
        MinSize=0,
        MaxSize=20,
        DesiredCapacity=5,
        VPCZoneIdentifier=mocked_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=True,
    )

    _ = client.set_desired_capacity(AutoScalingGroupName="test_asg", DesiredCapacity=10)

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=["test_asg"])
    instances = response["AutoScalingGroups"][0]["Instances"]
    instances.should.have.length_of(10)
    for instance in instances:
        instance["ProtectedFromScaleIn"].should.equal(True)


@mock_autoscaling
def test_set_desired_capacity_down():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    _ = client.create_launch_configuration(
        LaunchConfigurationName="test_launch_configuration",
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    _ = client.create_auto_scaling_group(
        AutoScalingGroupName="test_asg",
        LaunchConfigurationName="test_launch_configuration",
        MinSize=0,
        MaxSize=20,
        DesiredCapacity=5,
        VPCZoneIdentifier=mocked_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=True,
    )

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=["test_asg"])
    instance_ids = [
        instance["InstanceId"]
        for instance in response["AutoScalingGroups"][0]["Instances"]
    ]
    unprotected, protected = instance_ids[:2], instance_ids[2:]

    _ = client.set_instance_protection(
        AutoScalingGroupName="test_asg",
        InstanceIds=unprotected,
        ProtectedFromScaleIn=False,
    )

    _ = client.set_desired_capacity(AutoScalingGroupName="test_asg", DesiredCapacity=1)

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=["test_asg"])
    group = response["AutoScalingGroups"][0]
    group["DesiredCapacity"].should.equal(1)
    instance_ids = {instance["InstanceId"] for instance in group["Instances"]}
    set(protected).should.equal(instance_ids)
    set(unprotected).should_not.be.within(instance_ids)  # only unprotected killed


@mock_autoscaling
@mock_ec2
def test_terminate_instance_via_ec2_in_autoscaling_group():
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    _ = client.create_launch_configuration(
        LaunchConfigurationName="test_launch_configuration",
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )
    _ = client.create_auto_scaling_group(
        AutoScalingGroupName="test_asg",
        LaunchConfigurationName="test_launch_configuration",
        MinSize=1,
        MaxSize=20,
        VPCZoneIdentifier=mocked_networking["subnet1"],
        NewInstancesProtectedFromScaleIn=False,
    )

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=["test_asg"])
    original_instance_id = next(
        instance["InstanceId"]
        for instance in response["AutoScalingGroups"][0]["Instances"]
    )
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    ec2_client.terminate_instances(InstanceIds=[original_instance_id])

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=["test_asg"])
    replaced_instance_id = next(
        instance["InstanceId"]
        for instance in response["AutoScalingGroups"][0]["Instances"]
    )
    replaced_instance_id.should_not.equal(original_instance_id)


@mock_ec2
@mock_autoscaling
def test_attach_instances():
    asg_client = boto3.client("autoscaling", region_name="us-east-1")
    ec2_client = boto3.client("ec2", region_name="us-east-1")

    kwargs = {
        "KeyName": "foobar",
        "ImageId": EXAMPLE_AMI_ID,
        "MinCount": 1,
        "MaxCount": 1,
        "InstanceType": "c4.2xlarge",
        "TagSpecifications": [
            {"ResourceType": "instance", "Tags": [{"Key": "key", "Value": "val"}]},
        ],
    }
    fake_instance = ec2_client.run_instances(**kwargs)["Instances"][0]
    asg_name = str(uuid4())
    lc_name = str(uuid4())
    asg_client.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        ImageId="ami-pytest",
        InstanceType="t3.micro",
        KeyName="foobar",
    )
    asg_client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchConfigurationName=lc_name,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=[fake_instance["Placement"]["AvailabilityZone"]],
    )
    asg_client.attach_instances(
        InstanceIds=[fake_instance["InstanceId"]], AutoScalingGroupName=asg_name
    )
    instances = get_all_instances(asg_client)
    ours = [i for i in instances if i["AutoScalingGroupName"] == asg_name]
    len(ours).should.equal(1)
    for instance in ours:
        instance["LaunchConfigurationName"].should.equal(lc_name)
        instance["AutoScalingGroupName"].should.equal(asg_name)
        instance["InstanceType"].should.equal("c4.2xlarge")


@mock_autoscaling
def test_autoscaling_lifecyclehook():
    asg_name = str(uuid4())
    lc_name = str(uuid4())
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        ImageId="ami-pytest",
        InstanceType="t3.micro",
        KeyName="foobar",
    )
    client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchConfigurationName=lc_name,
        MinSize=0,
        MaxSize=1,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )
    client.put_lifecycle_hook(
        LifecycleHookName="test-lifecyclehook",
        AutoScalingGroupName=asg_name,
        LifecycleTransition="autoscaling:EC2_INSTANCE_TERMINATING",
    )

    response = client.describe_lifecycle_hooks(
        AutoScalingGroupName=asg_name, LifecycleHookNames=["test-lifecyclehook"]
    )
    len(response["LifecycleHooks"]).should.equal(1)
    for hook in response["LifecycleHooks"]:
        hook["LifecycleHookName"].should.equal("test-lifecyclehook")
        hook["AutoScalingGroupName"].should.equal(asg_name)
        hook["LifecycleTransition"].should.equal("autoscaling:EC2_INSTANCE_TERMINATING")

    client.delete_lifecycle_hook(
        LifecycleHookName="test-lifecyclehook", AutoScalingGroupName=asg_name
    )

    response = client.describe_lifecycle_hooks(
        AutoScalingGroupName=asg_name, LifecycleHookNames=["test-lifecyclehook"]
    )

    len(response["LifecycleHooks"]).should.equal(0)


@pytest.mark.parametrize("original,new", [(2, 1), (2, 3), (1, 5), (1, 1)])
@mock_autoscaling
def test_set_desired_capacity_without_protection(original, new):
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    lc_name = str(uuid4())
    asg_name = str(uuid4())
    client.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="t2.medium",
    )

    client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchConfigurationName=lc_name,
        AvailabilityZones=["us-east-1a"],
        MinSize=original,
        MaxSize=original,
        DesiredCapacity=original,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )

    resp = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    group = resp["AutoScalingGroups"][0]
    group["DesiredCapacity"].should.equal(original)
    instances = get_all_instances(client)
    ours = [i for i in instances if i["AutoScalingGroupName"] == asg_name]
    ours.should.have.length_of(original)

    client.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=new)

    group = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])[
        "AutoScalingGroups"
    ][0]
    group["DesiredCapacity"].should.equal(new)
    instances = get_all_instances(client)
    ours = [i for i in instances if i["AutoScalingGroupName"] == asg_name]
    ours.should.have.length_of(new)


@mock_autoscaling
@mock_ec2
def test_create_template_with_block_device():
    ec2_client = boto3.client("ec2", region_name="ap-southeast-2")
    lt_name = str(uuid4())
    ec2_client.create_launch_template(
        LaunchTemplateName=lt_name,
        LaunchTemplateData={
            "ImageId": EXAMPLE_AMI_ID,
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/sda1",
                    "Ebs": {
                        "VolumeSize": 20,
                        "DeleteOnTermination": True,
                        "VolumeType": "gp3",
                        "Encrypted": True,
                    },
                }
            ],
        },
    )

    ec2_client.run_instances(
        MaxCount=1, MinCount=1, LaunchTemplate={"LaunchTemplateName": lt_name}
    )
    ec2_client = boto3.client("ec2", region_name="ap-southeast-2")
    volumes = ec2_client.describe_volumes()["Volumes"]
    # The standard root volume
    volumes[0]["VolumeType"].should.equal("gp2")
    volumes[0]["Size"].should.equal(8)
    # Our Ebs-volume
    volumes[1]["VolumeType"].should.equal("gp3")
    volumes[1]["Size"].should.equal(20)
