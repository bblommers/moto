import boto3
import sure  # noqa # pylint: disable=unused-import
import pytest
import uuid

from moto import mock_autoscaling

from . import get_all_instances
from .utils import setup_networking
from tests import EXAMPLE_AMI_ID


def setup_autoscale_group(asg_name="tester_group"):
    lc_name = str(uuid.uuid4())
    mocked_networking = setup_networking()
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        ImageId=EXAMPLE_AMI_ID,
        InstanceType="m1.small",
    )

    client.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchConfigurationName=lc_name,
        MinSize=2,
        MaxSize=2,
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )


@mock_autoscaling
def test_create_policy():
    asg_name = str(uuid.uuid4())
    setup_autoscale_group(asg_name)
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.put_scaling_policy(
        PolicyName="ScaleUp",
        AdjustmentType="ExactCapacity",
        AutoScalingGroupName=asg_name,
        ScalingAdjustment=3,
        Cooldown=60,
    )

    policy = client.describe_policies(AutoScalingGroupName=asg_name)["ScalingPolicies"][
        0
    ]
    policy["PolicyName"].should.equal("ScaleUp")
    policy["AdjustmentType"].should.equal("ExactCapacity")
    policy["AutoScalingGroupName"].should.equal(asg_name)
    policy["ScalingAdjustment"].should.equal(3)
    policy["Cooldown"].should.equal(60)


@mock_autoscaling
def test_create_policy_default_values():
    asg_name = str(uuid.uuid4())
    setup_autoscale_group(asg_name)
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.put_scaling_policy(
        PolicyName="ScaleUp",
        AdjustmentType="ExactCapacity",
        AutoScalingGroupName=asg_name,
        ScalingAdjustment=3,
    )

    policy = client.describe_policies(AutoScalingGroupName=asg_name)["ScalingPolicies"][
        0
    ]
    policy["PolicyName"].should.equal("ScaleUp")

    # Defaults
    policy["Cooldown"].should.equal(300)


@mock_autoscaling
def test_update_policy():
    asg_name = str(uuid.uuid4())
    setup_autoscale_group(asg_name)
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.put_scaling_policy(
        PolicyName="ScaleUp",
        AdjustmentType="ExactCapacity",
        AutoScalingGroupName=asg_name,
        ScalingAdjustment=3,
    )

    policies = client.describe_policies(AutoScalingGroupName=asg_name)[
        "ScalingPolicies"
    ]
    policies.should.have.length_of(1)
    policies[0]["ScalingAdjustment"].should.equal(3)

    # Now update it by creating another with the same name
    client.put_scaling_policy(
        PolicyName="ScaleUp",
        AdjustmentType="ExactCapacity",
        AutoScalingGroupName=asg_name,
        ScalingAdjustment=2,
    )
    policies = client.describe_policies(AutoScalingGroupName=asg_name)[
        "ScalingPolicies"
    ]
    policies.should.have.length_of(1)
    policies[0]["ScalingAdjustment"].should.equal(2)


@mock_autoscaling
def test_delete_policy():
    asg_name = str(uuid.uuid4())
    setup_autoscale_group(asg_name)
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.put_scaling_policy(
        PolicyName="ScaleUp",
        AdjustmentType="ExactCapacity",
        AutoScalingGroupName=asg_name,
        ScalingAdjustment=3,
    )

    client.describe_policies(AutoScalingGroupName=asg_name)[
        "ScalingPolicies"
    ].should.have.length_of(1)

    client.delete_policy(PolicyName="ScaleUp")
    client.describe_policies(AutoScalingGroupName=asg_name)[
        "ScalingPolicies"
    ].should.have.length_of(0)


@mock_autoscaling
def test_execute_policy_exact_capacity():
    asg_name = str(uuid.uuid4())
    setup_autoscale_group(asg_name)
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.put_scaling_policy(
        PolicyName="ScaleUp",
        AdjustmentType="ExactCapacity",
        AutoScalingGroupName=asg_name,
        ScalingAdjustment=3,
    )

    client.execute_policy(PolicyName="ScaleUp")

    instances = get_all_instances(client)
    ours = [i for i in instances if i["AutoScalingGroupName"] == asg_name]
    ours.should.have.length_of(3)


@mock_autoscaling
def test_execute_policy_positive_change_in_capacity():
    asg_name = str(uuid.uuid4())
    setup_autoscale_group(asg_name)
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.put_scaling_policy(
        PolicyName="ScaleUp",
        AdjustmentType="ChangeInCapacity",
        AutoScalingGroupName=asg_name,
        ScalingAdjustment=3,
    )

    client.execute_policy(PolicyName="ScaleUp")

    instances = get_all_instances(client)
    ours = [i for i in instances if i["AutoScalingGroupName"] == asg_name]
    ours.should.have.length_of(5)


@pytest.mark.parametrize(
    "adjustment,nr_of_instances", [(1, 3), (50, 3), (100, 4), (250, 7)]
)
@mock_autoscaling
def test_execute_policy_percent_change_in_capacity(adjustment, nr_of_instances):
    """http://docs.aws.amazon.com/AutoScaling/latest/DeveloperGuide/as-scale-based-on-demand.html
    If PercentChangeInCapacity returns a value between 0 and 1,
    Auto Scaling will round it off to 1."""
    asg_name = str(uuid.uuid4())
    setup_autoscale_group(asg_name=asg_name)
    client = boto3.client("autoscaling", region_name="us-east-1")
    client.put_scaling_policy(
        PolicyName="ScaleUp",
        AdjustmentType="PercentChangeInCapacity",
        AutoScalingGroupName=asg_name,
        ScalingAdjustment=adjustment,
    )

    client.execute_policy(PolicyName="ScaleUp")

    instances = get_all_instances(client)
    ours = [i for i in instances if i["AutoScalingGroupName"] == asg_name]
    ours.should.have.length_of(nr_of_instances)
