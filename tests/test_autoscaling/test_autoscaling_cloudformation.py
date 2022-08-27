import boto3
import sure  # noqa # pylint: disable=unused-import
import json

from moto import mock_autoscaling, mock_cloudformation, mock_ec2, mock_elb

from . import get_all_groups
from .utils import setup_networking
from tests import EXAMPLE_AMI_ID
from uuid import uuid4


@mock_autoscaling
@mock_cloudformation
def test_launch_configuration():
    cf_client = boto3.client("cloudformation", region_name="us-east-1")
    client = boto3.client("autoscaling", region_name="us-east-1")
    lc_name = str(uuid4())
    stack_name = str(uuid4())[0:6]

    cf_template = """
Resources:
    LaunchConfiguration:
        Type: AWS::AutoScaling::LaunchConfiguration
        Properties:
            ImageId: {0}
            InstanceType: t2.micro
            LaunchConfigurationName: {1}
Outputs:
    LaunchConfigurationName:
        Value: !Ref LaunchConfiguration
""".strip().format(
        EXAMPLE_AMI_ID, lc_name
    )

    cf_client.create_stack(StackName=stack_name, TemplateBody=cf_template)
    stack = cf_client.describe_stacks(StackName=stack_name)["Stacks"][0]
    stack["Outputs"][0]["OutputValue"].should.be.equal(lc_name)

    res = client.describe_launch_configurations(LaunchConfigurationNames=[lc_name])
    lc = res["LaunchConfigurations"][0]
    lc["LaunchConfigurationName"].should.be.equal(lc_name)
    lc["ImageId"].should.be.equal(EXAMPLE_AMI_ID)
    lc["InstanceType"].should.be.equal("t2.micro")

    cf_template = """
Resources:
    LaunchConfiguration:
        Type: AWS::AutoScaling::LaunchConfiguration
        Properties:
            ImageId: {0}
            InstanceType: m5.large
            LaunchConfigurationName: {1}
Outputs:
    LaunchConfigurationName:
        Value: !Ref LaunchConfiguration
""".strip().format(
        EXAMPLE_AMI_ID, lc_name
    )

    cf_client.update_stack(StackName=stack_name, TemplateBody=cf_template)
    stack = cf_client.describe_stacks(StackName=stack_name)["Stacks"][0]
    stack["Outputs"][0]["OutputValue"].should.be.equal(lc_name)

    res = client.describe_launch_configurations(LaunchConfigurationNames=[lc_name])
    lc = res["LaunchConfigurations"][0]
    lc["LaunchConfigurationName"].should.be.equal(lc_name)
    lc["ImageId"].should.be.equal(EXAMPLE_AMI_ID)
    lc["InstanceType"].should.be.equal("m5.large")


@mock_autoscaling
@mock_cloudformation
def test_autoscaling_group_from_launch_config():
    subnet_id = setup_networking()["subnet1"]

    cf_client = boto3.client("cloudformation", region_name="us-east-1")
    client = boto3.client("autoscaling", region_name="us-east-1")
    asg_name = str(uuid4())[0:6]
    lc_name = str(uuid4())[0:6]
    lc_name2 = str(uuid4())[0:6]

    client.create_launch_configuration(
        LaunchConfigurationName=lc_name,
        InstanceType="t2.micro",
        ImageId=EXAMPLE_AMI_ID,
    )
    stack_name = str(uuid4())

    cf_template = """
Parameters:
    SubnetId:
        Type: AWS::EC2::Subnet::Id
Resources:
    AutoScalingGroup:
        Type: AWS::AutoScaling::AutoScalingGroup
        Properties:
            AutoScalingGroupName: {0}
            AvailabilityZones:
                - us-east-1a
            LaunchConfigurationName: {1}
            MaxSize: "5"
            MinSize: "1"
            VPCZoneIdentifier:
                - !Ref SubnetId
Outputs:
    AutoScalingGroupName:
        Value: !Ref AutoScalingGroup
""".strip().format(
        asg_name, lc_name
    )

    cf_client.create_stack(
        StackName=stack_name,
        TemplateBody=cf_template,
        Parameters=[{"ParameterKey": "SubnetId", "ParameterValue": subnet_id}],
    )
    stack = cf_client.describe_stacks(StackName=stack_name)["Stacks"][0]
    stack["Outputs"][0]["OutputValue"].should.be.equal(asg_name)

    asg = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])[
        "AutoScalingGroups"
    ][0]
    asg["AutoScalingGroupName"].should.be.equal(asg_name)
    asg["MinSize"].should.be.equal(1)
    asg["MaxSize"].should.be.equal(5)
    asg["LaunchConfigurationName"].should.be.equal(lc_name)

    client.create_launch_configuration(
        LaunchConfigurationName=lc_name2,
        InstanceType="t2.micro",
        ImageId=EXAMPLE_AMI_ID,
    )

    cf_template = """
Parameters:
    SubnetId:
        Type: AWS::EC2::Subnet::Id
Resources:
    AutoScalingGroup:
        Type: AWS::AutoScaling::AutoScalingGroup
        Properties:
            AutoScalingGroupName: {0}
            AvailabilityZones:
                - us-east-1a
            LaunchConfigurationName: {1}
            MaxSize: "6"
            MinSize: "2"
            VPCZoneIdentifier:
                - !Ref SubnetId
Outputs:
    AutoScalingGroupName:
        Value: !Ref AutoScalingGroup
""".strip().format(
        asg_name, lc_name2
    )

    cf_client.update_stack(
        StackName=stack_name,
        TemplateBody=cf_template,
        Parameters=[{"ParameterKey": "SubnetId", "ParameterValue": subnet_id}],
    )
    stack = cf_client.describe_stacks(StackName=stack_name)["Stacks"][0]
    stack["Outputs"][0]["OutputValue"].should.be.equal(asg_name)

    res = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    asg = res["AutoScalingGroups"][0]
    asg["AutoScalingGroupName"].should.be.equal(asg_name)
    asg["MinSize"].should.be.equal(2)
    asg["MaxSize"].should.be.equal(6)
    asg["LaunchConfigurationName"].should.be.equal(lc_name2)


@mock_autoscaling
@mock_cloudformation
@mock_ec2
def test_autoscaling_group_from_launch_template():
    subnet_id = setup_networking()["subnet1"]

    cf_client = boto3.client("cloudformation", region_name="us-east-1")
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    client = boto3.client("autoscaling", region_name="us-east-1")

    lt_name = str(uuid4())
    lt_name2 = str(uuid4())
    template_response = ec2_client.create_launch_template(
        LaunchTemplateName=lt_name,
        LaunchTemplateData={"ImageId": EXAMPLE_AMI_ID, "InstanceType": "t2.micro"},
    )
    launch_template_id = template_response["LaunchTemplate"]["LaunchTemplateId"]
    stack_name = str(uuid4())
    asg_name = str(uuid4())

    cf_template = """
Parameters:
    SubnetId:
        Type: AWS::EC2::Subnet::Id
    LaunchTemplateId:
        Type: String
Resources:
    AutoScalingGroup:
        Type: AWS::AutoScaling::AutoScalingGroup
        Properties:
            AutoScalingGroupName: {0}
            AvailabilityZones:
                - us-east-1a
            LaunchTemplate:
                LaunchTemplateId: !Ref LaunchTemplateId
                Version: "1"
            MaxSize: "5"
            MinSize: "1"
            VPCZoneIdentifier:
                - !Ref SubnetId
Outputs:
    AutoScalingGroupName:
        Value: !Ref AutoScalingGroup
""".strip().format(
        asg_name
    )

    cf_client.create_stack(
        StackName=stack_name,
        TemplateBody=cf_template,
        Parameters=[
            {"ParameterKey": "SubnetId", "ParameterValue": subnet_id},
            {"ParameterKey": "LaunchTemplateId", "ParameterValue": launch_template_id},
        ],
    )
    stack = cf_client.describe_stacks(StackName=stack_name)["Stacks"][0]
    stack["Outputs"][0]["OutputValue"].should.be.equal(asg_name)

    asg = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])[
        "AutoScalingGroups"
    ][0]
    asg["AutoScalingGroupName"].should.be.equal(asg_name)
    asg["MinSize"].should.be.equal(1)
    asg["MaxSize"].should.be.equal(5)
    lt = asg["LaunchTemplate"]
    lt["LaunchTemplateId"].should.be.equal(launch_template_id)
    lt["LaunchTemplateName"].should.be.equal(lt_name)
    lt["Version"].should.be.equal("1")

    template_response = ec2_client.create_launch_template(
        LaunchTemplateName=lt_name2,
        LaunchTemplateData={"ImageId": EXAMPLE_AMI_ID, "InstanceType": "m5.large"},
    )
    launch_template_id = template_response["LaunchTemplate"]["LaunchTemplateId"]

    cf_template = """
Parameters:
    SubnetId:
        Type: AWS::EC2::Subnet::Id
    LaunchTemplateId:
        Type: String
Resources:
    AutoScalingGroup:
        Type: AWS::AutoScaling::AutoScalingGroup
        Properties:
            AutoScalingGroupName: {0}
            AvailabilityZones:
                - us-east-1a
            LaunchTemplate:
                LaunchTemplateId: !Ref LaunchTemplateId
                Version: "1"
            MaxSize: "6"
            MinSize: "2"
            VPCZoneIdentifier:
                - !Ref SubnetId
Outputs:
    AutoScalingGroupName:
        Value: !Ref AutoScalingGroup
""".strip().format(
        asg_name
    )

    cf_client.update_stack(
        StackName=stack_name,
        TemplateBody=cf_template,
        Parameters=[
            {"ParameterKey": "SubnetId", "ParameterValue": subnet_id},
            {"ParameterKey": "LaunchTemplateId", "ParameterValue": launch_template_id},
        ],
    )
    stack = cf_client.describe_stacks(StackName=stack_name)["Stacks"][0]
    stack["Outputs"][0]["OutputValue"].should.be.equal(asg_name)

    asg = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])[
        "AutoScalingGroups"
    ][0]
    asg["AutoScalingGroupName"].should.be.equal(asg_name)
    asg["MinSize"].should.be.equal(2)
    asg["MaxSize"].should.be.equal(6)
    lt = asg["LaunchTemplate"]
    lt["LaunchTemplateId"].should.be.equal(launch_template_id)
    lt["LaunchTemplateName"].should.be.equal(lt_name2)
    lt["Version"].should.be.equal("1")


@mock_autoscaling
@mock_elb
@mock_cloudformation
@mock_ec2
def test_autoscaling_group_with_elb():
    asg_name = str(uuid4())
    as_logical = str(uuid4())[0:6]
    elb_name = str(uuid4())
    stack_name = str(uuid4())
    web_setup_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            as_logical: {
                "Type": "AWS::AutoScaling::AutoScalingGroup",
                "Properties": {
                    "AvailabilityZones": ["us-east-1a"],
                    "LaunchConfigurationName": {"Ref": "my-launch-config"},
                    "MinSize": "2",
                    "MaxSize": "2",
                    "DesiredCapacity": "2",
                    "LoadBalancerNames": [{"Ref": "my-elb"}],
                    "Tags": [
                        {
                            "Key": "propagated-test-tag",
                            "Value": "propagated-test-tag-value",
                            "PropagateAtLaunch": True,
                        },
                        {
                            "Key": "not-propagated-test-tag",
                            "Value": "not-propagated-test-tag-value",
                            "PropagateAtLaunch": False,
                        },
                    ],
                },
            },
            "ScheduledAction": {
                "Type": "AWS::AutoScaling::ScheduledAction",
                "Properties": {
                    "AutoScalingGroupName": asg_name,
                    "DesiredCapacity": 10,
                    "EndTime": "2022-08-01T00:00:00Z",
                    "MaxSize": 15,
                    "MinSize": 5,
                    "Recurrence": "* * * * *",
                    "StartTime": "2022-07-01T00:00:00Z",
                },
            },
            "my-launch-config": {
                "Type": "AWS::AutoScaling::LaunchConfiguration",
                "Properties": {
                    "ImageId": EXAMPLE_AMI_ID,
                    "InstanceType": "t2.medium",
                    "UserData": "some user data",
                },
            },
            "my-elb": {
                "Type": "AWS::ElasticLoadBalancing::LoadBalancer",
                "Properties": {
                    "AvailabilityZones": ["us-east-1a"],
                    "Listeners": [
                        {
                            "LoadBalancerPort": "80",
                            "InstancePort": "80",
                            "Protocol": "HTTP",
                        }
                    ],
                    "LoadBalancerName": elb_name,
                    "HealthCheck": {
                        "Target": "HTTP:80",
                        "HealthyThreshold": "3",
                        "UnhealthyThreshold": "5",
                        "Interval": "30",
                        "Timeout": "5",
                    },
                },
            },
        },
    }

    web_setup_template_json = json.dumps(web_setup_template)

    cf = boto3.client("cloudformation", region_name="us-east-1")
    ec2 = boto3.client("ec2", region_name="us-east-1")
    elb = boto3.client("elb", region_name="us-east-1")
    client = boto3.client("autoscaling", region_name="us-east-1")

    cf.create_stack(StackName=stack_name, TemplateBody=web_setup_template_json)

    resp = get_all_groups(client)
    autoscale_group = [a for a in resp if as_logical in a["AutoScalingGroupName"]][0]
    autoscale_group["LoadBalancerNames"].should.equal([elb_name])
    autoscale_group["LaunchConfigurationName"].should.contain("my-launch-config")
    instance_ids = [i["InstanceId"] for i in autoscale_group["Instances"]]

    # Confirm the Launch config was actually created
    resp = client.describe_launch_configurations(
        LaunchConfigurationNames=[autoscale_group["LaunchConfigurationName"]]
    )
    resp["LaunchConfigurations"].should.have.length_of(1)

    # Confirm the ELB was actually created
    resp = elb.describe_load_balancers(LoadBalancerNames=[elb_name])
    resp["LoadBalancerDescriptions"].should.have.length_of(1)

    resources = cf.list_stack_resources(StackName=stack_name)["StackResourceSummaries"]
    as_group_resource = [
        resource
        for resource in resources
        if resource["ResourceType"] == "AWS::AutoScaling::AutoScalingGroup"
    ][0]
    as_group_resource["PhysicalResourceId"].should.contain(as_logical)

    launch_config_resource = [
        resource
        for resource in resources
        if resource["ResourceType"] == "AWS::AutoScaling::LaunchConfiguration"
    ][0]
    launch_config_resource["PhysicalResourceId"].should.contain("my-launch-config")

    elb_resource = [
        resource
        for resource in resources
        if resource["ResourceType"] == "AWS::ElasticLoadBalancing::LoadBalancer"
    ][0]
    elb_resource["PhysicalResourceId"].should.contain(elb_name)

    # confirm the instances were created with the right tags
    reservations = ec2.describe_instances(InstanceIds=instance_ids)["Reservations"]

    reservations.should.have.length_of(1)
    reservations[0]["Instances"].should.have.length_of(2)
    for instance in reservations[0]["Instances"]:
        tag_keys = [t["Key"] for t in instance["Tags"]]
        tag_keys.should.contain("propagated-test-tag")
        tag_keys.should_not.contain("not-propagated-test-tag")

    # confirm scheduled scaling action was created
    response = client.describe_scheduled_actions(AutoScalingGroupName=asg_name)[
        "ScheduledUpdateGroupActions"
    ]
    response.should.have.length_of(1)


@mock_autoscaling
@mock_cloudformation
@mock_ec2
def test_autoscaling_group_update():
    as_id = str(uuid4())[0:6]
    asg_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            as_id: {
                "Type": "AWS::AutoScaling::AutoScalingGroup",
                "Properties": {
                    "AvailabilityZones": ["us-west-1a"],
                    "LaunchConfigurationName": {"Ref": "my-launch-config"},
                    "MinSize": "2",
                    "MaxSize": "2",
                    "DesiredCapacity": "2",
                },
            },
            "my-launch-config": {
                "Type": "AWS::AutoScaling::LaunchConfiguration",
                "Properties": {
                    "ImageId": EXAMPLE_AMI_ID,
                    "InstanceType": "t2.medium",
                    "UserData": "some user data",
                },
            },
        },
    }
    asg_template_json = json.dumps(asg_template)
    stack_name = str(uuid4())

    cf = boto3.client("cloudformation", region_name="us-west-1")
    ec2 = boto3.client("ec2", region_name="us-west-1")
    client = boto3.client("autoscaling", region_name="us-west-1")
    cf.create_stack(StackName=stack_name, TemplateBody=asg_template_json)

    asgs = get_all_groups(client)
    asg = [a for a in asgs if as_id in a["AutoScalingGroupName"]][0]
    asg_name1 = asg["AutoScalingGroupName"]
    asg["MinSize"].should.equal(2)
    asg["MaxSize"].should.equal(2)
    asg["DesiredCapacity"].should.equal(2)

    asg_template["Resources"][as_id]["Properties"]["MaxSize"] = 3
    asg_template["Resources"][as_id]["Properties"]["Tags"] = [
        {
            "Key": "propagated-test-tag",
            "Value": "propagated-test-tag-value",
            "PropagateAtLaunch": True,
        },
        {
            "Key": "not-propagated-test-tag",
            "Value": "not-propagated-test-tag-value",
            "PropagateAtLaunch": False,
        },
    ]
    asg_template_json = json.dumps(asg_template)
    cf.update_stack(StackName=stack_name, TemplateBody=asg_template_json)
    asgs = get_all_groups(client)
    asg = [a for a in asgs if as_id in a["AutoScalingGroupName"]][0]
    asg_name2 = asg["AutoScalingGroupName"]
    asg["MinSize"].should.equal(2)
    asg["MaxSize"].should.equal(3)
    asg["DesiredCapacity"].should.equal(2)

    # confirm the instances were created with the right tags
    reservations = ec2.describe_instances(
        Filters=[
            {"Name": "tag:aws:autoscaling:groupName", "Values": [asg_name1, asg_name2]}
        ]
    )["Reservations"]
    running_instance_count = 0
    for res in reservations:
        for instance in res["Instances"]:
            if instance["State"]["Name"] == "running":
                running_instance_count += 1
                instance["Tags"].should.contain(
                    {"Key": "propagated-test-tag", "Value": "propagated-test-tag-value"}
                )
                tag_keys = [t["Key"] for t in instance["Tags"]]
                tag_keys.should_not.contain("not-propagated-test-tag")
    running_instance_count.should.equal(2)
