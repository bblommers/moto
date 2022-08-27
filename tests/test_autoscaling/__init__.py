def get_all_launch_configs(client):
    resp = client.describe_launch_configurations()
    launch_configs = resp["LaunchConfigurations"]
    while resp.get("NextToken"):
        resp = client.describe_launch_configurations(NextToken=resp["NextToken"])
        launch_configs.extend(resp["LaunchConfigurations"])
    return launch_configs


def get_all_instances(client):
    resp = client.describe_auto_scaling_instances()
    instances = resp["AutoScalingInstances"]
    while resp.get("NextToken"):
        resp = client.describe_auto_scaling_instances(NextToken=resp["NextToken"])
        instances.extend(resp["AutoScalingInstances"])
    return instances


def get_all_groups(client):
    resp = client.describe_auto_scaling_groups()
    groups = resp["AutoScalingGroups"]
    while resp.get("NextToken"):
        resp = client.describe_auto_scaling_groups(NextToken=resp["NextToken"])
        groups.extend(resp["AutoScalingGroups"])
    return groups


def get_all_tags(client):
    resp = client.describe_tags()
    tags = resp["Tags"]
    while resp.get("NextToken"):
        resp = client.describe_tags(NextToken=resp["NextToken"])
        tags.extend(resp["Tags"])
    return tags
