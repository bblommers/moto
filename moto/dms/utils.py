from typing import Any, Iterable


def match_task_arn(task: dict[str, Any], arns: list[str]) -> bool:
    return task["ReplicationTaskArn"] in arns


def match_task_id(task: dict[str, Any], ids: list[str]) -> bool:
    return task["ReplicationTaskIdentifier"] in ids


def match_task_migration_type(task: dict[str, Any], migration_types: list[str]) -> bool:
    return task["MigrationType"] in migration_types


def match_task_endpoint_arn(task: dict[str, Any], endpoint_arns: list[str]) -> bool:
    return (
        task["SourceEndpointArn"] in endpoint_arns
        or task["TargetEndpointArn"] in endpoint_arns
    )


def match_task_replication_instance_arn(
    task: dict[str, Any], replication_instance_arns: list[str]
) -> bool:
    return task["ReplicationInstanceArn"] in replication_instance_arns


task_filter_functions = {
    "replication-task-arn": match_task_arn,
    "replication-task-id": match_task_id,
    "migration-type": match_task_migration_type,
    "endpoint-arn": match_task_endpoint_arn,
    "replication-instance-arn": match_task_replication_instance_arn,
}


def filter_tasks(tasks: Iterable[Any], filters: list[dict[str, Any]]) -> Any:
    matching_tasks = tasks

    for f in filters:
        filter_function = task_filter_functions.get(f["Name"])

        if not filter_function:
            continue

        matching_tasks = filter(
            lambda task: filter_function(task, f["Values"]), matching_tasks
        )

    return matching_tasks
