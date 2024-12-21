from typing import Any, Optional


def find_metric_transformation_by_name(
    metric_transformations: list[dict[str, Any]], metric_name: str
) -> Optional[dict[str, Any]]:
    for metric in metric_transformations:
        if metric["metricName"] == metric_name:
            return metric
    return None


def find_metric_transformation_by_namespace(
    metric_transformations: list[dict[str, Any]], metric_namespace: str
) -> Optional[dict[str, Any]]:
    for metric in metric_transformations:
        if metric["metricNamespace"] == metric_namespace:
            return metric
    return None


class MetricFilters:
    def __init__(self) -> None:
        self.metric_filters: list[dict[str, Any]] = []

    def add_filter(
        self,
        filter_name: str,
        filter_pattern: str,
        log_group_name: str,
        metric_transformations: str,
    ) -> None:
        self.metric_filters.append(
            {
                "filterName": filter_name,
                "filterPattern": filter_pattern,
                "logGroupName": log_group_name,
                "metricTransformations": metric_transformations,
            }
        )

    def get_matching_filters(
        self,
        prefix: str | None = None,
        log_group_name: str | None = None,
        metric_name: str | None = None,
        metric_namespace: str | None = None,
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for f in self.metric_filters:
            prefix_matches = prefix is None or f["filterName"].startswith(prefix)
            log_group_matches = (
                log_group_name is None or f["logGroupName"] == log_group_name
            )
            metric_name_matches = (
                metric_name is None
                or find_metric_transformation_by_name(
                    f["metricTransformations"], metric_name
                )
            )
            namespace_matches = (
                metric_namespace is None
                or find_metric_transformation_by_namespace(
                    f["metricTransformations"], metric_namespace
                )
            )

            if (
                prefix_matches
                and log_group_matches
                and metric_name_matches
                and namespace_matches
            ):
                result.append(f)

        return result

    def delete_filter(
        self, filter_name: str | None = None, log_group_name: str | None = None
    ) -> list[dict[str, Any]]:
        for f in self.metric_filters:
            if f["filterName"] == filter_name and f["logGroupName"] == log_group_name:
                self.metric_filters.remove(f)
        return self.metric_filters
