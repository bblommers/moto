from typing import Any

from moto.utilities.utils import get_partition


def parameter_arn(account_id: str, region: str, parameter_name: str) -> str:
    if parameter_name[0] == "/":
        parameter_name = parameter_name[1:]
    return f"arn:{get_partition(region)}:ssm:{region}:{account_id}:parameter/{parameter_name}"


def convert_to_tree(parameters: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Convert input into a smaller, less redundant data set in tree form
    Input: [{"Name": "/a/b/c", "Value": "af-south-1", ...}, ..]
    Output: {"a": {"b": {"c": {"Value": af-south-1}, ..}, ..}, ..}
    """
    tree_dict: dict[str, Any] = {}
    for p in parameters:
        current_level = tree_dict
        for path in p["Name"].split("/"):
            if path == "":
                continue
            if path not in current_level:
                current_level[path] = {}
            current_level = current_level[path]
        current_level["Value"] = p["Value"]
    return tree_dict


def convert_to_params(tree: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Inverse of 'convert_to_tree'
    """

    def m(
        tree: dict[str, Any], params: list[dict[str, Any]], current_path: str = ""
    ) -> None:
        for key, value in tree.items():
            if key == "Value":
                params.append({"Name": current_path, "Value": value})
            else:
                m(value, params, current_path + "/" + key)

    params: list[dict[str, Any]] = []
    m(tree, params)
    return params
