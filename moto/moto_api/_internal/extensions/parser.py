from moto.extensions import AbstractExtension

import ast
import importlib.util
import sys
from typing import Dict

import boto3
from moto.core.responses import _get_service_operations


class ExtensionParser:
    @staticmethod
    def parse_file(file_location: str) -> Dict[str, AbstractExtension]:
        # Load/Parse file content
        with open(file_location, mode="r") as f:
            file_content = f.read()
        parsed_file = ast.parse(file_content)

        # Should contain at least one extension
        classes = [c for c in parsed_file.body if isinstance(c, ast.ClassDef)]
        extension_definitions = [class_def for class_def in classes if "AbstractExtension" in [base.id for base in class_def.bases]]
        assert extension_definitions, "Should have at least a single class that extends moto.extensions.AbstractExtension"

        # Extensions should have a valid __init__
        for extension in extension_definitions:
            init_methods = [m for m in extension.body if isinstance(m, ast.FunctionDef) and m.name == "__init__"]
            assert len(init_methods) < 2, f"{extension.name} should have at most a single __init__ method"
            for init_method in init_methods:
                args = init_method.args.args
                assert [arg.arg for arg in args] == ["self"], f"{extension.name} should have only a single init-parameter called 'self'"

            assert [m for m in extension.body if isinstance(m, ast.FunctionDef) and m.name == "extension_id"], f"{extension.name} should implement method extension_id()"

        # Load the module
        file_name = file_location.split("/")[-1]
        spec = importlib.util.spec_from_file_location(file_name, file_location)
        extension_module = importlib.util.module_from_spec(spec)
        sys.modules[file_name] = extension_module
        spec.loader.exec_module(extension_module)

        # Instantiate the extensions
        extension_instances = {}
        for extension in extension_definitions:
            extension_instance = getattr(extension_module, extension.name)()

            # Validate the ServiceFeatures exist
            for feature in extension_instance.invoke_on():
                service, method = feature.split(":") if ":" in feature else (feature, feature)
                conn = boto3.client(service, region_name="us-east-1")
                op_names = _get_service_operations(conn, service)
                assert method in op_names, f"{feature} is not a recognized AWS feature"

            extension_id = extension_instance.extension_id()
            assert isinstance(extension_id, str) and len(extension_id) > 0, f"{extension.name}.extension_id() should return a non-empty identifier"
            extension_instances[extension_id] = extension_instance

        return extension_instances
