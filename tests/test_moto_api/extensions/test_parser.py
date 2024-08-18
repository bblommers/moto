import os

import pytest
from botocore.exceptions import UnknownServiceError

from moto.moto_api._internal.extensions.parser import ExtensionParser


valid_examples_dir = os.path.join(os.path.dirname(__file__), "valid_examples")
valid_examples = [os.path.join(valid_examples_dir, f) for f in os.listdir(valid_examples_dir) if os.path.isfile(os.path.join(valid_examples_dir, f))]


@pytest.mark.parametrize("file_name", valid_examples)
def test_valid_extensions_can_be_processed(file_name):
    ExtensionParser.parse_file(file_name)


def test_empty_extension():
    location = os.path.join(os.path.dirname(__file__), "invalid_examples", "empty_extension.py")
    with pytest.raises(AssertionError) as e:
        ExtensionParser.parse_file(location)
    assert "Should have at least a single class that extends moto.extensions.AbstractExtension" in str(e.value)


def test_no_extension():
    location = os.path.join(os.path.dirname(__file__), "invalid_examples", "no_extension.py")
    with pytest.raises(AssertionError) as e:
        ExtensionParser.parse_file(location)
    assert "Should have at least a single class that extends moto.extensions.AbstractExtension" in str(e.value)


def test_extension_with_init_params():
    location = os.path.join(os.path.dirname(__file__), "invalid_examples", "parametrized_init.py")
    with pytest.raises(AssertionError) as e:
        ExtensionParser.parse_file(location)
    assert "ParametrizedExtension should have only a single init-parameter called 'self'" in str(e.value)


def test_extension_with_multiple_init_methods():
    location = os.path.join(os.path.dirname(__file__), "invalid_examples", "multiple_inits.py")
    with pytest.raises(AssertionError) as e:
        ExtensionParser.parse_file(location)
    assert "NoOpExtensionWithMultipleInit should have at most a single __init__ method" in str(e.value)


def test_extension_without_identifier():
    location = os.path.join(os.path.dirname(__file__), "invalid_examples", "extension_without_id.py")
    with pytest.raises(AssertionError) as e:
        ExtensionParser.parse_file(location)
    assert "NoIdentifier should implement method extension_id()" in str(e.value)


def test_extension_with_empty_identifier():
    location = os.path.join(os.path.dirname(__file__), "invalid_examples", "invalid_id.py")
    with pytest.raises(AssertionError) as e:
        ExtensionParser.parse_file(location)
    assert "InvalidIdentifier.extension_id() should return a non-empty identifier" in str(e.value)


def test_extension_with_unknown_service():
    location = os.path.join(os.path.dirname(__file__), "invalid_examples", "unknown_service.py")
    with pytest.raises(UnknownServiceError) as e:
        ExtensionParser.parse_file(location)
