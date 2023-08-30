import functools
import inspect
import itertools
import os
import re
import unittest
from types import FunctionType
from typing import Any, Callable, Dict, Optional, Set, TypeVar, Union
from typing import ContextManager
from typing_extensions import ParamSpec
from unittest.mock import patch

import boto3
import botocore
import responses
from botocore.config import Config
from botocore.handlers import BUILTIN_HANDLERS

#import moto.backends as backends
from moto import settings
from .base_backend import BackendDict
from .botocore_stub_all import BotocoreStubOnDemand
from .custom_responses_mock import (
    get_response_mock,
    CallbackResponse,
    not_implemented_callback,
    reset_responses_mock,

)
from .models import DEFAULT_ACCOUNT_ID
from .model_instances import reset_model_data

botocore_stub_on_demand = BotocoreStubOnDemand()
BUILTIN_HANDLERS.append(("before-send", botocore_stub_on_demand))

P = ParamSpec("P")
T = TypeVar("T")


class MockAWS(ContextManager["MockAWS"]):
    nested_count = 0
    mocks_active = False

    def __init__(self):
        from moto.instance_metadata import instance_metadata_backends
        from moto.moto_api._internal.models import moto_api_backend

        self.backends = {}
        default_account_id = DEFAULT_ACCOUNT_ID
        default_backends = [
            instance_metadata_backends[default_account_id]["global"],
            moto_api_backend,
        ]

        self.FAKE_KEYS = {
            "AWS_ACCESS_KEY_ID": "foobar_key",
            "AWS_SECRET_ACCESS_KEY": "foobar_secret",
        }
        self.ORIG_KEYS: Dict[str, Optional[str]] = {}
        self.default_session_mock = patch("boto3.DEFAULT_SESSION", None)

        if self.__class__.nested_count == 0:
            print("nested_count == 0")
            self.reset()  # type: ignore[attr-defined]

    def __call__(
        self,
        func: Callable[P, T],
        reset: bool = True,
        remove_data: bool = True,
    ) -> Callable[P, T]:
        if inspect.isclass(func):
            # TODO
            return self.decorate_class(func)  # type: ignore
        return self.decorate_callable(func, reset, remove_data)

    def __enter__(self) -> "BaseMockAWS":
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()

    def start(self, reset: bool = True) -> None:
        if not self.__class__.mocks_active:
            self.default_session_mock.start()
            self.mock_env_variables()
            self.__class__.mocks_active = True

        self.__class__.nested_count += 1
        #if reset:
        #    for backend in self.backends.values():
        #        backend.reset()

        self.enable_patching(reset=False)  # type: ignore[attr-defined]

    def stop(self, remove_data: bool = True) -> None:
        self.__class__.nested_count -= 1

        if self.__class__.nested_count < 0:
            raise RuntimeError("Called stop() before start().")

        if self.__class__.nested_count == 0:
            if self.__class__.mocks_active:
                try:
                    self.default_session_mock.stop()
                except RuntimeError:
                    # We only need to check for this exception in Python 3.7
                    # https://bugs.python.org/issue36366
                    pass
                self.unmock_env_variables()
                self.__class__.mocks_active = False
                if remove_data:
                    # Reset the data across all backends
                    for backend in self.backends.values():
                        backend.reset()
                    # Remove references to all model instances that were created
                    reset_model_data()
            self.disable_patching()  # type: ignore[attr-defined]

    def decorate_callable(
        self, func: Callable[..., "BaseMockAWS"], reset: bool, remove_data: bool
    ) -> Callable[..., "BaseMockAWS"]:
        def wrapper(*args: Any, **kwargs: Any) -> "BaseMockAWS":
            self.start(reset=reset)
            try:
                result = func(*args, **kwargs)
            finally:
                self.stop(remove_data=remove_data)
            return result

        functools.update_wrapper(wrapper, func)
        wrapper.__wrapped__ = func  # type: ignore[attr-defined]
        return wrapper

    def mock_env_variables(self) -> None:
        # "Mock" the AWS credentials as they can't be mocked in Botocore currently
        # self.env_variables_mocks = mock.patch.dict(os.environ, FAKE_KEYS)
        # self.env_variables_mocks.start()
        for k, v in self.FAKE_KEYS.items():
            self.ORIG_KEYS[k] = os.environ.get(k, None)
            os.environ[k] = v

    def unmock_env_variables(self) -> None:
        # This doesn't work in Python2 - for some reason, unmocking clears the entire os.environ dict
        # Obviously bad user experience, and also breaks pytest - as it uses PYTEST_CURRENT_TEST as an env var
        # self.env_variables_mocks.stop()
        for k, v in self.ORIG_KEYS.items():
            if v:
                os.environ[k] = v
            else:
                del os.environ[k]

    def reset(self):
        botocore_stub_on_demand.reset()

    def enable_patching(self, reset: bool) -> None:
        botocore_stub_on_demand.enabled = True

    def disable_patching(self) -> None:
        print("disable_patching")
        botocore_stub_on_demand.enabled = False
        self.reset()

        try:
            pass #responses_mock.stop()
        except RuntimeError:
            pass
