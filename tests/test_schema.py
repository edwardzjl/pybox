import json

import pytest
from pybox.schema import CreateKernelRequest
from pydantic import ValidationError


def test_create_kernel_request_env_none():
    request = CreateKernelRequest(name="test")
    assert request.name == "test"
    assert request.env is None


def test_create_kernel_request_env_empty():
    request = CreateKernelRequest(name="test", env={})
    assert request.env == {}


def test_create_kernel_request_env_with_str_values():
    request = CreateKernelRequest(name="test", env={"VAR1": "value1", "VAR2": "value2"})
    assert request.env == {"VAR1": "value1", "VAR2": "value2"}


def test_create_kernel_request_env_with_non_str_values():
    env = {
        "VAR1": "value1",
        "KERNEL_VOLUME_MOUNTS": {"mount1": "/mnt1", "mount2": "/mnt2"},
        "KERNEL_VOLUMES": [{"volume1": "data1"}, {"volume2": "data2"}],
    }
    request = CreateKernelRequest(name="test", env=env)
    assert request.env["VAR1"] == "value1"
    assert request.env["KERNEL_VOLUME_MOUNTS"] == json.dumps(env["KERNEL_VOLUME_MOUNTS"])
    assert request.env["KERNEL_VOLUMES"] == json.dumps(env["KERNEL_VOLUMES"])


def test_create_kernel_request_invalid_env():
    with pytest.raises(ValidationError):
        CreateKernelRequest(name="test", env="not a dict")


if __name__ == "__main__":
    pytest.main()
