import os
from collections.abc import Iterator
from uuid import uuid4

import pytest
from pybox import LocalPyBoxManager


@pytest.fixture(scope="module")
def local_manager() -> Iterator[LocalPyBoxManager]:
    _mng = LocalPyBoxManager()
    yield _mng
    _mng.kernel_manager.shutdown_all(now=True)


def test_start_w_id(local_manager: LocalPyBoxManager):
    kernel_id = str(uuid4())
    box = local_manager.start(kernel_id)

    assert box.kernel_id == kernel_id
    assert box.kernel_id in local_manager.kernel_manager
    assert local_manager.kernel_manager.is_alive(box.kernel_id)


@pytest.mark.asyncio
async def test_start_async_w_id(local_manager: LocalPyBoxManager):
    kernel_id = str(uuid4())
    box = await local_manager.astart(kernel_id)

    assert box.kernel_id == kernel_id
    assert box.kernel_id in local_manager.async_kernel_manager
    assert local_manager.async_kernel_manager.is_alive(box.kernel_id)


def test_box_lifecycle(local_manager: LocalPyBoxManager):
    box = local_manager.start()

    assert box.kernel_id in local_manager.kernel_manager
    assert local_manager.kernel_manager.is_alive(box.kernel_id)

    local_manager.shutdown(box.kernel_id)

    assert box.kernel_id not in local_manager.kernel_manager


@pytest.mark.asyncio
async def test_box_lifecycle_async(local_manager: LocalPyBoxManager):
    box = await local_manager.astart()

    assert box.kernel_id in local_manager.async_kernel_manager
    assert local_manager.async_kernel_manager.is_alive(box.kernel_id)

    await local_manager.ashutdown(box.kernel_id)

    assert box.kernel_id not in local_manager.async_kernel_manager


def test_set_cwd(local_manager: LocalPyBoxManager):
    # even we don't set the cwd, it defaults to os.getcwd()
    # in order to test this is working, we need to change the cwd to a cross platform path
    kernel = local_manager.start(cwd=os.path.expanduser("~"))

    test_code = "import os\nprint(os.getcwd())"
    out = kernel.run(code=test_code)

    assert os.path.expanduser("~") + "\n" == out.text


@pytest.mark.asyncio
async def test_set_cwd_async(local_manager: LocalPyBoxManager):
    # even we don't set the cwd, it defaults to os.getcwd()
    # in order to test this is working, we need to change the cwd to a cross platform path
    kernel = await local_manager.astart(cwd=os.path.expanduser("~"))

    test_code = "import os\nprint(os.getcwd())"
    out = await kernel.arun(code=test_code)

    assert os.path.expanduser("~") + "\n" == out.text
