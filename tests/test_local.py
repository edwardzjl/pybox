import os
from collections.abc import Iterator
from typing import AsyncIterator
from uuid import uuid4

import pytest
import pytest_asyncio
from pybox import LocalPyBoxManager, PyBoxOut
from pybox.local import LocalPyBox


@pytest.fixture(scope="module")
def local_manager() -> Iterator[LocalPyBoxManager]:
    _mng = LocalPyBoxManager()
    yield _mng
    _mng.kernel_manager.shutdown_all(now=True)


@pytest_asyncio.fixture(scope="module")
async def alocal_manager() -> AsyncIterator[LocalPyBoxManager]:
    _mng = LocalPyBoxManager()
    yield _mng
    await _mng.async_kernel_manager.shutdown_all(now=True)


def test_start_w_id(local_manager: LocalPyBoxManager):
    kernel_id = str(uuid4())
    box = local_manager.start(kernel_id)

    assert box.kernel_id == kernel_id
    assert box.kernel_id in local_manager.kernel_manager
    assert local_manager.kernel_manager.is_alive(box.kernel_id)


@pytest.mark.asyncio
async def test_start_async_w_id(alocal_manager: LocalPyBoxManager):
    kernel_id = str(uuid4())
    box = await alocal_manager.astart(kernel_id)

    assert box.kernel_id == kernel_id
    assert box.kernel_id in alocal_manager.async_kernel_manager
    assert await alocal_manager.async_kernel_manager.is_alive(box.kernel_id)


def test_box_lifecycle(local_manager: LocalPyBoxManager):
    box = local_manager.start()

    assert box.kernel_id in local_manager.kernel_manager
    assert local_manager.kernel_manager.is_alive(box.kernel_id)

    local_manager.shutdown(box.kernel_id)

    assert box.kernel_id not in local_manager.kernel_manager


@pytest.mark.asyncio
async def test_box_lifecycle_async(alocal_manager: LocalPyBoxManager):
    box = await alocal_manager.astart()

    assert box.kernel_id in alocal_manager.async_kernel_manager
    assert await alocal_manager.async_kernel_manager.is_alive(box.kernel_id)

    await alocal_manager.ashutdown(box.kernel_id)

    assert box.kernel_id not in alocal_manager.async_kernel_manager


def test_set_cwd(local_manager: LocalPyBoxManager):
    # even we don't set the cwd, it defaults to os.getcwd()
    # in order to test this is working, we need to change the cwd to a cross platform path
    kernel = local_manager.start(cwd=os.path.expanduser("~"))

    test_code = "import os\nprint(os.getcwd())"
    out: PyBoxOut = kernel.run(code=test_code)
    assert len(out.data) == 1
    assert os.path.expanduser("~") + "\n" == out.data[0]["text/plain"]


@pytest.mark.asyncio
async def test_set_cwd_async(alocal_manager: LocalPyBoxManager):
    # even we don't set the cwd, it defaults to os.getcwd()
    # in order to test this is working, we need to change the cwd to a cross platform path
    kernel = await alocal_manager.astart(cwd=os.path.expanduser("~"))

    test_code = "import os\nprint(os.getcwd())"
    out: PyBoxOut = await kernel.arun(code=test_code)
    assert len(out.data) == 1
    assert os.path.expanduser("~") + "\n" == out.data[0]["text/plain"]


@pytest.fixture(scope="module")
def local_box(local_manager: LocalPyBoxManager) -> Iterator[LocalPyBox]:
    _box = local_manager.start()
    yield _box
    local_manager.shutdown(_box.kernel_id)


@pytest_asyncio.fixture(scope="module")
async def local_box_async(alocal_manager: LocalPyBoxManager) -> AsyncIterator[LocalPyBox]:
    _box = await alocal_manager.astart()
    yield _box
    await alocal_manager.ashutdown(_box.kernel_id)


def test_code_execute(local_box: LocalPyBox):
    test_code = "print('test')"
    out: PyBoxOut = local_box.run(code=test_code)

    assert len(out.data) == 1
    assert out.data[0]["text/plain"] == "test\n"


@pytest.mark.asyncio
async def test_code_execute_async(local_box_async: LocalPyBox):
    test_code = "print('test')"
    out = await local_box_async.arun(code=test_code)

    assert len(out.data) == 1
    assert out.data[0]["text/plain"] == "test\n"


def test_variable_reuse(local_box: LocalPyBox):
    code_round1 = """a = 1
print(a)"""
    local_box.run(code=code_round1)
    code_round2 = """a += 1
print(a)"""
    out: PyBoxOut = local_box.run(code=code_round2)

    assert len(out.data) == 1
    assert out.data[0]["text/plain"] == "2\n"


@pytest.mark.asyncio
async def test_variable_reuse_async(local_box_async: LocalPyBox):
    code_round1 = """a = 1
print(a)"""
    await local_box_async.arun(code=code_round1)
    code_round2 = """a += 1
print(a)"""
    out: PyBoxOut = await local_box_async.arun(code=code_round2)

    assert len(out.data) == 1
    assert out.data[0]["text/plain"] == "2\n"


def test_print_multi_line(local_box: LocalPyBox):
    code = """a = 1
print(a)
print(a)"""
    out: PyBoxOut = local_box.run(code=code)

    assert len(out.data) == 1
    assert out.data[0]["text/plain"] == "1\n1\n"


@pytest.mark.asyncio
async def test_print_multi_line_async(local_box_async: LocalPyBox):
    code = """a = 1
print(a)
print(a)"""
    out: PyBoxOut = await local_box_async.arun(code=code)

    assert len(out.data) == 1
    assert out.data[0]["text/plain"] == "1\n1\n"


def test_execute_exception(local_box: LocalPyBox):
    division_by_zero = "1 / 0"
    out: PyBoxOut = local_box.run(code=division_by_zero)
    assert out.data == []
    assert out.error is not None
    assert out.error.ename == "ZeroDivisionError"


@pytest.mark.asyncio
async def test_execute_exception_async(local_box_async: LocalPyBox):
    division_by_zero = "1 / 0"
    out: PyBoxOut = await local_box_async.arun(code=division_by_zero)
    assert out.data == []
    assert out.error is not None
    assert out.error.ename == "ZeroDivisionError"


def test_not_output(local_box: LocalPyBox):
    test_code = "a = 1"
    out: PyBoxOut = local_box.run(code=test_code)
    assert out.data == []


@pytest.mark.asyncio
async def test_not_output_async(local_box_async: LocalPyBox):
    test_code = "a = 1"
    out: PyBoxOut = await local_box_async.arun(code=test_code)
    assert out.data == []


def test_execute_timeout(local_box: LocalPyBox):
    timeout_code = """import time
time.sleep(10)"""
    out: PyBoxOut = local_box.run(code=timeout_code, timeout=1)

    assert out.data == []
    assert out.error is not None
    assert out.error.ename == "KeyboardInterrupt"


@pytest.mark.asyncio
async def test_execute_timeout_async(local_box_async: LocalPyBox):
    timeout_code = """import time
time.sleep(10)"""
    out: PyBoxOut = await local_box_async.arun(code=timeout_code, timeout=1)

    assert out.data == []
    assert out.error is not None
    assert out.error.ename == "KeyboardInterrupt"


def test_interrupt_kernel(local_box: LocalPyBox):
    code = "a = 1"
    local_box.run(code=code)

    timeout_code = """import time
time.sleep(10)"""
    out: PyBoxOut = local_box.run(code=timeout_code, timeout=1)

    assert out.data == []
    assert out.error is not None
    assert out.error.ename == "KeyboardInterrupt"

    res: PyBoxOut = local_box.run(code="print(a)")
    assert len(res.data) == 1
    assert res.data[0]["text/plain"] == "1\n"


@pytest.mark.asyncio
async def test_interrupt_kernel_async(local_box_async: LocalPyBox):
    code = "a = 1"
    await local_box_async.arun(code=code)

    timeout_code = """import time
time.sleep(10)"""
    out: PyBoxOut = await local_box_async.arun(code=timeout_code, timeout=1)

    assert out.data == []
    assert out.error is not None
    assert out.error.ename == "KeyboardInterrupt"

    res: PyBoxOut = await local_box_async.arun(code="print(a)")
    assert len(res.data) == 1
    assert res.data[0]["text/plain"] == "1\n"


def test_partial_execution_failed(local_box: LocalPyBox):
    code = """a = 1
b = 2
print(a)
print(c)"""
    out: PyBoxOut = local_box.run(code=code)

    assert len(out.data) == 1
    assert out.data[0]["text/plain"] == "1\n"
    assert out.error is not None
    assert out.error.ename == "NameError"


@pytest.mark.asyncio
async def test_partial_execution_failed_async(local_box_async: LocalPyBox):
    code = """a = 1
b = 2
print(a)
print(c)"""
    out: PyBoxOut = await local_box_async.arun(code=code)

    assert len(out.data) == 1
    assert out.data[0]["text/plain"] == "1\n"
    assert out.error is not None
    assert out.error.ename == "NameError"


@pytest.mark.skip(reason="matplotlib library is required")
def test_multi_channel_output(local_box: LocalPyBox):
    code = """import matplotlib.pyplot as plt
x = [1, 2, 3, 4, 5]
y = [2, 3, 5, 7, 11]

plt.plot(x, y)
plt.show()

x, y"""
    out: PyBoxOut = local_box.run(code=code)

    assert len(out.data) == 2
    assert "image/png" in out.data[0]
    assert out.data[1]["text/plain"] == "([1, 2, 3, 4, 5], [2, 3, 5, 7, 11])"


@pytest.mark.skip(reason="matplotlib library is required")
@pytest.mark.asyncio
async def test_multi_channel_output_async(local_box_async: LocalPyBox):
    code = """import matplotlib.pyplot as plt
x = [1, 2, 3, 4, 5]
y = [2, 3, 5, 7, 11]

plt.plot(x, y)
plt.show()

x, y"""
    out: PyBoxOut = await local_box_async.arun(code=code)

    assert len(out.data) == 2
    assert "image/png" in out.data[0]
    assert out.data[1]["text/plain"] == "([1, 2, 3, 4, 5], [2, 3, 5, 7, 11])"
