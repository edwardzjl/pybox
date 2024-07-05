import os
from collections.abc import Iterator
from uuid import uuid4

import pytest
from pybox import CodeExecutionError, LocalPyBox, LocalPyBoxManager


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
    assert box.kernel_id in local_manager.kernel_manager
    assert local_manager.kernel_manager.is_alive(box.kernel_id)


def test_box_lifecycle(local_manager: LocalPyBoxManager):
    box = local_manager.start()

    assert box.kernel_id in local_manager.kernel_manager
    assert local_manager.kernel_manager.is_alive(box.kernel_id)

    local_manager.shutdown(box.kernel_id)

    assert box.kernel_id not in local_manager.kernel_manager


@pytest.mark.asyncio
async def test_box_lifecycle_async(local_manager: LocalPyBoxManager):
    box = await local_manager.astart()

    assert box.kernel_id in local_manager.kernel_manager
    assert local_manager.kernel_manager.is_alive(box.kernel_id)

    await local_manager.ashutdown(box.kernel_id)

    assert box.kernel_id not in local_manager.kernel_manager


def test_set_cwd(local_manager: LocalPyBoxManager):
    # even we don't set the cwd, it defaults to os.getcwd()
    # in order to test this is working, we need to change the cwd to a cross platform path
    kernel = local_manager.start(cwd=os.path.expanduser("~"))

    test_code = "import os\nprint(os.getcwd())"
    out = kernel.run(code=test_code)

    assert os.path.expanduser("~") + "\n" == out.content


@pytest.mark.asyncio
async def test_set_cwd_async(local_manager: LocalPyBoxManager):
    # even we don't set the cwd, it defaults to os.getcwd()
    # in order to test this is working, we need to change the cwd to a cross platform path
    kernel = await local_manager.astart(cwd=os.path.expanduser("~"))

    test_code = "import os\nprint(os.getcwd())"
    out = await kernel.arun(code=test_code)

    assert os.path.expanduser("~") + "\n" == out.content


@pytest.fixture
def local_box(local_manager: LocalPyBoxManager) -> Iterator[LocalPyBox]:
    _box = local_manager.start()
    yield _box
    local_manager.shutdown(_box.kernel_id)


def test_code_execute(local_box: LocalPyBox):
    test_code = "print('test')"
    out = local_box.run(code=test_code)

    assert out.type == "stdout"
    assert out.content == "test\n"


@pytest.mark.asyncio
async def test_code_execute_async(local_box: LocalPyBox):
    test_code = "print('test')"
    out = await local_box.arun(code=test_code)

    assert out.type == "stdout"
    assert out.content == "test\n"


def test_variable_reuse(local_box: LocalPyBox):
    code_round1 = """a = 1
print(a)"""
    local_box.run(code=code_round1)
    code_round2 = """a += 1
print(a)"""
    out = local_box.run(code=code_round2)

    assert out.content == "2\n"


@pytest.mark.asyncio
async def test_variable_reuse_async(local_box: LocalPyBox):
    code_round1 = """a = 1
print(a)"""
    await local_box.arun(code=code_round1)
    code_round2 = """a += 1
print(a)"""
    out = await local_box.arun(code=code_round2)

    assert out.content == "2\n"


def test_print_multi_line(local_box: LocalPyBox):
    code = """a = 1
print(a)
print(a)"""
    out = local_box.run(code=code)

    assert out.content == "1\n1\n"


@pytest.mark.asyncio
async def test_print_multi_line_async(local_box: LocalPyBox):
    code = """a = 1
print(a)
print(a)"""
    out = await local_box.arun(code=code)

    assert out.content == "1\n1\n"


def test_execute_exception(local_box: LocalPyBox):
    division_by_zero = "1 / 0"
    # with self.assertRaisesRegex(CodeExecutionException, ".*division by zero.*"):
    # TODO: test the actual exception
    with pytest.raises(CodeExecutionError):
        local_box.run(code=division_by_zero)


@pytest.mark.asyncio
async def test_execute_exception_async(local_box: LocalPyBox):
    division_by_zero = "1 / 0"
    # with self.assertRaisesRegex(CodeExecutionException, ".*division by zero.*"):
    # TODO: test the actual exception
    with pytest.raises(CodeExecutionError):
        await local_box.arun(code=division_by_zero)


def test_not_output(local_box: LocalPyBox):
    test_code = "a = 1"
    res = local_box.run(code=test_code)
    assert res is None


@pytest.mark.asyncio
async def test_not_output_async(local_box: LocalPyBox):
    test_code = "a = 1"
    res = await local_box.arun(code=test_code)
    assert res is None
