from collections.abc import Iterator

import pytest
from pybox import LocalPyBoxManager
from pybox.local import LocalPyBox
from pybox.schema import CodeExecutionError


@pytest.fixture(scope="module")
def box_local_manager() -> Iterator[LocalPyBoxManager]:
    _mng = LocalPyBoxManager()
    yield _mng
    _mng.kernel_manager.shutdown_all()


@pytest.fixture(scope="module")
def local_box(box_local_manager: LocalPyBoxManager) -> Iterator[LocalPyBox]:
    _box = box_local_manager.start()
    yield _box
    box_local_manager.shutdown(_box.kernel_id)


def test_code_execute(local_box: LocalPyBox):
    test_code = "print('test')"
    out = local_box.run(code=test_code)

    assert out.text == "test\n"


@pytest.mark.asyncio
async def test_code_execute_async(local_box: LocalPyBox):
    test_code = "print('test')"
    out = await local_box.arun(code=test_code)

    assert out.text == "test\n"


def test_variable_reuse(local_box: LocalPyBox):
    code_round1 = """a = 1
print(a)"""
    local_box.run(code=code_round1)
    code_round2 = """a += 1
print(a)"""
    out = local_box.run(code=code_round2)

    assert out.text == "2\n"


@pytest.mark.asyncio
async def test_variable_reuse_async(local_box: LocalPyBox):
    code_round1 = """a = 1
print(a)"""
    await local_box.arun(code=code_round1)
    code_round2 = """a += 1
print(a)"""
    out = await local_box.arun(code=code_round2)

    assert out.text == "2\n"


def test_print_multi_line(local_box: LocalPyBox):
    code = """a = 1
print(a)
print(a)"""
    out = local_box.run(code=code)

    assert out.text == "1\n1\n"


@pytest.mark.asyncio
async def test_print_multi_line_async(local_box: LocalPyBox):
    code = """a = 1
print(a)
print(a)"""
    out = await local_box.arun(code=code)

    assert out.text == "1\n1\n"


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


def test_execute_timeout(local_box: LocalPyBox):
    timeout_code = """import time
time.sleep(10)"""
    with pytest.raises(CodeExecutionError) as exc_info:  # noqa: PT012
        local_box.run(code=timeout_code, timeout=1)
        assert exc_info.value.args[0] == "KeyboardInterrupt"


@pytest.mark.asyncio
async def test_execute_timeout_async(local_box: LocalPyBox):
    timeout_code = """import time
time.sleep(10)"""
    with pytest.raises(CodeExecutionError) as exc_info:  # noqa: PT012
        await local_box.arun(code=timeout_code, timeout=1)
        assert exc_info.value.args[0] == "KeyboardInterrupt"


def test_interrupt_kernel(local_box: LocalPyBox):
    code = "a = 1"
    local_box.run(code=code)

    timeout_code = """import time
time.sleep(10)"""
    with pytest.raises(CodeExecutionError) as exc_info:  # noqa: PT012
        local_box.run(code=timeout_code, timeout=1)
        assert exc_info.value.args[0] == "KeyboardInterrupt"

    res = local_box.run(code="print(a)")
    assert res.text == "1\n"


@pytest.mark.asyncio
async def test_interrupt_kernel_async(local_box: LocalPyBox):
    code = "a = 1"
    await local_box.arun(code=code)

    timeout_code = """import time
time.sleep(10)"""
    with pytest.raises(CodeExecutionError) as exc_info:  # noqa: PT012
        await local_box.arun(code=timeout_code, timeout=1)
        assert exc_info.value.args[0] == "KeyboardInterrupt"

    res = await local_box.arun(code="print(a)")
    assert res.text == "1\n"
