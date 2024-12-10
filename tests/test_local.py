import os
import platform
from collections.abc import Iterator
from typing import AsyncIterator
from uuid import uuid4

import pytest
import pytest_asyncio
from pybox import LocalPyBoxManager, PyBoxOut
from pybox.local import LocalPyBox


class TestLocalBox:
    @pytest.fixture(scope="class")
    def local_manager(self) -> Iterator[LocalPyBoxManager]:
        _mng = LocalPyBoxManager()
        yield _mng
        _mng.kernel_manager.shutdown_all(now=True)

    def test_box_lifecycle(self, local_manager: LocalPyBoxManager):
        box = local_manager.start()
        kernel_id = box.kernel_id

        assert kernel_id in local_manager.kernel_manager
        assert local_manager.kernel_manager.is_alive(kernel_id)

        local_manager.shutdown(kernel_id, now=True)
        assert kernel_id not in local_manager.kernel_manager

    def test_start_w_id(self, local_manager: LocalPyBoxManager):
        kernel_id = str(uuid4())
        box = local_manager.start(kernel_id)
        assert box.kernel_id == kernel_id
        assert kernel_id in local_manager.kernel_manager
        assert local_manager.kernel_manager.is_alive(box.kernel_id)
        local_manager.shutdown(box.kernel_id)

    def test_set_cwd(self, local_manager: LocalPyBoxManager):
        # even we don't set the cwd, it defaults to os.getcwd()
        # in order to test this is working, we need to change the cwd to a cross platform path
        box = local_manager.start(cwd=os.path.expanduser("~"))
        test_code = "import os\nprint(os.getcwd())"
        out: PyBoxOut = box.run(code=test_code)
        assert len(out.data) == 1
        assert os.path.expanduser("~") + "\n" == out.data[0]["text/plain"]
        local_manager.shutdown(box.kernel_id, now=True)

    # NOTE: Share one box for all tests
    @pytest.fixture(scope="class")
    def local_box(self, local_manager: LocalPyBoxManager) -> Iterator[LocalPyBox]:
        _box = local_manager.start()
        yield _box
        local_manager.shutdown(_box.kernel_id, now=True)

    def test_code_execute(self, local_box: LocalPyBox):
        test_code = "print('test')"
        out: PyBoxOut = local_box.run(code=test_code)

        assert len(out.data) == 1
        assert out.data[0]["text/plain"] == "test\n"

    def test_variable_reuse(self, local_box: LocalPyBox):
        code_round1 = """a = 1
print(a)"""
        local_box.run(code=code_round1)
        code_round2 = """a += 1
print(a)"""
        out: PyBoxOut = local_box.run(code=code_round2)

        assert len(out.data) == 1
        assert out.data[0]["text/plain"] == "2\n"

    def test_print_multi_line(self, local_box: LocalPyBox):
        code = """a = 1
print(a)
print(a)"""
        out: PyBoxOut = local_box.run(code=code)

        assert len(out.data) == 1
        assert out.data[0]["text/plain"] == "1\n1\n"

    def test_execute_exception(self, local_box: LocalPyBox):
        division_by_zero = "1 / 0"
        out: PyBoxOut = local_box.run(code=division_by_zero)
        assert out.data == []
        assert out.error is not None
        assert out.error.ename == "ZeroDivisionError"

    def test_no_output(self, local_box: LocalPyBox):
        test_code = "a = 1"
        out: PyBoxOut = local_box.run(code=test_code)
        assert out.data == []

    # TODO: this will block the tests
    @pytest.mark.skip(reason="This will block the tests")
    def test_execute_timeout(self, local_box: LocalPyBox):
        timeout_code = """import time
time.sleep(10)"""
        # IDK why
        if platform.system() == "Windows":
            with pytest.raises(TimeoutError):
                local_box.run(code=timeout_code, timeout=1)
        else:
            local_box.run(code=timeout_code, timeout=1)

    def test_partial_execution_failed(self, local_box: LocalPyBox):
        code = """a = 1
b = 2
print(a)
print(c)"""
        out: PyBoxOut = local_box.run(code=code)

        assert len(out.data) == 1
        assert out.data[0]["text/plain"] == "1\n"
        assert out.error is not None
        assert out.error.ename == "NameError"


@pytest.mark.asyncio(loop_scope="class")
class TestAsyncLocalBox:
    @pytest_asyncio.fixture(loop_scope="class", scope="class")
    async def async_local_manager(self) -> AsyncIterator[LocalPyBoxManager]:
        _mng = LocalPyBoxManager()
        yield _mng
        await _mng.async_kernel_manager.shutdown_all(now=True)

    async def test_box_lifecycle_async(self, async_local_manager: LocalPyBoxManager):
        box = await async_local_manager.astart()
        kernel_id = box.kernel_id

        assert kernel_id in async_local_manager.async_kernel_manager
        assert await async_local_manager.async_kernel_manager.is_alive(kernel_id)

        await async_local_manager.ashutdown(kernel_id)
        assert kernel_id not in async_local_manager.async_kernel_manager

    async def test_start_async_w_id(self, async_local_manager: LocalPyBoxManager):
        kernel_id = str(uuid4())
        box = await async_local_manager.astart(kernel_id)
        assert box.kernel_id == kernel_id
        assert kernel_id in async_local_manager.async_kernel_manager
        assert await async_local_manager.async_kernel_manager.is_alive(kernel_id)
        await async_local_manager.ashutdown(kernel_id)

    async def test_set_cwd_async(self, async_local_manager: LocalPyBoxManager):
        # even we don't set the cwd, it defaults to os.getcwd()
        # in order to test this is working, we need to change the cwd to a cross platform path
        box = await async_local_manager.astart(cwd=os.path.expanduser("~"))
        test_code = "import os\nprint(os.getcwd())"
        out: PyBoxOut = await box.arun(code=test_code)
        assert len(out.data) == 1
        assert os.path.expanduser("~") + "\n" == out.data[0]["text/plain"]

    # NOTE: Share one box for all tests
    @pytest_asyncio.fixture(loop_scope="class", scope="class")
    async def async_local_box(
        self,
        async_local_manager: LocalPyBoxManager,
    ) -> AsyncIterator[LocalPyBox]:
        _box = await async_local_manager.astart()
        yield _box
        await async_local_manager.ashutdown(_box.kernel_id, now=True)

    async def test_code_execute_async(self, async_local_box: LocalPyBox):
        test_code = "print('test')"
        out = await async_local_box.arun(code=test_code)

        assert len(out.data) == 1
        assert out.data[0]["text/plain"] == "test\n"

    async def test_variable_reuse_async(self, async_local_box: LocalPyBox):
        code_round1 = """a = 1
print(a)"""
        await async_local_box.arun(code=code_round1)
        code_round2 = """a += 1
print(a)"""
        out: PyBoxOut = await async_local_box.arun(code=code_round2)

        assert len(out.data) == 1
        assert out.data[0]["text/plain"] == "2\n"

    async def test_print_multi_line_async(self, async_local_box: LocalPyBox):
        code = """a = 1
print(a)
print(a)"""
        out: PyBoxOut = await async_local_box.arun(code=code)

        assert len(out.data) == 1
        assert out.data[0]["text/plain"] == "1\n1\n"

    async def test_execute_exception_async(self, async_local_box: LocalPyBox):
        division_by_zero = "1 / 0"
        out: PyBoxOut = await async_local_box.arun(code=division_by_zero)
        assert out.data == []
        assert out.error is not None
        assert out.error.ename == "ZeroDivisionError"

    async def test_no_output_async(self, async_local_box: LocalPyBox):
        test_code = "a = 1"
        out: PyBoxOut = await async_local_box.arun(code=test_code)
        assert out.data == []

    # TODO: this will block the tests
    @pytest.mark.skip(reason="This will block the tests")
    async def test_execute_timeout_async(self, async_local_box: LocalPyBox):
        timeout_code = """import time
time.sleep(10)"""
        # IDK why
        if platform.system() == "Windows":
            with pytest.raises(TimeoutError):
                await async_local_box.arun(code=timeout_code, timeout=1)
        else:
            await async_local_box.arun(code=timeout_code, timeout=1)

    async def test_partial_execution_failed_async(self, async_local_box: LocalPyBox):
        code = """a = 1
b = 2
print(a)
print(c)"""
        out: PyBoxOut = await async_local_box.arun(code=code)

        assert len(out.data) == 1
        assert out.data[0]["text/plain"] == "1\n"
        assert out.error is not None
        assert out.error.ename == "NameError"
