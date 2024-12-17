import os
from collections.abc import Iterator
from typing import AsyncIterator
from uuid import uuid4

import pytest
import pytest_asyncio
from pybox import AsyncLocalPyBox, AsyncLocalPyBoxManager, LocalPyBox, LocalPyBoxManager, PyBoxOut


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

    def test_box_lifecycle_w_context_manager(self, local_manager: LocalPyBoxManager):
        with local_manager.start() as box:
            kernel_id = box.kernel_id

            assert kernel_id in local_manager.kernel_manager
            assert local_manager.kernel_manager.is_alive(kernel_id)

        assert kernel_id not in local_manager.kernel_manager
        with pytest.raises(KeyError):
            assert not local_manager.kernel_manager.is_alive(kernel_id)

    def test_start_w_id(self, local_manager: LocalPyBoxManager):
        kernel_id = str(uuid4())
        with local_manager.start(kernel_id) as box:
            assert box.kernel_id == kernel_id
            assert kernel_id in local_manager.kernel_manager
            assert local_manager.kernel_manager.is_alive(box.kernel_id)

    def test_set_cwd(self, local_manager: LocalPyBoxManager):
        # even we don't set the cwd, it defaults to os.getcwd()
        # in order to test this is working, we need to change the cwd to a cross platform path
        with local_manager.start(cwd=os.path.expanduser("~")) as box:
            test_code = "import os\nprint(os.getcwd())"
            out: PyBoxOut = box.run(code=test_code)
            assert len(out.data) == 1
            assert os.path.expanduser("~") + "\n" == out.data[0]["text/plain"]

    @pytest.fixture
    def local_box(self, local_manager: LocalPyBoxManager) -> Iterator[LocalPyBox]:
        with local_manager.start() as _box:
            yield _box

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

    # TODO: This UT is buggy.
    # If we use function scope `LocalPyBox` fixture, and include this UT, the test process will hang.
    # If we use class scope `LocalPyBox` fixture, and include this UT, then `TimeoutError` is not raised.
    # If we only run this UT in the class, it works.
    # The async version does not have this issue.
    @pytest.mark.skip(reason="WIP")
    def test_execute_timeout(self, local_box: LocalPyBox):
        timeout_code = """import time
time.sleep(60)"""
        with pytest.raises(TimeoutError):
            local_box.run(code=timeout_code, timeout=10)

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
    async def async_local_manager(self) -> AsyncIterator[AsyncLocalPyBoxManager]:
        _mng = AsyncLocalPyBoxManager()
        yield _mng
        await _mng.async_kernel_manager.shutdown_all(now=True)

    async def test_box_lifecycle_async(self, async_local_manager: AsyncLocalPyBoxManager):
        box = await async_local_manager.start()
        kernel_id = box.kernel_id

        assert kernel_id in async_local_manager.async_kernel_manager
        assert await async_local_manager.async_kernel_manager.is_alive(kernel_id)

        await async_local_manager.shutdown(kernel_id)
        assert kernel_id not in async_local_manager.async_kernel_manager

    async def test_box_lifecycle_w_async_context_manager(self, async_local_manager: AsyncLocalPyBoxManager):
        async with await async_local_manager.start() as box:
            kernel_id = box.kernel_id

            assert kernel_id in async_local_manager.async_kernel_manager
            assert await async_local_manager.async_kernel_manager.is_alive(kernel_id)

        assert kernel_id not in async_local_manager.async_kernel_manager
        with pytest.raises(KeyError):
            assert not await async_local_manager.async_kernel_manager.is_alive(kernel_id)

    async def test_start_async_w_id(self, async_local_manager: AsyncLocalPyBoxManager):
        kernel_id = str(uuid4())
        async with await async_local_manager.start(kernel_id) as box:
            assert box.kernel_id == kernel_id
            assert kernel_id in async_local_manager.async_kernel_manager
            assert await async_local_manager.async_kernel_manager.is_alive(kernel_id)

    async def test_set_cwd_async(self, async_local_manager: AsyncLocalPyBoxManager):
        # even we don't set the cwd, it defaults to os.getcwd()
        # in order to test this is working, we need to change the cwd to a cross platform path
        async with await async_local_manager.start(cwd=os.path.expanduser("~")) as box:
            test_code = "import os\nprint(os.getcwd())"
            out: PyBoxOut = await box.run(code=test_code)
            assert len(out.data) == 1
            assert os.path.expanduser("~") + "\n" == out.data[0]["text/plain"]

    @pytest_asyncio.fixture(loop_scope="class")
    async def async_local_box(
        self,
        async_local_manager: AsyncLocalPyBoxManager,
    ) -> AsyncIterator[AsyncLocalPyBox]:
        async with await async_local_manager.start() as _box:
            yield _box

    async def test_code_execute_async(self, async_local_box: AsyncLocalPyBox):
        test_code = "print('test')"
        out = await async_local_box.run(code=test_code)

        assert len(out.data) == 1
        assert out.data[0]["text/plain"] == "test\n"

    async def test_variable_reuse_async(self, async_local_box: AsyncLocalPyBox):
        code_round1 = """a = 1
print(a)"""
        await async_local_box.run(code=code_round1)
        code_round2 = """a += 1
print(a)"""
        out: PyBoxOut = await async_local_box.run(code=code_round2)

        assert len(out.data) == 1
        assert out.data[0]["text/plain"] == "2\n"

    async def test_print_multi_line_async(self, async_local_box: AsyncLocalPyBox):
        code = """a = 1
print(a)
print(a)"""
        out: PyBoxOut = await async_local_box.run(code=code)

        assert len(out.data) == 1
        assert out.data[0]["text/plain"] == "1\n1\n"

    async def test_execute_exception_async(self, async_local_box: AsyncLocalPyBox):
        division_by_zero = "1 / 0"
        out: PyBoxOut = await async_local_box.run(code=division_by_zero)
        assert out.data == []
        assert out.error is not None
        assert out.error.ename == "ZeroDivisionError"

    async def test_no_output_async(self, async_local_box: AsyncLocalPyBox):
        test_code = "a = 1"
        out: PyBoxOut = await async_local_box.run(code=test_code)
        assert out.data == []

    async def test_execute_timeout_async(self, async_local_box: AsyncLocalPyBox):
        timeout_code = """import time
time.sleep(60)"""
        with pytest.raises(TimeoutError):
            await async_local_box.run(code=timeout_code, timeout=10)

    async def test_partial_execution_failed_async(self, async_local_box: AsyncLocalPyBox):
        code = """a = 1
b = 2
print(a)
print(c)"""
        out: PyBoxOut = await async_local_box.run(code=code)

        assert len(out.data) == 1
        assert out.data[0]["text/plain"] == "1\n"
        assert out.error is not None
        assert out.error.ename == "NameError"
