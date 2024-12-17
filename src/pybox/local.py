from __future__ import annotations

import logging
import platform
import queue
from typing import TYPE_CHECKING
from uuid import uuid4

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from jupyter_client import AsyncKernelManager, AsyncMultiKernelManager, KernelManager, MultiKernelManager
from jupyter_client.multikernelmanager import DuplicateKernelError

if TYPE_CHECKING:
    from types import TracebackType


from pybox.base import BasePyBox, BasePyBoxManager
from pybox.schema import (
    ExecutionResponse,
    PyBoxOut,
)

logger = logging.getLogger(__name__)

SYSTEM_PLATFORM = platform.system()


class LocalPyBox(BasePyBox):
    def __init__(self, km: KernelManager, mkm: MultiKernelManager | None = None):
        self.km = km
        self.mkm = mkm
        self.client = self.km.client()

    @property
    def kernel_id(self) -> str | None:
        return self.km.kernel_id

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        # If we use `self.km.shutdown_kernel(now=True)`, the kernel_id will last in the multi_kernel_manager.
        if self.mkm is not None:
            self.mkm.shutdown_kernel(kernel_id=self.kernel_id, now=True)
        # 返回 False 让异常继续传播, 返回 True 会抑制异常
        return False

    def run(self, code: str, timeout: int = 60) -> PyBoxOut:
        if not self.client.channels_running:
            # `wait_for_ready` raises a RuntimeError if the kernel is not ready
            try:
                self.client.wait_for_ready(timeout=timeout)
            except RuntimeError as e:
                msg = "Timeout waiting for kernel ready."
                raise TimeoutError(msg) from e

        msg_id = self.client.execute(code)
        self.__wait_for_execute_reply(msg_id, timeout=timeout)
        return self.__get_kernel_output(msg_id, timeout=timeout)

    def __wait_for_execute_reply(self, msg_id: str, **kwargs) -> ExecutionResponse | None:
        # wait for the "execute_reply"
        while True:
            try:
                shell_msg = self.client.get_shell_msg(**kwargs)
                # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#execution-results>
                # error execution may have extra messages, for example a stream std error
                if (shell_msg["parent_header"]["msg_id"] != msg_id) or (shell_msg["msg_type"] != "execute_reply"):
                    continue
            except queue.Empty as e:
                self.__interrupt_kernel()
                msg = "Timeout getting execute reply."
                raise TimeoutError(msg) from e
            return ExecutionResponse.model_validate(shell_msg)

    def __get_kernel_output(self, msg_id: str, **kwargs) -> PyBoxOut:
        """Retrieves output from a kernel.

        Args:
            msg_id (str): request message id.

        Returns:
            PyBoxOut: result of the code execution

        Raises:
            TimeoutError: if the code execution times out
        """
        pybox_out = PyBoxOut()
        while True:
            # Poll the message
            try:
                message = self.client.get_iopub_msg(**kwargs)
                logger.debug("kernel execution message: [%s]", message)
                response = ExecutionResponse.model_validate(message)
                if response.parent_header.msg_id != msg_id:
                    # This message does not belong to the current execution
                    # As we break early once we get the result, this could happen
                    logger.debug("Ignoring message from other execution.")
                elif response.msg_type == "execute_input":
                    # Ignore broadcast message
                    # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#code-inputs>
                    logger.debug("Ignoring broadcast execution input.")
                elif response.msg_type in ["execute_result", "display_data"]:
                    # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#id6>
                    # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#display-data>
                    pybox_out.data.append(response.content.data)
                elif response.msg_type == "stream":
                    # 'stream' is treated as second-class citizen. If 'execute_result', 'display_data' or 'execute_reply.error' exists,
                    # We ignore the 'stream' message. If only all other messages has nothing to display, we will use the 'stream' message.
                    # See <https://jupyter-client.readthedocs.io/en/stable/messaging.html#streams-stdout-stderr-etc>
                    pybox_out.data.append({"text/plain": response.content.text})
                elif response.msg_type == "error":
                    # See <https://jupyter-client.readthedocs.io/en/stable/messaging.html#execution-errors>
                    pybox_out.error = response.content

                elif response.msg_type == "status":  # noqa: SIM102
                    # According to the document <https://jupyter-client.readthedocs.io/en/latest/messaging.html#request-reply>
                    # The idle message will be published after processing the request and publishing associated IOPub messages
                    if response.content.execution_state == "idle":
                        return pybox_out
            except queue.Empty as e:
                logger.warning("No iopub message received.")
                msg = "Kernel execution timed out."
                raise TimeoutError(msg) from e

    def __interrupt_kernel(self) -> None:
        """send an interrupt message to the kernel."""
        if SYSTEM_PLATFORM == "Windows":
            logger.warning("Interrupt signal is not supported on Windows.")
            return
        try:
            interrupt_msg = self.client.session.msg("interrupt_request", content={})
            self.client.control_channel.send(interrupt_msg)
            control_msg = self.client.get_control_msg(timeout=5)
            # TODO: Do you need to determine whether the parent id is equal to the interrupt message id?
            # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#kernel-interrupt>
            if control_msg["msg_type"] == "interrupt_reply":
                status = control_msg["content"]["status"]
                if status == "ok":
                    logger.info(
                        "Kernel %s interrupt signal sent successfully.",
                        self.kernel_id,
                    )
                else:
                    logger.warning(
                        "Kernel %s interrupt signal sent failed: %s",
                        self.kernel_id,
                        status,
                    )
        except Exception as e:  # noqa: BLE001
            # TODO: What should I do if sending an interrupt message times out or fails?
            logger.warning(
                "Failed to send interrupt message to kernel %s: %s",
                self.kernel_id,
                e,
            )


class AsyncLocalPyBox(LocalPyBox):
    def __init__(self, km: AsyncKernelManager, mkm: AsyncMultiKernelManager | None = None):
        self.km = km
        self.mkm = mkm
        self.client = self.km.client()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, traceback: TracebackType | None
    ) -> bool:
        # If we use `await self.km.shutdown_kernel(now=True)`, the kernel_id will last in the multi_kernel_manager.
        if self.mkm is not None:
            await self.mkm.shutdown_kernel(kernel_id=self.kernel_id, now=True)
        # 返回 False 让异常继续传播, 返回 True 会抑制异常
        return False

    async def run(self, code: str, timeout: int = 60) -> PyBoxOut:
        if not self.client.channels_running:
            # `wait_for_ready` raises a RuntimeError if the kernel is not ready
            try:
                await self.client.wait_for_ready(timeout=timeout)
            except RuntimeError as e:
                msg = "Timeout waiting for kernel ready."
                raise TimeoutError(msg) from e

        msg_id = self.client.execute(code)
        await self.__await_for_execute_reply(msg_id, timeout=timeout)
        return await self.__aget_kernel_output(msg_id, timeout=timeout)

    async def __await_for_execute_reply(self, msg_id: str, **kwargs) -> ExecutionResponse | None:
        while True:
            try:
                shell_msg = await self.client.get_shell_msg(**kwargs)
                # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#execution-results>
                # error execution may have extra messages, for example a stream std error
                if (shell_msg["parent_header"]["msg_id"] != msg_id) or (shell_msg["msg_type"] != "execute_reply"):
                    continue
            except queue.Empty as e:
                await self.__ainterrupt_kernel()
                msg = "Timeout getting execute reply."
                raise TimeoutError(msg) from e
            return ExecutionResponse.model_validate(shell_msg)

    async def __aget_kernel_output(self, msg_id: str, **kwargs) -> PyBoxOut:
        """Retrieves output from a kernel asynchronously.

        Args:
            msg_id (str): request message id.

        Returns:
            PyBoxOut: result of the code execution

        Raises:
            TimeoutError: if the code execution times out
        """
        pybox_out = PyBoxOut()
        while True:
            # Poll the message
            try:
                message = await self.client.get_iopub_msg(**kwargs)
                logger.debug("kernel execution message: [%s]", message)
                response = ExecutionResponse.model_validate(message)
                if response.parent_header.msg_id != msg_id:
                    # This message does not belong to the current execution
                    # As we break early once we get the result, this could happen
                    logger.debug("Ignoring message from other execution.")
                elif response.msg_type == "execute_input":
                    # Ignore broadcast message
                    # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#code-inputs>
                    logger.debug("Ignoring broadcast execution input.")
                elif response.msg_type in ["execute_result", "display_data"]:
                    # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#id6>
                    # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#display-data>
                    pybox_out.data.append(response.content.data)
                elif response.msg_type == "stream":
                    # 'stream' is treated as second-class citizen. If 'execute_result', 'display_data' or 'execute_reply.error' exists,
                    # We ignore the 'stream' message. If only all other messages has nothing to display, we will use the 'stream' message.
                    # See <https://jupyter-client.readthedocs.io/en/stable/messaging.html#streams-stdout-stderr-etc>
                    pybox_out.data.append({"text/plain": response.content.text})
                elif response.msg_type == "error":
                    # See <https://jupyter-client.readthedocs.io/en/stable/messaging.html#execution-errors>
                    pybox_out.error = response.content
                elif response.msg_type == "status":  # noqa: SIM102
                    # According to the document <https://jupyter-client.readthedocs.io/en/latest/messaging.html#request-reply>
                    # The idle message will be published after processing the request and publishing associated IOPub messages
                    if response.content.execution_state == "idle":
                        return pybox_out
            except queue.Empty as e:
                logger.warning("No iopub message received.")
                msg = "Kernel execution timed out."
                raise TimeoutError(msg) from e

    async def __ainterrupt_kernel(self) -> None:
        """send an interrupt message to the kernel."""
        if SYSTEM_PLATFORM == "Windows":
            logger.warning("Interrupt signal is not supported on Windows.")
            return
        try:
            interrupt_msg = self.client.session.msg("interrupt_request", content={})
            self.client.control_channel.send(interrupt_msg)
            control_msg = await self.client.get_control_msg(timeout=5)
            # TODO: Do you need to determine whether the parent id is equal to the interrupt message id?
            # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#kernel-interrupt>
            if control_msg["msg_type"] == "interrupt_reply":
                status = control_msg["content"]["status"]
                if status == "ok":
                    logger.info(
                        "Kernel %s interrupt signal sent successfully.",
                        self.kernel_id,
                    )
                else:
                    logger.warning(
                        "Kernel %s interrupt signal sent failed: %s",
                        self.kernel_id,
                        status,
                    )
        except Exception as e:  # noqa: BLE001
            # TODO: What should I do if sending an interrupt message times out or fails?
            logger.warning(
                "Failed to send interrupt message to kernel %s: %s",
                self.kernel_id,
                e,
            )


class LocalPyBoxManager(BasePyBoxManager):
    def __init__(
        self,
        kernel_manager: MultiKernelManager | None = None,
        profile_dir: str | None = None,
    ):
        self.profile_dir = profile_dir
        if kernel_manager is None:
            self.kernel_manager = MultiKernelManager()
        else:
            self.kernel_manager = kernel_manager

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup(now=True)
        if exc_type is not None:
            return False

        return True

    def start(
        self,
        kernel_id: str | None = None,
        **kwargs,
    ) -> LocalPyBox:
        if kernel_id is None:
            kernel_id = str(uuid4())
        logger.debug("Starting new kernel with ID %s", kernel_id)

        if self.profile_dir is not None:
            kwargs["extra_arguments"] = ["--profile-dir", self.profile_dir]
        try:
            kid = self.kernel_manager.start_kernel(kernel_id=kernel_id, **kwargs)
        except DuplicateKernelError:
            # it's OK if the kernel already exists
            kid = kernel_id
        km = self.kernel_manager.get_kernel(kernel_id=kid)
        return LocalPyBox(km=km, mkm=self.kernel_manager)

    def shutdown(
        self,
        kernel_id: str,
        *,
        now: bool = False,
        restart: bool = False,
    ) -> None:
        try:
            self.kernel_manager.shutdown_kernel(kernel_id=kernel_id, now=now, restart=restart)
        except KeyError:
            logger.warning("kernel %s not found", kernel_id)
        else:
            logger.info("Kernel %s shut down", kernel_id)

    def shutdown_all(self, *args, **kwargs):
        if len(self.kernel_manager):
            self.kernel_manager.shutdown_all(*args, **kwargs)


class AsyncLocalPyBoxManager(LocalPyBoxManager):
    def __init__(
        self,
        async_kernel_manager: AsyncMultiKernelManager | None = None,
        profile_dir: str | None = None,
    ):
        self.profile_dir = profile_dir
        if async_kernel_manager is None:
            self.async_kernel_manager = AsyncMultiKernelManager()
        else:
            self.async_kernel_manager = async_kernel_manager

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.acleanup(now=True)

        if exc_type is not None:
            return False

        return True

    async def start(
        self,
        kernel_id: str | None = None,
        **kwargs,
    ) -> AsyncLocalPyBox:
        if kernel_id is None:
            kernel_id = str(uuid4())
        logger.debug("Starting new kernel with ID %s", kernel_id)

        if self.profile_dir is not None:
            kwargs["extra_arguments"] = ["--profile-dir", self.profile_dir]

        try:
            kid = await self.async_kernel_manager.start_kernel(kernel_id=kernel_id, **kwargs)
        except DuplicateKernelError:
            # it's OK if the kernel already exists
            kid = kernel_id
        km = self.async_kernel_manager.get_kernel(kernel_id=kid)
        return AsyncLocalPyBox(km=km, mkm=self.async_kernel_manager)

    async def shutdown(
        self,
        kernel_id: str,
        *,
        now: bool = False,
        restart: bool = False,
    ) -> None:
        try:
            await self.async_kernel_manager.shutdown_kernel(kernel_id=kernel_id, now=now, restart=restart)
        except KeyError:
            logger.warning("kernel %s not found", kernel_id)
        else:
            logger.info("Kernel %s shut down", kernel_id)

    async def shutdown_all(self, *args, **kwargs):
        if len(self.async_kernel_manager):
            await self.async_kernel_manager.shutdown_all(*args, **kwargs)
