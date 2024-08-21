from __future__ import annotations

import logging
import queue
from typing import TYPE_CHECKING
from uuid import uuid4

from pybox.base import BasePyBox, BasePyBoxManager
from pybox.schema import (
    CodeExecutionError,
    ExecutionResponse,
    PyBoxOut,
)

logger = logging.getLogger(__name__)


try:
    from jupyter_client import AsyncMultiKernelManager, MultiKernelManager
    from jupyter_client.multikernelmanager import DuplicateKernelError

    if TYPE_CHECKING:
        from jupyter_client.asynchronous import AsyncKernelClient
        from jupyter_client.blocking import BlockingKernelClient
except ImportError:
    logger.warning(
        "LocalKernelManager is not recommended for production usage.\nFor local development, please use `pip install pppybox[local]` to install related dependencies."
    )


class LocalPyBox(BasePyBox):
    def __init__(self, kernel_id: str, client: BlockingKernelClient | AsyncKernelClient):
        self.kernel_id = kernel_id
        self.client = client

    def run(self, code: str, timeout: int = 60) -> PyBoxOut | None:
        if not self.client.channels_running:
            self.client.wait_for_ready()

        msg_id = self.client.execute(code)
        self.__wait_for_execute_reply(msg_id, timeout=timeout)
        return self.__get_kernel_output(msg_id, timeout=timeout)

    def __wait_for_execute_reply(self, msg_id: str, **kwargs) -> ExecutionResponse | None:
        # wait for the "execute_reply"
        while True:
            try:
                shell_msg = self.client.get_shell_msg(**kwargs)
                if (shell_msg["parent_header"]["msg_id"] != msg_id) or (shell_msg["msg_type"] != "execute_reply"):
                    continue
                # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#execution-results>
                # error execution may have extra messages, for example a stream std error
                response = ExecutionResponse.model_validate(shell_msg)
                if response.content.status == "error":
                    raise CodeExecutionError(
                        ename=response.content.ename,
                        evalue=response.content.evalue,
                        traceback=response.content.traceback,
                    )
            except queue.Empty:
                logger.warning("No shell message received.")
                return None
            else:
                return response

    def __get_kernel_output(self, msg_id: str, **kwargs) -> PyBoxOut | None:
        """Retrieves output from a kernel.

        Args:
            msg_id (str): request message id.

        Returns:
            PyBoxOut | None: result of the code execution

        Raises:
            CodeExecutionException: if the code execution fails
        """
        result = None
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
                    result = PyBoxOut(data=response.content.data)
                elif response.msg_type == "stream":
                    # 'stream' is treated as second-class citizen. If 'execute_result', 'display_data' or 'execute_reply.error' exists,
                    # We ignore the 'stream' message. If only all other messages has nothing to display, we will use the 'stream' message.
                    # See <https://jupyter-client.readthedocs.io/en/stable/messaging.html#streams-stdout-stderr-etc>
                    if not result:
                        result = PyBoxOut(data={"text/plain": response.content.text})
                elif response.msg_type == "status":  # noqa: SIM102
                    # According to the document <https://jupyter-client.readthedocs.io/en/latest/messaging.html#request-reply>
                    # The idle message will be published after processing the request and publishing associated IOPub messages
                    if response.content.execution_state == "idle":
                        break
            except queue.Empty:
                logger.warning("No iopub message received.")
                break
        return result

    async def arun(self, code: str, timeout: int = 60) -> PyBoxOut | None:
        if not self.client.channels_running:
            await self.client._async_wait_for_ready()  # noqa: SLF001

        msg_id = self.client.execute(code)
        await self.__await_for_execute_reply(msg_id, timeout=timeout)
        return await self.__aget_kernel_output(msg_id, timeout=timeout)

    async def __await_for_execute_reply(self, msg_id: str, **kwargs) -> ExecutionResponse | None:
        while True:
            try:
                shell_msg = await self.client._async_get_shell_msg(  # noqa: SLF001
                    **kwargs
                )
                if (shell_msg["parent_header"]["msg_id"] != msg_id) or (shell_msg["msg_type"] != "execute_reply"):
                    continue
                # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#execution-results>
                # error execution may have extra messages, for example a stream std error
                response = ExecutionResponse.model_validate(shell_msg)
                if response.content.status == "error":
                    raise CodeExecutionError(
                        ename=response.content.ename,
                        evalue=response.content.evalue,
                        traceback=response.content.traceback,
                    )
            except queue.Empty:
                logger.warning("No shell message received.")
                return None
            else:
                return response

    async def __aget_kernel_output(self, msg_id: str, **kwargs) -> PyBoxOut | None:
        """Retrieves output from a kernel asynchronously.

        Args:
            msg_id (str): request message id.

        Returns:
            PyBoxOut | None: result of the code execution

        Raises:
            CodeExecutionException: if the code execution fails
        """
        result = None
        while True:
            # Poll the message
            try:
                message = await self.client._async_get_iopub_msg(  # noqa: SLF001
                    **kwargs
                )
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
                    result = PyBoxOut(data=response.content.data)
                elif response.msg_type == "stream":
                    # 'stream' is treated as second-class citizen. If 'execute_result', 'display_data' or 'execute_reply.error' exists,
                    # We ignore the 'stream' message. If only all other messages has nothing to display, we will use the 'stream' message.
                    # See <https://jupyter-client.readthedocs.io/en/stable/messaging.html#streams-stdout-stderr-etc>
                    if not result:
                        result = PyBoxOut(data={"text/plain": response.content.text})
                elif response.msg_type == "status":  # noqa: SIM102
                    # According to the document <https://jupyter-client.readthedocs.io/en/latest/messaging.html#request-reply>
                    # The idle message will be published after processing the request and publishing associated IOPub messages
                    if response.content.execution_state == "idle":
                        break
            except queue.Empty:
                logger.warning("No iopub message received.")
                break
        return result


class LocalPyBoxManager(BasePyBoxManager):
    def __init__(
        self,
        kernel_manager: MultiKernelManager | None = None,
        async_kernel_manager: AsyncMultiKernelManager | None = None,
    ):
        if kernel_manager is None:
            self.kernel_manager = MultiKernelManager()
        else:
            self.kernel_manager = kernel_manager
        if async_kernel_manager is None:
            self.async_kernel_manager = AsyncMultiKernelManager()
        else:
            self.async_kernel_manager = async_kernel_manager

    # TODO: I cannot use __del__ in async context
    # maybe I shouldn't clean up the kernels in the __del__ method
    def __del__(self):
        """clean up all the kernels."""
        logger.info("Shutting down all kernels")
        self.kernel_manager.shutdown_all(now=True)
        self.kernel_manager.__del__()

    def start(
        self,
        kernel_id: str | None = None,
        **kwargs,
    ) -> LocalPyBox:
        if kernel_id is None:
            kernel_id = str(uuid4())
        logger.debug("Starting new kernel with ID %s", kernel_id)
        try:
            kid = self.kernel_manager.start_kernel(kernel_id=kernel_id, **kwargs)
        except DuplicateKernelError:
            # it's OK if the kernel already exists
            kid = kernel_id
        km = self.kernel_manager.get_kernel(kernel_id=kid)
        return LocalPyBox(kernel_id=kid, client=km.client())

    async def astart(
        self,
        kernel_id: str | None = None,
        **kwargs,
    ) -> LocalPyBox:
        if kernel_id is None:
            kernel_id = str(uuid4())
        logger.debug("Starting new kernel with ID %s", kernel_id)
        try:
            kid = await self.async_kernel_manager.start_kernel(kernel_id=kernel_id, **kwargs)
        except DuplicateKernelError:
            # it's OK if the kernel already exists
            kid = kernel_id
        km = self.async_kernel_manager.get_kernel(kernel_id=kid)
        return LocalPyBox(kernel_id=kid, client=km.client())

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

    async def ashutdown(
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
