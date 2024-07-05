from __future__ import annotations

import logging
import queue
from typing import TYPE_CHECKING
from uuid import uuid4

try:
    from jupyter_client import MultiKernelManager
    from jupyter_client.multikernelmanager import DuplicateKernelError
except ImportError as e:
    raise ImportError(
        msg="LocalKernelManager is not recommended for production use.\nFor local development, please use `pipenv install --dev` to install related dependencies."
    ) from e

if TYPE_CHECKING:
    from jupyter_client.asynchronous import AsyncKernelClient
    from jupyter_client.blocking import BlockingKernelClient

from pybox.base import BasePyBox, BasePyBoxManager
from pybox.schema import (
    CodeExecutionError,
    ExecutionResponse,
    PyBoxOut,
)

logger = logging.getLogger(__name__)


class LocalPyBox(BasePyBox):
    def __init__(self, kernel_id: str, client: BlockingKernelClient | AsyncKernelClient):
        self.kernel_id = kernel_id
        self.client = client

    def run(self, code: str) -> PyBoxOut | None:
        if not self.client.channels_running:
            self.client.wait_for_ready()

        msg_id = self.client.execute(code)
        self.__wait_for_execute_reply(msg_id)
        return self.__get_kernel_output(msg_id)

    def __wait_for_execute_reply(self, msg_id: str) -> ExecutionResponse | None:
        # wait for the "execute_reply"
        while True:
            try:
                shell_msg = self.client.get_shell_msg(timeout=60)
                if (shell_msg["parent_header"]["msg_id"] != msg_id) or (shell_msg["msg_type"] != "execute_reply"):
                    continue
                # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#execution-results>
                # error execution may have extra messages, for example a stream std error
                response = ExecutionResponse.model_validate(shell_msg)
                if response.content.status == "error":
                    raise CodeExecutionError(response.content.traceback)
            except queue.Empty:
                logger.warning("Shell msg is empty.")
                return None
            else:
                return response

    def __get_kernel_output(self, msg_id: str) -> PyBoxOut | None:
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
                message = self.client.get_iopub_msg(timeout=60)
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
                    # TODO: I can definitly do better here
                    if response.content.data.image_png:
                        result = PyBoxOut(type="image/png", content=response.content.data.image_png)
                    elif response.content.data.text_plain:
                        result = PyBoxOut(type="text/plain", content=response.content.data.text_plain)
                elif response.msg_type == "stream":
                    # 'stream' is treated as second-class citizen. If 'execute_result', 'display_data' or 'execute_reply.error' exists,
                    # We ignore the 'stream' message. If only all other messages has nothing to display, we will use the 'stream' message.
                    # See <https://jupyter-client.readthedocs.io/en/stable/messaging.html#streams-stdout-stderr-etc>
                    if not result:
                        result = PyBoxOut(type=response.content.name, content=response.content.text)
                elif response.msg_type == "status":  # noqa: SIM102
                    # According to the document <https://jupyter-client.readthedocs.io/en/latest/messaging.html#request-reply>
                    # The idle message will be published after processing the request and publishing associated IOPub messages
                    if response.content.execution_state == "idle":
                        break
            except queue.Empty:
                logger.warning("Get iopub msg is empty.")
                break
        return result

    async def arun(self, code: str) -> PyBoxOut | None:
        if not self.client.channels_running:
            await self.client._async_wait_for_ready()  # noqa: SLF001

        msg_id = self.client.execute(code)
        await self.__await_for_execute_reply(msg_id)
        return await self.__aget_kernel_output(msg_id)

    async def __await_for_execute_reply(self, msg_id: str) -> ExecutionResponse | None:
        while True:
            try:
                shell_msg = await self.client._async_get_shell_msg(  # noqa: SLF001
                    timeout=60
                )
                if (shell_msg["parent_header"]["msg_id"] != msg_id) or (shell_msg["msg_type"] != "execute_reply"):
                    continue
                # See <https://jupyter-client.readthedocs.io/en/latest/messaging.html#execution-results>
                # error execution may have extra messages, for example a stream std error
                response = ExecutionResponse.model_validate(shell_msg)
                if response.content.status == "error":
                    raise CodeExecutionError(response.content.traceback)
            except queue.Empty:
                logger.warning("Shell msg is empty.")
                return None
            else:
                return response

    async def __aget_kernel_output(self, msg_id: str) -> PyBoxOut | None:
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
                    timeout=60
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
                    # TODO: I can definitly do better here
                    if response.content.data.image_png:
                        result = PyBoxOut(type="image/png", content=response.content.data.image_png)
                    elif response.content.data.text_plain:
                        result = PyBoxOut(type="text/plain", content=response.content.data.text_plain)
                elif response.msg_type == "stream":
                    # 'stream' is treated as second-class citizen. If 'execute_result', 'display_data' or 'execute_reply.error' exists,
                    # We ignore the 'stream' message. If only all other messages has nothing to display, we will use the 'stream' message.
                    # See <https://jupyter-client.readthedocs.io/en/stable/messaging.html#streams-stdout-stderr-etc>
                    if not result:
                        result = PyBoxOut(type=response.content.name, content=response.content.text)
                elif response.msg_type == "status":  # noqa: SIM102
                    # According to the document <https://jupyter-client.readthedocs.io/en/latest/messaging.html#request-reply>
                    # The idle message will be published after processing the request and publishing associated IOPub messages
                    if response.content.execution_state == "idle":
                        break
            except queue.Empty:
                logger.warning("Get iopub msg is empty.")
                break
        return result


class LocalPyBoxManager(BasePyBoxManager):
    def __init__(self, kernel_manager: MultiKernelManager | None = None):
        if kernel_manager is None:
            self.kernel_manager = MultiKernelManager()
        else:
            self.kernel_manager = kernel_manager

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
            kid = await self.kernel_manager._async_start_kernel(  # noqa: SLF001
                kernel_id=kernel_id, **kwargs
            )
        except DuplicateKernelError:
            # it's OK if the kernel already exists
            kid = kernel_id
        km = self.kernel_manager.get_kernel(kernel_id=kid)
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
            await self.kernel_manager._async_shutdown_kernel(  # noqa: SLF001
                kernel_id=kernel_id, now=now, restart=restart
            )
        except KeyError:
            logger.warning("kernel %s not found", kernel_id)
        else:
            logger.info("Kernel %s shut down", kernel_id)
