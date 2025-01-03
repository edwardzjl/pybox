from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import aiohttp
import requests
from dotenv import dotenv_values
from websockets.asyncio.client import connect as aconnect
from websockets.sync.client import connect

from pybox.base import BasePyBox, BasePyBoxManager
from pybox.schema import (
    CreateKernelRequest,
    ExecutionRequest,
    ExecutionResponse,
    Kernel,
    PyBoxOut,
)

logger = logging.getLogger(__name__)


class RemotePyBox(BasePyBox):
    def __init__(self, kernel: Kernel, ws_url: str | None = None):
        super().__init__()
        self.kernel = kernel
        self.kernel_id = kernel.id
        self.ws_url = ws_url

    def run(self, code: str) -> PyBoxOut:
        payload = ExecutionRequest.of_code(code)
        logger.debug("kernel execution payload: %s", payload.model_dump_json())
        pybox_out = PyBoxOut()
        with connect(self.ws_url) as websocket:
            logger.debug("connecting to kernel [%s] with url: %s", self.kernel_id, self.ws_url)
            websocket.send(payload.model_dump_json())
            while message := websocket.recv():
                logger.debug("kernel execution message: [%s]", message)
                response = ExecutionResponse.model_validate_json(message)
                if response.parent_header.msg_id != payload.header.msg_id:
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
                        break
        return pybox_out

    def handle_init_message(self):
        # There will be 3 messages send by the kernel after the connection is established
        # A common pattern is msg["parent_header"]["msg_type"] == "kernel_info_request"
        # and msg["content"]["execution_state"] == "busy" for fist message and "idle" for the following 2 message
        session_id = None
        parent_msg_id = None
        with connect(self.ws_url) as conn:
            while message := conn.recv():
                logger.debug("kernel init message: %s", message)
                response = ExecutionResponse.model_validate_json(message)

                # Some sanity check
                if session_id is None:
                    session_id = response.header.session
                    logger.debug(
                        "kernel [%s] responds with session: %s",
                        self.kernel_id,
                        session_id,
                    )
                elif session_id != response.header.session:
                    logger.warning(
                        "Found messages with multiple session ids in one init progress! previous: %s, current: %s",
                        session_id,
                        response.header.session,
                    )

                if response.msg_type != "status" or response.parent_header.msg_type != "kernel_info_request":
                    logger.warning("Unexpected init message: %s", message)

                if response.content.execution_state == "busy":
                    # This should be the first message that we received
                    parent_msg_id = response.parent_header.msg_id
                if (
                    parent_msg_id
                    and response.parent_header.msg_id == parent_msg_id
                    and response.content.execution_state == "idle"
                ):
                    # This message indicates the kernel is ready
                    break
                # And we ignored 1 message that I did not find too much related to the above 2 messages


class AsyncRemotePyBox(RemotePyBox):
    async def run(self, code: str) -> PyBoxOut:
        payload = ExecutionRequest.of_code(code)
        logger.debug("kernel execution payload: %s", payload.model_dump_json())
        pybox_out = PyBoxOut()
        async with asyncio.timeout(300):
            async with aconnect(self.ws_url) as websocket:
                logger.debug(
                    "connecting to kernel [%s] with url: %s",
                    self.kernel_id,
                    self.ws_url,
                )
                await websocket.send(payload.model_dump_json())
                while message := await websocket.recv():
                    logger.debug("kernel execution message: [%s]", message)
                    response = ExecutionResponse.model_validate_json(message)
                    if response.parent_header.msg_id != payload.header.msg_id:
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
                            break
        return pybox_out

    async def ahandle_init_message(self):
        # There will be 3 messages send by the kernel after the connection is established
        # A common pattern is msg["parent_header"]["msg_type"] == "kernel_info_request"
        # and msg["content"]["execution_state"] == "busy" for fist message and "idle" for the following 2 message
        session_id = None
        parent_msg_id = None
        async with aconnect(self.ws_url) as conn:
            while message := await conn.recv():
                logger.debug("kernel init message: %s", message)
                response = ExecutionResponse.model_validate_json(message)

                # Some sanity check
                if session_id is None:
                    session_id = response.header.session
                    logger.debug(
                        "kernel [%s] responds with session: %s",
                        self.kernel.id,
                        session_id,
                    )
                elif session_id != response.header.session:
                    logger.warning(
                        "Found messages with multiple session ids in one init progress! previous: %s, current: %s",
                        session_id,
                        response.header.session,
                    )

                if response.msg_type != "status" or response.parent_header.msg_type != "kernel_info_request":
                    logger.warning("Unexpected init message: %s", message)

                if response.content.execution_state == "busy":
                    # This should be the first message that we received
                    parent_msg_id = response.parent_header.msg_id
                if (
                    parent_msg_id
                    and response.parent_header.msg_id == parent_msg_id
                    and response.content.execution_state == "idle"
                ):
                    # This message indicates the kernel is ready
                    break
                # And we ignored 1 message that I did not find too much related to the above 2 messages


class RemotePyBoxManager(BasePyBoxManager):
    def __init__(
        self,
        host: str,
        env_file: str | None = None,
        kernel_env: dict[str, Any] | None = None,
    ):
        super().__init__()
        self.host = host
        self.env_file = env_file
        self.kernel_env = dotenv_values(env_file)
        if kernel_env:
            self.kernel_env.update(kernel_env)

    def start(
        self,
        kernel_id: str | None = None,
        cwd: str | None = None,
    ) -> RemotePyBox:
        env = self.kernel_env.copy()
        if kernel_id:
            env["KERNEL_ID"] = kernel_id
        if cwd:
            env["KERNEL_WORKING_DIR"] = cwd
        kernel_request = CreateKernelRequest(env=env)
        _body = kernel_request.model_dump()
        logger.debug("Starting kernel with payload %s", _body)
        response = requests.post(urljoin(self.host, "/api/kernels"), json=_body, timeout=60)
        if not response.ok:
            if "Kernel already exists:" in response.text:
                # kernel already exists, in which case the `kernel_id` must not be None
                # enterprise gateway does not provide a good response code
                response = requests.get(urljoin(self.host, f"/api/kernels/{kernel_id}"), timeout=60)
                if not response.ok:
                    error_msg = f"Error starting kernel: {response.status_code}\n{response.content}"
                    raise RuntimeError(error_msg)
            else:
                error_msg = f"Error starting kernel: {response.status_code}\n{response.content}"
                raise RuntimeError(error_msg)

        kernel = Kernel.model_validate_json(response.text)
        box = RemotePyBox(kernel, self.get_ws_url(kernel.id))
        try:
            box.handle_init_message()
        except Exception:
            # We may encounter some error during handling the init message, but we can still use the kernel
            logger.exception("Error swallowing kernel init messages")
        logger.info("Started kernel with id %s", kernel.id)
        return box

    def shutdown(
        self,
        kernel_id: str,
    ) -> None:
        response = requests.delete(urljoin(str(self.host), f"/api/kernels/{kernel_id}"), timeout=60)
        if not response.ok:
            if response.status_code == requests.codes.not_found:
                logger.warning("kernel %s not found", kernel_id)
            else:
                err_msg = f"Error deleting kernel {kernel_id}: {response.status_code}\n{response.content}"
                raise RuntimeError(err_msg)
        logger.info("Kernel %s shut down", kernel_id)

    def get_ws_url(self, kernel_id: str) -> str:
        base = urlparse(self.host)
        ws_scheme = "wss" if base.scheme == "https" else "ws"
        ws_base = urlunparse(base._replace(scheme=ws_scheme))
        return urljoin(ws_base, f"/api/kernels/{kernel_id}/channels")


class AsyncRemotePyBoxManager(RemotePyBoxManager):
    async def start(
        self,
        kernel_id: str | None = None,
        cwd: str | None = None,
    ) -> AsyncRemotePyBox:
        env = self.kernel_env.copy()
        if kernel_id:
            env["KERNEL_ID"] = kernel_id
        if cwd:
            env["KERNEL_WORKING_DIR"] = cwd
        kernel_request = CreateKernelRequest(env=env)
        _body = kernel_request.model_dump()
        logger.debug("Starting kernel with payload %s", _body)
        async with aiohttp.ClientSession(self.host) as session, session.post("/api/kernels", json=_body) as response:
            resp_text = await response.text()
            if not response.ok:
                if "Kernel already exists:" in resp_text:
                    # kernel already exists, in which case the `kernel_id` must not be None
                    # enterprise gateway does not provide a good response code
                    async with session.get(f"/api/kernels/{kernel_id}") as resp:
                        resp_text = await resp.text()
                        if not resp.ok:
                            error_msg = f"Error starting kernel: {resp.status}\n{resp.content}"
                            raise RuntimeError(error_msg)
                else:
                    error_msg = f"Error starting kernel: {response.status}\n{response.content}"
                    raise RuntimeError(error_msg)
        kernel = Kernel.model_validate_json(resp_text)
        box = AsyncRemotePyBox(kernel, self.get_ws_url(kernel.id))
        try:
            await box.ahandle_init_message()
        except Exception:
            # We may encounter some error during handling the init message, but we can still use the kernel
            logger.exception("Error swallowing kernel init messages")
        logger.info("Started kernel with id %s", kernel.id)
        return box

    async def shutdown(
        self,
        kernel_id: str,
    ) -> None:
        async with aiohttp.ClientSession() as session, session.delete(
            urljoin(str(self.host), f"/api/kernels/{kernel_id}")
        ) as response:
            if not response.ok:
                if response.status == requests.codes.not_found:
                    logger.warning("kernel %s not found", kernel_id)
                else:
                    err_msg = f"Error deleting kernel {kernel_id}: {response.status}\n{response.content}"
                    raise RuntimeError(err_msg)
        logger.info("Kernel %s shut down", kernel_id)
