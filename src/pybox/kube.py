from __future__ import annotations

import logging
from typing import Any

from dotenv import dotenv_values
from jkclient import CreateKernelRequest, JupyterKernelClient, Kernel
from jupyter_client import AsyncKernelClient, BlockingKernelClient

from pybox import LocalPyBox
from pybox.base import BasePyBoxManager

logger = logging.getLogger(__name__)


class KubePyBoxManager(BasePyBoxManager):
    """Kubernetes kernel pybox, used to create a custom kernel and connect to it to execute code"""

    def __init__(
        self,
        *,
        incluster: bool,
        env_file: str | None = None,
        kernel_env: dict[str, Any] | None = None,
    ):
        self.env_file = env_file
        self.kernel_env = dotenv_values(env_file)
        if kernel_env:
            self.kernel_env.update(kernel_env)

        self.client = JupyterKernelClient(incluster=incluster)

    def start(self, kernel_id: str, cwd: str, **kwargs) -> LocalPyBox:
        """Retrieve an existing kernel or create a new one in kubernetes

        Args:
            kernel_id (str): kernel_id
            cwd (str): kernel_working_dir

        Returns:
            LocalPyBox: kubernetes kernel box
        """
        env = self.kernel_env.copy()

        if kernel_id:
            env["KERNEL_ID"] = kernel_id
        if cwd:
            env["KERNEL_WORKING_DIR"] = cwd
        if username := kwargs.pop("username", None):
            env["KERNEL_USERNAME"] = username

        # Create kernel custom resource
        kernel_request = CreateKernelRequest(env=env)
        kernel: Kernel = self.client.create(kernel_request, **kwargs)

        # New kernel clinet
        kernel_client = BlockingKernelClient()
        kernel_client.load_connection_info(kernel.conn_info)

        return LocalPyBox(kernel_id=kernel_id, client=kernel_client)

    async def astart(self, kernel_id: str, cwd: str, **kwargs) -> LocalPyBox:
        """Retrieve an existing kernel or create a new one in kubernetes

        Args:
            kernel_id (str): kubernetes kernel id
            cwd (str): kernel workdir

        Returns:
            LocalPyBox: An iPython kernel that executes code.
        """
        env = self.kernel_env.copy()

        if kernel_id:
            env["KERNEL_ID"] = kernel_id
        if cwd:
            env["KERNEL_WORKING_DIR"] = cwd
        if username := kwargs.pop("username", None):
            env["KERNEL_USERNAME"] = username

        # Create kernel custom resource
        kernel_request = CreateKernelRequest(env=env)
        kernel: Kernel = await self.client.acreate(kernel_request, **kwargs)

        # New kernel clinet
        kernel_client = AsyncKernelClient()
        kernel_client.load_connection_info(kernel.conn_info)

        return LocalPyBox(kernel_id=kernel_id, client=kernel_client)

    def shutdown(self, kernel_id: str, **kwargs) -> None:
        """Shutdown the kernel in kubernetes.

        Args:
            kernel_id (str): kernel_id
        """
        self.client.delete_by_kernel_id(kernel_id, **kwargs)

    async def ashutdown(self, kernel_id: str, **kwargs) -> None:
        """Shutdown the kubernetes kernel by kernel id.

        Args:
            kernel_id (str):  kubernetes kernel id
        """
        return await self.client.adelete_by_kernel_id(kernel_id, **kwargs)
