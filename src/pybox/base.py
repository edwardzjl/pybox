from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pybox.schema import PyBoxOut

logger = logging.getLogger(__name__)


class BasePyBox(ABC):
    """A PyBox is an interface to a kernel that can execute code."""

    @abstractmethod
    def run(self, code: str, **kwargs) -> PyBoxOut | None:
        """Execute code in the PyBox and return the result.

        Args:
            code (str): code to execute

        Returns:
            PyBoxOut | None: result of the code execution

        Raises:
            CodeExecutionException: if the code execution fails
        """

    async def arun(self, code: str, **kwargs) -> PyBoxOut | None:
        """Asynchoronously execute code in the PyBox and return the result.
        Default implementation is to call the synchronous `run` method.

        Args:
            code (str): code to execute

        Returns:
            PyBoxOut | None: result of the code execution

        Raises:
            CodeExecutionException: if the code execution fails
        """
        return self.run(code=code, **kwargs)


class BasePyBoxManager(ABC):
    """Abstract Base class for managing kernels"""

    @abstractmethod
    def start(
        self,
        kernel_id: str | None = None,
        **kwargs,
    ) -> BasePyBox:
        """Retrieve an existing kernel or create a new one.

        Args:
            kernel_id (str): pybox id.

        Returns:
            Kernel: An iPython kernel that executes code.
        """

    async def astart(
        self,
        kernel_id: str | None = None,
        **kwargs,
    ) -> BasePyBox:
        """Retrieve an existing kernel or create a new one.

        Args:
            kernel_id (str): kernel id.

        Returns:
            Kernel: An iPython kernel that executes code.
        """
        return self.start(kernel_id=kernel_id, **kwargs)

    @abstractmethod
    def shutdown(
        self,
        kernel_id: str,
        **kwargs,
    ) -> None:
        """Shutdown the kernel."""
        ...

    async def ashutdown(
        self,
        kernel_id: str,
        **kwargs,
    ) -> None:
        """Shutdown the kernel."""
        self.shutdown(kernel_id=kernel_id, **kwargs)
