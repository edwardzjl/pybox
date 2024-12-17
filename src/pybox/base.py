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
    def run(self, code: str, **kwargs) -> PyBoxOut:
        """Execute code in the PyBox and return the result.

        Args:
            code (str): code to execute

        Returns:
            PyBoxOut: result of the code execution

        Raises:
            TimeoutError: if the code execution times out
        """


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

    @abstractmethod
    def shutdown(
        self,
        kernel_id: str,
        **kwargs,
    ) -> None:
        """Shutdown the kernel."""
        ...
