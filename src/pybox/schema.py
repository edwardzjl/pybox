from __future__ import annotations

import json
from datetime import datetime  # noqa: TCH003
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator

from pybox.utils import clean_ansi_codes


class PyBoxOut(BaseModel):
    data: list[dict[str, str]] = []
    """Complete output of the code execution. """
    error: ErrorContent | None = None
    """Information about errors that occurred during execution."""


class CreateKernelRequest(BaseModel):
    name: str | None = None
    """Kernel spec name (defaults to default kernel spec for server)."""
    env: dict[str, Any] | None = None
    """A dictionary of environment variables and values to include in the kernel process - subject to filtering."""

    @field_validator("env")
    @classmethod
    def convert_env_value_to_str(cls, v: dict[str, Any]) -> dict[str, str]:
        if "KERNEL_VOLUME_MOUNTS" in v and not isinstance(v["KERNEL_VOLUME_MOUNTS"], str):
            v["KERNEL_VOLUME_MOUNTS"] = json.dumps(v["KERNEL_VOLUME_MOUNTS"])
        if "KERNEL_VOLUMES" in v and not isinstance(v["KERNEL_VOLUMES"], str):
            v["KERNEL_VOLUMES"] = json.dumps(v["KERNEL_VOLUMES"])
        return v

    def model_dump(self):
        return super().model_dump(by_alias=True, exclude_none=True)

    def model_dump_json(self):
        return super().model_dump_json(by_alias=True, exclude_none=True)


class Kernel(BaseModel):
    id: str
    name: str
    last_activity: datetime
    execution_state: str
    """idk if it belongs to {'starting', 'idle', 'busy', 'restarting', 'dead'}"""
    connections: int


class ExecutionHeader(BaseModel):
    msg_id: str = Field(default_factory=lambda: uuid4().hex)
    """msg_id in response is not a UUID."""
    msg_type: str = Field(default="execute_request")
    """I see 'execute_request', 'execute_input', 'execute_reply', 'stream' and 'status', don't know if there's more."""
    username: str | None = None
    """When set to None, the response will generate a fake 'username'."""
    session: str | None = None
    """Optional in request."""
    date: datetime | None = None
    """Optional in request."""
    version: str | None = None
    """Optional in request."""


class ExecutionContent(BaseModel):
    code: str
    silent: bool = False
    store_history: bool = False
    user_expressions: dict = {}
    allow_stdin: bool = False


class ExecutionRequest(BaseModel):
    header: ExecutionHeader = Field(default_factory=ExecutionHeader)
    parent_header: dict = {}
    """IDK what this is for but if I don't include it, the kernel will disconnect."""
    metadata: dict = {}
    """IDK what this is for but if I don't include it, the kernel will disconnect."""
    content: ExecutionContent
    buffers: list = None
    """This seems optional."""
    channel: str = "shell"
    """I see there's 'iopub' and 'shell', don't know if there's more."""

    def model_dump(self):
        return super().model_dump(by_alias=True, exclude_none=True)

    def model_dump_json(self):
        return super().model_dump_json(by_alias=True, exclude_none=True)

    @staticmethod
    def of_code(code: str) -> ExecutionRequest:
        return ExecutionRequest(content=ExecutionContent(code=code))


class ExecutionStatusContent(BaseModel):
    execution_state: str
    """I see 'busy' and 'idle', don't know if there's more."""


class ExecutionReplyContent(BaseModel):
    status: str
    """I see 'ok' and 'error', don't know if there's more."""
    execution_count: int
    user_expressions: dict
    payload: list
    # Following are only available when status == 'error', and seems duplicated. Maybe I should just ignore extra fields.
    traceback: list[str] | None = None
    ename: str | None = None
    evalue: str | None = None
    engine_info: dict | None = None


class ExecutionInputContent(BaseModel):
    code: str
    execution_count: int


class StreamContent(BaseModel):
    name: str
    """I see 'stdout', don't know if there's more."""
    text: str


class ErrorContent(BaseModel):
    ename: str
    evalue: str
    traceback: list[str]

    def __str__(self) -> str:
        return "\n".join([clean_ansi_codes(line) for line in self.traceback])


class ExecutionResultContent(BaseModel):
    data: dict
    metadata: dict
    execution_count: int | None = 0
    transient: dict | None = None


class ExecutionResponse(BaseModel):
    header: ExecutionHeader
    msg_id: str
    """Identical to header.msg_id."""
    msg_type: str
    """I see 'execute_request', 'execute_input', 'execute_reply', 'stream' and 'status', don't know if there's more.
    Identical to header.msg_type."""
    parent_header: ExecutionHeader
    metadata: dict
    content: (
        ExecutionStatusContent
        | ExecutionReplyContent
        | ExecutionInputContent
        | StreamContent
        | ErrorContent
        | ExecutionResultContent
    )
    buffers: list
    channel: str | None = None
    """I see there's 'iopub' and 'shell', don't know if there's more."""
