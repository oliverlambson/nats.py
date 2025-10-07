"""Consumer protocols and types for JetStream."""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Protocol,
    overload,
)

if TYPE_CHECKING:
    from .. import api
    from ..message import Message


@dataclass
class ConsumerInfo:
    """Information about a consumer."""

    stream_name: str
    name: str
    config: dict[str, Any]
    created: int
    delivered: dict[str, int]
    ack_floor: dict[str, int]
    num_ack_pending: int
    num_redelivered: int
    num_waiting: int
    num_pending: int
    cluster: dict[str, Any] | None = None
    push_bound: bool | None = None

    @classmethod
    def from_response(cls, data: api.ConsumerInfo) -> ConsumerInfo:
        return cls(
            stream_name=data["stream_name"],
            name=data["name"],
            config=data["config"],
            created=data["created"],
            delivered=data["delivered"],
            ack_floor=data["ack_floor"],
            num_ack_pending=data["num_ack_pending"],
            num_redelivered=data["num_redelivered"],
            num_waiting=data["num_waiting"],
            num_pending=data["num_pending"],
            cluster=data.get("cluster"),
            push_bound=data.get("push_bound"),
        )


class MessageBatch(Protocol):
    """Protocol for a batch of messages retrieved from a JetStream consumer."""

    def __aiter__(self) -> AsyncIterator[Message]:
        """Return self as an async iterator."""
        ...

    async def __anext__(self) -> Message:
        """Get the next message from the batch."""
        ...


class MessageStream(Protocol):
    """Protocol for a continuous stream of messages from a JetStream consumer."""

    def __aiter__(self) -> AsyncIterator[Message]:
        """Return self as an async iterator."""
        ...

    async def __anext__(self) -> Message:
        """Get the next message from the stream."""
        ...


class Consumer(Protocol):
    """Protocol for a JetStream consumer."""

    @property
    def name(self) -> str:
        """Get the consumer name."""
        ...

    @property
    def stream_name(self) -> str:
        """Get the stream name."""
        ...

    @property
    def info(self) -> ConsumerInfo:
        """Get cached consumer info."""
        ...

    async def get_info(self) -> ConsumerInfo:
        """Get consumer info from the server."""
        ...

    @overload
    async def fetch(
        self,
        max_messages: int,
        max_wait: float | None = None,
        heartbeat: float | None = None,
    ) -> MessageBatch:
        """Fetch a batch of messages from the consumer."""
        ...

    @overload
    async def fetch(
        self,
        *,
        max_wait: float | None = None,
        max_bytes: int | None = None,
        heartbeat: float | None = None,
    ) -> MessageBatch:
        """Fetch a batch of messages from the consumer."""
        ...

    @overload
    async def fetch_nowait(
        self,
        *,
        max_messages: int | None = None,
    ) -> MessageBatch:
        """Fetch a batch of messages from the consumer without waiting."""
        ...

    @overload
    async def fetch_nowait(
        self,
        *,
        max_bytes: int | None = None,
    ) -> MessageBatch:
        """Fetch a batch of messages from the consumer without waiting."""
        ...

    async def next(self, max_wait: float = 5.0) -> Message:
        """Fetch a single message from the consumer."""
        ...

    async def consume(
        self,
        callback: Callable[[Message], Awaitable[None]],
        *,
        max_messages: int = 1,
        max_wait: float | None = None,
        heartbeat: float | None = None,
        max_bytes: int | None = None,
    ) -> MessageStream:
        """Continuously consume messages using a callback handler."""
        ...

    async def messages(
        self,
        *,
        heartbeat: float | None = None,
        max_wait: float | None = None,
        max_messages: int | None = None,
        max_bytes: int | None = None,
    ) -> AsyncIterator[Message]:
        """Get an async iterator for continuous message consumption."""
        ...
