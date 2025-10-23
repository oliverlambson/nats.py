"""NATS subscription implementation.

This module provides the Subscription class which represents an active
subscription to a NATS subject. Subscriptions can be used as async
iterators and context managers for ergonomic message handling.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, suppress
from typing import TYPE_CHECKING, Self, TypeVar

if TYPE_CHECKING:
    import types

    from nats.client import Client
from nats.client.errors import MessageQueueFull
from nats.client.message import Message

T = TypeVar("T")


class MessageQueue:
    """A message queue with both message count and byte size limits.

    This wraps asyncio.Queue to add byte-size tracking in addition to
    the standard message count limit.
    """

    _queue: asyncio.Queue[Message]
    _max_messages: int | None
    _max_bytes: int | None
    _pending_messages: int
    _pending_bytes: int
    _callbacks: list[Callable[[Message], None]]

    def __init__(self, max_messages: int | None = None, max_bytes: int | None = None):
        """Initialize the message queue.

        Args:
            max_messages: Maximum number of messages (None for unlimited)
            max_bytes: Maximum total bytes of message payloads (None for unlimited)
        """
        # Create underlying queue with maxsize (0 means unlimited)
        maxsize = max_messages if max_messages is not None else 0
        self._queue = asyncio.Queue(maxsize=maxsize)
        self._max_messages = max_messages
        self._max_bytes = max_bytes
        self._pending_messages = 0
        self._pending_bytes = 0
        self._callbacks = []

    def put_nowait(self, msg: Message) -> None:
        """Put a message in the queue without blocking.

        Args:
            msg: The message to enqueue

        Raises:
            MessageQueueFull: If message count or byte limit would be exceeded
        """
        msg_size = len(msg.data)

        # Check byte limit before attempting to put
        if self._max_bytes is not None and self._pending_bytes + msg_size > self._max_bytes:
            raise MessageQueueFull("byte", self._pending_bytes + msg_size, self._max_bytes)

        # Invoke callbacks before queuing
        for callback in self._callbacks:
            try:
                callback(msg)
            except Exception as e:
                # Log callback errors but don't disrupt message flow
                import logging

                logger = logging.getLogger(__name__)
                logger.exception("Error in message callback: %s", e)

        # Try to put in queue - will raise QueueFull if message limit exceeded
        try:
            self._queue.put_nowait(msg)
        except asyncio.QueueFull:
            # Convert to our custom exception
            raise MessageQueueFull("message", self._pending_messages + 1, self._max_messages) from None

        # Update counters after successful put
        self._pending_messages += 1
        self._pending_bytes += msg_size

    async def get(self, timeout: float | None = None) -> Message:
        """Get a message from the queue.

        Args:
            timeout: Timeout in seconds (None means wait forever)

        Returns:
            The next message

        Raises:
            asyncio.TimeoutError: If timeout is reached
            asyncio.QueueShutDown: If queue is shut down
        """
        # Get message from queue first
        if timeout is not None:
            msg = await asyncio.wait_for(self._queue.get(), timeout)
        else:
            msg = await self._queue.get()

        # Update counters after successful get (only if no exception)
        self._pending_messages -= 1
        self._pending_bytes -= len(msg.data)

        return msg

    def pending(self) -> tuple[int, int]:
        """Get the number of pending messages and bytes.

        Returns:
            Tuple of (pending_messages, pending_bytes)
        """
        return (self._pending_messages, self._pending_bytes)

    def shutdown(self, immediate: bool = False) -> None:
        """Shutdown the queue.

        Args:
            immediate: If True, discard all pending messages
        """
        self._queue.shutdown(immediate=immediate)

    def add_callback(self, callback: Callable[[Message], None]) -> None:
        """Add a callback to be invoked when a message is received.

        Args:
            callback: Function to be called when a message is queued
        """
        self._callbacks.append(callback)

    def remove_callback(self, callback: Callable[[Message], None]) -> None:
        """Remove a callback from the queue.

        Args:
            callback: Function to remove from the callback list
        """
        with suppress(ValueError):
            self._callbacks.remove(callback)


class Subscription(AsyncIterator[Message], AbstractAsyncContextManager["Subscription"]):
    """A subscription to a NATS subject.

    This class represents an active subscription to a NATS subject.
    It can be used as an async iterator to receive messages or as an
    async context manager to automatically close the subscription when done.

    Examples:
        # As an async iterator
        async for msg in subscription:
            process(msg)

        # As a context manager
        async with await client.subscribe("my.subject") as subscription:
            msg = await subscription.next()
            process(msg)
    """

    _subject: str
    _sid: str
    _queue_group: str
    _client: Client
    _pending_queue: MessageQueue
    _closed: bool
    _slow_consumer_reported: bool

    def __init__(
        self,
        subject: str,
        sid: str,
        queue_group: str,
        pending_queue: MessageQueue,
        client: Client,
    ):
        self._subject = subject
        self._sid = sid
        self._queue_group = queue_group
        self._client = client
        self._pending_queue = pending_queue
        self._closed = False
        self._slow_consumer_reported = False

    @property
    def subject(self) -> str:
        """Get the subscription subject."""
        return self._subject

    @property
    def queue_group(self) -> str:
        """Get the queue group name."""
        return self._queue_group

    @property
    def closed(self) -> bool:
        """Get whether the subscription is closed."""
        return self._closed

    def pending(self) -> tuple[int, int]:
        """Get the number of pending messages and bytes.

        Returns:
            Tuple of (pending_messages, pending_bytes)
        """
        return self._pending_queue.pending()

    def add_callback(self, callback: Callable[[Message], None]) -> None:
        """Add a callback to be invoked when a message is received.

        Callbacks are invoked synchronously as soon as a message is received,
        before it is queued in the subscription's message queue.

        Note: Avoid performing heavy computation or blocking operations in callbacks,
        as this will block the I/O pipeline and prevent other messages from being received.

        Args:
            callback: Function to be called when a message is received
        """
        self._pending_queue.add_callback(callback)

    def remove_callback(self, callback: Callable[[Message], None]) -> None:
        """Remove a callback from the subscription.

        Args:
            callback: Function to remove from the callback list
        """
        self._pending_queue.remove_callback(callback)

    async def next(self, timeout: float | None = None) -> Message:
        """Get the next message from the subscription.

        Args:
            timeout: How long to wait for the next message in seconds.
                    If None, wait indefinitely.

        Returns:
            The next message

        Raises:
            asyncio.TimeoutError: If timeout is reached waiting for a message
            RuntimeError: If the subscription is closed and queue is empty
        """
        try:
            return await self._pending_queue.get(timeout)
        except asyncio.QueueShutDown:
            msg = "Subscription is closed"
            raise RuntimeError(msg) from None

    async def __anext__(self) -> Message:
        """Get the next message from the subscription.

        This allows using the subscription as an async iterator:
            async for msg in subscription:
                ...
        """
        try:
            return await self.next()
        except RuntimeError:
            raise StopAsyncIteration from None

    async def unsubscribe(self) -> None:
        """Unsubscribe from this subscription.

        This sends an UNSUB command to the server and marks the subscription as closed,
        preventing further messages from being added to the queue.
        """
        if not self._closed:
            # Send UNSUB to server and remove from client's subscription map
            await self._client._unsubscribe(self._sid)
            # Shutdown queue immediately (discard pending messages)
            self._pending_queue.shutdown(immediate=True)
            # Mark as closed
            self._closed = True

    async def drain(self) -> None:
        """Drain the subscription.

        This unsubscribes from the server (stopping new messages), allowing all pending
        messages that are already in the queue to be processed. After drain, the
        subscription is marked as closed but pending messages can still be consumed.
        """
        if not self._closed:
            # Send UNSUB to server to stop new messages
            await self._client._unsubscribe(self._sid)
            # Shutdown queue gracefully (allow pending messages to be consumed)
            self._pending_queue.shutdown(immediate=False)
            # Keep in client's subscription list until queue is drained
            # Mark as closed
            self._closed = True

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: types.TracebackType | None
    ) -> None:
        """Exit the async context manager, closing the subscription."""
        await self.unsubscribe()
