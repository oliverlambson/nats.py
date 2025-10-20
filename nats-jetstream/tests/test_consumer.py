"""Tests for JetStream consumer functionality."""

import asyncio

import pytest
from nats.jetstream import JetStream


@pytest.mark.asyncio
async def test_consumer_fetch(jetstream: JetStream):
    """Test fetching a batch of messages from a consumer."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_fetch", subjects=["FETCH.*"])

    # Publish some messages
    messages = []
    for i in range(10):
        message = f"message {i}".encode()
        _ack = await jetstream.publish(f"FETCH.{i}", message)
        messages.append((f"FETCH.{i}", message))

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="fetch_consumer", durable_name="fetch_consumer", filter_subject="FETCH.*", deliver_policy="all"
    )

    # Fetch a batch of messages
    batch = await consumer.fetch(max_messages=5, max_wait=1.0)

    # Collect all messages from the batch
    received = []
    async for msg in batch:
        received.append((msg.subject, msg.data))
        # Acknowledge the message
        await msg.ack()

    # Verify we received the expected messages
    assert len(received) == 5
    for i in range(5):
        assert received[i] == messages[i]

    # Fetch another batch
    batch2 = await consumer.fetch(max_messages=5, max_wait=1.0)

    # Collect all messages from the second batch
    received2 = []
    async for msg in batch2:
        received2.append((msg.subject, msg.data))
        # Acknowledge the message
        await msg.ack()

    # Verify we received the remaining messages
    assert len(received2) == 5
    for i in range(5):
        assert received2[i] == messages[i + 5]


@pytest.mark.asyncio
async def test_consumer_fetch_with_max_wait(jetstream: JetStream):
    """Test fetching messages with max_wait timeout working properly."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_fetch_timeout", subjects=["TIMEOUT.*"])

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="timeout_consumer", durable_name="timeout_consumer", filter_subject="TIMEOUT.*", deliver_policy="all"
    )

    # Fetch a batch with a very short timeout (no messages in stream)
    batch = await consumer.fetch(max_messages=5, max_wait=0.5)

    # Should timeout without any messages
    received = []
    async for msg in batch:
        received.append(msg)

    # Verify we received no messages due to timeout
    assert len(received) == 0


@pytest.mark.asyncio
async def test_consumer_fetch_nowait(jetstream: JetStream):
    """Test fetching messages with nowait option enabled.

    This is equivalent to FetchNoWait in the Go client.
    """
    # Create a stream
    stream = await jetstream.create_stream(name="test_no_wait", subjects=["NOWAIT.*"])

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="no_wait_consumer", durable_name="no_wait_consumer", filter_subject="NOWAIT.*", deliver_policy="all"
    )

    # Fetch with nowait when no messages are available
    batch = await consumer.fetch_nowait(max_messages=5)

    # Should return immediately with no messages
    received = []
    async for msg in batch:
        received.append(msg)

    # Verify we received no messages
    assert len(received) == 0

    # Now publish some messages
    messages = []
    for i in range(3):
        message = f"nowait {i}".encode()
        _ack = await jetstream.publish(f"NOWAIT.{i}", message)
        messages.append((f"NOWAIT.{i}", message))

    # Fetch with nowait - should get available messages
    batch = await consumer.fetch_nowait(max_messages=5)

    # Collect all messages from the batch
    received = []
    async for msg in batch:
        received.append((msg.subject, msg.data))
        await msg.ack()

    # Verify we received only the available messages
    assert len(received) == 3
    for i in range(3):
        assert received[i] == messages[i]


@pytest.mark.asyncio
async def test_consumer_fetch_with_max_bytes(jetstream: JetStream):
    """Test fetching messages with max_bytes option."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_max_bytes", subjects=["MAXBYTES.*"])

    # Publish some messages with different sizes
    small_msg = b"small"  # 5 bytes
    large_msg = b"a" * 1000  # 1000 bytes

    await jetstream.publish("MAXBYTES.1", small_msg)
    await jetstream.publish("MAXBYTES.2", large_msg)
    await jetstream.publish("MAXBYTES.3", small_msg)

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="max_bytes_consumer", durable_name="max_bytes_consumer", filter_subject="MAXBYTES.*", deliver_policy="all"
    )

    # Fetch with max_bytes limiting to approximately 1 large message + 1 small message
    batch = await consumer.fetch(
        max_bytes=1010,  # Just enough for the large message and one small one
        max_wait=1.0,
    )

    # Collect all messages from the batch
    received = []
    async for msg in batch:
        received.append(msg.data)
        await msg.ack()

    # Verify we received at most 2 messages due to byte limit
    assert 1 <= len(received) <= 2

    # If we got 2 messages, verify they match what we expect
    if len(received) == 2:
        assert small_msg in received
        assert large_msg in received


@pytest.mark.skip(reason="FIXME")
@pytest.mark.asyncio
async def test_consumer_delete_during_fetch(jetstream: JetStream):
    """Test deleting a consumer while a fetch is in progress."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_delete", subjects=["DELETE.*"])

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="delete_consumer", durable_name="delete_consumer", filter_subject="DELETE.*", deliver_policy="all"
    )

    # Start a fetch with a long max_wait - since there are no messages, it will wait
    batch = await consumer.fetch(max_messages=10, max_wait=5.0)

    # In another task, delete the consumer after a short delay
    async def delete_after_delay():
        await asyncio.sleep(0.5)
        await stream.delete_consumer(consumer.name)

    # Start the deletion task
    delete_task = asyncio.create_task(delete_after_delay())

    # Process messages - since there are none and the consumer is deleted,
    # we expect an exception when trying to iterate
    with pytest.raises(Exception) as excinfo:
        async for msg in batch:
            # We shouldn't get here, but if we do, acknowledge the message
            await msg.ack()

    # Verify the exception is related to consumer deletion
    assert "consumer" in str(excinfo.value).lower() or "timeout" in str(excinfo.value).lower()

    # Make sure the delete task completes
    await delete_task

    # Now try to publish some messages - this won't affect the test outcome
    # since the consumer is already deleted
    for i in range(5):
        await jetstream.publish(f"DELETE.{i}", f"delete {i}".encode())


@pytest.mark.asyncio
async def test_consumer_next(jetstream: JetStream):
    """Test fetching single messages one by one (similar to Next in Go client)."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_next", subjects=["NEXT.*"])

    # Publish some messages
    messages = []
    for i in range(5):
        message = f"next {i}".encode()
        await jetstream.publish(f"NEXT.{i}", message)
        messages.append((f"NEXT.{i}", message))

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="next_consumer", durable_name="next_consumer", filter_subject="NEXT.*", deliver_policy="all"
    )

    # Fetch messages one by one
    received = []
    for i in range(5):
        msg = await consumer.next()
        received.append((msg.subject, msg.data))
        await msg.ack()

    # Verify we received all expected messages
    assert len(received) == 5
    for i in range(5):
        assert received[i] == messages[i]

    # Attempt to fetch another message with a short timeout
    with pytest.raises(asyncio.TimeoutError):
        await consumer.next(max_wait=0.5)


@pytest.mark.skip(reason="FIXME")
@pytest.mark.asyncio
async def test_consumer_delete_during_next(jetstream: JetStream):
    """Test deleting a consumer while waiting for a message with next."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_delete_next", subjects=["DELNEXT.*"])

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="delete_next_consumer",
        durable_name="delete_next_consumer",
        filter_subject="DELNEXT.*",
        deliver_policy="all",
    )

    # In another task, delete the consumer after a short delay
    async def delete_after_delay():
        await asyncio.sleep(0.5)
        await stream.delete_consumer(consumer.name)

    # Start the deletion task
    delete_task = asyncio.create_task(delete_after_delay())

    # Try to get a next message with a longer timeout (should be interrupted by deletion)
    # since there are no messages, next() will wait until the consumer is deleted
    with pytest.raises(Exception) as excinfo:
        await consumer.next(max_wait=2.0)

    # Verify the exception is related to consumer deletion
    assert "consumer" in str(excinfo.value).lower() or "delete" in str(excinfo.value).lower()

    # Make sure the delete task completes
    await delete_task

    # Now try to publish a message - this won't affect the test outcome
    # since the consumer is already deleted
    await jetstream.publish("DELNEXT.1", b"test message")


@pytest.mark.asyncio
async def test_fetch_single_messages_one_by_one(jetstream: JetStream):
    """Test fetching single messages one by one using fetch with batch=1."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_fetch_single", subjects=["SINGLE.*"])

    # Publish some messages
    messages = []
    for i in range(5):
        message = f"single {i}".encode()
        await jetstream.publish(f"SINGLE.{i}", message)
        messages.append((f"SINGLE.{i}", message))

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="single_consumer", durable_name="single_consumer", filter_subject="SINGLE.*", deliver_policy="all"
    )

    # Fetch messages one by one using batch=1
    received = []
    for i in range(5):
        batch = await consumer.fetch(max_messages=1, max_wait=1.0)
        async for msg in batch:
            received.append((msg.subject, msg.data))
            await msg.ack()

    # Verify we received all expected messages
    assert len(received) == 5
    for i in range(5):
        assert received[i] == messages[i]


@pytest.mark.asyncio
async def test_consumer_messages_as_iterator(jetstream: JetStream):
    """Test using messages() method to get a message stream for async iteration."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_consume_iter", subjects=["CONSUMEITER.*"])

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="consume_iter_consumer",
        durable_name="consume_iter_consumer",
        filter_subject="CONSUMEITER.*",
        deliver_policy="all",
    )

    # Create a message stream for manual iteration
    message_stream = await consumer.messages(
        max_messages=3,
        max_wait=1.0,
    )

    # Publish some messages first
    messages = []
    for i in range(3):
        message = f"consume_iter {i}".encode()
        await jetstream.publish(f"CONSUMEITER.{i}", message)
        messages.append((f"CONSUMEITER.{i}", message))

    # Give some time for messages to be processed
    await asyncio.sleep(0.5)

    # Collect messages using the iterator
    received = []

    async def collect_messages():
        async for msg in message_stream:
            received.append((msg.subject, msg.data))
            await msg.ack()
            if len(received) >= 3:
                break

    # Start collecting in the background
    collect_task = asyncio.create_task(collect_messages())

    try:
        # Wait for collection to complete
        await asyncio.wait_for(collect_task, timeout=5.0)

        # Stop the message stream
        await message_stream.stop()

        # Verify we received all the messages
        assert len(received) == 3
        for i in range(3):
            assert received[i] == messages[i]

    finally:
        # Ensure cleanup
        if not collect_task.done():
            collect_task.cancel()

        await message_stream.stop()


@pytest.mark.asyncio
async def test_consumer_messages_with_max_bytes_and_max_messages(jetstream: JetStream):
    """Test messages() method with both max_messages and max_bytes specified."""
    # Create a stream
    stream = await jetstream.create_stream(name="test_messages_both", subjects=["MSGBOTH.*"])

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="messages_both_consumer",
        durable_name="messages_both_consumer",
        filter_subject="MSGBOTH.*",
        deliver_policy="all",
        ack_policy="explicit",
    )

    # Publish a few messages
    for i in range(3):
        await jetstream.publish(f"MSGBOTH.{i}", f"message {i}".encode())

    # Test messages() with both limits
    message_stream = await consumer.messages(
        max_messages=5,  # Allow up to 5 messages
        max_bytes=100,  # Limit to 100 bytes per batch
        max_wait=1.0,
    )

    try:
        received = []
        async for msg in message_stream:
            received.append((msg.subject, msg.data))
            await msg.ack()
            if len(received) >= 3:  # Got all published messages
                break

        # Should have received all 3 messages
        assert len(received) == 3

    finally:
        await message_stream.stop()


@pytest.mark.asyncio
async def test_consumer_prioritized_fetch(jetstream: JetStream):
    """Test priority-based message consumption using fetch.

    Based on Go test: TestConsumerPrioritized - Fetch subtest.
    Validates that lower priority values receive messages first.
    Requires NATS server 2.12.0+
    """
    # Create a stream
    stream = await jetstream.create_stream(name="test_prioritized", subjects=["PRIORITY.*"])

    # Create a consumer with prioritized policy
    consumer = await stream.create_consumer(
        name="priority_consumer",
        durable_name="priority_consumer",
        filter_subject="PRIORITY.*",
        deliver_policy="all",
        ack_policy="explicit",
        priority_policy="prioritized",
        priority_groups=["A"],
    )

    # Start both fetch requests BEFORE publishing messages
    # This ensures both requests are waiting when messages arrive
    # Lower priority consumer (priority=1) requests 100 messages
    batch1_task = asyncio.create_task(
        consumer.fetch(max_messages=100, max_wait=5.0, priority_group="A", priority=1)
    )

    # Higher priority consumer (priority=0) requests 75 messages
    batch2_task = asyncio.create_task(
        consumer.fetch(max_messages=75, max_wait=5.0, priority_group="A", priority=0)
    )

    # Give both requests time to be sent to the server
    await asyncio.sleep(0.1)

    # Now publish 100 messages - they should be distributed by priority
    for i in range(100):
        await jetstream.publish("PRIORITY.test", f"message {i}".encode())

    # Wait for both batches to complete
    batch2 = await batch2_task
    batch1 = await batch1_task

    # Collect messages from higher priority consumer (should get 75)
    received2 = []
    async for msg in batch2:
        received2.append(msg.data)
        await msg.ack()

    # Collect messages from lower priority consumer (should get 25)
    received1 = []
    async for msg in batch1:
        received1.append(msg.data)
        await msg.ack()

    # Higher priority (0) should have received 75 messages
    assert len(received2) == 75
    # Lower priority (1) should have received remaining 25 messages
    assert len(received1) == 25


@pytest.mark.asyncio
async def test_consumer_prioritized_next(jetstream: JetStream):
    """Test priority-based message consumption using next() method.
    Requires NATS server 2.12.0+
    """
    # Create a stream
    stream = await jetstream.create_stream(name="test_prioritized_next", subjects=["PNEXT.*"])

    # Create a consumer with prioritized policy
    consumer = await stream.create_consumer(
        name="priority_next_consumer",
        durable_name="priority_next_consumer",
        filter_subject="PNEXT.*",
        deliver_policy="all",
        ack_policy="explicit",
        priority_policy="prioritized",
        priority_groups=["B"],
    )

    # Publish 10 messages
    for i in range(10):
        await jetstream.publish("PNEXT.test", f"next {i}".encode())

    # Higher priority consumer should get messages
    received_high = []
    for _ in range(7):
        msg = await consumer.next(max_wait=1.0, priority_group="B", priority=0)
        received_high.append(msg.data)
        await msg.ack()

    # Lower priority consumer should get remaining messages
    received_low = []
    for _ in range(3):
        msg = await consumer.next(max_wait=1.0, priority_group="B", priority=5)
        received_low.append(msg.data)
        await msg.ack()

    assert len(received_high) == 7
    assert len(received_low) == 3


@pytest.mark.asyncio
async def test_consumer_prioritized_messages_stream(jetstream: JetStream):
    """Test priority-based message consumption using messages() stream.

    Based on Go test: TestConsumerPrioritized - Messages subtest.
    Requires NATS server 2.12.0+
    """
    # Create a stream
    stream = await jetstream.create_stream(name="test_prioritized_stream", subjects=["PSTREAM.*"])

    # Create a consumer with prioritized policy
    consumer = await stream.create_consumer(
        name="priority_stream_consumer",
        durable_name="priority_stream_consumer",
        filter_subject="PSTREAM.*",
        deliver_policy="all",
        ack_policy="explicit",
        priority_policy="prioritized",
        priority_groups=["C"],
    )

    # Publish 50 messages
    for i in range(50):
        await jetstream.publish("PSTREAM.test", f"stream {i}".encode())

    # Create two message streams with different priorities
    stream1 = await consumer.messages(
        max_messages=10,
        max_wait=2.0,
        priority_group="C",
        priority=1,  # Lower priority
    )

    stream2 = await consumer.messages(
        max_messages=10,
        max_wait=2.0,
        priority_group="C",
        priority=0,  # Higher priority
    )

    try:
        # Higher priority stream should receive more messages
        received1 = []
        received2 = []

        async def collect1():
            async for msg in stream1:
                received1.append(msg.data)
                await msg.ack()
                if len(received1) >= 20:
                    break

        async def collect2():
            async for msg in stream2:
                received2.append(msg.data)
                await msg.ack()
                if len(received2) >= 30:
                    break

        # Run both collectors concurrently
        await asyncio.gather(
            asyncio.wait_for(collect1(), timeout=5.0),
            asyncio.wait_for(collect2(), timeout=5.0),
        )

        # Higher priority (0) should receive more messages than lower priority (1)
        # Exact distribution may vary, but higher priority should dominate
        assert len(received2) >= len(received1)

    finally:
        await stream1.stop()
        await stream2.stop()
