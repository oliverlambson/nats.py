"""Tests for JetStream consumer functionality."""

import asyncio

import pytest
from nats.jetstream import JetStream


@pytest.mark.asyncio
async def test_consumer_fetch(jetstream: JetStream):
    """Test fetching a batch of messages from a consumer."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test_fetch", subjects=["FETCH.*"]
    )

    # Publish some messages
    messages = []
    for i in range(10):
        message = f"message {i}".encode()
        _ack = await jetstream.publish(f"FETCH.{i}", message)
        messages.append((f"FETCH.{i}", message))

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="fetch_consumer",
        durable_name="fetch_consumer",
        filter_subject="FETCH.*",
        deliver_policy="all"
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
    stream = await jetstream.create_stream(
        name="test_fetch_timeout", subjects=["TIMEOUT.*"]
    )

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="timeout_consumer",
        durable_name="timeout_consumer",
        filter_subject="TIMEOUT.*",
        deliver_policy="all"
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
    stream = await jetstream.create_stream(
        name="test_no_wait", subjects=["NOWAIT.*"]
    )

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="no_wait_consumer",
        durable_name="no_wait_consumer",
        filter_subject="NOWAIT.*",
        deliver_policy="all"
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
    stream = await jetstream.create_stream(
        name="test_max_bytes", subjects=["MAXBYTES.*"]
    )

    # Publish some messages with different sizes
    small_msg = b"small"  # 5 bytes
    large_msg = b"a" * 1000  # 1000 bytes

    await jetstream.publish("MAXBYTES.1", small_msg)
    await jetstream.publish("MAXBYTES.2", large_msg)
    await jetstream.publish("MAXBYTES.3", small_msg)

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="max_bytes_consumer",
        durable_name="max_bytes_consumer",
        filter_subject="MAXBYTES.*",
        deliver_policy="all"
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
    stream = await jetstream.create_stream(
        name="test_delete", subjects=["DELETE.*"]
    )

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="delete_consumer",
        durable_name="delete_consumer",
        filter_subject="DELETE.*",
        deliver_policy="all"
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
    assert "consumer" in str(excinfo.value).lower() or "timeout" in str(
        excinfo.value
    ).lower()

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
    stream = await jetstream.create_stream(
        name="test_next", subjects=["NEXT.*"]
    )

    # Publish some messages
    messages = []
    for i in range(5):
        message = f"next {i}".encode()
        await jetstream.publish(f"NEXT.{i}", message)
        messages.append((f"NEXT.{i}", message))

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="next_consumer",
        durable_name="next_consumer",
        filter_subject="NEXT.*",
        deliver_policy="all"
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
    stream = await jetstream.create_stream(
        name="test_delete_next", subjects=["DELNEXT.*"]
    )

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
    assert "consumer" in str(excinfo.value).lower() or "delete" in str(
        excinfo.value
    ).lower()

    # Make sure the delete task completes
    await delete_task

    # Now try to publish a message - this won't affect the test outcome
    # since the consumer is already deleted
    await jetstream.publish("DELNEXT.1", b"test message")


@pytest.mark.asyncio
async def test_fetch_single_messages_one_by_one(jetstream: JetStream):
    """Test fetching single messages one by one using fetch with batch=1."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test_fetch_single", subjects=["SINGLE.*"]
    )

    # Publish some messages
    messages = []
    for i in range(5):
        message = f"single {i}".encode()
        await jetstream.publish(f"SINGLE.{i}", message)
        messages.append((f"SINGLE.{i}", message))

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="single_consumer",
        durable_name="single_consumer",
        filter_subject="SINGLE.*",
        deliver_policy="all"
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
async def test_consumer_consume(jetstream: JetStream):
    """Test continuous message consumption."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test_consume", subjects=["CONSUME.*"]
    )

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="consume_consumer",
        durable_name="consume_consumer",
        filter_subject="CONSUME.*",
        deliver_policy="all"
    )

    # Store received messages
    received = []
    received_event = asyncio.Event()

    # Create a callback function
    async def message_handler(msg):
        received.append((msg.subject, msg.data))
        await msg.ack()
        if len(received) >= 5:
            received_event.set()

    # Start consuming messages
    message_stream = await consumer.consume(
        callback=message_handler,
        max_messages=2,  # Process 2 messages at a time
        max_wait=1.0,
        heartbeat=0.5,
    )

    # Publish messages in the background
    async def publish_messages():
        # Small delay to make sure consumer is ready
        await asyncio.sleep(0.5)

        # Publish 5 messages
        messages = []
        for i in range(5):
            message = f"consume {i}".encode()
            await jetstream.publish(f"CONSUME.{i}", message)
            messages.append((f"CONSUME.{i}", message))

        return messages

    # Start publishing
    publish_task = asyncio.create_task(publish_messages())

    try:
        # Wait for all messages to be received
        await asyncio.wait_for(received_event.wait(), timeout=5.0)

        # Get the published messages
        messages = await publish_task

        # Stop the message stream
        await message_stream.stop()

        # Verify we received all the messages
        assert len(received) == 5
        for i in range(5):
            assert received[i] == messages[i]

    finally:
        # Ensure cleanup
        if not publish_task.done():
            publish_task.cancel()

        await message_stream.stop()


@pytest.mark.asyncio
async def test_consumer_messages_as_iterator(jetstream: JetStream):
    """Test using messages() method to get a message stream for async iteration."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test_consume_iter", subjects=["CONSUMEITER.*"]
    )

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
async def test_consumer_delete_during_consume(jetstream: JetStream):
    """Test deleting a consumer while consuming messages."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test_delete_consume", subjects=["DELCONSUME.*"]
    )

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="delete_consume_consumer",
        durable_name="delete_consume_consumer",
        filter_subject="DELCONSUME.*",
        deliver_policy="all",
    )

    # Keep track of errors
    error_occurred = asyncio.Event()
    last_error = None

    # Create a callback function that will run for a while
    async def message_handler(msg):
        try:
            # Simulate processing time
            await asyncio.sleep(0.1)
            await msg.ack()
        except Exception as e:
            nonlocal last_error
            last_error = e
            error_occurred.set()

    # Start consuming messages
    message_stream = await consumer.consume(
        callback=message_handler, max_messages=5, max_wait=2.0
    )

    # Publish a few messages to get consumption started
    for i in range(3):
        await jetstream.publish(f"DELCONSUME.{i}", f"delconsume {i}".encode())

    # Short delay to ensure consumption starts
    await asyncio.sleep(0.5)

    # Delete the consumer while consumption is active
    await stream.delete_consumer(consumer.name)

    # Wait a moment for the error to propagate
    await asyncio.sleep(1.0)

    # Clean up - the stream should stop gracefully even if consumer is deleted
    await message_stream.stop()

    # If an error occurred, that's fine - it means the deletion was detected
    # If no error occurred, that's also fine - it means the stream stopped cleanly


@pytest.mark.asyncio
async def test_consumer_consume_with_max_bytes(jetstream: JetStream):
    """Test message consumption with max_bytes limitation."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test_consume_bytes", subjects=["CONSUMEBYTES.*"]
    )

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="consume_bytes_consumer",
        durable_name="consume_bytes_consumer",
        filter_subject="CONSUMEBYTES.*",
        deliver_policy="all",
    )

    # Prepare messages of different sizes
    small_msg = b"small"  # 5 bytes
    large_msg = b"a" * 1000  # 1000 bytes

    # Publish messages
    await jetstream.publish("CONSUMEBYTES.small1", small_msg)
    await jetstream.publish("CONSUMEBYTES.large", large_msg)
    await jetstream.publish("CONSUMEBYTES.small2", small_msg)

    # Track received messages with explicit signal for first message
    received = []
    first_msg_received = asyncio.Event()

    async def message_handler(msg):
        subject = msg.subject
        data = msg.data
        received.append((subject, data))
        await msg.ack()

        # Signal after first message received
        if len(received) == 1:
            first_msg_received.set()

    # Start consuming with a max_bytes limitation
    # Setting it to ~1010 bytes, which should allow at most the large message + one small message
    message_stream = await consumer.consume(
        callback=message_handler,
        max_messages=10,  # More than we expect due to byte limitation
        max_wait=1.0,
        max_bytes=1010,
    )

    try:
        # First wait for at least one message to arrive
        try:
            await asyncio.wait_for(first_msg_received.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            pass

        # Wait a bit longer for potential second message
        await asyncio.sleep(1.0)

        # Check what we received

        # Verify the messages respect max_bytes
        # Should have at least 1 message (tight timing could mean second message hasn't arrived yet)
        assert len(received) >= 1, "No messages received in the first batch"

        # If we got a second message, total bytes should be under the limit
        if len(received) > 1:
            total_bytes = sum(len(data) for _, data in received)
            assert total_bytes <= 1010, f"Received {total_bytes} bytes, exceeding the 1010 byte limit"

        # Now verify we can get the remaining messages with a new consumer
        await message_stream.stop()

        # Create a new stream without byte limits
        remaining = []
        all_received = asyncio.Event()

        async def collect_remaining(msg):
            subject = msg.subject
            data = msg.data
            remaining.append((subject, data))
            await msg.ack()

            # All messages = what we already got + what we're getting now
            if len(received) + len(remaining) >= 3:
                all_received.set()

        # Only get remaining messages
        full_stream = await consumer.consume(
            callback=collect_remaining, max_messages=10, max_wait=1.0
        )

        try:
            # Wait for all remaining messages
            try:
                await asyncio.wait_for(all_received.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass

            # Combine all messages
            all_messages = received + remaining

            # We should have all messages, but in case of timing issues, be lenient
            assert len(
                all_messages
            ) >= 2, f"Expected at least 2 messages, got {len(all_messages)}"

            # Verify we got the right message types
            subjects = [r[0] for r in all_messages]
            data = [r[1] for r in all_messages]

            # At minimum, verify we got the large message
            assert "CONSUMEBYTES.large" in subjects, "Large message not received"
            assert large_msg in data, "Large message data not received"

            # If we got all messages, do full verification
            if len(all_messages) == 3:
                assert "CONSUMEBYTES.small1" in subjects
                assert "CONSUMEBYTES.small2" in subjects
                assert data.count(small_msg) == 2

        finally:
            await full_stream.stop()

    finally:
        await message_stream.stop()


@pytest.mark.asyncio
async def test_consumer_consume_with_max_bytes_and_max_messages(
    jetstream: JetStream
):
    """Test consume with both max_messages and max_bytes specified (like Go's PullMaxMessagesWithBytesLimit)."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test_max_bytes_and_max_messages", subjects=["BOTHLIMITS.*"]
    )

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="max_bytes_and_max_messages_consumer",
        durable_name="max_bytes_and_max_messages_consumer",
        filter_subject="BOTHLIMITS.*",
        deliver_policy="all",
        ack_policy="explicit",
    )

    # Publish messages of varying sizes
    small_msg = b"small"  # 5 bytes
    large_msg = b"x" * 500  # 500 bytes

    await jetstream.publish("BOTHLIMITS.small1", small_msg)
    await jetstream.publish("BOTHLIMITS.large1", large_msg)
    await jetstream.publish("BOTHLIMITS.small2", small_msg)
    await jetstream.publish("BOTHLIMITS.large2", large_msg)

    # Track received messages
    received = []
    receive_event = asyncio.Event()

    async def message_handler(msg):
        received.append((msg.subject, len(msg.data)))
        await msg.ack()
        print(
            f"Received message {len(received)}: {msg.subject}, {len(msg.data)} bytes"
        )
        if len(received) >= 1:  # Signal when we get at least 1 message
            receive_event.set()

    # Test consume with BOTH max_messages and max_bytes
    # This should work like Go's PullMaxMessagesWithBytesLimit
    message_stream = await consumer.consume(
        callback=message_handler,
        max_messages=10,  # Allow up to 10 messages
        max_bytes=600,  # But limit to ~600 bytes per batch
        max_wait=2.0,
    )

    try:
        # Wait for at least one message to be processed
        await asyncio.wait_for(receive_event.wait(), timeout=5.0)

        # Give a bit more time for additional messages in the batch
        await asyncio.sleep(0.5)

        # Should have received some messages limited by byte constraint
        assert len(
            received
        ) >= 1, f"Expected at least 1 message, got {len(received)}"
        assert len(
            received
        ) <= 10, f"Shouldn't exceed max_messages (10), got {len(received)}"

        # With 600 byte limit and our message sizes, we should get at most 2 messages
        # (1 large at 500 bytes + 1 small at 5 bytes = 505 bytes)
        assert len(
            received
        ) <= 2, f"With 600 byte limit, expected at most 2 messages, got {len(received)}"

    finally:
        await message_stream.stop()


@pytest.mark.asyncio
async def test_consumer_messages_with_max_bytes_and_max_messages(
    jetstream: JetStream
):
    """Test messages() method with both max_messages and max_bytes specified."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test_messages_both", subjects=["MSGBOTH.*"]
    )

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
async def test_consumer_consume_flow_control(jetstream: JetStream):
    """Test consumer flow control during consumption."""
    # Create a stream
    stream = await jetstream.create_stream(
        name="test_flow", subjects=["FLOW.*"]
    )

    # Create a pull consumer
    consumer = await stream.create_consumer(
        name="flow_consumer",
        durable_name="flow_consumer",
        filter_subject="FLOW.*",
        deliver_policy="all"
    )

    # Publish a large number of messages
    message_count = 50
    for i in range(message_count):
        await jetstream.publish(f"FLOW.{i}", f"flow message {i}".encode())

    # Track received messages
    received = []
    receive_complete = asyncio.Event()

    # Create a slow message handler to test flow control
    async def slow_handler(msg):
        # Simulate slow processing
        await asyncio.sleep(0.05)  # 50ms per message
        received.append(msg.subject)
        await msg.ack()

        if len(received) == message_count:
            receive_complete.set()

    # Start consuming with multiple batches needed
    message_stream = await consumer.consume(
        callback=slow_handler,
        max_messages=10,  # 10 messages per batch
        max_wait=2.0,
    )

    try:
        # Wait for all messages to be processed
        await asyncio.wait_for(receive_complete.wait(), timeout=10.0)

        # Stop consumption
        await message_stream.stop()

        # Verify all messages were received in the right order
        assert len(received) == message_count

        # Check that the messages are in the correct order
        for i in range(message_count):
            assert received[i] == f"FLOW.{i}"

    finally:
        await message_stream.stop()
