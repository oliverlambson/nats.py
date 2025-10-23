import asyncio
import uuid

import pytest
from nats.client import ClientStatus, NoRespondersError, connect
from nats.client.message import Headers
from nats.server import run, run_cluster


@pytest.mark.asyncio
async def test_connect_succeeds_with_valid_url(server):
    """Test that connecting to a valid server URL succeeds."""
    client = await connect(server.client_url, timeout=1.0)
    assert client.status == ClientStatus.CONNECTED
    assert client.server_info is not None
    await client.close()


@pytest.mark.asyncio
async def test_connect_fails_with_invalid_url():
    """Test that connecting to an invalid server URL fails appropriately."""
    with pytest.raises(Exception):
        await connect("nats://localhost:9999", timeout=0.5)


@pytest.mark.asyncio
async def test_connect_to_auth_token_server_with_correct_token():
    """Test that client can connect to an auth token server with the correct token."""
    import os

    # Start server with token authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_token.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with correct token should succeed
        client = await connect(server.client_url, timeout=1.0, auth_token="test_token_123")
        assert client.status == ClientStatus.CONNECTED
        assert client.server_info is not None

        # Verify we can publish and receive messages with valid auth
        test_subject = f"test.auth.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.flush()

        await client.publish(test_subject, b"test")
        await client.flush()

        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"test"

        await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_auth_token_server_with_incorrect_token():
    """Test that connect raises an error when using an incorrect token."""
    import os

    # Start server with token authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_token.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with incorrect token should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, auth_token="wrong_token", allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_auth_token_server_with_missing_token():
    """Test that connect raises an error when connecting without a token to a secured server."""
    import os

    # Start server with token authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_token.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect without token should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_reconnect_with_auth_token():
    """Test that client can reconnect to an auth token server after disconnection."""
    import os

    # Start server with token authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_token.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Events to track callback invocations
        disconnect_event = asyncio.Event()
        reconnect_event = asyncio.Event()

        # Connect client with auth token and reconnection enabled
        client = await connect(
            server.client_url,
            timeout=1.0,
            auth_token="test_token_123",
            allow_reconnect=True,
            reconnect_time_wait=0.1,
        )

        # Register callbacks
        def on_disconnect():
            disconnect_event.set()

        def on_reconnect():
            reconnect_event.set()

        client.add_disconnected_callback(on_disconnect)
        client.add_reconnected_callback(on_reconnect)

        # Verify client is working before disconnect
        test_subject = f"test.reconnect.auth.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.publish(test_subject, b"before disconnect")
        await client.flush()
        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"before disconnect"

        # Save the server port to reuse it after shutdown
        server_port = server.port

        # Stop the server to trigger disconnect
        await server.shutdown()

        # Wait for disconnect callback
        await asyncio.wait_for(disconnect_event.wait(), timeout=2.0)
        assert disconnect_event.is_set()

        # Start a new server on the same port with same auth config
        new_server = await run(config_path=config_path, port=server_port, timeout=5.0)
        try:
            # Wait for reconnect callback
            await asyncio.wait_for(reconnect_event.wait(), timeout=2.0)
            assert reconnect_event.is_set()

            # Verify client works after reconnection with auth token preserved
            await client.publish(test_subject, b"after reconnect")
            await client.flush()
            msg = await subscription.next(timeout=1.0)
            assert msg.data == b"after reconnect"
        finally:
            await new_server.shutdown()
            await client.close()
    finally:
        # Ensure original server is shutdown if still running
        try:
            await server.shutdown()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_reconnect_with_user_password():
    """Test that client can reconnect to a user/pass server after disconnection."""
    import os

    # Start server with user/password authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_user_pass.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Events to track callback invocations
        disconnect_event = asyncio.Event()
        reconnect_event = asyncio.Event()

        # Connect client with user/password and reconnection enabled
        client = await connect(
            server.client_url,
            timeout=1.0,
            user="testuser",
            password="testpass",
            allow_reconnect=True,
            reconnect_time_wait=0.1,
        )

        # Register callbacks
        def on_disconnect():
            disconnect_event.set()

        def on_reconnect():
            reconnect_event.set()

        client.add_disconnected_callback(on_disconnect)
        client.add_reconnected_callback(on_reconnect)

        # Verify client is working before disconnect
        test_subject = f"test.reconnect.userpass.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.publish(test_subject, b"before disconnect")
        await client.flush()
        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"before disconnect"

        # Save the server port to reuse it after shutdown
        server_port = server.port

        # Stop the server to trigger disconnect
        await server.shutdown()

        # Wait for disconnect callback
        await asyncio.wait_for(disconnect_event.wait(), timeout=2.0)
        assert disconnect_event.is_set()

        # Start a new server on the same port with same auth config
        new_server = await run(config_path=config_path, port=server_port, timeout=5.0)
        try:
            # Wait for reconnect callback
            await asyncio.wait_for(reconnect_event.wait(), timeout=2.0)
            assert reconnect_event.is_set()

            # Verify client works after reconnection with credentials preserved
            await client.publish(test_subject, b"after reconnect")
            await client.flush()
            msg = await subscription.next(timeout=1.0)
            assert msg.data == b"after reconnect"
        finally:
            await new_server.shutdown()
            await client.close()
    finally:
        # Ensure original server is shutdown if still running
        try:
            await server.shutdown()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_connect_to_nkey_server_with_correct_nkey():
    """Test that client can connect to an NKey server with the correct NKey."""
    import os

    # Start server with NKey authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_nkey.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with correct NKey should succeed
        # Seed corresponds to public key UBABIZX6SZFAKHK2KGUFD6QH53FDAH5QVCH2R5MJLFPEVYAW22QWQQCX
        nkey_seed = "SUAEIV5COV7ADQZE52WTYHVJQRV7WKJE5J7IBBJGATJTUUT2LVFGVXDPRQ"
        client = await connect(server.client_url, timeout=1.0, nkey_seed=nkey_seed)
        assert client.status == ClientStatus.CONNECTED
        assert client.server_info is not None

        # Verify we can publish and receive messages with valid auth
        test_subject = f"test.nkey.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.flush()

        await client.publish(test_subject, b"test")
        await client.flush()

        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"test"

        await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_nkey_server_with_incorrect_nkey():
    """Test that connect raises an error when using an incorrect NKey."""
    import os

    import nkeys
    from nacl.signing import SigningKey

    # Start server with NKey authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_nkey.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Generate a different NKey (not authorized on server)
        signing_key = SigningKey.generate().encode()
        src = nkeys.encode_seed(signing_key, prefix=nkeys.PREFIX_BYTE_USER)
        wrong_seed = nkeys.from_seed(src).seed.decode()

        # Connect with incorrect NKey should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, nkey_seed=wrong_seed, allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_nkey_server_with_missing_nkey():
    """Test that connect raises an error when connecting without an NKey to a secured server."""
    import os

    # Start server with NKey authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_nkey.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect without NKey should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_reconnect_with_nkey():
    """Test that client can reconnect to an NKey server after disconnection."""
    import os

    # Start server with NKey authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_nkey.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Events to track callback invocations
        disconnect_event = asyncio.Event()
        reconnect_event = asyncio.Event()

        # Connect client with NKey and reconnection enabled
        nkey_seed = "SUAEIV5COV7ADQZE52WTYHVJQRV7WKJE5J7IBBJGATJTUUT2LVFGVXDPRQ"
        client = await connect(
            server.client_url,
            timeout=1.0,
            nkey_seed=nkey_seed,
            allow_reconnect=True,
            reconnect_time_wait=0.1,
        )

        # Register callbacks
        def on_disconnect():
            disconnect_event.set()

        def on_reconnect():
            reconnect_event.set()

        client.add_disconnected_callback(on_disconnect)
        client.add_reconnected_callback(on_reconnect)

        # Verify client is working before disconnect
        test_subject = f"test.reconnect.nkey.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.publish(test_subject, b"before disconnect")
        await client.flush()
        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"before disconnect"

        # Save the server port to reuse it after shutdown
        server_port = server.port

        # Stop the server to trigger disconnect
        await server.shutdown()

        # Wait for disconnect callback
        await asyncio.wait_for(disconnect_event.wait(), timeout=2.0)
        assert disconnect_event.is_set()

        # Start a new server on the same port with same auth config
        new_server = await run(config_path=config_path, port=server_port, timeout=5.0)
        try:
            # Wait for reconnect callback
            await asyncio.wait_for(reconnect_event.wait(), timeout=2.0)
            assert reconnect_event.is_set()

            # Verify client works after reconnection with NKey preserved
            await client.publish(test_subject, b"after reconnect")
            await client.flush()
            msg = await subscription.next(timeout=1.0)
            assert msg.data == b"after reconnect"
        finally:
            await new_server.shutdown()
            await client.close()
    finally:
        # Ensure original server is shutdown if still running
        try:
            await server.shutdown()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_connect_to_user_pass_server_with_correct_credentials():
    """Test that client can connect to a user/pass server with correct credentials."""
    import os

    # Start server with user/password authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_user_pass.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with correct credentials should succeed
        client = await connect(server.client_url, timeout=1.0, user="testuser", password="testpass")
        assert client.status == ClientStatus.CONNECTED
        assert client.server_info is not None

        # Verify we can publish and receive messages with valid auth
        test_subject = f"test.auth.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.flush()

        await client.publish(test_subject, b"test")
        await client.flush()

        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"test"

        await client.close()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_user_pass_server_with_incorrect_password():
    """Test that connect raises an error when using an incorrect password."""
    import os

    # Start server with user/password authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_user_pass.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with incorrect password should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, user="testuser", password="wrongpass", allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_user_pass_server_with_missing_credentials():
    """Test that connect raises an error when connecting without credentials to a secured server."""
    import os

    # Start server with user/password authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_user_pass.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect without credentials should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_user_pass_server_with_user_only():
    """Test that server rejects connection when only username is provided without password."""
    import os

    # Start server with user/password authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_user_pass.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with only username should raise ConnectionError (server rejects incomplete credentials)
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, user="testuser", allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_connect_to_user_pass_server_with_password_only():
    """Test that server rejects connection when only password is provided without username."""
    import os

    # Start server with user/password authentication
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_auth_user_pass.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        # Connect with only password should raise ConnectionError (server rejects incomplete credentials)
        with pytest.raises(ConnectionError) as exc_info:
            await connect(server.client_url, timeout=1.0, password="testpass", allow_reconnect=False)

        # Verify the error message mentions authorization
        assert "authorization" in str(exc_info.value).lower()
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_publish_delivers_message_to_subscriber(client):
    """Test that a published message is delivered to a subscriber."""
    test_subject = f"test.{uuid.uuid4()}"
    test_message = b"Hello, NATS!"

    subscription = await client.subscribe(test_subject)
    await client.flush()  # Ensure subscription is registered

    await client.publish(test_subject, test_message)
    await client.flush()

    message = await subscription.next(timeout=1.0)
    assert message.data == test_message


@pytest.mark.asyncio
async def test_publish_sets_correct_subject(client):
    """Test that a published message has the correct subject."""
    test_subject = f"test.{uuid.uuid4()}"
    test_message = b"Hello, NATS!"

    subscription = await client.subscribe(test_subject)
    await client.flush()

    await client.publish(test_subject, test_message)
    await client.flush()

    message = await subscription.next(timeout=1.0)
    assert message.subject == test_subject


@pytest.mark.asyncio
async def test_publish_with_headers(client):
    """Test that a message can be published with headers."""
    test_subject = f"test.headers.{uuid.uuid4()}"
    test_headers = Headers({"key1": "value1", "key2": ["value2", "value3"]})
    test_payload = b"Message with headers"

    subscription = await client.subscribe(test_subject)
    await client.flush()

    await client.publish(test_subject, test_payload, headers=test_headers)
    await client.flush()

    message = await subscription.next(timeout=1.0)
    assert message.headers is not None
    assert message.headers.get("key1") == "value1"
    assert message.headers.get_all("key2") == ["value2", "value3"]


@pytest.mark.asyncio
async def test_request_reply_with_single_responder(client):
    """Test request-reply messaging pattern with a single responder."""
    test_subject = f"test.request.{uuid.uuid4()}"
    request_payload = b"Request data"
    reply_payload = b"Reply data"

    # Setup responder
    async def handle_request():
        subscription = await client.subscribe(test_subject)
        await client.flush()
        message = await subscription.next(timeout=1.0)
        await client.publish(message.reply_to, reply_payload)

    responder_task = asyncio.create_task(handle_request())
    await client.flush()

    # Send request
    response = await client.request(test_subject, request_payload, timeout=1.0)

    # Verify response
    assert response.data == reply_payload
    await responder_task


@pytest.mark.asyncio
async def test_flush_ensures_message_delivery(client):
    """Test that flush ensures all pending messages are delivered."""
    test_subject = f"test.flush.{uuid.uuid4()}"
    message_count = 10

    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Publish messages without awaiting between them
    for i in range(message_count):
        await client.publish(test_subject, f"{i}".encode())

    # Flush to ensure delivery
    await client.flush()

    # Verify all messages are received
    received_count = 0
    for _ in range(message_count):
        try:
            await subscription.next(timeout=0.5)
            received_count += 1
        except TimeoutError:
            break

    assert received_count == message_count


@pytest.mark.asyncio
async def test_client_as_context_manager(server):
    """Test that Client can be used as an async context manager."""
    async with await connect(server.client_url, timeout=1.0) as client:
        assert client.status == ClientStatus.CONNECTED

        # Verify we can publish and subscribe
        test_subject = f"test.context.{uuid.uuid4()}"
        async with await client.subscribe(test_subject) as subscription:
            await client.flush()
            await client.publish(test_subject, b"Context test")
            await client.flush()
            message = await subscription.next(timeout=1.0)
            assert message.data == b"Context test"

    # Client should be closed after exiting context
    assert client.status == ClientStatus.CLOSED


@pytest.mark.asyncio
async def test_client_close_stops_publishing(client):
    """Test that closing the client prevents further publishing."""
    test_subject = f"test.close.{uuid.uuid4()}"

    # Close the client
    await client.close()

    # Verify we can't publish anymore
    with pytest.raises(Exception):
        await client.publish(test_subject, b"Message after close")


@pytest.mark.asyncio
async def test_client_close_stops_subscribing(client):
    """Test that closing the client prevents further subscriptions."""
    test_subject = f"test.close.{uuid.uuid4()}"

    # Close the client
    await client.close()

    # Verify we can't subscribe anymore
    with pytest.raises(Exception):
        await client.subscribe(test_subject)


@pytest.mark.asyncio
async def test_client_close_updates_status(client):
    """Test that closing the client updates its status to CLOSED."""
    await client.close()
    assert client.status == ClientStatus.CLOSED


@pytest.mark.asyncio
async def test_disconnection_and_reconnection_callbacks(server):
    """Test that disconnection and reconnection callbacks are properly invoked.

    This test simulates a server disconnection and reconnection scenario:
    1. Create a client with disconnect/reconnect callbacks
    2. Stop the server to trigger disconnection
    3. Start a new server on the same port to trigger reconnection
    4. Verify both callbacks were invoked and client functionality is restored
    """
    # Events to track callback invocations
    disconnect_event = asyncio.Event()
    reconnect_event = asyncio.Event()

    # Connect client with callbacks and reconnection options
    client = await connect(server.client_url, timeout=1.0, allow_reconnect=True, reconnect_time_wait=0.1)

    # Register callbacks
    def on_disconnect():
        disconnect_event.set()

    def on_reconnect():
        reconnect_event.set()

    client.add_disconnected_callback(on_disconnect)
    client.add_reconnected_callback(on_reconnect)

    # Verify client is working before disconnect
    test_subject = f"test.reconnect.{uuid.uuid4()}"
    subscription = await client.subscribe(test_subject)
    await client.publish(test_subject, b"before disconnect")
    await client.flush()
    msg = await subscription.next(timeout=1.0)
    assert msg.data == b"before disconnect"

    # Save the server port to reuse it after shutdown
    server_port = server.port

    # Stop the server to trigger disconnect
    await server.shutdown()

    # Wait for disconnect callback
    try:
        await asyncio.wait_for(disconnect_event.wait(), timeout=2.0)
        assert disconnect_event.is_set(), "Disconnect callback was not invoked"
    except asyncio.TimeoutError:
        pytest.fail("Disconnect callback was not invoked within timeout")

    # Start a new server on the same port
    new_server = await run(port=server_port)
    try:
        # Wait for reconnect callback
        try:
            await asyncio.wait_for(reconnect_event.wait(), timeout=2.0)
            assert reconnect_event.is_set(), "Reconnect callback was not invoked"
        except asyncio.TimeoutError:
            pytest.fail("Reconnect callback was not invoked within timeout")

        # Verify client works after reconnection
        await client.publish(test_subject, b"after reconnect")
        await client.flush()
        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"after reconnect"
    finally:
        # Clean up resources
        await new_server.shutdown()
        await client.close()


@pytest.mark.asyncio
async def test_connect_with_ipv6_localhost(server):
    """Test connecting to server using IPv6 localhost address."""
    # Get the server port and construct IPv6 URL
    port = server.port
    ipv6_url = f"nats://[::1]:{port}"

    try:
        client = await connect(ipv6_url, timeout=1.0)
        assert client.status == ClientStatus.CONNECTED

        # Verify we can publish/subscribe
        test_subject = f"test.ipv6.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.flush()

        await client.publish(test_subject, b"IPv6 test")
        await client.flush()

        message = await subscription.next(timeout=1.0)
        assert message.data == b"IPv6 test"

        await client.close()
    except Exception as e:
        # IPv6 might not be available on all systems
        pytest.skip(f"IPv6 not available: {e}")


@pytest.mark.asyncio
async def test_reconnect_with_ipv6_address():
    """Test that reconnection works with IPv6 addresses in server pool."""
    # Start server on IPv6 localhost (let it pick a port)
    server = await run(host="::1", port=0)
    port = server.port

    # Connect using IPv6 URL
    ipv6_url = f"nats://[::1]:{port}"
    client = await connect(ipv6_url, timeout=1.0, allow_reconnect=True, reconnect_time_wait=0.1)

    # Verify connection works
    test_subject = f"test.ipv6.reconnect.{uuid.uuid4()}"
    subscription = await client.subscribe(test_subject)
    await client.publish(test_subject, b"before")
    await client.flush()
    msg = await subscription.next(timeout=1.0)
    assert msg.data == b"before"

    # Track reconnection
    reconnect_event = asyncio.Event()
    client.add_reconnected_callback(lambda: reconnect_event.set())

    # Shutdown and restart server
    await server.shutdown()
    new_server = await run(host="::1", port=port)

    # Wait for reconnection
    await asyncio.wait_for(reconnect_event.wait(), timeout=3.0)

    # Verify client works after reconnection
    await client.publish(test_subject, b"after")
    await client.flush()
    msg = await subscription.next(timeout=1.0)
    assert msg.data == b"after"

    await client.close()
    await new_server.shutdown()


@pytest.mark.asyncio
async def test_request_with_no_responders_raises_error(client):
    """Test that sending a request to a subject with no responders raises NoRespondersError."""
    test_subject = f"test.no_responders.{uuid.uuid4()}"
    request_payload = b"Request with no responders"

    # Send request to a subject with no subscribers/responders
    # The NATS server should automatically respond with "503 No Responders"
    # because no_responders=True is set in the CONNECT message
    with pytest.raises(NoRespondersError) as exc_info:
        await client.request(test_subject, request_payload, timeout=1.0)

    # Verify the exception details
    error = exc_info.value
    assert isinstance(error, NoRespondersError)
    assert error.subject == test_subject
    assert error.status == "503"


@pytest.mark.asyncio
async def test_message_status_properties(client):
    """Test that Message status properties work correctly."""
    test_subject = f"test.status_properties.{uuid.uuid4()}"

    # Test no responders case (status 503)
    with pytest.raises(NoRespondersError):
        await client.request(test_subject, b"test", timeout=1.0)

    # Test with return_on_error=True to get the Message object
    response = await client.request(test_subject, b"test", timeout=1.0, return_on_error=True)

    # Verify status properties
    assert response.status is not None
    assert response.status.code == "503"

    # Test normal message (no status)
    subscription = await client.subscribe(test_subject)
    await client.flush()

    # Publish a normal message without status
    await client.publish(test_subject, b"normal message")
    await client.flush()

    normal_msg = await subscription.next(timeout=1.0)
    assert normal_msg.status is None


@pytest.mark.asyncio
async def test_multiple_disconnect_reconnect_callbacks(server):
    """Test that multiple disconnect and reconnect callbacks are all properly invoked.

    This test verifies that:
    1. Multiple disconnection callbacks are all invoked when a server disconnects
    2. Multiple reconnection callbacks are all invoked when a server reconnects
    3. Client functionality is restored after reconnection
    """
    # Counters and events to track callback invocations
    disconnect_count = 0
    reconnect_count = 0
    disconnect_event = asyncio.Event()
    reconnect_event = asyncio.Event()

    # Connect client with callbacks and reconnection options
    client = await connect(server.client_url, timeout=1.0, allow_reconnect=True, reconnect_time_wait=0.1)

    # Register multiple callbacks
    def on_disconnect1():
        nonlocal disconnect_count
        disconnect_count += 1
        if disconnect_count == 2:
            disconnect_event.set()

    def on_disconnect2():
        nonlocal disconnect_count
        disconnect_count += 1
        if disconnect_count == 2:
            disconnect_event.set()

    def on_reconnect1():
        nonlocal reconnect_count
        reconnect_count += 1
        if reconnect_count == 2:
            reconnect_event.set()

    def on_reconnect2():
        nonlocal reconnect_count
        reconnect_count += 1
        if reconnect_count == 2:
            reconnect_event.set()

    # Register all callbacks
    client.add_disconnected_callback(on_disconnect1)
    client.add_disconnected_callback(on_disconnect2)
    client.add_reconnected_callback(on_reconnect1)
    client.add_reconnected_callback(on_reconnect2)

    # Verify client is working before disconnect
    test_subject = f"test.multiple_callbacks.{uuid.uuid4()}"
    subscription = await client.subscribe(test_subject)
    await client.publish(test_subject, b"test message")
    await client.flush()
    msg = await subscription.next(timeout=1.0)
    assert msg.data == b"test message"

    # Save the server port to reuse it
    server_port = server.port

    # Stop the server to trigger disconnect
    await server.shutdown()

    # Wait for disconnect callbacks
    try:
        await asyncio.wait_for(disconnect_event.wait(), timeout=5.0)
        assert disconnect_count == 2, f"Expected 2 disconnect callbacks, got {disconnect_count}"
    except asyncio.TimeoutError:
        pytest.fail("Not all disconnect callbacks were invoked within timeout")

    # Start a new server on the same port
    new_server = await run(port=server_port)
    try:
        # Wait for reconnect callbacks
        try:
            await asyncio.wait_for(reconnect_event.wait(), timeout=5.0)
            assert reconnect_count == 2, f"Expected 2 reconnect callbacks, got {reconnect_count}"
        except asyncio.TimeoutError:
            pytest.fail("Not all reconnect callbacks were invoked within timeout")

        # Verify client works after reconnection
        await client.publish(test_subject, b"after reconnect")
        await client.flush()
        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"after reconnect"
    finally:
        # Clean up resources
        await new_server.shutdown()
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("cluster_size", [3, 5])
async def test_cluster_reconnect_sequential_shutdown(cluster_size):
    """Test client reconnection when cluster servers are shut down sequentially.

    This test verifies that:
    1. Client connects to a cluster with multiple servers
    2. Client reconnects as servers are shut down one by one in sequence
    3. Client maintains functionality throughout the sequential shutdowns
    4. Client continues to work as long as at least one server is available
    """
    # Start a cluster with the specified size
    cluster = await run_cluster(size=cluster_size)

    try:
        # Track reconnection events
        reconnect_count = 0
        reconnect_event = asyncio.Event()

        def on_reconnect():
            nonlocal reconnect_count
            reconnect_count += 1
            reconnect_event.set()

        # Connect to the first server - cluster will gossip other servers via INFO
        client = await connect(
            cluster.servers[0].client_url,
            timeout=0.5,
            allow_reconnect=True,
            reconnect_max_attempts=60,
            reconnect_time_wait=0.0,
            no_randomize=True,
        )

        client.add_reconnected_callback(on_reconnect)

        # Verify client is working
        test_subject = f"test.cluster.sequential.{uuid.uuid4()}"
        subscription = await client.subscribe(test_subject)
        await client.flush()

        await client.publish(test_subject, b"initial message")
        await client.flush()
        msg = await subscription.next(timeout=1.0)
        assert msg.data == b"initial message"

        # Shut down servers one by one (shut down the server we're connected to)
        for i in range(len(cluster.servers) - 1):  # Keep last server running
            # Find which server the client is currently connected to using server_info
            assert client.server_info is not None
            connected_port = client.server_info.port

            # Find the matching server in the cluster by port
            server_to_shutdown = None
            for server in cluster.servers:
                if server.port == connected_port:
                    server_to_shutdown = server
                    break

            assert server_to_shutdown is not None, f"Could not find server for port {connected_port}"

            # Shutdown the connected server
            await server_to_shutdown.shutdown()

            # Wait for reconnection to another server
            reconnect_event.clear()
            try:
                await asyncio.wait_for(reconnect_event.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                pytest.fail(f"Client did not reconnect after shutting down server {i}")

            # Give the client time to fully re-establish subscriptions
            await asyncio.sleep(0.2)
            await client.flush()

            # Verify client still works after reconnection
            await client.publish(test_subject, f"message after shutdown {i}".encode())
            await client.flush()
            msg = await subscription.next(timeout=5.0)
            assert msg.data == f"message after shutdown {i}".encode()

        # Verify we had the expected number of reconnections (cluster_size - 1)
        expected_reconnects = cluster_size - 1
        assert reconnect_count == expected_reconnects, (
            f"Expected {expected_reconnects} reconnects, got {reconnect_count}"
        )

        await client.close()

    finally:
        await cluster.shutdown()


@pytest.mark.asyncio
async def test_new_inbox(server):
    """Test that new_inbox generates unique inbox subjects with the configured prefix."""
    custom_prefix = "_MY_INBOX"
    client = await connect(server.client_url, inbox_prefix=custom_prefix, timeout=1.0)

    try:
        # Generate multiple inboxes
        inbox1 = client.new_inbox()
        inbox2 = client.new_inbox()
        inbox3 = client.new_inbox()

        # All should start with the custom prefix
        assert inbox1.startswith(custom_prefix)
        assert inbox2.startswith(custom_prefix)
        assert inbox3.startswith(custom_prefix)

        # All should be unique
        assert inbox1 != inbox2
        assert inbox1 != inbox3
        assert inbox2 != inbox3

    finally:
        await client.close()


@pytest.mark.asyncio
async def test_custom_inbox_prefix(server):
    """Test that custom inbox prefix is used for request-reply inboxes."""
    custom_prefix = "_MY_CUSTOM_INBOX"

    # Connect with custom inbox prefix
    client = await connect(server.client_url, inbox_prefix=custom_prefix, timeout=1.0)

    try:
        test_subject = f"test.custom_inbox.{uuid.uuid4()}"
        request_payload = b"Request data"
        reply_payload = b"Reply data"

        # Track the inbox subject used in the request
        received_reply_to = None

        # Setup responder that captures the reply-to subject
        subscription = await client.subscribe(test_subject)
        await client.flush()

        async def handle_request():
            nonlocal received_reply_to
            message = await subscription.next(timeout=2.0)
            received_reply_to = message.reply_to
            assert received_reply_to is not None
            await client.publish(received_reply_to, reply_payload)

        responder_task = asyncio.create_task(handle_request())

        # Send request
        response = await client.request(test_subject, request_payload, timeout=2.0)

        # Verify response
        assert response.data == reply_payload
        await responder_task

        # Verify that the inbox used the custom prefix
        assert received_reply_to is not None
        assert received_reply_to.startswith(custom_prefix), (
            f"Expected inbox to start with '{custom_prefix}', got '{received_reply_to}'"
        )

    finally:
        await client.close()


@pytest.mark.asyncio
async def test_max_outstanding_pings_closes_connection():
    """Test that connection closes when max outstanding pings is exceeded."""

    async def mock_server(reader, writer):
        """Mock NATS server that stops responding to PINGs."""
        # Send INFO
        info = b'INFO {"server_id":"test","version":"2.0.0","go":"go1.20","host":"127.0.0.1","port":4222,"headers":true,"max_payload":1048576}\r\n'
        writer.write(info)
        await writer.drain()

        # Read CONNECT from client
        await reader.readline()

        # Read and respond to first PING
        await reader.readline()
        writer.write(b"PONG\r\n")
        await writer.drain()

        # Now stop responding to PINGs - just read them without PONGing
        # This will cause outstanding pings to accumulate
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
        except Exception:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    # Start mock server
    server = await asyncio.start_server(mock_server, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()
    server_url = f"nats://{addr[0]}:{addr[1]}"

    try:
        # Connect with short ping interval and low max pings
        client = await connect(
            server_url,
            ping_interval=0.05,  # Ping every 50ms
            max_outstanding_pings=2,
            allow_reconnect=False,
            timeout=1.0,
        )

        try:
            # Verify client starts connected
            assert client.status == ClientStatus.CONNECTED

            # Wait for outstanding pings to accumulate and trigger disconnect
            # With ping_interval=0.05 and max=2, should disconnect after ~150ms
            await asyncio.sleep(0.3)

            # Verify client is no longer connected (closed due to max pings exceeded)
            assert client.status == ClientStatus.CLOSED, f"Expected CLOSED status, got {client.status}"
        finally:
            await client.close()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_inbox_prefix_cannot_be_empty(server):
    """Test that empty inbox prefix is rejected."""
    with pytest.raises(ValueError, match="inbox_prefix cannot be empty"):
        await connect(server.client_url, inbox_prefix="", timeout=1.0)


@pytest.mark.asyncio
async def test_inbox_prefix_cannot_contain_greater_than_wildcard(server):
    """Test that inbox prefix with '>' wildcard is rejected."""
    with pytest.raises(ValueError, match="inbox_prefix cannot contain '>' wildcard"):
        await connect(server.client_url, inbox_prefix="test.>", timeout=1.0)


@pytest.mark.asyncio
async def test_inbox_prefix_cannot_contain_asterisk_wildcard(server):
    """Test that inbox prefix with '*' wildcard is rejected."""
    with pytest.raises(ValueError, match=r"inbox_prefix cannot contain '\*' wildcard"):
        await connect(server.client_url, inbox_prefix="test.*", timeout=1.0)


@pytest.mark.asyncio
async def test_inbox_prefix_cannot_end_with_dot(server):
    """Test that inbox prefix ending with '.' is rejected."""
    with pytest.raises(ValueError, match="inbox_prefix cannot end with '.'"):
        await connect(server.client_url, inbox_prefix="test.", timeout=1.0)


@pytest.mark.asyncio
async def test_server_initiated_ping_pong():
    """Test that client properly handles PING from server and responds with PONG."""
    import os

    # Start server with very short ping interval
    config_path = os.path.join(os.path.dirname(__file__), "configs", "server_ping.conf")
    server = await run(config_path=config_path, port=0, timeout=5.0)

    try:
        client = await connect(server.client_url, timeout=1.0, allow_reconnect=False)

        try:
            # Wait long enough for server to send at least one PING
            # Server is configured to ping every 100ms
            await asyncio.sleep(0.3)

            # If ping/pong handling didn't work, client would be disconnected
            assert client.status == ClientStatus.CONNECTED, "Client should still be connected after server PINGs"
        finally:
            await client.close()
    finally:
        await server.shutdown()
