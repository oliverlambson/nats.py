"""Tests for StreamConfig None semantics for unlimited values."""

import pytest
from nats.jetstream.stream import StreamConfig


def test_stream_config_defaults_to_none_for_limits():
    """Test that limit fields default to None (unlimited)."""
    config = StreamConfig(
        name="test",
        subjects=["TEST.*"],
    )

    assert config.max_msgs is None
    assert config.max_bytes is None
    assert config.max_consumers is None
    assert config.max_age is None


def test_stream_config_none_converts_to_api_values():
    """Test that None converts to appropriate API values in to_request()."""
    config = StreamConfig(
        name="test",
        subjects=["TEST.*"],
        max_msgs=None,
        max_bytes=None,
        max_consumers=None,
        max_age=None,
    )

    request = config.to_request()

    # None should convert to API's "unlimited" values
    assert request["max_msgs"] == -1
    assert request["max_bytes"] == -1
    assert request["max_consumers"] == -1
    assert request["max_age"] == 0


def test_stream_config_explicit_limits_preserved():
    """Test that explicit limit values are preserved."""
    config = StreamConfig(
        name="test",
        subjects=["TEST.*"],
        max_msgs=1000,
        max_bytes=1024 * 1024,
        max_consumers=10,
        max_age=3600_000_000_000,
    )

    request = config.to_request()

    assert request["max_msgs"] == 1000
    assert request["max_bytes"] == 1024 * 1024
    assert request["max_consumers"] == 10
    assert request["max_age"] == 3600_000_000_000


def test_stream_config_from_response_converts_unlimited_to_none():
    """Test that from_response converts API unlimited values to None."""
    api_response = {
        "name": "test",
        "subjects": ["TEST.*"],
        "max_msgs": -1,  # API unlimited
        "max_bytes": -1,  # API unlimited
        "max_consumers": -1,  # API unlimited
        "max_age": 0,  # API unlimited
        "num_replicas": 1,
        "retention": "limits",
        "storage": "file",
    }

    config = StreamConfig.from_response(api_response)

    # API unlimited values should become None
    assert config.max_msgs is None
    assert config.max_bytes is None
    assert config.max_consumers is None
    assert config.max_age is None


def test_stream_config_from_response_preserves_explicit_limits():
    """Test that from_response preserves explicit limit values."""
    api_response = {
        "name": "test",
        "subjects": ["TEST.*"],
        "max_msgs": 1000,
        "max_bytes": 1024 * 1024,
        "max_consumers": 10,
        "max_age": 3600_000_000_000,
        "num_replicas": 1,
        "retention": "limits",
        "storage": "file",
    }

    config = StreamConfig.from_response(api_response)

    assert config.max_msgs == 1000
    assert config.max_bytes == 1024 * 1024
    assert config.max_consumers == 10
    assert config.max_age == 3600_000_000_000


def test_stream_config_roundtrip_with_none():
    """Test roundtrip conversion with None values."""
    # Start with None (unlimited)
    config1 = StreamConfig(
        name="test",
        subjects=["TEST.*"],
        max_msgs=None,
        max_bytes=None,
    )

    # Convert to API request
    request = config1.to_request()
    assert request["max_msgs"] == -1
    assert request["max_bytes"] == -1

    # Convert back from API response
    config2 = StreamConfig.from_response(request)
    assert config2.max_msgs is None
    assert config2.max_bytes is None


def test_stream_config_roundtrip_with_explicit_values():
    """Test roundtrip conversion with explicit limit values."""
    # Start with explicit limits
    config1 = StreamConfig(
        name="test",
        subjects=["TEST.*"],
        max_msgs=1000,
        max_bytes=1024 * 1024,
    )

    # Convert to API request
    request = config1.to_request()
    assert request["max_msgs"] == 1000
    assert request["max_bytes"] == 1024 * 1024

    # Convert back from API response
    config2 = StreamConfig.from_response(request)
    assert config2.max_msgs == 1000
    assert config2.max_bytes == 1024 * 1024


def test_stream_config_from_kwargs_with_none():
    """Test that from_kwargs handles None values correctly."""
    config = StreamConfig.from_kwargs(
        name="test",
        subjects=["TEST.*"],
        max_msgs=None,
        max_bytes=None,
    )

    assert config.max_msgs is None
    assert config.max_bytes is None

    request = config.to_request()
    assert request["max_msgs"] == -1
    assert request["max_bytes"] == -1


def test_stream_config_from_kwargs_without_limits():
    """Test that from_kwargs works when limit fields are omitted."""
    config = StreamConfig.from_kwargs(
        name="test",
        subjects=["TEST.*"],
    )

    # Should default to None
    assert config.max_msgs is None
    assert config.max_bytes is None
    assert config.max_consumers is None
    assert config.max_age is None


def test_stream_config_mixed_limits():
    """Test that we can mix None (unlimited) and explicit limits."""
    config = StreamConfig(
        name="test",
        subjects=["TEST.*"],
        max_msgs=1000,  # Limited
        max_bytes=None,  # Unlimited
        max_consumers=5,  # Limited
        max_age=None,  # Unlimited
    )

    request = config.to_request()

    assert request["max_msgs"] == 1000
    assert request["max_bytes"] == -1
    assert request["max_consumers"] == 5
    assert request["max_age"] == 0


@pytest.mark.asyncio
async def test_create_stream_with_none_limits(jetstream):
    """Test creating a stream with None limits (unlimited)."""
    from nats.jetstream.stream import StreamConfig

    config = StreamConfig(
        name="test_unlimited",
        subjects=["TEST.*"],
        max_msgs=None,  # Unlimited
        max_bytes=None,  # Unlimited
    )

    stream = await jetstream.create_stream(config)

    # Verify the stream was created with unlimited settings
    assert stream.info.config.max_msgs is None
    assert stream.info.config.max_bytes is None


@pytest.mark.asyncio
async def test_create_stream_kwargs_without_limits(jetstream):
    """Test creating a stream via kwargs without specifying limits."""
    stream = await jetstream.create_stream(
        name="test_defaults",
        subjects=["TEST.*"],
        # No max_msgs, max_bytes specified
    )

    # Should default to None (unlimited)
    assert stream.info.config.max_msgs is None
    assert stream.info.config.max_bytes is None


@pytest.mark.asyncio
async def test_create_stream_with_explicit_limits(jetstream):
    """Test creating a stream with explicit limits."""
    stream = await jetstream.create_stream(
        name="test_limited",
        subjects=["TEST.*"],
        max_msgs=1000,
        max_bytes=1024 * 1024,
    )

    assert stream.info.config.max_msgs == 1000
    assert stream.info.config.max_bytes == 1024 * 1024
