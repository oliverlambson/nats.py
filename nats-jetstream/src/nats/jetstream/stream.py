"""Stream management for JetStream."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Literal,
    Protocol,
    runtime_checkable,
)

from .consumer import Consumer, ConsumerInfo
from .consumer.pull import PullConsumer

CONSUMER_ACTION_CREATE = "create"
CONSUMER_ACTION_UPDATE = "update"
CONSUMER_ACTION_CREATE_OR_UPDATE = ""

if TYPE_CHECKING:
    from . import JetStream, api


@dataclass
class LostStreamData:
    msgs: list[int] | None = None
    bytes: int | None = None

    @classmethod
    def from_response(cls, data: api.LostStreamData) -> LostStreamData:
        return cls(
            msgs=data.get("msgs"),
            bytes=data.get("bytes"),
        )


@dataclass
class ExternalStreamSource:
    api: str
    deliver: str | None = None

    @classmethod
    def from_response(cls, data: api.ExternalStreamSource) -> ExternalStreamSource:
        api_prefix = data["api"]
        deliver = data.get("deliver")

        return cls(
            api=api_prefix,
            deliver=deliver,
        )


@dataclass
class SubjectTransform:
    src: str
    dest: str

    @classmethod
    def from_response(cls, data: api.SubjectTransform) -> SubjectTransform:
        src = data["src"]
        dest = data["dest"]

        return cls(
            src=src,
            dest=dest,
        )


@dataclass
class StreamSource:
    name: str
    opt_start_seq: int | None = None
    opt_start_time: int | None = None
    filter_subject: str | None = None
    external: ExternalStreamSource | None = None
    subject_transforms: Any | None = None

    @classmethod
    def from_response(cls, data: api.StreamSource) -> StreamSource:
        name = data["name"]
        opt_start_seq = data.get("opt_start_seq")
        opt_start_time = data.get("opt_start_time")
        filter_subject = data.get("filter_subject")
        subject_transforms = data.get("subject_transforms")

        external = None
        if data.get("external"):
            external = ExternalStreamSource.from_response(data["external"])

        return cls(
            name=name,
            opt_start_seq=opt_start_seq,
            opt_start_time=opt_start_time,
            filter_subject=filter_subject,
            external=external,
            subject_transforms=subject_transforms,
        )


@dataclass
class StreamSourceInfo:
    name: str
    lag: int
    active: int
    filter_subject: str | None = None
    external: ExternalStreamSource | None = None
    error: dict[str, Any] | None = None
    subject_transforms: Any | None = None

    @classmethod
    def from_response(cls, data: api.StreamSourceInfo) -> StreamSourceInfo:
        name = data["name"]
        lag = data["lag"]
        active = data["active"]
        filter_subject = data.get("filter_subject")
        subject_transforms = data.get("subject_transforms")
        error = data.get("error")

        external = None
        if data.get("external"):
            external = ExternalStreamSource.from_response(data["external"])

        return cls(
            name=name,
            lag=lag,
            active=active,
            filter_subject=filter_subject,
            external=external,
            error=error,
            subject_transforms=subject_transforms,
        )


@dataclass
class Placement:
    cluster: str | None = None
    tags: list[str] | None = None

    @classmethod
    def from_response(cls, data: api.Placement) -> Placement:
        cluster = data.get("cluster")
        tags = data.get("tags")

        return cls(
            cluster=cluster,
            tags=tags,
        )


@dataclass
class PeerInfo:
    name: str
    current: bool
    active: float
    lag: int | None = None
    offline: bool | None = None
    observer: bool | None = None

    @classmethod
    def from_response(cls, data: api.PeerInfo) -> PeerInfo:
        name = data["name"]
        current = data["current"]
        active = data["active"]
        lag = data.get("lag")
        offline = data.get("offline")
        observer = data.get("observer")

        return cls(
            name=name,
            current=current,
            active=active,
            lag=lag,
            offline=offline,
            observer=observer,
        )


@dataclass
class ClusterInfo:
    name: str | None = None
    leader: str | None = None
    replicas: list[PeerInfo] | None = None
    raft_group: str | None = None

    @classmethod
    def from_response(cls, data: api.ClusterInfo) -> ClusterInfo:
        name = data.get("name")
        leader = data.get("leader")
        raft_group = data.get("raft_group")

        replicas = None
        if data.get("replicas"):
            replicas = [PeerInfo.from_response(r) for r in data["replicas"]]

        return cls(
            name=name,
            leader=leader,
            replicas=replicas,
            raft_group=raft_group,
        )


@dataclass
class StreamState:
    messages: int
    bytes: int
    first_sequence: int
    last_sequence: int
    consumer_count: int
    deleted: list[int] | None = None
    num_deleted: int | None = None
    lost: LostStreamData | None = None
    subjects: dict[str, int] | None = None

    @classmethod
    def from_response(cls, data: api.StreamState) -> StreamState:
        messages = data["messages"]
        bytes_val = data["bytes"]
        first_sequence = data["first_seq"]
        last_sequence = data["last_seq"]
        consumer_count = data["consumer_count"]
        deleted = data.get("deleted")
        num_deleted = data.get("num_deleted")
        subjects = data.get("subjects")

        lost = None
        if data.get("lost"):
            lost = LostStreamData.from_response(data["lost"])

        return cls(
            messages=messages,
            bytes=bytes_val,
            first_sequence=first_sequence,
            last_sequence=last_sequence,
            consumer_count=consumer_count,
            deleted=deleted,
            num_deleted=num_deleted,
            lost=lost,
            subjects=subjects,
        )


@dataclass
class StreamConfig:
    """Configuration for a JetStream stream"""

    # All fields are optional with defaults
    max_age: int = 0  # Maximum age of any message in nanoseconds (0 for unlimited)
    max_bytes: int = -1  # Maximum size in bytes (-1 for unlimited)
    max_consumers: int = -1  # Maximum number of consumers (-1 for unlimited)
    max_msgs: int = -1  # Maximum number of messages (-1 for unlimited)
    num_replicas: int = 1  # Number of replicas to keep for each message
    retention: Literal["limits", "interest",
                       "workqueue"] = "limits"  # How messages are retained
    storage: Literal["file", "memory"] = "file"  # Storage backend to use

    allow_direct: bool | None = None  # Allow direct access to get messages
    allow_msg_ttl: bool | None = None  # Enable per-message TTL using headers
    allow_rollup_hdrs: bool | None = None  # Allow Nats-Rollup header usage
    compression: Literal["none", "s2"] | None = None  # Compression algorithm
    deny_delete: bool | None = None  # Restrict message deletion via API
    deny_purge: bool | None = None  # Restrict stream purge via API
    description: str | None = None  # Stream description
    discard: Literal["old", "new"] | None = None  # Discard policy
    discard_new_per_subject: bool | None = None  # Apply discard new per subject
    duplicate_window: int | None = None  # Duplicate tracking window in ns
    first_seq: int | None = None  # First message sequence number
    max_msg_size: int | None = None  # Maximum message size (-1 for unlimited)
    max_msgs_per_subject: int | None = None  # Per-subject message limit
    metadata: dict[str, str] | None = None  # Additional metadata
    mirror: StreamSource | None = None  # Mirror configuration
    mirror_direct: bool | None = None  # Allow direct access for mirrors
    name: str | None = None  # Stream name
    no_ack: bool | None = None  # Disable message acknowledgment
    placement: Placement | None = None  # Replica placement directives
    republish: api.Republish | None = None  # Message republishing config
    sealed: bool | None = None  # Prevent message deletion and updates
    sources: list[StreamSource] | None = None  # Replication sources
    subject_delete_marker_ttl: int | None = None  # TTL for delete markers
    subject_transform: SubjectTransform | None = None  # Subject transform
    subjects: list[str] | None = None  # Subjects to consume
    template_owner: str | None = None  # Managing template name
    consumer_limits: api.StreamConsumerLimits | None = None  # Consumer limits

    @classmethod
    def from_response(cls, config: api.StreamConfig) -> StreamConfig:
        """Create a StreamConfig from an API response"""
        max_age = config.get("max_age", 0)
        max_bytes = config.get("max_bytes", -1)
        max_consumers = config.get("max_consumers", -1)
        max_msgs = config.get("max_msgs", -1)
        num_replicas = config.get("num_replicas", 1)
        retention = config.get("retention", "limits")
        storage = config.get("storage", "file")

        allow_direct = config.get("allow_direct")
        allow_msg_ttl = config.get("allow_msg_ttl")
        allow_rollup_hdrs = config.get("allow_rollup_hdrs")
        compression = config.get("compression")
        deny_delete = config.get("deny_delete")
        deny_purge = config.get("deny_purge")
        description = config.get("description")
        discard = config.get("discard")
        discard_new_per_subject = config.get("discard_new_per_subject")
        duplicate_window = config.get("duplicate_window")
        first_seq = config.get("first_seq")
        max_msg_size = config.get("max_msg_size")
        max_msgs_per_subject = config.get("max_msgs_per_subject")
        metadata = config.get("metadata")
        mirror_direct = config.get("mirror_direct")
        name = config.get("name")
        no_ack = config.get("no_ack")
        republish = config.get("republish")
        sealed = config.get("sealed")
        subjects = config.get("subjects")
        template_owner = config.get("template_owner")

        mirror = None
        if config.get("mirror"):
            mirror = StreamSource.from_response(config["mirror"])

        sources = None
        if config.get("sources"):
            sources = [StreamSource.from_response(s) for s in config["sources"]]

        subject_transform = None
        if config.get("subject_transform"):
            subject_transform = SubjectTransform.from_response(config["subject_transform"])

        placement = None
        if config.get("placement"):
            placement = Placement.from_response(config["placement"])

        return cls(
            max_age=max_age,
            max_bytes=max_bytes,
            max_consumers=max_consumers,
            max_msgs=max_msgs,
            num_replicas=num_replicas,
            retention=retention,
            storage=storage,
            allow_direct=allow_direct,
            allow_msg_ttl=allow_msg_ttl,
            allow_rollup_hdrs=allow_rollup_hdrs,
            compression=compression,
            deny_delete=deny_delete,
            deny_purge=deny_purge,
            description=description,
            discard=discard,
            discard_new_per_subject=discard_new_per_subject,
            duplicate_window=duplicate_window,
            first_seq=first_seq,
            max_msg_size=max_msg_size,
            max_msgs_per_subject=max_msgs_per_subject,
            metadata=metadata,
            mirror=mirror,
            mirror_direct=mirror_direct,
            name=name,
            no_ack=no_ack,
            placement=placement,
            republish=republish,
            sealed=sealed,
            sources=sources,
            subjects=subjects,
            subject_transform=subject_transform,
            template_owner=template_owner,
        )


@dataclass
class StreamInfo:
    config: StreamConfig
    created: int
    state: StreamState
    cluster: ClusterInfo | None = None
    mirror: StreamSourceInfo | None = None
    sources: list[StreamSourceInfo] | None = None

    @classmethod
    def from_response(cls, data: api.StreamInfo) -> StreamInfo:
        config = StreamConfig.from_response(data["config"])
        created = data["created"]
        state = StreamState.from_response(data["state"])
        cluster = ClusterInfo.from_response(data["cluster"]) if data.get("cluster") else None

        mirror = None
        if data.get("mirror"):
            mirror = StreamSourceInfo.from_response(data["mirror"])

        sources = None
        if data.get("sources"):
            sources = [StreamSourceInfo.from_response(s) for s in data["sources"]]

        return cls(
            config=config,
            created=created,
            state=state,
            cluster=cluster,
            mirror=mirror,
            sources=sources,
        )


@runtime_checkable
class StreamManager(Protocol):
    """Protocol for managing JetStream streams."""

    async def stream_names(self) -> AsyncIterator[str]:
        """Get an async iterator over all stream names.

        Yields:
            Stream names one at a time.
        """
        ...

    async def list_streams(self,
                           subject: str | None = None
                           ) -> AsyncIterator[StreamInfo]:
        """Get an async iterator over all streams.

        Args:
            subject: Optional subject filter to match streams against

        Yields:
            StreamInfo objects one at a time.
        """
        ...

    async def create_stream(self, **config) -> Stream:
        """Create a new stream."""
        ...

    async def update_stream(self, **config) -> StreamInfo:
        """Update an existing stream."""
        ...

    async def delete_stream(self, name: str) -> bool:
        """Delete a stream."""
        ...

    async def get_stream_info(
        self,
        name: str,
        *,
        deleted_details: bool = False,
        subjects_filter: str | None = None,
        offset: int | None = None,
    ) -> StreamInfo:
        """Get information about a stream."""
        ...

    async def get_stream(self, name: str) -> Stream:
        """Get a stream by name."""
        ...


@dataclass
class StreamMessage:
    """A message stored in a JetStream stream."""

    subject: str
    sequence: int
    data: bytes
    time: datetime
    headers: dict[str, str] | None = None

    def __getitem__(self, key):
        """Support dictionary-like access to message attributes."""
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(f"StreamMessage has no attribute '{key}'")


class Stream:
    """A JetStream stream."""

    def __init__(
        self,
        jetstream: JetStream,
        name: str,
        info: StreamInfo | None = None
    ) -> None:
        self._jetstream = jetstream
        self._name = name
        self._info = info

    @property
    def name(self) -> str:
        """Get the stream name."""
        return self._name

    @property
    def info(self) -> StreamInfo | None:
        """Get cached stream info."""
        return self._info

    async def get_info(
        self,
        *,
        deleted_details: bool = True,
        subjects_filter: str | None = None,
        offset: int | None = None,
    ) -> StreamInfo:
        """Get stream info from the server."""
        info = await self._jetstream.get_stream_info(
            self._name,
            deleted_details=deleted_details,
            subjects_filter=subjects_filter,
            offset=offset,
        )
        self._info = info
        return info

    async def purge(
        self,
        *,
        filter: str | None = None,
        keep: int | None = None,
        sequence: int | None = None,
    ) -> None:
        """Purge messages from the stream.

        Args:
            filter: Optional subject filter to purge messages for
            keep: Optional number of messages to keep
            sequence: Optional sequence number to purge up to
        """
        # Get the API client from the manager
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        # Build purge request
        request = {}
        if filter is not None:
            request["filter"] = filter
        if keep is not None:
            request["keep"] = keep
        if sequence is not None:
            request["seq"] = sequence

        # Send purge request
        await api.stream_purge(self._name, **request)

    async def _get_message(
        self,
        *,
        sequence: int | None = None,
        last_by_subject: str | None = None
    ) -> StreamMessage:
        """Internal helper to get a message either by sequence or last by subject.

        Args:
            sequence: Optional sequence number to get
            last_by_subject: Optional subject to get last message for

        Returns:
            The stream message

        Raises:
            RuntimeError: If API client is not available
        """
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        # handle direct gets
        if self._info and self._info.config.allow_direct:
            # Use direct message access endpoint
            if sequence is not None:
                response = await api._client.request(
                    f"$JS.API.DIRECT.GET.{self._name}",
                    json.dumps({
                        "seq": sequence
                    }).encode(),
                )
            else:
                # Must be last by subject
                response = await api._client.request(
                    f"$JS.API.DIRECT.GET.{self._name}.{last_by_subject}", b""
                )

            # Direct get returns raw message with headers
            if not hasattr(response, "headers") or not response.headers:
                # If no headers at all, treat as empty headers
                return StreamMessage(
                    subject=last_by_subject or "",
                    sequence=sequence or 0,
                    data=response.data,
                    time=datetime.now(timezone.utc),
                    headers=None,
                )

            # Parse required headers
            stream = response.headers.get("Nats-Stream")
            if not stream:
                raise RuntimeError(
                    "Direct get response missing Nats-Stream header"
                )

            subject = response.headers.get("Nats-Subject")
            if not subject:
                raise RuntimeError(
                    "Direct get response missing Nats-Subject header"
                )

            sequence = response.headers.get("Nats-Sequence")
            if not sequence:
                raise RuntimeError(
                    "Direct get response missing Nats-Sequence header"
                )

            timestamp = response.headers.get("Nats-Time-Stamp")
            if not timestamp:
                raise RuntimeError(
                    "Direct get response missing Nats-Time-Stamp header"
                )

            # Convert list values to single values for all headers
            headers = {}
            for key, value in response.headers.items():
                if isinstance(value, list):
                    headers[key] = value[0] if value else None
                else:
                    headers[key] = value

            # Remove metadata headers
            for key in [
                    "Nats-Stream",
                    "Nats-Subject",
                    "Nats-Sequence",
                    "Nats-Time-Stamp",
            ]:
                headers.pop(key, None)

            # Set headers to None if empty after removing metadata
            if not headers:
                headers = None

            return StreamMessage(
                subject=subject[0] if isinstance(subject, list) else subject,
                sequence=int(
                    sequence[0] if isinstance(sequence, list) else sequence
                ),
                data=response.data,
                time=datetime.fromisoformat(
                    timestamp[0] if isinstance(timestamp, list) else timestamp
                ),
                headers=headers,
            )
        else:
            # Use standard message get which returns:
            # {
            #   "message": {
            #     "subject": "...",
            #     "seq": N,
            #     "time": "...",
            #     "data": "..." (base64),
            #     "hdrs": "..." (base64, optional)
            #     "stream": "...",
            #     "domain": "..."
            #   },
            #   "type": "io.nats.jetstream.api.v1.stream_msg_get_response"
            # }
            response = await api.stream_msg_get(
                self._name, seq=sequence, last_by_subj=last_by_subject
            )
            message = response["message"]

        # Decode base64 data if present
        data = b""  # Initialize with empty bytes by default
        if "data" in message:
            data = base64.b64decode(message["data"])

        # Parse headers if present
        headers = None
        if "hdrs" in message:
            try:
                headers_bytes = base64.b64decode(message["hdrs"])
                headers_str = headers_bytes.decode("utf-8")
                if headers_str.startswith("NATS/1.0\r\n"):
                    # Parse NATS headers format
                    headers = {}
                    lines = headers_str.split("\r\n")
                    for line in lines[1:]:  # Skip NATS/1.0
                        if not line:  # Skip empty lines
                            continue
                        if ":" in line:
                            key, value = line.split(":", 1)
                            headers[key.strip()] = value.strip()

                    # If no headers parsed, set to None
                    if not headers:
                        headers = None
            except (ValueError, UnicodeDecodeError):
                # If we can't parse headers, just ignore them
                headers = None

        return StreamMessage(
            subject=message["subject"],
            sequence=message["seq"],
            data=data,
            time=datetime.fromisoformat(
                message["time"].replace("Z", "+00:00")
            ),
            headers=headers,
        )

    async def get_message(self, sequence: int) -> StreamMessage:
        """Get a message by sequence number.

        If the stream has allow_direct=true configured, this will use direct message access.
        Otherwise, it will use the standard message get API.

        Args:
            sequence: The sequence number of the message to get

        Returns:
            The stream message including subject, data, headers, etc.
        """
        return await self._get_message(sequence=sequence)

    async def get_last_message_for_subject(
        self, subject: str
    ) -> StreamMessage:
        """Get the last message for a subject.

        If the stream has allow_direct=true configured, this will use direct message access.
        Otherwise, it will use the standard message get API.

        Args:
            subject: The subject to get the last message for

        Returns:
            The stream message including subject, data, headers, etc.
        """
        return await self._get_message(last_by_subject=subject)

    async def delete_message(self, sequence: int) -> None:
        """Delete a message from the stream.

        This removes the message from the stream without secure erasure.
        For secure deletion that overwrites data, use secure_delete_message().

        Args:
            sequence: The sequence number of the message to delete
        """
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        await api.stream_msg_delete(self._name, seq=sequence, no_erase=True)

    async def secure_delete_message(self, sequence: int) -> None:
        """Securely delete a message from the stream.

        This removes the message and overwrites its data with random data
        for secure erasure. This is slower than regular deletion.

        Args:
            sequence: The sequence number of the message to delete
        """
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        await api.stream_msg_delete(self._name, seq=sequence, no_erase=False)

    async def upsert_consumer(
        self,
        action: str = CONSUMER_ACTION_CREATE_OR_UPDATE,
        **config
    ) -> Consumer:
        """Upsert a consumer for this stream, mirroring Go's upsertConsumer logic.

        Args:
            name: Name of the consumer
            action: Action to perform ("create" or "update")
            **config: Additional consumer configuration

        Returns:
            The created/updated consumer
        """
        # Get API client from jetstream
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        name = config.get("name")
        if name is None:
            durable_name = config.get("durable_name")
            if durable_name is None:
                name = f"consumer-{base64.b64encode(str(datetime.now(timezone.utc)).encode()).decode()}"
            else:
                name = durable_name

        request = {
            "action": action,
            "stream_name": self._name,
            "config": {
                **config,
                "name": name,
            },
        }

        # Create/update consumer via API
        response = await api.consumer_create(**request)
        consumer_info = ConsumerInfo.from_response(response)

        # Check if this is a push consumer (has deliver_subject)
        if consumer_info.config.get("deliver_subject"):
            raise NotImplementedError("Push consumers are not supported yet")

        # Return new consumer instance using the actual name from the server response
        return PullConsumer(self, consumer_info)

    async def create_consumer(self, name: str, **config) -> Consumer:
        """Create a consumer for this stream.

        Args:
            name: Name of the consumer
            **config: Additional consumer configuration

        Returns:
            The created consumer
        """
        # Get API client from jetstream
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        # Delegate to upsert_consumer with "create" action
        return await self.upsert_consumer(
            action=CONSUMER_ACTION_CREATE, **{
                **config, "name": name
            }
        )

    async def get_consumer_info(self, consumer_name: str) -> ConsumerInfo:
        """Get consumer info."""
        # Get API client from jetstream
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        # Get consumer info via API
        response = await api.consumer_info(self._name, consumer_name)
        return ConsumerInfo.from_response(response)

    async def get_consumer(self, consumer_name: str) -> Consumer:
        """Get a consumer by name."""
        # Get consumer info
        consumer_info = await self.get_consumer_info(consumer_name)

        # Check if this is a push consumer (has deliver_subject)
        if consumer_info.config.get("deliver_subject"):
            raise NotImplementedError("Push consumers are not supported yet")

        # Return new consumer instance
        return PullConsumer(self, consumer_info)

    async def delete_consumer(self, consumer_name: str) -> bool:
        """Delete a consumer."""
        # Get API client from jetstream
        api = getattr(self._jetstream, "_api", None)
        if api is None:
            raise RuntimeError("JetStream does not have an API client")

        # Delete consumer via API
        response = await api.consumer_delete(self._name, consumer_name)
        return response["success"]

    async def update_consumer(self, consumer_name: str, **config) -> Consumer:
        """Update a consumer.

        Args:
            consumer_name: Name of the consumer to update
            **config: New consumer configuration

        Returns:
            The updated consumer
        """
        return await self.upsert_consumer(
            action=CONSUMER_ACTION_UPDATE, name=consumer_name, **config
        )
