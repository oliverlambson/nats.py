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

    def to_request(self) -> api.ExternalStreamSource:
        """Convert to API request format."""
        result: api.ExternalStreamSource = {"api": self.api}
        if self.deliver is not None:
            result["deliver"] = self.deliver
        return result


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

    def to_request(self) -> api.SubjectTransform:
        """Convert to API request format."""
        result: api.SubjectTransform = {"src": self.src, "dest": self.dest}
        return result


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

    def to_request(self) -> api.StreamSource:
        """Convert to API request format."""
        result: api.StreamSource = {"name": self.name}
        if self.opt_start_seq is not None:
            result["opt_start_seq"] = self.opt_start_seq
        if self.opt_start_time is not None:
            result["opt_start_time"] = self.opt_start_time
        if self.filter_subject is not None:
            result["filter_subject"] = self.filter_subject
        if self.external is not None:
            result["external"] = self.external.to_request()
        if self.subject_transforms is not None:
            result["subject_transforms"] = self.subject_transforms
        return result


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

    def to_request(self) -> api.Placement:
        """Convert to API request format."""
        result: api.Placement = {}
        if self.cluster is not None:
            result["cluster"] = self.cluster
        if self.tags is not None:
            result["tags"] = self.tags
        return result


@dataclass
class Republish:
    src: str
    dest: str
    headers_only: bool | None = None

    @classmethod
    def from_response(cls, data: api.Republish) -> Republish:
        src = data["src"]
        dest = data["dest"]
        headers_only = data.get("headers_only")

        return cls(
            src=src,
            dest=dest,
            headers_only=headers_only,
        )

    def to_request(self) -> api.Republish:
        """Convert to API request format."""
        result: api.Republish = {"src": self.src, "dest": self.dest}
        if self.headers_only is not None:
            result["headers_only"] = self.headers_only
        return result


@dataclass
class StreamConsumerLimits:
    inactive_threshold: int | None = None
    max_ack_pending: int | None = None

    @classmethod
    def from_response(cls, data: api.StreamConsumerLimits) -> StreamConsumerLimits:
        inactive_threshold = data.get("inactive_threshold")
        max_ack_pending = data.get("max_ack_pending")

        return cls(
            inactive_threshold=inactive_threshold,
            max_ack_pending=max_ack_pending,
        )

    def to_request(self) -> api.StreamConsumerLimits:
        """Convert to API request format."""
        result: api.StreamConsumerLimits = {}
        if self.inactive_threshold is not None:
            result["inactive_threshold"] = self.inactive_threshold
        if self.max_ack_pending is not None:
            result["max_ack_pending"] = self.max_ack_pending
        return result


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
    republish: Republish | None = None  # Message republishing config
    sealed: bool | None = None  # Prevent message deletion and updates
    sources: list[StreamSource] | None = None  # Replication sources
    subject_delete_marker_ttl: int | None = None  # TTL for delete markers
    subject_transform: SubjectTransform | None = None  # Subject transform
    subjects: list[str] | None = None  # Subjects to consume
    template_owner: str | None = None  # Managing template name
    consumer_limits: StreamConsumerLimits | None = None  # Consumer limits

    @classmethod
    def from_kwargs(cls, **kwargs) -> StreamConfig:
        """Create a StreamConfig from keyword arguments, converting dicts to dataclasses."""
        # Convert mirror dict to StreamSource (with nested conversion)
        if "mirror" in kwargs and isinstance(kwargs["mirror"], dict):
            mirror_dict = kwargs["mirror"].copy()
            if "external" in mirror_dict and isinstance(mirror_dict["external"], dict):
                mirror_dict["external"] = ExternalStreamSource(**mirror_dict["external"])
            kwargs["mirror"] = StreamSource(**mirror_dict)

        # Convert placement dict to Placement
        if "placement" in kwargs and isinstance(kwargs["placement"], dict):
            kwargs["placement"] = Placement(**kwargs["placement"])

        # Convert republish dict to Republish
        if "republish" in kwargs and isinstance(kwargs["republish"], dict):
            kwargs["republish"] = Republish(**kwargs["republish"])

        # Convert subject_transform dict to SubjectTransform
        if "subject_transform" in kwargs and isinstance(kwargs["subject_transform"], dict):
            kwargs["subject_transform"] = SubjectTransform(**kwargs["subject_transform"])

        # Convert consumer_limits dict to StreamConsumerLimits
        if "consumer_limits" in kwargs and isinstance(kwargs["consumer_limits"], dict):
            kwargs["consumer_limits"] = StreamConsumerLimits(**kwargs["consumer_limits"])

        # Convert sources list of dicts to list of StreamSource
        if "sources" in kwargs and kwargs["sources"] is not None:
            converted_sources = []
            for source in kwargs["sources"]:
                if isinstance(source, dict):
                    source_dict = source.copy()
                    if "external" in source_dict and isinstance(source_dict["external"], dict):
                        source_dict["external"] = ExternalStreamSource(**source_dict["external"])
                    converted_sources.append(StreamSource(**source_dict))
                else:
                    converted_sources.append(source)
            kwargs["sources"] = converted_sources

        return cls(**kwargs)

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

        republish = None
        if config.get("republish"):
            republish = Republish.from_response(config["republish"])

        consumer_limits = None
        if config.get("consumer_limits"):
            consumer_limits = StreamConsumerLimits.from_response(config["consumer_limits"])

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
            consumer_limits=consumer_limits,
        )

    def to_request(self) -> api.StreamConfig:
        """Convert StreamConfig to an API request dictionary."""
        result: api.StreamConfig = {
            "max_age": self.max_age,
            "max_bytes": self.max_bytes,
            "max_consumers": self.max_consumers,
            "max_msgs": self.max_msgs,
            "num_replicas": self.num_replicas,
            "retention": self.retention,
            "storage": self.storage,
        }

        # Add optional fields only if not None
        if self.allow_direct is not None:
            result["allow_direct"] = self.allow_direct
        if self.allow_msg_ttl is not None:
            result["allow_msg_ttl"] = self.allow_msg_ttl
        if self.allow_rollup_hdrs is not None:
            result["allow_rollup_hdrs"] = self.allow_rollup_hdrs
        if self.compression is not None:
            result["compression"] = self.compression
        if self.deny_delete is not None:
            result["deny_delete"] = self.deny_delete
        if self.deny_purge is not None:
            result["deny_purge"] = self.deny_purge
        if self.description is not None:
            result["description"] = self.description
        if self.discard is not None:
            result["discard"] = self.discard
        if self.discard_new_per_subject is not None:
            result["discard_new_per_subject"] = self.discard_new_per_subject
        if self.duplicate_window is not None:
            result["duplicate_window"] = self.duplicate_window
        if self.first_seq is not None:
            result["first_seq"] = self.first_seq
        if self.max_msg_size is not None:
            result["max_msg_size"] = self.max_msg_size
        if self.max_msgs_per_subject is not None:
            result["max_msgs_per_subject"] = self.max_msgs_per_subject
        if self.metadata is not None:
            result["metadata"] = self.metadata
        if self.mirror is not None:
            result["mirror"] = self.mirror.to_request()
        if self.mirror_direct is not None:
            result["mirror_direct"] = self.mirror_direct
        if self.name is not None:
            result["name"] = self.name
        if self.no_ack is not None:
            result["no_ack"] = self.no_ack
        if self.placement is not None:
            result["placement"] = self.placement.to_request()
        if self.republish is not None:
            result["republish"] = self.republish.to_request()
        if self.sealed is not None:
            result["sealed"] = self.sealed
        if self.sources is not None:
            result["sources"] = [s.to_request() for s in self.sources]
        if self.subject_delete_marker_ttl is not None:
            result["subject_delete_marker_ttl"] = self.subject_delete_marker_ttl
        if self.subject_transform is not None:
            result["subject_transform"] = self.subject_transform.to_request()
        if self.subjects is not None:
            result["subjects"] = self.subjects
        if self.template_owner is not None:
            result["template_owner"] = self.template_owner
        if self.consumer_limits is not None:
            result["consumer_limits"] = self.consumer_limits.to_request()

        return result


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

    async def _upsert_consumer(
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

        # Delegate to _upsert_consumer with "create" action
        return await self._upsert_consumer(
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
        return await self._upsert_consumer(
            action=CONSUMER_ACTION_UPDATE, name=consumer_name, **config
        )
