from __future__ import annotations

import json
import logging
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeGuard,
    Unpack,
    cast,
    get_args,
    get_origin,
    overload,
)

from .types import (
    AccountInfoResponse_AccountInfo,
    ConsumerCreateRequest,
    ConsumerCreateResponse_ConsumerInfo,
    ConsumerDeleteResponse_Variant1,
    ConsumerInfoResponse_ConsumerInfo,
    ConsumerListResponse,
    ConsumerNamesResponse,
    Error as ApiError,
    ErrorResponse,
    StreamCreateRequest,
    StreamCreateResponse_StreamInfo,
    StreamDeleteResponse_Variant1,
    StreamInfoRequest,
    StreamInfoResponse_StreamInfo,
    StreamListResponse,
    StreamMsgDeleteRequest,
    StreamMsgDeleteResponse_Variant1,
    StreamMsgGetRequest,
    StreamMsgGetResponse_Variant1,
    StreamNamesRequest,
    StreamNamesResponse,
    StreamPurgeRequest,
    StreamPurgeResponse_Variant1,
    StreamUpdateRequest,
    StreamUpdateResponse_StreamInfo,
)

if TYPE_CHECKING:
    from nats.client import Client as NatsClient

logger = logging.getLogger("nats.jetstream.api")


class Error(Exception):

    def __init__(
        self,
        message: str,
        code: int | None = None,
        description: str | None = None
    ):
        super().__init__(message)
        self.code = code
        self.description = description

    @classmethod
    def from_response(cls, error: ApiError) -> Error:
        return cls(
            message=error.get("description", "Unknown error"),
            code=error.get("code"),
            description=error.get("description"),
        )


def is_error_response(data: Any) -> TypeGuard[ErrorResponse]:
    return isinstance(data, dict) and "error" in data


def check_response[ResponseT](
    data: Any, expected_type: type[ResponseT]
) -> tuple[bool, set[str] | None, set[str] | None]:
    if not isinstance(data, dict):
        return False, None, None

    # Get expected keys from TypedDict annotations
    expected_keys = set(expected_type.__annotations__.keys())
    actual_keys = set(data.keys())

    # Find missing required keys and unknown keys
    missing_keys = expected_keys - actual_keys
    unknown_keys = actual_keys - expected_keys

    # Filter out optional keys from missing_keys
    # TypedDict stores required/optional info in __required_keys__ (Python 3.9+)
    if hasattr(expected_type, '__required_keys__'):
        required_keys = expected_type.__required_keys__
        missing_keys = missing_keys & required_keys

    is_valid = len(missing_keys) == 0

    return is_valid, unknown_keys if unknown_keys else None, missing_keys if missing_keys else None


class Client:

    def __init__(self, client: NatsClient, prefix: str = "$JS.API") -> None:
        self._client = client
        self._prefix = prefix

    async def account_info(self) -> AccountInfoResponse_AccountInfo:
        return await self.request_json(
            f"{self._prefix}.INFO",
            response_type=AccountInfoResponse_AccountInfo,
        )

    async def consumer_create(
        self, **request: Unpack[ConsumerCreateRequest]
    ) -> ConsumerCreateResponse_ConsumerInfo:
        stream_name = request.get("stream_name")
        if not stream_name:
            raise ValueError("stream_name is required")

        consumer_config = request.get("config")
        if not consumer_config:
            raise ValueError("config is required")

        consumer_name = consumer_config.get("name")
        if not consumer_name:
            raise ValueError("name is required")

        return await self.request_json(
            f"{self._prefix}.CONSUMER.CREATE.{stream_name}.{consumer_name}",
            request,
            response_type=ConsumerCreateResponse_ConsumerInfo,
        )

    async def consumer_delete(
        self, stream_name: str, consumer_name: str
    ) -> ConsumerDeleteResponse_Variant1:
        return await self.request_json(
            f"{self._prefix}.CONSUMER.DELETE.{stream_name}.{consumer_name}",
            response_type=ConsumerDeleteResponse_Variant1,
        )

    async def consumer_info(
        self, stream_name: str, consumer_name: str
    ) -> ConsumerInfoResponse_ConsumerInfo:
        return await self.request_json(
            f"{self._prefix}.CONSUMER.INFO.{stream_name}.{consumer_name}",
            response_type=ConsumerInfoResponse_ConsumerInfo,
        )

    async def consumer_list(self, stream_name: str, offset: int | None = None) -> ConsumerListResponse:
        """Get information about all consumers in a stream."""
        request = {}
        if offset is not None:
            request["offset"] = offset
        return await self.request_json(
            f"{self._prefix}.CONSUMER.LIST.{stream_name}",
            request if request else None,
            response_type=ConsumerListResponse,
        )

    async def consumer_names(self, stream_name: str, offset: int | None = None) -> ConsumerNamesResponse:
        """Get a list of all consumer names in a stream."""
        request = {}
        if offset is not None:
            request["offset"] = offset

        return await self.request_json(
            f"{self._prefix}.CONSUMER.NAMES.{stream_name}",
            request if request else None,
            response_type=ConsumerNamesResponse,
        )

    async def stream_create(
        self, name: str, **kwargs: Unpack[StreamCreateRequest]
    ) -> StreamCreateResponse_StreamInfo:
        # Validate max_msgs
        max_msgs = kwargs.get("max_msgs", -1)
        if max_msgs < -1:
            raise ValueError(
                "max_msgs must be -1 (unlimited) or a positive number"
            )

        # Validate mirror configuration
        mirror = kwargs.get("mirror")
        subjects = kwargs.get("subjects")
        if mirror is not None and subjects:
            raise ValueError("Cannot specify both mirror and subjects")

        return await self.request_json(
            f"{self._prefix}.STREAM.CREATE.{name}",
            {
                "name": name,
                **kwargs,
            },
            response_type=StreamCreateResponse_StreamInfo,
        )

    async def stream_delete(self, name: str) -> StreamDeleteResponse_Variant1:
        return await self.request_json(
            f"{self._prefix}.STREAM.DELETE.{name}",
            response_type=StreamDeleteResponse_Variant1,
        )

    async def stream_info(
        self, name: str, **request: Unpack[StreamInfoRequest]
    ) -> StreamInfoResponse_StreamInfo:
        return await self.request_json(
            f"{self._prefix}.STREAM.INFO.{name}",
            request if request else None,
            response_type=StreamInfoResponse_StreamInfo,
        )

    async def stream_list(
        self,
        offset: int | None = None,
        subject: str | None = None
    ) -> StreamListResponse:
        """Get information about all streams.

        Args:
            offset: Optional offset for pagination
            subject: Optional subject filter

        Returns:
            Response containing stream information and pagination info
        """
        request = {}
        if offset is not None:
            request["offset"] = offset
        if subject is not None:
            request["subject"] = subject

        return await self.request_json(
            f"{self._prefix}.STREAM.LIST",
            request if request else None,
            response_type=StreamListResponse,
        )

    async def stream_msg_delete(
        self, name: str, **request: Unpack[StreamMsgDeleteRequest]
    ) -> StreamMsgDeleteResponse_Variant1:
        return await self.request_json(
            f"{self._prefix}.STREAM.MSG.DELETE.{name}",
            request if request else None,
            response_type=StreamMsgDeleteResponse_Variant1,
        )

    async def stream_msg_get(
        self, name: str, **request: Unpack[StreamMsgGetRequest]
    ) -> StreamMsgGetResponse_Variant1:
        return await self.request_json(
            f"{self._prefix}.STREAM.MSG.GET.{name}",
            request if request else None,
            response_type=StreamMsgGetResponse_Variant1,
        )

    async def stream_names(
        self, **kwargs: Unpack[StreamNamesRequest]
    ) -> StreamNamesResponse:
        """Get a list of all stream names.

        Args:
            offset: Optional offset for pagination
            subject: Optional subject filter

        Returns:
            Response containing stream names and pagination info
        """
        return await self.request_json(
            f"{self._prefix}.STREAM.NAMES",
            kwargs,
            response_type=StreamNamesResponse,
        )

    async def stream_purge(
        self, name: str, **request: Unpack[StreamPurgeRequest]
    ) -> StreamPurgeResponse_Variant1:
        return await self.request_json(
            f"{self._prefix}.STREAM.PURGE.{name}",
            request if request else None,
            response_type=StreamPurgeResponse_Variant1,
        )

    async def stream_update(
        self, name: str, **config: Unpack[StreamUpdateRequest]
    ) -> StreamUpdateResponse_StreamInfo:
        return await self.request_json(
            f"{self._prefix}.STREAM.UPDATE.{name}",
            {
                "name": name,
                **config,
            },
            response_type=StreamUpdateResponse_StreamInfo,
        )

    @overload
    async def request_json[ResponseT](
        self,
        subject: str,
        payload: Any | None = None,
        *,
        response_type: type[ResponseT],
        raise_on_error: Literal[True] = True,
    ) -> ResponseT:
        ...

    @overload
    async def request_json[ResponseT](
        self,
        subject: str,
        payload: Any | None = None,
        *,
        response_type: type[ResponseT],
        raise_on_error: Literal[False],
    ) -> ResponseT | ErrorResponse:
        ...

    async def request_json[ResponseT](
        self,
        subject: str,
        payload: Any | None = None,
        *,
        response_type: type[ResponseT],
        raise_on_error: bool = True,
        timeout: float = 5.0,
    ) -> ResponseT | ErrorResponse:
        request_id = str(uuid.uuid4())[:8]
        encoded_payload = json.dumps(payload
                                     ).encode() if payload is not None else b""
        logger.debug(
            "[%s] request: %s %r", request_id, subject, encoded_payload
        )
        response = await self._client.request(
            subject, encoded_payload, timeout=timeout
        )

        try:
            data = json.loads(response.data.decode())
            logger.debug("[%s] response: %r", request_id, data)

            if raise_on_error and is_error_response(data):
                raise Error.from_response(data["error"])

            is_valid, unknown_keys, missing_keys = check_response(
                data, response_type
            )
            if not is_valid:
                if missing_keys:
                    logger.warning(
                        "[%s] Missing keys in response: %s", request_id,
                        missing_keys
                    )
                if unknown_keys:
                    logger.warning(
                        "[%s] Unknown keys in response: %s", request_id,
                        unknown_keys
                    )
                if not missing_keys and not unknown_keys:
                    logger.warning(
                        "[%s] Expected %s, got %s", request_id,
                        response_type.__name__,
                        type(data).__name__
                    )

            return cast(ResponseT, data)
        except json.JSONDecodeError as e:
            logger.exception("[%s] failed to decode response", request_id)
            raise Error("failed to decode response") from e
