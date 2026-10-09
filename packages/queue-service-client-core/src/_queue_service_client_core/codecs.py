"""Opt-in typed payload codecs that preserve opaque JSON wire values."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar

from _queue_service_client_core.errors import QueueClientError
from _queue_service_client_core.models import Task

__all__ = [
    "PayloadDecodeError",
    "PayloadDecoder",
    "PayloadEncoder",
    "TypedClaimView",
    "TypedTaskView",
    "decode_payload",
    "encode_payload",
    "measure_json_bytes",
]

T = TypeVar("T")
T_contra = TypeVar("T_contra", contravariant=True)
T_co = TypeVar("T_co", covariant=True)


class PayloadEncoder(Protocol[T_contra]):
    """Encode an application value into a JSON-serializable wire payload."""

    @property
    def codec_name(self) -> str: ...

    def encode(self, value: T_contra) -> Any: ...


class PayloadDecoder(Protocol[T_co]):
    """Decode an opaque JSON wire payload into an application value."""

    @property
    def codec_name(self) -> str: ...

    @property
    def value_type(self) -> str: ...

    def decode(self, raw: Any) -> T_co: ...


class PayloadDecodeError(QueueClientError):
    """Bounded decode failure: codec/type metadata only; raw kept for recovery."""

    def __init__(
        self,
        *,
        codec_name: str,
        value_type: str,
        raw: Any,
        reason: str = "decode_failed",
    ) -> None:
        self.codec_name = codec_name
        self.value_type = value_type
        self._raw = raw
        self.reason = reason
        super().__init__(reason)

    @property
    def raw(self) -> Any:
        """Explicit recovery access to the opaque wire value (never logged here)."""

        return self._raw

    def __str__(self) -> str:
        return (
            f"PayloadDecodeError(codec_name={self.codec_name!r}, "
            f"value_type={self.value_type!r}, reason={self.reason!r})"
        )

    def __repr__(self) -> str:
        return self.__str__()


def measure_json_bytes(value: Any) -> int:
    """Byte size of a JSON value using the same compact encoding as admission."""

    return len(
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
            sort_keys=True,
        ).encode("utf-8")
    )


def encode_payload(
    value: Any,
    encoder: PayloadEncoder[Any] | None = None,
    *,
    max_bytes: int | None = None,
) -> Any:
    """Encode (optional) then enforce payload size before the HTTP request body.

    Default ``encoder is None`` keeps the application value as the opaque wire
    JSON payload unchanged.
    """

    wire = value if encoder is None else encoder.encode(value)
    if max_bytes is not None:
        if isinstance(max_bytes, bool) or type(max_bytes) is not int or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        try:
            size = measure_json_bytes(wire)
        except (TypeError, ValueError) as exc:
            raise ValueError("payload must be a JSON value") from exc
        if size > max_bytes:
            raise ValueError(
                f"payload exceeds max_bytes ({size} > {max_bytes})"
            )
    return wire


def decode_payload(
    raw: Any,
    decoder: PayloadDecoder[T] | None = None,
) -> Any:
    """Decode an opaque wire payload; default returns the raw JSON value."""

    if decoder is None:
        return raw
    try:
        return decoder.decode(raw)
    except PayloadDecodeError:
        raise
    except Exception as exc:
        raise PayloadDecodeError(
            codec_name=decoder.codec_name,
            value_type=decoder.value_type,
            raw=raw,
            reason=type(exc).__name__,
        ) from exc


@dataclass(frozen=True, slots=True)
class TypedTaskView(Generic[T]):
    """Task plus decoded application payload; ``raw_payload`` is the wire value."""

    task: Task
    payload: T
    raw_payload: Any

    @classmethod
    def from_task(cls, task: Task, decoder: PayloadDecoder[T]) -> TypedTaskView[T]:
        raw = task.payload
        return cls(task=task, payload=decode_payload(raw, decoder), raw_payload=raw)


@dataclass(frozen=True, slots=True)
class TypedClaimView(Generic[T]):
    """Claim handle plus decoded application payload; wire value preserved."""

    claim: Any
    payload: T
    raw_payload: Any

    @classmethod
    def from_claim(
        cls,
        claim: Any,
        decoder: PayloadDecoder[T] | None = None,
    ) -> TypedClaimView[T]:
        raw = claim.task.payload
        active = decoder
        if active is None:
            active = getattr(claim, "payload_decoder", None)
        if active is None:
            raise ValueError("payload_decoder is required when claim has none")
        return cls(
            claim=claim,
            payload=decode_payload(raw, active),
            raw_payload=raw,
        )
