from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from hashlib import sha256
from typing import Iterable, Mapping
from urllib.parse import urlsplit, urlunsplit


_ALLOWED_MATURITY = {
    "security",
    "stable",
    "release_candidate",
    "preview",
    "research",
}


def _normalize_url(url: str) -> str:
    parts = urlsplit(url.strip())
    if parts.scheme != "https" or not parts.netloc:
        raise ValueError("source_url must be an absolute https URL")
    return urlunsplit((parts.scheme, parts.netloc.lower(), parts.path.rstrip("/"), parts.query, ""))


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class FrontierEvent:
    title: str
    technology: str
    domain: str
    source_url: str
    published_at: str
    maturity: str
    summary: str
    target_boundaries: tuple[str, ...] = ()
    primary_source: bool = True
    metadata: Mapping[str, object] = field(default_factory=dict)

    def validate(self) -> None:
        for name in ("title", "technology", "domain", "summary"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} must not be empty")
        _normalize_url(self.source_url)
        _parse_time(self.published_at)
        if self.maturity not in _ALLOWED_MATURITY:
            raise ValueError(f"unsupported maturity: {self.maturity}")
        if not self.primary_source:
            raise ValueError("frontier events must be grounded in a primary source")

    @property
    def event_id(self) -> str:
        self.validate()
        identity = "\n".join(
            (
                self.technology.casefold().strip(),
                self.title.casefold().strip(),
                _normalize_url(self.source_url),
            )
        )
        return sha256(identity.encode("utf-8")).hexdigest()[:24]

    def as_dict(self) -> Mapping[str, object]:
        return {
            "event_id": self.event_id,
            "title": self.title,
            "technology": self.technology,
            "domain": self.domain,
            "source_url": _normalize_url(self.source_url),
            "published_at": _parse_time(self.published_at).isoformat(),
            "maturity": self.maturity,
            "summary": self.summary,
            "target_boundaries": list(self.target_boundaries),
            "primary_source": self.primary_source,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class FrontierPacket:
    target_day: str
    events: tuple[FrontierEvent, ...]
    duplicate_count: int
    stale_count: int
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def technologies(self) -> tuple[str, ...]:
        return tuple(sorted({event.technology for event in self.events}, key=str.casefold))

    @property
    def domains(self) -> tuple[str, ...]:
        return tuple(sorted({event.domain for event in self.events}, key=str.casefold))

    def as_dict(self) -> Mapping[str, object]:
        return {
            "schema": "glaciereq.echo.frontier-packet.v1",
            "target_day": self.target_day,
            "generated_at": self.generated_at,
            "event_count": len(self.events),
            "duplicate_count": self.duplicate_count,
            "stale_count": self.stale_count,
            "technologies": list(self.technologies),
            "domains": list(self.domains),
            "events": [event.as_dict() for event in self.events],
        }


class FrontierContinuityEngine:
    """Turn primary-source technology changes into deterministic daily packets.

    ECHO does not decide architecture here.  It preserves, deduplicates, orders,
    and carries frontier evidence so AKOS and Tower can make an informed move
    without rediscovering or silently dropping the signal on the next session.
    """

    def __init__(self, *, maximum_age_days: int = 45) -> None:
        if maximum_age_days < 0:
            raise ValueError("maximum_age_days must be non-negative")
        self.maximum_age_days = maximum_age_days

    def build_packet(
        self,
        events: Iterable[FrontierEvent],
        *,
        target_day: date,
    ) -> FrontierPacket:
        target_midnight = datetime(
            target_day.year,
            target_day.month,
            target_day.day,
            tzinfo=timezone.utc,
        )
        seen: dict[str, FrontierEvent] = {}
        duplicates = 0
        stale = 0
        for event in events:
            event.validate()
            age_days = (target_midnight - _parse_time(event.published_at)).total_seconds() / 86400.0
            if age_days > self.maximum_age_days:
                stale += 1
                continue
            event_id = event.event_id
            if event_id in seen:
                duplicates += 1
                continue
            seen[event_id] = event

        ordered = tuple(
            sorted(
                seen.values(),
                key=lambda event: (
                    -_parse_time(event.published_at).timestamp(),
                    event.domain.casefold(),
                    event.technology.casefold(),
                    event.event_id,
                ),
            )
        )
        return FrontierPacket(
            target_day=target_day.isoformat(),
            events=ordered,
            duplicate_count=duplicates,
            stale_count=stale,
        )

    @staticmethod
    def unseen(packet: FrontierPacket, processed_event_ids: Iterable[str]) -> tuple[FrontierEvent, ...]:
        processed = {item.strip() for item in processed_event_ids if item.strip()}
        return tuple(event for event in packet.events if event.event_id not in processed)

    @staticmethod
    def innovation_payload(packet: FrontierPacket) -> Mapping[str, object]:
        """Produce a portable payload for AKOS/Tower evaluation."""
        return {
            "schema": "glaciereq.echo.frontier-to-innovation.v1",
            "target_day": packet.target_day,
            "events": [event.as_dict() for event in packet.events],
            "required_outcome": ["implement", "experiment", "hold_with_reason", "reject_with_reason"],
        }
