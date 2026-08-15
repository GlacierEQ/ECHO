from datetime import date

import pytest

from echo.frontier import FrontierContinuityEngine, FrontierEvent


def event(title: str, technology: str, published_at: str, *, source: str = "https://example.org/release") -> FrontierEvent:
    return FrontierEvent(
        title=title,
        technology=technology,
        domain="runtime",
        source_url=source,
        published_at=published_at,
        maturity="stable",
        summary="A primary-source capability change with a measurable runtime impact.",
        target_boundaries=("execution",),
    )


def test_packet_deduplicates_and_orders_newest_first():
    engine = FrontierContinuityEngine(maximum_age_days=45)
    older = event("Older", "Alpha", "2026-08-10T00:00:00Z", source="https://example.org/alpha")
    newer = event("Newer", "Beta", "2026-08-14T00:00:00Z", source="https://example.org/beta")
    packet = engine.build_packet([older, newer, newer], target_day=date(2026, 8, 15))
    assert [item.technology for item in packet.events] == ["Beta", "Alpha"]
    assert packet.duplicate_count == 1
    assert packet.stale_count == 0


def test_packet_rejects_non_primary_source():
    engine = FrontierContinuityEngine()
    invalid = FrontierEvent(
        title="Rumor",
        technology="Gamma",
        domain="runtime",
        source_url="https://example.org/rumor",
        published_at="2026-08-15T00:00:00Z",
        maturity="research",
        summary="Unverified secondary-source rumor.",
        primary_source=False,
    )
    with pytest.raises(ValueError, match="primary source"):
        engine.build_packet([invalid], target_day=date(2026, 8, 15))


def test_stale_signals_do_not_pollute_daily_packet():
    engine = FrontierContinuityEngine(maximum_age_days=7)
    stale = event("Ancient", "OldTech", "2026-07-01T00:00:00Z")
    packet = engine.build_packet([stale], target_day=date(2026, 8, 15))
    assert packet.events == ()
    assert packet.stale_count == 1


def test_unseen_preserves_frontier_work_across_sessions():
    engine = FrontierContinuityEngine()
    a = event("A", "Alpha", "2026-08-15T00:00:00Z", source="https://example.org/a")
    b = event("B", "Beta", "2026-08-15T01:00:00Z", source="https://example.org/b")
    packet = engine.build_packet([a, b], target_day=date(2026, 8, 15))
    remaining = engine.unseen(packet, [a.event_id])
    assert [item.event_id for item in remaining] == [b.event_id]


def test_innovation_payload_requires_state_change_or_measured_reason():
    engine = FrontierContinuityEngine()
    item = event("A", "Alpha", "2026-08-15T00:00:00Z")
    packet = engine.build_packet([item], target_day=date(2026, 8, 15))
    payload = engine.innovation_payload(packet)
    assert payload["schema"] == "glaciereq.echo.frontier-to-innovation.v1"
    assert "implement" in payload["required_outcome"]
    assert "experiment" in payload["required_outcome"]
