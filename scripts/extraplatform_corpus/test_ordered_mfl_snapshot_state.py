from __future__ import annotations

from ordered_mfl_snapshot_state import PublisherStateError, require_current_parent, reserve_next_rows


def _row(position: int, season: int, league_id: str, *, ordinal: int | None = None) -> dict[str, object]:
    return {
        "stage": 1,
        "target_per_year": 2000,
        "year_order": season,
        "year_ordinal": position + 1 if ordinal is None else ordinal,
        "season": season,
        "league_id": league_id,
        "source": "registry_minus_authoritative_39k_index",
    }


def test_reservation_keeps_manifest_order_and_marks_existing_identity() -> None:
    manifest = [_row(0, 2000, "00002"), _row(1, 2000, "00005"), _row(2, 2001, "00009", ordinal=1)]

    result = reserve_next_rows(
        manifest,
        existing_identities={(2000, "00005")},
        reserved_positions=set(),
        limit=2,
    )

    assert result["reserved"] == [
        {"manifest_position": 0, "season": 2000, "league_id": "00002", "status": "reserved"},
        {"manifest_position": 1, "season": 2000, "league_id": "00005", "status": "already_present"},
    ]
    assert result["next_position"] == 2


def test_reservation_rejects_a_duplicate_canonical_identity_in_manifest() -> None:
    manifest = [_row(0, 2000, "2"), _row(1, 2000, "00002")]

    try:
        reserve_next_rows(manifest, existing_identities=set(), reserved_positions=set(), limit=2)
    except PublisherStateError as exc:
        assert "duplicate canonical identity" in str(exc)
    else:
        raise AssertionError("duplicate manifest identity was accepted")


def test_reservation_rejects_a_gap_before_the_requested_window() -> None:
    manifest = [_row(0, 2000, "00002"), _row(2, 2000, "00005")]

    try:
        reserve_next_rows(manifest, existing_identities=set(), reserved_positions=set(), limit=2)
    except PublisherStateError as exc:
        assert "year_ordinal" in str(exc)
    else:
        raise AssertionError("out-of-order manifest was accepted")


def test_publisher_rejects_a_stale_parent_snapshot() -> None:
    try:
        require_current_parent(expected_parent_snapshot_id="snapshot-a", current_snapshot_id="snapshot-b")
    except PublisherStateError as exc:
        assert "stale publisher" in str(exc)
    else:
        raise AssertionError("stale publisher was allowed to overwrite the current snapshot")
