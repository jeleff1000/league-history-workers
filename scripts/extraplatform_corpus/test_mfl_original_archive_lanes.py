from __future__ import annotations

from mfl_original_archive_lanes import build_lane_plan


def _artifact(artifact_id: int, size: int, run_id: int = 1) -> dict[str, object]:
    return {
        "artifact_id": artifact_id,
        "run_id": run_id,
        "name": f"mfl-register-batch-{artifact_id}",
        "expired": False,
        "size_in_bytes": size,
        "digest": f"sha256:{artifact_id:064x}"[-71:],
    }


def test_build_lane_plan_assigns_each_available_artifact_once_with_balanced_bytes() -> None:
    inventory = {
        "run_count": 177,
        "runs": [
            {"run_id": 1, "artifacts": [_artifact(1, 11), _artifact(2, 10), _artifact(3, 9)]},
            {"run_id": 2, "artifacts": [_artifact(4, 8, run_id=2), _artifact(5, 7, run_id=2), _artifact(6, 6, run_id=2)]},
        ],
    }

    plan = build_lane_plan(inventory, lane_count=3)

    assert plan["artifact_count"] == 6
    assert plan["total_bytes"] == 51
    assert [lane["bytes"] for lane in plan["lanes"]] == [17, 17, 17]
    assert sorted(artifact["artifact_id"] for lane in plan["lanes"] for artifact in lane["artifacts"]) == [1, 2, 3, 4, 5, 6]


def test_build_lane_plan_rejects_an_expired_source_artifact() -> None:
    inventory = {
        "run_count": 177,
        "runs": [{"run_id": 1, "artifacts": [{**_artifact(1, 1), "expired": True}]}],
    }

    try:
        build_lane_plan(inventory, lane_count=1)
    except ValueError as exc:
        assert "expired" in str(exc)
    else:
        raise AssertionError("expired source artifact entered a recovery lane")


def test_build_lane_plan_rejects_a_report_with_an_incorrect_artifact_total() -> None:
    inventory = {
        "run_count": 177,
        "artifact_count": 2,
        "runs": [{"run_id": 1, "artifacts": [_artifact(1, 1)]}],
    }

    try:
        build_lane_plan(inventory, lane_count=1)
    except ValueError as exc:
        assert "artifact count" in str(exc)
    else:
        raise AssertionError("a truncated inventory entered a recovery lane")
