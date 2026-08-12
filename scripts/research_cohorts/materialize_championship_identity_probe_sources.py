"""Turn championship-identity probe JSON payloads into team-week Parquet rows.

The probe artifacts have one root league-season identity and a nested
``matchup_rows`` list.  This adapter only exposes those source facts at the
same team-week grain used by the read-only receipt path; it does not connect to
or modify the canonical cache.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


FIELDS = ("week", "manager", "team_key", "team_name", "is_playoffs", "champion", "is_championship")


def materialize(*, selected_path: Path, sources_root: Path, out_manifest: Path) -> dict[str, int]:
    selected = json.loads(selected_path.read_text(encoding="utf-8"))
    if not isinstance(selected, list) or not selected:
        raise SystemExit("selected artifact manifest is empty")
    out_dir = out_manifest.parent / "championship_identity_probe_parquet"
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, str | int]] = []
    total_rows = 0
    for item in selected:
        artifact_id = int(item["artifact_id"])
        candidates = list((sources_root / str(artifact_id)).rglob("championship_identity_probe.json"))
        if len(candidates) != 1:
            raise SystemExit(f"{artifact_id}: expected exactly one championship_identity_probe.json, found {len(candidates)}")
        payload = json.loads(candidates[0].read_text(encoding="utf-8"))
        db_name = payload.get("db_name")
        year = payload.get("year")
        rows = payload.get("matchup_rows")
        if not isinstance(db_name, str) or not db_name or not isinstance(year, int) or not isinstance(rows, list):
            raise SystemExit(f"{artifact_id}: malformed championship identity payload")
        records = []
        for row in rows:
            if not isinstance(row, dict) or row.get("week") is None or row.get("manager") is None:
                continue
            records.append({
                "db_name": db_name,
                "year": year,
                **{field: row.get(field) for field in FIELDS},
            })
        if not records:
            raise SystemExit(f"{artifact_id}: payload has no usable matchup rows")
        out_path = out_dir / f"{artifact_id}.parquet"
        pq.write_table(pa.Table.from_pylist(records), out_path)
        manifest.append({"artifact_id": artifact_id, "path": str(out_path.resolve())})
        total_rows += len(records)
    out_manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return {"artifacts": len(manifest), "matchup_rows": total_rows}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--selected", type=Path, required=True)
    parser.add_argument("--sources-root", type=Path, required=True)
    parser.add_argument("--out-manifest", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(materialize(
        selected_path=args.selected,
        sources_root=args.sources_root,
        out_manifest=args.out_manifest,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
