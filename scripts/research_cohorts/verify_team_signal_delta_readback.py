"""Independently verify only the cache cells authorized by a team-signal delta."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb


KEY_JOIN = """
  d.db_name=p.db_name
  AND CAST(d."year" AS INTEGER)=CAST(p."year" AS INTEGER)
  AND CAST(d."week" AS INTEGER)=CAST(p."week" AS INTEGER)
  AND d.NFL_player_id IS NOT DISTINCT FROM p.NFL_player_id
  AND d.platform IS NOT DISTINCT FROM p.platform
  AND d.manager IS NOT DISTINCT FROM p.manager
  AND (p.team_key IS NULL OR d.team_key=p.team_key)
  AND (p.team_name IS NULL OR d.team_name=p.team_name)
"""

FIELDS = {
    "win": "source_win",
    "loss": "source_loss",
    "tie": "source_tie",
    "team_points": "source_team_points",
    "is_playoffs": "source_playoffs",
    "champion": "source_champion",
    "final_playoff_seed": "source_final_playoff_seed",
    "made_playoffs": "source_made_playoffs",
    "has_po_signal": "source_has_po_signal",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify(
    *,
    base: Path,
    delta: Path,
    before: Path,
    ops: Path,
    ops_before: Path,
    apply_report: Path | None = None,
) -> dict[str, object]:
    prestate = json.loads(before.read_text(encoding="utf-8"))
    con = duckdb.connect(str(base), read_only=True)
    try:
        dpath = str(delta.resolve()).replace("'", "''")
        con.execute(f"CREATE TEMP VIEW d AS SELECT DISTINCT * FROM read_parquet('{dpath}')")
        dcols = {row[0] for row in con.execute("DESCRIBE d").fetchall()}
        pcols = {row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()}
        missing_source = [
            field for field, source in FIELDS.items()
            if field not in pcols or source not in dcols
        ]
        if missing_source:
            raise ValueError(f"readback delta lacks source fields: {missing_source}")
        exact_provenance = all(f"canonical_{field}" in dcols for field in FIELDS)
        if not exact_provenance and apply_report is None:
            raise ValueError("readback delta lacks canonical prestate; apply receipt is required")
        apply_receipt = (
            json.loads(apply_report.read_text(encoding="utf-8"))
            if apply_report is not None else {}
        )
        total = int(con.execute("SELECT COUNT(*) FROM d").fetchone()[0])
        unmatched = int(con.execute(
            f"SELECT COUNT(*) FROM d WHERE NOT EXISTS (SELECT 1 FROM public.player_fantasy p WHERE {KEY_JOIN})"
        ).fetchone()[0])
        authorized: dict[str, int] = {}
        disagreements: dict[str, int] = {}
        for field, source in FIELDS.items():
            if exact_provenance:
                canonical = f"canonical_{field}"
                condition = f"d.{canonical} IS NULL AND d.{source} IS NOT NULL"
                if field == "win":
                    condition = (
                        f"({condition}) OR "
                        f"(CAST(d.{canonical} AS VARCHAR)='0' AND CAST(d.{source} AS VARCHAR)='1')"
                    )
                count = int(con.execute(f"SELECT COUNT(*) FROM d WHERE {condition}").fetchone()[0])
                if count:
                    authorized[field] = count
                disagreements[field] = int(con.execute(
                    f"SELECT COUNT(*) FROM public.player_fantasy p JOIN d ON {KEY_JOIN} "
                    f"WHERE ({condition}) AND p.{field} IS DISTINCT FROM d.{source}"
                ).fetchone()[0])
            else:
                expected = int(apply_receipt.get("improvements_by_field", {}).get(field, 0))
                expected += int(apply_receipt.get("replacements_by_field", {}).get(field, 0))
                if expected:
                    authorized[field] = expected
                matches = int(con.execute(
                    f"SELECT COUNT(*) FROM public.player_fantasy p JOIN d ON {KEY_JOIN} "
                    f"WHERE d.{source} IS NOT NULL AND p.{field} IS NOT DISTINCT FROM d.{source}"
                ).fetchone()[0])
                disagreements[field] = int(con.execute(
                    f"SELECT COUNT(*) FROM public.player_fantasy p JOIN d ON {KEY_JOIN} "
                    f"WHERE d.{source} IS NOT NULL AND p.{field} IS DISTINCT FROM d.{source}"
                ).fetchone()[0])
                if matches < expected:
                    raise ValueError(
                        f"saved apply receipt cannot be supported for {field}: "
                        f"expected_at_least={expected}, matching_current_cells={matches}"
                    )
        schema = con.execute("DESCRIBE public.player_fantasy").fetchall()
        player_rows = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0])
    finally:
        con.close()
    result = {
        "candidate_rows": total,
        "unmatched_candidate_keys": unmatched,
        "authorized_cells_by_field": authorized,
        "source_value_disagreements": disagreements,
        "readback_proof_mode": (
            "exact_authorized_cell_readback"
            if exact_provenance else "legacy_apply_receipt_minimum_match"
        ),
        "schema_unchanged": schema == [tuple(row) for row in prestate["schema"]],
        "player_rows_unchanged": player_rows == prestate["player_rows"],
        "ops_unchanged": _sha256(ops) == ops_before.read_text(encoding="utf-8").split()[0],
        "fresh_restore": True,
        "new_lineage": False,
    }
    if not (
        total > 0
        and unmatched == 0
        and bool(authorized)
        and (not any(disagreements.values()) or not exact_provenance)
        and result["schema_unchanged"]
        and result["player_rows_unchanged"]
        and result["ops_unchanged"]
    ):
        raise ValueError(json.dumps(result, sort_keys=True))
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True, type=Path)
    parser.add_argument("--delta", required=True, type=Path)
    parser.add_argument("--before", required=True, type=Path)
    parser.add_argument("--ops", required=True, type=Path)
    parser.add_argument("--ops-before", required=True, type=Path)
    parser.add_argument("--apply-report", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    try:
        result = verify(
            base=args.base,
            delta=args.delta,
            before=args.before,
            ops=args.ops,
            ops_before=args.ops_before,
            apply_report=args.apply_report,
        )
    except (OSError, ValueError, duckdb.Error) as exc:
        raise SystemExit(str(exc)) from exc
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
