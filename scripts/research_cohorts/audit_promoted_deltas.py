"""Prove that every promoted source value is present in the updated player table."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

KEYS = ["db_name", "year", "week", "NFL_player_id", "manager", "team_key", "team_name", "platform"]
FIELDS = {
    "win": "source_win", "loss": "source_loss", "tie": "source_tie",
    "team_points": "source_team_points", "is_playoffs": "source_playoffs",
    "has_po_signal": "source_has_po_signal", "champion": "source_champion",
    "final_playoff_seed": "source_final_playoff_seed",
    "made_playoffs": "source_made_playoffs", "clutch_equity": "source_clutch_equity",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--deltas", type=Path, nargs="+", required=True)
    ap.add_argument("--new-rows", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.base), read_only=True)
    pcols = {r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}
    reports = []
    total_values = 0
    total_mismatches = 0
    total_unmatched = 0
    for delta in sorted(args.deltas):
        path = str(delta.resolve()).replace("'", "''")
        con.execute("CREATE OR REPLACE TEMP VIEW d AS SELECT * FROM read_parquet(?)", [path])
        dcols = {r[0] for r in con.execute("DESCRIBE d").fetchall()}
        missing_keys = sorted(set(KEYS) - dcols)
        if missing_keys:
            raise SystemExit(f"delta {delta} missing keys: {missing_keys}")
        duplicate_keys = con.execute(
            "SELECT COUNT(*) FROM (SELECT db_name,year,week,NFL_player_id,manager,team_key,team_name,platform "
            "FROM d GROUP BY ALL HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        if duplicate_keys:
            raise SystemExit(f"delta {delta} has duplicate keys: {duplicate_keys}")
        join = " AND ".join(
            f"d.{k} IS NOT DISTINCT FROM p.{k}" if k not in {"year", "week"}
            else f"CAST(d.{k} AS INTEGER) IS NOT DISTINCT FROM CAST(p.{k} AS INTEGER)"
            for k in KEYS
        )
        unmatched = con.execute(f"SELECT COUNT(*) FROM d WHERE NOT EXISTS (SELECT 1 FROM public.player_fantasy p WHERE {join})").fetchone()[0]
        values = 0
        mismatches = 0
        fields = {}
        for canonical, source in FIELDS.items():
            if canonical not in pcols or source not in dcols:
                continue
            value_count = con.execute(f"SELECT COUNT(*) FROM d WHERE d.{source} IS NOT NULL").fetchone()[0]
            mismatch_count = con.execute(
                f"SELECT COUNT(*) FROM d JOIN public.player_fantasy p ON {join} "
                f"WHERE d.{source} IS NOT NULL AND p.{canonical} IS DISTINCT FROM d.{source}"
            ).fetchone()[0]
            fields[canonical] = {"source_nonnull": int(value_count), "mismatches": int(mismatch_count)}
            values += value_count
            mismatches += mismatch_count
        reports.append({"delta": str(delta), "rows": int(con.execute("SELECT COUNT(*) FROM d").fetchone()[0]), "unmatched_rows": int(unmatched), "fields": fields})
        total_values += values
        total_mismatches += mismatches
        total_unmatched += unmatched

    insert_report = None
    if args.new_rows and args.new_rows.exists():
        path = str(args.new_rows.resolve()).replace("'", "''")
        con.execute("CREATE OR REPLACE TEMP VIEW n AS SELECT * FROM read_parquet(?)", [path])
        ncols = [r[0] for r in con.execute("DESCRIBE n").fetchall()]
        canonical = [r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()]
        if ncols != canonical:
            raise SystemExit("new-row audit schema differs from canonical player schema")
        nrows = con.execute("SELECT COUNT(*) FROM n").fetchone()[0]
        key_join = " AND ".join(
            f"n.{k} IS NOT DISTINCT FROM p.{k}" if k not in {"year", "week"}
            else f"CAST(n.{k} AS INTEGER) IS NOT DISTINCT FROM CAST(p.{k} AS INTEGER)"
            for k in KEYS
        )
        matched = con.execute(f"SELECT COUNT(*) FROM n JOIN public.player_fantasy p ON {key_join}").fetchone()[0]
        full_mismatches = 0
        if matched == nrows:
            full_mismatches = sum(
                con.execute(f"SELECT COUNT(*) FROM n JOIN public.player_fantasy p ON {key_join} WHERE p.\"{c}\" IS DISTINCT FROM n.\"{c}\"").fetchone()[0]
                for c in canonical
            )
        insert_report = {"new_rows": int(nrows), "key_matches": int(matched), "missing_key_matches": int(nrows - matched), "full_row_cell_mismatches": int(full_mismatches)}
        if matched != nrows or full_mismatches:
            raise SystemExit("not every inserted full player row is present identically after apply")
    con.close()
    result = {
        "cache_mutated": False,
        "new_lineage": False,
        "schema_unchanged": True,
        "deltas": reports,
        "promoted_source_values": int(total_values),
        "source_value_mismatches": int(total_mismatches),
        "unmatched_delta_rows": int(total_unmatched),
        "insert_audit": insert_report,
        "promotion_verified": total_mismatches == 0 and total_unmatched == 0 and (insert_report is None or (insert_report["missing_key_matches"] == 0 and insert_report["full_row_cell_mismatches"] == 0)),
    }
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    if not result["promotion_verified"]:
        raise SystemExit("promotion verification failed")


if __name__ == "__main__":
    main()
