"""Combine strict loss/tie-only candidates without choosing conflicting values."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

try:
    from .verify_exact_team_null_candidate_contract import KEY_COLUMNS, verify
except ImportError:
    from verify_exact_team_null_candidate_contract import KEY_COLUMNS, verify


def _one(root: Path) -> Path:
    files = list(root.rglob("team_promotable_player_delta.parquet"))
    if len(files) != 1:
        raise SystemExit(f"{root}: expected exactly one candidate delta, found {len(files)}")
    return files[0]


def combine(roots: list[Path], out_delta: Path, out_receipt: Path) -> dict[str, int]:
    if len(roots) < 2:
        raise SystemExit("at least two candidate roots are required")
    contracts = [verify(root) for root in roots]
    con = duckdb.connect()
    try:
        selects = []
        for root in roots:
            path = str(_one(root).resolve()).replace("'", "''")
            selects.append(
                "SELECT " + ", ".join(KEY_COLUMNS) + ", source_loss, source_tie "
                f"FROM read_parquet('{path}')"
            )
        con.execute("CREATE TEMP VIEW all_candidates AS " + " UNION ALL ".join(selects))
        key = ", ".join('"' + column + '"' for column in KEY_COLUMNS)
        conflicts = int(con.execute(f"""
            SELECT COUNT(*) FROM (
              SELECT {key} FROM all_candidates GROUP BY {key} HAVING
                COUNT(DISTINCT COALESCE(CAST(source_loss AS VARCHAR), '<NULL>')) > 1
                OR COUNT(DISTINCT COALESCE(CAST(source_tie AS VARCHAR), '<NULL>')) > 1
            )
        """).fetchone()[0])
        if conflicts:
            raise SystemExit(f"conflicting duplicate source values: {conflicts}")
        raw_rows = int(con.execute("SELECT COUNT(*) FROM all_candidates").fetchone()[0])
        out_path = str(out_delta.resolve()).replace("'", "''")
        con.execute(f"""
            COPY (
              SELECT {key},
                     CAST(NULL AS INTEGER) AS source_win,
                     MAX(source_loss) AS source_loss,
                     MAX(source_tie) AS source_tie,
                     CAST(NULL AS DOUBLE) AS source_team_points,
                     CAST(NULL AS INTEGER) AS source_playoffs,
                     CAST(NULL AS INTEGER) AS source_champion,
                     CAST(NULL AS INTEGER) AS source_final_playoff_seed
              FROM all_candidates GROUP BY {key}
            ) TO '{out_path}' (FORMAT PARQUET)
        """)
        unique_rows = int(con.execute(f"SELECT COUNT(*) FROM (SELECT {key} FROM all_candidates GROUP BY {key})").fetchone()[0])
    finally:
        con.close()
    result = {
        "candidate_bundles": len(roots),
        "raw_rows": raw_rows,
        "unique_rows": unique_rows,
        "duplicates_collapsed": raw_rows - unique_rows,
        "conflicting_duplicate_keys": conflicts,
    }
    out_receipt.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-root", type=Path, action="append", required=True)
    parser.add_argument("--out-delta", type=Path, required=True)
    parser.add_argument("--out-receipt", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(combine(args.candidate_root, args.out_delta, args.out_receipt), sort_keys=True))


if __name__ == "__main__":
    main()
