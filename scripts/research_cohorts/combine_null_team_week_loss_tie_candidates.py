"""Build a loss/tie-only delta for audited null-NFL-ID player recipients.

The source outcome is at team-week grain.  For canonical player rows that
lack an NFL_player_id, the safe recipient key is the already-audited manager
team-week, not a fabricated null player identity.  The output remains the
existing delta schema and is consumed by the ordinary in-place updater.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

try:
    from .combine_exact_team_null_candidates import _one
    from .verify_exact_team_null_candidate_contract import verify
except ImportError:
    from combine_exact_team_null_candidates import _one
    from verify_exact_team_null_candidate_contract import verify


TEAM_WEEK_COLUMNS = (
    "db_name", "year", "week", "manager", "team_key", "team_name", "platform",
)


def combine(roots: list[Path], out_delta: Path, out_receipt: Path) -> dict[str, int]:
    if not roots:
        raise SystemExit("at least one candidate root is required")
    for root in roots:
        verify(root)
    con = duckdb.connect()
    try:
        selects = []
        for root in roots:
            path = str(_one(root).resolve()).replace("'", "''")
            selects.append(
                "SELECT db_name, year, week, manager, team_key, team_name, platform, "
                "source_loss, source_tie FROM read_parquet(" + repr(path) + ") "
                "WHERE NFL_player_id IS NULL"
            )
        con.execute("CREATE TEMP VIEW candidates AS " + " UNION ALL ".join(selects))
        raw_rows = int(con.execute("SELECT COUNT(*) FROM candidates").fetchone()[0])
        key = ", ".join('"' + column + '"' for column in TEAM_WEEK_COLUMNS)
        conflicts = int(con.execute(f"""
            SELECT COUNT(*) FROM (
              SELECT {key} FROM candidates GROUP BY {key} HAVING
                COUNT(DISTINCT COALESCE(CAST(source_loss AS VARCHAR), '<NULL>')) > 1
                OR COUNT(DISTINCT COALESCE(CAST(source_tie AS VARCHAR), '<NULL>')) > 1
            )
        """).fetchone()[0])
        if conflicts:
            raise SystemExit(f"conflicting null-player team-week values: {conflicts}")
        out_path = str(out_delta.resolve()).replace("'", "''")
        con.execute(f"""
            COPY (
              SELECT {key},
                     CAST(NULL AS VARCHAR) AS NFL_player_id,
                     CAST(NULL AS INTEGER) AS source_win,
                     MAX(source_loss) AS source_loss,
                     MAX(source_tie) AS source_tie,
                     CAST(NULL AS DOUBLE) AS source_team_points,
                     CAST(NULL AS INTEGER) AS source_playoffs,
                     CAST(NULL AS INTEGER) AS source_champion,
                     CAST(NULL AS INTEGER) AS source_final_playoff_seed
              FROM candidates GROUP BY {key}
            ) TO '{out_path}' (FORMAT PARQUET)
        """)
        unique_team_weeks = int(con.execute(f"SELECT COUNT(*) FROM (SELECT {key} FROM candidates GROUP BY {key})").fetchone()[0])
    finally:
        con.close()
    receipt = {
        "candidate_bundles": len(roots),
        "raw_rows": raw_rows,
        "unique_team_weeks": unique_team_weeks,
        "duplicates_collapsed": raw_rows - unique_team_weeks,
        "conflicting_team_weeks": conflicts,
    }
    out_receipt.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-root", type=Path, action="append", required=True)
    parser.add_argument("--out-delta", type=Path, required=True)
    parser.add_argument("--out-receipt", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(combine(args.candidate_root, args.out_delta, args.out_receipt), sort_keys=True))


if __name__ == "__main__":
    main()
