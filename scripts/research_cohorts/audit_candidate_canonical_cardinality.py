"""Read-only cardinality audit for exact player-cell promotion candidates."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

KEY = ("db_name", "year", "week", "NFL_player_id", "manager", "team_key", "team_name", "platform")


def audit(base: Path, delta: Path, out: Path, examples: Path) -> dict[str, int]:
    con = duckdb.connect(str(base), read_only=True)
    try:
        path = str(delta.resolve()).replace("'", "''")
        con.execute(f"CREATE TEMP VIEW delta AS SELECT * FROM read_parquet('{path}')")
        key = ", ".join('d."' + col + '"' for col in KEY)
        join = """
          d.db_name=p.db_name
          AND CAST(d.year AS INTEGER)=CAST(p.year AS INTEGER)
          AND CAST(d.week AS INTEGER)=CAST(p.week AS INTEGER)
          AND d.NFL_player_id IS NOT DISTINCT FROM p.NFL_player_id
          AND d.platform IS NOT DISTINCT FROM p.platform
          AND d.manager IS NOT DISTINCT FROM p.manager
          AND (p.team_key IS NULL OR d.team_key=p.team_key)
          AND (p.team_name IS NULL OR d.team_name=p.team_name)
        """
        candidate_keys = int(con.execute("SELECT COUNT(*) FROM delta").fetchone()[0])
        matched_physical_rows = int(con.execute(f"SELECT COUNT(*) FROM delta d JOIN public.player_fantasy p ON {join}").fetchone()[0])
        rows = con.execute(f"""
          SELECT {key}, COUNT(*) AS physical_recipient_rows
          FROM delta d JOIN public.player_fantasy p ON {join}
          GROUP BY {key} HAVING COUNT(*) > 1
          ORDER BY physical_recipient_rows DESC
          LIMIT 100
        """).fetchall()
        columns = [x[0] for x in con.description]
        duplicate_recipient_keys = int(con.execute(f"""
          SELECT COUNT(*) FROM (
            SELECT {key} FROM delta d JOIN public.player_fantasy p ON {join}
            GROUP BY {key} HAVING COUNT(*) > 1
          )
        """).fetchone()[0])
    finally:
        con.close()
    result = {
        "candidate_keys": candidate_keys,
        "matched_physical_rows": matched_physical_rows,
        "duplicate_recipient_keys": duplicate_recipient_keys,
        "excess_physical_rows": matched_physical_rows - candidate_keys,
        "read_only": True,
    }
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    examples.write_text(json.dumps([dict(zip(columns, row)) for row in rows], indent=2) + "\n", encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--delta", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--examples", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(audit(args.base, args.delta, args.out, args.examples), sort_keys=True))


if __name__ == "__main__":
    main()
