"""Merge overlapping team-signal candidate classes by exact canonical key."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


KEY_COLUMNS = (
    "db_name", "year", "week", "NFL_player_id", "manager", "team_key",
    "team_name", "platform",
)


def merge(*, normal: Path, replacement: Path, out: Path, report: Path) -> dict[str, object]:
    con = duckdb.connect()
    try:
        paths = [str(normal.resolve()).replace("'", "''"), str(replacement.resolve()).replace("'", "''")]
        con.execute(
            "CREATE TEMP VIEW raw AS SELECT * FROM read_parquet(["
            + ",".join(f"'{path}'" for path in paths)
            + "], union_by_name=true)"
        )
        columns = [row[0] for row in con.execute("DESCRIBE raw").fetchall()]
        missing = sorted(set(KEY_COLUMNS) - set(columns))
        if missing:
            raise ValueError(f"delta missing exact key columns: {missing}")
        source_columns = [
            column for column in columns
            if column.startswith("source_") and column != "source_files"
        ]
        if not source_columns:
            raise ValueError("delta has no source values")
        key = ", ".join(f'"{column}"' for column in KEY_COLUMNS)
        conflicts = con.execute(
            "SELECT COUNT(*) FROM (SELECT "
            + key
            + " FROM raw GROUP BY "
            + key
            + " HAVING "
            + " OR ".join(
                f'COUNT(DISTINCT CAST("{column}" AS VARCHAR)) '
                f'FILTER (WHERE "{column}" IS NOT NULL) > 1'
                for column in source_columns
            )
            + ")"
        ).fetchone()[0]
        if conflicts:
            raise ValueError(f"conflicting source values for exact delta keys: {conflicts}")
        select = [key]
        select.extend(f'MAX("{column}") AS "{column}"' for column in source_columns)
        if "source_files" in columns:
            select.append("STRING_AGG(DISTINCT source_files, '|' ORDER BY source_files) AS source_files")
        output_path = str(out.resolve()).replace("'", "''")
        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            "COPY (SELECT "
            + ", ".join(select)
            + " FROM raw GROUP BY "
            + key
            + f") TO '{output_path}' (FORMAT PARQUET)"
        )
        raw_rows = int(con.execute("SELECT COUNT(*) FROM raw").fetchone()[0])
        output_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0])
        result = {
            "raw_rows": raw_rows,
            "merged_rows": output_rows,
            "duplicate_rows_collapsed": raw_rows - output_rows,
            "conflicting_source_keys": int(conflicts),
            "key_columns": list(KEY_COLUMNS),
            "source_columns": source_columns,
        }
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return result
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--normal", required=True, type=Path)
    parser.add_argument("--replacement", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    try:
        print(json.dumps(merge(**vars(args)), sort_keys=True))
    except (OSError, ValueError, duckdb.Error) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
