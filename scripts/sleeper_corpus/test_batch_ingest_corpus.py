"""Regression tests for corpus crawl planning."""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent))
import batch_ingest_corpus as planner


class BuildPlanTest(unittest.TestCase):
    def test_pooled_l4_target_caps_a_cohort_across_years(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            drain = Path(td) / "drain_leagues.parquet"
            con = duckdb.connect()
            con.execute("""
                CREATE TABLE source (
                    league_id VARCHAR,
                    season INTEGER,
                    prev_league_id VARCHAR,
                    teams VARCHAR,
                    roster VARCHAR,
                    ppr VARCHAR,
                    td VARCHAR,
                    is_dynasty BOOLEAN,
                    best_ball BOOLEAN,
                    created_year INTEGER
                )
            """)
            con.executemany(
                "INSERT INTO source VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    ("old", 2023, None, "12t", "flx", "ppr", "4pt", False, False, 2023),
                    ("middle", 2024, None, "12t", "flx", "ppr", "4pt", False, False, 2024),
                    ("new", 2025, None, "12t", "flx", "ppr", "4pt", False, False, 2025),
                    ("dynasty", 2025, None, "12t", "flx", "ppr", "4pt", True, False, 2025),
                ],
            )
            con.execute(f"COPY source TO '{drain.as_posix()}' (FORMAT PARQUET)")
            con.close()

            original_drain = planner.DRAIN
            planner.DRAIN = drain
            try:
                plan = planner.build_plan(
                    2, (2023, 2025), ["single_season", "dynasty"], pool_cohorts=True
                )
            finally:
                planner.DRAIN = original_drain

        self.assertEqual([row["league_id"] for row in plan], ["dynasty", "new"])


if __name__ == "__main__":
    unittest.main()
