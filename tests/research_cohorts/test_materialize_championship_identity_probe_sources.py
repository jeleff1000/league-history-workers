import json
from pathlib import Path

import duckdb


def test_materializes_probe_matchup_rows_with_root_league_identity(tmp_path: Path) -> None:
    from scripts.research_cohorts.materialize_championship_identity_probe_sources import materialize

    source_root = tmp_path / "sources" / "900"
    source_root.mkdir(parents=True)
    (source_root / "championship_identity_probe.json").write_text(json.dumps({
        "db_name": "smpl_ffl_1",
        "year": 2010,
        "matchup_rows": [{
            "week": 16, "manager": "Owner", "team_key": "17", "team_name": "Team",
            "is_playoffs": 1, "champion": 1, "is_championship": 1,
        }],
    }))
    selected = tmp_path / "selected.json"
    selected.write_text(json.dumps([{"artifact_id": "900"}]))
    out_manifest = tmp_path / "manifest.json"

    result = materialize(selected_path=selected, sources_root=tmp_path / "sources", out_manifest=out_manifest)

    assert result == {"artifacts": 1, "matchup_rows": 1}
    manifest = json.loads(out_manifest.read_text())
    con = duckdb.connect()
    row = con.execute(f"SELECT db_name, year, week, manager, team_key, is_playoffs, champion FROM read_parquet('{manifest[0]['path']}')").fetchone()
    assert row == ("smpl_ffl_1", 2010, 16, "Owner", "17", 1, 1)
