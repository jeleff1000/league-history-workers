"""Verify that the exact canonical cache contains every validated sidecar fill."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb


KEY = ["db_name", "year", "week", "NFL_player_id", "platform", "manager", "team_key", "team_name"]
TEAM_FIELDS = {
    "win": "source_win", "team_points": "source_team_points", "is_playoffs": "source_playoffs",
    "champion": "source_champion", "final_playoff_seed": "source_final_playoff_seed",
    "made_playoffs": "source_made_playoffs",
}
EXACT_FIELDS = {
    "win": "source_win", "team_points": "source_team_points", "is_playoffs": "source_playoffs",
    "has_po_signal": "source_has_po_signal", "champion": "source_champion",
    "final_playoff_seed": "source_final_playoff_seed", "made_playoffs": "source_made_playoffs",
    "clutch_equity": "source_clutch_equity",
}
STRUCTURED_FIELDS = {"is_playoffs": "source_is_playoffs", "champion": "source_champion"}
SETTINGS_FIELDS = ("scoring_pass_td", "playoff_teams", "roster_FLX", "roster_SUPER_FLEX", "roster_IDP", "sleeper_best_ball")


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def join_sql(left: str, right: str) -> str:
    return " AND ".join(f"{left}.{q(c)} IS NOT DISTINCT FROM {right}.{q(c)}" for c in KEY)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--team-delta", type=Path, required=True)
    ap.add_argument("--exact-delta", type=Path, required=True)
    ap.add_argument("--structured-delta", type=Path, required=True)
    ap.add_argument("--settings-delta", type=Path, required=True)
    ap.add_argument("--promotion-evidence", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    evidence_dir = args.promotion_evidence
    pre = json.loads((evidence_dir / "pre_promotion.json").read_text())
    post = json.loads((evidence_dir / "post_promotion.json").read_text())
    player_report = json.loads((evidence_dir / "player_apply_report.json").read_text())
    settings_report = json.loads((evidence_dir / "settings_apply_report.json").read_text())
    expected_ops = (evidence_dir / "ops_before.sha256").read_text().split()[0]

    con = duckdb.connect(str(args.base), read_only=True)
    result = {"checks": {}, "remaining_nulls": {}, "unmatched_structured": {}}
    pcols = [r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()]
    scols = [r[0] for r in con.execute("DESCRIBE public.league_settings").fetchall()]
    prows = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0])
    srows = int(con.execute("SELECT COUNT(*) FROM public.league_settings").fetchone()[0])
    result["checks"]["player_schema"] = pcols == pre["player_columns"] == post["player_columns"]
    result["checks"]["settings_schema"] = scols == pre["settings_columns"] == post["settings_columns"]
    result["checks"]["player_row_count"] = prows == pre["player_rows"] == post["player_rows"]
    result["checks"]["settings_row_count"] = srows == pre["settings_rows"] == post["settings_rows"]
    if not all(result["checks"].values()):
        raise SystemExit(f"cache structural check failed: {result['checks']}")

    def view(name: str, path: Path) -> set[str]:
        escaped = str(path.resolve()).replace("'", "''")
        con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM read_parquet('{escaped}')")
        return {r[0] for r in con.execute(f"DESCRIBE {name}").fetchall()}

    team_cols = view("team_delta", args.team_delta)
    exact_cols = view("exact_delta", args.exact_delta)
    structured_cols = view("structured_delta", args.structured_delta)
    settings_cols = view("settings_delta", args.settings_delta)

    for field, source in TEAM_FIELDS.items():
        if field not in pcols or source not in team_cols:
            continue
        result["remaining_nulls"][f"team_{field}"] = int(con.execute(
            f"SELECT COUNT(*) FROM team_delta d WHERE d.{q(source)} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.player_fantasy p WHERE {join_sql('p','d')} AND p.{q(field)} IS NOT NULL)"
        ).fetchone()[0])
    for field, source in EXACT_FIELDS.items():
        if field not in pcols or source not in exact_cols:
            continue
        result["remaining_nulls"][f"exact_{field}"] = int(con.execute(
            f"SELECT COUNT(*) FROM exact_delta d WHERE d.{q(source)} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.player_fantasy p WHERE {join_sql('p','d')} AND p.{q(field)} IS NOT NULL)"
        ).fetchone()[0])
    for field, source in STRUCTURED_FIELDS.items():
        if field not in pcols or source not in structured_cols:
            continue
        result["unmatched_structured"][field] = int(con.execute(
            f"SELECT COUNT(*) FROM structured_delta d WHERE d.{q(source)} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.player_fantasy p WHERE lower(trim(cast(p.manager AS VARCHAR)))=lower(trim(cast(d.manager AS VARCHAR))) AND p.db_name=d.db_name AND CAST(p.year AS INTEGER)=CAST(d.year AS INTEGER) AND CAST(p.week AS INTEGER)=CAST(d.week AS INTEGER) AND p.{q(field)} IS NOT NULL)"
        ).fetchone()[0])
    for field in SETTINGS_FIELDS:
        source = f"source_{field}"
        if field not in scols or source not in settings_cols:
            continue
        result["remaining_nulls"][f"settings_{field}"] = int(con.execute(
            f"SELECT COUNT(*) FROM settings_delta d WHERE d.{q(source)} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.league_settings s WHERE s.db_name=d.db_name AND CAST(s.year AS INTEGER)=CAST(d.year AS INTEGER) AND s.{q(field)} IS NOT NULL)"
        ).fetchone()[0])
    con.close()

    result["checks"]["ops_hash"] = sha(args.ops) == expected_ops
    result["checks"]["player_apply_report_readback"] = all(int(v) == 0 for v in player_report.get("readback_remaining_nulls", {}).values())
    result["checks"]["settings_apply_report_readback"] = all(int(v) == 0 for v in settings_report.get("readback_remaining_nulls", {}).values())
    result["checks"]["source_cells_present_after_save"] = all(int(v) == 0 for v in result["remaining_nulls"].values())
    allowed_structured = int(player_report.get("structured_unmatched_keys", 0))
    result["checks"]["structured_unmatched_only_known_quarantine"] = all(v <= allowed_structured for v in result["unmatched_structured"].values())
    result["checks"]["all_checks_pass"] = all(result["checks"].values())
    result["expected_reports"] = {"player": player_report, "settings": settings_report}
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    if not result["checks"]["all_checks_pass"]:
        raise SystemExit(json.dumps(result, sort_keys=True))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
