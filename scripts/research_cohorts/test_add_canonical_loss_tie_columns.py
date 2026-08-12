import hashlib
import json

import duckdb

from add_canonical_loss_tie_columns import add_columns
from canonical_player_schema import BASE_PLAYER_COLUMNS, EXPECTED_COHORT_COLUMNS


def _hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _base(path):
    columns = BASE_PLAYER_COLUMNS + sorted(EXPECTED_COHORT_COLUMNS)
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute(
        "CREATE TABLE public.player_fantasy (" + ", ".join(
            f'"{column}" VARCHAR' for column in columns
        ) + ")"
    )
    con.execute("INSERT INTO public.player_fantasy (db_name) VALUES ('league-a')")
    con.close()


def test_add_columns_adds_only_loss_and_tie_and_preserves_rows_and_ops(tmp_path):
    base = tmp_path / "base.duckdb"
    ops = tmp_path / "ops.duckdb"
    report = tmp_path / "report.json"
    _base(base)
    ops.write_bytes(b"protected-ops")
    before_ops = _hash(ops)

    result = add_columns(base=base, ops=ops, report=report)

    assert result["cache_mutated"] is True
    assert result["new_lineage"] is False
    assert result["added_columns"] == ["loss", "tie"]
    assert result["player_rows_before"] == result["player_rows_after"] == 1
    assert result["ops_unchanged"] is True
    assert _hash(ops) == before_ops
    con = duckdb.connect(str(base), read_only=True)
    assert [row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()][-2:] == ["loss", "tie"]
    assert con.execute("SELECT loss, tie FROM public.player_fantasy").fetchone() == (None, None)
    con.close()
    assert json.loads(report.read_text())["added_columns"] == ["loss", "tie"]


def test_add_columns_is_idempotent_without_extra_schema_drift(tmp_path):
    base = tmp_path / "base.duckdb"
    ops = tmp_path / "ops.duckdb"
    _base(base)
    ops.write_bytes(b"protected-ops")

    add_columns(base=base, ops=ops, report=tmp_path / "first.json")
    result = add_columns(base=base, ops=ops, report=tmp_path / "second.json")

    assert result["added_columns"] == []
    assert result["schema_after"] == result["schema_before"]
