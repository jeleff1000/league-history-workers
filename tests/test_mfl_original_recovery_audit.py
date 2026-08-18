from scripts.extraplatform_corpus.mfl_original_recovery_audit import (
    build_newest_source_manifest,
    build_expected_registry,
    classify_expected_identities,
    extract_mfl_identities_from_duckdb,
    extract_mfl_identities_from_index,
    normalize_mfl_identity,
    select_newest_payloads,
    validate_expected_registry,
)


def test_normalize_mfl_identity_accepts_plain_and_db_name_forms():
    assert normalize_mfl_identity(2006, "26487") == (2006, "26487")
    assert normalize_mfl_identity("2006", "mfl_2006_26487") == (2006, "26487")


def test_classify_expected_identities_requires_one_conflict_free_full_payload():
    expected = {(2006, "1"), (2006, "2"), (2006, "3")}
    observed = {
        (2006, "1"): {"full_schema_payloads": 1, "conflicting_payloads": 0},
        (2006, "2"): {"full_schema_payloads": 0, "conflicting_payloads": 0},
        (2006, "3"): {"full_schema_payloads": 2, "conflicting_payloads": 1},
    }

    report = classify_expected_identities(expected, observed)

    assert report["recovered"] == [(2006, "1")]
    assert report["missing_full_payload"] == [(2006, "2")]
    assert report["conflicting_payloads"] == [(2006, "3")]


def test_extract_mfl_identities_from_verbose_index_without_loading_whole_json(tmp_path):
    index = tmp_path / "mfl_register_all_runs.json"
    index.write_text(
        '{"leagues":[{"db_name":"mfl_2006_26487"},'
        '{"db_name":"mfl_2016_10001"}]}',
        encoding="utf-8",
    )

    assert extract_mfl_identities_from_index(index) == {
        (2006, "26487"),
        (2016, "10001"),
    }


def test_validate_expected_registry_rejects_wrong_total_or_year_counts():
    identities = {(2004, "1"), (2004, "2"), (2005, "3")}
    assert validate_expected_registry(
        identities, expected_total=3, expected_by_year={2004: 2, 2005: 1}
    ) == {2004: 2, 2005: 1}

    try:
        validate_expected_registry(
            identities, expected_total=3, expected_by_year={2004: 1, 2005: 2}
        )
    except ValueError as exc:
        assert "per-year" in str(exc)
    else:
        raise AssertionError("wrong per-year registry must fail")


def test_extract_mfl_identities_from_source_duckdb(tmp_path):
    import duckdb

    source = tmp_path / "mfl_register_chunk.duckdb"
    connection = duckdb.connect(str(source))
    connection.execute("CREATE TABLE league_settings(year INTEGER, db_name VARCHAR)")
    connection.execute(
        "INSERT INTO league_settings VALUES (2006, 'mfl_2006_26487'), (2016, 'mfl_2016_10001')"
    )
    connection.close()

    assert extract_mfl_identities_from_duckdb(source) == {
        (2006, "26487"),
        (2016, "10001"),
    }


def test_build_expected_registry_unions_proof_and_source_chunks(tmp_path):
    import duckdb

    proof = tmp_path / "proof.json"
    proof.write_text('{"leagues":[{"db_name":"mfl_2004_1"}]}', encoding="utf-8")
    source = tmp_path / "source.duckdb"
    connection = duckdb.connect(str(source))
    connection.execute("CREATE TABLE league_settings(season INTEGER, db_name VARCHAR)")
    connection.execute("INSERT INTO league_settings VALUES (2004, 'mfl_2004_2')")
    connection.close()

    report = build_expected_registry(
        proof_index=proof,
        source_chunks=[source],
        expected_total=2,
        expected_by_year={2004: 2},
    )

    assert report["proof_identity_count"] == 1
    assert report["source_identity_count"] == 1
    assert report["identity_count"] == 2
    assert report["identities"] == [(2004, "1"), (2004, "2")]


def test_select_newest_payloads_uses_timestamp_then_artifact_id_tiebreaker():
    selected = select_newest_payloads({
        (2004, "1"): [
            {"artifact_id": 10, "created_at": "2026-08-14T00:00:00Z", "payload_sha256": "old"},
            {"artifact_id": 11, "created_at": "2026-08-15T00:00:00Z", "payload_sha256": "new"},
        ],
        (2004, "2"): [
            {"artifact_id": 20, "created_at": "2026-08-15T00:00:00Z", "payload_sha256": "first"},
            {"artifact_id": 21, "created_at": "2026-08-15T00:00:00Z", "payload_sha256": "second"},
        ],
    })

    assert selected[(2004, "1")]["payload_sha256"] == "new"
    assert selected[(2004, "2")]["payload_sha256"] == "second"


def test_build_newest_source_manifest_records_protected_fallbacks():
    expected = {(2004, "1"), (2016, "2")}
    candidates = {
        (2004, "1"): [{"artifact_id": 9, "created_at": "2026-08-15T00:00:00Z", "payload_sha256": "x"}],
    }

    manifest = build_newest_source_manifest(
        expected=expected,
        campaign_candidates=candidates,
        protected_fallbacks={(2016, "2")},
        protected_source_run_id=31978502002,
    )

    assert manifest["campaign_count"] == 1
    assert manifest["protected_fallback_count"] == 1
    assert manifest["missing_count"] == 0
    assert manifest["entries"] == [
        {"season": 2004, "league_id": "1", "source_type": "campaign_artifact", "artifact_id": 9,
         "created_at": "2026-08-15T00:00:00Z", "payload_sha256": "x"},
        {"season": 2016, "league_id": "2", "source_type": "protected_source_chunk",
         "protected_source_run_id": 31978502002},
    ]


def test_build_newest_source_manifest_requires_exact_protected_artifact_provenance():
    expected = {(2016, "2")}

    manifest = build_newest_source_manifest(
        expected=expected,
        campaign_candidates={},
        protected_fallbacks={(2016, "2")},
        protected_source_run_id=31978502002,
        protected_fallback_provenance={
            (2016, "2"): {
                "artifact_id": 9272058815,
                "artifact_name": "mfl-mass-source-31927415400",
                "created_at": "2026-08-16T23:43:08Z",
                "digest": "sha256:source",
            }
        },
    )

    assert manifest["entries"] == [{
        "season": 2016,
        "league_id": "2",
        "source_type": "protected_source_chunk",
        "protected_source_run_id": 31978502002,
        "artifact_id": 9272058815,
        "artifact_name": "mfl-mass-source-31927415400",
        "created_at": "2026-08-16T23:43:08Z",
        "digest": "sha256:source",
    }]


def test_build_newest_source_manifest_rejects_incomplete_protected_provenance():
    try:
        build_newest_source_manifest(
            expected={(2016, "2")},
            campaign_candidates={},
            protected_fallbacks={(2016, "2")},
            protected_source_run_id=31978502002,
            protected_fallback_provenance={},
        )
    except ValueError as exc:
        assert "provenance" in str(exc)
    else:
        raise AssertionError("protected fallback without exact provenance must fail")
