from normalize_artifact_promotion_candidates import normalize_source_row


def test_player_week_row_emits_only_non_null_mapped_fields_with_exact_player_key():
    candidates = normalize_source_row(
        artifact_id="101",
        source_grain="player_week",
        source_key=("db_name", "year", "week", "NFL_player_id"),
        source_to_target=(("is_started", "is_started"), ("win", "win")),
        row={
            "db_name": "league", "year": 2020, "week": 6, "NFL_player_id": "00-1",
            "is_started": 1, "win": None,
        },
    )

    assert candidates == [{
        "artifact_id": "101", "source_grain": "player_week",
        "db_name": "league", "year": 2020, "week": 6, "NFL_player_id": "00-1",
        "team_key": None, "manager": None,
        "source_column": "is_started", "target_column": "is_started", "source_value": 1,
    }]


def test_manager_week_row_fans_out_by_manager_and_never_invents_a_player_id():
    candidates = normalize_source_row(
        artifact_id="102",
        source_grain="team_week_manager",
        source_key=("db_name", "year", "week", "manager"),
        source_to_target=(("win", "win"), ("is_playoffs", "is_playoffs")),
        row={
            "db_name": "league", "year": 2020, "week": 14, "manager": "A Team",
            "win": 1, "is_playoffs": 1,
        },
    )

    assert [(row["NFL_player_id"], row["manager"], row["target_column"]) for row in candidates] == [
        (None, "A Team", "win"), (None, "A Team", "is_playoffs"),
    ]


def test_row_with_missing_contract_key_is_rejected_not_partially_promoted():
    try:
        normalize_source_row(
            artifact_id="103",
            source_grain="team_week_manager",
            source_key=("db_name", "year", "week", "manager"),
            source_to_target=(("win", "win"),),
            row={"db_name": "league", "year": 2020, "week": 6, "manager": None, "win": 1},
        )
    except ValueError as exc:
        assert str(exc) == "missing required source key: manager"
    else:
        raise AssertionError("expected missing manager to be rejected")
