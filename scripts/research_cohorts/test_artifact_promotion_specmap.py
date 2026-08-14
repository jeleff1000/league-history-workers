from artifact_promotion_specmap import bind_to_canonical_schema, classify_source_schema


def test_player_week_schema_requires_exact_player_identity_and_can_fill_player_cells():
    contract = classify_source_schema({
        "db_name", "year", "week", "NFL_player_id", "is_rostered",
        "is_started", "win", "team_points", "is_playoffs", "champion",
    })

    assert contract.grain == "player_week"
    assert contract.source_key == ("db_name", "year", "week", "NFL_player_id")
    assert contract.cache_key == ("db_name", "year", "week", "NFL_player_id")
    assert contract.allowed_target_columns == (
        "is_rostered", "is_started", "win", "team_points", "is_playoffs", "champion",
    )
    assert contract.may_insert_missing_player_rows is True


def test_team_week_schema_fans_signals_out_by_team_key_not_player_identity():
    contract = classify_source_schema({
        "db_name", "year", "week", "team_key", "manager", "win", "loss",
        "tie", "team_points", "is_playoffs", "champion",
    })

    assert contract.grain == "team_week"
    assert contract.source_key == ("db_name", "year", "week", "team_key")
    assert contract.cache_key == ("db_name", "year", "week", "team_key")
    assert contract.allowed_target_columns == (
        "win", "loss", "tie", "team_points", "is_playoffs",
    )
    assert contract.quarantined_source_columns == ("champion",)
    assert contract.may_insert_missing_player_rows is False


def test_team_champion_is_writable_only_with_explicit_championship_week_evidence():
    contract = classify_source_schema({
        "db_name", "year", "week", "team_key", "is_championship", "champion",
    })

    assert contract.allowed_target_columns == ("champion",)
    assert contract.quarantined_source_columns == ()


def test_team_week_schema_uses_manager_when_team_key_is_not_available():
    contract = classify_source_schema({
        "db_name", "year", "week", "manager", "win", "loss", "tie", "is_playoffs",
    })

    assert contract.grain == "team_week_manager"
    assert contract.source_key == ("db_name", "year", "week", "manager")
    assert contract.cache_key == ("db_name", "year", "week", "manager")
    assert contract.allowed_target_columns == ("win", "loss", "tie", "is_playoffs")
    assert contract.may_insert_missing_player_rows is False


def test_league_year_settings_schema_only_maps_declared_cohort_dimensions():
    contract = classify_source_schema({
        "db_name", "year", "team_count", "ppr", "pass_td", "playoff_teams",
        "roster_config", "format", "draft_style",
    })

    assert contract.grain == "league_year_settings"
    assert contract.source_key == ("db_name", "year")
    assert contract.cache_key == ("db_name", "year")
    assert contract.allowed_target_columns == (
        "cohort_teams", "cohort_scoring", "cohort_pass_td", "cohort_playoff_teams",
        "cohort_roster", "cohort_best_ball", "cohort_dynasty",
    )


def test_team_signal_without_team_identity_is_non_promotable_even_when_it_has_an_outcome():
    contract = classify_source_schema({
        "db_name", "year", "week", "win", "team_points", "is_playoffs",
    })

    assert contract.grain == "unattributable_team_signal"
    assert contract.allowed_target_columns == ()
    assert contract.terminal_reason == "missing_team_identity"


def test_aggregate_only_schema_never_targets_the_canonical_cache():
    contract = classify_source_schema({"rank_slot", "mean_spread", "sample_size"})

    assert contract.grain == "aggregate_only"
    assert contract.source_key == ()
    assert contract.cache_key == ()
    assert contract.allowed_target_columns == ()


def test_runtime_schema_binding_blocks_only_source_fields_absent_from_the_approved_cache():
    contract = classify_source_schema({
        "db_name", "year", "week", "NFL_player_id", "win", "loss", "tie",
    })

    bound = bind_to_canonical_schema(contract, {
        "db_name", "year", "week", "NFL_player_id", "win",
    })

    assert bound.writable_target_columns == ("win",)
    assert bound.blocked_source_columns == ("loss", "tie")
    assert bound.cache_key_present is True
