from __future__ import annotations

from scripts.yahoo_corpus.scheduler import (
    Candidate,
    CredentialScheduler,
    select_cross_era,
    select_spacing,
)


def candidate(
    task_id: str,
    grant_id: str,
    season: int,
    cohort: str = "10t_flx_std_4pt",
    lineage: str | None = None,
) -> Candidate:
    return Candidate(
        task_id=task_id,
        grant_id=grant_id,
        league_key=f"{season}.l.{task_id}",
        season=season,
        cohort_slug=cohort,
        lineage_id=lineage or task_id,
    )


def test_cross_era_selects_three_per_era_on_distinct_grants() -> None:
    years = [2002, 2005, 2008, 2011, 2014, 2017, 2018, 2020, 2022, 2023, 2024, 2025]
    candidates = [candidate(f"t{i}", f"g{i}", year) for i, year in enumerate(years)]

    selected = select_cross_era(candidates, limit=12)

    assert len(selected) == 12
    assert len({row.grant_id for row in selected}) == 12
    assert {row.era for row in selected} == {
        "2001-2009",
        "2010-2017",
        "2018-2022",
        "2023-2025",
    }
    assert {era: sum(row.era == era for row in selected) for era in {row.era for row in selected}} == {
        "2001-2009": 3,
        "2010-2017": 3,
        "2018-2022": 3,
        "2023-2025": 3,
    }


def test_cross_era_fills_documented_shortfall_from_other_eras() -> None:
    candidates = [candidate("early", "g0", 2005)]
    candidates.extend(candidate(f"recent{i}", f"g{i + 1}", 2023 + (i % 3)) for i in range(5))

    selected = select_cross_era(candidates, limit=4)

    assert len(selected) == 4
    assert any(row.season == 2005 for row in selected)
    assert len({row.grant_id for row in selected}) == 4


def test_spacing_chooses_widely_separated_years_per_grant() -> None:
    candidates = []
    for grant in ("g1", "g2", "g3", "g4", "g5"):
        candidates.extend(candidate(f"{grant}-{year}", grant, year) for year in (2003, 2012, 2024, 2025))

    selected = select_spacing(candidates, grants=4, years_per_grant=3)

    assert len(selected) == 12
    assert len({row.grant_id for row in selected}) == 4
    for grant in {row.grant_id for row in selected}:
        years = sorted(row.season for row in selected if row.grant_id == grant)
        assert years == [2003, 2012, 2025]


def test_scheduler_never_repeats_grant_when_alternative_ready() -> None:
    scheduler = CredentialScheduler(
        [
            candidate("a1", "g1", 2005),
            candidate("a2", "g1", 2020),
            candidate("b1", "g2", 2012),
        ]
    )

    first = scheduler.next(now=0)
    assert first is not None
    scheduler.complete(first.task_id)
    second = scheduler.next(now=1)

    assert [first.grant_id, second.grant_id] == ["g1", "g2"]


def test_scheduler_maximizes_year_distance_within_grant() -> None:
    scheduler = CredentialScheduler(
        [
            candidate("old", "g1", 2004),
            candidate("middle", "g1", 2015),
            candidate("new", "g1", 2025),
        ],
        state={"last_year_by_grant": {"g1": 2016}},
    )

    dispatched = scheduler.next(now=0)

    assert dispatched is not None
    assert dispatched.task_id == "old"


def test_rate_limited_grant_is_requeued_behind_ready_grant() -> None:
    scheduler = CredentialScheduler(
        [candidate("a", "g1", 2005), candidate("b", "g2", 2015)]
    )
    first = scheduler.next(now=0)
    assert first is not None
    scheduler.rate_limit(first.task_id, now=0, cooldown_seconds=60)

    second = scheduler.next(now=1)
    assert second is not None
    assert second.grant_id != first.grant_id

    scheduler.complete(second.task_id)
    assert scheduler.next(now=30) is None
    assert scheduler.next(now=61).task_id == first.task_id


def test_resume_preserves_last_grant_and_dispatch_trace() -> None:
    candidates = [candidate("a", "g1", 2005), candidate("b", "g2", 2015)]
    scheduler = CredentialScheduler(candidates)
    first = scheduler.next(now=0)
    assert first is not None
    scheduler.complete(first.task_id)

    resumed = CredentialScheduler(candidates, state=scheduler.to_dict())
    second = resumed.next(now=1)

    assert second is not None
    assert second.grant_id != first.grant_id
    assert resumed.to_dict()["dispatch_trace"][:2] == [first.task_id, second.task_id]


def test_dispatch_spaces_on_person_not_token() -> None:
    """One user with TWO grants (League A and League B) must never run back-to-back.

    Yahoo throttles the account, so 'Mike, League A' followed immediately by
    'Mike, League B' is the same account twice in a row even though the tokens
    differ. While other people still have work, every Mike task must have a
    different person's league-year on either side of it.
    """
    mike = "user-mike"
    tasks = (
        # Mike: three league-years spread across TWO grants.
        Candidate("t-mike-a1", "grant-mike-a", "449.l.1", 2021, "", "449.l.1", mike),
        Candidate("t-mike-a2", "grant-mike-a", "423.l.1", 2019, "", "423.l.1", mike),
        Candidate("t-mike-b1", "grant-mike-b", "449.l.2", 2017, "", "449.l.2", mike),
        Candidate("t-ann1", "grant-ann", "449.l.3", 2020, "", "449.l.3", "user-ann"),
        Candidate("t-ann2", "grant-ann", "423.l.3", 2018, "", "423.l.3", "user-ann"),
        Candidate("t-ann3", "grant-ann", "414.l.3", 2016, "", "414.l.3", "user-ann"),
        Candidate("t-bo1", "grant-bo", "449.l.4", 2021, "", "449.l.4", "user-bo"),
        Candidate("t-bo2", "grant-bo", "423.l.4", 2019, "", "423.l.4", "user-bo"),
        Candidate("t-bo3", "grant-bo", "414.l.4", 2015, "", "414.l.4", "user-bo"),
    )
    scheduler = CredentialScheduler(tasks)

    order = []
    while True:
        row = scheduler.next(0.0)
        if row is None:
            break
        order.append(row)
        scheduler.complete(row.task_id)

    assert len(order) == 9
    identities = [row.identity_id for row in order]
    # No person runs twice in a row -- Mike's two tokens do not sneak past this.
    assert all(a != b for a, b in zip(identities, identities[1:])), identities
    # Mike's league-years are maximally spread: one per full rotation.
    mike_positions = [i for i, ident in enumerate(identities) if ident == mike]
    assert all(b - a >= 3 for a, b in zip(mike_positions, mike_positions[1:])), mike_positions


def test_rate_limit_cools_down_the_person_not_just_the_token() -> None:
    mike = "user-mike"
    tasks = (
        Candidate("t-mike-a", "grant-mike-a", "449.l.1", 2021, "", "449.l.1", mike),
        Candidate("t-mike-b", "grant-mike-b", "449.l.2", 2020, "", "449.l.2", mike),
    )
    scheduler = CredentialScheduler(tasks)
    first = scheduler.next(0.0)
    assert first is not None
    scheduler.rate_limit(first.task_id, now=0.0, cooldown_seconds=120)

    # Mike's OTHER token is throttled too -- nothing dispatchable during cooldown.
    assert scheduler.next(1.0) is None
    assert scheduler.next(121.0) is not None
