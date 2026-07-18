from __future__ import annotations

from dataclasses import dataclass

from scripts.yahoo_corpus.inventory import (
    Grant,
    YahooGrantAdapter,
    discover_candidates,
    load_grants,
)
from scripts.yahoo_corpus.scheduler import Candidate


class FakeReader:
    def query(self, sql: str, database: str) -> list[dict]:
        assert database == "___ops"
        assert "league_credentials" in sql
        return [
            {
                "database_name": "alpha",
                "league_id": "423.l.100",
                "encrypted_refresh_token": "enc-a",
            },
            {
                "database_name": "beta",
                "league_id": "449.l.200",
                "encrypted_refresh_token": "enc-b",
            },
            {
                "database_name": "alpha-renewed",
                "league_id": "449.l.101",
                "encrypted_refresh_token": "enc-a-copy",
            },
        ]


def test_load_grants_deduplicates_plaintext_and_keeps_anchor_keys() -> None:
    tokens = {"enc-a": "token-a", "enc-a-copy": "token-a", "enc-b": "token-b"}

    grants = load_grants(
        FakeReader(),
        ["key"],
        decryptor=lambda encrypted, _: tokens[encrypted],
    )

    assert len(grants) == 2
    alpha = next(row for row in grants if set(row.anchor_keys) == {"423.l.100", "449.l.101"})
    assert alpha.refresh_token == "token-a"
    assert alpha.grant_id.startswith("grant-")
    assert "token-a" not in repr(alpha)


@dataclass
class FakeAdapter:
    grant: Grant
    years: list[int]
    trace: list[str]

    def step(self) -> Candidate | None:
        self.trace.append(self.grant.grant_id)
        if not self.years:
            return None
        year = self.years.pop(0)
        return Candidate(
            task_id=f"{self.grant.grant_id}-{year}",
            grant_id=self.grant.grant_id,
            league_key=f"{year}.l.{year}",
            season=year,
            cohort_slug="10t_flx_std_4pt",
            lineage_id=self.grant.grant_id,
        )


def test_discovery_rotates_grants_one_step_at_a_time() -> None:
    grants = [
        Grant("g1", "secret-1", ("1.l.1",), ("db1",)),
        Grant("g2", "secret-2", ("2.l.2",), ("db2",)),
        Grant("g3", "secret-3", ("3.l.3",), ("db3",)),
    ]
    trace: list[str] = []
    years = {"g1": [2005, 2015], "g2": [2010, 2020], "g3": [2023]}

    rows, checkpoint = discover_candidates(
        grants,
        adapter_factory=lambda grant: FakeAdapter(grant, years[grant.grant_id], trace),
    )

    assert len(rows) == 5
    assert trace[:6] == ["g1", "g2", "g3", "g1", "g2", "g3"]
    assert checkpoint["completed_grants"] == ["g1", "g2", "g3"]


def test_discovery_filters_incomplete_and_out_of_range_seasons() -> None:
    grant = Grant("g1", "secret", ("1.l.1",), ("db",))
    trace: list[str] = []

    rows, _ = discover_candidates(
        [grant],
        adapter_factory=lambda item: FakeAdapter(item, [1999, 2024, 2026], trace),
    )

    assert [row.season for row in rows] == [2024]


class FakeYahooClient:
    def __init__(self) -> None:
        self.refreshed = False
        self.xml = {
            "423.l.100": "anchor",
            "449.l.200": "visible",
            "399.l.90": "renewed",
        }

    def refresh(self) -> None:
        self.refreshed = True

    def discover_games(self) -> list[dict[str, str]]:
        return [{"game_key": "449", "season": "2024"}]

    def discover_leagues(self, game: dict[str, str]) -> list[dict]:
        return [{"league_key": "449.l.200", "season": "2024"}]

    def fetch_settings_xml(self, league_key: str) -> str:
        return self.xml[league_key]


def test_yahoo_adapter_discovers_visible_leagues_and_follows_renewal_links() -> None:
    grant = Grant("g1", "secret", ("423.l.100",), ("db",))
    client = FakeYahooClient()
    parsed = {
        "anchor": {"metadata": {"season": 2023}},
        "visible": {"metadata": {"season": 2024}},
        "renewed": {"metadata": {"season": 2022}},
    }
    renewals = {"anchor": ["399.l.90"], "visible": [], "renewed": []}
    adapter = YahooGrantAdapter(
        grant,
        client,
        parse_xml=lambda xml, _: parsed[xml],
        classify=lambda _: {"cohort_slug": "10t_flx_std_4pt", "classification_status": "classified"},
        renewal_keys=lambda xml: renewals[xml],
    )

    rows = []
    while True:
        item = adapter.step()
        if item is None:
            break
        rows.append(item)

    assert client.refreshed is True
    assert {row.league_key for row in rows} == {"423.l.100", "449.l.200", "399.l.90"}
    assert next(row for row in rows if row.league_key == "399.l.90").lineage_id == "423.l.100"
    assert adapter.failures == []
