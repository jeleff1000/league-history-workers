"""Private credential inventory with round-robin Yahoo discovery."""

from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Protocol

from scripts.yahoo_corpus.scheduler import Candidate


CREDENTIAL_SQL = """
SELECT database_name, league_id, encrypted_refresh_token
FROM main.league_credentials
WHERE encrypted_refresh_token IS NOT NULL
  AND length(encrypted_refresh_token) > 0
ORDER BY database_name
"""


@dataclass(frozen=True)
class Grant:
    grant_id: str
    refresh_token: str = field(repr=False)
    anchor_keys: tuple[str, ...]
    source_databases: tuple[str, ...] = field(repr=False)


class InventoryAdapter(Protocol):
    def step(self) -> Candidate | None:
        """Perform at most one grant-scoped discovery unit, or return None when exhausted."""


class SeasonEnumerationAdapter:
    """Enumerate a grant's league-YEARS directly, treating each season as independent.

    Yahoo's ``/users;use_login=1/games;game_codes=nfl`` returns EVERY season the
    user played, and each game's ``leagues`` collection returns that season's
    leagues with their season and team count. That is the whole corpus for this
    grant -- no renewal-chain walking, and no per-league settings fetch.

    Both of those cost only throttle: the chain rediscovers league keys the
    games enumeration already returned (plus league-years the grant cannot
    read), and settings are re-read by the import anyway. Cohort classification
    moves to validation, which derives it from imported data.
    """

    def __init__(
        self,
        grant: Grant,
        client: Any,
        *,
        anchor_season: Callable[[str], int] | None = None,
    ) -> None:
        self.grant = grant
        self.client = client
        self.anchor_season = anchor_season
        self.failures: list[dict[str, str]] = []
        self._initialized = False
        self._queue: deque[tuple[str, int]] = deque()
        self._seen: set[str] = set()

    @property
    def pending(self) -> bool:
        return bool(self._queue)

    def _initialize(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self.client.refresh()
        # Yahoo may allow settings access for an OAuth grant while denying the
        # account-wide games endpoint (403: application not authorized).  The
        # grant still carries anchor league keys from our credential inventory,
        # so keep those and continue instead of discarding the whole account.
        try:
            games = self.client.discover_games()
        except Exception as exc:
            # The account-wide games collection is not authorized for every
            # Yahoo grant.  Existing league keys in the credential bank remain
            # valid anchors, so continue with those instead of dropping the
            # entire manager from discovery.
            self.failures.append(
                {"stage": "discover_games", "error_class": type(exc).__name__}
            )
            games = []
            if self.anchor_season is not None:
                for league_key in self.grant.anchor_keys:
                    try:
                        season = int(self.anchor_season(league_key))
                    except Exception as anchor_exc:
                        self.failures.append(
                            {
                                "stage": "anchor_settings",
                                "error_class": type(anchor_exc).__name__,
                            }
                        )
                        continue
                    if 2001 <= season <= 2025:
                        self._queue.append((league_key, season))
                        self._seen.add(league_key)
        for game in games:
            try:
                leagues = self.client.discover_leagues(game)
            except Exception as exc:
                # One unreadable season must not cost the grant's other seasons.
                self.failures.append(
                    {"stage": "discover_leagues", "error_class": type(exc).__name__}
                )
                continue
            for league in leagues:
                league_key = str(league.get("league_key") or "")
                if not league_key or league_key in self._seen:
                    continue
                try:
                    season = int(str(league.get("season") or game.get("season") or 0))
                except ValueError:
                    continue
                self._seen.add(league_key)
                self._queue.append((league_key, season))

    def step(self) -> Candidate | None:
        try:
            self._initialize()
        except Exception as exc:
            self.failures.append({"stage": "authentication", "error_class": type(exc).__name__})
            self._queue.clear()
            return None
        while self._queue:
            league_key, season = self._queue.popleft()
            task_digest = hashlib.sha256(league_key.encode("utf-8")).hexdigest()[:16]
            return Candidate(
                task_id=f"yahoo-{task_digest}",
                grant_id=self.grant.grant_id,
                league_key=league_key,
                season=season,
                # Cohort is derived at validation time from the imported
                # settings; planning does not need to pre-classify.
                cohort_slug="",
                lineage_id=league_key,
                # identity_id is assigned once the whole plan is known (a
                # grant's league SET is the account fingerprint) -- see
                # cli.assign_identities.
            )
        return None


class YahooGrantAdapter:
    """Lazily enumerate every league-year reachable by one Yahoo grant."""

    def __init__(
        self,
        grant: Grant,
        client: Any,
        *,
        parse_xml: Callable[[str, str], dict[str, Any]],
        classify: Callable[[dict[str, Any]], dict[str, Any]],
        renewal_keys: Callable[[str], list[str]],
    ) -> None:
        self.grant = grant
        self.client = client
        self.parse_xml = parse_xml
        self.classify = classify
        self.renewal_keys = renewal_keys
        self.failures: list[dict[str, str]] = []
        self._initialized = False
        self._queue: deque[tuple[str, str]] = deque(
            (league_key, league_key) for league_key in grant.anchor_keys
        )
        self._queued = set(grant.anchor_keys)
        self._seen: set[str] = set()
        self._attempts: dict[str, int] = {}

    MAX_LEAGUE_ATTEMPTS = 4

    @property
    def pending(self) -> bool:
        """True while league keys remain to try -- including requeued retries."""
        return bool(self._queue)

    def _initialize(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self.client.refresh()
        try:
            games = self.client.discover_games()
        except Exception as exc:
            # Preserve the credential bank's anchor league-years when Yahoo
            # denies account-wide games discovery.  The per-league settings
            # request below is the same route used by quick imports.
            self.failures.append(
                {"stage": "discover_games", "error_class": type(exc).__name__}
            )
            games = []
        for game in games:
            try:
                leagues = self.client.discover_leagues(game)
            except Exception as exc:
                self.failures.append(
                    {"stage": "discover_leagues", "error_class": type(exc).__name__}
                )
                continue
            for league in leagues:
                league_key = str(league.get("league_key") or "")
                if league_key and league_key not in self._queued:
                    self._queue.append((league_key, league_key))
                    self._queued.add(league_key)

    def step(self) -> Candidate | None:
        try:
            self._initialize()
        except Exception as exc:
            self.failures.append({"stage": "authentication", "error_class": type(exc).__name__})
            self._queue.clear()
            return None
        while self._queue:
            league_key, lineage_id = self._queue.popleft()
            if league_key in self._seen:
                continue
            try:
                xml = self.client.fetch_settings_xml(league_key)
                self._seen.add(league_key)
                settings = self.parse_xml(xml, league_key)
                classification = self.classify(settings)
                for linked_key in self.renewal_keys(xml):
                    if linked_key and linked_key not in self._queued and linked_key not in self._seen:
                        self._queue.append((linked_key, lineage_id))
                        self._queued.add(linked_key)
                metadata = settings.get("metadata") or {}
                season = int(metadata.get("season") or 0)
                cohort_slug = str(classification.get("cohort_slug") or "")
                if classification.get("classification_status") != "classified" or not cohort_slug:
                    self.failures.append(
                        {"stage": "classification", "error_class": "IncompleteClassification"}
                    )
                    continue
                task_digest = hashlib.sha256(league_key.encode("utf-8")).hexdigest()[:16]
                return Candidate(
                    task_id=f"yahoo-{task_digest}",
                    grant_id=self.grant.grant_id,
                    league_key=league_key,
                    season=season,
                    cohort_slug=cohort_slug,
                    lineage_id=lineage_id,
                )
            except Exception as exc:
                # A throttled/transient settings fetch must NOT cost us the
                # league-year: requeue it and yield control so the rotation
                # spaces the retry behind every other grant. Only give up
                # after MAX_LEAGUE_ATTEMPTS.
                # Authentication/authorization/not-found responses are
                # deterministic for this grant and should not be retried four
                # times.  YahooRequestFailure exposes the HTTP status without
                # leaking provider response bodies.
                status = getattr(exc, "status", None)
                if status in {401, 403, 404}:
                    self._seen.add(league_key)
                    self.failures.append(
                        {"stage": "settings", "error_class": type(exc).__name__}
                    )
                    continue
                attempts = self._attempts.get(league_key, 0) + 1
                self._attempts[league_key] = attempts
                if attempts < self.MAX_LEAGUE_ATTEMPTS:
                    self._queue.append((league_key, lineage_id))
                    self.failures.append(
                        {"stage": "settings-retry", "error_class": type(exc).__name__}
                    )
                    return None
                self._seen.add(league_key)
                self.failures.append(
                    {"stage": "settings", "error_class": type(exc).__name__}
                )
        return None


def _grant_id(refresh_token: str) -> str:
    digest = hashlib.sha256(refresh_token.encode("utf-8")).hexdigest()[:16]
    return f"grant-{digest}"


def load_grants(
    reader: Any,
    encryption_keys: list[str],
    *,
    decryptor: Callable[[str, list[str]], str],
) -> list[Grant]:
    """Load and deduplicate Yahoo grants without serializing plaintext tokens."""
    grouped: dict[str, dict[str, set[str]]] = {}
    for row in reader.query(CREDENTIAL_SQL, database="___ops"):
        token = decryptor(str(row["encrypted_refresh_token"]), encryption_keys)
        group = grouped.setdefault(token, {"anchors": set(), "databases": set()})
        league_key = str(row.get("league_id") or "").strip()
        database_name = str(row.get("database_name") or "").strip()
        if league_key:
            group["anchors"].add(league_key)
        if database_name:
            group["databases"].add(database_name)
    return sorted(
        (
            Grant(
                grant_id=_grant_id(token),
                refresh_token=token,
                anchor_keys=tuple(sorted(values["anchors"])),
                source_databases=tuple(sorted(values["databases"])),
            )
            for token, values in grouped.items()
        ),
        key=lambda row: row.grant_id,
    )


def discover_candidates(
    grants: Iterable[Grant],
    *,
    adapter_factory: Callable[[Grant], InventoryAdapter],
    checkpoint: dict[str, Any] | None = None,
    max_operations: int | None = None,
    stop_when: Callable[[list[Candidate]], bool] | None = None,
) -> tuple[list[Candidate], dict[str, Any]]:
    """Rotate one discovery operation per grant until every adapter is exhausted."""
    prior = checkpoint or {}
    completed = set(prior.get("completed_grants", []))
    candidates: dict[str, Candidate] = {}
    for raw in prior.get("candidates", []):
        row = Candidate(**raw)
        candidates[row.task_id] = row
    adapters = {
        grant.grant_id: adapter_factory(grant)
        for grant in grants
        if grant.grant_id not in completed
    }
    operations = 0
    while adapters and (max_operations is None or operations < max_operations):
        exhausted: list[str] = []
        for grant_id in sorted(adapters):
            if max_operations is not None and operations >= max_operations:
                break
            adapter = adapters[grant_id]
            row = adapter.step()
            operations += 1
            if row is None:
                # None means "no candidate this operation" -- only retire the
                # grant when nothing is left to try. A grant with requeued
                # (throttled) leagues stays in the rotation, and is never
                # checkpointed as completed with work outstanding.
                if not getattr(adapter, "pending", False):
                    exhausted.append(grant_id)
                    completed.add(grant_id)
                continue
            if 2001 <= row.season <= 2025:
                candidates[row.task_id] = row
        for grant_id in exhausted:
            adapters.pop(grant_id, None)
        if stop_when is not None and stop_when(list(candidates.values())):
            break
    state = {
        "completed_grants": sorted(completed),
        "candidates": [
            {
                "task_id": row.task_id,
                "grant_id": row.grant_id,
                "league_key": row.league_key,
                "season": row.season,
                "cohort_slug": row.cohort_slug,
                "lineage_id": row.lineage_id,
                "identity_id": row.identity_id,
            }
            for row in sorted(candidates.values(), key=lambda item: item.task_id)
        ],
        "operations": int(prior.get("operations", 0)) + operations,
    }
    return list(candidates.values()), state
