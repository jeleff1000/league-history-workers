"""Deterministic cohort selection and OAuth-grant-aware dispatch scheduling."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from typing import Any, Iterable


ERAS: tuple[tuple[str, int, int], ...] = (
    ("2001-2009", 2001, 2009),
    ("2010-2017", 2010, 2017),
    ("2018-2022", 2018, 2022),
    ("2023-2025", 2023, 2025),
)


@dataclass(frozen=True)
class Candidate:
    task_id: str
    grant_id: str
    league_key: str
    season: int
    cohort_slug: str
    lineage_id: str
    # Hashed Yahoo user identity. One PERSON can authorize more than once (one
    # token per league they imported), and Yahoo rate-limits the ACCOUNT, not
    # the token -- so spacing must be measured between people, not grants.
    identity_id: str = ""

    @property
    def spacing_key(self) -> str:
        """The unit dispatch spaces on: the person, falling back to the token."""
        return self.identity_id or self.grant_id

    @property
    def era(self) -> str:
        for label, start, end in ERAS:
            if start <= self.season <= end:
                return label
        return "out-of-range"


def _candidate_order(candidates: Iterable[Candidate]) -> list[Candidate]:
    rows = list(candidates)
    cohort_counts = Counter(row.cohort_slug for row in rows)
    return sorted(
        rows,
        key=lambda row: (
            cohort_counts[row.cohort_slug],
            row.season,
            row.grant_id,
            row.task_id,
        ),
    )


def select_cross_era(candidates: Iterable[Candidate], limit: int = 12) -> list[Candidate]:
    """Select a balanced cross-era pilot, preferring unique grants and lineages."""
    eligible = [row for row in candidates if row.era != "out-of-range"]
    if limit <= 0 or not eligible:
        return []
    selected: list[Candidate] = []
    selected_tasks: set[str] = set()
    used_grants: set[str] = set()
    used_lineages: set[str] = set()
    base, remainder = divmod(limit, len(ERAS))

    for index, (label, _, _) in enumerate(ERAS):
        target = base + (1 if index < remainder else 0)
        era_rows = _candidate_order(row for row in eligible if row.era == label)
        for row in era_rows:
            if len([item for item in selected if item.era == label]) >= target:
                break
            if row.grant_id in used_grants or row.lineage_id in used_lineages:
                continue
            selected.append(row)
            selected_tasks.add(row.task_id)
            used_grants.add(row.grant_id)
            used_lineages.add(row.lineage_id)

    remaining = _candidate_order(row for row in eligible if row.task_id not in selected_tasks)
    while len(selected) < limit and remaining:
        row = min(
            remaining,
            key=lambda item: (
                item.grant_id in used_grants,
                item.lineage_id in used_lineages,
                item.season,
                item.task_id,
            ),
        )
        remaining.remove(row)
        selected.append(row)
        used_grants.add(row.grant_id)
        used_lineages.add(row.lineage_id)
    return selected[:limit]


def _widest_year_sample(rows: list[Candidate], count: int) -> list[Candidate]:
    by_year: dict[int, Candidate] = {}
    for row in sorted(rows, key=lambda item: (item.season, item.task_id)):
        by_year.setdefault(row.season, row)
    available = list(by_year.values())
    if len(available) <= count:
        return available
    selected = [available[0], available[-1]] if count > 1 else [available[0]]
    while len(selected) < count:
        unused = [row for row in available if row not in selected]
        selected.append(
            max(
                unused,
                key=lambda row: (
                    min(abs(row.season - picked.season) for picked in selected),
                    -row.season,
                ),
            )
        )
    return sorted(selected, key=lambda row: row.season)


def select_spacing(
    candidates: Iterable[Candidate], *, grants: int = 4, years_per_grant: int = 3
) -> list[Candidate]:
    """Choose long-history grants and maximally separated seasons within each grant."""
    grouped: dict[str, list[Candidate]] = defaultdict(list)
    for row in candidates:
        if row.era != "out-of-range":
            grouped[row.grant_id].append(row)
    eligible = [
        (grant_id, rows)
        for grant_id, rows in grouped.items()
        if len({row.season for row in rows}) >= years_per_grant
    ]
    eligible.sort(
        key=lambda item: (
            -(max(row.season for row in item[1]) - min(row.season for row in item[1])),
            item[0],
        )
    )
    selected: list[Candidate] = []
    for _, rows in eligible[:grants]:
        selected.extend(_widest_year_sample(rows, years_per_grant))
    return selected


class CredentialScheduler:
    """Dispatch tasks fairly across grants while retaining resumable state."""

    def __init__(self, candidates: Iterable[Candidate], state: dict[str, Any] | None = None):
        self.candidates = {row.task_id: row for row in candidates}
        raw = state or {}
        self.completed = set(raw.get("completed", []))
        self.failed = dict(raw.get("failed", {}))
        self.in_flight = set(raw.get("in_flight", []))
        self.last_grant_id = raw.get("last_grant_id")
        self.last_year_by_grant = {
            str(key): int(value) for key, value in raw.get("last_year_by_grant", {}).items()
        }
        self.last_used_by_grant = {
            str(key): int(value) for key, value in raw.get("last_used_by_grant", {}).items()
        }
        self.cooldown_until = {
            str(key): float(value) for key, value in raw.get("cooldown_until", {}).items()
        }
        self.dispatch_trace = list(raw.get("dispatch_trace", []))
        self._tick = int(raw.get("tick", len(self.dispatch_trace)))

    def _pending(self, now: float) -> list[Candidate]:
        return [
            row
            for row in self.candidates.values()
            if row.task_id not in self.completed
            and row.task_id not in self.failed
            and row.task_id not in self.in_flight
            and self.cooldown_until.get(row.spacing_key, 0) <= now
        ]

    def next_ready_time(self, now: float) -> float | None:
        """Earliest cooldown expiry among tasks blocked only by an identity cooldown."""
        expiries = [
            self.cooldown_until.get(row.spacing_key, 0)
            for row in self.candidates.values()
            if row.task_id not in self.completed
            and row.task_id not in self.failed
            and row.task_id not in self.in_flight
        ]
        pending = [expiry for expiry in expiries if expiry > now]
        return min(pending) if pending else None

    def next(self, now: float) -> Candidate | None:
        # Scheduling state is keyed by spacing_key (the person), so a user who
        # authorized twice cannot be dispatched back-to-back via two tokens.
        ready = self._pending(now)
        if not ready:
            return None
        keys = {row.spacing_key for row in ready}
        if self.last_grant_id in keys and len(keys) > 1:
            keys.remove(self.last_grant_id)
        spacing_key = min(
            keys,
            key=lambda key: (self.last_used_by_grant.get(key, -1), key),
        )
        grant_rows = [row for row in ready if row.spacing_key == spacing_key]
        last_year = self.last_year_by_grant.get(spacing_key)
        if last_year is None:
            chosen = min(grant_rows, key=lambda row: (row.season, row.task_id))
        else:
            chosen = min(
                grant_rows,
                key=lambda row: (-abs(row.season - last_year), row.season, row.task_id),
            )
        self.in_flight.add(chosen.task_id)
        self.last_grant_id = chosen.spacing_key
        self.last_year_by_grant[chosen.spacing_key] = chosen.season
        self.last_used_by_grant[chosen.spacing_key] = self._tick
        self._tick += 1
        self.dispatch_trace.append(chosen.task_id)
        return chosen

    def complete(self, task_id: str) -> None:
        self.in_flight.discard(task_id)
        self.completed.add(task_id)

    def fail(self, task_id: str, error_class: str) -> None:
        self.in_flight.discard(task_id)
        self.failed[task_id] = error_class

    def rate_limit(self, task_id: str, *, now: float, cooldown_seconds: float) -> None:
        self.in_flight.discard(task_id)
        # Yahoo throttles the account, so cool down the PERSON: a second token
        # belonging to the same user is equally throttled.
        spacing_key = self.candidates[task_id].spacing_key
        self.cooldown_until[spacing_key] = max(
            self.cooldown_until.get(spacing_key, 0), now + cooldown_seconds
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "completed": sorted(self.completed),
            "failed": dict(sorted(self.failed.items())),
            "in_flight": sorted(self.in_flight),
            "last_grant_id": self.last_grant_id,
            "last_year_by_grant": dict(sorted(self.last_year_by_grant.items())),
            "last_used_by_grant": dict(sorted(self.last_used_by_grant.items())),
            "cooldown_until": dict(sorted(self.cooldown_until.items())),
            "dispatch_trace": list(self.dispatch_trace),
            "tick": self._tick,
            "candidates": [asdict(row) for row in self.candidates.values()],
        }
