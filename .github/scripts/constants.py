"""Shared constants for GitHub Actions workflows.

SINGLE SOURCE OF TRUTH — do not duplicate these values in workflow YAML.
Import this file in workflow steps that need these values.
"""

import re

CRITICAL_TABLES = [
    "matchup",
    "player_fantasy",
    "draft",
    "transactions",
    "schedule",
]

PROTECTED_TABLES = {
    "credentials",
    "oauth_tokens",
    "user_credentials",
    "tokens",
}

# Database name sanitization
DB_NAME_SANITIZE_PATTERN = re.compile(r"[^a-zA-Z0-9]+")
DB_NAME_SANITIZE_REPLACEMENT = "_"


def sanitize_db_name(name: str, fallback: str = "db") -> str:
    """Sanitize a league name into a valid database name.

    This mirrors db_utils.sanitize_database_name() but is
    usable in workflow scripts without the full multi_league package.
    """
    if not name:
        return fallback
    result = DB_NAME_SANITIZE_PATTERN.sub(DB_NAME_SANITIZE_REPLACEMENT, name.lower()).strip("_")
    if not result:
        return fallback
    if result[0].isdigit():
        result = "l_" + result
    return result[:63]


# Workflow timeouts (minutes)
TIMEOUTS = {
    "yahoo_quick_import": 60,
    "yahoo_full_import": 120,
    "playoff_odds": 180,
    "weekly_update": 90,
}
