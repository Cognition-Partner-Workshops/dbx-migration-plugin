#!/usr/bin/env python3
"""PostToolUse hint for the DBX migration factory.

Reads a Devin PostToolUse event ({"tool_name", "tool_input", "tool_response": {"success",
"output", "error"}}) and, when a Databricks or legacy-source call failed for an auth / scope
reason, injects the factory's response policy into the agent's context: register a D10, do not
widen permissions, do not switch identity, do not retry in a loop.
"""
from __future__ import annotations

import json
import re
import sys

_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("databricks-scope", re.compile(
        r"PERMISSION_DENIED|INSUFFICIENT_PERMISSIONS|does not have (?:USE|SELECT|MODIFY|CREATE|MANAGE|EXECUTE|OWNERSHIP)"
        r"|User does not have permission|is not owner of|Only the owner|requires (?:MANAGE|ALL PRIVILEGES)",
        re.IGNORECASE)),
    ("databricks-auth", re.compile(
        r"Invalid access token|401 Unauthorized|status 401|configuration does not support OAuth tokens"
        r"|cannot configure default credentials|default auth: |invalid_client|token (?:has )?expired|403 Forbidden|status 403",
        re.IGNORECASE)),
    ("legacy-readonly", re.compile(
        r"read[- ]only transaction|permission denied for (?:table|schema|relation|database)|ORA-01031|ORA-00942"
        r"|The user does not have (?:INSERT|UPDATE|DELETE|CREATE) access|Msg 229|Msg 262|Access denied for user",
        re.IGNORECASE)),
)

_HINTS = {
    "databricks-scope": (
        "Databricks scope failure on the migration principal. Do not request or grant wider permissions, "
        "do not switch to a personal or admin identity, and do not retry unchanged. Register a D10 in "
        ".migration/04_dependency_register.md with the exact securable and privilege from the error, fire the "
        "access request through the user, and continue with work that does not need it. See target-routing "
        "(auth for unattended sessions) and 1-migration_setup (access model)."
    ),
    "databricks-auth": (
        "Databricks authentication failure. The factory runs as the engagement migration service principal via "
        "environment OAuth M2M (DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET from named secrets); "
        "do not run `databricks auth login`, do not mint a PAT, do not paste tokens. Run the factory-doctor "
        "preflight, and if the named secrets are missing or expired register a D10 and stop the unit."
    ),
    "legacy-readonly": (
        "Legacy source refused a statement or lacks privileges. Legacy is read-only in every phase: if the "
        "statement was a write, that is a guardrail violation, not a permissions problem; if it was a read, "
        "register a D10 with the object and privilege and do not attempt to work around it."
    ),
}


def classify(text: str) -> str | None:
    for name, rx in _PATTERNS:
        if rx.search(text):
            return name
    return None


def main(stdin_text: str | None = None) -> int:
    raw = stdin_text if stdin_text is not None else sys.stdin.read()
    try:
        event = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        return 0
    if not isinstance(event, dict):
        return 0
    resp = event.get("tool_response") or {}
    if not isinstance(resp, dict):
        return 0
    text = " ".join(str(resp.get(k) or "") for k in ("output", "error"))
    if not text.strip():
        return 0
    kind = classify(text)
    if kind is None:
        return 0
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PostToolUse",
            "additionalContext": f"[dbx-migration-factory] {_HINTS[kind]}",
        }
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
