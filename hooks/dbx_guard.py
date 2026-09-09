#!/usr/bin/env python3
"""PreToolUse guard for the DBX migration factory.

Reads a Devin PreToolUse event on stdin ({"tool_name", "tool_input": {"command"}}),
locates the engagement's `.migration/allowed_targets.json`, and vetoes shell commands that
would (a) write to a Databricks catalog outside the allowlist, (b) deploy a bundle to a
production target, or (c) run anything other than reads against a legacy source.

Config file shape (`.migration/allowed_targets.json`, written at setup, committed before STOP A):

    {
      "catalogs": ["migration_cat"],                      # required; the dbx-recon CLI reads this too
      "legacy_sources": ["LEGACY_TD_DSN", "tdprod.corp"], # secret names / hosts / DSNs / profiles
      "guard_mode": "block",                              # block (default) | warn
      "forbidden_bundle_targets": ["prod", "production"]  # optional override
    }

Outside a migration workspace (no `.migration/allowed_targets.json` up the tree) the guard
approves everything. Malformed input approves (plugin hooks fail open by platform design; the
factory-doctor reports whether the hook is loaded).
"""
from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

CONFIG_REL = Path(".migration") / "allowed_targets.json"
_MAX_SCRIPT_BYTES = 4 * 1024 * 1024
DEFAULT_FORBIDDEN_BUNDLE_TARGETS = ("prod", "production")

_SEG = r"(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$-]*)"
_THREE_PART = re.compile(rf"(?<![\w`.])({_SEG})\.({_SEG})\.({_SEG})(?![\w`.])")

_WRITE_STMT = re.compile(
    r"""(?:\b(?:
        INSERT\s+(?:INTO|OVERWRITE)\b
      | UPDATE\s+(?:TOP\s*\([^)]*\)\s+)?(?!SET\b)\S+(?:\s+(?:AS\s+)?(?!SET\b|WITH\b)[\w`\[\]$]+)?(?:\s+WITH\s*\([^)]*\))?\s+SET\b
      | DELETE\s+FROM\b
      | MERGE\s+INTO\b
      | TRUNCATE\s+TABLE\b
      | CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+|EXTERNAL\s+|STREAMING\s+|MATERIALIZED\s+|LIVE\s+)*(?:TABLE|VIEW|SCHEMA|DATABASE|CATALOG|FUNCTION|PROCEDURE|VOLUME|INDEX|TRIGGER|SEQUENCE)\b
      | DROP\s+(?:TABLE|VIEW|SCHEMA|DATABASE|CATALOG|FUNCTION|PROCEDURE|VOLUME|INDEX|TRIGGER|SEQUENCE)\b
      | ALTER\s+(?:TABLE|VIEW|SCHEMA|DATABASE|CATALOG|FUNCTION|PROCEDURE|VOLUME)\b
      | REPLACE\s+TABLE\b
      | GRANT\s+.+?\bON\b
      | REVOKE\s+.+?\bON\b
      | COPY\s+INTO\b
      | (?:OPTIMIZE|VACUUM|RESTORE|REFRESH)\s+TABLE\b
    )
      # procedure calls: a qualified name, or a call sitting at the start of a SQL string/statement
      # (a bare `docker exec ...` must not match)
      | \b(?:EXEC(?:UTE)?|CALL)\s+(?!IMMEDIATE\b)(?:\[?[\w$]+\]?\.)+\[?[\w$]+
      | (?<=['";\n])\s*(?:EXEC(?:UTE)?|CALL)\s+(?!IMMEDIATE\b)[\[@`\w]
    )""",
    re.IGNORECASE | re.VERBOSE,
)
_USE_CATALOG = re.compile(rf"\bUSE\s+CATALOG\s+({_SEG})", re.IGNORECASE)
_CREATE_CATALOG = re.compile(rf"\b(?:CREATE|DROP|ALTER)\s+CATALOG\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?({_SEG})", re.IGNORECASE)
_SCHEMA_TWO_PART = re.compile(
    rf"\b(?:CREATE|DROP|ALTER)\s+(?:SCHEMA|DATABASE)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?({_SEG})\.({_SEG})(?![\w`.])",
    re.IGNORECASE,
)
_ON_CATALOG = re.compile(rf"\bON\s+CATALOG\s+({_SEG})", re.IGNORECASE)
_ON_SCHEMA = re.compile(rf"\bON\s+(?:SCHEMA|DATABASE)\s+({_SEG})\.({_SEG})(?![\w`.])", re.IGNORECASE)

# one match per bundle invocation, bounded by the shell separators so chained commands are each checked
_BUNDLE_DEPLOY = re.compile(r"\bdatabricks\s+bundle\b([^;&|\n]*?\b(?:deploy|run|destroy)\b[^;&|\n]*)", re.IGNORECASE)
_BUNDLE_TARGET = re.compile(r"(?:^|\s)(?:-t|--target)(?:=|\s+)(\S+)")
_TARGET_CATALOG_FLAG = re.compile(r"--target-catalog(?:=|\s+)(\S+)")
_SCRIPT_INPUT = re.compile(r"(?<![<>])<\s*(?!<)([^\s<>|;&]+)|(?:^|\s)@([^\s;&|]+)|(?:^|\s)(?:-f|-i|--file|--input)(?:=|\s+)([^\s;&|]+)")
_CLI_SECURABLE = re.compile(
    r"\bdatabricks\s+(grants\s+(?:update|delete)|schemas\s+(?:create|delete|update)|tables\s+delete|volumes\s+(?:create|delete)|catalogs\s+(?:create|delete|update))\s+(.*)",
    re.IGNORECASE,
)
# Position of the catalog-bearing positional argument per CLI verb (CLI signatures:
# `grants update SECURABLE_TYPE FULL_NAME`, `schemas create NAME CATALOG_NAME`, `volumes create CATALOG_NAME ...`).
_CLI_CATALOG_ARG = {"grants update": 1, "grants delete": 1, "schemas create": 1}
# a client is recognised by its basename: `databricks`, `./bin/databricks` and `/opt/x/databricks` alike
_CLIENT_PREFIX = r"(?:^|[\s;&|(])(?:[^\s;&|()<>'\"]*/)?"
_DATABRICKS_CONTEXT = re.compile(_CLIENT_PREFIX + r"(?:databricks|dbx-recon|spark-sql|dbsqlcli)\b|--target-catalog")

# Clients that only ever talk to legacy engines in a migration; any write through them is a violation.
_LEGACY_ONLY_CLIENTS = re.compile(
    _CLIENT_PREFIX + r"(?:bteq|sqlplus|sqlldr|snowsql|mload|fastload|fastexport|tbuild|tdload)\b", re.IGNORECASE
)


@dataclass
class GuardConfig:
    catalogs: list[str]
    legacy_sources: list[str] = field(default_factory=list)
    mode: str = "block"
    forbidden_bundle_targets: tuple[str, ...] = DEFAULT_FORBIDDEN_BUNDLE_TARGETS
    path: Path | None = None

    @classmethod
    def from_dict(cls, data: dict, path: Path | None = None) -> "GuardConfig":
        catalogs = data.get("catalogs")
        if not isinstance(catalogs, list) or not catalogs:
            raise ValueError("allowed_targets.json must contain a non-empty 'catalogs' list")
        legacy = data.get("legacy_sources", [])
        if not isinstance(legacy, list):
            raise ValueError("'legacy_sources' must be a list")
        mode = str(data.get("guard_mode", "block")).lower()
        if mode not in ("block", "warn"):
            raise ValueError("'guard_mode' must be 'block' or 'warn'")
        fbt = data.get("forbidden_bundle_targets", list(DEFAULT_FORBIDDEN_BUNDLE_TARGETS))
        if not isinstance(fbt, list):
            raise ValueError("'forbidden_bundle_targets' must be a list")
        return cls(
            catalogs=[_norm(c) for c in catalogs],
            legacy_sources=[str(s) for s in legacy if str(s).strip()],
            mode=mode,
            forbidden_bundle_targets=tuple(str(t).lower() for t in fbt),
            path=path,
        )


@dataclass
class Verdict:
    decision: str  # approve | block
    reason: str = ""
    violations: list[str] = field(default_factory=list)


def _norm(ident: str) -> str:
    return ident.strip().strip("`").lower()


def find_config(start: Path) -> Path | None:
    for d in [start, *start.parents]:
        candidate = d / CONFIG_REL
        if candidate.is_file():
            return candidate
    return None


def load_config(start: Path) -> GuardConfig | None:
    path = find_config(start.resolve())
    if path is None:
        return None
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"{path} must be a JSON object")
    return GuardConfig.from_dict(data, path)


# a literal handed to a dynamic-SQL executor is a statement, not data
_DYNAMIC_SQL_CALLER = re.compile(
    r"(?:\bEXEC(?:UTE)?\s+IMMEDIATE|\bsp_executesql|\bEXEC(?:UTE)?\s*\(|\.(?:execute|executemany|sql|run_query)\s*\()\s*N?\s*$",
    re.IGNORECASE,
)


def _sql_view(text: str, sql_only: bool = False) -> str:
    """The text as the write detector reads it, offsets preserved: comments blanked, and the
    contents of single-quoted SQL literals blanked so `WHERE note = 'DROP TABLE x'` is a read.

    Shell quoting is respected: a top-level '...' is an argument (usually the SQL itself) and is
    kept whole; literals are masked inside a double-quoted argument, or everywhere when the text
    is a script file (`sql_only`). A literal that feeds a dynamic-SQL executor stays visible."""
    out = list(text)
    i, n, dq = 0, len(text), False

    def blank(a: int, b: int) -> None:
        for k in range(a, b):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        c = text[i]
        if c == "\\" and not sql_only:
            i += 2
        elif text.startswith("--", i):
            j = text.find("\n", i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            j = n if j < 0 else j + 2
            blank(i, j)
            i = j
        elif c == '"' and not sql_only:
            dq = not dq
            i += 1
        elif c == "'":
            if dq or sql_only:
                j = i + 1
                while j < n:
                    if text[j] == "'":
                        if text.startswith("''", j):
                            j += 2
                            continue
                        break
                    j += 1
                if not _DYNAMIC_SQL_CALLER.search(text, max(0, i - 40), i):
                    blank(i + 1, j)
                i = j + 1
            else:
                j = text.find("'", i + 1)
                i = n if j < 0 else j + 1
        else:
            i += 1
    return "".join(out)


def _join_continuations(cmd: str) -> str:
    return re.sub(r"\\\r?\n", " ", cmd)


def _write_segments(text: str) -> list[tuple[int, str]]:
    """Each write statement, as (offset, text) from its leading keyword to the next ';' or end."""
    out = []
    for m in _WRITE_STMT.finditer(text):
        end = text.find(";", m.end())
        out.append((m.start(), text[m.start(): end if end != -1 else len(text)]))
    return out


def _script_inputs(cmd: str) -> list[str]:
    """Files a client is told to execute: `< f`, `@f`, `-f f`, `--file f`, `-i f`, `--input f`."""
    files = []
    for m in _SCRIPT_INPUT.finditer(cmd):
        f = next(g for g in m.groups() if g).strip("'\"")
        if f and not f.startswith("-") and f != "<":
            files.append(f)
    return files


def _inline_scripts(cmd: str, root: Path, cfg: GuardConfig) -> tuple[str, list[str]]:
    """Command text plus the contents of every referenced script. Scripts the guard cannot inspect in
    full (unreadable, or larger than `_MAX_SCRIPT_BYTES`) are returned as unreadable."""
    unreadable: list[str] = []
    if not (_DATABRICKS_CONTEXT.search(cmd) or _LEGACY_ONLY_CLIENTS.search(cmd) or _mentions_legacy(cmd, cfg)):
        return cmd, unreadable
    parts = [cmd]
    for f in _script_inputs(cmd):
        p = Path(os.path.expandvars(os.path.expanduser(f)))
        if not p.is_absolute():
            p = root / p
        try:
            with p.open(errors="replace") as fh:
                body = fh.read(_MAX_SCRIPT_BYTES + 1)
        except OSError:
            unreadable.append(f)
            continue
        if len(body) > _MAX_SCRIPT_BYTES:
            unreadable.append(f)
            continue
        parts.append("\n;\n" + _sql_view(body, sql_only=True))
    return "\n".join(parts), unreadable


def _catalogs_in_segment(seg: str) -> set[str]:
    """Catalog of the statement's *write target*: the first qualified securable after the verb.

    Later identifiers in the same statement are sources (CTAS `AS SELECT FROM prod...`, MERGE
    `USING`), and reading from outside the allowlist is legitimate.
    """
    earliest: tuple[int, str] | None = None
    for rx in (_THREE_PART, _SCHEMA_TWO_PART, _ON_SCHEMA, _CREATE_CATALOG, _ON_CATALOG):
        m = rx.search(seg)
        if m and (earliest is None or m.start() < earliest[0]):
            earliest = (m.start(), _norm(m.group(1)))
    return {earliest[1]} if earliest else set()


def _check_databricks_writes(cmd: str, cfg: GuardConfig) -> list[str]:
    violations: list[str] = []
    allowed = set(cfg.catalogs)
    text = _sql_view(cmd)
    in_dbx = bool(_DATABRICKS_CONTEXT.search(cmd))

    use_cats = [(m.start(), _norm(m.group(1))) for m in _USE_CATALOG.finditer(text)]

    for offset, seg in _write_segments(text):
        # the catalog in force is the last USE CATALOG *before* this statement, not the first in the command
        use_cat = next((c for pos, c in reversed(use_cats) if pos < offset), None)
        cats = _catalogs_in_segment(seg)
        head = seg.strip().split("\n", 1)[0][:80]
        if cats:
            bad = sorted(c for c in cats if c not in allowed)
            if bad:
                violations.append(f"write to catalog(s) {bad} outside allowlist {sorted(allowed)}: `{head}`")
        elif use_cat is not None:
            if use_cat not in allowed:
                violations.append(f"write under USE CATALOG {use_cat!r} outside allowlist {sorted(allowed)}: `{head}`")
        elif in_dbx:
            violations.append(
                f"write with unresolvable catalog (not three-part qualified, no USE CATALOG) in a Databricks command: `{head}`"
            )

    for m in _TARGET_CATALOG_FLAG.finditer(cmd):
        cat = _norm(m.group(1).strip("'\""))
        if cat not in allowed:
            violations.append(f"--target-catalog {cat!r} outside allowlist {sorted(allowed)}")

    for m in _CLI_SECURABLE.finditer(cmd):
        verb = re.sub(r"\s+", " ", m.group(1).lower())
        args = [a for a in re.split(r"[\s;&|]+", m.group(2)) if a and not a.startswith("-")]
        idx = _CLI_CATALOG_ARG.get(verb, 0)
        if len(args) <= idx:
            continue
        name = args[idx].strip("'\"")
        first = _norm(name.split(".")[0])
        if re.fullmatch(r"[a-z_][a-z0-9_$-]*", first) and first not in allowed:
            violations.append(f"CLI mutation of securable {name!r} outside allowlist {sorted(allowed)}")

    for m in _BUNDLE_DEPLOY.finditer(cmd):
        tm = _BUNDLE_TARGET.search(m.group(1))
        if tm and tm.group(1).strip("'\"").lower() in cfg.forbidden_bundle_targets:
            violations.append(f"bundle deploy/run to forbidden target {tm.group(1)!r}; production deploys happen only at STOP E under the cutover principal")
    return violations


def _mentions_legacy(cmd: str, cfg: GuardConfig) -> list[str]:
    hits = []
    for tok in cfg.legacy_sources:
        if re.search(rf"(?<![\w-]){re.escape(tok)}(?![\w-])", cmd, re.IGNORECASE):
            hits.append(tok)
    return hits


def _check_uninspectable_scripts(cmd: str, cfg: GuardConfig, unreadable: list[str]) -> list[str]:
    """A script the guard cannot read in full is a script it cannot clear: fail closed."""
    if not unreadable:
        return []
    if _mentions_legacy(cmd, cfg) or _LEGACY_ONLY_CLIENTS.search(cmd):
        return [f"legacy client fed script(s) {unreadable} that the guard cannot read in full (missing, unreadable or over "
                f"{_MAX_SCRIPT_BYTES // (1024 * 1024)} MiB); inline the SQL or split it so it can be inspected "
                "(legacy is read-only in every phase)"]
    if _DATABRICKS_CONTEXT.search(cmd):
        return [f"Databricks client fed script(s) {unreadable} that the guard cannot read in full (missing, unreadable or over "
                f"{_MAX_SCRIPT_BYTES // (1024 * 1024)} MiB); inline the SQL or split it so its write targets can be checked"]
    return []


def _check_legacy_writes(cmd: str, cfg: GuardConfig) -> list[str]:
    hits = _mentions_legacy(cmd, cfg)
    legacy_client = bool(_LEGACY_ONLY_CLIENTS.search(cmd))
    text = _sql_view(cmd)
    segs = _write_segments(text)
    if not segs:
        return []
    heads = [s.strip().split("\n", 1)[0][:80] for _, s in segs]
    if hits:
        return [f"non-read statement against legacy source {hits}: `{heads[0]}` (legacy is read-only in every phase)"]
    if legacy_client:
        return [f"non-read statement through a legacy-only client: `{heads[0]}` (legacy is read-only in every phase)"]
    return []


def evaluate(command: str, cfg: GuardConfig, root: Path | None = None) -> Verdict:
    command = _join_continuations(command)
    full, unreadable = _inline_scripts(command, root or _project_root(), cfg)
    violations = (_check_uninspectable_scripts(full, cfg, unreadable) + _check_databricks_writes(full, cfg)
                  + _check_legacy_writes(full, cfg))
    if not violations:
        return Verdict("approve")
    reason = (
        "dbx-migration-factory guard: "
        + "; ".join(violations)
        + ". Fix the command or, if the target is legitimate, add it to .migration/allowed_targets.json via a "
          "recorded decision (.migration/06_decisions.md); never work around the guard."
    )
    if cfg.mode == "warn":
        return Verdict("approve", "WARN (guard_mode=warn): " + reason, violations)
    return Verdict("block", reason, violations)


def _project_root() -> Path:
    for var in ("CLAUDE_PROJECT_DIR", "DEVIN_PROJECT_DIR"):
        v = os.environ.get(var)
        if v:
            return Path(v)
    return Path.cwd()


def main(stdin_text: str | None = None) -> int:
    raw = stdin_text if stdin_text is not None else sys.stdin.read()
    try:
        event = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        return 0
    if not isinstance(event, dict):
        return 0
    tool_input = event.get("tool_input") or {}
    command = tool_input.get("command") if isinstance(tool_input, dict) else None
    if not isinstance(command, str) or not command.strip():
        return 0
    try:
        cfg = load_config(_project_root())
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        # A broken allowlist is itself a violation of setup step 7: refuse writes rather than guess.
        print(json.dumps({"decision": "block", "reason": f"dbx-migration-factory guard: cannot read {CONFIG_REL}: {exc}"}))
        print(f"dbx-migration-factory guard: cannot read {CONFIG_REL}: {exc}", file=sys.stderr)
        return 2
    if cfg is None:
        return 0
    verdict = evaluate(command, cfg)
    if verdict.decision == "block":
        print(json.dumps({"decision": "block", "reason": verdict.reason}))
        print(verdict.reason, file=sys.stderr)
        return 2
    if verdict.reason:
        print(json.dumps({"decision": "approve", "reason": verdict.reason}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
