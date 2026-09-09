#!/usr/bin/env python3
"""Static enumeration + lineage round-trip for the oracle-plsql skill (SKILL.md sections 1-2).

Reads Oracle SQL / PL-SQL text only (no engine), builds the census (section 1 repo-file rules)
and the read/write/call edge list (section 2 rules), classifies every edge FACT or INFERRED,
and exits non-zero if any edge ends up UNVERIFIABLE. UNVERIFIABLE is emitted by the completeness
pass: every lineage-bearing keyword (FROM/JOIN/INSERT/UPDATE/DELETE/MERGE/TRUNCATE, EXECUTE
IMMEDIATE, OPEN FOR, DBMS_SQL, qualified calls) that no section-2 rule consumed becomes an
UNVERIFIABLE edge, so unsupported syntax fails the run instead of silently dropping lineage.

    python3 skills/oracle-plsql/examples/round_trip.py [--albion PATH] [--report PATH]
    python3 skills/oracle-plsql/examples/round_trip.py --selftest   # negative fixtures must fail

Default inputs: examples/fixture/*.sql and, when present, the Albion estate package at
../../../albion-insurance-data-estate/api_legacy/plsql/pkg_policy_inquiry.sql (read-only).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "fixture"
ALBION_DEFAULT = HERE.parents[3] / "albion-insurance-data-estate" / "api_legacy" / "plsql" / "pkg_policy_inquiry.sql"

# a SQL*Plus directive may be indented; `SET col = ...` (UPDATE) and `EXECUTE IMMEDIATE` (PL/SQL) are not directives
SQLPLUS_DIRECTIVE = re.compile(
    r"^\s*(SET\s+(PAGESIZE|LINESIZE|DEFINE|FEEDBACK|VERIFY|HEADING|ECHO|SERVEROUTPUT|TERMOUT|TRIMSPOOL|TIMING|AUTOCOMMIT|SQLBLANKLINES)"
    r"\b(?!\s*=)|(?:SPO(?:OL)?|WHENEVER|DEFINE|COLUMN|PROMPT|ACCEPT|TTITLE|BREAK|COMPUTE|HO(?:ST)?)\b|EXEC(?:UTE)?\s+(?!IMMEDIATE\b)"
    r"|@@?\S|STA(?:RT)?\s+(?!WITH\b)|[!$])", re.I)
# SQL*Plus lines that are lineage of the script itself (SKILL.md section 2, SQL*Plus row): includes, spool, OS shell
INCLUDE_RE = re.compile(r"^\s*(@@|@|STA(?:RT)?\s+(?!WITH\b))\s*(\S+)", re.I | re.M)
SPOOL_RE = re.compile(r"^\s*SPO(?:OL)?\s+(\S+)", re.I | re.M)
HOST_RE = re.compile(r"^\s*(?:HO(?:ST)?\b|[!$])(.*)$", re.I | re.M)
IDENT = r"[A-Za-z_][A-Za-z0-9_$#]*"
QNAME = rf"(?:{IDENT}\.)?{IDENT}"
KEYWORDS = {
    "DUAL", "SELECT", "WHERE", "AND", "OR", "ON", "SET", "VALUES", "USING", "WHEN", "THEN", "ELSE", "END",
    "NULL", "TABLE", "LOOP", "INTO", "IN", "AS", "IS", "NOT", "EXISTS", "CASE", "UPDATE", "DELETE", "INSERT",
    "MERGE", "MATCHED", "SYSDATE", "SYSTIMESTAMP", "LEVEL", "ROWNUM", "USER", "CONNECT", "START",
}
# Oracle-supplied packages whose calls carry no table lineage (pure / session / output helpers)
PURE_PACKAGES = {
    "DBMS_OUTPUT", "DBMS_UTILITY", "DBMS_ASSERT", "DBMS_LOB", "DBMS_RANDOM", "DBMS_CRYPTO", "UTL_RAW", "STANDARD",
    "DBMS_STANDARD", "DBMS_APPLICATION_INFO", "DBMS_SESSION", "DBMS_LOCK", "DBMS_METADATA", "DBMS_STATS", "SYS",
}
# handled by dedicated census/lineage rules (never a call edge)
RULED_PACKAGES = {"DBMS_RLS", "DBMS_REDACT", "DBMS_SCHEDULER", "DBMS_MVIEW"}
# side effects the static model does not describe: a call is UNVERIFIABLE lineage until a human classifies it
EXTERNAL_EFFECT_PACKAGES = {
    "DBMS_SQL", "UTL_FILE", "UTL_HTTP", "UTL_SMTP", "UTL_MAIL", "UTL_TCP", "DBMS_AQ", "DBMS_AQADM", "DBMS_PIPE",
    "DBMS_ALERT", "DBMS_XMLGEN", "DBMS_XSLPROCESSOR", "DBMS_JOB", "DBMS_FILE_TRANSFER", "DBMS_DATAPUMP",
}
COLLECTION_METHODS = {"COUNT", "EXTEND", "DELETE", "TRIM", "EXISTS", "FIRST", "LAST", "NEXT", "PRIOR", "LIMIT"}
PROCEDURAL_CLASSES = {"PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE BODY", "PACKAGE PROCEDURE", "PACKAGE FUNCTION", "TRIGGER"}


@dataclass
class Node:
    key: str
    cls: str
    file: str
    status: str = "enumerated"      # enumerated | not-in-census | external
    signals: dict = field(default_factory=dict)


@dataclass
class Edge:
    src: str
    dst: str
    kind: str                       # reads | writes | consumes-sequence | calls | schedules | defines-on | alias-of | replication
    evidence: str                   # FACT | INFERRED | UNVERIFIABLE
    risk: str = ""
    detail: str = ""
    ops: str = ""                   # writes only: DML events fired on dst (INSERT|UPDATE|DELETE), "" when unknown


QQUOTE_CLOSER = {"[": "]", "(": ")", "{": "}", "<": ">"}


def literal_end(text: str, i: int) -> int:
    """If text[i] opens a literal, return the index just past it, else return i. Literals: '...' with '' escape,
    q'X...X' with ANY single-character delimiter (paired brackets close with their partner), "quoted identifier"."""
    n = len(text)
    ch = text[i]
    if ch in "qQ" and i + 2 < n and text[i + 1] == "'" and not text[i + 2].isspace() \
            and (i == 0 or not re.match(r"[\w$#]", text[i - 1])):
        closer = QQUOTE_CLOSER.get(text[i + 2], text[i + 2]) + "'"
        j = text.find(closer, i + 3)
        return n if j < 0 else j + 2
    if ch == "'":
        j = i + 1
        while j < n:
            if text[j] == "'":
                if j + 1 < n and text[j + 1] == "'":
                    j += 2
                    continue
                return j + 1
            j += 1
        return n
    if ch == '"':
        j = text.find('"', i + 1)
        return n if j < 0 else j + 1
    return i


def strip_comments(text: str) -> str:
    """Lexical comment removal. Literals (see literal_end) are opaque, so `--` or `/*` inside one never swallows
    the SQL that follows it; comments become spaces, newlines are kept."""
    out: list[str] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch == "-" and text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
            continue
        if ch == "/" and text.startswith("/*", i):
            j = text.find("*/", i + 2)
            j = n if j < 0 else j + 2
            out.append(re.sub(r"[^\n]", " ", text[i:j]))
            i = j
            continue
        j = literal_end(text, i)
        if j > i:
            out.append(text[i:j])
            i = j
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def normalize_qquotes(text: str) -> str:
    """Rewrite every q'X...X' literal as an ordinary '...' literal ('' escapes) so all later rules see one string
    syntax; ordinary strings and quoted identifiers are copied, never rescanned."""
    out: list[str] = []
    i, n = 0, len(text)
    while i < n:
        j = literal_end(text, i)
        if j == i:
            out.append(text[i])
            i += 1
            continue
        lit = text[i:j]
        if lit[0] in "qQ":
            body = lit[3:-2] if len(lit) >= 5 and lit.endswith("'") else lit[3:]
            out.append("'" + body.replace("'", "''") + "'")
        else:
            out.append(lit)
        i = j
    return "".join(out)


def unquote(value: str) -> str:
    """Value of a '...' literal argument ('' -> '); non-literal expressions are returned as written."""
    value = value.strip()
    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        return value[1:-1].replace("''", "'")
    return value


def call_args(text: str, open_paren: int) -> str:
    """Argument text of the call whose '(' is at `open_paren`, up to its balanced ')' (literals opaque), so a ');'
    inside an action string or a nested TO_TIMESTAMP_TZ(...) never ends the call early."""
    depth, i, n = 0, open_paren, len(text)
    while i < n:
        j = literal_end(text, i)
        if j > i:
            i = j
            continue
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return text[open_paren + 1:i]
        i += 1
    return text[open_paren + 1:]


def statement_end(text: str, start: int) -> int:
    """Index just past the first ';' at or after `start` that is outside a literal (comments already stripped)."""
    i, n = start, len(text)
    while i < n:
        j = literal_end(text, i)
        if j > i:
            i = j
            continue
        if text[i] == ";":
            return i + 1
        i += 1
    return n


def skip_balanced(text: str, open_paren: int) -> int:
    """Index just past the ')' that balances the '(' at `open_paren` (literals opaque)."""
    return min(len(text), open_paren + 1 + len(call_args(text, open_paren)) + 1)


def top_level_cut(text: str, stop: re.Pattern) -> str:
    """`text` up to the first match of `stop` at parenthesis depth 0, or up to an unmatched ')' (literals opaque)."""
    depth, i, n = 0, 0, len(text)
    while i < n:
        j = literal_end(text, i)
        if j > i:
            i = j
            continue
        ch = text[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return text[:i]
        elif depth == 0:
            m = stop.match(text, i)
            if m and (not m.group(0)[0].isalpha() or i == 0 or not re.match(r"[\w$#]", text[i - 1])):
                return text[:i]
        i += 1
    return text


def split_top_level(text: str) -> list[str]:
    """Split on commas at parenthesis depth 0 (literals opaque)."""
    parts: list[str] = []
    depth, start, i, n = 0, 0, 0, len(text)
    while i < n:
        j = literal_end(text, i)
        if j > i:
            i = j
            continue
        ch = text[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
        i += 1
    parts.append(text[start:])
    return parts


def call_params(text: str, positional: list[str]) -> dict[str, str]:
    """{lower-case formal: actual text} of a PL/SQL call: positional actuals (PL/SQL requires them first) are mapped
    onto `positional`, named actuals (`formal => value`) onto their formal; surplus positionals are dropped. Split on
    top-level commas only: an actual may be a full expression such as TO_TIMESTAMP_TZ('...', '...')."""
    out: dict[str, str] = {}
    for i, p in enumerate(split_top_level(text)):
        m = re.match(r"\s*(\w+)\s*=>\s*(.*?)\s*$", p, re.S)
        if m:
            out[m.group(1).lower()] = m.group(2)
        elif i < len(positional) and p.strip():
            out[positional[i]] = p.strip()
    return out


def literal_param(params: dict[str, str], name: str) -> str | None:
    """Value of a string-literal actual, None when absent or not a literal (a variable, an expression, NULL)."""
    v = params.get(name, "").strip()
    return unquote(v) if len(v) >= 2 and v[0] == "'" and v[-1] == "'" else None


def norm(name: str, default_owner: str) -> str:
    name = name.strip().strip('"').upper()
    if "." not in name:
        return f"{default_owner}.{name}"
    return name


class Estate:
    def __init__(self) -> None:
        self.nodes: dict[str, Node] = {}
        self.edges: list[Edge] = []
        self.synonyms: dict[str, str] = {}      # OWNER.SYN -> target key (or key@LINK)
        self.public_synonyms: dict[str, str] = {}

    def add(self, key: str, cls: str, file: str, **signals) -> Node:
        n = self.nodes.get(key)
        if n is None:
            n = Node(key, cls, file, signals=signals)
            self.nodes[key] = n
        else:
            n.signals.update(signals)
        return n

    def resolve(self, raw: str, default_owner: str) -> tuple[str, str, str]:
        """Return (key, evidence, risk) for a statically named object."""
        raw = raw.strip().strip('"')
        if "@" in raw:
            obj, link = raw.split("@", 1)
            key = f"{norm(obj, default_owner)}@{link.upper()}"
            self.add(key, "EXTERNAL_TABLE", "-", ).status = "external"
            return key, "INFERRED", "external-db-link"
        key = norm(raw, default_owner)
        _owner, bare = key.split(".", 1)
        # Oracle resolution order for an UNQUALIFIED name: own-schema object, private synonym, public synonym.
        # A qualified OWNER.NAME never falls through to a same-named public synonym. A private synonym shares the
        # schema namespace with tables, so its census row is an alias row, never the edge target.
        syn = self.synonyms.get(key) or (
            self.public_synonyms.get(bare) if "." not in raw and key not in self.nodes else None)
        if key in self.nodes and not syn:
            return key, "FACT", ""
        if syn:
            if "@" in syn:
                self.add(syn, "EXTERNAL_TABLE", "-").status = "external"
                return syn, "INFERRED", "external-db-link"
            if syn in self.nodes:
                return syn, "FACT", ""
            self.add(syn, "TABLE", "-").status = "not-in-census"
            return syn, "FACT", "dangling-synonym"
        # fixed name, not enumerated: the edge is a fact of the text; the node is an inventory gap
        self.add(key, "TABLE", "-").status = "not-in-census"
        return key, "FACT", ""

    def edge(self, src: str, raw_dst: str, kind: str, default_owner: str, detail: str = "", ops: str = "") -> None:
        dst, evidence, risk = self.resolve(raw_dst, default_owner)
        self.edges.append(Edge(src, dst, kind, evidence, risk, detail, ops))


# --------------------------------------------------------------------------- census (section 1)

CREATE_RE = re.compile(
    rf"CREATE\s+(?:OR\s+REPLACE\s+)?"
    rf"(?:(?:GLOBAL|PRIVATE)\s+TEMPORARY\s+|PUBLIC\s+|(?:NON)?EDITIONABLE\s+|UNIQUE\s+|BITMAP\s+|(?:NO\s+)?FORCE\s+|SHARED\s+)*"
    rf"(MATERIALIZED\s+VIEW\s+LOG\s+ON|MATERIALIZED\s+VIEW|PACKAGE\s+BODY|DATABASE\s+LINK|SEQUENCE|TABLE|INDEX|VIEW|"
    rf"PROCEDURE|FUNCTION|PACKAGE|TRIGGER|SYNONYM|ROLE)\s+({QNAME})",
    re.I,
)
PROCEDURAL_CREATE = {"PACKAGE", "PACKAGE BODY", "PROCEDURE", "FUNCTION", "TRIGGER"}
PUBLIC_SYN_RE = re.compile(rf"CREATE\s+(?:OR\s+REPLACE\s+)?PUBLIC\s+SYNONYM\s+({IDENT})\s+FOR\s+({QNAME}(?:@{IDENT})?)", re.I)
PRIV_SYN_RE = re.compile(rf"CREATE\s+(?:OR\s+REPLACE\s+)?SYNONYM\s+({QNAME})\s+FOR\s+({QNAME}(?:@{IDENT})?)", re.I)
MEMBER_RE = re.compile(rf"^\s*(PROCEDURE|FUNCTION)\s+({IDENT})", re.I | re.M)
MEMBER_HEAD_STOP_RE = re.compile(r";|\b(?:IS|AS)\b", re.I)
# call heads only: the argument list runs to the balanced ')' (call_args), never to the first ');' in the text
SCHED_RE = re.compile(r"DBMS_SCHEDULER\.CREATE_(JOB|PROGRAM)\s*\(", re.I)
RLS_RE = re.compile(r"DBMS_RLS\.ADD_POLICY\s*\(", re.I)
REDACT_RE = re.compile(r"DBMS_REDACT\.ADD_POLICY\s*\(", re.I)
# formal parameter order of the ruled calls (Oracle PL/SQL Packages and Types Reference), for positional actuals.
# CREATE_JOB is overloaded on its 2nd formal: an inline job (job_type, job_action, ...) or a program-based one
# (program_name, ...); the value tells them apart
SCHED_PROGRAM_FORMALS = ["program_name", "program_type", "program_action", "number_of_arguments", "enabled", "comments"]
SCHED_JOB_INLINE_FORMALS = ["job_name", "job_type", "job_action", "number_of_arguments", "start_date", "repeat_interval",
                            "end_date", "job_class", "enabled", "auto_drop", "comments"]
SCHED_JOB_PROGRAM_FORMALS = ["job_name", "program_name", "start_date", "repeat_interval", "end_date", "job_class", "enabled",
                             "auto_drop", "comments"]
SCHED_JOB_TYPES = {"PLSQL_BLOCK", "STORED_PROCEDURE", "EXECUTABLE", "CHAIN", "EXTERNAL_SCRIPT", "SQL_SCRIPT", "BACKUP_SCRIPT"}
RLS_FORMALS = ["object_schema", "object_name", "policy_name", "function_schema", "policy_function", "statement_types",
               "update_check", "enable", "static_policy", "policy_type", "long_predicate", "sec_relevant_cols"]
REDACT_FORMALS = ["object_schema", "object_name", "policy_name", "policy_description", "column_name", "column_description",
                  "function_type", "function_parameters", "expression", "enable"]
# top-level DML that makes the non-procedural remainder of a file a DML SCRIPT unit (GRANT ... DELETE ON and
# ON DELETE CASCADE do not qualify)
LOOSE_DML_RE = re.compile(
    rf"\b(?:INSERT\s+INTO|MERGE\s+INTO|TRUNCATE\s+TABLE)\s+{QNAME}|\bUPDATE\s+{QNAME}\s+SET\b|\bDELETE\s+(?:FROM\s+)?{QNAME}\s*(?:WHERE\b|;)",
    re.I)
# a non-DML remainder is still a script unit when it queries or calls something: a standalone query / CALL, or a
# statement of an anonymous block that is a routine call (`poladm.pkg.run(...)`, `prc_log_event(...)`, `run_all;`)
LOOSE_QUERY_RE = re.compile(rf"\bSELECT\b[\s\S]*?\bFROM\b|\bCALL\s+{QNAME}", re.I)
CALL_STMT_RE = re.compile(rf"(?:;|\bBEGIN\b|\bTHEN\b|\bELSE\b|\bLOOP\b)\s*({IDENT}(?:\.{IDENT}){{0,2}})\s*(?:\(|;)", re.I)
# `poladm.run_all;` / `run_all;`: a procedure call statement without an argument list
PARENLESS_CALL_RE = re.compile(rf"(?:;|\bBEGIN\b|\bTHEN\b|\bELSE\b|\bLOOP\b)\s*({IDENT}(?:\.{IDENT}){{0,2}})\s*;", re.I)
STATEMENT_WORDS = KEYWORDS | {
    "COMMIT", "ROLLBACK", "RETURN", "EXIT", "RAISE", "CONTINUE", "GOTO", "BEGIN", "DECLARE", "EXCEPTION", "IF", "ELSIF",
    "WHILE", "FOR", "FORALL", "OPEN", "CLOSE", "FETCH", "EXECUTE", "SAVEPOINT", "LOCK", "PIPE", "RAISE_APPLICATION_ERROR",
}
CTAS_RE = re.compile(r"\bAS\s*\(?\s*(?:SELECT|WITH)\b", re.I)
INDEX_ON_RE = re.compile(rf"\s+ON\s+({QNAME})\s*\(", re.I)
GRANT_RE = re.compile(rf"GRANT\s+([A-Z ,()_]+?)\s+ON\s+({QNAME})\s+TO\s+({IDENT})", re.I)
ROLE_GRANT_RE = re.compile(rf"GRANT\s+({IDENT})\s+TO\s+({IDENT})\s*;", re.I)


def unit_spans(text: str) -> list[tuple[int, int]]:
    """(start, end) offsets of the SQL*Plus '/'-terminated units of a file; a PL/SQL unit ends at its '/'."""
    spans, start = [], 0
    for m in re.finditer(r"^\s*/\s*$", text, re.M):
        spans.append((start, m.start()))
        start = m.end()
    spans.append((start, len(text)))
    return spans


def uncovered(text: str, covered: list[tuple[int, int]]) -> str:
    """The file text outside `covered` spans (procedural units and CREATE statements that own their own lineage)."""
    out, pos = [], 0
    for s, e in sorted(covered):
        if s > pos:
            out.append(text[pos:s])
        pos = max(pos, e)
    out.append(text[pos:])
    return "".join(out)


def package_members(unit_text: str) -> list[tuple[str, str, int, int]]:
    """(NAME, PROCEDURE|FUNCTION, start, end) of the top-level members of a package spec or body. A declaration
    (`PROCEDURE p(...);`: spec entries, body forward declarations) ends at its ';'; an implementation (`... IS|AS ...`)
    ends at its `END p;` and owns its nested local subprograms. An implementation closed by a bare `END;` ends at the
    next member header (or the unit end) instead."""
    out: list[tuple[str, str, int, int]] = []
    pos = 0
    for mm in MEMBER_RE.finditer(unit_text):
        if mm.start() < pos:
            continue
        name = mm.group(2)
        after = mm.end() + len(top_level_cut(unit_text[mm.end():], MEMBER_HEAD_STOP_RE))
        if after >= len(unit_text) or unit_text[after] == ";":
            end = after + 1
        else:
            em = re.compile(rf"\bEND\s+{re.escape(name)}\s*;", re.I).search(unit_text, after)
            nxt = MEMBER_RE.search(unit_text, after)
            end = em.end() if em else (nxt.start() if nxt else len(unit_text))
        out.append((name.upper(), mm.group(1).upper(), mm.start(), end))
        pos = end
    return out


def loose_unit_class(loose_static: str) -> str:
    """Census class of a file's non-procedural remainder (literals blanked), '' when it carries no lineage: DML SCRIPT
    for top-level DML, PLSQL SCRIPT for an anonymous block that queries or calls a routine, SQL SCRIPT for standalone
    queries / CALL statements. Calls into the pure and ruled Oracle packages (DBMS_OUTPUT, DBMS_SCHEDULER, ...) and
    control statements (NULL; COMMIT; RETURN;) do not make a unit."""
    if LOOSE_DML_RE.search(loose_static):
        return "DML SCRIPT"
    calls = [m.group(1).upper() for m in CALL_STMT_RE.finditer(loose_static)]
    calls = [c for c in calls if c.split(".")[0] not in PURE_PACKAGES | RULED_PACKAGES and c not in STATEMENT_WORDS]
    if not calls and not any(LOOSE_QUERY_RE.search(stmt) for stmt in loose_static.split(";")):
        return ""
    return "PLSQL SCRIPT" if re.search(r"\bBEGIN\b", loose_static, re.I) else "SQL SCRIPT"


def census_file(est: Estate, path: Path, default_owner: str, sqlplus_units: list, proc_units: list, deferred: list) -> None:
    """`deferred` collects (src, raw_dst, kind, owner) edges whose target may be enumerated by a later file (grants,
    index base tables, synonym targets); `run` resolves them after the whole census."""
    raw = path.read_text()
    text = normalize_qquotes(strip_comments(raw))
    # same offsets as `text`, string literals blanked: object headers are never matched inside a literal
    blanked = STRING_LIT_RE.sub(lambda m: "'" + " " * (len(m.group(0)) - 2) + "'", text)
    spans = unit_spans(text)
    covered: list[tuple[int, int]] = []       # spans whose lineage belongs to a named unit, not to the file's loose DML
    fname = path.name
    is_sqlplus = any(SQLPLUS_DIRECTIVE.match(ln) for ln in raw.splitlines())
    if is_sqlplus:
        key = f"{default_owner}.{path.stem.upper()}"
        est.add(key, "SQLPLUS_SCRIPT", fname,
                lines=len(raw.splitlines()), substitution_vars=len(set(re.findall(r"&&?(\w+)", raw))))
        sqlplus_units.append((key, text, path))
    for m in PUBLIC_SYN_RE.finditer(blanked):
        est.public_synonyms[m.group(1).upper()] = norm(m.group(2).split("@")[0], default_owner) + (
            "@" + m.group(2).split("@")[1].upper() if "@" in m.group(2) else "")
        est.add(f"PUBLIC.{m.group(1).upper()}", "PUBLIC SYNONYM", fname)
        deferred.append((f"PUBLIC.{m.group(1).upper()}", m.group(2), "alias-of", default_owner))
    for m in PRIV_SYN_RE.finditer(blanked):
        if re.search(r"PUBLIC\s+SYNONYM\s+" + re.escape(m.group(1)), blanked, re.I):
            continue
        est.synonyms[norm(m.group(1), default_owner)] = norm(m.group(2).split("@")[0], default_owner) + (
            "@" + m.group(2).split("@")[1].upper() if "@" in m.group(2) else "")
        est.add(norm(m.group(1), default_owner), "SYNONYM", fname)
        deferred.append((norm(m.group(1), default_owner), m.group(2), "alias-of", default_owner))
    for m in CREATE_RE.finditer(blanked):
        cls = re.sub(r"\s+", " ", m.group(1).upper())
        name = m.group(2)
        stmt_end = statement_end(text, m.end())
        if cls not in PROCEDURAL_CREATE:
            covered.append((m.start(), stmt_end))     # DDL owns its text (ON DELETE CASCADE, CTAS, ... are not script DML)
        if cls in ("SYNONYM",):
            continue
        if cls == "MATERIALIZED VIEW LOG ON":
            key = f"MLOG$_{norm(name, default_owner)}"
            est.add(key, "MATERIALIZED VIEW LOG", fname)
            est.edges.append(Edge(norm(name, default_owner), key, "defines-on", "FACT"))
            continue
        if cls == "ROLE":
            est.add(f"ROLE.{name.upper()}", "ROLE", fname)
            continue
        key = norm(name, default_owner)
        node = est.add(key, cls, fname)
        if cls == "TABLE":
            body = text[m.end():stmt_end]
            cols = re.findall(r"^\s*(\w+)\s+(NUMBER(?!\s*\()|NUMBER\s*\([^)]*\)|DATE|CHAR\s*\(\d+\)|TIMESTAMP[^,]*|RAW\s*\(\d+\)|CLOB|BINARY_DOUBLE|INTERVAL[^,]*)",
                              body, re.I | re.M)
            node.signals["type_traps"] = sorted({re.sub(r"\s+", " ", c[1].upper()) for c in cols if
                                                 re.match(r"NUMBER$|DATE|CHAR|TIMESTAMP|RAW|CLOB|BINARY|INTERVAL", c[1], re.I)})
            node.signals["constraints"] = len(re.findall(r"CONSTRAINT\s+\w+", body, re.I))
            node.signals["temporary"] = bool(re.search(r"TEMPORARY\s+TABLE\s+" + re.escape(name), blanked, re.I))
            if CTAS_RE.search(blanked[m.end():stmt_end]):
                node.signals["ctas"] = True                 # CREATE TABLE ... AS SELECT: the subquery's reads are lineage
                proc_units.append((key, cls, text[m.start():stmt_end]))
        if cls == "INDEX":
            im = INDEX_ON_RE.match(blanked, m.end())
            if im:
                deferred.append((key, im.group(1), "defines-on", default_owner))
            else:
                unverifiable(est, key, default_owner, "defines-on", "unparsed-index-target", text[m.start():stmt_end])
        if cls in PROCEDURAL_CREATE:
            # a PL/SQL unit ends at its SQL*Plus '/' (or at the next object header when a file has no '/')
            end = next((e for s, e in spans if s <= m.start() < e), len(text))
            nxt = CREATE_RE.search(blanked, m.end())
            if nxt and nxt.start() < end:
                end = nxt.start()
            unit_text = text[m.start():end]
            covered.append((m.start(), end))
            node.signals["lines"] = unit_text.count("\n")
            if cls in ("PACKAGE", "PACKAGE BODY"):
                # each member's body is its own lineage unit (callers resolve to OWNER.PKG.MEMBER); package-level
                # declarations (constants, cursors, state) and the initialization block stay on the package node
                members = package_members(unit_text)
                for mname, mcls, s, e in members:
                    est.add(f"{key}.{mname}", f"PACKAGE {mcls}", fname)
                    proc_units.append((f"{key}.{mname}", f"PACKAGE {mcls}", unit_text[s:e]))
                proc_units.append((key, cls, uncovered(unit_text, [(s, e) for _, _, s, e in members])))
            else:
                proc_units.append((key, cls, unit_text))
        if cls in ("VIEW", "MATERIALIZED VIEW"):
            unit_text = text[m.start():stmt_end]         # lexical end: a ';' inside a projected literal is text
            proc_units.append((key, cls, unit_text))
            if cls == "MATERIALIZED VIEW":
                node.signals["refresh"] = " ".join(re.findall(r"REFRESH\s+(\w+)\s+ON\s+(\w+)", unit_text, re.I)[0]) if re.search(
                    r"REFRESH\s+\w+\s+ON", unit_text, re.I) else "?"
    script_key = f"{default_owner}.{path.stem.upper()}"

    def unparsed_call(kind: str, risk: str, call_text: str) -> None:
        # the call is real lineage the static model cannot name: the file becomes the script unit that carries it
        est.add(script_key, "SQLPLUS_SCRIPT" if is_sqlplus else "PLSQL SCRIPT", fname)
        unverifiable(est, script_key, default_owner, kind, risk, call_text)

    for m in SCHED_RE.finditer(text):
        covered.append((m.start(), statement_end(text, skip_balanced(text, m.end() - 1))))
        call_text = text[m.start():skip_balanced(text, m.end() - 1)]
        is_job = m.group(1).upper() == "JOB"
        if is_job:
            actuals = split_top_level(call_args(text, m.end() - 1))
            second = unquote(actuals[1]).upper() if len(actuals) > 1 and "=>" not in actuals[1] else ""
            formals = SCHED_JOB_INLINE_FORMALS if second in SCHED_JOB_TYPES else SCHED_JOB_PROGRAM_FORMALS
        else:
            formals = SCHED_PROGRAM_FORMALS
        params = call_params(call_args(text, m.end() - 1), formals)
        name = literal_param(params, "job_name" if is_job else "program_name")
        if name is None:
            unparsed_call("schedules", "scheduler-name-not-literal", call_text)
            continue
        cls = "SCHEDULER " + m.group(1).upper()
        key = norm(name, default_owner)
        est.add(key, cls, fname, repeat_interval=unquote(params.get("repeat_interval", "")),
                start_date=unquote(params.get("start_date", "")))
        program = literal_param(params, "program_name") if is_job else None
        if is_job and program is None and "program_name" in params:
            unverifiable(est, key, default_owner, "schedules", "scheduler-program-not-literal", call_text)
        elif program:
            est.edges.append(Edge(key, norm(program, default_owner), "schedules", "FACT"))
        action_formal = "job_action" if is_job else "program_action"
        action = literal_param(params, action_formal)
        if action:
            proc_units.append((key, cls, action))
        elif action_formal in params:
            unverifiable(est, key, default_owner, "calls", "scheduler-action-not-literal", call_text)
    for m in RLS_RE.finditer(text):
        covered.append((m.start(), statement_end(text, skip_balanced(text, m.end() - 1))))
        call_text = text[m.start():skip_balanced(text, m.end() - 1)]
        params = call_params(call_args(text, m.end() - 1), RLS_FORMALS)
        # object_schema / function_schema default to the current schema when omitted or NULL
        schema = literal_param(params, "object_schema") or default_owner
        fschema = literal_param(params, "function_schema") or default_owner
        obj, pol, fn = (literal_param(params, k) for k in ("object_name", "policy_name", "policy_function"))
        if obj is None or pol is None or fn is None:
            unparsed_call("defines-on", "policy-call-not-literal", call_text)
            continue
        key = f"{schema}.{pol}".upper()
        est.add(key, "VPD POLICY", fname)
        est.edges.append(Edge(key, f"{schema}.{obj}".upper(), "defines-on", "FACT"))
        est.edges.append(Edge(key, f"{fschema}.{fn}".upper(), "calls", "FACT"))
    for m in REDACT_RE.finditer(text):
        covered.append((m.start(), statement_end(text, skip_balanced(text, m.end() - 1))))
        call_text = text[m.start():skip_balanced(text, m.end() - 1)]
        params = call_params(call_args(text, m.end() - 1), REDACT_FORMALS)
        schema = literal_param(params, "object_schema") or default_owner
        obj, pol = literal_param(params, "object_name"), literal_param(params, "policy_name")
        if obj is None or pol is None:
            unparsed_call("defines-on", "policy-call-not-literal", call_text)
            continue
        key = f"{schema}.{pol}".upper()
        est.add(key, "REDACTION POLICY", fname, column=literal_param(params, "column_name"))
        est.edges.append(Edge(key, f"{schema}.{obj}".upper(), "defines-on", "FACT"))
    for m in GRANT_RE.finditer(blanked):
        priv, obj, grantee = m.groups()
        key = f"GRANT.{grantee.upper()}.{norm(obj, default_owner)}.{re.sub(r'[^A-Z]', '', priv.upper().split('(')[0])}"
        est.add(key, "GRANT", fname, public=grantee.upper() == "PUBLIC", column_level="(" in priv)
        deferred.append((key, obj, "defines-on", default_owner))
    for m in ROLE_GRANT_RE.finditer(blanked):
        est.add(f"ROLEGRANT.{m.group(2).upper()}.{m.group(1).upper()}", "ROLE MEMBERSHIP", fname)
    # the remainder outside every procedural unit / DDL statement / ruled DBMS_* call (loose MERGE files, anonymous
    # blocks, seed rows after a CREATE TABLE, report queries, call-only orchestration scripts) is one script unit named
    # after the file; a SQL*Plus script already scans its whole text
    loose = uncovered(text, covered)
    loose_cls = "" if is_sqlplus else loose_unit_class(STRING_LIT_RE.sub("''", loose))
    if loose_cls:
        key = f"{default_owner}.{path.stem.upper()}"
        est.add(key, loose_cls, fname)
        proc_units.append((key, loose_cls, loose))


# --------------------------------------------------------------------------- lineage (section 2)

READ_RE = re.compile(rf"\b(?:FROM|JOIN)\s+({QNAME}(?:@{IDENT})?)\b(?!\s*\()", re.I)
# names declared by a WITH clause: `WITH n [(cols)] AS (` and every following `), n [(cols)] AS (` (recursive ones included)
CTE_RE = re.compile(rf"(?:\bWITH\s+(?:RECURSIVE\s+)?|\)\s*,\s*)({IDENT})\s*(?:\([^)]*\))?\s+AS\s*\(", re.I)
WRITE_RE = re.compile(rf"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|DELETE|MERGE\s+INTO|TRUNCATE\s+TABLE)\s+({QNAME})", re.I)
SEQ_RE = re.compile(rf"\b({QNAME})\.(?:NEXTVAL|CURRVAL)\b", re.I)
TRIGGER_ON_RE = re.compile(rf"\bON\s+({QNAME})\s+(?:FOR\s+EACH\s+ROW|REFERENCING|WHEN|DECLARE|BEGIN)", re.I | re.S)
CALL_RE = re.compile(rf"\b({IDENT}\.{IDENT}(?:\.{IDENT})?)\s*\(", re.I)
UNQUAL_CALL_RE = re.compile(rf"(?<![.\w])({IDENT})\s*\(", re.I)
EXEC_IMM_RE = re.compile(r"EXECUTE\s+IMMEDIATE\s+('(?:[^']|'')*'|[A-Za-z_]\w*\b(?!\s*[(.]))(\s*\|\|)?", re.I)
EXEC_IMM_ANY_RE = re.compile(r"EXECUTE\s+IMMEDIATE\b", re.I)
OPEN_FOR_RE = re.compile(rf"OPEN\s+{IDENT}\s+FOR\s+(?!SELECT\b|WITH\b)({IDENT}|'|\()", re.I)
OPEN_FOR_VAR_RE = re.compile(rf"OPEN\s+{IDENT}\s+FOR\s+({IDENT})\s*(?:;|USING\b)", re.I)
DYN_ASSIGN_RE = re.compile(rf"({IDENT})\s*:=\s*('(?:[^']|'')*')\s*\|\|", re.I)
# `v := 'whole literal';` and a declaration default `v VARCHAR2(n) [CHAR|BYTE] := | DEFAULT 'whole literal';`
STRING_TYPE = r"(?:VARCHAR2|NVARCHAR2|CHAR|CLOB|LONG|STRING)\s*(?:\(\s*\d+\s*(?:BYTE|CHAR)?\s*\))?"
LIT_ASSIGN_RE = re.compile(
    rf"(?<![.\w])({IDENT})\s*(?:{STRING_TYPE}\s*(?:\bDEFAULT\s+|:=\s*)|:=\s*)('(?:[^']|'')*')\s*;", re.I)
ANY_ASSIGN_RE = re.compile(rf"(?<![.\w])({IDENT})\s*(?:{STRING_TYPE}\s*(?::=|\bDEFAULT\b)|:=)", re.I)
MVIEW_REFRESH_RE = re.compile(r"DBMS_MVIEW\.REFRESH\s*\(\s*(?:list\s*=>\s*)?'([^']+)'", re.I)
MVIEW_REFRESH_ANY_RE = re.compile(r"DBMS_MVIEW\.REFRESH\s*\(", re.I)
SUBST_IDENT_RE = re.compile(r"(?:FROM|JOIN|INTO|UPDATE)\s+&&?\w+", re.I)
STRING_LIT_RE = re.compile(r"'(?:[^']|'')*'")
# completeness pass: every lineage-bearing keyword and what may legitimately follow it
LINEAGE_KW_RE = re.compile(
    r"\b(INSERT\s+INTO|MERGE\s+INTO|TRUNCATE\s+TABLE|DELETE\s+FROM|DELETE|UPDATE|FROM|JOIN)\s*(\S{0,40})", re.I)
NEXT_IDENT_RE = re.compile(rf"^{QNAME}(?:@{IDENT})?\b")


# DML events on an edge: `INSERT|UPDATE(COL_A,COL_B)|DELETE`; `UPDATE` without a column list means any / unknown columns
TRIGGER_EVENTS_RE = re.compile(rf"\b(INSERT|DELETE|UPDATE(?:\s+OF\s+((?:{IDENT}\s*,\s*)*{IDENT}))?)\b", re.I)
OPS_TOKEN_RE = re.compile(r"(\w+)(?:\(([^)]*)\))?")
SET_STOP_RE = re.compile(r"WHERE\b|RETURNING\b|LOG\s+ERRORS\b|WHEN\s+(?:NOT\s+)?MATCHED\b", re.I)


def parse_ops(ops: str) -> dict[str, set[str] | None]:
    """{'UPDATE': {'A', 'B'}, 'INSERT': None} for 'INSERT|UPDATE(A,B)'; None = every column / unknown."""
    out: dict[str, set[str] | None] = {}
    for tok in filter(None, ops.split("|")):
        m = OPS_TOKEN_RE.fullmatch(tok)
        if not m:
            continue
        ev = m.group(1).upper()
        cols = {c.strip().upper() for c in m.group(2).split(",") if c.strip()} if m.group(2) is not None else None
        out[ev] = None if cols is None or (ev in out and out[ev] is None) else (out.get(ev) or set()) | cols
    return out


def format_ops(parsed: dict[str, set[str] | None]) -> str:
    return "|".join(ev + (f"({','.join(sorted(cols))})" if cols is not None else "") for ev, cols in sorted(parsed.items()))


def merge_ops(a: str, b: str) -> str:
    """Union of two event strings: columns union per event, an unbounded UPDATE absorbs a bounded one."""
    pa, pb = parse_ops(a), parse_ops(b)
    for ev, cols in pb.items():
        if ev not in pa:
            pa[ev] = cols
        elif pa[ev] is None or cols is None:
            pa[ev] = None
        else:
            pa[ev] = pa[ev] | cols
    return format_ops(pa)


def trigger_events(header: str) -> str:
    """'INSERT|UPDATE(STATUS)' for `BEFORE INSERT OR UPDATE OF status ON t`; '' when the timing clause is not
    recognised."""
    tm = re.search(r"\b(?:BEFORE|AFTER|INSTEAD\s+OF)\b(.*?)\bON\b", header, re.I | re.S)
    if not tm:
        return ""
    ops = ""
    for ev, cols in TRIGGER_EVENTS_RE.findall(tm.group(1)):
        ev = ev.split()[0].upper()
        ops = merge_ops(ops, ev + (f"({','.join(c.strip().upper() for c in cols.split(','))})" if cols else ""))
    return ops


def set_columns(clause: str) -> str:
    """'(A,B)' for the columns assigned by a SET list (`col = expr`, `t.col = expr`, `(a, b) = (subquery)`); '' when an
    item has another shape (SET ROW = rec, ...), which callers treat as "any column"."""
    cols: set[str] = set()
    for part in split_top_level(top_level_cut(clause, SET_STOP_RE)):
        m = re.match(rf"\s*(?:{IDENT}\.)?({IDENT})\s*=", part)
        gm = re.match(rf"\s*\(\s*((?:{IDENT}\s*,\s*)*{IDENT})\s*\)\s*=", part)
        if m and m.group(1).upper() != "ROW":
            cols.add(m.group(1).upper())
        elif gm:
            cols |= {c.strip().upper() for c in gm.group(1).split(",")}
        else:
            return ""
    return f"({','.join(sorted(cols))})" if cols else ""


def dml_ops(verb: str, statement: str) -> str:
    """DML events a statement fires on its target, with the assigned columns of an UPDATE: `UPDATE t SET a = 1` ->
    'UPDATE(A)'; MERGE -> its INSERT / UPDATE(cols) / DELETE (`DELETE WHERE`) branches; TRUNCATE is DDL and fires no
    row trigger, so it carries the pseudo-event TRUNCATE. A SET list the parser cannot read yields a bare UPDATE."""
    verb = re.sub(r"\s+", " ", verb.strip())
    if verb.startswith("MERGE"):
        ops = ""
        if re.search(r"\bNOT\s+MATCHED\s+THEN\s+INSERT\b", statement, re.I):
            ops = merge_ops(ops, "INSERT")
        um = re.search(r"\bMATCHED\s+THEN\s+UPDATE\s+SET\b", statement, re.I)
        if um:
            ops = merge_ops(ops, "UPDATE" + set_columns(statement[um.end():]))
        if re.search(r"\bDELETE\s+WHERE\b", statement, re.I):
            ops = merge_ops(ops, "DELETE")
        return ops or "INSERT|UPDATE"
    if verb == "UPDATE":
        sm = re.search(r"\bSET\b", statement, re.I)
        return "UPDATE" + (set_columns(statement[sm.end():]) if sm else "")
    return verb.split(" ")[0]


def ops_fire(trigger_ops: str, writer_ops: str) -> tuple[bool, str]:
    """Does a write with `writer_ops` fire a trigger declared for `trigger_ops`? (fires, risk): an `UPDATE OF cols`
    trigger fires for an UPDATE whose SET list names one of its columns; an UPDATE with an unreadable SET list fires
    with risk 'update-columns-unknown'. Unknown shapes on either side fire (conservative)."""
    t, w = parse_ops(trigger_ops), parse_ops(writer_ops)
    if not t or not w:
        return True, ""
    unknown = False
    for ev, tcols in t.items():
        if ev not in w:
            continue
        wcols = w[ev]
        if ev != "UPDATE" or tcols is None or (wcols is not None and tcols & wcols):
            return True, ""
        if wcols is None:
            unknown = True
    return (True, "update-columns-unknown") if unknown else (False, "")


FROM_KW_RE = re.compile(r"\bFROM\b", re.I)
EXPR_FROM_WORDS = {"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TIMEZONE_REGION",
                   "TIMEZONE_ABBR", "LEADING", "TRAILING", "BOTH"}
# words that end a FROM list item's alias position (an Oracle table alias is never one of these)
FROM_LIST_STOP = KEYWORDS | {
    "GROUP", "HAVING", "ORDER", "UNION", "MINUS", "INTERSECT", "FOR", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "CROSS",
    "NATURAL", "OUTER", "PIVOT", "UNPIVOT", "MODEL", "WITH", "VERSIONS", "FETCH", "OFFSET", "RETURNING", "BETWEEN", "LIKE",
    "PARTITION", "SUBPARTITION", "SAMPLE", "BULK", "LIMIT", "WHERE", "FROM", "ON", "USING",
}
JOIN_HEAD_RE = re.compile(
    r"(?:(?:LEFT|RIGHT|FULL|INNER|CROSS)\s+(?:OUTER\s+)?|NATURAL\s+(?:(?:LEFT|RIGHT|FULL|INNER)\s+)?(?:OUTER\s+)?)?JOIN\b", re.I)
JOIN_COND_STOP_RE = re.compile(
    r",|WHERE\b|GROUP\b|HAVING\b|ORDER\b|START\b|CONNECT\b|UNION\b|MINUS\b|INTERSECT\b|FOR\b|MODEL\b|PIVOT\b|UNPIVOT\b|"
    r"FETCH\b|OFFSET\b|LEFT\b|RIGHT\b|FULL\b|INNER\b|CROSS\b|NATURAL\b|JOIN\b", re.I)
TABLE_EXPR_RE = re.compile(r"(?:TABLE|LATERAL)\s*\(", re.I)
QUOTED_QNAME_RE = re.compile(rf'"[^"]*"(?:\s*\.\s*(?:"[^"]*"|{IDENT}))*')
SOURCE_NAME_RE = re.compile(rf"{QNAME}(?:@{IDENT})?")
ALIAS_MOD_RE = re.compile(r"(?:PARTITION|SUBPARTITION|SAMPLE)\s*(?:BLOCK\s*)?\(", re.I)


def from_list_tail(stmt: str, pos: int) -> list[tuple[str, str]]:
    """The items after the first one of the FROM list starting at `pos` (just past FROM), as (kind, text) with kind
    'name' | 'function' | 'quoted' | 'subst' | 'subquery' | 'other'. The first item is consumed by READ_RE / the
    completeness pass; later items are separated from it by top-level commas, optionally through ANSI join clauses
    (`FROM a JOIN b ON a.x = b.x, c`). Stops at ')', ';' or a clause keyword."""
    n = len(stmt)

    def skip_ws(i: int) -> int:
        return i + len(stmt[i:]) - len(stmt[i:].lstrip())

    def source(i: int) -> tuple[str, str, int] | None:
        """One table expression at i: (kind, text, index after it) or None when nothing parseable starts here."""
        if i >= n:
            return None
        ch = stmt[i]
        if ch == "(":
            return "subquery", "", skip_balanced(stmt, i)
        if ch == '"':
            qm = QUOTED_QNAME_RE.match(stmt, i)
            return "quoted", qm.group(0), qm.end()
        if ch == "&":
            sm = re.match(r"&&?\w+", stmt[i:])
            if sm:
                return "subst", sm.group(0), i + sm.end()
            return None
        tm = TABLE_EXPR_RE.match(stmt, i)
        if tm:
            return "subquery", "", skip_balanced(stmt, tm.end() - 1)
        nm = SOURCE_NAME_RE.match(stmt, i)
        if not nm:
            return None
        fm = re.match(r"\s*\(", stmt[nm.end():])
        if fm:
            return "function", nm.group(0), skip_balanced(stmt, nm.end() + fm.end() - 1)
        return "name", nm.group(0), nm.end()

    def alias(i: int) -> int:
        """Skip PARTITION (...) / SAMPLE (...) modifiers and an optional alias."""
        while True:
            i = skip_ws(i)
            pm = ALIAS_MOD_RE.match(stmt, i)
            if pm:
                i = skip_balanced(stmt, pm.end() - 1)
                continue
            am = re.match(IDENT, stmt[i:])
            if am and am.group(0).upper() not in FROM_LIST_STOP:
                i += am.end()
                continue
            return i

    items: list[tuple[str, str]] = []
    i, first = pos, True
    while True:
        i = skip_ws(i)
        src = source(i)
        if src is None:
            if not first and i < n:
                items.append(("other", stmt[i:i + 20].split("\n")[0]))
            break
        kind, item, i = src
        if not first:
            items.append((kind, item))
        first = False
        while True:                                 # after a source: alias, then `,` | ANSI join | end of list
            i = alias(i)
            if i < n and stmt[i] == ",":
                i += 1
                break
            jm = JOIN_HEAD_RE.match(stmt, i)
            if not jm:
                return items
            joined = source(skip_ws(jm.end()))         # the joined source itself is READ_RE's (JOIN x)
            if joined is None:
                return items
            i = alias(joined[2])
            um = re.match(r"USING\s*\(", stmt[i:], re.I)
            if um:
                i = skip_balanced(stmt, i + um.end() - 1)
            elif re.match(r"ON\b", stmt[i:], re.I):
                i += 2 + len(top_level_cut(stmt[i + 2:], JOIN_COND_STOP_RE))
    return items


def preceding_word(text: str, pos: int) -> str:
    m = re.search(r"(\w+)\W*$", text[max(0, pos - 40):pos])
    return m.group(1).upper() if m else ""


def unverifiable(est: Estate, key: str, owner: str, kind: str, risk: str, snippet: str) -> None:
    est.edges.append(Edge(key, f"{owner}.<?>", kind, "UNVERIFIABLE", risk, re.sub(r"\s+", " ", snippet.strip())[:80]))


def completeness_pass(est: Estate, key: str, owner: str, static: str) -> None:
    """Flag lineage-bearing keywords whose operand no section-2 rule can consume."""
    for m in LINEAGE_KW_RE.finditer(static):
        kw = re.sub(r"\s+", " ", m.group(1).upper())
        rest = static[m.end(1):].lstrip()
        nxt = rest[:60]
        prev = preceding_word(static, m.start())
        if kw == "UPDATE" and (prev == "FOR" or prev == "OR" or prev == "THEN" or prev == "BEFORE" or prev == "AFTER"
                               or re.match(r"(OF|ON|SET|NOWAIT|SKIP)\b", nxt, re.I) or re.match(r"[;)]|$", nxt)):
            continue                                   # FOR UPDATE, INSERT OR UPDATE ON, WHEN MATCHED THEN UPDATE SET, UPDATE OF col
        if kw == "DELETE" and (prev in ("THEN", "OR", "BEFORE", "AFTER") or re.match(r"(WHERE|ON)\b", nxt, re.I) or re.match(r"[;)]|$", nxt)):
            continue                                   # WHEN MATCHED THEN DELETE, l_tab.DELETE, trigger event list
        if kw == "FROM" and prev in ("EXTRACT", "DISTINCT", "TRIM", "LEADING", "TRAILING", "BOTH", "YEAR", "MONTH", "DAY",
                                     "HOUR", "MINUTE", "SECOND", "TIMEZONE_HOUR"):
            continue                                   # EXTRACT(x FROM d), TRIM(c FROM s)
        if kw == "FROM" and re.match(r"(DUAL|TABLE\s*\()", nxt, re.I):
            continue
        if kw in ("FROM", "JOIN") and nxt.startswith("("):
            continue                                   # inline view / subquery: its own FROM is scanned
        if nxt.startswith("&"):
            continue                                   # substitution: SUBST_IDENT_RE -> INFERRED
        if NEXT_IDENT_RE.match(nxt):
            im = NEXT_IDENT_RE.match(nxt)
            if kw in ("FROM", "JOIN") and re.match(r"\s*\(", nxt[im.end():]):
                # table function FROM fn(...): only resolvable when fn is in the census
                fn = norm(im.group(0), owner)
                if fn in est.nodes and est.nodes[fn].cls in PROCEDURAL_CLASSES:
                    continue
                unverifiable(est, key, owner, "reads", "table-function-not-in-census", m.group(0) + nxt[:20])
                continue
            continue                                   # plain identifier: READ_RE / WRITE_RE consumed it
        if nxt.startswith('"'):
            unverifiable(est, key, owner, "writes" if kw not in ("FROM", "JOIN") else "reads", "quoted-identifier", m.group(0) + nxt[:20])
            continue
        if nxt.startswith(("(", ":")):
            unverifiable(est, key, owner, "writes", "updatable-inline-view-or-bind-target", m.group(0) + nxt[:20])
            continue
        unverifiable(est, key, owner, "writes" if kw not in ("FROM", "JOIN") else "reads", "unparsed-operand", m.group(0) + nxt[:20])


def sqlplus_directive_lineage(est: Estate, key: str, text: str, path: Path, owner: str) -> None:
    """SQL*Plus lines that are lineage of the script itself: `@file` / `@@file` / `START file` include another script
    (`@@` resolves against the calling script's directory; `@` and `START` against the working directory then SQLPATH,
    which is statically unknown, so the script's directory stands in for both), `SPOOL file` writes a report file
    (`SPOOL OFF|OUT` closes it), `HOST` / `!` / `$` run an OS command."""
    for m in INCLUDE_RE.finditer(text):
        target = m.group(2).strip("'\"")
        if "&" in target:
            est.edges.append(Edge(key, f"{owner}.<&include>", "includes", "INFERRED", "substitution-in-identifier", m.group(0).strip()))
            continue
        rel = Path(target if Path(target).suffix else target + ".sql")
        resolved = path.parent / rel                      # `@@`: the caller's directory; `@` / START: cwd/SQLPATH stand-in
        dst = f"{owner}.{rel.stem.upper()}"
        how = "@@ (caller-relative)" if m.group(1) == "@@" else "@ / START (cwd, then SQLPATH)"
        if resolved.is_file():
            est.add(dst, "SQL FILE", resolved.name)         # a plain DDL/DML file has object rows but no script row yet
            est.edges.append(Edge(key, dst, "includes", "FACT", "", how))
        else:
            est.add(dst, "SQL FILE", "-").status = "not-in-census"
            est.edges.append(Edge(key, dst, "includes", "INFERRED", "missing-include", f"{how}: {target} not in repo"))
    for m in SPOOL_RE.finditer(text):
        target = m.group(1).strip("'\"")
        if target.upper() in ("OFF", "OUT"):
            continue
        if "&" in target:
            est.edges.append(Edge(key, f"{owner}.<&spool>", "writes", "INFERRED", "substitution-in-identifier", m.group(0).strip()))
            continue
        dst = f"FILE.{target.upper()}"
        est.add(dst, "SPOOL FILE", "-").status = "external"
        est.edges.append(Edge(key, dst, "writes", "FACT", "", "SPOOL target"))
    for m in HOST_RE.finditer(text):
        est.edges.append(Edge(key, "OS.<shell>", "calls", "INFERRED", "os-shell", m.group(0).strip()[:80]))


def lineage_unit(est: Estate, key: str, cls: str, text: str, default_owner: str) -> None:
    owner = key.split(".")[0] if "." in key else default_owner
    # dynamic SQL first (on the raw text, so string-literal statements are visible)
    dyn_vars = {m.group(1).upper(): m.group(2) for m in DYN_ASSIGN_RE.finditer(text)}
    assigned_n = Counter(m.group(1).upper() for m in ANY_ASSIGN_RE.finditer(text))
    assigned = set(assigned_n)
    # a variable whose EVERY assignment is one complete literal holds static SQL: parse each literal as such; a literal
    # assignment followed by any other assignment (`v := v || ...`, `v := build()`) is only a literal prefix -> INFERRED
    whole_lits: dict[str, list[str]] = {}
    for m in LIT_ASSIGN_RE.finditer(text):
        whole_lits.setdefault(m.group(1).upper(), []).append(m.group(2))
    for var, lits in list(whole_lits.items()):
        if len(lits) != assigned_n[var] or var in dyn_vars:
            dyn_vars.setdefault(var, lits[0])
            del whole_lits[var]
    exec_imm_seen = 0
    for m in EXEC_IMM_RE.finditer(text):
        exec_imm_seen += 1
        arg = m.group(1)
        if arg.startswith("'") and not m.group(2):
            lineage_unit(est, key, cls, arg.strip("'").replace("''", "'"), default_owner)  # literal: parse as static
        elif arg.startswith("'"):
            unverifiable(est, key, owner, "writes", "dynamic-sql-expression", m.group(0))   # 'lit' || expr: shape unknown
        elif arg.upper() in whole_lits:
            for lit in whole_lits[arg.upper()]:
                lineage_unit(est, key, cls, lit.strip("'").replace("''", "'"), default_owner)
        elif arg.upper() in dyn_vars:
            prefix = dyn_vars[arg.upper()]
            pm = re.search(rf"(INSERT\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO|FROM)\s+({QNAME})?", prefix, re.I)
            est.edges.append(Edge(key, f"{owner}.<{arg.upper()}>", "writes" if pm and pm.group(1).upper() != "FROM" else "reads",
                                  "INFERRED", "dynamic-sql", f"literal prefix {prefix.strip()!r}"))
        elif arg.upper() in assigned:
            unverifiable(est, key, owner, "writes", "dynamic-sql-no-literal-prefix", m.group(0))
        else:
            unverifiable(est, key, owner, "writes", "dynamic-sql-unassigned-variable", m.group(0))
    if len(EXEC_IMM_ANY_RE.findall(text)) != exec_imm_seen:
        unverifiable(est, key, owner, "writes", "dynamic-sql-expression", "EXECUTE IMMEDIATE <non-literal, non-variable>")
    for m in OPEN_FOR_RE.finditer(text):
        vm = OPEN_FOR_VAR_RE.match(text, m.start())
        lit = re.compile(r"OPEN\s+\w+\s+FOR\s+('(?:[^']|'')*')\s*(;|USING\b)", re.I).match(text, m.start())
        if vm and vm.group(1).upper() in whole_lits:
            for lit in whole_lits[vm.group(1).upper()]:
                lineage_unit(est, key, cls, lit.strip("'").replace("''", "'"), default_owner)
        elif vm and vm.group(1).upper() in dyn_vars.keys() | assigned:
            est.edges.append(Edge(key, f"{owner}.<{vm.group(1).upper()}>", "reads", "INFERRED", "dynamic-sql", "OPEN ... FOR <variable>"))
        elif lit:
            lineage_unit(est, key, cls, lit.group(1).strip("'").replace("''", "'"), default_owner)
        else:
            unverifiable(est, key, owner, "reads", "dynamic-cursor-expression", m.group(0))
    for m in MVIEW_REFRESH_ANY_RE.finditer(text):
        if not MVIEW_REFRESH_RE.match(text, m.start()):
            unverifiable(est, key, owner, "calls", "mview-refresh-non-literal", text[m.start():m.start() + 60])
    if SUBST_IDENT_RE.search(text):
        est.edges.append(Edge(key, f"{owner}.<&var>", "reads", "INFERRED", "substitution-in-identifier"))
    # VPD predicate functions build SQL text from SYS_CONTEXT: the tables inside the string are INFERRED reads
    if cls.endswith("FUNCTION") and re.search(r"RETURN\s+'", text, re.I) and re.search(r"SYS_CONTEXT", text, re.I):
        for lit in STRING_LIT_RE.findall(text):
            for rm in READ_RE.finditer(lit):
                dst, _, _ = est.resolve(rm.group(1), owner)
                est.edges.append(Edge(key, dst, "reads", "INFERRED", "dynamic-predicate", "VPD predicate string"))
    # static SQL: work on text with string literals blanked so literals never look like identifiers
    static = STRING_LIT_RE.sub("''", text)
    if cls == "TRIGGER":
        tm = TRIGGER_ON_RE.search(static)
        if tm:
            events = trigger_events(static[:tm.end()])
            est.nodes[key].signals["events"] = events
            est.edge(key, tm.group(1), "defines-on", owner, ops=events)
            est.edge(key, tm.group(1), "writes", owner, ":NEW in-flight row", ops=events)
    for stmt in static.split(";"):                      # CTE names are statement-local: scope them per statement
        ctes = {m.group(1).upper() for m in CTE_RE.finditer(stmt)}
        for m in READ_RE.finditer(stmt):
            name = m.group(1)
            if name.split(".")[-1].upper() in KEYWORDS or name.upper().startswith("TABLE("):
                continue
            if "." not in name and name.upper() in ctes:
                continue                                 # a WITH-clause alias, not a physical object (its body is scanned)
            if preceding_word(stmt, m.start()) == "DELETE":
                continue                                 # DELETE FROM t: a write (WRITE_RE), not a query source
            if preceding_word(stmt, m.start()) in EXPR_FROM_WORDS:
                continue                                 # EXTRACT(YEAR FROM col), TRIM(LEADING x FROM col): a column
            est.edge(key, name, "reads", owner)
        for fm in FROM_KW_RE.finditer(stmt):             # `FROM a, b c, (subquery) s, d`: every comma-joined source
            for kind, item in from_list_tail(stmt, fm.end()):
                if kind == "name":
                    if item.split(".")[-1].upper() in KEYWORDS or ("." not in item and item.upper() in ctes):
                        continue
                    est.edge(key, item, "reads", owner)
                elif kind == "function":
                    fn = norm(item, owner)
                    if not (fn in est.nodes and est.nodes[fn].cls in PROCEDURAL_CLASSES):
                        unverifiable(est, key, owner, "reads", "table-function-not-in-census", ", " + item)
                elif kind == "quoted":
                    unverifiable(est, key, owner, "reads", "quoted-identifier", ", " + item)
                elif kind == "subst":
                    est.edges.append(Edge(key, f"{owner}.<&var>", "reads", "INFERRED", "substitution-in-identifier"))
                elif kind == "other":
                    unverifiable(est, key, owner, "reads", "unparsed-operand", ", " + item)
    for m in WRITE_RE.finditer(static):
        if m.group(1).split(".")[-1].upper() in KEYWORDS | {"OF", "FROM"}:
            continue  # `UPDATE ON t`, `UPDATE OF col`, `UPDATE SET` inside MERGE are not writes
        if preceding_word(static, m.start()) in ("FOR", "THEN", "OR", "BEFORE", "AFTER") or m.group(1).upper().endswith(".DELETE"):
            continue  # FOR UPDATE t?, trigger event lists, collection.DELETE
        verb = m.group(0)[:m.start(1) - m.start()].upper()
        est.edge(key, m.group(1), "writes", owner, ops=dml_ops(verb, static[m.start():statement_end(static, m.end())]))
    for m in SEQ_RE.finditer(static):
        est.edge(key, m.group(1), "consumes-sequence", owner)
    for m in MVIEW_REFRESH_RE.finditer(text):
        for mv in m.group(1).split(","):
            est.edge(key, mv.strip(), "calls", owner, "DBMS_MVIEW.REFRESH")
    for m in CALL_RE.finditer(static):
        callee = m.group(1).upper()
        head, member = callee.split(".")[0], callee.split(".")[-1]
        if head in PURE_PACKAGES or head in RULED_PACKAGES or member in COLLECTION_METHODS:
            continue
        if head in EXTERNAL_EFFECT_PACKAGES:
            unverifiable(est, key, owner, "calls", "external-side-effect-package", callee)
            continue
        cand = callee if callee in est.nodes else None
        if cand is None and callee.count(".") == 2 and ".".join(callee.split(".")[:2]) in est.nodes:
            cand = callee  # package member not declared separately
        if cand is None and callee.count(".") == 1 and f"{owner}.{callee}" in est.nodes:
            cand = f"{owner}.{callee}"
        if cand is None:
            if callee.count(".") == 2 or any(k.startswith(head + ".") for k in est.nodes):
                # SCHEMA.PROC / SCHEMA.PKG.MEMBER naming a schema we know: the call is a fact, the callee an inventory gap
                est.add(".".join(callee.split(".")[:2]), "PROCEDURE", "-").status = "not-in-census"
                est.edges.append(Edge(key, callee, "calls", "FACT", "", "callee not in census"))
            else:
                # X.Y(...) where X is neither a known schema, a census package, nor a record variable we can see
                unverifiable(est, key, owner, "calls", "unresolved-qualified-call", callee)
            continue
        if cand != key and not cand.startswith(key + ".") and \
                est.nodes[cand.rsplit(".", 1)[0] if cand not in est.nodes else cand].cls not in ("TABLE", "VIEW", "SEQUENCE", "MATERIALIZED VIEW"):
            est.edges.append(Edge(key, cand, "calls", "FACT"))
    # an unqualified name resolves to a member of the enclosing package first (a sibling from inside a member, an own
    # member from the package-level code), then to a same-schema routine
    pkg = ""
    if key.count(".") == 2:
        pkg = key.rsplit(".", 1)[0]
    elif est.nodes.get(key) and est.nodes[key].cls in ("PACKAGE", "PACKAGE BODY"):
        pkg = key
    for m in UNQUAL_CALL_RE.finditer(static):
        cands = ([f"{pkg}.{m.group(1).upper()}"] if pkg else []) + [f"{owner}.{m.group(1).upper()}"]
        cand = next((c for c in cands if c in est.nodes and est.nodes[c].cls in PROCEDURAL_CLASSES), None)
        if cand and cand != key and not key.startswith(cand + "."):
            est.edges.append(Edge(key, cand, "calls", "FACT", "", "unqualified call resolved against the census"))
    for m in PARENLESS_CALL_RE.finditer(static):
        callee = m.group(1).upper()
        if callee in STATEMENT_WORDS or callee.split(".")[0] in PURE_PACKAGES | RULED_PACKAGES:
            continue
        cands = [callee, f"{owner}.{callee}"] + ([f"{pkg}.{callee}"] if pkg else [])
        cand = next((c for c in cands if c in est.nodes and est.nodes[c].cls in PROCEDURAL_CLASSES), None)
        if cand and cand != key and not key.startswith(cand + "."):
            est.edges.append(Edge(key, cand, "calls", "FACT", "", "call statement without argument list"))
    completeness_pass(est, key, owner, static)


def run(fixture_dir: Path, albion: Path | None) -> dict:
    est = Estate()
    sqlplus_units: list = []
    proc_units: list = []
    deferred: list = []
    files = sorted(fixture_dir.glob("*.sql"))
    for p in files:
        census_file(est, p, "POLADM", sqlplus_units, proc_units, deferred)
    albion_units: list = []
    if albion and albion.exists():
        census_file(est, albion, "ODS", [], albion_units, deferred)
    for key, obj, kind, owner in deferred:  # resolved after the whole census so cross-file targets land on enumerated nodes
        est.edge(key, obj, kind, owner)
    # lineage
    for key, cls, text in proc_units + albion_units:
        lineage_unit(est, key, cls, text, key.split(".")[0])
    for key, text, path in sqlplus_units:
        lineage_unit(est, key, "SQLPLUS_SCRIPT", text, key.split(".")[0])
        sqlplus_directive_lineage(est, key, text, path, key.split(".")[0])
    if albion_units:
        # documented replication feed (architecture_overview.md / package header comment): GoldenGate copy of Teradata STG_POLICY_360
        est.add("TERADATA.STG_POLICY_360", "EXTERNAL_TABLE", "docs").status = "external"
        est.edges.append(Edge("TERADATA.STG_POLICY_360", "ODS.ODS_POLICY_360", "replication", "INFERRED", "freshness",
                              "GoldenGate nightly copy, up to 26h stale (architecture_overview.md)"))
    dedupe_edges(est)
    trigger_fan_out(est)
    dedupe_edges(est)
    return {"nodes": est.nodes, "edges": est.edges, "files": [p.name for p in files], "albion": bool(albion_units)}


def dedupe_edges(est: Estate) -> None:
    """One edge per (src, dst, kind, evidence, risk); the DML events of merged write edges are unioned."""
    keep: dict[tuple, Edge] = {}
    for e in est.edges:
        k = (e.src, e.dst, e.kind, e.evidence, e.risk)
        if k in keep:
            if e.ops and keep[k].ops != e.ops:
                keep[k].ops = merge_ops(keep[k].ops, e.ops) if keep[k].ops else e.ops
        else:
            keep[k] = e
    est.edges = list(keep.values())


def side_effect_closure(est: Estate, root: str) -> list[Edge]:
    """Transitive writes / sequence draws / calls reachable from `root` through call edges, cycle-safe."""
    out: list[Edge] = []
    seen, stack = {root}, [root]
    while stack:
        cur = stack.pop()
        for e in est.edges:
            if e.src != cur or e.kind not in ("writes", "consumes-sequence", "calls"):
                continue
            out.append(e)
            if e.kind == "calls":
                callee = e.dst if e.dst in est.nodes else e.dst.rsplit(".", 1)[0]
                for nxt in (e.dst, callee):
                    if nxt not in seen:
                        seen.add(nxt)
                        stack.append(nxt)
    return out


def trigger_fan_out(est: Estate) -> None:
    """Writers of a trigger's base table inherit the trigger's full side-effect closure (section 2 trigger row).

    Iterates to a fixed point so a trigger whose closure writes a second triggered table cascades too; the
    per-trigger closure is cycle-safe (visited set) and the outer loop stops when no new edge appears.
    """
    for _ in range(32):
        added = 0
        triggers = [e for e in est.edges if e.kind == "defines-on" and est.nodes.get(e.src) and est.nodes[e.src].cls == "TRIGGER"]
        existing = {(e.src, e.dst, e.kind) for e in est.edges}
        for t in triggers:
            closure = [c for c in side_effect_closure(est, t.src) if c.dst != t.dst]
            # only writers whose DML events (and, for UPDATE OF, columns) the trigger declares fire it; a write of
            # unknown shape is assumed to fire, an UPDATE with an unreadable SET list fires as INFERRED
            writers: dict[str, str] = {}
            for w in est.edges:
                if w.kind != "writes" or w.dst != t.dst or w.src == t.src:
                    continue
                fires, risk = ops_fire(t.ops, w.ops)
                if fires and (w.src not in writers or not risk):
                    writers[w.src] = risk
            for w, risk in writers.items():
                for c in closure:
                    if w in (c.src, c.dst) or (w, c.dst, c.kind) in existing:
                        continue
                    via = f"trigger fan-out via {t.src}" + (f" -> {c.src}" if c.src != t.src else "")
                    est.edges.append(Edge(w, c.dst, c.kind, "INFERRED" if risk else "FACT", risk, via, c.ops))
                    existing.add((w, c.dst, c.kind))
                    added += 1
        if not added:
            return


def report(res: dict) -> tuple[str, int]:
    nodes, edges = res["nodes"], res["edges"]
    fixture_nodes = {k: n for k, n in nodes.items() if not k.startswith("ODS.PKG_POLICY_INQUIRY") and n.file != "pkg_policy_inquiry.sql"
                     and not k.startswith("ODS.ODS_") and not k.startswith("TERADATA.")}
    albion_keys = set(nodes) - set(fixture_nodes)
    alb_edges = [e for e in edges if e.src.startswith("ODS.PKG_POLICY_INQUIRY") or e.src.startswith("TERADATA.")]
    fix_edges = [e for e in edges if e not in alb_edges]
    ev = Counter(e.evidence for e in fix_edges)
    aev = Counter(e.evidence for e in alb_edges)
    out = ["# oracle-plsql static round-trip report", "",
           f"Fixture files: {len(res['files'])}; census rows: {sum(n.status == 'enumerated' for n in fixture_nodes.values())} enumerated "
           f"(+{sum(n.status != 'enumerated' for n in fixture_nodes.values())} referenced-only) "
           f"({', '.join(f'{c} {n}' for c, n in sorted(Counter(n.cls for n in fixture_nodes.values() if n.status == 'enumerated').items()))})",
           f"Fixture edges: {len(fix_edges)} = FACT {ev['FACT']}, INFERRED {ev['INFERRED']}, UNVERIFIABLE {ev['UNVERIFIABLE']}", ""]
    if res["albion"]:
        out += [f"Albion pkg_policy_inquiry.sql: census rows {sum(nodes[k].status == 'enumerated' for k in albion_keys)} enumerated "
                f"(+{sum(nodes[k].status != 'enumerated' for k in albion_keys)} referenced-only) ({', '.join(sorted(albion_keys))})",
                f"Albion edges: {len(alb_edges)} = FACT {aev['FACT']}, INFERRED {aev['INFERRED']}, UNVERIFIABLE {aev['UNVERIFIABLE']}", ""]
    out += ["## INFERRED edges (genuinely dynamic / external; listed per template Acceptance)", "",
            "| src | dst | kind | risk | detail |", "|---|---|---|---|---|"]
    out += [f"| {e.src} | {e.dst} | {e.kind} | {e.risk} | {e.detail} |" for e in edges if e.evidence == "INFERRED"]
    out += ["", "## Nodes referenced but not enumerated (inventory gaps, not lineage uncertainty)", ""]
    out += [f"- {k} ({n.cls}, {n.status})" for k, n in nodes.items() if n.status != "enumerated"] or ["- none"]
    out += ["", "## Census (fixture + Albion)", "", "| key | class | file | signals |", "|---|---|---|---|"]
    out += [f"| {k} | {n.cls} | {n.file} | {json.dumps(n.signals, sort_keys=True) if n.signals else ''} |" for k, n in sorted(nodes.items())]
    out += ["", "## FACT edges", "", "| src | dst | kind | detail |", "|---|---|---|---|"]
    out += [f"| {e.src} | {e.dst} | {e.kind} | {e.detail}{' [' + e.ops + ']' if e.ops else ''} |" for e in edges if e.evidence == "FACT"]
    unverifiable = ev["UNVERIFIABLE"] + aev["UNVERIFIABLE"]
    return "\n".join(out) + "\n", unverifiable


# --------------------------------------------------------------------------- self-test (negative fixtures)

NEGATIVE_CASES: dict[str, tuple[str, str]] = {
    # name: (Oracle text that a section-2 rule does NOT model, expected UNVERIFIABLE risk)
    "quoted_read": ("CREATE OR REPLACE VIEW poladm.v_q AS SELECT 1 FROM \"POLADM\".\"Policy\";", "quoted-identifier"),
    "quoted_write": ("CREATE OR REPLACE PROCEDURE poladm.p_q IS BEGIN INSERT INTO \"Policy\" VALUES (1); END;", "quoted-identifier"),
    "inline_view_update": ("CREATE OR REPLACE PROCEDURE poladm.p_iv IS BEGIN UPDATE (SELECT * FROM poladm.policy) SET a = 1; END;",
                           "updatable-inline-view-or-bind-target"),
    "table_function_read": ("CREATE OR REPLACE VIEW poladm.v_tf AS SELECT * FROM fn_not_enumerated(1);", "table-function-not-in-census"),
    "table_function_unknown_schema": ("CREATE OR REPLACE VIEW poladm.v_tf2 AS SELECT * FROM claims.fn_remote(1);", "unresolved-qualified-call"),
    "exec_imm_concat": ("CREATE OR REPLACE PROCEDURE poladm.p_c (p IN VARCHAR2) IS BEGIN EXECUTE IMMEDIATE 'DELETE FROM ' || p; END;",
                        "dynamic-sql-expression"),
    "exec_imm_unassigned": ("CREATE OR REPLACE PROCEDURE poladm.p_u (p_sql IN VARCHAR2) IS BEGIN EXECUTE IMMEDIATE p_sql; END;",
                            "dynamic-sql-unassigned-variable"),
    "exec_imm_no_prefix": ("CREATE OR REPLACE PROCEDURE poladm.p_np IS l_s VARCHAR2(4000); BEGIN l_s := build(); EXECUTE IMMEDIATE l_s; END;",
                           "dynamic-sql-no-literal-prefix"),
    "exec_imm_function_call": ("CREATE OR REPLACE PROCEDURE poladm.p_fc IS BEGIN EXECUTE IMMEDIATE build_sql(1); END;", "dynamic-sql-expression"),
    "open_for_expression": ("CREATE OR REPLACE PROCEDURE poladm.p_of (c OUT SYS_REFCURSOR, t IN VARCHAR2) IS BEGIN OPEN c FOR 'SELECT * FROM ' || t; END;",
                            "dynamic-cursor-expression"),
    "dbms_sql": ("CREATE OR REPLACE PROCEDURE poladm.p_ds IS c INTEGER; BEGIN c := DBMS_SQL.OPEN_CURSOR; DBMS_SQL.PARSE(c, 'x', 1); END;",
                 "external-side-effect-package"),
    "utl_file": ("CREATE OR REPLACE PROCEDURE poladm.p_uf IS f UTL_FILE.FILE_TYPE; BEGIN f := UTL_FILE.FOPEN('D', 'x', 'w'); END;",
                 "external-side-effect-package"),
    "unresolved_pkg_call": ("CREATE OR REPLACE PROCEDURE poladm.p_up IS BEGIN some_unknown_pkg.do_it(1); END;", "unresolved-qualified-call"),
    "mview_refresh_var": ("CREATE OR REPLACE PROCEDURE poladm.p_mv (l IN VARCHAR2) IS BEGIN DBMS_MVIEW.REFRESH(l); END;", "mview-refresh-non-literal"),
    "index_quoted_target": ("CREATE INDEX poladm.ix_q ON \"POLADM\".\"Policy\" (id);", "unparsed-index-target"),
    "comma_join_quoted_second_table": ("CREATE OR REPLACE VIEW poladm.v_cq AS SELECT 1 FROM poladm.a x, \"POLADM\".\"Policy\" p;",
                                       "quoted-identifier"),
    "comma_join_table_function_second": ("CREATE OR REPLACE VIEW poladm.v_cf AS SELECT 1 FROM poladm.a x, fn_not_enumerated(x.id) f;",
                                         "table-function-not-in-census"),
    # ruled DBMS_* calls whose naming arguments are not literals (or are missing): controlled uncertainty, never a crash
    "sched_job_name_variable": ("DECLARE l_name VARCHAR2(30) := 'JOB_X'; BEGIN DBMS_SCHEDULER.CREATE_JOB(job_name => l_name,\n"
                                "  job_type => 'PLSQL_BLOCK', job_action => 'BEGIN NULL; END;'); END;", "scheduler-name-not-literal"),
    "sched_job_no_args": ("BEGIN DBMS_SCHEDULER.CREATE_JOB(); END;", "scheduler-name-not-literal"),
    "sched_program_positional_variable_name": ("DECLARE p VARCHAR2(30); BEGIN DBMS_SCHEDULER.CREATE_PROGRAM(p, 'PLSQL_BLOCK', 'BEGIN NULL; END;'); END;",
                                               "scheduler-name-not-literal"),
    "sched_job_action_variable": ("DECLARE a VARCHAR2(200) := build_action(); BEGIN DBMS_SCHEDULER.CREATE_JOB('POLADM.JOB_A', 'PLSQL_BLOCK', a); END;",
                                  "scheduler-action-not-literal"),
    "sched_job_program_variable": ("DECLARE p VARCHAR2(30) := pick(); BEGIN DBMS_SCHEDULER.CREATE_JOB('POLADM.JOB_V', program_name => p); END;",
                                   "scheduler-program-not-literal"),
    "rls_policy_missing_args": ("BEGIN DBMS_RLS.ADD_POLICY('POLADM', 'T_V'); END;", "policy-call-not-literal"),
    "rls_policy_function_variable": ("DECLARE f VARCHAR2(30) := 'FN'; BEGIN DBMS_RLS.ADD_POLICY(object_schema => 'POLADM', object_name => 'T_V',\n"
                                     "  policy_name => 'POL_V', policy_function => f); END;", "policy-call-not-literal"),
    "redact_policy_object_variable": ("DECLARE t VARCHAR2(30) := 'T_R'; BEGIN DBMS_REDACT.ADD_POLICY(object_schema => 'POLADM', object_name => t,\n"
                                      "  policy_name => 'POL_R', column_name => 'NINO', function_type => DBMS_REDACT.FULL); END;", "policy-call-not-literal"),
    "redact_policy_no_args": ("BEGIN DBMS_REDACT.ADD_POLICY; DBMS_REDACT.ADD_POLICY(); END;", "policy-call-not-literal"),
}
POSITIVE_TEXT = """
CREATE TABLE poladm.t_ok (id NUMBER, d DATE);
CREATE OR REPLACE PROCEDURE poladm.p_ok IS
  l_n NUMBER; l_rows SYS.ODCINUMBERLIST := SYS.ODCINUMBERLIST();
  CURSOR c IS SELECT id FROM poladm.t_ok FOR UPDATE OF id NOWAIT;
BEGIN
  SELECT EXTRACT(YEAR FROM d), TRIM(LEADING 'x' FROM 'xy') INTO l_n, l_n FROM poladm.t_ok WHERE ROWNUM = 1;
  UPDATE poladm.t_ok SET id = 1 WHERE id IN (SELECT id FROM (SELECT id FROM poladm.t_ok));
  MERGE INTO poladm.t_ok t USING (SELECT 1 id FROM dual) s ON (t.id = s.id)
    WHEN MATCHED THEN UPDATE SET d = SYSDATE DELETE WHERE d IS NULL
    WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);
  DELETE poladm.t_ok WHERE id = -1;
  l_rows.EXTEND; l_rows.DELETE; DBMS_OUTPUT.PUT_LINE(l_rows.COUNT);
  EXECUTE IMMEDIATE 'TRUNCATE TABLE poladm.t_ok';
END;
/
"""
@dataclass
class Case:
    """Supported syntax: edges that MUST exist as (src, dst, kind), node keys that must NOT exist, (key, class)
    pairs that MUST be in the census, and (src, dst, kind) edges that must NOT be drawn (wrong attribution)."""
    sql: str
    want: set = field(default_factory=set)
    forbid: set = field(default_factory=set)
    want_nodes: set = field(default_factory=set)
    forbid_edges: set = field(default_factory=set)
    want_evidence: set = field(default_factory=set)   # (src, dst, kind, evidence, risk) that MUST exist exactly so
    files: dict = field(default_factory=dict)        # extra {relative path: text} written next to the case's own file


POSITIVE_CASES: dict[str, Case] = {
    "cte_multiple": Case(
        "CREATE OR REPLACE VIEW poladm.v_cte AS\n"
        "WITH recent AS (SELECT * FROM poladm.policy WHERE d > SYSDATE - 7),\n"
        "     agg (n) AS (SELECT count(*) FROM recent r JOIN poladm.broker b ON b.id = r.broker_id)\n"
        "SELECT * FROM agg JOIN recent ON 1 = 1;",
        want={("POLADM.V_CTE", "POLADM.POLICY", "reads"), ("POLADM.V_CTE", "POLADM.BROKER", "reads")},
        forbid={"POLADM.RECENT", "POLADM.AGG"},
    ),
    "cte_recursive": Case(
        "CREATE OR REPLACE VIEW poladm.v_tree AS\n"
        "WITH tree (id, lvl) AS (SELECT id, 1 FROM poladm.broker WHERE parent_id IS NULL\n"
        "  UNION ALL SELECT b.id, t.lvl + 1 FROM tree t JOIN poladm.broker b ON b.parent_id = t.id)\n"
        "SELECT * FROM tree;",
        want={("POLADM.V_TREE", "POLADM.BROKER", "reads")},
        forbid={"POLADM.TREE"},
    ),
    "cte_scope_is_per_statement": Case(
        "CREATE OR REPLACE PROCEDURE poladm.p_cte IS l_n NUMBER; BEGIN\n"
        "  WITH recent AS (SELECT id FROM poladm.policy) SELECT count(*) INTO l_n FROM recent;\n"
        "  SELECT count(*) INTO l_n FROM recent;   -- a real table in the next statement, no WITH in scope\n"
        "END;",
        want={("POLADM.P_CTE", "POLADM.POLICY", "reads"), ("POLADM.P_CTE", "POLADM.RECENT", "reads")},
    ),
    "qualified_name_ignores_public_synonym": Case(
        "CREATE OR REPLACE PUBLIC SYNONYM broker FOR poladm.broker;\n"
        "CREATE OR REPLACE VIEW ods.v_a AS SELECT 1 FROM claims.broker;\n"
        "CREATE OR REPLACE VIEW ods.v_b AS SELECT 1 FROM broker;",
        want={("ODS.V_A", "CLAIMS.BROKER", "reads"), ("ODS.V_B", "POLADM.BROKER", "reads")},
    ),
    "comment_markers_inside_literals": Case(
        "CREATE OR REPLACE PROCEDURE poladm.p_lit IS l_s VARCHAR2(200); l_n NUMBER; BEGIN\n"
        "  SELECT '-- not a comment' INTO l_s FROM poladm.t_a;\n"
        "  SELECT 'it''s /* not a comment' INTO l_s FROM poladm.t_b;\n"
        "  l_s := q'{ -- q-quoted /* }'; SELECT 1 INTO l_n FROM poladm.t_c;\n"
        "  EXECUTE IMMEDIATE 'DELETE FROM poladm.t_d WHERE note = ''--'' /* ';\n"
        "  SELECT 1 AS \"x -- /* y\" INTO l_n FROM poladm.t_e; /* real comment -- */ SELECT 1 INTO l_n FROM poladm.t_f; -- real\n"
        "END;",
        want={("POLADM.P_LIT", f"POLADM.T_{c}", "reads") for c in "ABCEF"} | {("POLADM.P_LIT", "POLADM.T_D", "writes")},
    ),
    "unique_and_bitmap_index_in_census": Case(
        "CREATE TABLE poladm.t_u (id NUMBER, flag CHAR(1));\n"
        "CREATE UNIQUE INDEX poladm.ux_t_u ON poladm.t_u (id);\n"
        "CREATE BITMAP INDEX poladm.bx_t_u ON poladm.t_u (flag);\n"
        "CREATE INDEX poladm.ix_t_u ON poladm.t_u (flag, id);",
        want={("POLADM.UX_T_U", "POLADM.T_U", "defines-on"), ("POLADM.BX_T_U", "POLADM.T_U", "defines-on"),
              ("POLADM.IX_T_U", "POLADM.T_U", "defines-on")},
        want_nodes={("POLADM.UX_T_U", "INDEX"), ("POLADM.BX_T_U", "INDEX"), ("POLADM.IX_T_U", "INDEX"), ("POLADM.T_U", "TABLE")},
    ),
    "index_base_table_in_later_file_and_function_based": Case(
        "CREATE UNIQUE INDEX poladm.ux_later ON poladm.t_later (UPPER(code), TRUNC(d));\n"
        "CREATE INDEX poladm.ix_later ON poladm.t_later (id)\n  TABLESPACE idx_ts LOCAL;\n"
        "CREATE TABLE poladm.t_later (id NUMBER, code VARCHAR2(10), d DATE);",
        want={("POLADM.UX_LATER", "POLADM.T_LATER", "defines-on"), ("POLADM.IX_LATER", "POLADM.T_LATER", "defines-on")},
        want_nodes={("POLADM.T_LATER", "TABLE")},
    ),
    "ctas_reads_every_source": Case(
        "CREATE TABLE poladm.policy_bak AS SELECT p.*, b.name FROM poladm.policy p JOIN poladm.broker b ON b.id = p.broker_id;\n"
        "CREATE GLOBAL TEMPORARY TABLE poladm.gtt_cp ON COMMIT PRESERVE ROWS AS (SELECT id FROM poladm.party WHERE 1 = 0);\n"
        "CREATE TABLE poladm.t_with AS WITH x AS (SELECT id FROM poladm.premium_txn) SELECT * FROM x;\n"
        "CREATE TABLE poladm.t_plain (id NUMBER, d DATE DEFAULT SYSDATE);",
        want={("POLADM.POLICY_BAK", "POLADM.POLICY", "reads"), ("POLADM.POLICY_BAK", "POLADM.BROKER", "reads"),
              ("POLADM.GTT_CP", "POLADM.PARTY", "reads"), ("POLADM.T_WITH", "POLADM.PREMIUM_TXN", "reads")},
        forbid={"POLADM.CTAS_READS_EVERY_SOURCE", "POLADM.X"},
        forbid_edges={("POLADM.T_WITH", "POLADM.X", "reads")},
        want_nodes={("POLADM.POLICY_BAK", "TABLE"), ("POLADM.GTT_CP", "TABLE"), ("POLADM.T_WITH", "TABLE"), ("POLADM.T_PLAIN", "TABLE")},
    ),
    "delete_from_is_a_write_not_a_read": Case(
        "CREATE OR REPLACE PROCEDURE poladm.p_del IS BEGIN\n"
        "  DELETE FROM poladm.stg_a WHERE loaded = 'Y';\n"
        "  DELETE poladm.stg_b WHERE EXISTS (SELECT 1 FROM poladm.policy p WHERE p.id = stg_b.id);\n"
        "  DELETE FROM poladm.stg_c c WHERE c.party_id IN (SELECT id FROM poladm.party WHERE status = 'X');\n"
        "END;",
        want={("POLADM.P_DEL", "POLADM.STG_A", "writes"), ("POLADM.P_DEL", "POLADM.STG_B", "writes"),
              ("POLADM.P_DEL", "POLADM.STG_C", "writes"), ("POLADM.P_DEL", "POLADM.POLICY", "reads"),
              ("POLADM.P_DEL", "POLADM.PARTY", "reads")},
        forbid_edges={("POLADM.P_DEL", "POLADM.STG_A", "reads"), ("POLADM.P_DEL", "POLADM.STG_B", "reads"),
                      ("POLADM.P_DEL", "POLADM.STG_C", "reads")},
    ),
    "trigger_fan_out_matches_dml_events": Case(
        "CREATE TABLE poladm.t_ev (id NUMBER, v NUMBER);\n"
        "CREATE OR REPLACE PROCEDURE poladm.log_i IS BEGIN INSERT INTO poladm.log_ins VALUES (1); END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.log_u IS BEGIN INSERT INTO poladm.log_upd VALUES (1); END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.log_d IS BEGIN INSERT INTO poladm.log_del VALUES (1); END;\n/\n"
        "CREATE OR REPLACE TRIGGER poladm.trg_i BEFORE INSERT ON poladm.t_ev FOR EACH ROW BEGIN poladm.log_i(); END;\n/\n"
        "CREATE OR REPLACE TRIGGER poladm.trg_u AFTER UPDATE OF v ON poladm.t_ev FOR EACH ROW BEGIN poladm.log_u(); END;\n/\n"
        "CREATE OR REPLACE TRIGGER poladm.trg_d AFTER DELETE ON poladm.t_ev FOR EACH ROW BEGIN poladm.log_d(); END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.p_ins IS BEGIN INSERT INTO poladm.t_ev VALUES (1, 1); END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.p_upd IS BEGIN UPDATE poladm.t_ev SET v = 2; END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.p_del IS BEGIN DELETE FROM poladm.t_ev WHERE id = 1; END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.p_mrg IS BEGIN MERGE INTO poladm.t_ev t USING poladm.src s ON (t.id = s.id)\n"
        "  WHEN MATCHED THEN UPDATE SET t.v = s.v WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v); END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.p_trunc IS BEGIN EXECUTE IMMEDIATE 'TRUNCATE TABLE poladm.t_ev'; END;",
        want={("POLADM.P_INS", "POLADM.LOG_INS", "writes"), ("POLADM.P_UPD", "POLADM.LOG_UPD", "writes"),
              ("POLADM.P_DEL", "POLADM.LOG_DEL", "writes"), ("POLADM.P_MRG", "POLADM.LOG_INS", "writes"),
              ("POLADM.P_MRG", "POLADM.LOG_UPD", "writes")},
        forbid_edges={("POLADM.P_INS", "POLADM.LOG_UPD", "writes"), ("POLADM.P_INS", "POLADM.LOG_DEL", "writes"),
                      ("POLADM.P_UPD", "POLADM.LOG_INS", "writes"), ("POLADM.P_UPD", "POLADM.LOG_DEL", "writes"),
                      ("POLADM.P_DEL", "POLADM.LOG_INS", "writes"), ("POLADM.P_DEL", "POLADM.LOG_UPD", "writes"),
                      ("POLADM.P_MRG", "POLADM.LOG_DEL", "writes"), ("POLADM.P_TRUNC", "POLADM.LOG_INS", "writes"),
                      ("POLADM.P_TRUNC", "POLADM.LOG_UPD", "writes"), ("POLADM.P_TRUNC", "POLADM.LOG_DEL", "writes")},
    ),
    "enumerated_synonym_resolves_to_target": Case(
        "CREATE TABLE poladm.policy (id NUMBER);\n"
        "CREATE TABLE poladm.broker (id NUMBER);\n"
        "CREATE OR REPLACE SYNONYM poladm.pol_syn FOR poladm.policy;\n"
        "CREATE OR REPLACE PUBLIC SYNONYM brk FOR poladm.broker;\n"
        "CREATE OR REPLACE SYNONYM poladm.missing_syn FOR poladm.not_enumerated;\n"
        "CREATE OR REPLACE SYNONYM poladm.remote_syn FOR claims.claim@claims_link;\n"
        "CREATE OR REPLACE VIEW poladm.v_syn AS SELECT p.id FROM pol_syn p JOIN brk b ON b.id = p.id;\n"
        "CREATE OR REPLACE VIEW poladm.v_miss AS SELECT 1 FROM poladm.missing_syn;\n"
        "CREATE OR REPLACE VIEW poladm.v_rem AS SELECT 1 FROM remote_syn;",
        want={("POLADM.V_SYN", "POLADM.POLICY", "reads"), ("POLADM.V_SYN", "POLADM.BROKER", "reads"),
              ("POLADM.V_MISS", "POLADM.NOT_ENUMERATED", "reads"), ("POLADM.V_REM", "CLAIMS.CLAIM@CLAIMS_LINK", "reads"),
              ("POLADM.POL_SYN", "POLADM.POLICY", "alias-of"), ("PUBLIC.BRK", "POLADM.BROKER", "alias-of"),
              ("POLADM.MISSING_SYN", "POLADM.NOT_ENUMERATED", "alias-of"), ("POLADM.REMOTE_SYN", "CLAIMS.CLAIM@CLAIMS_LINK", "alias-of")},
        forbid_edges={("POLADM.V_SYN", "POLADM.POL_SYN", "reads"), ("POLADM.V_SYN", "PUBLIC.BRK", "reads"),
                      ("POLADM.V_MISS", "POLADM.MISSING_SYN", "reads"), ("POLADM.V_REM", "POLADM.REMOTE_SYN", "reads")},
        want_nodes={("POLADM.POL_SYN", "SYNONYM"), ("PUBLIC.BRK", "PUBLIC SYNONYM"), ("POLADM.MISSING_SYN", "SYNONYM"),
                    ("POLADM.REMOTE_SYN", "SYNONYM")},
    ),
    "indented_sqlplus_script": Case(
        "    SET PAGESIZE 0 FEEDBACK OFF\n"
        "    SPOOL page.lst\n"
        "    SELECT policy_no FROM poladm.policy WHERE ROWNUM <= 10;\n"
        "    SPOOL OFF",
        want={("POLADM.INDENTED_SQLPLUS_SCRIPT", "POLADM.POLICY", "reads")},
        want_nodes={("POLADM.INDENTED_SQLPLUS_SCRIPT", "SQLPLUS_SCRIPT")},
    ),
    "plsql_set_and_execute_immediate_are_not_directives": Case(
        "CREATE OR REPLACE PROCEDURE poladm.p_np IS BEGIN\n"
        "  UPDATE poladm.t_y\n"
        "     SET feedback = 1;\n"
        "  EXECUTE IMMEDIATE 'TRUNCATE TABLE poladm.t_x';\n"
        "END;",
        want={("POLADM.P_NP", "POLADM.T_Y", "writes"), ("POLADM.P_NP", "POLADM.T_X", "writes")},
        forbid={"POLADM.PLSQL_SET_AND_EXECUTE_IMMEDIATE_ARE_NOT_DIRECTIVES"},
    ),
    "scheduler_action_any_qquote_delimiter": Case(
        "CREATE OR REPLACE PROCEDURE poladm.prc_a (p IN NUMBER) IS BEGIN NULL; END;\n"
        "/\n"
        "BEGIN\n"
        "  DBMS_SCHEDULER.CREATE_PROGRAM(program_name => 'POLADM.PRG_Q', program_type => 'PLSQL_BLOCK',\n"
        "    program_action => q'!DECLARE l NUMBER; BEGIN poladm.prc_a(1); INSERT INTO poladm.t_q VALUES (1);\n"
        "      DELETE FROM poladm.t_r WHERE note = 'x'; END;!', enabled => TRUE);\n"
        "  DBMS_SCHEDULER.CREATE_JOB(job_name => 'POLADM.JOB_Q', program_name => 'POLADM.PRG_Q',\n"
        "    start_date => TO_TIMESTAMP_TZ('2019-04-01 02:40:00 Europe/London', 'YYYY-MM-DD HH24:MI:SS TZR'), enabled => FALSE);\n"
        "  DBMS_SCHEDULER.CREATE_JOB(job_name => 'POLADM.JOB_INLINE', job_type => 'PLSQL_BLOCK',\n"
        "    job_action => 'BEGIN poladm.prc_a(2); INSERT INTO poladm.t_s VALUES (''y''); END;', enabled => FALSE);\n"
        "END;",
        want={("POLADM.PRG_Q", "POLADM.PRC_A", "calls"), ("POLADM.PRG_Q", "POLADM.T_Q", "writes"),
              ("POLADM.PRG_Q", "POLADM.T_R", "writes"), ("POLADM.JOB_Q", "POLADM.PRG_Q", "schedules"),
              ("POLADM.JOB_INLINE", "POLADM.PRC_A", "calls"), ("POLADM.JOB_INLINE", "POLADM.T_S", "writes")},
        want_nodes={("POLADM.PRG_Q", "SCHEDULER PROGRAM"), ("POLADM.JOB_Q", "SCHEDULER JOB"), ("POLADM.JOB_INLINE", "SCHEDULER JOB")},
    ),
    "mixed_ddl_and_top_level_dml": Case(
        "CREATE TABLE poladm.t_m (id NUMBER, ref_id NUMBER REFERENCES poladm.t_m (id) ON DELETE CASCADE);\n"
        "CREATE OR REPLACE VIEW poladm.v_m AS SELECT id FROM poladm.t_m;\n"
        "INSERT INTO poladm.t_m (id) SELECT id FROM poladm.t_src;\n"
        "MERGE INTO poladm.t_m t USING poladm.t_src s ON (t.id = s.id) WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);\n"
        "CREATE OR REPLACE PROCEDURE poladm.p_m IS BEGIN DELETE FROM poladm.t_p; END;\n"
        "/\n"
        "UPDATE poladm.t_m SET id = 0 WHERE id IS NULL;\n"
        "GRANT SELECT, DELETE ON poladm.t_m TO ods_reader;",
        want={("POLADM.MIXED_DDL_AND_TOP_LEVEL_DML", "POLADM.T_M", "writes"),
              ("POLADM.MIXED_DDL_AND_TOP_LEVEL_DML", "POLADM.T_SRC", "reads"),
              ("POLADM.V_M", "POLADM.T_M", "reads"), ("POLADM.P_M", "POLADM.T_P", "writes")},
        forbid={"POLADM.CASCADE"},
        want_nodes={("POLADM.MIXED_DDL_AND_TOP_LEVEL_DML", "DML SCRIPT")},
        forbid_edges={("POLADM.P_M", "POLADM.T_M", "writes"), ("POLADM.MIXED_DDL_AND_TOP_LEVEL_DML", "POLADM.T_P", "writes"),
                      ("POLADM.MIXED_DDL_AND_TOP_LEVEL_DML", "POLADM.T_M", "reads")},
    ),
    "ddl_only_file_is_not_a_dml_script": Case(
        "CREATE TABLE poladm.t_d (id NUMBER, p_id NUMBER REFERENCES poladm.t_d (id) ON DELETE CASCADE);\n"
        "GRANT INSERT, UPDATE, DELETE ON poladm.t_d TO poladm_app;",
        forbid={"POLADM.DDL_ONLY_FILE_IS_NOT_A_DML_SCRIPT", "POLADM.CASCADE"},
    ),
    "semicolon_inside_view_literal": Case(
        "CREATE OR REPLACE VIEW poladm.v_semi AS\n"
        "SELECT 'a;b' AS tag, q'[x;y]' AS tag2, \"c;d\", 'it''s;' AS tag3\n"
        "  FROM poladm.t_v v JOIN poladm.t_w w ON w.id = v.id;\n"
        "CREATE MATERIALIZED VIEW ods.mv_semi REFRESH FAST ON DEMAND AS SELECT 'p;q' AS x FROM poladm.t_z;",
        want={("POLADM.V_SEMI", "POLADM.T_V", "reads"), ("POLADM.V_SEMI", "POLADM.T_W", "reads"), ("ODS.MV_SEMI", "POLADM.T_Z", "reads")},
        want_nodes={("ODS.MV_SEMI", "MATERIALIZED VIEW")},
    ),
}


POSITIVE_CASES.update({
    "comma_join_two_tables": Case(
        "CREATE OR REPLACE VIEW poladm.v_c2 AS SELECT a.id, b.v FROM poladm.t_ca a, poladm.t_cb b WHERE a.id = b.id;",
        want={("POLADM.V_C2", "POLADM.T_CA", "reads"), ("POLADM.V_C2", "POLADM.T_CB", "reads")},
        forbid={"POLADM.A", "POLADM.B"},
    ),
    "comma_join_three_tables_and_ansi_mix": Case(
        "CREATE OR REPLACE VIEW poladm.v_c3 AS\n"
        "SELECT a.id, b.v, c.w, d.x FROM poladm.t_ca a, poladm.t_cb b JOIN poladm.t_cc c ON c.id = b.id AND c.k IN (1, 2),\n"
        "  claims.t_cd d, poladm.t_ce@ods_link e\n"
        " WHERE a.id = b.id AND d.id = a.id GROUP BY a.id, b.v, c.w, d.x ORDER BY 1, 2;",
        want={("POLADM.V_C3", "POLADM.T_CA", "reads"), ("POLADM.V_C3", "POLADM.T_CB", "reads"),
              ("POLADM.V_C3", "POLADM.T_CC", "reads"), ("POLADM.V_C3", "CLAIMS.T_CD", "reads"),
              ("POLADM.V_C3", "POLADM.T_CE@ODS_LINK", "reads")},
        forbid={"POLADM.A", "POLADM.B", "POLADM.C", "POLADM.D", "POLADM.E", "POLADM.V", "POLADM.W", "POLADM.X"},
    ),
    "comma_join_nested_and_select_list_commas": Case(
        "CREATE OR REPLACE PROCEDURE poladm.p_cn IS l_n NUMBER; l_y NUMBER; BEGIN\n"
        "  FOR r IN (SELECT s.id FROM (SELECT i.id FROM poladm.t_in i, poladm.t_in2 j WHERE i.id = j.id) s, poladm.t_out o\n"
        "            WHERE o.id = s.id) LOOP NULL; END LOOP;\n"
        "  SELECT EXTRACT(YEAR FROM d), other_col INTO l_y, l_n FROM poladm.t_dates WHERE k IN (1, 2);\n"
        "  SELECT count(*) INTO l_n FROM poladm.t_g GROUP BY g1, g2 ORDER BY g1, g2;\n"
        "  DELETE FROM poladm.t_del WHERE id IN (SELECT id FROM poladm.t_x, poladm.t_y);\n"
        "END;",
        want={("POLADM.P_CN", "POLADM.T_IN", "reads"), ("POLADM.P_CN", "POLADM.T_IN2", "reads"),
              ("POLADM.P_CN", "POLADM.T_OUT", "reads"), ("POLADM.P_CN", "POLADM.T_DATES", "reads"),
              ("POLADM.P_CN", "POLADM.T_G", "reads"), ("POLADM.P_CN", "POLADM.T_X", "reads"),
              ("POLADM.P_CN", "POLADM.T_Y", "reads"), ("POLADM.P_CN", "POLADM.T_DEL", "writes")},
        forbid={"POLADM.S", "POLADM.O", "POLADM.OTHER_COL", "POLADM.G1", "POLADM.G2", "POLADM.D", "POLADM.I", "POLADM.J"},
    ),
    "select_only_report_file_is_a_script": Case(
        "SELECT p.policy_no, b.broker_name\n  FROM poladm.policy p JOIN poladm.broker b ON b.broker_id = p.broker_id\n"
        " WHERE p.expiry_dt < SYSDATE;\n"
        "SELECT count(*) FROM poladm.party;",
        want={("POLADM.SELECT_ONLY_REPORT_FILE_IS_A_SCRIPT", "POLADM.POLICY", "reads"),
              ("POLADM.SELECT_ONLY_REPORT_FILE_IS_A_SCRIPT", "POLADM.BROKER", "reads"),
              ("POLADM.SELECT_ONLY_REPORT_FILE_IS_A_SCRIPT", "POLADM.PARTY", "reads")},
        want_nodes={("POLADM.SELECT_ONLY_REPORT_FILE_IS_A_SCRIPT", "SQL SCRIPT")},
    ),
    "call_only_anonymous_block_is_a_script": Case(
        "CREATE OR REPLACE PROCEDURE poladm.run_all IS BEGIN INSERT INTO poladm.t_run VALUES (1); END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.log_run (p IN NUMBER) IS BEGIN INSERT INTO poladm.t_log VALUES (p); END;\n/\n"
        "DECLARE\n  l_n NUMBER(10);\nBEGIN\n  poladm.run_all;\n  log_run(l_n);\n  DBMS_OUTPUT.PUT_LINE('done');\n  COMMIT;\nEND;\n/\n"
        "CALL poladm.log_run(2);",
        want={("POLADM.CALL_ONLY_ANONYMOUS_BLOCK_IS_A_SCRIPT", "POLADM.RUN_ALL", "calls"),
              ("POLADM.CALL_ONLY_ANONYMOUS_BLOCK_IS_A_SCRIPT", "POLADM.LOG_RUN", "calls"),
              ("POLADM.RUN_ALL", "POLADM.T_RUN", "writes")},
        want_nodes={("POLADM.CALL_ONLY_ANONYMOUS_BLOCK_IS_A_SCRIPT", "PLSQL SCRIPT")},
        forbid_edges={("POLADM.CALL_ONLY_ANONYMOUS_BLOCK_IS_A_SCRIPT", "POLADM.T_RUN", "writes"),
                      ("POLADM.CALL_ONLY_ANONYMOUS_BLOCK_IS_A_SCRIPT", "POLADM.T_LOG", "writes")},
    ),
    "ruled_package_block_and_control_statements_are_not_a_script": Case(
        "CREATE OR REPLACE PROCEDURE poladm.prc_r IS BEGIN NULL; END;\n/\n"
        "BEGIN\n  DBMS_SCHEDULER.CREATE_JOB(job_name => 'POLADM.JOB_R', job_type => 'PLSQL_BLOCK',\n"
        "    job_action => 'BEGIN poladm.prc_r; END;', enabled => FALSE);\n  DBMS_OUTPUT.PUT_LINE('created');\n  COMMIT;\nEND;\n/\n"
        "BEGIN\n  NULL;\nEND;\n/\n"
        "GRANT SELECT ON poladm.t_r TO ods_reader;",
        want={("POLADM.JOB_R", "POLADM.PRC_R", "calls")},
        forbid={"POLADM.RULED_PACKAGE_BLOCK_AND_CONTROL_STATEMENTS_ARE_NOT_A_SCRIPT"},
        want_nodes={("POLADM.JOB_R", "SCHEDULER JOB")},
    ),
    "package_members_own_their_effects": Case(
        "CREATE OR REPLACE PROCEDURE poladm.log_a IS BEGIN INSERT INTO poladm.log_a_t VALUES (1); END;\n/\n"
        "CREATE OR REPLACE PACKAGE poladm.pkg_two AS\n"
        "  c_limit CONSTANT NUMBER := 100;\n"
        "  PROCEDURE write_a(p IN NUMBER);\n"
        "  PROCEDURE write_b(p IN NUMBER);\n"
        "  FUNCTION rate RETURN NUMBER;\n"
        "END pkg_two;\n/\n"
        "CREATE OR REPLACE PACKAGE BODY poladm.pkg_two AS\n"
        "  CURSOR c_cfg IS SELECT v FROM poladm.pkg_cfg;\n"
        "  PROCEDURE helper(p IN NUMBER);\n"
        "  FUNCTION rate RETURN NUMBER IS l NUMBER; BEGIN SELECT r INTO l FROM poladm.rates; RETURN l; END rate;\n"
        "  PROCEDURE helper(p IN NUMBER) IS\n"
        "    PROCEDURE inner_h IS BEGIN INSERT INTO poladm.t_inner VALUES (p); END inner_h;\n"
        "  BEGIN inner_h; END helper;\n"
        "  PROCEDURE write_a(p IN NUMBER) IS BEGIN INSERT INTO poladm.t_a VALUES (p * rate()); helper(p); END write_a;\n"
        "  PROCEDURE write_b(p IN NUMBER) IS BEGIN UPDATE poladm.t_b SET v = p WHERE id = c_limit; END write_b;\n"
        "BEGIN\n  INSERT INTO poladm.pkg_init_log VALUES (SYSDATE);\n"
        "END pkg_two;\n/\n"
        "CREATE OR REPLACE TRIGGER poladm.trg_a AFTER INSERT ON poladm.t_a FOR EACH ROW BEGIN poladm.log_a(); END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.caller_a IS BEGIN poladm.pkg_two.write_a(1); END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.caller_b IS BEGIN pkg_two.write_b(2); END;",
        want={("POLADM.PKG_TWO.WRITE_A", "POLADM.T_A", "writes"), ("POLADM.PKG_TWO.WRITE_B", "POLADM.T_B", "writes"),
              ("POLADM.PKG_TWO.RATE", "POLADM.RATES", "reads"), ("POLADM.PKG_TWO.HELPER", "POLADM.T_INNER", "writes"),
              ("POLADM.PKG_TWO.WRITE_A", "POLADM.PKG_TWO.RATE", "calls"), ("POLADM.PKG_TWO.WRITE_A", "POLADM.PKG_TWO.HELPER", "calls"),
              ("POLADM.PKG_TWO", "POLADM.PKG_CFG", "reads"), ("POLADM.PKG_TWO", "POLADM.PKG_INIT_LOG", "writes"),
              ("POLADM.CALLER_A", "POLADM.PKG_TWO.WRITE_A", "calls"), ("POLADM.CALLER_B", "POLADM.PKG_TWO.WRITE_B", "calls"),
              ("POLADM.PKG_TWO.WRITE_A", "POLADM.LOG_A_T", "writes")},
        forbid={"POLADM.PKG_TWO.INNER_H"},
        forbid_edges={("POLADM.PKG_TWO", "POLADM.T_A", "writes"), ("POLADM.PKG_TWO", "POLADM.T_B", "writes"),
                      ("POLADM.PKG_TWO", "POLADM.RATES", "reads"), ("POLADM.PKG_TWO", "POLADM.LOG_A_T", "writes"),
                      ("POLADM.PKG_TWO.WRITE_B", "POLADM.T_A", "writes"), ("POLADM.PKG_TWO.WRITE_B", "POLADM.LOG_A_T", "writes"),
                      ("POLADM.PKG_TWO.WRITE_A", "POLADM.T_B", "writes"), ("POLADM.PKG_TWO.RATE", "POLADM.T_A", "writes")},
        want_nodes={("POLADM.PKG_TWO", "PACKAGE"), ("POLADM.PKG_TWO.WRITE_A", "PACKAGE PROCEDURE"),
                    ("POLADM.PKG_TWO.WRITE_B", "PACKAGE PROCEDURE"), ("POLADM.PKG_TWO.RATE", "PACKAGE FUNCTION"),
                    ("POLADM.PKG_TWO.HELPER", "PACKAGE PROCEDURE")},
    ),
    "trigger_update_of_columns": Case(
        "CREATE TABLE poladm.t_c (id NUMBER, status VARCHAR2(10), premium NUMBER);\n"
        "CREATE OR REPLACE PROCEDURE poladm.log_s IS BEGIN INSERT INTO poladm.log_status VALUES (1); END;\n/\n"
        "CREATE OR REPLACE TRIGGER poladm.trg_s AFTER UPDATE OF status, id ON poladm.t_c FOR EACH ROW BEGIN poladm.log_s(); END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.p_status IS BEGIN UPDATE poladm.t_c t SET t.status = 'X' WHERE id = 1; END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.p_prem IS BEGIN UPDATE poladm.t_c SET premium = premium * 1.1, id = id WHERE 1 = 1; END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.p_prem_only IS BEGIN UPDATE poladm.t_c SET premium = (SELECT 1 FROM dual WHERE 1 = 1)\n"
        "  RETURNING premium, id INTO l_a, l_b; END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.p_mrg_s IS BEGIN MERGE INTO poladm.t_c t USING poladm.src s ON (t.id = s.id)\n"
        "  WHEN MATCHED THEN UPDATE SET t.status = s.status WHEN NOT MATCHED THEN INSERT (id, status) VALUES (s.id, s.status); END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.p_mrg_p IS BEGIN MERGE INTO poladm.t_c t USING poladm.src s ON (t.id = s.id)\n"
        "  WHEN MATCHED THEN UPDATE SET t.premium = s.premium WHERE t.status IS NOT NULL\n"
        "  WHEN NOT MATCHED THEN INSERT (id, premium) VALUES (s.id, s.premium); END;\n/\n"
        "CREATE OR REPLACE PROCEDURE poladm.p_row IS l_rec poladm.t_c%ROWTYPE; BEGIN UPDATE poladm.t_c SET ROW = l_rec WHERE id = 1; END;",
        want={("POLADM.P_STATUS", "POLADM.LOG_STATUS", "writes"), ("POLADM.P_PREM", "POLADM.LOG_STATUS", "writes"),
              ("POLADM.P_MRG_S", "POLADM.LOG_STATUS", "writes")},
        forbid_edges={("POLADM.P_PREM_ONLY", "POLADM.LOG_STATUS", "writes"), ("POLADM.P_MRG_P", "POLADM.LOG_STATUS", "writes")},
        want_evidence={("POLADM.P_STATUS", "POLADM.LOG_STATUS", "writes", "FACT", ""),
                       ("POLADM.P_ROW", "POLADM.LOG_STATUS", "writes", "INFERRED", "update-columns-unknown")},
    ),
    "sched_positional_arguments": Case(
        "CREATE OR REPLACE PROCEDURE poladm.prc_p IS BEGIN INSERT INTO poladm.t_p VALUES (1); END;\n/\n"
        "BEGIN\n"
        "  DBMS_SCHEDULER.CREATE_PROGRAM('POLADM.PRG_P', 'PLSQL_BLOCK', 'BEGIN poladm.prc_p; END;', 0, TRUE, 'nightly');\n"
        "  DBMS_SCHEDULER.CREATE_JOB('POLADM.JOB_P', 'POLADM.PRG_P', SYSTIMESTAMP, 'FREQ=DAILY; BYHOUR=2', NULL,\n"
        "    'DEFAULT_JOB_CLASS', TRUE, TRUE, 'program-based, all positional');\n"
        "  DBMS_SCHEDULER.CREATE_JOB('JOB_I', 'PLSQL_BLOCK', 'BEGIN prc_p; END;', 0, TO_TIMESTAMP_TZ('2026-01-01 02:00:00 UTC',\n"
        "    'YYYY-MM-DD HH24:MI:SS TZR'), 'FREQ=HOURLY', NULL, 'DEFAULT_JOB_CLASS', FALSE);\n"
        "  DBMS_SCHEDULER.CREATE_JOB('JOB_M', program_name => 'PRG_P', enabled => TRUE);\n"
        "END;",
        want={("POLADM.JOB_P", "POLADM.PRG_P", "schedules"), ("POLADM.PRG_P", "POLADM.PRC_P", "calls"),
              ("POLADM.JOB_I", "POLADM.PRC_P", "calls"), ("POLADM.JOB_M", "POLADM.PRG_P", "schedules"),
              ("POLADM.PRC_P", "POLADM.T_P", "writes")},
        want_nodes={("POLADM.PRG_P", "SCHEDULER PROGRAM"), ("POLADM.JOB_P", "SCHEDULER JOB"), ("POLADM.JOB_I", "SCHEDULER JOB"),
                    ("POLADM.JOB_M", "SCHEDULER JOB")},
        forbid={"POLADM.SCHED_POSITIONAL_ARGUMENTS", "POLADM.PLSQL_BLOCK", "POLADM.SYSTIMESTAMP"},
    ),
    "policy_positional_and_defaulted_schema": Case(
        "CREATE TABLE poladm.t_v (id NUMBER, nino VARCHAR2(9));\n"
        "CREATE OR REPLACE FUNCTION poladm.fn_v (s IN VARCHAR2, o IN VARCHAR2) RETURN VARCHAR2 IS BEGIN RETURN '1=1'; END;\n/\n"
        "BEGIN\n"
        "  DBMS_RLS.ADD_POLICY('POLADM', 'T_V', 'POL_V', 'POLADM', 'FN_V', 'SELECT,UPDATE', FALSE, TRUE);\n"
        "  DBMS_RLS.ADD_POLICY(object_name => 't_v', policy_name => 'POL_V2', policy_function => 'fn_v');\n"
        "  DBMS_REDACT.ADD_POLICY('POLADM', 'T_V', 'RED_V', NULL, 'NINO', NULL, DBMS_REDACT.FULL, NULL, '1=1', TRUE);\n"
        "  DBMS_REDACT.ADD_POLICY(object_name => 'T_V', policy_name => 'RED_V2', column_name => 'NINO', function_type => DBMS_REDACT.PARTIAL);\n"
        "END;",
        want={("POLADM.POL_V", "POLADM.T_V", "defines-on"), ("POLADM.POL_V", "POLADM.FN_V", "calls"),
              ("POLADM.POL_V2", "POLADM.T_V", "defines-on"), ("POLADM.POL_V2", "POLADM.FN_V", "calls"),
              ("POLADM.RED_V", "POLADM.T_V", "defines-on"), ("POLADM.RED_V2", "POLADM.T_V", "defines-on")},
        want_nodes={("POLADM.POL_V", "VPD POLICY"), ("POLADM.POL_V2", "VPD POLICY"), ("POLADM.RED_V", "REDACTION POLICY"),
                    ("POLADM.RED_V2", "REDACTION POLICY")},
        forbid={"POLADM.POLICY_POSITIONAL_AND_DEFAULTED_SCHEMA", "POLADM.NULL"},
    ),
    "sqlplus_includes_spool_host": Case(
        "SET ECHO OFF FEEDBACK OFF\n"
        "WHENEVER SQLERROR EXIT FAILURE ROLLBACK\n"
        "SPOOL run_all.log\n"
        "@@01_tables\n"
        "@lib/02_views.sql\n"
        "START 03_loads 2026-01-01 100\n"
        "  @@04_step.sql\n"
        "@missing_child\n"
        "HOST rm -f run_all.tmp\n"
        "!ls -l\n"
        "SELECT count(*) FROM poladm.t_i;\n"
        "SPOOL OFF\n"
        "EXIT\n",
        files={
            "01_tables.sql": "CREATE TABLE poladm.t_i (id NUMBER);\n",
            "lib/02_views.sql": "CREATE OR REPLACE VIEW poladm.v_i AS SELECT id FROM poladm.t_i;\n",
            "03_loads.sql": "INSERT INTO poladm.t_i SELECT 1 FROM dual;\nCOMMIT;\n",
            "04_step.sql": "SPOOL &out\n@@05_leaf\nSPO OFF\n",
            "05_leaf.sql": "CREATE TABLE poladm.t_leaf (id NUMBER);\n",
        },
        want={("POLADM.SQLPLUS_INCLUDES_SPOOL_HOST", "POLADM.01_TABLES", "includes"),
              ("POLADM.SQLPLUS_INCLUDES_SPOOL_HOST", "POLADM.02_VIEWS", "includes"),
              ("POLADM.SQLPLUS_INCLUDES_SPOOL_HOST", "POLADM.03_LOADS", "includes"),
              ("POLADM.SQLPLUS_INCLUDES_SPOOL_HOST", "POLADM.04_STEP", "includes"),
              ("POLADM.04_STEP", "POLADM.05_LEAF", "includes"),
              ("POLADM.SQLPLUS_INCLUDES_SPOOL_HOST", "FILE.RUN_ALL.LOG", "writes"),
              ("POLADM.SQLPLUS_INCLUDES_SPOOL_HOST", "POLADM.T_I", "reads")},
        want_evidence={("POLADM.SQLPLUS_INCLUDES_SPOOL_HOST", "POLADM.01_TABLES", "includes", "FACT", ""),
                       ("POLADM.04_STEP", "POLADM.05_LEAF", "includes", "FACT", ""),
                       ("POLADM.SQLPLUS_INCLUDES_SPOOL_HOST", "POLADM.MISSING_CHILD", "includes", "INFERRED", "missing-include"),
                       ("POLADM.SQLPLUS_INCLUDES_SPOOL_HOST", "OS.<shell>", "calls", "INFERRED", "os-shell"),
                       ("POLADM.04_STEP", "POLADM.<&spool>", "writes", "INFERRED", "substitution-in-identifier")},
        want_nodes={("POLADM.SQLPLUS_INCLUDES_SPOOL_HOST", "SQLPLUS_SCRIPT"), ("POLADM.04_STEP", "SQLPLUS_SCRIPT"),
                    ("POLADM.01_TABLES", "SQL FILE"), ("POLADM.03_LOADS", "DML SCRIPT"), ("POLADM.02_VIEWS", "SQL FILE")},
        forbid={"FILE.OFF", "POLADM.OFF", "POLADM.EXIT", "POLADM.RM"},
    ),
    "dynamic_sql_whole_literal_variables": Case(
        "CREATE OR REPLACE PROCEDURE poladm.p_dyn (c OUT SYS_REFCURSOR, p IN NUMBER) IS\n"
        "  l_sql VARCHAR2(4000);\n"
        "  l_q   CLOB := 'SELECT id FROM poladm.t_q WHERE id = :1';\n"
        "  l_d   VARCHAR2(200 CHAR) DEFAULT 'DELETE FROM poladm.t_d WHERE id = :1';\n"
        "  l_pre VARCHAR2(4000);\n"
        "  l_cat VARCHAR2(4000);\n"
        "BEGIN\n"
        "  l_sql := 'INSERT INTO poladm.t_dyn SELECT * FROM poladm.t_src WHERE x = ''lit''';\n"
        "  EXECUTE IMMEDIATE l_sql;\n"
        "  OPEN c FOR l_q USING p;\n"
        "  OPEN c FOR l_q;\n"
        "  EXECUTE IMMEDIATE l_d USING p;\n"
        "  l_pre := 'SELECT * FROM poladm.t_pre';\n"
        "  l_pre := l_pre || ' WHERE id = ' || p;\n"
        "  EXECUTE IMMEDIATE l_pre;\n"
        "  l_cat := 'DELETE FROM ' || 't_' || p;\n"
        "  EXECUTE IMMEDIATE l_cat;\n"
        "END;",
        want={("POLADM.P_DYN", "POLADM.T_DYN", "writes"), ("POLADM.P_DYN", "POLADM.T_SRC", "reads"),
              ("POLADM.P_DYN", "POLADM.T_Q", "reads"), ("POLADM.P_DYN", "POLADM.T_D", "writes")},
        want_evidence={("POLADM.P_DYN", "POLADM.<L_PRE>", "reads", "INFERRED", "dynamic-sql"),
                       ("POLADM.P_DYN", "POLADM.<L_CAT>", "writes", "INFERRED", "dynamic-sql")},
        forbid={"POLADM.<L_SQL>", "POLADM.<L_Q>", "POLADM.<L_D>", "POLADM.T_PRE"},
    ),
})


def selftest() -> int:
    failures: list[str] = []
    with tempfile.TemporaryDirectory() as td:
        for name, (sql, risk) in NEGATIVE_CASES.items():
            d = Path(td) / name
            d.mkdir()
            (d / f"{name}.sql").write_text(sql + "\n/\n")
            res = run(d, None)
            hits = [e for e in res["edges"] if e.evidence == "UNVERIFIABLE"]
            if not hits:
                failures.append(f"{name}: expected an UNVERIFIABLE edge ({risk}), got none")
            elif risk not in {e.risk for e in hits}:
                failures.append(f"{name}: expected risk {risk}, got {sorted({e.risk for e in hits})}")
        d = Path(td) / "positive"
        d.mkdir()
        (d / "positive.sql").write_text(POSITIVE_TEXT)
        res = run(d, None)
        bad = [e for e in res["edges"] if e.evidence == "UNVERIFIABLE"]
        if bad:
            failures.append("positive: supported syntax flagged: " + "; ".join(f"{e.risk} [{e.detail}]" for e in bad))
        kinds = {(e.kind, e.dst) for e in res["edges"] if e.src == "POLADM.P_OK"}
        if ("writes", "POLADM.T_OK") not in kinds or ("reads", "POLADM.T_OK") not in kinds:
            failures.append(f"positive: expected reads+writes of POLADM.T_OK, got {sorted(kinds)}")
        for name, case in POSITIVE_CASES.items():
            d = Path(td) / name
            d.mkdir()
            (d / f"{name}.sql").write_text(case.sql + "\n/\n")
            for rel, body in case.files.items():
                (d / rel).parent.mkdir(parents=True, exist_ok=True)
                (d / rel).write_text(body)
            res = run(d, None)
            have = {(e.src, e.dst, e.kind) for e in res["edges"]}
            for w in sorted(case.want - have):
                failures.append(f"{name}: missing edge {w}; got {sorted(have)}")
            for w in sorted(case.forbid_edges & have):
                failures.append(f"{name}: wrongly attributed edge {w}")
            graded = {(e.src, e.dst, e.kind, e.evidence, e.risk) for e in res["edges"]}
            for w in sorted(case.want_evidence - graded):
                failures.append(f"{name}: missing edge {w}; got {sorted(g for g in graded if g[:3] == w[:3])}")
            for k in sorted(case.forbid & set(res["nodes"])):
                failures.append(f"{name}: phantom node {k} ({res['nodes'][k].cls}, {res['nodes'][k].status})")
            for k, cls in sorted(case.want_nodes):
                n = res["nodes"].get(k)
                if n is None or n.cls != cls or n.status != "enumerated":
                    failures.append(f"{name}: census row {k} ({cls}) missing, got {(n.cls, n.status) if n else None}")
            bad = [e for e in res["edges"] if e.evidence == "UNVERIFIABLE"]
            if bad:
                failures.append(f"{name}: supported syntax flagged: " + "; ".join(f"{e.risk} [{e.detail}]" for e in bad))
    # real fixture: zero UNVERIFIABLE, transitive trigger fan-out present, no Albion-only nodes without Albion
    res = run(FIXTURE, None)
    if any(e.evidence == "UNVERIFIABLE" for e in res["edges"]):
        failures.append("fixture: UNVERIFIABLE edges present: " + "; ".join(
            f"{e.src} {e.risk} [{e.detail}]" for e in res["edges"] if e.evidence == "UNVERIFIABLE"))
    if any(k.startswith("TERADATA.") for k in res["nodes"]):
        failures.append("fixture-only run contains the Albion TERADATA replication node")
    want = {("POLADM.09_MRG_POLICY_FROM_STG", "POLADM.POLICY_AUDIT_LOG", "writes"),
            ("POLADM.09_MRG_POLICY_FROM_STG", "POLADM.AUDIT_SEQ", "consumes-sequence"),
            ("POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING", "POLADM.POLICY_AUDIT_LOG", "writes")}
    have = {(e.src, e.dst, e.kind) for e in res["edges"]}
    for w in sorted(want - have):
        failures.append(f"fixture: missing transitive trigger fan-out edge {w}")
    # member effects live on the member the scheduler program calls, not on the package node
    for w in sorted({e for e in have if e[0] == "POLADM.PKG_POLICY_RENEWAL" and e[2] in ("writes", "consumes-sequence")}):
        failures.append(f"fixture: package-level attribution of a member effect {w}")
    if not any(e.src == "POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING" and e.dst == "POLADM.POLICY_AUDIT_LOG"
               for e in side_effect_closure(SimpleNamespace(nodes=res["nodes"], edges=res["edges"]), "POLADM.PRG_NIGHTLY_RENEWAL")):
        failures.append("fixture: PRG_NIGHTLY_RENEWAL's closure does not reach RENEW_EXPIRING's trigger fan-out")
    if any(k.startswith("POLADM.13_JOB") or k.startswith("POLADM.15_SYN") for k in res["nodes"]):
        failures.append("fixture: a DBMS_SCHEDULER / DBMS_RLS / DBMS_REDACT-only anonymous block became a script row")
    job = res["nodes"].get("POLADM.JOB_NIGHTLY_RENEWAL")
    want_start = "TO_TIMESTAMP_TZ('2019-04-01 02:40:00 Europe/London', 'YYYY-MM-DD HH24:MI:SS TZR')"
    if not job or job.signals.get("start_date") != want_start:
        failures.append(f"fixture: scheduler start_date truncated: {job.signals.get('start_date') if job else None!r}")
    if any(k in res["nodes"] for k in ("POLADM.H", "ODS.H")):
        failures.append("fixture: CTE alias became a node")
    if ALBION_DEFAULT.exists():
        res = run(FIXTURE, ALBION_DEFAULT)
        if not any(e.src == "TERADATA.STG_POLICY_360" for e in res["edges"]):
            failures.append("albion run: replication edge missing")
        if any(e.evidence == "UNVERIFIABLE" for e in res["edges"]):
            failures.append("albion run: UNVERIFIABLE edges present")
    for f in failures:
        print("FAIL", f)
    print(f"selftest: {len(NEGATIVE_CASES)} negative cases, {1 + len(POSITIVE_CASES)} positive cases, fixture checks -> {'FAIL' if failures else 'OK'}")
    return 1 if failures else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixture", type=Path, default=FIXTURE)
    ap.add_argument("--albion", type=Path, default=ALBION_DEFAULT)
    ap.add_argument("--report", type=Path, default=HERE / "round_trip_report.md")
    ap.add_argument("--json", action="store_true", help="print edges as JSON instead of the summary")
    ap.add_argument("--selftest", action="store_true", help="run the negative/positive syntax cases and fixture invariants")
    a = ap.parse_args()
    if a.selftest:
        return selftest()
    res = run(a.fixture, a.albion if a.albion.exists() else None)
    text, unverifiable = report(res)
    if a.report:
        a.report.write_text(text)
    if a.json:
        print(json.dumps([asdict(e) for e in res["edges"]], indent=1))
    else:
        print("\n".join(text.splitlines()[:8]))
        if unverifiable:
            print("UNVERIFIABLE edges (unsupported lineage-bearing syntax):")
            for e in res["edges"]:
                if e.evidence == "UNVERIFIABLE":
                    print(f"  {e.src} {e.kind} {e.risk}: {e.detail}")
        print(f"report -> {a.report}")
    return 1 if unverifiable else 0


if __name__ == "__main__":
    sys.exit(main())
