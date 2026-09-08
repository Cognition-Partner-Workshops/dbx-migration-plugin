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

HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "fixture"
ALBION_DEFAULT = HERE.parents[3] / "albion-insurance-data-estate" / "api_legacy" / "plsql" / "pkg_policy_inquiry.sql"

SQLPLUS_DIRECTIVE = re.compile(
    r"^(SET\s+(PAGESIZE|LINESIZE|DEFINE|FEEDBACK|VERIFY|HEADING|ECHO|SERVEROUTPUT|TERMOUT|TRIMSPOOL|TIMING|AUTOCOMMIT|SQLBLANKLINES)"
    r"|SPOOL|WHENEVER|DEFINE|COLUMN|EXEC(UTE)?\s|@@?|PROMPT|ACCEPT|TTITLE|BREAK|COMPUTE)\b", re.I)
QQUOTE_RE = re.compile(r"q'([\[({<])(.*?)[\])}>]'", re.I | re.S)
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


QQUOTE_CLOSER = {"[": "]", "(": ")", "{": "}", "<": ">"}


def strip_comments(text: str) -> str:
    """Lexical comment removal. '...' ('' escape), q'[...]' (any delimiter) and "quoted identifiers" are opaque,
    so `--` or `/*` inside a literal never swallows the SQL that follows it; comments become spaces, newlines are kept."""
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
        if ch in "qQ" and i + 2 < n and text[i + 1] == "'" and (i == 0 or not re.match(r"[\w$#]", text[i - 1])):
            closer = QQUOTE_CLOSER.get(text[i + 2], text[i + 2]) + "'"
            j = text.find(closer, i + 3)
            j = n if j < 0 else j + 2
            out.append(text[i:j])
            i = j
            continue
        if ch == "'":
            j = i + 1
            while j < n:
                if text[j] == "'":
                    if j + 1 < n and text[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            j = min(j + 1, n)
            out.append(text[i:j])
            i = j
            continue
        if ch == '"':
            j = text.find('"', i + 1)
            j = n if j < 0 else j + 1
            out.append(text[i:j])
            i = j
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def named_args(text: str) -> list[tuple[str, str]]:
    """`name => value` pairs of a PL/SQL call, splitting on top-level commas only: a value may be a full expression
    such as TO_TIMESTAMP_TZ('...', '...') (balanced parentheses, '...' and q'[...]' literals are opaque)."""
    parts: list[str] = []
    depth, start, i, n = 0, 0, 0, len(text)
    while i < n:
        ch = text[i]
        if ch in "qQ" and i + 2 < n and text[i + 1] == "'" and (i == 0 or not re.match(r"[\w$#]", text[i - 1])):
            j = text.find(QQUOTE_CLOSER.get(text[i + 2], text[i + 2]) + "'", i + 3)
            i = n if j < 0 else j + 2
            continue
        if ch == "'":
            j = i + 1
            while j < n:
                if text[j] == "'":
                    if j + 1 < n and text[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            i = min(j + 1, n)
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
        i += 1
    parts.append(text[start:])
    out: list[tuple[str, str]] = []
    for p in parts:
        m = re.match(r"\s*(\w+)\s*=>\s*(.*?)\s*$", p, re.S)
        if m:
            out.append((m.group(1), m.group(2)))
    return out


def lift_qquotes(text: str) -> tuple[str, dict[str, str]]:
    """Replace q'[...]' literals with plain placeholders so argument parsing is not derailed by ');' inside them."""
    lifted: dict[str, str] = {}

    def _sub(m: re.Match) -> str:
        tok = f"'@@Q{len(lifted)}@@'"
        lifted[tok.strip("'")] = m.group(2)
        return tok

    return QQUOTE_RE.sub(_sub, text), lifted


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
        if key in self.nodes:
            return key, "FACT", ""
        _owner, bare = key.split(".", 1)
        # Oracle resolution order for an UNQUALIFIED name: own-schema object, private synonym, public synonym.
        # A qualified OWNER.NAME never falls through to a same-named public synonym.
        syn = self.synonyms.get(key) or (self.public_synonyms.get(bare) if "." not in raw else None)
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

    def edge(self, src: str, raw_dst: str, kind: str, default_owner: str, detail: str = "") -> None:
        dst, evidence, risk = self.resolve(raw_dst, default_owner)
        self.edges.append(Edge(src, dst, kind, evidence, risk, detail))


# --------------------------------------------------------------------------- census (section 1)

CREATE_RE = re.compile(
    rf"CREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+TEMPORARY\s+|PUBLIC\s+|EDITIONABLE\s+)?"
    rf"(MATERIALIZED\s+VIEW\s+LOG\s+ON|MATERIALIZED\s+VIEW|PACKAGE\s+BODY|DATABASE\s+LINK|SEQUENCE|TABLE|INDEX|VIEW|"
    rf"PROCEDURE|FUNCTION|PACKAGE|TRIGGER|SYNONYM|ROLE)\s+({QNAME})",
    re.I,
)
PUBLIC_SYN_RE = re.compile(rf"CREATE\s+(?:OR\s+REPLACE\s+)?PUBLIC\s+SYNONYM\s+({IDENT})\s+FOR\s+({QNAME}(?:@{IDENT})?)", re.I)
PRIV_SYN_RE = re.compile(rf"CREATE\s+(?:OR\s+REPLACE\s+)?SYNONYM\s+({QNAME})\s+FOR\s+({QNAME}(?:@{IDENT})?)", re.I)
MEMBER_RE = re.compile(rf"^\s*(PROCEDURE|FUNCTION)\s+({IDENT})", re.I | re.M)
SCHED_RE = re.compile(r"DBMS_SCHEDULER\.CREATE_(JOB|PROGRAM)\s*\((.*?)\);", re.I | re.S)
RLS_RE = re.compile(r"DBMS_RLS\.ADD_POLICY\s*\((.*?)\);", re.I | re.S)
REDACT_RE = re.compile(r"DBMS_REDACT\.ADD_POLICY\s*\((.*?)\);", re.I | re.S)
GRANT_RE = re.compile(rf"GRANT\s+([A-Z ,()_]+?)\s+ON\s+({QNAME})\s+TO\s+({IDENT})", re.I)
ROLE_GRANT_RE = re.compile(rf"GRANT\s+({IDENT})\s+TO\s+({IDENT})\s*;", re.I)


def split_units(sql: str) -> list[str]:
    """Split on SQL*Plus '/' terminators, keeping PL/SQL blocks whole."""
    return [u for u in re.split(r"^\s*/\s*$", sql, flags=re.M) if u.strip()]


def census_file(est: Estate, path: Path, default_owner: str, sqlplus_units: list, proc_units: list, grants: list) -> None:
    raw = path.read_text()
    text, qlits = lift_qquotes(strip_comments(raw))
    fname = path.name
    is_sqlplus = any(SQLPLUS_DIRECTIVE.match(ln) for ln in raw.splitlines())
    if is_sqlplus:
        key = f"{default_owner}.{path.stem.upper()}"
        est.add(key, "SQLPLUS_SCRIPT", fname,
                lines=len(raw.splitlines()), substitution_vars=len(set(re.findall(r"&&?(\w+)", raw))))
        sqlplus_units.append((key, text))
    for m in PUBLIC_SYN_RE.finditer(text):
        est.public_synonyms[m.group(1).upper()] = norm(m.group(2).split("@")[0], default_owner) + (
            "@" + m.group(2).split("@")[1].upper() if "@" in m.group(2) else "")
        est.add(f"PUBLIC.{m.group(1).upper()}", "PUBLIC SYNONYM", fname)
    for m in PRIV_SYN_RE.finditer(text):
        if re.search(r"PUBLIC\s+SYNONYM\s+" + re.escape(m.group(1)), text, re.I):
            continue
        est.synonyms[norm(m.group(1), default_owner)] = norm(m.group(2).split("@")[0], default_owner) + (
            "@" + m.group(2).split("@")[1].upper() if "@" in m.group(2) else "")
        est.add(norm(m.group(1), default_owner), "SYNONYM", fname)
    for m in CREATE_RE.finditer(text):
        cls = re.sub(r"\s+", " ", m.group(1).upper())
        name = m.group(2)
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
            body = text[m.end():]
            body = body[: body.find(";")]
            cols = re.findall(r"^\s*(\w+)\s+(NUMBER(?!\s*\()|NUMBER\s*\([^)]*\)|DATE|CHAR\s*\(\d+\)|TIMESTAMP[^,]*|RAW\s*\(\d+\)|CLOB|BINARY_DOUBLE|INTERVAL[^,]*)",
                              body, re.I | re.M)
            node.signals["type_traps"] = sorted({re.sub(r"\s+", " ", c[1].upper()) for c in cols if
                                                 re.match(r"NUMBER$|DATE|CHAR|TIMESTAMP|RAW|CLOB|BINARY|INTERVAL", c[1], re.I)})
            node.signals["constraints"] = len(re.findall(r"CONSTRAINT\s+\w+", body, re.I))
            node.signals["temporary"] = bool(re.search(r"GLOBAL\s+TEMPORARY\s+TABLE\s+" + re.escape(name), text, re.I))
        if cls in ("PACKAGE", "PACKAGE BODY", "PROCEDURE", "FUNCTION", "TRIGGER"):
            unit_text = text[m.start():]
            nxt = CREATE_RE.search(text, m.end())
            unit_text = unit_text[: (nxt.start() - m.start()) if nxt else None]
            node.signals["lines"] = unit_text.count("\n")
            proc_units.append((key, cls, unit_text))
            if cls == "PACKAGE":
                for mm in MEMBER_RE.finditer(unit_text):
                    est.add(f"{key}.{mm.group(2).upper()}", f"PACKAGE {mm.group(1).upper()}", fname)
            if cls == "PACKAGE BODY":
                for mm in MEMBER_RE.finditer(unit_text):
                    est.add(f"{key}.{mm.group(2).upper()}", f"PACKAGE {mm.group(1).upper()}", fname)
        if cls in ("VIEW", "MATERIALIZED VIEW"):
            unit_text = text[m.start():]
            unit_text = unit_text[: unit_text.find(";") + 1]
            proc_units.append((key, cls, unit_text))
            if cls == "MATERIALIZED VIEW":
                node.signals["refresh"] = " ".join(re.findall(r"REFRESH\s+(\w+)\s+ON\s+(\w+)", unit_text, re.I)[0]) if re.search(
                    r"REFRESH\s+\w+\s+ON", unit_text, re.I) else "?"
    for m in SCHED_RE.finditer(text):
        args = {k.lower(): v.strip().strip("'") for k, v in named_args(m.group(2))}
        name = args.get("job_name") or args.get("program_name")
        cls = "SCHEDULER " + m.group(1).upper()
        key = norm(name, default_owner)
        est.add(key, cls, fname, repeat_interval=args.get("repeat_interval", ""), start_date=args.get("start_date", ""))
        action = args.get("program_action") or args.get("job_action") or ""
        action = qlits.get(action, action)
        if args.get("program_name") and m.group(1).upper() == "JOB":
            est.edges.append(Edge(key, norm(args["program_name"], default_owner), "schedules", "FACT"))
        if action:
            proc_units.append((key, cls, action.replace("''", "'")))
    for m in RLS_RE.finditer(text):
        args = {k.lower(): v.strip().strip("'") for k, v in named_args(m.group(1))}
        key = f"{args['object_schema']}.{args['policy_name']}".upper()
        est.add(key, "VPD POLICY", fname)
        est.edges.append(Edge(key, f"{args['object_schema']}.{args['object_name']}".upper(), "defines-on", "FACT"))
        est.edges.append(Edge(key, f"{args['function_schema']}.{args['policy_function']}".upper(), "calls", "FACT"))
    for m in REDACT_RE.finditer(text):
        args = {k.lower(): v.strip().strip("'") for k, v in named_args(m.group(1))}
        key = f"{args['object_schema']}.{args['policy_name']}".upper()
        est.add(key, "REDACTION POLICY", fname, column=args.get("column_name"))
        est.edges.append(Edge(key, f"{args['object_schema']}.{args['object_name']}".upper(), "defines-on", "FACT"))
    for m in GRANT_RE.finditer(text):
        priv, obj, grantee = m.groups()
        key = f"GRANT.{grantee.upper()}.{norm(obj, default_owner)}.{re.sub(r'[^A-Z]', '', priv.upper().split('(')[0])}"
        est.add(key, "GRANT", fname, public=grantee.upper() == "PUBLIC", column_level="(" in priv)
        grants.append((key, obj, default_owner))
    for m in ROLE_GRANT_RE.finditer(text):
        est.add(f"ROLEGRANT.{m.group(2).upper()}.{m.group(1).upper()}", "ROLE MEMBERSHIP", fname)
    # loose MERGE / DML files (no CREATE header) become a unit named after the file
    if not is_sqlplus and not CREATE_RE.search(text) and re.search(r"\bMERGE\s+INTO\b|\bINSERT\s+INTO\b", text, re.I):
        key = f"{default_owner}.{path.stem.upper()}"
        est.add(key, "DML SCRIPT", fname)
        proc_units.append((key, "DML SCRIPT", text))
    elif re.search(r"\bMERGE\s+INTO\b", text, re.I) and not re.search(r"CREATE\s+(OR\s+REPLACE\s+)?(PACKAGE|PROCEDURE|FUNCTION|TRIGGER)", text, re.I):
        key = f"{default_owner}.{path.stem.upper()}"
        est.add(key, "DML SCRIPT", fname)
        proc_units.append((key, "DML SCRIPT", text))


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
OPEN_FOR_VAR_RE = re.compile(rf"OPEN\s+{IDENT}\s+FOR\s+({IDENT})\s*;", re.I)
DYN_ASSIGN_RE = re.compile(rf"({IDENT})\s*:=\s*('(?:[^']|'')*')\s*\|\|", re.I)
ANY_ASSIGN_RE = re.compile(rf"({IDENT})\s*:=", re.I)
MVIEW_REFRESH_RE = re.compile(r"DBMS_MVIEW\.REFRESH\s*\(\s*(?:list\s*=>\s*)?'([^']+)'", re.I)
MVIEW_REFRESH_ANY_RE = re.compile(r"DBMS_MVIEW\.REFRESH\s*\(", re.I)
SUBST_IDENT_RE = re.compile(r"(?:FROM|JOIN|INTO|UPDATE)\s+&&?\w+", re.I)
STRING_LIT_RE = re.compile(r"'(?:[^']|'')*'")
# completeness pass: every lineage-bearing keyword and what may legitimately follow it
LINEAGE_KW_RE = re.compile(
    r"\b(INSERT\s+INTO|MERGE\s+INTO|TRUNCATE\s+TABLE|DELETE\s+FROM|DELETE|UPDATE|FROM|JOIN)\s*(\S{0,40})", re.I)
NEXT_IDENT_RE = re.compile(rf"^{QNAME}(?:@{IDENT})?\b")


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


def lineage_unit(est: Estate, key: str, cls: str, text: str, default_owner: str) -> None:
    owner = key.split(".")[0] if "." in key else default_owner
    # dynamic SQL first (on the raw text, so string-literal statements are visible)
    dyn_vars = {m.group(1).upper(): m.group(2) for m in DYN_ASSIGN_RE.finditer(text)}
    assigned = {m.group(1).upper() for m in ANY_ASSIGN_RE.finditer(text)}
    exec_imm_seen = 0
    for m in EXEC_IMM_RE.finditer(text):
        exec_imm_seen += 1
        arg = m.group(1)
        if arg.startswith("'") and not m.group(2):
            lineage_unit(est, key, cls, arg.strip("'").replace("''", "'"), default_owner)  # literal: parse as static
        elif arg.startswith("'"):
            unverifiable(est, key, owner, "writes", "dynamic-sql-expression", m.group(0))   # 'lit' || expr: shape unknown
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
        if vm and vm.group(1).upper() in dyn_vars.keys() | assigned:
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
    if cls == "FUNCTION" and re.search(r"RETURN\s+'", text, re.I) and re.search(r"SYS_CONTEXT", text, re.I):
        for lit in STRING_LIT_RE.findall(text):
            for rm in READ_RE.finditer(lit):
                dst, _, _ = est.resolve(rm.group(1), owner)
                est.edges.append(Edge(key, dst, "reads", "INFERRED", "dynamic-predicate", "VPD predicate string"))
    # static SQL: work on text with string literals blanked so literals never look like identifiers
    static = STRING_LIT_RE.sub("''", text)
    if cls == "TRIGGER":
        tm = TRIGGER_ON_RE.search(static)
        if tm:
            est.edge(key, tm.group(1), "defines-on", owner)
            est.edge(key, tm.group(1), "writes", owner, ":NEW in-flight row")
    for stmt in static.split(";"):                      # CTE names are statement-local: scope them per statement
        ctes = {m.group(1).upper() for m in CTE_RE.finditer(stmt)}
        for m in READ_RE.finditer(stmt):
            name = m.group(1)
            if name.split(".")[-1].upper() in KEYWORDS or name.upper().startswith("TABLE("):
                continue
            if "." not in name and name.upper() in ctes:
                continue                                 # a WITH-clause alias, not a physical object (its body is scanned)
            est.edge(key, name, "reads", owner)
    for m in WRITE_RE.finditer(static):
        if m.group(1).split(".")[-1].upper() in KEYWORDS | {"OF", "FROM"}:
            continue  # `UPDATE ON t`, `UPDATE OF col`, `UPDATE SET` inside MERGE are not writes
        if preceding_word(static, m.start()) in ("FOR", "THEN", "OR", "BEFORE", "AFTER") or m.group(1).upper().endswith(".DELETE"):
            continue  # FOR UPDATE t?, trigger event lists, collection.DELETE
        est.edge(key, m.group(1), "writes", owner)
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
    for m in UNQUAL_CALL_RE.finditer(static):
        cand = f"{owner}.{m.group(1).upper()}"
        if cand in est.nodes and est.nodes[cand].cls in PROCEDURAL_CLASSES and cand != key and not key.startswith(cand + "."):
            est.edges.append(Edge(key, cand, "calls", "FACT", "", "unqualified call resolved against the census"))
    completeness_pass(est, key, owner, static)


def run(fixture_dir: Path, albion: Path | None) -> dict:
    est = Estate()
    sqlplus_units: list = []
    proc_units: list = []
    grants: list = []
    files = sorted(fixture_dir.glob("*.sql"))
    for p in files:
        census_file(est, p, "POLADM", sqlplus_units, proc_units, grants)
    albion_units: list = []
    if albion and albion.exists():
        census_file(est, albion, "ODS", [], albion_units, grants)
    for key, obj, owner in grants:  # resolved after the whole census so cross-file grants land on enumerated nodes
        est.edge(key, obj, "defines-on", owner)
    # lineage
    for key, cls, text in proc_units + albion_units:
        lineage_unit(est, key, cls, text, key.split(".")[0])
    for key, text in sqlplus_units:
        lineage_unit(est, key, "SQLPLUS_SCRIPT", text, key.split(".")[0])
    if albion_units:
        # documented replication feed (architecture_overview.md / package header comment): GoldenGate copy of Teradata STG_POLICY_360
        est.add("TERADATA.STG_POLICY_360", "EXTERNAL_TABLE", "docs").status = "external"
        est.edges.append(Edge("TERADATA.STG_POLICY_360", "ODS.ODS_POLICY_360", "replication", "INFERRED", "freshness",
                              "GoldenGate nightly copy, up to 26h stale (architecture_overview.md)"))
    trigger_fan_out(est)
    # dedupe
    seen, uniq = set(), []
    for e in est.edges:
        k = (e.src, e.dst, e.kind, e.evidence, e.risk)
        if k not in seen:
            seen.add(k)
            uniq.append(e)
    est.edges = uniq
    return {"nodes": est.nodes, "edges": est.edges, "files": [p.name for p in files], "albion": bool(albion_units)}


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
            writers = {w.src for w in est.edges if w.kind == "writes" and w.dst == t.dst and w.src != t.src}
            for w in writers:
                for c in closure:
                    if w in (c.src, c.dst) or (w, c.dst, c.kind) in existing:
                        continue
                    via = f"trigger fan-out via {t.src}" + (f" -> {c.src}" if c.src != t.src else "")
                    est.edges.append(Edge(w, c.dst, c.kind, "FACT", "", via))
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
    out += [f"| {e.src} | {e.dst} | {e.kind} | {e.detail} |" for e in edges if e.evidence == "FACT"]
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
# name: (Oracle text, edges that MUST exist as (src, dst, kind), node keys that must NOT exist)
POSITIVE_CASES: dict[str, tuple[str, set[tuple[str, str, str]], set[str]]] = {
    "cte_multiple": (
        "CREATE OR REPLACE VIEW poladm.v_cte AS\n"
        "WITH recent AS (SELECT * FROM poladm.policy WHERE d > SYSDATE - 7),\n"
        "     agg (n) AS (SELECT count(*) FROM recent r JOIN poladm.broker b ON b.id = r.broker_id)\n"
        "SELECT * FROM agg JOIN recent ON 1 = 1;",
        {("POLADM.V_CTE", "POLADM.POLICY", "reads"), ("POLADM.V_CTE", "POLADM.BROKER", "reads")},
        {"POLADM.RECENT", "POLADM.AGG"},
    ),
    "cte_recursive": (
        "CREATE OR REPLACE VIEW poladm.v_tree AS\n"
        "WITH tree (id, lvl) AS (SELECT id, 1 FROM poladm.broker WHERE parent_id IS NULL\n"
        "  UNION ALL SELECT b.id, t.lvl + 1 FROM tree t JOIN poladm.broker b ON b.parent_id = t.id)\n"
        "SELECT * FROM tree;",
        {("POLADM.V_TREE", "POLADM.BROKER", "reads")},
        {"POLADM.TREE"},
    ),
    "cte_scope_is_per_statement": (
        "CREATE OR REPLACE PROCEDURE poladm.p_cte IS l_n NUMBER; BEGIN\n"
        "  WITH recent AS (SELECT id FROM poladm.policy) SELECT count(*) INTO l_n FROM recent;\n"
        "  SELECT count(*) INTO l_n FROM recent;   -- a real table in the next statement, no WITH in scope\n"
        "END;",
        {("POLADM.P_CTE", "POLADM.POLICY", "reads"), ("POLADM.P_CTE", "POLADM.RECENT", "reads")},
        set(),
    ),
    "qualified_name_ignores_public_synonym": (
        "CREATE OR REPLACE PUBLIC SYNONYM broker FOR poladm.broker;\n"
        "CREATE OR REPLACE VIEW ods.v_a AS SELECT 1 FROM claims.broker;\n"
        "CREATE OR REPLACE VIEW ods.v_b AS SELECT 1 FROM broker;",
        {("ODS.V_A", "CLAIMS.BROKER", "reads"), ("ODS.V_B", "POLADM.BROKER", "reads")},
        set(),
    ),
    "comment_markers_inside_literals": (
        "CREATE OR REPLACE PROCEDURE poladm.p_lit IS l_s VARCHAR2(200); l_n NUMBER; BEGIN\n"
        "  SELECT '-- not a comment' INTO l_s FROM poladm.t_a;\n"
        "  SELECT 'it''s /* not a comment' INTO l_s FROM poladm.t_b;\n"
        "  l_s := q'{ -- q-quoted /* }'; SELECT 1 INTO l_n FROM poladm.t_c;\n"
        "  EXECUTE IMMEDIATE 'DELETE FROM poladm.t_d WHERE note = ''--'' /* ';\n"
        "  SELECT 1 AS \"x -- /* y\" INTO l_n FROM poladm.t_e; /* real comment -- */ SELECT 1 INTO l_n FROM poladm.t_f; -- real\n"
        "END;",
        {("POLADM.P_LIT", f"POLADM.T_{c}", "reads") for c in "ABCEF"} | {("POLADM.P_LIT", "POLADM.T_D", "writes")},
        set(),
    ),
}


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
        for name, (sql, want, forbid) in POSITIVE_CASES.items():
            d = Path(td) / name
            d.mkdir()
            (d / f"{name}.sql").write_text(sql + "\n/\n")
            res = run(d, None)
            have = {(e.src, e.dst, e.kind) for e in res["edges"]}
            for w in sorted(want - have):
                failures.append(f"{name}: missing edge {w}; got {sorted(have)}")
            for k in sorted(forbid & set(res["nodes"])):
                failures.append(f"{name}: phantom node {k} ({res['nodes'][k].cls}, {res['nodes'][k].status})")
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
            ("POLADM.PKG_POLICY_RENEWAL", "POLADM.POLICY_AUDIT_LOG", "writes")}
    have = {(e.src, e.dst, e.kind) for e in res["edges"]}
    for w in sorted(want - have):
        failures.append(f"fixture: missing transitive trigger fan-out edge {w}")
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
