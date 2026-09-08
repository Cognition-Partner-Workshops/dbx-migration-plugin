#!/usr/bin/env python3
"""Static enumeration + lineage round-trip for the oracle-plsql skill (SKILL.md sections 1-2).

Reads Oracle SQL / PL-SQL text only (no engine), builds the census (section 1 repo-file rules)
and the read/write/call edge list (section 2 rules), classifies every edge FACT or INFERRED,
and exits non-zero if any edge ends up UNVERIFIABLE (i.e. a rule failed to classify it).

    python3 skills/oracle-plsql/examples/round_trip.py [--albion PATH] [--report PATH]

Default inputs: examples/fixture/*.sql and, when present, the Albion estate package at
../../../albion-insurance-data-estate/api_legacy/plsql/pkg_policy_inquiry.sql (read-only).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
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


def strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    return "\n".join(re.sub(r"--.*$", "", ln) for ln in text.splitlines())


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
        syn = self.synonyms.get(key) or self.public_synonyms.get(bare)
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
ARG_RE = re.compile(r"(\w+)\s*=>\s*('(?:[^']|'')*'|[^,)]+)", re.S)
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
        args = {k.lower(): v.strip().strip("'") for k, v in ARG_RE.findall(m.group(2))}
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
        args = {k.lower(): v.strip().strip("'") for k, v in ARG_RE.findall(m.group(1))}
        key = f"{args['object_schema']}.{args['policy_name']}".upper()
        est.add(key, "VPD POLICY", fname)
        est.edges.append(Edge(key, f"{args['object_schema']}.{args['object_name']}".upper(), "defines-on", "FACT"))
        est.edges.append(Edge(key, f"{args['function_schema']}.{args['policy_function']}".upper(), "calls", "FACT"))
    for m in REDACT_RE.finditer(text):
        args = {k.lower(): v.strip().strip("'") for k, v in ARG_RE.findall(m.group(1))}
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

READ_RE = re.compile(rf"\b(?:FROM|JOIN)\s+({QNAME}(?:@{IDENT})?)(?!\s*\()", re.I)
WRITE_RE = re.compile(rf"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO)\s+({QNAME})", re.I)
SEQ_RE = re.compile(rf"\b({QNAME})\.(?:NEXTVAL|CURRVAL)\b", re.I)
TRIGGER_ON_RE = re.compile(rf"\bON\s+({QNAME})\s+(?:FOR\s+EACH\s+ROW|REFERENCING|WHEN|DECLARE|BEGIN)", re.I | re.S)
CALL_RE = re.compile(rf"\b({IDENT}\.{IDENT}(?:\.{IDENT})?)\s*\(", re.I)
EXEC_IMM_RE = re.compile(r"EXECUTE\s+IMMEDIATE\s+('(?:[^']|'')*'|[A-Za-z_]\w*)", re.I)
OPEN_FOR_VAR_RE = re.compile(rf"OPEN\s+{IDENT}\s+FOR\s+({IDENT})\s*;", re.I)
DYN_ASSIGN_RE = re.compile(rf"({IDENT})\s*:=\s*('(?:[^']|'')*')\s*\|\|", re.I)
MVIEW_REFRESH_RE = re.compile(r"DBMS_MVIEW\.REFRESH\s*\(\s*(?:list\s*=>\s*)?'([^']+)'", re.I)
SUBST_IDENT_RE = re.compile(r"(?:FROM|JOIN|INTO|UPDATE)\s+&&?\w+", re.I)
STRING_LIT_RE = re.compile(r"'(?:[^']|'')*'")


def lineage_unit(est: Estate, key: str, cls: str, text: str, default_owner: str) -> None:
    owner = key.split(".")[0] if "." in key else default_owner
    # dynamic SQL first (on the raw text, so string-literal statements are visible)
    dyn_vars = {m.group(1).upper(): m.group(2) for m in DYN_ASSIGN_RE.finditer(text)}
    for m in EXEC_IMM_RE.finditer(text):
        arg = m.group(1)
        if arg.startswith("'"):
            lineage_unit(est, key, cls, arg.strip("'").replace("''", "'"), default_owner)  # literal: parse as static
        else:
            prefix = dyn_vars.get(arg.upper(), "")
            pm = re.search(rf"(INSERT\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO|FROM)\s+({QNAME})?", prefix, re.I)
            est.edges.append(Edge(key, f"{owner}.<{arg.upper()}>", "writes" if pm and pm.group(1).upper() != "FROM" else "reads",
                                  "INFERRED", "dynamic-sql", f"literal prefix {prefix.strip()!r}"))
    for m in OPEN_FOR_VAR_RE.finditer(text):
        est.edges.append(Edge(key, f"{owner}.<{m.group(1).upper()}>", "reads", "INFERRED", "dynamic-sql", "OPEN ... FOR <variable>"))
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
    for m in READ_RE.finditer(static):
        name = m.group(1)
        if name.split(".")[-1].upper() in KEYWORDS or name.upper().startswith("TABLE("):
            continue
        est.edge(key, name, "reads", owner)
    for m in WRITE_RE.finditer(static):
        if m.group(1).split(".")[-1].upper() in KEYWORDS | {"OF"}:
            continue  # `UPDATE ON t`, `UPDATE OF col`, `UPDATE SET` inside MERGE are not writes
        est.edge(key, m.group(1), "writes", owner)
    for m in SEQ_RE.finditer(static):
        est.edge(key, m.group(1), "consumes-sequence", owner)
    for m in MVIEW_REFRESH_RE.finditer(static.replace("''", "'") if False else text):
        for mv in m.group(1).split(","):
            est.edge(key, mv.strip(), "calls", owner, "DBMS_MVIEW.REFRESH")
    for m in CALL_RE.finditer(static):
        callee = m.group(1).upper()
        head = callee.split(".")[0]
        if head in ("DBMS_OUTPUT", "DBMS_UTILITY", "DBMS_ASSERT", "DBMS_RLS", "DBMS_REDACT", "DBMS_SCHEDULER", "DBMS_MVIEW", "SYS"):
            continue
        cand = callee if callee in est.nodes else None
        if cand is None and callee.count(".") == 2 and ".".join(callee.split(".")[:2]) in est.nodes:
            cand = callee  # package member not declared separately
        if cand is None and callee.count(".") == 1 and f"{owner}.{callee}" in est.nodes:
            cand = f"{owner}.{callee}"
        if cand and cand != key and not cand.startswith(key + ".") and \
                est.nodes[cand.rsplit(".", 1)[0] if cand not in est.nodes else cand].cls not in ("TABLE", "VIEW", "SEQUENCE", "MATERIALIZED VIEW"):
            est.edges.append(Edge(key, cand, "calls", "FACT"))


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
        # documented replication feed (architecture_overview.md / package header comment): GoldenGate copy of Teradata STG_POLICY_360
        est.add("TERADATA.STG_POLICY_360", "EXTERNAL_TABLE", "docs").status = "external"
    # lineage
    for key, cls, text in proc_units + albion_units:
        lineage_unit(est, key, cls, text, key.split(".")[0])
    for key, text in sqlplus_units:
        lineage_unit(est, key, "SQLPLUS_SCRIPT", text, key.split(".")[0])
    if albion_units:
        est.edges.append(Edge("TERADATA.STG_POLICY_360", "ODS.ODS_POLICY_360", "replication", "INFERRED", "freshness",
                              "GoldenGate nightly copy, up to 26h stale (architecture_overview.md)"))
    # trigger fan-out: writers of a trigger's base table inherit the trigger's writes
    for e in [e for e in est.edges if e.kind == "defines-on" and est.nodes[e.src].cls == "TRIGGER"]:
        trig_writes = [w for w in est.edges if w.src == e.src and w.kind in ("writes", "consumes-sequence", "calls") and w.dst != e.dst]
        for w in [w for w in est.edges if w.kind == "writes" and w.dst == e.dst and w.src != e.src]:
            for tw in trig_writes:
                est.edges.append(Edge(w.src, tw.dst, tw.kind, "FACT", "", f"trigger fan-out via {e.src}"))
    # dedupe
    seen, uniq = set(), []
    for e in est.edges:
        k = (e.src, e.dst, e.kind, e.evidence, e.risk)
        if k not in seen:
            seen.add(k)
            uniq.append(e)
    est.edges = uniq
    return {"nodes": est.nodes, "edges": est.edges, "files": [p.name for p in files], "albion": bool(albion_units)}


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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixture", type=Path, default=FIXTURE)
    ap.add_argument("--albion", type=Path, default=ALBION_DEFAULT)
    ap.add_argument("--report", type=Path, default=HERE / "round_trip_report.md")
    ap.add_argument("--json", action="store_true", help="print edges as JSON instead of the summary")
    a = ap.parse_args()
    res = run(a.fixture, a.albion if a.albion.exists() else None)
    text, unverifiable = report(res)
    if a.report:
        a.report.write_text(text)
    if a.json:
        print(json.dumps([asdict(e) for e in res["edges"]], indent=1))
    else:
        print("\n".join(text.splitlines()[:8]))
        print(f"report -> {a.report}")
    return 1 if unverifiable else 0


if __name__ == "__main__":
    sys.exit(main())
