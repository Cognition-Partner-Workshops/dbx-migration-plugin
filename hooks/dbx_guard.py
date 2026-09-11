#!/usr/bin/env python3
"""PreToolUse guard for the DBX migration factory: recognise the client, allow known-read shapes,
block the rest.

Reads a Devin PreToolUse event on stdin ({"tool_name", "tool_input": {"command"}}), locates the
engagement's `.migration/allowed_targets.json`, splits the shell command into simple commands and
judges each one by the program it runs:

* `databricks`: only an explicit read allowlist passes (`current-user me`, `<securable> list|get`,
  `sql execute`, `api get`, `bundle validate|summary`, `fs ls|cat|head`, `jobs|pipelines|
  warehouses|clusters list|get...`, `workspace list|export|get-status`, `secrets list-scopes|
  list-secrets`, `auth describe|env|token`, `--version`, `-h`). A mutation passes only when the
  securable it names sits in an allowlisted catalog (`tables delete mig_cat.s.t`, `fs rm
  dbfs:/Volumes/mig_cat/...`). SQL handed to `sql execute` / `spark-sql` / `dbsqlcli` must write
  only to allowlisted catalogs: three-part names, `USE CATALOG`/`--catalog` per segment, and a
  write that resolves to no catalog blocks, as do `IDENTIFIER(<non-literal>)` in a write and
  `EXECUTE IMMEDIATE <non-literal>`.
* `curl`/`wget`/`http` to `$DATABRICKS_HOST` or a *.databricks.com / *.azuredatabricks.net host:
  GET without a body only.
* `databricks bundle deploy|run|destroy` and `dbt run|build|seed`: a literal `-t/--target` that is
  in `bundle_targets` and not in `forbidden_bundle_targets`; anything else blocks.
* Identity: `databricks auth login|configure`, `--profile/-p/--host`, and `DATABRICKS_TOKEN=`,
  `DATABRICKS_HOST=`, `DATABRICKS_CONFIG_PROFILE=`, `DATABRICKS_CLIENT_*=` (inline, `export`,
  `env`) around a `databricks`, `dbx-recon` or `spark-sql` command block.
* Legacy-only clients (bteq, sqlplus, snowsql, fastexport) and generic SQL clients (sqlcmd, psql,
  mysql, isql, ...) whose command mentions a `legacy_sources` entry: every statement must be a
  read shape (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, HELP, USE, DECLARE, SET <session option>, a
  client directive) with no write keyword anywhere in it. Loaders (sqlldr, mload, fastload,
  tbuild, tdload, `bcp ... in`) always block.
* Generic SQL clients elsewhere: a non-read statement needs a host or DSN name that is a literal
  in `target_hosts`; a variable, an IP, an unlisted name or an empty list blocks.
* Any write under `.migration/` (redirects, `sed -i`, `tee`, `cp/mv/rm/rmdir/truncate/chmod/
  mkdir/touch`, `git checkout|restore|rm`, inline Python naming a `.migration/` path with a write
  call) blocks, except under `.migration/recon/` and `.migration/waves/` (harness and workflow
  outputs). Reads stay approved.
* `python x.py` / `spark-submit x.py` / `python -c` / a Python heredoc: a literal SQL string
  handed to `.execute(`, `.sql(`, `execute_statement(` or `statement=` is judged like the client
  above (legacy read shapes when the text mentions a legacy source, allowlisted catalogs
  otherwise); anything else in a program approves. A `spark-submit` script that cannot be read
  blocks. Programs that open their own connection (Python, JDBC, perl) are otherwise covered by
  the doctor's read-only source-principal row, not by this hook.

Config (`.migration/allowed_targets.json`, written at setup, committed before STOP A):

    {
      "catalogs": ["migration_cat"],                       # required; dbx-recon reads it too
      "legacy_sources": ["LEGACY_TD_DSN", "tdprod.corp"],  # secret names / hosts / DSNs / profiles
      "guard_mode": "block",                               # block (default) | warn
      "target_hosts": ["fixture-host", "LAKEBASE_DSN"],    # hosts / DSN names generic clients may write to
      "bundle_targets": ["migration", "dev"],              # bundle/dbt targets that may be deployed
      "forbidden_bundle_targets": ["prod", "production"]   # extra denylist (default shown)
    }

`target_hosts` and `bundle_targets` are optional and fail closed: missing or empty, every
generic-client write and every bundle/dbt deploy blocks.

Text the guard reads: the command, shell scripts it runs (`bash x.sh`, `source x`), SQL files
handed to a client (`< f`, `-f f`, `@f`), heredoc bodies, here-strings, literal `echo`/`printf`/
`cat` producers piped in, `sh -c '...'`, `ssh host '...'`, `docker exec ...` and `alias` bodies.
Text it cannot read blocks where a client is involved: an unreadable or oversized script, command
substitution, `eval`, a shell fed a `$`-built string, decoded bytes or process substitution, an
expansion inside the SQL argument, an unquoted heredoc that expands, an opaque program piped into
a legacy client. A command that changes directory is judged against the allowlist of every
workspace it enters as well as the one it starts in.

Outside a migration workspace (no `.migration/allowed_targets.json` up the tree) the guard
approves everything. Malformed input approves (plugin hooks fail open by platform design; the
factory-doctor reports whether the hook is loaded and whether it blocks its probe command, which
carries `__dbx_guard_probe__` and always blocks). `hooks/tests/test_probe_table.py` is the
red-team table this policy is pinned to; add a row there before changing a shape.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import sys
from dataclasses import dataclass, field
from pathlib import Path

CONFIG_REL = Path(".migration") / "allowed_targets.json"
_MAX_SCRIPT_BYTES = 4 * 1024 * 1024
DEFAULT_FORBIDDEN_BUNDLE_TARGETS = ("prod", "production")
PROBE_SENTINEL = "__dbx_guard_probe__"   # named by the doctor's HOOK_PROBE_COMMAND; a command naming it always blocks

_SEG = r"(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$-]*)"
_TARGET_NAME = re.compile(rf"({_SEG})\.({_SEG})\.({_SEG})(?:\.{_SEG})?(?![\w`.])")
_WRITE_STMT = re.compile(
    r"""(?:\b(?:
        INSERT\s+(?:INTO|OVERWRITE)\b
      | UPDATE\s+(?:TOP\s*\([^)]*\)\s+)?(?!SET\b)\S+(?:\s+(?:AS\s+)?(?!SET\b|WITH\b)[\w`\[\]$]+)?(?:\s+WITH\s*\([^)]*\))?\s+SET\b
      | DELETE\s+FROM\b
      | MERGE\s+(?:WITH\s+SCHEMA\s+EVOLUTION\s+)?INTO\b
      | TRUNCATE\s+TABLE\b
      | CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+|EXTERNAL\s+|STREAMING\s+|MATERIALIZED\s+|LIVE\s+)*(?:TABLE|VIEW|SCHEMA|DATABASE|CATALOG|FUNCTION|PROCEDURE|VOLUME|INDEX|TRIGGER|SEQUENCE)\b
      | DROP\s+(?:TABLE|VIEW|SCHEMA|DATABASE|CATALOG|FUNCTION|PROCEDURE|VOLUME|INDEX|TRIGGER|SEQUENCE)\b
      | UNDROP\s+(?:TABLE|SCHEMA)\b
      | ALTER\s+(?:TABLE|VIEW|SCHEMA|DATABASE|CATALOG|FUNCTION|PROCEDURE|VOLUME)\b
      | COMMENT\s+ON\b
      | REPLACE\s+TABLE\b
      | GRANT\s+.+?\bON\b
      | REVOKE\s+.+?\bON\b
      | COPY\s+INTO\b
      | (?:OPTIMIZE|VACUUM|RESTORE|REFRESH)\s+TABLE\b
    )
      | \b(?:EXEC(?:UTE)?|CALL)\s+(?!IMMEDIATE\b)(?:\[?[\w$]+\]?\.)+\[?[\w$]+
      | (?<=['";\n])\s*(?:EXEC(?:UTE)?|CALL)\s+(?!IMMEDIATE\b)[\[@`\w]
    )""",
    re.IGNORECASE | re.VERBOSE,
)
_USE_CATALOG = re.compile(rf"\bUSE\s+CATALOG\s+({_SEG})", re.IGNORECASE)
_CREATE_CATALOG = re.compile(rf"\b(?:CREATE|DROP|ALTER)\s+CATALOG\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?({_SEG})", re.IGNORECASE)
_SCHEMA_TWO_PART = re.compile(
    rf"\b(?:CREATE|DROP|ALTER|UNDROP)\s+(?:SCHEMA|DATABASE)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?({_SEG})\.({_SEG})(?![\w`.])",
    re.IGNORECASE,
)
_ON_CATALOG = re.compile(rf"\bON\s+CATALOG\s+({_SEG})", re.IGNORECASE)
_ON_SCHEMA = re.compile(rf"\bON\s+(?:SCHEMA|DATABASE)\s+({_SEG})\.({_SEG})(?![\w`.])", re.IGNORECASE)
# the securable a write acts on sits right after its verb phrase; anything qualified later in the
# statement (CTAS `AS SELECT FROM`, MERGE `USING`, INSERT ... SELECT) is a source
_WRITE_TARGET_HEAD = re.compile(
    r"""(?:
        INSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?
      | UPDATE(?:\s+TOP\s*\([^)]*\))?
      | DELETE\s+FROM
      | MERGE\s+(?:WITH\s+SCHEMA\s+EVOLUTION\s+)?INTO
      | TRUNCATE\s+TABLE
      | COPY\s+INTO
      | (?:OPTIMIZE|VACUUM|RESTORE|REFRESH)\s+TABLE
      | REPLACE\s+TABLE
      | CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+|EXTERNAL\s+|STREAMING\s+|MATERIALIZED\s+|LIVE\s+)*
        (?:TABLE|VIEW|FUNCTION|PROCEDURE|VOLUME|INDEX|TRIGGER|SEQUENCE)(?:\s+IF\s+NOT\s+EXISTS)?
      | DROP\s+(?:TABLE|VIEW|FUNCTION|PROCEDURE|VOLUME|INDEX|TRIGGER|SEQUENCE)(?:\s+IF\s+EXISTS)?
      | UNDROP\s+TABLE
      | ALTER\s+(?:TABLE|VIEW|FUNCTION|PROCEDURE|VOLUME)(?:\s+IF\s+EXISTS)?
      | COMMENT\s+ON(?:\s+(?:TABLE|VIEW|MATERIALIZED\s+VIEW|VOLUME|FUNCTION|PROCEDURE|COLUMN))?
      | (?:GRANT|REVOKE)\s+.+?\bON\s+(?:(?:TABLE|VIEW|MATERIALIZED\s+VIEW|FUNCTION|PROCEDURE|VOLUME)\s+)?
      | (?:EXEC(?:UTE)?|CALL)
    )\s*""",
    re.IGNORECASE | re.VERBOSE,
)
_IDENTIFIER_LITERAL = re.compile(r"\bIDENTIFIER\s*\(\s*'([^']*)'\s*\)", re.IGNORECASE)
_IDENTIFIER_DYNAMIC = re.compile(r"\bIDENTIFIER\s*\(", re.IGNORECASE)
_EXEC_IMMEDIATE_DYNAMIC = re.compile(r"\bEXEC(?:UTE)?\s+IMMEDIATE\s+(?!')\S", re.IGNORECASE)
_BUNDLE_TARGET = re.compile(r"(?:^|\s)(?:-t|--target)(?:=|\s+)(\S+)")

# clients are recognised by basename: `databricks`, `./bin/databricks` and `/opt/x/databricks` alike
_CLIENT_PREFIX = r"(?:^|[\s;&|(])(?:[^\s;&|()<>'\"]*/)?"
_DATABRICKS_CONTEXT = re.compile(_CLIENT_PREFIX + r"(?:databricks|dbx-recon|spark-sql|dbsqlcli)\b|--target-catalog")
_LEGACY_ONLY = ("bteq", "sqlplus", "sqlldr", "snowsql", "mload", "fastload", "fastexport", "tbuild", "tdload")
_LOADERS = ("sqlldr", "mload", "fastload", "tbuild", "tdload")
_GENERIC = ("psql", "pgcli", "sqlcmd", "osql", "isql", "tsql", "mysql", "mariadb", "sqlite3", "bcp", "beeline", "trino", "presto")
_LEGACY_ONLY_CLIENTS = re.compile(_CLIENT_PREFIX + "(?:" + "|".join(_LEGACY_ONLY) + r")\b", re.IGNORECASE)
_GENERIC_SQL_CLIENTS = re.compile(_CLIENT_PREFIX + "(?:" + "|".join(_GENERIC) + r")\b", re.IGNORECASE)
_IDENTITY_CLIENTS = ("databricks", "dbx-recon", "spark-sql")
_PYTHON = re.compile(r"python[0-9.]*|spark-submit")
_REST_CLIENTS = ("curl", "wget", "http", "https", "xh")

_SHELLS = ("sh", "bash", "zsh", "dash", "ksh")
_SEPARATORS = (";", "&&", "||", "|", "&", "(", ")", "{", "}", "\n")
_PREFIX_WORDS = ("sudo", "env", "nohup", "time", "exec", "command", "nice", "xargs", "timeout", "stdbuf")
_ASSIGN = re.compile(r"^[A-Za-z_]\w*=")
_IDENTITY_VAR = re.compile(r"^(?:export\s+)?DATABRICKS_(?:TOKEN|HOST|CONFIG_PROFILE|CLIENT_\w+)=")
# flags whose value is the SQL text itself (psql -c, sqlcmd/isql/snowsql -Q/-q, spark-sql/dbsqlcli/mysql -e ...)
_SQL_VALUE_FLAGS = ("-c", "-Q", "-q", "-e", "--query", "--sql", "--statement", "--execute", "--command")
_HOST_FLAGS = ("-S", "-h", "-H", "--host", "--server")
_OPAQUE_PRODUCER = re.compile(
    r"(?:^|[\s;&|(])(?:base64\s+(?:-[A-Za-z]*d[A-Za-z]*|--decode)|xxd\s+-r|openssl\s+enc\b|gunzip|gzip\s+-d|zcat|"
    r"uudecode|curl|wget|python[0-9.]*|perl|ruby|node)\b", re.IGNORECASE)
_HEREDOC_OPEN = re.compile(r"<<-?\s*(['\"\\]?)([A-Za-z_][\w-]*)\1?")
_HEREDOC = re.compile(r"<<-?\s*(['\"\\]?)([A-Za-z_][\w-]*)\1?[^\n]*\n")
_DYNAMIC_SQL_EXECUTOR = (r"(?:\bEXEC(?:UTE)?\s+IMMEDIATE|\bsp_executesql|\bEXEC(?:UTE)?\s*\(|"
                         r"\.(?:execute|executemany|sql|run_query|execute_statement)\s*\(|\bstatement\s*=)")
_DYNAMIC_SQL_CALLER = re.compile(_DYNAMIC_SQL_EXECUTOR + r"\s*N?\s*$", re.IGNORECASE)
_PY_LITERAL = re.compile(_DYNAMIC_SQL_EXECUTOR + r"\s*[rbuf]*(['\"]{3}|['\"])(.*?)\1", re.IGNORECASE | re.DOTALL)
_PY_WRITE = re.compile(r"""['"][wax]\+?['"]|\.write\w*\(|json\.dump\(|os\.(?:remove|unlink|rename|replace|chmod|rmdir|makedirs|mkdir)\(|"""
                       r"""shutil\.|\.(?:unlink|rename|rmdir|mkdir|touch|chmod)\(""")
_MIGRATION_PATH = re.compile(r"[^\s'\"()]*\.migration/[^\s'\"()]*")

# read shapes: leading keyword, then no write keyword anywhere at statement level
_READ_HEAD = ("SELECT", "WITH", "SET", "USE", "DECLARE")
_DESCRIBE_HEAD = ("SHOW", "DESC", "DESCRIBE", "EXPLAIN", "HELP", "GO")
_SQLPLUS_DIRECTIVE = ("SPOOL", "PROMPT", "DEFINE", "COLUMN", "WHENEVER", "EXIT", "QUIT", "TTITLE", "BTITLE", "BREAK",
                      "COMPUTE", "TIMING", "REM", "REMARK", "PAUSE", "CLEAR")
_DIRECTIVE_LINE = re.compile(r"^\s*(?:[.\\:@/]|GO\b|(?:" + "|".join(_SQLPLUS_DIRECTIVE) + r")\b)", re.IGNORECASE)
_PSQL_META = re.compile(r"\\(?:d\S*|l\S*|x|q|\?|h\S*|timing|echo|pset|set|unset|conninfo|encoding|z|sf|sv|a|t|H|C|f)\b")
_SQLCMD_DIRECTIVE = re.compile(r":(?:setvar|exit|quit|on\s+error|help|list\w*|reset|xml|error|out|perftrace)\b", re.IGNORECASE)
_SET_DENY = re.compile(r"SET\s+(?:IDENTITY_INSERT|TRANSACTION|IMPLICIT_TRANSACTIONS|ROLE|SESSION\s+AUTHORIZATION)\b", re.IGNORECASE)
_NON_READ_WORD = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|CREATE|ALTER|GRANT|REVOKE|DENY|EXEC(?:UTE)?|CALL|KILL|BACKUP|RESTORE|DBCC|"
    r"WAITFOR|BEGIN|COMMIT|ROLLBACK|ENABLE|DISABLE|INTO|BULK|OPENROWSET|OPENQUERY|SHUTDOWN|RECONFIGURE|WRITETEXT|UPDATETEXT|"
    r"sp_\w+|xp_\w+)\b", re.IGNORECASE)

# Databricks CLI: read verbs per command group; mutations that name a securable, with the index of
# the positional carrying its catalog (`grants update SECURABLE_TYPE FULL_NAME`, `schemas create
# NAME CATALOG`, `volumes create CATALOG SCHEMA NAME TYPE`)
_UC_READ = {"list", "get", "exists"}
_DBX_READ = {
    "current-user": {"me"}, "catalogs": _UC_READ, "schemas": _UC_READ, "tables": _UC_READ, "volumes": _UC_READ,
    "functions": _UC_READ, "metastores": _UC_READ, "external-locations": _UC_READ, "storage-credentials": _UC_READ,
    "connections": _UC_READ, "grants": {"get", "get-effective"}, "jobs": {"list", "get", "list-runs", "get-run", "get-run-output"},
    "pipelines": {"list", "get", "list-updates", "get-update", "list-pipeline-events"}, "warehouses": {"list", "get"},
    "clusters": {"list", "get", "events", "spark-versions", "list-node-types", "list-zones"},
    "workspace": {"list", "export", "get-status"}, "secrets": {"list-scopes", "list-secrets"},
    "auth": {"describe", "env", "token", "profiles"}, "fs": {"ls", "cat", "head"}, "api": {"get"}, "bundle": {"validate", "summary"},
}
_CLI_CATALOG_ARG = {"grants update": 1, "grants delete": 1, "schemas create": 1, "schemas delete": 0, "schemas update": 0,
                    "tables delete": 0, "volumes create": 0, "volumes delete": 0, "volumes update": 0, "functions delete": 0,
                    "functions update": 0, "catalogs create": 0, "catalogs delete": 0, "catalogs update": 0}
_DBX_VALUE_FLAGS = {"-o", "--output", "--log-level", "--log-file", "--log-format", "--progress-format", "-t", "--target", "-p",
                    "--profile", "--host", "--warehouse-id", "--catalog", "--schema", "--format", "--wait-timeout", "--json",
                    "--var", "--file", "--language", "--string-value", "--bytes-value", "-e", "--statement", "--query"}
_UC_PATH = re.compile(r"unity-catalog/(?:tables|schemas|volumes|functions)/([^/?\s]+)")
_VOLUME_PATH = re.compile(r"^(?:dbfs:)?/Volumes/([^/]+)/")
_DBX_HOST = re.compile(r"\$\{?DATABRICKS_HOST\b|\.(?:cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com)\b", re.IGNORECASE)
_HTTP_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")


@dataclass
class GuardConfig:
    catalogs: list[str]
    legacy_sources: list[str] = field(default_factory=list)
    mode: str = "block"
    forbidden_bundle_targets: tuple[str, ...] = DEFAULT_FORBIDDEN_BUNDLE_TARGETS
    target_hosts: list[str] = field(default_factory=list)
    bundle_targets: list[str] = field(default_factory=list)
    path: Path | None = None

    @classmethod
    def from_dict(cls, data: dict, path: Path | None = None) -> GuardConfig:
        catalogs = data.get("catalogs")
        if not isinstance(catalogs, list) or not catalogs:
            raise ValueError("allowed_targets.json must contain a non-empty 'catalogs' list")
        lists = {}
        for key in ("legacy_sources", "forbidden_bundle_targets", "target_hosts", "bundle_targets"):
            default = list(DEFAULT_FORBIDDEN_BUNDLE_TARGETS) if key == "forbidden_bundle_targets" else []
            value = data.get(key, default)
            if not isinstance(value, list):
                raise ValueError(f"'{key}' must be a list")
            lists[key] = [str(x).strip() for x in value if str(x).strip()]
        mode = str(data.get("guard_mode", "block")).lower()
        if mode not in ("block", "warn"):
            raise ValueError("'guard_mode' must be 'block' or 'warn'")
        return cls(
            catalogs=[_norm(c) for c in catalogs],
            legacy_sources=lists["legacy_sources"],
            mode=mode,
            forbidden_bundle_targets=tuple(t.lower() for t in lists["forbidden_bundle_targets"]),
            target_hosts=[h.lower() for h in lists["target_hosts"]],
            bundle_targets=[t.lower() for t in lists["bundle_targets"]],
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


def _sql_view(text: str) -> str:
    """SQL text as the detectors read it, offsets preserved: comments blanked, and the contents of
    single-quoted literals blanked (`WHERE note = 'DROP TABLE x'` is a read) unless the literal
    feeds a dynamic-SQL executor, where it is the statement."""
    out, i, n = list(text), 0, len(text)

    def blank(a: int, b: int) -> None:
        for k in range(a, b):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        if text.startswith("--", i):
            j = text.find("\n", i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            j = n if j < 0 else j + 2
            blank(i, j)
            i = j
        elif text[i] == "'":
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


# ---------------------------------------------------------------- shell model: words, simple commands, pipelines

# redirection operators stay inside their simple command; every other punctuation run ends it. A
# descriptor attached to the operator (`2>&1`, `0<f`) is part of it; separated by a space it is
# an operand (`cat 2 >log` prints the file named 2)
_REDIRECT_OP = re.compile(r"\d*(?:<{1,3}|<>|<&|>{1,2}|>&|>\||&>{1,2})")
_STDIN_OP = re.compile(r"0?<")


def _raw_words(cmd: str) -> list[str]:
    """The command split at unquoted, unescaped blanks, each word kept as written."""
    words: list[str] = []
    cur, quote, i = "", "", 0
    while i < len(cmd):
        ch = cmd[i]
        if quote:
            if ch == "\\" and quote == '"' and i + 1 < len(cmd):
                cur += cmd[i:i + 2]
                i += 2
                continue
            cur += ch
            if ch == quote:
                quote = ""
        elif ch == "\\" and i + 1 < len(cmd):
            cur += cmd[i:i + 2]
            i += 2
            continue
        elif ch in "'\"":
            quote = ch
            cur += ch
        elif ch in " \t\r":
            if cur:
                words.append(cur)
            cur = ""
        else:
            cur += ch
        i += 1
    if cur:
        words.append(cur)
    return words


def _shell_tokens(cmd: str) -> list[str]:
    """Shell words with redirection operators, separators and newlines as their own tokens; a
    quoted argument is one word, so a `<` or a line break inside it is never an operator. A
    descriptor written against its operator is one token with it (`2>&`, `0<`): only unquoted
    digits opening a word with the operator right behind them in the source are a descriptor."""
    def lexer(punctuation: str | bool) -> shlex.shlex:
        lex = shlex.shlex(cmd, posix=True, punctuation_chars=punctuation)
        lex.whitespace = " \t\r"
        lex.whitespace_split = True
        lex.commenters = ""
        return lex

    try:
        toks = list(lexer("();<>|&\n"))
    except ValueError:  # unbalanced quote: fall back to whitespace words, quotes stripped
        return [t.strip("'\"") for t in re.split(r"\s+|(?<!<)(<)(?!<)", cmd) if t]
    words = list(lexer(False))
    raw = _raw_words(cmd)
    if len(raw) != len(words):
        raw = [""] * len(words)
    out: list[str] = []
    i = 0
    for word, source in zip(words, raw):
        j, acc = i, ""
        while j < len(toks) and len(acc) < len(word):
            acc += toks[j]
            j += 1
        if acc != word:   # the two splits disagree: fuse nothing, a digit stays an operand
            return toks
        k = i
        while k < j:
            nxt = toks[k + 1] if k + 1 < j else ""
            if k == i and toks[k].isdigit() and _REDIRECT_OP.fullmatch(nxt) and not nxt[0].isdigit() \
                    and source.startswith(toks[k] + nxt):
                out.append(toks[k] + nxt)
                k += 2
            else:
                out.append(toks[k])
                k += 1
        i = j
    out.extend(toks[i:])
    return out


_PIPE_OPS = ("|", "|&")


@dataclass
class _Simple:
    """One simple command: its words, the words of the heredoc bodies it opened, the operator
    before it (`|`/`|&`: the left side feeds its stdin), how many groups opened right before it
    and closed between it and the previous command, and how many words are its own (`own`): the
    rest were handed down by the redirections of a group it sits in."""
    words: list[str] = field(default_factory=list)
    body: list[str] = field(default_factory=list)
    sep: str = ""
    opened: int = 0
    closed: int = 0
    own: int | None = None

    def empty(self) -> bool:
        return not self.words and not self.body


_GROUP_STDIN = re.compile(r"0?(?:<|<<<|<&)")
_LOCAL_STDIN = re.compile(r"0?(?:<|<<<|<<-?|<&)")


def _stdin_replaced(cmds: list[_Simple], c: _Simple) -> bool:
    """Whether member `c` reads something other than the stdin the closing group is given: its
    own stdin redirection or heredoc, or a pipe feeding it (directly or the group it runs in).
    `<&0` duplicates stdin onto itself, which replaces nothing."""
    words = c.words if c.own is None else c.words[:c.own]
    for w, operand in zip(words, [*words[1:], ""]):
        if _LOCAL_STDIN.fullmatch(w) and not (w.endswith("<&") and operand == "0"):
            return True
    return _pipe_into(cmds, next(k for k, x in enumerate(cmds) if x is c)) >= 0


def _commands(toks: list[str]) -> list[_Simple]:
    """Tokens split into simple commands at `;`, `&&`, `||`, `|`, `|&`, `&`, group delimiters and
    line breaks. Redirections belong to the command they sit in; a `<<TAG` body (read from the
    next line up to the line holding TAG alone) belongs to the command that opened it. A
    redirection written after `)`/`}` is repeated on every member of the group, the group's
    stdin only on members that did not replace it themselves."""
    punct = re.compile(r"[();<>|&\n]+")
    out: list[_Simple] = [_Simple()]
    pending: list[tuple[str, list[str]]] = []
    open_groups: list[int] = []
    closed_group: list[_Simple] = []
    operand_of: list[_Simple] = []

    def close_group() -> None:
        nonlocal closed_group
        start = open_groups.pop() if open_groups else 0
        closed_group = out[start:-1]
        for c in closed_group:
            c.own = len(c.words)

    def separate(tok: str) -> None:
        operand_of.clear()
        op = re.sub(r"[()\n]", "", tok)
        if not out[-1].empty():
            out.append(_Simple())
        cur = out[-1]
        cur.sep = op or cur.sep or ("\n" if "\n" in tok else "")
        for ch in tok:
            if ch == "(":
                cur.opened += 1
                open_groups.append(len(out) - 1)
            elif ch == ")":
                cur.closed += 1
                close_group()

    i = 0
    while i < len(toks):
        tok = toks[i]
        if tok in ("<<", "<<-") and i + 1 < len(toks) and not punct.fullmatch(toks[i + 1]):
            pending.append((toks[i + 1].lstrip("-"), out[-1].body))
            out[-1].words.extend(toks[i:i + 2])
            i += 2
        elif punct.fullmatch(tok) and not _REDIRECT_OP.fullmatch(tok):
            if "\n" in tok:
                while pending:
                    tag, body = pending.pop(0)
                    i += 1
                    while i < len(toks):
                        if toks[i] == tag and "\n" in toks[i - 1] and (i + 1 >= len(toks) or "\n" in toks[i + 1]):
                            break
                        if not punct.fullmatch(toks[i]):
                            body.append(toks[i])
                        i += 1
            separate(tok)
            i += 1
        elif tok == "{" and out[-1].empty():
            out[-1].opened += 1
            open_groups.append(len(out) - 1)
            i += 1
        elif tok == "}" and out[-1].empty():
            out[-1].closed += 1
            close_group()
            i += 1
        elif operand_of:
            for c in operand_of:
                c.words.append(tok)
            operand_of.clear()
            i += 1
        elif _REDIRECT_OP.fullmatch(tok) and out[-1].empty() and out[-1].closed and closed_group:
            owners = [c for c in closed_group
                      if not (_GROUP_STDIN.fullmatch(tok) and _stdin_replaced(out, c))]
            for c in owners:
                c.words.append(tok)
            operand_of = owners or [_Simple()]   # nobody inherits it: the operand is still consumed
            i += 1
        else:
            out[-1].words.append(tok)
            i += 1
    return [c for c in out if not c.empty()]


# producers whose stdout the guard can read as text; `tee` passes its stdin through
_TEXT_PRODUCERS = ("cat", "echo", "printf", "tee")


def _pipe_into(cmds: list[_Simple], k: int) -> int:
    """Index of the command whose separator is the pipe feeding cmds[k]'s stdin: k itself, or the
    command opening the group k runs in (`ls | (cd x; bteq)`); -1 when nothing is piped in."""
    depth, at = 0, []
    for c in cmds[:k + 1]:
        depth += c.opened - c.closed
        at.append(depth)
    j, level = k, at[k]
    while j >= 0:
        if cmds[j].sep in _PIPE_OPS:
            return j
        if level <= 0:
            return -1
        while j >= 0 and not (cmds[j].opened and at[j] - cmds[j].opened < level <= at[j]):
            j -= 1
        if j >= 0:
            level = at[j] - cmds[j].opened if cmds[j].sep not in _PIPE_OPS else level
    return -1


def _producers(cmds: list[_Simple], k: int) -> list[_Simple]:
    """Commands whose output reaches cmds[k]'s stdin: the left of each `|`, or every member of the
    group on its left."""
    out = []
    k = _pipe_into(cmds, k)
    while k > 0 and cmds[k].sep in _PIPE_OPS:
        unwind = cmds[k].closed
        k -= 1
        out.append(cmds[k])
        while unwind > 0 and k > 0:
            unwind -= cmds[k].opened
            if unwind <= 0:
                break
            unwind += cmds[k].closed
            k -= 1
            out.append(cmds[k])
    return out


def _piped_scripts(producer: _Simple) -> list[str]:
    """Script files a text producer hands the client on its stdin: `cat fix.sql | bteq`, `@fix.sql`
    on a line of `cat <<EOF | sqlplus` or in `echo @fix.sql | bteq`."""
    files = [tok[1:] for tok in producer.body if tok.startswith("@") and len(tok) > 1]
    base = producer.words[0].rsplit("/", 1)[-1] if producer.words else ""
    if base == "cat":
        operands: list[str] = []
        stdin: list[str] = []
        words = producer.words
        i = 1
        while i < len(words):
            w = words[i]
            if _STDIN_OP.fullmatch(w):
                stdin.extend(words[i + 1:i + 2])
                i += 2
            elif _REDIRECT_OP.fullmatch(w) or w in ("<<", "<<-"):
                i += 2
            else:
                if w == "-" or not w.startswith("-"):
                    operands.append(w)
                i += 1
        files.extend(w for w in operands if w != "-")
        if not operands or "-" in operands:  # cat reads its stdin only without an operand, or with `-`
            files.extend(stdin)
    elif base in ("echo", "printf"):
        files.extend(w[1:] for w in producer.words[1:] if w.startswith("@") and len(w) > 1)
    return files


_SCRIPT_FLAGS = ("-f", "-i", "--file", "--input")


def _scripts_of(cmds: list[_Simple], k: int, words: list[str] | None = None) -> list[str]:
    """Files cmds[k] is told to execute: `< f`, `@f`, `-f f`, `--file f`, `-i f`, `--input f`,
    `@f` on a heredoc line (SQL*Plus / BTEQ `.RUN`), and what a text producer pipes into it."""
    files = []
    for producer in _producers(cmds, k):
        files.extend(_piped_scripts(producer))
    words = cmds[k].words if words is None else words
    skip = False
    for i, tok in enumerate(words):
        if skip:  # operand of a redirection other than `<`: a log file, a descriptor, a here-string
            skip = False
            continue
        nxt = words[i + 1] if i + 1 < len(words) else ""
        if _STDIN_OP.fullmatch(tok) or tok in _SCRIPT_FLAGS:
            f = nxt
        elif _REDIRECT_OP.fullmatch(tok):
            skip = True
            continue
        elif tok.startswith("@"):
            f = tok[1:]
        elif tok.startswith(tuple(fl + "=" for fl in _SCRIPT_FLAGS)):
            f = tok.split("=", 1)[1]
        else:
            continue
        if f and not f.startswith("-") and not re.fullmatch(r"[<>|;&()]+", f):
            files.append(f)
    files.extend(tok[1:] for tok in cmds[k].body if tok.startswith("@") and len(tok) > 1)
    return files


def _script_inputs(cmd: str, cfg: GuardConfig | None = None) -> list[str]:
    """Script files of every simple command; given a config, only of those naming a client or a
    legacy source (the `-f` of `rm -f x && databricks jobs list` belongs to `rm`)."""
    cmds = _commands(_shell_tokens(cmd))
    return [f for k, c in enumerate(cmds)
            if cfg is None or _has_context(" ".join(c.words), cfg)
            for f in _scripts_of(cmds, k)]


def _raw_tokens(cmd: str) -> list[str]:
    """Shell words with their quotes kept, so `'$x'` (literal) and `"$x"` (expanded) stay distinct."""
    lex = shlex.shlex(cmd, posix=False, punctuation_chars=True)
    lex.whitespace_split = True
    lex.commenters = ""
    try:
        return list(lex)
    except ValueError:
        return [t for t in re.split(r"\s+", cmd) if t]


def _live(text: str) -> str:
    """The text with single-quoted spans and backslash-escaped characters blanked: what remains of
    `$` and backticks is what the shell would expand."""
    out, i, n = list(text), 0, len(text)
    while i < n:
        c = text[i]
        if c == "\\" and i + 1 < n:
            out[i] = out[i + 1] = " "
            i += 2
        elif c == "'":
            j = text.find("'", i + 1)
            j = n if j < 0 else j
            for k in range(i, min(j + 1, n)):
                out[k] = " "
            i = j + 1
        else:
            i += 1
    return "".join(out)


_SUBSTITUTION = re.compile(r"\$\(|(?<![\w])[<>]\(")
_BACKTICK = re.compile(r"`([^`]*)`")


def _command_backticks(live: str) -> bool:
    """A backtick span that is not a plain SQL identifier (`mig_cat`) runs a command."""
    return any(not re.fullmatch(r"[\w$-]+", m.group(1)) for m in _BACKTICK.finditer(live))


def _expands(text: str) -> bool:
    live = _live(text)
    return "$" in live or _command_backticks(live)


def _substitutes(text: str) -> bool:
    live = _live(text)
    return bool(_SUBSTITUTION.search(live)) or _command_backticks(live)


def _strip_quotes(tok: str) -> str:
    if len(tok) >= 2 and tok[0] == tok[-1] and tok[0] in "'\"":
        return tok[1:-1]
    return tok


def _shell_script_inputs(cmd: str) -> list[str]:
    """Scripts run by a shell or sourced: `bash x.sh`, `sh -x x.sh`, `bash < x.sh`, `source x`,
    `. x`. `-c` forms carry their text in the command and are not files."""
    toks = _raw_tokens(cmd)
    files: list[str] = []
    for i, tok in enumerate(toks):
        base = tok.rsplit("/", 1)[-1]
        if not (base in _SHELLS or tok in ("source", ".")):
            continue
        if i > 0 and toks[i - 1] not in _SEPARATORS and toks[i - 1] not in _PREFIX_WORDS:
            continue
        j = i + 1
        while j < len(toks) and toks[j].startswith("-") and toks[j] not in _SEPARATORS:
            if toks[j] == "-c":
                j = len(toks)
            j += 1
        if j < len(toks) and toks[j] == "<":
            j += 1
        if j < len(toks) and toks[j] not in _SEPARATORS and not toks[j].startswith("<"):
            files.append(_strip_quotes(toks[j]))
    return files


def _read_script(f: str, root: Path) -> str | None:
    p = Path(os.path.expandvars(os.path.expanduser(f)))
    if not p.is_absolute():
        p = root / p
    try:
        with p.open(errors="replace") as fh:
            body = fh.read(_MAX_SCRIPT_BYTES + 1)
    except OSError:
        return None
    return None if len(body) > _MAX_SCRIPT_BYTES else body


def _mentions_legacy(text: str, cfg: GuardConfig) -> list[str]:
    return [tok for tok in cfg.legacy_sources if re.search(rf"(?<![\w-]){re.escape(tok)}(?![\w-])", text, re.IGNORECASE)]


def _has_context(text: str, cfg: GuardConfig) -> bool:
    return bool(_DATABRICKS_CONTEXT.search(text) or _LEGACY_ONLY_CLIENTS.search(text) or _GENERIC_SQL_CLIENTS.search(text)
                or _mentions_legacy(text, cfg))


def _sql_bearing(toks: list[str], i: int) -> bool:
    """Whether token i is where a client reads its SQL from: the value of a SQL flag, the positional
    after `tools query`, or a composite string rather than a bare value."""
    prev = toks[i - 1] if i > 0 else ""
    if prev in _SQL_VALUE_FLAGS or toks[i].split("=", 1)[0] in _SQL_VALUE_FLAGS:
        return True
    if i >= 2 and toks[i - 2] == "tools" and prev == "query":
        return True
    return bool(re.search(r"[\s;]", _strip_quotes(toks[i])))


def _check_opaque_execution(cmd: str, cfg: GuardConfig) -> list[str]:
    """Constructs that only produce the statement at run time. Always: `eval`/`sh -c` on a
    `$`-built string, opaque bytes piped into a shell, a shell fed by process substitution. Where a
    client is involved: any substitution, an expansion inside the SQL argument, an unquoted heredoc
    that expands, `xargs`."""
    violations: list[str] = []
    toks = _raw_tokens(cmd)
    ctx = _has_context(cmd, cfg)

    def seg_start(i: int) -> bool:
        return i == 0 or toks[i - 1] in _SEPARATORS or toks[i - 1] in _PREFIX_WORDS

    for i, tok in enumerate(toks):
        base = tok.rsplit("/", 1)[-1]
        if tok == "eval" and seg_start(i):
            if _expands(" ".join(toks[i + 1:]).split(";", 1)[0]):
                violations.append("`eval` of a runtime-built string; the guard cannot read what it would run")
        elif base in _SHELLS and seg_start(i):
            if i > 0 and toks[i - 1] == "|":
                producer = " ".join(toks[:i - 1])
                if _OPAQUE_PRODUCER.search(producer) or _expands(producer):
                    violations.append(f"text piped into `{base}` comes from a decoder, download, program or expansion the "
                                      "guard cannot read")
            j = i + 1
            while j < len(toks) and toks[j].startswith("-") and toks[j] not in _SEPARATORS:
                if toks[j] == "-c" and j + 1 < len(toks) and _expands(toks[j + 1]):
                    violations.append(f"`{base} -c` on a runtime-built string; the guard cannot read what it would run")
                j += 1
            if j < len(toks) and toks[j].startswith("<("):
                violations.append(f"`{base}` fed by process substitution; the guard cannot read what it would run")
        elif tok == "xargs" and ctx:
            violations.append("`xargs` builds a client invocation from stdin; the guard cannot read the statement it would run")
    if ctx:
        if _substitutes(cmd):
            violations.append("command/process substitution in a Databricks or legacy command; the statement is built at "
                              "run time, so inline it as text")
        for i, tok in enumerate(toks):
            if _expands(tok) and _sql_bearing(toks, i):
                violations.append(f"shell expansion inside the SQL argument `{tok[:60]}`; expand it in the command text so "
                                  "the guard can read the statement")
                break
        for m in _HEREDOC.finditer(cmd):
            if m.group(1):
                continue
            end = re.search(rf"^\s*{re.escape(m.group(2))}\s*$", cmd[m.end():], re.MULTILINE)
            body = cmd[m.end(): m.end() + end.start()] if end else cmd[m.end():]
            if _expands(body):
                violations.append(f"unquoted heredoc <<{m.group(2)} expands `$`/backticks in its body; quote the delimiter "
                                  f"(<<'{m.group(2)}') or inline the values")
                break
    return violations


# ---------------------------------------------------------------- segments: the program each simple command runs

@dataclass
class _Seg:
    argv: list[str]                 # program and arguments after prefixes, wrappers, aliases, redirections
    words: list[str]                # the simple command as tokenised (redirections included)
    heredocs: list[str]             # raw heredoc bodies it opened
    assigns: list[str]              # VAR=value prefixes
    herestring: str | None
    stdin: list[str]                # literal text piped in (echo/printf words, cat heredocs)
    opaque: str | None              # a piped producer whose output the guard cannot read
    scripts: list[str]              # files it executes
    ctx: str = ""                   # text of the wrapper (`ssh host`) this command was nested in

    @property
    def argv0(self) -> str:
        return self.argv[0].rsplit("/", 1)[-1] if self.argv else ""

    @property
    def text(self) -> str:
        return " ".join([*self.words, *self.heredocs, *self.stdin, self.herestring or "", self.ctx])


def _heredoc_bodies(text: str) -> list[str]:
    """Raw heredoc bodies in the order their `<<` operators appear."""
    out, regions, last_end = [], [], 0
    for m in _HEREDOC_OPEN.finditer(text):
        if any(a <= m.start() < b for a, b in regions):
            continue
        nl = text.find("\n", m.end())
        if nl < 0:
            break
        start = max(nl + 1, last_end)
        t = re.compile(r"^\t*" + re.escape(m.group(2)) + r"[ \t]*$", re.MULTILINE).search(text, start)
        end = len(text) if t is None else t.start()
        out.append(text[start:end])
        last_end = len(text) if t is None else t.end() + 1
        regions.append((start, last_end))
    return out


_VALUE_FLAGS = {
    "docker": {"-e", "--env", "-u", "--user", "-w", "--workdir", "--name", "-v", "--volume", "-p", "--publish", "--network",
               "--entrypoint", "--platform"},
    "kubectl": {"-c", "--container", "-n", "--namespace"},
    "ssh": {"-p", "-i", "-o", "-l", "-F", "-J", "-L", "-R", "-D", "-b", "-m", "-c", "-E", "-e", "-I", "-Q", "-S", "-W", "-B"},
}


def _unwrap(argv: list[str]) -> tuple[list[str], str]:
    """The command a wrapper runs: `docker|podman exec|run [flags] NAME cmd`, `docker compose exec
    [flags] SVC cmd`, `kubectl exec ... -- cmd`, `ssh [flags] HOST cmd`. Returns (argv, wrapper text)."""
    base = argv[0].rsplit("/", 1)[-1] if argv else ""
    i = 1
    if base in ("podman", "nerdctl"):
        base = "docker"
    if base == "docker" and len(argv) > 2:
        if argv[1] == "compose" and len(argv) > 3 and argv[2] in ("exec", "run"):
            i = 3
        elif argv[1] in ("exec", "run"):
            i = 2
        else:
            return argv, ""
    elif base == "kubectl" and len(argv) > 2 and argv[1] == "exec":
        i = argv.index("--") + 1 if "--" in argv else 2
    elif base != "ssh":
        return argv, ""
    while i < len(argv) and argv[i].startswith("-"):
        i += 2 if argv[i] in _VALUE_FLAGS[base] else 1
    i += 0 if base == "kubectl" and "--" in argv else 1   # the container / host word
    rest = argv[i:]
    if len(rest) == 1 and re.search(r"\s", rest[0]):
        try:
            rest = shlex.split(rest[0])
        except ValueError:
            pass
    return rest, " ".join(argv[:i])


def _program(words: list[str], assigns: list[str]) -> list[str]:
    i = 0
    while i < len(words):
        w = words[i]
        if _ASSIGN.match(w):
            assigns.append(w)
            i += 1
        elif w in _PREFIX_WORDS:
            i += 1 + (w == "timeout")
            while i < len(words) and words[i].startswith("-"):
                i += 2 if w in ("sudo", "env") and words[i] in ("-u", "-i", "-C", "-g") else 1
        else:
            break
    return words[i:]


def _strip_redirects(words: list[str]) -> tuple[list[str], str | None]:
    out, here, i = [], None, 0
    while i < len(words):
        w = words[i]
        if w in ("<<", "<<-") or _REDIRECT_OP.fullmatch(w):
            if w.endswith("<<<") and i + 1 < len(words):
                here = words[i + 1]
            i += 2
        else:
            out.append(w)
            i += 1
    return out, here


def _nested_shell(seg: _Seg) -> str | None:
    """Shell text a segment hands to another shell: `sh -c '...'`, `eval '...'`, a literal piped
    into `bash`. Runtime-built variants are the opaque-execution check's business."""
    argv = seg.argv
    if seg.argv0 in _SHELLS:
        for i, w in enumerate(argv[1:-1], 1):
            if re.fullmatch(r"-[A-Za-z]*c[A-Za-z]*", w):
                arg = argv[i + 1] if argv[i + 1] != "--" or i + 2 >= len(argv) else argv[i + 2]
                return None if _expands(arg) else arg
        if len(argv) == 1 and seg.stdin and not seg.opaque:
            return "\n".join(seg.stdin)
    elif seg.argv0 == "eval" and len(argv) > 1:
        text = " ".join(argv[1:])
        return None if _expands(text) else text
    return None


def _segments(text: str, ctx: str = "", depth: int = 0) -> list[_Seg]:
    cmds = _commands(_shell_tokens(text))
    bodies = _heredoc_bodies(text)
    heredocs_of: list[list[str]] = []
    for c in cmds:
        n = sum(1 for w in c.words if w in ("<<", "<<-"))
        heredocs_of.append(bodies[:n])
        bodies = bodies[n:]
    aliases: dict[str, list[str]] = {}
    out: list[_Seg] = []
    for k, c in enumerate(cmds):
        words, herestring = _strip_redirects(c.words)
        if words and words[0] in aliases:
            words = aliases[words[0]] + words[1:]
        assigns: list[str] = []
        argv = _program(words, assigns)
        argv, wrapper = _unwrap(argv)
        stdin, opaque = [], None
        for p in _producers(cmds, k):
            base = p.words[0].rsplit("/", 1)[-1] if p.words else ""
            if base not in _TEXT_PRODUCERS or _expands(" ".join(p.words)):
                opaque = base or "?"
            elif base in ("echo", "printf"):
                stdin.append(" ".join(w for w in _strip_redirects(p.words)[0][1:] if not re.fullmatch(r"-[neE]+", w)))
            elif base == "cat":
                stdin.extend(heredocs_of[cmds.index(p)])
        redirects = [w for i, w in enumerate(c.words) if _REDIRECT_OP.fullmatch(w) or (i and _REDIRECT_OP.fullmatch(c.words[i - 1]))]
        seg = _Seg(argv, c.words, heredocs_of[k], assigns, herestring, stdin, opaque,
                   _scripts_of(cmds, k, argv + redirects if wrapper else None), " ".join(x for x in (ctx, wrapper) if x))
        if seg.argv0 == "alias":
            for a in argv[1:]:
                if "=" in a:
                    name, value = a.split("=", 1)
                    aliases[name] = shlex.split(value) if value else []
        nested = _nested_shell(seg)
        out.append(seg)
        if nested is not None and depth < 4:
            out.extend(_segments(nested, seg.ctx, depth + 1))
    return out


# ---------------------------------------------------------------- policy

def _flag_values(argv: list[str], flags: tuple[str, ...]) -> list[str]:
    out = []
    for i, w in enumerate(argv[1:], 1):
        if w in flags and i + 1 < len(argv) and not argv[i + 1].startswith("-"):
            out.append(argv[i + 1])
        elif "=" in w and w.split("=", 1)[0] in flags:
            out.append(w.split("=", 1)[1])
    return out


def _sql_text(seg: _Seg, root: Path, extra: list[str] = ()) -> tuple[str, list[str]]:
    """Every piece of SQL a client segment executes, joined, plus the script files it cannot read."""
    parts = [*extra, *_flag_values(seg.argv, _SQL_VALUE_FLAGS), *seg.stdin, *seg.heredocs]
    if seg.herestring:
        parts.append(seg.herestring)
    unreadable = []
    for f in seg.scripts:
        body = _read_script(f, root)
        (unreadable.append(f) if body is None else parts.append(body))
    return "\n;\n".join(parts), unreadable


def _unreadable_msg(files: list[str], who: str) -> str:
    return (f"{who} fed script(s) {files} that the guard cannot read in full (missing, unreadable or over "
            f"{_MAX_SCRIPT_BYTES // (1024 * 1024)} MiB); inline the SQL or split it so it can be inspected")


def _statements(sql: str) -> list[str]:
    """Statements of a SQL text: split at `;`, with directive lines (`.LOGON`, `\\dt`, `:setvar`,
    `@file`, `GO`, SQL*Plus words) as statements of their own."""
    out: list[str] = []
    for chunk in _sql_view(sql).split(";"):
        cur: list[str] = []
        for line in chunk.split("\n"):
            if _DIRECTIVE_LINE.match(line):
                out.append(" ".join(cur))
                out.append(line.strip())
                cur = []
            elif line.strip():
                cur.append(line.strip())
        out.append(" ".join(cur))
    return [s for s in out if s]


def _is_read(stmt: str) -> bool:
    s = stmt.strip()
    if s.startswith("."):
        return not re.match(r"\.os\b", s, re.IGNORECASE)
    if s.startswith("\\"):
        return bool(_PSQL_META.match(s))
    if s.startswith(":"):
        return bool(_SQLCMD_DIRECTIVE.match(s))
    if s.startswith("@") or s == "/":
        return True
    m = re.match(r"[A-Za-z_]+", s)
    head = m.group(0).upper() if m else ""
    if head in _DESCRIBE_HEAD or head in _SQLPLUS_DIRECTIVE:
        return True
    if head not in _READ_HEAD or (head == "SET" and _SET_DENY.match(s)):
        return False
    return not _NON_READ_WORD.search(s)


def _non_read(seg: _Seg, sql: str) -> list[str]:
    bad = [s for s in _statements(sql) if not _is_read(s)]
    if seg.argv0 == "bcp" and "in" in seg.argv[1:4]:
        bad.insert(0, "bcp ... in (loader)")
    if seg.opaque:
        bad.insert(0, f"stdin from `{seg.opaque}`, a program or expansion the guard cannot read")
    return [b.split("\n", 1)[0][:80] for b in bad]


def _hosts(seg: _Seg) -> list[str]:
    """Host / DSN candidates of a generic client: host flags, positionals, `$VAR` names, URI hosts."""
    sql = set(_flag_values(seg.argv, _SQL_VALUE_FLAGS)) | set(seg.scripts)
    out = list(_flag_values(seg.argv, _HOST_FLAGS))
    out += [w for w in seg.argv[1:] if not w.startswith("-") and w not in sql]
    joined = " ".join(seg.argv[1:])
    out += re.findall(r"\$\{?(\w+)\}?", joined)
    out += re.findall(r"://(?:[^@/\s]*@)?([^:/?\s]+)", joined)
    out += re.findall(r"(?i)\b(?:host|server|data source)=([^;\s]+)", joined)
    return [re.split(r"[,:\\]", h, 1)[0].lower() for h in out if h]


def _check_sql_client(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    base = seg.argv0
    legacy_only = base in _LEGACY_ONLY
    hits = _mentions_legacy(seg.text, cfg)
    tail = " (legacy is read-only in every phase)"
    if base in _LOADERS:
        return [f"`{base}` is a loader: nothing but reads ever runs against a legacy source" + tail]
    sql, unreadable = _sql_text(seg, root)
    bad = _non_read(seg, sql)
    violations = []
    if unreadable:
        if legacy_only or hits:
            violations.append(_unreadable_msg(unreadable, "legacy client") + tail)
        else:
            bad.insert(0, f"script(s) {unreadable} the guard cannot read")
    if not bad:
        return violations
    if legacy_only:
        violations.append(f"non-read statement through a legacy-only client `{base}`: `{bad[0]}`" + tail)
    elif hits:
        violations.append(f"non-read statement against legacy source {hits}: `{bad[0]}`" + tail)
    elif not any(h in cfg.target_hosts for h in _hosts(seg)):
        violations.append(f"non-read statement through `{base}` to a host that is not a literal in target_hosts "
                          f"{cfg.target_hosts} (seen: {sorted(set(_hosts(seg)))[:6]}; an empty list blocks every write): `{bad[0]}`")
    return violations


def _catalogs_in_segment(seg: str) -> set[str]:
    """Catalog of the statement's write target: the securable named right after the verb phrase,
    when qualified. An unqualified target resolves to nothing (the caller falls back to `USE
    CATALOG`), never to a qualified source further along."""
    start = len(seg) - len(seg.lstrip())
    for rx in (_SCHEMA_TWO_PART, _CREATE_CATALOG):
        m = rx.match(seg, start)
        if m:
            return {_norm(m.group(1))}
    for rx in (_ON_SCHEMA, _ON_CATALOG):
        m = rx.search(seg)
        if m:
            return {_norm(m.group(1))}
    head = _WRITE_TARGET_HEAD.match(seg, start)
    if head:
        m = _TARGET_NAME.match(seg, head.end())
        if m:
            return {_norm(m.group(1))}
    return set()


def _catalog_violations(sql: str, cfg: GuardConfig, default: str | None, in_dbx: bool) -> list[str]:
    """Writes in Databricks SQL must target an allowlisted catalog: three-part name, else the last
    `USE CATALOG` before the statement, else the `--catalog` default; none of those resolves in a
    Databricks client → block. Dynamic names (`IDENTIFIER(<expr>)`, `EXECUTE IMMEDIATE <var>`) block."""
    allowed = set(cfg.catalogs)
    text = _sql_view(_IDENTIFIER_LITERAL.sub(r"\1", sql))
    violations = []
    if in_dbx and _EXEC_IMMEDIATE_DYNAMIC.search(text):
        violations.append("EXECUTE IMMEDIATE on a non-literal; the statement is built at run time, so inline it as text")
    use_cats = [(m.start(), _norm(m.group(1))) for m in _USE_CATALOG.finditer(text)]
    for offset, seg in _write_segments(text):
        use_cat = next((c for pos, c in reversed(use_cats) if pos < offset), default)
        cats = _catalogs_in_segment(seg)
        head = seg.strip().split("\n", 1)[0][:80]
        if _IDENTIFIER_DYNAMIC.search(seg):
            violations.append(f"IDENTIFIER(<non-literal>) names the target of a write at run time: `{head}`")
        elif cats:
            bad = sorted(c for c in cats if c not in allowed)
            if bad:
                violations.append(f"write to catalog(s) {bad} outside allowlist {sorted(allowed)}: `{head}`")
        elif use_cat is not None:
            if use_cat not in allowed:
                violations.append(f"write under USE CATALOG {use_cat!r} outside allowlist {sorted(allowed)}: `{head}`")
        elif in_dbx:
            violations.append(f"write with unresolvable catalog (not three-part qualified, no USE CATALOG) in a Databricks "
                              f"command: `{head}`")
    return violations


def _check_target(kind: str, words: list[str], cfg: GuardConfig) -> list[str]:
    m = _BUNDLE_TARGET.search(" " + " ".join(words))
    if not m:
        return [f"`{kind}` without a literal -t/--target; allowed bundle_targets {cfg.bundle_targets} (empty = every deploy blocks)"]
    t = m.group(1)
    if _expands(t) or not re.fullmatch(r"[\w.-]+", t):
        return [f"`{kind}` target `{t}` is not a literal; use a name from bundle_targets {cfg.bundle_targets}"]
    if t.lower() in cfg.forbidden_bundle_targets:
        return [f"`{kind}` to forbidden target {t!r} (forbidden_bundle_targets); production deploys happen only at STOP E"]
    if t.lower() not in cfg.bundle_targets:
        return [f"`{kind}` target {t!r} not in bundle_targets {cfg.bundle_targets} (empty = every deploy blocks)"]
    return []


def _check_databricks(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    argv = seg.argv[1:]
    if any(w in ("--version", "-v", "-h", "--help", "version", "help") for w in argv):
        return []
    path, i = [], 0
    while i < len(argv):
        if argv[i].startswith("-") and argv[i] != "--":
            i += 2 if argv[i] in _DBX_VALUE_FLAGS else 1
        else:
            path.append(argv[i])
            i += 1
    sql_flags = _flag_values(seg.argv, _SQL_VALUE_FLAGS)
    group, verb, args = (path + ["", ""])[0], (path + ["", ""])[1], path[2:]
    default = next(iter(_flag_values(seg.argv, ("--catalog",))), None)
    default = _norm(default) if default else None
    if (group == "sql" and verb == "execute") or (path[:4] == ["experimental", "aitools", "tools", "query"]) or (not path and sql_flags):
        positional = [] if group == "sql" else [w for w in path[4:] if w != "--"]
        sql, unreadable = _sql_text(seg, root, positional)
        return ([_unreadable_msg(unreadable, "Databricks client")] if unreadable else []) + _catalog_violations(sql, cfg, default, True)
    if group == "bundle":
        if verb in ("deploy", "run", "destroy"):
            return _check_target(f"databricks bundle {verb}", argv, cfg)
    elif group == "api" and verb != "get":
        m = _UC_PATH.search(" ".join(args))
        if m and _norm(m.group(1).split(".")[0]) in cfg.catalogs:
            return []
    elif group == "fs" and verb in ("cp", "rm", "mkdir", "mkdirs"):
        cats = [_norm(m.group(1)) for a in args if (m := _VOLUME_PATH.match(a))]
        if cats and all(c in cfg.catalogs for c in cats):
            return []
        return [f"`databricks fs {verb}` outside an allowlisted volume (/Volumes/<catalog>/...): {args}"]
    key = f"{group} {verb}"
    if key in _CLI_CATALOG_ARG:
        idx = _CLI_CATALOG_ARG[key]
        name = args[idx] if len(args) > idx else ""
        if name and _norm(name.split(".")[0]) in cfg.catalogs:
            return []
        return [f"CLI mutation of securable {name!r} outside allowlist {sorted(cfg.catalogs)}"]
    if verb in _DBX_READ.get(group, ()):
        return []
    return [f"`databricks {group} {verb}`".rstrip() + " is not in the guard's read allowlist (fail closed); reads are "
            "list/get shapes, writes go through an allowlisted securable or the migration workflow"]


def _check_rest(seg: _Seg) -> list[str]:
    argv = seg.argv[1:]
    if not any(_DBX_HOST.search(w) for w in argv):
        return []
    method, body = "GET", False
    for i, w in enumerate(argv):
        nxt = argv[i + 1] if i + 1 < len(argv) else ""
        if seg.argv0 == "curl":
            if w in ("-X", "--request"):
                method = nxt.upper()
            elif w.startswith(("-X", "--request=")):
                method = w.split("=", 1)[-1][2 if w.startswith("-X") else 0:].upper()
            elif w.startswith(("--data", "--json", "--form", "--upload-file")) or re.fullmatch(r"-[a-zA-Z]*[dFT][a-zA-Z]*", w):
                body = True
        elif seg.argv0 == "wget":
            if w.startswith("--method"):
                method = (w.split("=", 1)[1] if "=" in w else nxt).upper()
            elif w.startswith(("--post-", "--body-")):
                body = True
        elif w.upper() in _HTTP_METHODS:
            method = w.upper()
        elif not w.startswith("-") and re.match(r"^[\w.-]+:?=(?!=)", w):
            body = True
    if method in ("GET", "HEAD") and not body:
        return []
    how = f"REST {method}{' with a request body' if body else ''}"
    return [f"{how} to a Databricks workspace through `{seg.argv0}`; only GET without a body passes"]


def _check_identity(segs: list[_Seg]) -> list[str]:
    violations = []
    exported = [w for s in segs for w in [*s.assigns, *s.argv[:2]] if _IDENTITY_VAR.match(w)]
    exported += [s.argv[1] for s in segs if s.argv0 == "export" and len(s.argv) > 1 and _IDENTITY_VAR.match(s.argv[1])]
    for s in segs:
        if s.argv0 not in _IDENTITY_CLIENTS:
            continue
        for v in exported:
            violations.append(f"identity swap: `{v.split('=', 1)[0]}=` set around `{s.argv0}`; the session runs as the doctor-verified "
                              "migration principal only")
        if s.argv0 == "databricks":
            flags = [w for w in s.argv[1:] if w in ("--profile", "-p", "--host") or w.startswith(("--profile=", "--host="))]
            if flags or s.argv[1:3] == ["auth", "login"] or s.argv[1:2] == ["configure"]:
                violations.append(f"identity swap through `databricks {' '.join(flags or s.argv[1:3])}`; the session runs as the "
                                  "doctor-verified migration principal only")
    return violations


def _protected(path: str, cwd: str) -> bool:
    p = os.path.normpath(path if path.startswith("/") else os.path.join(cwd, path))
    parts = p.split("/")
    if ".migration" not in parts:
        return False
    rest = parts[parts.index(".migration") + 1:]
    return not (len(rest) > 1 and rest[0] in ("recon", "waves"))


_WRITE_ANY_OPERAND = ("rm", "rmdir", "truncate", "chmod", "chown", "chgrp", "touch", "mkdir", "shred", "unlink", "tee", "mv")
_WRITE_LAST_OPERAND = ("cp", "rsync", "install", "ln")
_GIT_WRITES = ("checkout", "restore", "rm", "mv", "clean")


def _check_integrity(segs: list[_Seg]) -> list[str]:
    """Nothing but the recon harness and the workflow writes under `.migration/`."""
    violations, cwd = [], ""

    def hit(path: str, how: str) -> None:
        if _protected(path, cwd):
            violations.append(f"`{how}` writes `{path}` under .migration/ (only .migration/recon/ and .migration/waves/ are "
                              "written by commands; ledgers and the allowlist change only through a recorded decision)")

    for s in segs:
        base, argv = s.argv0, s.argv
        operands = [w for w in argv[1:] if not w.startswith("-")]
        for op, operand in zip(s.words, [*s.words[1:], ""]):
            if _REDIRECT_OP.fullmatch(op) and ">" in op and operand and not operand.isdigit() and operand != "-":
                hit(operand, f"{op} {operand}")
        if base == "cd" and operands and not _expands(operands[0]):
            cwd = os.path.normpath(operands[0] if operands[0].startswith("/") else os.path.join(cwd, operands[0]))
        elif base in _WRITE_ANY_OPERAND or (base in ("sed", "perl") and any(w.startswith(("-i", "--in-place")) for w in argv)):
            for o in operands:
                hit(o, base)
        elif base in _WRITE_LAST_OPERAND and operands:
            hit(operands[-1], base)
        elif base == "git" and argv[1:2] and argv[1] in _GIT_WRITES:
            for o in operands[1:]:
                hit(o, f"git {argv[1]}")
        elif _PYTHON.fullmatch(base) or base in ("perl", "ruby", "node"):
            text = "\n".join([*_flag_values(argv, ("-c", "-e")), *s.heredocs])
            if _PY_WRITE.search(text):
                for p in _MIGRATION_PATH.findall(text):
                    hit(p, f"{base} script")
    return violations


def _check_python(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    """Literal SQL handed to an executor inside a program the command runs (`-c`, heredoc, script
    file); the rest of the program is the doctor's read-only-principal row's business."""
    argv = seg.argv
    texts = [*_flag_values(argv, ("-c",)), *seg.heredocs]
    if "-m" not in argv and not texts:
        script = next((w for w in argv[1:] if w.endswith(".py")), None)
        if script:
            body = _read_script(script, root)
            if body is None and seg.argv0 == "spark-submit":
                return [f"spark-submit script {script!r} cannot be read; the guard cannot clear a Spark job it cannot inspect"]
            texts.append(body or "")
    text = "\n".join(texts)
    hits = _mentions_legacy(seg.text + " " + text, cfg)
    violations = []
    for m in _PY_LITERAL.finditer(text):
        lit = m.group(2)
        if hits:
            bad = [s for s in _statements(lit) if not _is_read(s)]
            if bad:
                violations.append(f"non-read statement against legacy source {hits} in a program: `{bad[0][:80]}` "
                                  "(legacy is read-only in every phase)")
        else:
            violations += _catalog_violations(lit, cfg, None, False)
    return violations


def _check_segment(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    base = seg.argv0
    if base == "databricks":
        return _check_databricks(seg, cfg, root)
    if base in ("spark-sql", "dbsqlcli"):
        sql, unreadable = _sql_text(seg, root)
        return ([_unreadable_msg(unreadable, "Databricks client")] if unreadable else []) + _catalog_violations(sql, cfg, None, True)
    if base == "dbx-recon":
        return [f"--target-catalog {_norm(c)!r} outside allowlist {sorted(cfg.catalogs)}"
                for c in _flag_values(seg.argv, ("--target-catalog",)) if _norm(c) not in cfg.catalogs]
    if base == "dbt" and seg.argv[1:2] and seg.argv[1] in ("run", "build", "seed"):
        return _check_target(f"dbt {seg.argv[1]}", seg.argv[2:], cfg)
    if base in _REST_CLIENTS:
        return _check_rest(seg)
    if base in _LEGACY_ONLY or base in _GENERIC:
        return _check_sql_client(seg, cfg, root)
    if _PYTHON.fullmatch(base):
        return _check_python(seg, cfg, root)
    return []


def evaluate(command: str, cfg: GuardConfig, root: Path | None = None) -> Verdict:
    command = _join_continuations(command)
    root = root or _project_root()
    parts, unreadable_shell = [command], []
    for f in _shell_script_inputs(command):
        body = _read_script(f, root)
        (unreadable_shell.append(f) if body is None else parts.append("\n;\n" + _join_continuations(body)))
    text = "\n".join(parts)
    violations: list[str] = []
    if PROBE_SENTINEL in text:
        violations.append(f"`{PROBE_SENTINEL}` is the factory-doctor's hook probe; it always blocks so the doctor can tell the "
                          "hook is loaded without touching Databricks")
    if unreadable_shell:
        violations.append(f"shell script(s) {unreadable_shell} the command would run cannot be read in full (missing, unreadable "
                          f"or over {_MAX_SCRIPT_BYTES // (1024 * 1024)} MiB); the guard cannot clear what it cannot read")
    violations += _check_opaque_execution(text, cfg)
    segs = _segments(text)
    violations += _check_identity(segs) + _check_integrity(segs)
    for seg in segs:
        violations += _check_segment(seg, cfg, root)
    return _verdict(list(dict.fromkeys(violations)), cfg)


def _verdict(violations: list[str], cfg: GuardConfig) -> Verdict:
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


def _cd_targets(cmd: str) -> list[str | None]:
    """Directories the command changes into (`cd d`, `pushd d`), in order; None for one the guard
    cannot resolve (`cd -`, an unexpanded variable, a substitution)."""
    toks = _raw_tokens(cmd)
    out: list[str | None] = []
    for i, tok in enumerate(toks):
        if tok not in ("cd", "pushd") or (i > 0 and toks[i - 1] not in _SEPARATORS):
            continue
        j = i + 1
        while j < len(toks) and toks[j].startswith("-") and len(toks[j]) > 1 and toks[j] not in _SEPARATORS:
            j += 1
        if j >= len(toks) or toks[j] in _SEPARATORS:
            out.append(os.path.expanduser("~"))
            continue
        target = _strip_quotes(toks[j])
        if target == "-" or _expands(target):
            expanded = os.path.expandvars(target)
            out.append(None if target == "-" or _expands(expanded) else expanded)
        else:
            out.append(target)
    return out


def evaluate_with_workdirs(command: str, cfg: GuardConfig, root: Path) -> Verdict:
    """`evaluate` against the starting workspace and every workspace the command `cd`s into: a
    write must be allowed by each allowlist involved, and a client command that moves to a
    directory the guard cannot resolve is not clearable."""
    violations: list[str] = []
    first = evaluate(command, cfg, root)
    violations += first.violations or ([first.reason] if first.decision == "block" else [])
    seen = {cfg.path}
    cwd = root
    for target in _cd_targets(command):
        if target is None:
            if _has_context(command, cfg):
                violations.append("command changes to a directory the guard cannot resolve before running a Databricks or "
                                  "legacy client; the allowlist in force there is unknown")
            break
        cwd = (cwd / os.path.expanduser(target)).resolve()
        try:
            other = load_config(cwd)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            violations.append(f"cannot read {CONFIG_REL} for {cwd}: {exc}")
            continue
        if other is None or other.path in seen:
            continue
        seen.add(other.path)
        v = evaluate(command, other, cwd)
        violations += [f"[{other.path}] {x}" for x in v.violations]
    return _verdict(list(dict.fromkeys(violations)), cfg)


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
        # a broken allowlist is itself a violation of setup step 7: refuse rather than guess
        print(json.dumps({"decision": "block", "reason": f"dbx-migration-factory guard: cannot read {CONFIG_REL}: {exc}"}))
        print(f"dbx-migration-factory guard: cannot read {CONFIG_REL}: {exc}", file=sys.stderr)
        return 2
    if cfg is None:
        return 0
    verdict = evaluate_with_workdirs(command, cfg, _project_root())
    if verdict.decision == "block":
        print(json.dumps({"decision": "block", "reason": verdict.reason}))
        print(verdict.reason, file=sys.stderr)
        return 2
    if verdict.reason:
        print(json.dumps({"decision": "approve", "reason": verdict.reason}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
