#!/usr/bin/env python3
"""PreToolUse guard for the DBX migration factory: recognise the client, allow known-read shapes,
block the rest.

Reads a Devin PreToolUse event on stdin ({"tool_name", "tool_input": {"command"}}), locates the
engagement's `.migration/allowed_targets.json`, splits the shell command into simple commands and
judges each by the program it runs:

* `databricks`: an explicit read allowlist passes (`current-user me`, `<securable> list|get`, `sql
  execute`, `api get`, `bundle validate|summary`, `fs ls|cat|head`, `jobs|pipelines|warehouses|
  clusters list|get...`, `workspace list|export|get-status`, `secrets list-scopes|list-secrets`,
  `auth describe|profiles`, `--version`, `-h`; `auth token|env` print the bearer token and block). A
  mutation passes only when the securable it names sits in an allowlisted catalog (`tables delete
  mig_cat.s.t`, `fs rm dbfs:/Volumes/mig_cat/...`). SQL handed to `sql execute` / `spark-sql` /
  `dbsqlcli` may write only to allowlisted catalogs: three-part names, `USE CATALOG` / `--catalog`
  per segment; a write resolving to no catalog, `IDENTIFIER(<non-literal>)` in a write and
  `EXECUTE IMMEDIATE <non-literal>` block.
* `curl`/`wget`/`http` to `$DATABRICKS_HOST` or a *.databricks.com / *.azuredatabricks.net host:
  GET without a body only.
* `databricks bundle deploy|run|destroy` and `dbt run|build|seed`: a literal `-t/--target` that is
  in `bundle_targets` and not in `forbidden_bundle_targets`.
* Identity: `databricks auth login|configure`, `--profile/-p/--host`, and `DATABRICKS_TOKEN=`,
  `DATABRICKS_HOST=`, `DATABRICKS_CONFIG_PROFILE=`, `DATABRICKS_CLIENT_*=` (inline, `export`, `env`)
  around a `databricks`, `dbx-recon` or `spark-sql` command block, as does any write to the CLI's
  credential store (`.databrickscfg` anywhere, `~/.databricks/`, `~/.config/databricks/`).
* Legacy-only clients (bteq, sqlplus, snowsql, fastexport) and generic SQL clients (sqlcmd, psql,
  mysql, isql, ...) whose command mentions a `legacy_sources` entry: every statement must be a read
  shape (SELECT, WITH, SHOW, DESCRIBE, HELP, USE, DECLARE, SET <session option>, a client directive,
  EXPLAIN of a read shape -- `EXPLAIN ANALYZE` executes its statement) with no write keyword anywhere
  and none of the side-effecting functions a SELECT can smuggle (`_SIDE_EFFECT_FN`: nextval/setval,
  pg_terminate/cancel_backend, pg_reload_conf, advisory locks, lo_*, dblink*, pg_sleep, set_config,
  Oracle `.NEXTVAL`/DBMS_*/UTL_*, T-SQL OPENROWSET/OPENQUERY/OPENDATASOURCE/xp_*/sp_*): a denylist
  exception to the read grammar, because they mutate, disrupt or reach outside the source while
  the statement still starts with SELECT. Loaders (sqlldr, mload, fastload, tbuild, tdload,
  `bcp ... in`) always block. Generic clients elsewhere: a non-read statement needs a host or DSN
  name that is a literal in `target_hosts`; a variable, an IP, an unlisted name or an empty list
  blocks.
* Any write under `.migration/` (redirects, `sed -i`, `tee`, `cp/mv/rm/rmdir/truncate/chmod/mkdir/
  touch`, `git checkout|restore|rm`, inline Python naming a `.migration/` path with a write call)
  blocks, except under `.migration/recon/` and `.migration/waves/`. Reads stay approved. The same
  writer detection protects the running guard's own plugin tree (`hooks.json`, `hooks/**`, resolved
  through symlinks) from edits, `chmod`, `rm`, `mv`; a checkout of this repo elsewhere is an ordinary
  development target.
* `python x.py` / `spark-submit x.py` / `python -c` / a Python heredoc: a literal SQL string handed
  to `.execute(`, `.sql(`, `execute_statement(` or `statement=` is judged like the clients above;
  anything else in a program approves, and an unreadable `spark-submit` script blocks. Programs
  that open their own connection (Python, JDBC, perl) are otherwise the business of the doctor's
  `source_principal_read_only` row, not of this hook.

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
handed to a client (`< f`, `-f f`, `@f`), heredocs, here-strings, literal `echo`/`printf`/`cat`
producers piped in, `sh -c '...'`, `ssh host '...'`, `docker exec ...` and `alias` bodies. Text it
cannot read blocks where a client is involved: an unreadable or oversized script, command
substitution, `eval`, a shell fed a `$`-built string, decoded bytes or process substitution, an
expansion inside the SQL argument, an unquoted heredoc that expands, an opaque program piped into a
legacy client. A redirection after `)`/`}` reaches every member of the group. A command that
changes directory is judged against the allowlist of every workspace it enters as well.

Outside a migration workspace (no `.migration/allowed_targets.json` up the tree) the guard approves
everything. Malformed input approves (plugin hooks fail open by platform design; the factory-doctor
reports whether the hook is loaded and whether it blocks its probe command, which carries
`__dbx_guard_probe__` and always blocks). `hooks/tests/test_probe_table.py` is the red-team table
this policy is pinned to; add a row there before changing a shape.
"""
from __future__ import annotations

import fnmatch
import itertools
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
PROBE_SENTINEL = "__dbx_guard_probe__"   # prefix of the doctor's HOOK_PROBE_COMMAND token; a command naming it always blocks
_PROBE = re.compile(re.escape(PROBE_SENTINEL) + r"\w*")

_SEG = r"(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$-]*)"
_OBJ = r"TABLE|VIEW|FUNCTION|PROCEDURE|VOLUME|INDEX|TRIGGER|SEQUENCE"
# a write statement's verb phrase; the match ends where its target securable starts
_WRITE = re.compile(
    rf"""(?:\b(?:
        INSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?
      | UPDATE\s+(?!STATISTICS\b|SET\b|OF\b)(?:TOP\s*\([^)]*\))?
      | DELETE\s+FROM
      | MERGE\s+(?:WITH\s+SCHEMA\s+EVOLUTION\s+)?INTO
      | (?:TRUNCATE|REPLACE|RESTORE|REFRESH|REORG|ANALYZE)\s+TABLE
      | (?:OPTIMIZE|VACUUM)(?:\s+TABLE)?
      | REFRESH\s+MATERIALIZED\s+VIEW
      | MSCK\s+REPAIR\s+TABLE
      | SYNC\s+(?:AS\s+EXTERNAL\s+)?(?=(?:SCHEMA|TABLE)\b)(?:TABLE\s+)?
      | COPY\s+INTO
      | CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+|EXTERNAL\s+|STREAMING\s+|MATERIALIZED\s+|LIVE\s+)*
        (?=(?:{_OBJ}|SCHEMA|DATABASE|CATALOG)\b)(?:(?:{_OBJ})\s+(?:IF\s+NOT\s+EXISTS\s+)?)?
      | (?:DROP|ALTER)\s+(?=(?:{_OBJ}|SCHEMA|DATABASE|CATALOG)\b)(?:(?:{_OBJ})\s+(?:IF\s+EXISTS\s+)?)?
      | UNDROP\s+(?=(?:TABLE|SCHEMA)\b)(?:TABLE\s+)?
      | COMMENT\s+ON\s+(?:(?:{_OBJ}|MATERIALIZED\s+VIEW|COLUMN)\s+)?
      | (?:GRANT|REVOKE|DENY)\s+.+?\bON\s+(?:(?:{_OBJ}|MATERIALIZED\s+VIEW)\s+)?
      | (?:EXEC(?:UTE)?|CALL)\s+(?!IMMEDIATE\b)(?=(?:\[?[\w$]+\]?\.)+\[?[\w$]+)
    )
      | (?:^|(?<=[;\n]))\s*(?:EXEC(?:UTE)?|CALL)\s+(?!IMMEDIATE\b)(?=[\[@`\w])
    )\s*""",
    re.IGNORECASE | re.VERBOSE,
)
_TARGET = re.compile(rf"(?:(CATALOG|SCHEMA|DATABASE)\s+)?(?:IF\s+(?:NOT\s+)?EXISTS\s+)?({_SEG})((?:\.{_SEG})*)(?![\w`.])",
                     re.IGNORECASE)
_USE_CATALOG = re.compile(rf"\bUSE\s+CATALOG\s+({_SEG})", re.IGNORECASE)
_IDENTIFIER_LITERAL = re.compile(r"\bIDENTIFIER\s*\(\s*'([^']*)'\s*\)", re.IGNORECASE)
_IDENTIFIER_DYNAMIC = re.compile(r"\bIDENTIFIER\s*\(", re.IGNORECASE)
_EXEC_IMMEDIATE_DYNAMIC = re.compile(r"\bEXEC(?:UTE)?\s+IMMEDIATE\s+(?!')\S", re.IGNORECASE)
_BUNDLE_TARGET = re.compile(r"(?:^|\s)(?:-t|--target)(?:=|\s+)(\S+)")
_PERMISSION = re.compile(r"^\s*(?:GRANT|REVOKE|DENY)\b.*\bON\s+CATALOG\b|^\s*(?:CREATE|ALTER|DROP)\s+CATALOG\b", re.IGNORECASE | re.DOTALL)

_LEGACY_ONLY = ("bteq", "sqlplus", "sqlldr", "snowsql", "mload", "fastload", "fastexport", "tbuild", "tdload")
_LOADERS = ("sqlldr", "mload", "fastload", "tbuild", "tdload")
_WRITERS = ("pg_restore", "pgloader", "liquibase", "flyway", "sqitch")   # migration tools: every run writes its target
_GENERIC = ("psql", "pgcli", "sqlcmd", "osql", "isql", "tsql", "mysql", "mariadb", "sqlite3", "bcp", "beeline", "trino", "presto",
            "mssql-cli", "go-sqlcmd", "usql", *_WRITERS)
_DBX_CLIENTS = ("databricks", "dbx-recon", "spark-sql", "dbsqlcli")
_IDENTITY_CLIENTS = ("databricks", "dbx-recon", "spark-sql")
_PYTHON = re.compile(r"python[0-9.]*|spark-submit")
_REST_CLIENTS = ("curl", "wget", "http", "https", "xh")
_CLIENT_WORD = re.compile(r"(?<![\w-])(?:" + "|".join(map(re.escape, (*_DBX_CLIENTS, *_LEGACY_ONLY, *_GENERIC))) + r")(?![\w-])")

_SHELLS = ("sh", "bash", "zsh", "dash", "ksh")
_SEPARATORS = (";", "&&", "||", "|", "|&", "&", "(", ")", "{", "}", "\n")
_PREFIX_WORDS = ("sudo", "env", "nohup", "time", "exec", "command", "nice", "xargs", "timeout", "stdbuf", "uvx", "npx", "pipx")
_PREFIX_VALUE_FLAGS = {"sudo": ("-u", "-g", "-C"), "env": ("-u", "--unset", "-C", "-S"), "uvx": ("--from", "--with", "-p", "--python"),
                       "npx": ("-p", "--package"), "pipx": ("--spec",)}
_ASSIGN = re.compile(r"^[A-Za-z_]\w*=")
# credential / endpoint / profile selection of the Databricks CLI and SDK: never changed from a session
_IDENTITY_VAR = re.compile(r"^(?:DATABRICKS_\w+|ARM_CLIENT_\w+|ARM_TENANT_ID|AZURE_\w+|GOOGLE_CREDENTIALS|GOOGLE_APPLICATION_CREDENTIALS)$")
_CONFIG_HOME_VAR = ("HOME", "XDG_CONFIG_HOME")   # where ~/.databrickscfg is looked up; protected on a client's own segment
_FUNCTION_DEF = re.compile(r"(?:^|[;&|\n{}()]\s*)(?:function\s+[\w.-]+|[\w.-]+\s*\(\s*\))")
# flags whose value is the SQL text itself (psql -c, sqlcmd/isql/snowsql -Q/-q, spark-sql/dbsqlcli/mysql -e ...)
_SQL_VALUE_FLAGS = ("-c", "-Q", "-q", "-e", "--query", "--sql", "--statement", "--execute", "--command")
_SCRIPT_FLAGS = ("-f", "-i", "--file", "--input")
_HOST_FLAGS = ("-S", "-h", "-H", "--host", "--server", "--hostname", "--url", "-url")
_HOST_ENV = ("PGHOST", "PGHOSTADDR", "PGSERVICE", "MYSQL_HOST", "SQLCMDSERVER")
_DSN_POSITIONAL = ("isql", "usql", "pgloader")   # clients whose first positional is the DSN / URL, not a database name
_RUN_FILE = re.compile(r"(?<!\S)@(\S+)|^\s*\.RUN\s+FILE\s*=?\s*(\S+)", re.IGNORECASE | re.MULTILINE)
_DYNAMIC_SQL_EXECUTOR = (r"(?:\bEXEC(?:UTE)?\s+IMMEDIATE|\bsp_executesql|\bEXEC(?:UTE)?\s*\(|"
                         r"\.(?:execute|executemany|sql|run_query|execute_statement)\s*\(|\bstatement\s*=)")
_DYNAMIC_SQL_CALLER = re.compile(_DYNAMIC_SQL_EXECUTOR + r"\s*N?\s*$", re.IGNORECASE)
_PY_LITERAL = re.compile(_DYNAMIC_SQL_EXECUTOR + r"\s*[rbuf]*(['\"]{3}|['\"])(.*?)\1", re.IGNORECASE | re.DOTALL)
_PY_WRITE = re.compile(r"""['"][wax]\+?['"]|\.write\w*\(|json\.dump\(|os\.(?:remove|unlink|rename|replace|chmod|rmdir|makedirs|mkdir)\(|"""
                       r"""shutil\.|\.(?:unlink|rename|rmdir|mkdir|touch|chmod)\(""")
_MIGRATION_PATH = re.compile(r"[^\s'\"()]*\.migration(?:/[^\s'\"()]*)?")
_RMTREE = re.compile(r"rmtree\(\s*(?:['\"]([^'\"]*)['\"]|(os\.getcwd\(\)|Path\.cwd\(\)|Path\(\s*(?:['\"]\.?['\"])?\s*\)))")
_SQL_OUT_PATH = re.compile(r"(?i)(?:\bTO\s+|\\[ow]\s+|:out\s+|\bSPOOL\s+|\bFILE\s*=\s*)'?([^\s'\"]*\.migration(?:/[^\s'\"]*)?)")

# read shapes: leading keyword, then no write keyword anywhere at statement level
_READ_HEAD = ("SELECT", "WITH", "SET", "USE", "DECLARE")
_DESCRIBE_HEAD = ("SHOW", "DESC", "DESCRIBE", "HELP", "GO")
# `EXPLAIN [ANALYZE|VERBOSE|...] [(options)] <statement>`: the statement is judged on its own (ANALYZE executes it)
_EXPLAIN = re.compile(r"EXPLAIN\b(?:\s+(?:ANALYZE|VERBOSE|PLAN|EXTENDED|CODEGEN|COST|FORMATTED|QUERY\s+PLAN)\b|\s*\([^)]*\)|\s+FOR\b)*\s*",
                      re.IGNORECASE)
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
# functions that mutate, disrupt or reach outside the source from inside a SELECT: sequences, backend
# control, large objects, dblink, locks, sleeps, session config, Oracle DBMS_*/UTL_* packages, T-SQL
# linked-server access. A statement calling one is not a read (denylist exception to the read grammar).
_SIDE_EFFECT_FN = re.compile(
    r"\b(?:nextval|setval|set_config|pg_sleep(?:_for|_until)?|pg_terminate_backend|pg_cancel_backend|pg_reload_conf|"
    r"pg_rotate_logfile|pg_(?:try_)?advisory_\w*lock\w*|lo_(?:import|export|unlink|creat|create|put|truncate)|dblink\w*|"
    r"(?:sys\.)?(?:dbms|utl)_\w+(?:\.\w+)*|opendatasource)\s*\(|\.NEXTVAL\b", re.IGNORECASE)

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
    "auth": {"describe", "profiles"}, "fs": {"ls", "cat", "head"}, "api": {"get"}, "bundle": {"validate", "summary"},
}
_TOKEN_PRINTERS = ("auth token", "auth env")   # print the bearer token into the session log
# (catalog lifecycle and permissions -- `catalogs create|update|delete`, `grants update`, `schemas delete` -- are not
# object writes inside an allowlisted catalog and stay blocked whatever the allowlist says)
_CLI_CATALOG_ARG = {"schemas create": 1, "schemas update": 0, "tables delete": 0, "volumes create": 0, "volumes delete": 0,
                    "volumes update": 0, "functions delete": 0, "functions update": 0}
_DBX_VALUE_FLAGS = {"-o", "--output", "--log-level", "--log-file", "--log-format", "--progress-format", "-t", "--target", "-p",
                    "--profile", "--host", "--warehouse-id", "--catalog", "--schema", "--format", "--wait-timeout", "--json",
                    "--var", "--file", "--language", "--string-value", "--bytes-value", "-e", "--statement", "--query"}
_UC_PATH = re.compile(r"unity-catalog/(?:tables|schemas|volumes|functions)/([^/?\s]+)")
_VOLUME_PATH = re.compile(r"^(?:dbfs:)?/Volumes/([^/]+)/")
_DBX_HOST = re.compile(r"\$\{?DATABRICKS_HOST\b|\.(?:cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com)\b", re.IGNORECASE)
_HTTP_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")
_CURL_VALUE_SHORT = "dFTHouAebcmwxEKUyYzCDQrtPO"   # short options that take a value (end a bundled cluster)


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
            bundle_targets=lists["bundle_targets"],   # DAB / dbt targets are case-sensitive: compared exactly
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
    """The allowlist path of the nearest `.migration/` directory up the tree, whether or not the
    file exists there (a workspace with the directory but no readable allowlist fails closed);
    None only when no `.migration/` exists at all."""
    for d in [start, *start.parents]:
        if (d / CONFIG_REL).is_file() or (d / CONFIG_REL.parent).is_dir():
            return d / CONFIG_REL
    return None


def load_config(start: Path) -> GuardConfig | None:
    path = find_config(start.resolve())
    if path is None:
        return None
    data = json.loads(path.read_text())   # OSError when .migration/ exists without the file: the caller blocks
    if not isinstance(data, dict):
        raise ValueError(f"{path} must be a JSON object")
    return GuardConfig.from_dict(data, path)


_SQL_OPAQUE = re.compile(r"--[^\n]*|/\*.*?(?:\*/|\Z)|'(?:[^']|'')*(?:'|\Z)", re.DOTALL)


def _sql_view(text: str) -> str:
    """SQL text as the detectors read it, offsets preserved: comments blanked, and the contents of
    single-quoted literals blanked (`WHERE note = 'DROP TABLE x'` is a read) unless the literal
    feeds a dynamic-SQL executor, where it is the statement."""
    def blank(m: re.Match) -> str:
        s = m.group()
        if s[0] != "'":
            return re.sub(r"[^\n]", " ", s)
        if _DYNAMIC_SQL_CALLER.search(text, max(0, m.start() - 40), m.start()):
            return s
        closed = len(s) > 1 and s.endswith("'")
        return "'" + re.sub(r"[^\n]", " ", s[1:len(s) - closed]) + "'" * closed
    return _SQL_OPAQUE.sub(blank, text)


# ---------------------------------------------------------------- shell model: words, simple commands, what feeds them

_OP = re.compile(r"[<>]\(|<<<|<<-|&>>|<<|<>|<&|>>|>&|>\||&>|\|&|\|\||&&|[;|&()<>\n]")
_REDIRECT_OP = re.compile(r"\d*(?:<{1,3}-?|<>|<&|>{1,2}|>&|>\||&>{1,2})")
_STDIN_OP = re.compile(r"0?<")
_PIPE_OPS = ("|", "|&")
_SUBSTITUTION = re.compile(r"\$\(|(?<![\w])[<>]\(")
_BACKTICK = re.compile(r"`([^`]*)`")


def _shell_tokens(cmd: str) -> tuple[list[str], list[str]]:
    """(words, raw words): quotes removed / kept, with operators, `(`/`)` and line breaks as tokens
    of their own. An unquoted digit run against `<`/`>` is a descriptor and stays with its operator
    (`2>&1`, `0<f`). A heredoc body replaces its delimiter token; its raw form is single-quoted when
    the delimiter was, so `_expands` reads it like the shell would."""
    out, raw = [], []
    pending: list[tuple[int, bool, bool]] = []   # (delimiter token index, strip tabs, quoted delimiter)
    word, rword, quote, plain, started, i, n = "", "", "", True, False, 0, len(cmd)

    def flush() -> None:
        nonlocal word, rword, plain, started
        if started:
            out.append(word)
            raw.append(rword)
        word, rword, plain, started = "", "", True, False

    while i < n:
        ch = cmd[i]
        if quote:
            if quote == '"' and ch == "\\" and i + 1 < n and cmd[i + 1] in '"\\$`\n':
                word, rword, i = word + cmd[i + 1], rword + cmd[i:i + 2], i + 2
                continue
            if ch == quote:
                quote = ""
            else:
                word += ch
            rword += ch
            i += 1
        elif ch == "\\" and i + 1 < n:
            if cmd[i + 1] != "\n":
                word, rword, plain, started = word + cmd[i + 1], rword + cmd[i:i + 2], False, True
            i += 2
        elif ch in "'\"":
            quote, plain, started, rword = ch, False, True, rword + ch
            i += 1
        elif ch in " \t\r":
            flush()
            i += 1
        elif m := _OP.match(cmd, i):
            op = m.group()
            if started and plain and word.isdigit() and op[0] in "<>":
                word, op, started = "", word + op, False
            flush()
            out.append(op)
            raw.append(op)
            i = m.end()
            if op in ("<<", "<<-"):
                pending.append((len(out), op == "<<-", i < n and cmd[i:i + 1] in "'\"\\"))
            elif op == "\n":
                for idx, tabs, quoted in pending:
                    if idx >= len(out):
                        break
                    tag, body = out[idx], []
                    while i < n:
                        nl = cmd.find("\n", i)
                        nl = n if nl < 0 else nl
                        line = cmd[i:nl]
                        i = nl + 1
                        if (line.lstrip("\t") if tabs else line).rstrip() == tag:
                            break
                        body.append(line)
                    out[idx] = "\n".join(body) + "\n"
                    raw[idx] = f"'{out[idx]}'" if quoted else out[idx]
                pending.clear()
        else:
            word, rword, started = word + ch, rword + ch, True
            i += 1
    flush()
    return out, raw


@dataclass
class _Simple:
    """One simple command: its words (redirections and heredoc bodies included), their raw forms,
    and the producers whose stdout reaches its stdin."""
    words: list[str] = field(default_factory=list)
    raw: list[str] = field(default_factory=list)
    feeds: list[_Simple] = field(default_factory=list)


def _commands(cmd: str) -> list[_Simple]:
    """Simple commands, split at `;`, `&&`, `||`, `|`, `&`, group delimiters and line breaks. Fail
    closed on bash's descriptor rules: what a `|` feeds reaches every member of a group on its right,
    and a redirection after `)`/`}` is repeated on every member of the group it closes."""
    toks, raws = _shell_tokens(cmd)
    out: list[_Simple] = []
    groups: list[tuple[int, list[_Simple]]] = []   # (index of the first member, stdin the group inherits)
    feed: list[_Simple] = []                        # what the next command's stdin receives
    closed: list[_Simple] = []                      # members of the group just closed
    cur: _Simple | None = None
    i = 0
    while i < len(toks):
        tok = toks[i]
        if _REDIRECT_OP.fullmatch(tok):
            if cur is None and not closed:
                cur = _Simple(feeds=list(feed))
                out.append(cur)
            for c in closed if cur is None else [cur]:
                c.words.extend(toks[i:i + 2])
                c.raw.extend(raws[i:i + 2])
            i += 2
        elif tok == "\n" and cur is None and feed and not closed:
            i += 1                                      # a line break after `|` continues the pipeline
        elif tok in _SEPARATORS and (tok not in ("{", "}") or cur is None):
            if tok in ("(", "{"):
                groups.append((len(out), feed))
            elif tok in (")", "}"):
                start, feed = groups.pop() if groups else (0, [])
                closed = out[start:]
            elif tok in _PIPE_OPS:
                producers = closed or ([cur] if cur else [])
                feed = producers + [f for p in producers for f in p.feeds]
                closed = []
            else:
                feed = groups[-1][1] if groups else []
                closed = []
            cur = None
            i += 1
        else:
            if cur is None:
                cur = _Simple(feeds=list(feed))
                out.append(cur)
                closed = []
            cur.words.append(tok)
            cur.raw.append(raws[i])
            i += 1
    return [c for c in out if c.words]


def _strip_redirects(words: list[str]) -> tuple[list[str], list[str], str | None]:
    """(arguments, heredoc bodies, here-string) of a simple command."""
    args, heredocs, here, i = [], [], None, 0
    while i < len(words):
        w, operand = words[i], words[i + 1] if i + 1 < len(words) else ""
        if _REDIRECT_OP.fullmatch(w):
            if w.endswith("<<<"):
                here = operand
            elif w.endswith(("<<", "<<-")):
                heredocs.append(operand)
            i += 2
        else:
            args.append(w)
            i += 1
    return args, heredocs, here


def _at_files(texts: list[str]) -> list[str]:
    """File directives on script lines: SQL*Plus `@fix.sql`, BTEQ `.RUN FILE=fix.sql` / `.RUN FILE @fix.sql`."""
    return [(m.group(1) or m.group(2)).lstrip("@") for t in texts for m in _RUN_FILE.finditer(t)]


def _piped_scripts(producer: _Simple) -> list[str]:
    """Script files a text producer hands the client on its stdin: `cat fix.sql | bteq`, `@fix.sql`
    on a line of `cat <<EOF | sqlplus` or in `echo @fix.sql | bteq`."""
    args, heredocs, _ = _strip_redirects(producer.words)
    base = args[0].rsplit("/", 1)[-1] if args else ""
    files = _at_files(heredocs)
    if base == "cat":
        operands = [w for w in args[1:] if w == "-" or not w.startswith("-")]
        files += [w for w in operands if w != "-"]
        if not operands or "-" in operands:   # cat reads its stdin only without an operand, or with `-`
            files += [f for op, f in zip(producer.words, producer.words[1:]) if _STDIN_OP.fullmatch(op)]
    elif base in ("echo", "printf"):
        files += _at_files(args[1:])
    return files


def _scripts_of(c: _Simple, words: list[str] | None = None) -> list[str]:
    """Files the command is told to execute: `< f`, `@f`, `-f f`, `--file f`, `-i f`, `--input f`,
    `@f` on a heredoc line, and what a text producer pipes into it."""
    files = [f for p in c.feeds for f in _piped_scripts(p)]
    words = c.words if words is None else words
    args, heredocs, _ = _strip_redirects(words)
    for i, tok in enumerate(args):
        nxt = args[i + 1] if i + 1 < len(args) else ""
        f = nxt if tok in _SCRIPT_FLAGS else tok[1:] if tok.startswith("@") else \
            tok.split("=", 1)[1] if tok.startswith(tuple(fl + "=" for fl in _SCRIPT_FLAGS)) else ""
        if f and not f.startswith("-"):
            files.append(f)
    files += [f for op, f in itertools.pairwise(words) if _STDIN_OP.fullmatch(op)]
    return files + _at_files(heredocs)


def _script_inputs(cmd: str, cfg: GuardConfig | None = None) -> list[str]:
    """Script files of every simple command; given a config, only of those naming a client or a
    legacy source (the `-f` of `rm -f x && databricks jobs list` belongs to `rm`)."""
    return [f for c in _commands(cmd) if cfg is None or _has_context(" ".join(_strip_redirects(c.words)[0]), cfg)
            for f in _scripts_of(c)]


def _live(text: str) -> str:
    """The text with single-quoted spans and backslash-escaped characters blanked: what remains of
    `$` and backticks is what the shell would expand."""
    return re.sub(r"\\.|'[^']*'?", lambda m: " " * len(m.group()), text, flags=re.DOTALL)


def _command_backticks(live: str) -> bool:
    """A backtick span that is not a plain SQL identifier (`mig_cat`) runs a command."""
    return any(not re.fullmatch(r"[\w$-]+", m.group(1)) for m in _BACKTICK.finditer(live))


def _expands(text: str) -> bool:
    live = _live(text)
    return "$" in live or _command_backticks(live)


def _substitutes(text: str) -> bool:
    live = _live(text)
    return bool(_SUBSTITUTION.search(live)) or _command_backticks(live)


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
    """Whether the text names a Databricks, legacy or generic SQL client or a legacy source."""
    return bool(_CLIENT_WORD.search(text) or "--target-catalog" in text or _mentions_legacy(text, cfg))


# ---------------------------------------------------------------- segments: the program each simple command runs

@dataclass
class _Seg:
    argv: list[str]                 # program and arguments after prefixes, wrappers, aliases, redirections
    words: list[str]                # the simple command as tokenised (redirections included)
    raw: list[str]                  # the same words with their quoting kept
    heredocs: list[str]             # heredoc bodies it opened
    assigns: list[str]              # VAR=value prefixes
    herestring: str | None
    stdin: list[str]                # literal text piped in (echo/printf words, cat heredocs)
    opaque: str | None              # a piped producer whose output the guard cannot read
    scripts: list[str]              # files it executes
    ctx: str = ""                   # text of the wrapper (`ssh host`) this command was nested in
    fed: list[str] = field(default_factory=list)   # words of the producers piped into it

    @property
    def argv0(self) -> str:
        return self.argv[0].rsplit("/", 1)[-1] if self.argv else ""

    @property
    def text(self) -> str:
        return " ".join([*self.words, *self.stdin, self.herestring or "", self.ctx])

    def raw_of(self, word: str) -> str:
        return self.raw[self.words.index(word)] if word in self.words else word


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
                if w == "env" and words[i] in ("-u", "--unset") and i + 1 < len(words):
                    assigns.append(words[i + 1] + "=")   # unsetting a variable changes the environment too
                i += 2 if words[i] in _PREFIX_VALUE_FLAGS.get(w, ()) else 1
        else:
            break
    return words[i:]


def _shell_c_arg(argv: list[str]) -> str | None:
    """The string a `sh -c` form runs, whatever options precede `-c`."""
    for i, w in enumerate(argv[1:-1], 1):
        if re.fullmatch(r"-[A-Za-z]*c[A-Za-z]*", w):
            return argv[i + 1] if argv[i + 1] != "--" or i + 2 >= len(argv) else argv[i + 2]
    return None


def _nested_shell(seg: _Seg) -> str | None:
    """Shell text a segment hands to another shell: `sh -c '...'`, `eval '...'`, a literal piped
    into `bash`. Runtime-built variants are `_check_opaque`'s business."""
    if seg.argv0 in _SHELLS:
        arg = _shell_c_arg(seg.argv)
        if arg is not None:
            return None if _expands(seg.raw_of(arg)) else arg
        if len(seg.argv) == 1 and seg.stdin and not seg.opaque:
            return "\n".join(seg.stdin)
    elif seg.argv0 == "eval" and len(seg.argv) > 1:
        return None if any(_expands(seg.raw_of(w)) for w in seg.argv[1:]) else " ".join(seg.argv[1:])
    return None


def _shell_script(seg: _Seg) -> str | None:
    """The script file a shell segment runs: `bash x.sh`, `sh -x x.sh`, `bash < x.sh`, `source x`,
    `. x`. `-c` forms carry their text in the command and are not files."""
    if seg.argv0 in ("source", ".") and len(seg.argv) > 1:
        return seg.argv[1]
    if seg.argv0 in _SHELLS and _shell_c_arg(seg.argv) is None:
        positional = [w for w in seg.argv[1:] if not w.startswith("-")]
        return positional[0] if positional else seg.scripts[0] if seg.scripts else None
    return None


def _segments(text: str, ctx: str = "", depth: int = 0) -> list[_Seg]:
    aliases: dict[str, list[str]] = {}
    out: list[_Seg] = []
    for c in _commands(text):
        words, heredocs, herestring = _strip_redirects(c.words)
        if words and words[0] in aliases:
            words = aliases[words[0]] + words[1:]
        assigns: list[str] = []
        argv = _program(words, assigns)
        argv, wrapper = _unwrap(argv)
        stdin, opaque = [], None
        for p in c.feeds:
            pargs, pheredocs, _ = _strip_redirects(p.words)
            base = pargs[0].rsplit("/", 1)[-1] if pargs else ""
            if base not in ("cat", "echo", "printf", "tee") or _expands(" ".join(p.raw)):
                opaque = base or "?"
            elif base in ("echo", "printf"):
                stdin.append(" ".join(w for w in pargs[1:] if not re.fullmatch(r"-[neE]+", w)))
            elif base == "cat":
                stdin.extend(pheredocs)
        redirects = [w for i, w in enumerate(c.words) if _REDIRECT_OP.fullmatch(w) or (i and _REDIRECT_OP.fullmatch(c.words[i - 1]))]
        seg = _Seg(argv, c.words, c.raw, heredocs, assigns, herestring, stdin, opaque,
                   _scripts_of(c, argv + redirects if wrapper else None), " ".join(x for x in (ctx, wrapper) if x),
                   [w for p in c.feeds for w in p.words])
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


def _sql_bearing(seg: _Seg, i: int) -> bool:
    """Whether word i is where a client reads its SQL from: the value of a SQL flag, the positional
    after `tools query`, or a composite string rather than a bare value."""
    w, prev = seg.words, seg.words[i - 1] if i > 0 else ""
    return (prev in _SQL_VALUE_FLAGS or w[i].split("=", 1)[0] in _SQL_VALUE_FLAGS
            or (i >= 2 and w[i - 2] == "tools" and prev == "query") or bool(re.search(r"[\s;]", w[i])))


def _check_opaque(segs: list[_Seg], cmd: str, cfg: GuardConfig) -> list[str]:
    """Constructs that only produce the statement at run time. Always: `eval`/`sh -c` on a
    `$`-built string, an opaque producer piped into a shell, a shell fed by process substitution.
    Where a client is involved: any substitution, an expansion inside the SQL argument, an unquoted
    heredoc that expands, `xargs`."""
    violations: list[str] = []
    ctx = _has_context(cmd, cfg)
    for s in segs:
        base = s.argv0
        if base == "eval" and len(s.argv) > 1 and _nested_shell(s) is None:
            violations.append("`eval` of a runtime-built string; the guard cannot read what it would run")
        elif base in _SHELLS:
            if s.opaque:
                violations.append(f"text piped into `{base}` comes from a decoder, download, program or expansion the "
                                  "guard cannot read")
            arg = _shell_c_arg(s.argv)
            if arg is not None and _expands(s.raw_of(arg)):
                violations.append(f"`{base} -c` on a runtime-built string; the guard cannot read what it would run")
            if "<(" in s.words:
                violations.append(f"`{base}` fed by process substitution; the guard cannot read what it would run")
        elif "xargs" in s.words and ctx:
            violations.append("`xargs` builds a client invocation from stdin; the guard cannot read the statement it would run")
        if ctx and s.argv and _expands(s.raw_of(s.argv[0]).rsplit("/", 1)[-1]):
            violations.append(f"variable `{s.raw_of(s.argv[0])[:40]}` in command position; the guard cannot tell which program "
                              "it would run (same class as `eval`)")
        if ctx:
            for i, r in enumerate(s.raw):
                if _expands(r) and _sql_bearing(s, i) and not _REDIRECT_OP.fullmatch(s.words[i - 1] if i else ""):
                    violations.append(f"shell expansion inside the SQL argument `{r[:60]}`; expand it in the command text so "
                                      "the guard can read the statement")
                    break
            for i, w in enumerate(s.words[:-1]):
                if w.endswith(("<<", "<<-")) and _REDIRECT_OP.fullmatch(w) and _expands(s.raw[i + 1]):
                    violations.append("unquoted heredoc expands `$`/backticks in its body; quote the delimiter (<<'EOF') or "
                                      "inline the values")
                    break
    if ctx and _substitutes(cmd):
        violations.append("command/process substitution in a Databricks or legacy command; the statement is built at run "
                          "time, so inline it as text")
    if ctx and _FUNCTION_DEF.search(re.sub(r'"(?:[^"\\]|\\.)*"|\'[^\']*\'', " ", cmd)):
        violations.append("shell function defined in a Databricks or legacy command; the guard cannot follow what a call of "
                          "it would run (same class as `eval`)")
    return violations


# ---------------------------------------------------------------- policy

_FLAG_WORD = re.compile(r"-{1,2}[\w.-]+(?:=\S*)?")


def _flag_values(argv: list[str], flags: tuple[str, ...]) -> list[str]:
    """Values of `flags` in argv (`-e SQL`, `--file=x`). The next word is the value unless it is itself a
    flag word; `-- a comment` or `--\\nDROP ...` is SQL, not a flag."""
    out = []
    for i, w in enumerate(argv[1:], 1):
        if w in flags and i + 1 < len(argv) and not _FLAG_WORD.fullmatch(argv[i + 1]):
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
    if head == "EXPLAIN":
        rest = s[_EXPLAIN.match(s).end():]
        return not rest or _is_read(rest)
    if head in _DESCRIBE_HEAD or head in _SQLPLUS_DIRECTIVE:
        return True
    if head not in _READ_HEAD or (head == "SET" and _SET_DENY.match(s)):
        return False
    return not (_NON_READ_WORD.search(s) or _SIDE_EFFECT_FN.search(s))


def _non_read(seg: _Seg, sql: str) -> list[str]:
    bad = [s for s in _statements(sql) if not _is_read(s)]
    if seg.argv0 == "bcp" and "in" in seg.argv[1:4]:
        bad.insert(0, "bcp ... in (loader)")
    if seg.argv0 in _WRITERS:
        bad.insert(0, f"{seg.argv0} (a migration tool: every run writes its target)")
    if seg.opaque:
        bad.insert(0, f"stdin from `{seg.opaque}`, a program or expansion the guard cannot read")
    return [b.split("\n", 1)[0][:80] for b in bad]


def _hosts(seg: _Seg) -> list[str]:
    """Host / DSN candidates of a generic client: host flags and environment, URI and keyword hosts,
    `$VAR` names, and the first positional only for clients whose positional is a DSN (a database
    name or a SQL argument is never a host)."""
    sql = set(_flag_values(seg.argv, _SQL_VALUE_FLAGS)) | set(seg.scripts)
    out = list(_flag_values(seg.argv, _HOST_FLAGS))
    out += [a.split("=", 1)[1] for a in seg.assigns if a.split("=", 1)[0] in _HOST_ENV]
    if seg.argv0 in _DSN_POSITIONAL:
        out += [w for w in seg.argv[1:2] if not w.startswith("-") and w not in sql]
    joined = " ".join(w for w in seg.argv[1:] if w not in sql)
    out += re.findall(r"\$\{?(\w+)\}?", joined)
    out += re.findall(r"://(?:[^@/\s]*@)?([^:/?\s;]+)", joined)
    out += re.findall(r"(?i)\b(?:host|hostaddr|server|data source|addr)=([^;\s]+)", joined)
    return [re.split(r"[,:\\]", re.sub(r"^(?:tcp|np|lpc):", "", h, flags=re.IGNORECASE), 1)[0].lower() for h in out if h]


def _check_sql_client(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    base = seg.argv0
    legacy_only = base in _LEGACY_ONLY
    hits = _mentions_legacy(seg.text, cfg)
    tail = " (legacy is read-only in every phase)"
    if base in _LOADERS:
        return [f"`{base}` is a loader: nothing but reads ever runs against a legacy source" + tail]
    extra = [seg.argv[1]] if base == "bcp" and len(seg.argv) > 2 and "queryout" in seg.argv[2:4] else []
    sql, unreadable = _sql_text(seg, root, extra)
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


def _target_catalog(text: str, pos: int) -> str | None:
    """Catalog of the securable a write names right after its verb phrase: `CATALOG c`, `SCHEMA
    c.s`, or a three-part name. An unqualified target resolves to nothing (the caller falls back
    to `USE CATALOG`), never to a qualified source further along (CTAS, MERGE USING, INSERT SELECT)."""
    m = _TARGET.match(text, pos)
    if not m:
        return None
    kind, parts = (m.group(1) or "").upper(), 1 + m.group(3).count(".")
    if kind == "CATALOG" or (kind and parts >= 2) or parts >= 3:
        return _norm(m.group(2))
    return None


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
    for m in _WRITE.finditer(text):
        end = text.find(";", m.end())
        stmt = text[m.start(): end if end != -1 else len(text)]
        use_cat = next((c for pos, c in reversed(use_cats) if pos < m.start()), default)
        cat = _target_catalog(text, m.end())
        head = stmt.strip().split("\n", 1)[0][:80]
        if _IDENTIFIER_DYNAMIC.search(stmt):
            violations.append(f"IDENTIFIER(<non-literal>) names the target of a write at run time: `{head}`")
        elif in_dbx and _PERMISSION.match(stmt):
            violations.append(f"catalog lifecycle / permission change `{head}`; the allowlist authorizes object writes inside a "
                              "catalog, never grants or the catalog itself (those happen at STOP E)")
        elif cat is not None:
            if cat not in allowed:
                violations.append(f"write to catalog(s) {[cat]} outside allowlist {sorted(allowed)}: `{head}`")
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
    if t not in cfg.bundle_targets:
        return [f"`{kind}` target {t!r} not in bundle_targets {cfg.bundle_targets} (exact, case-sensitive; empty = every deploy blocks)"]
    return []


def _check_databricks(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    argv = seg.argv[1:]
    if any(w in ("--version", "-v", "-h", "--help", "version", "help") for w in argv):
        return []
    path, i = [], 0
    while i < len(argv):
        if argv[i] == "-" or _FLAG_WORD.fullmatch(argv[i]):
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
        remote = [a for a in args if a.startswith(("dbfs:", "/")) or "://" in a]
        cats = [_norm(m.group(1)) if (m := _VOLUME_PATH.match(a)) else None for a in remote]
        if remote and len(args) >= (2 if verb == "cp" else 1) and all(c in cfg.catalogs for c in cats) and not any(map(_expands, args)):
            return []
        return [(f"`databricks fs {verb}` with a remote path outside an allowlisted volume (dbfs:/Volumes/<catalog>/...; every "
                 f"remote end of a `cp`, no variables): {args}")]
    key = f"{group} {verb}"
    if key in _TOKEN_PRINTERS:
        return [f"`databricks {key}` prints the session's bearer token (credential exposure); `auth describe` shows the identity without it"]
    if key in ("catalogs create", "catalogs update", "catalogs delete", "schemas delete", "grants update", "grants delete"):
        return [(f"`databricks {key}` on {' '.join(args) or '<securable>'!r}: the allowlist authorizes object writes inside a "
                 "catalog, never catalog lifecycle or permissions (those happen at STOP E)")]
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
            elif w.startswith(("--data", "--json", "--form", "--upload-file")):
                body = True
            elif w.startswith("-") and not w.startswith("--"):   # bundled short options: `-sSd'{}'`, `-F`, `-T`
                first = next((ch for ch in w[1:] if ch in _CURL_VALUE_SHORT), "")
                body = body or (first != "" and first in "dFT")
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
    """The session runs as the doctor-verified migration principal only: no credential / endpoint /
    profile variable is set, exported or unset (the shell persists it for the next command), no
    HOME/XDG_CONFIG_HOME or profile flag is put on a client's own segment."""
    violations = []
    tail = "; the session runs as the doctor-verified migration principal only"
    for s in segs:
        client = s.argv0 in _IDENTITY_CLIENTS
        persistent = s.argv0 in ("export", "unset", "declare", "typeset", "setenv") or not s.argv   # outlives the command
        names = [a.split("=", 1)[0] for a in s.assigns]
        if persistent:
            names += [w.split("=", 1)[0] for w in s.argv[1:] if not w.startswith("-")]
        for n in names:
            if _IDENTITY_VAR.match(n) and (client or persistent):
                violations.append(f"identity swap: `{n}=` {'around `' + s.argv0 + '`' if client else 'changed for the session'}" + tail)
            elif n in _CONFIG_HOME_VAR and client:
                violations.append(f"identity swap: `{n}=` moves the Databricks config lookup around `{s.argv0}`" + tail)
        if client and s.argv0 != "spark-sql":
            flags = [w for w in s.argv[1:] if w in ("--profile", "-p", "--host") or w.startswith(("--profile=", "--host="))]
            if flags or s.argv[1:3] == ["auth", "login"] or s.argv[1:2] == ["configure"]:
                violations.append(f"identity swap through `{s.argv0} {' '.join(flags or s.argv[1:3])}`" + tail)
    return violations


# heads that only read their operands; anything else naming `.migration` is a write (fail closed)
_READ_HEADS = frozenset(("cat", "less", "more", "head", "tail", "grep", "rg", "egrep", "fgrep", "zgrep", "diff", "cmp", "ls", "stat",
                         "wc", "file", "jq", "yq", "md5sum", "sha1sum", "sha256sum", "sort", "uniq", "cut", "tr", "awk", "gawk",
                         "mawk", "sed", "tree", "du", "echo", "printf", "test", "[", "[[", "cd", "pushd", "popd", "dirname",
                         "basename", "realpath", "readlink", "which", "type", "find", "tar", "unzip", "column", "nl", "od", "xxd",
                         "strings", "pytest", "ruff", "true", "false", "sleep", "date", "env", "printenv", "set", "export", "unset",
                         "dbx-recon", "databricks"))   # the two clients read the allowlist; their outputs go through --out / -o
_WRITE_LAST_OPERAND = ("cp", "rsync", "install", "ln", "scp")
_RECURSIVE_HEADS = ("rm", "chmod", "chown", "chgrp", "rsync", "chattr", "setfacl")
# the Databricks CLI's identity store: `~/.databrickscfg` (any directory), `~/.databricks/` (token cache), `~/.config/databricks/`
_IDENTITY_FILE = re.compile(r"(?:^|/)(?:\.databrickscfg|\.databricks(?:/.*)?|\.config/databricks(?:/.*)?)$")
_GUARD_TREE = Path(os.path.realpath(__file__)).parent.parent   # the running plugin: hooks.json + hooks/**
_PATH_LITERAL = re.compile(r"['\"]((?:[~./$]|/)[^'\"\n]{0,300})['\"]")
_OUTPUT_FLAGS = ("-o", "-O", "--output", "--out", "--out-file", "--output-file", "--outfile", "--file")
_GIT_DESTRUCTIVE = ("--hard", "--merge", "--keep")


def _touch(path: str, cwd: str, root: Path) -> str:
    """How a literal path relates to the protected part of `.migration/`: 'inside' (a protected entry,
    literally or through a glob / brace / `?` that could match one), 'self' (`.migration` itself),
    'above' (`.`, `..`, `$PWD`, `~` or an absolute path at or above the workspace root); failing that,
    'identity' for the Databricks CLI's credential store, 'guard' / 'guard-above' for the running
    guard's own tree (`_guard_touch`), or ''."""
    p = re.sub(r"\$\{?PWD\}?|\$\(pwd\)", ".", path)
    p = os.path.expanduser(re.sub(r"\{[^{}]*(?:,|\.\.)[^{}]*\}", "*", p))   # a brace list could name anything in it
    if p in ("", "-") or p.isdigit():
        return ""
    p = os.path.normpath(p if p.startswith("/") else os.path.join(cwd, p))
    return _touch_migration(p, root) or _touch_identity(p) or _guard_touch(p, root)


def _touch_identity(p: str) -> str:
    home = re.sub(r"\$\{?HOME\}?", "~", p)
    name = p.rsplit("/", 1)[-1]
    glob = name.startswith(".") and re.search(r"[*?\[]", name) and fnmatch.fnmatchcase(".databrickscfg", name)
    return "identity" if _IDENTITY_FILE.search(home) or glob else ""


def _touch_migration(p: str, root: Path) -> str:
    parts = [x for x in p.split("/") if x not in ("", ".")]
    for i, part in enumerate(parts):
        # a glob counts only at the top of the workspace, where the ledger lives; a literal `.migration` anywhere
        if part == ".migration" or (i == 0 and not p.startswith("/") and re.search(r"[*?\[]", part)
                                     and fnmatch.fnmatchcase(".migration", part)):
            rest = parts[i + 1:]
            if not rest:
                return "self"
            return "" if len(rest) > 1 and rest[0] in ("recon", "waves") else "inside"
    if p.startswith("/"):
        try:
            p = os.path.relpath(p, root)
        except ValueError:
            return ""
        parts = [x for x in p.split("/") if x not in ("", ".")]
    return "above" if all(x == ".." for x in parts) else ""


def _guard_touch(p: str, root: Path) -> str:
    """How a normalised path relates to the running guard's own tree (symlinks resolved, a glob
    component matched against the real names): 'guard' for `hooks.json` / `hooks/**`, 'guard-above'
    for the plugin directory or an ancestor of it, '' otherwise."""
    parts = os.path.realpath(p if p.startswith("/") else os.path.join(root, p)).strip("/").split("/")
    tree = [*_GUARD_TREE.parts[1:], "hooks"]
    for i, part in enumerate(parts[:len(tree)]):
        if not (part == tree[i] or (re.search(r"[*?\[]", part) and fnmatch.fnmatchcase(tree[i], part))
                or (i == len(tree) - 1 and (part == "hooks.json" or fnmatch.fnmatchcase("hooks.json", part)))):
            return ""
    return "guard" if len(parts) >= len(tree) else "guard-above"


def _in_place(base: str, argv: list[str]) -> bool:
    if base == "sed":
        return any(re.match(r"-[nEersuz]*i", w) or w.startswith("--in-place") for w in argv[1:])
    if base == "perl":
        return any(re.match(r"-[a-zA-Z]*i", w) for w in argv[1:])
    if base in ("awk", "gawk", "mawk"):
        return "--inplace" in argv or ("-i" in argv and "inplace" in argv)
    return False


def _patch_texts(s: _Seg, files: list[str], root: Path, how: str) -> list[str]:
    """Violations of a patch applier: the guard reads every patch it is given and blocks one it
    cannot read or one that touches `.migration/`."""
    if s.opaque or (not files and not s.stdin and not s.heredocs):
        return [f"`{how}` on a patch the guard cannot read (stdin from a program or the terminal); write it to a file first"]
    out = []
    for f in files:
        body = _read_script(f, root)
        if body is None:
            out.append(_unreadable_msg([f], f"`{how}`"))
        elif _MIGRATION_PATH.search(body):
            out.append(f"`{how}` of a patch that touches .migration/ ({f}); ledgers and the allowlist change only through a recorded decision")
    if _MIGRATION_PATH.search("\n".join([*s.stdin, *s.heredocs])):
        out.append(f"`{how}` of a patch that touches .migration/; ledgers and the allowlist change only through a recorded decision")
    return out


def _check_integrity(segs: list[_Seg], root: Path) -> list[str]:
    """Nothing but the recon harness and the workflow writes under `.migration/`: any head whose
    literal operands name a protected entry blocks unless it only reads; a destructive recursive
    verb on `.`, `..`, the workspace root or `.migration` itself blocks too."""
    violations, cwd = [], ""
    all_kinds = ("inside", "self", "above")

    def hit(path: str, how: str, kinds: tuple[str, ...] = ("inside", "self"), destructive: bool = False) -> None:
        kind = _touch(path, cwd, root)
        if kind in kinds:
            violations.append(f"`{how}` writes `{path}` under .migration/ (only .migration/recon/ and .migration/waves/ are "
                              "written by commands; ledgers and the allowlist change only through a recorded decision)")
        elif kind == "identity":
            violations.append(f"`{how}` writes `{path}`, the Databricks CLI's credential store; the session runs as the "
                              "doctor-verified migration principal only")
        elif kind == "guard" or (kind == "guard-above" and (destructive or "above" in kinds)):
            violations.append(f"`{how}` on `{path}` inside the running guard's plugin tree ({_GUARD_TREE}); the hook is never edited, "
                              "disabled or removed from a session (a block is a finding to report)")

    for s in segs:
        base, argv = s.argv0, s.argv
        operands = [w for w in argv[1:] if not w.startswith("-")]
        values = operands + [w.split("=", 1)[1] for w in argv[1:] if "=" in w]          # `dd of=`, `--output=`
        for op, operand in zip(s.words, [*s.words[1:], ""]):
            if _REDIRECT_OP.fullmatch(op) and ">" in op:
                hit(operand, f"{op} {operand}")
        for flag, value in itertools.pairwise(argv):
            if flag in _OUTPUT_FLAGS:
                hit(value, f"{base} {flag}")
        if base in ("cd", "pushd"):
            if operands and not _expands(operands[0]):
                cwd = os.path.normpath(operands[0] if operands[0].startswith("/") else os.path.join(cwd, operands[0]))
        elif base == "git":
            verb, gops = (argv[1] if len(argv) > 1 else ""), operands[1:]
            if verb == "clean" or (verb == "reset" and any(w in argv for w in _GIT_DESTRUCTIVE)):
                violations.append(f"`git {verb}` discards working-copy changes across the workspace, .migration/ included; "
                                  "revert a ledger only through a recorded decision")
            elif verb in ("checkout", "restore", "rm", "mv"):
                for o in gops:
                    hit(o, f"git {verb}", all_kinds if verb in ("checkout", "restore") else ("inside", "self"))
            elif verb in ("apply", "am"):
                violations += _patch_texts(s, gops + s.scripts, root, f"git {verb}")
        elif base == "patch":
            violations += _patch_texts(s, s.scripts, root, "patch")
        elif _PYTHON.fullmatch(base) or base in ("perl", "ruby", "node") and not _in_place(base, argv):
            text = "\n".join([*_flag_values(argv, ("-c", "-e")), *s.heredocs, *s.stdin])
            if _PY_WRITE.search(text):
                for p in [*_MIGRATION_PATH.findall(text), *_PATH_LITERAL.findall(text)]:
                    hit(p, f"{base} script")
                for m in _RMTREE.finditer(text):
                    hit(m.group(1) if m.group(1) is not None else ".", f"{base} rmtree", all_kinds)
        elif base == "find":
            if any(w in argv for w in ("-delete", "-exec", "-execdir", "-ok", "-okdir", "-fls")) or any(w.startswith("-fprint") for w in argv):
                for o in operands:
                    hit(o, "find with an action", all_kinds)
        elif base in ("tar", "bsdtar"):
            if any(re.match(r"-?[a-zA-Z]*x", w) for w in argv[1:2]) or "--extract" in argv or "--get" in argv:
                for d in _flag_values(argv, ("-C", "--directory")) or ["."]:
                    hit(d, f"{base} extract into", all_kinds)
        elif base == "unzip":
            if not any(w in argv for w in ("-l", "-t", "-p", "-z", "-Z")):
                for d in _flag_values(argv, ("-d",)) or ["."]:
                    hit(d, "unzip into", all_kinds)
        elif base in _GENERIC or base in _LEGACY_ONLY:
            sql = " ".join([*_flag_values(argv, _SQL_VALUE_FLAGS), *s.heredocs, *s.stdin, s.herestring or ""])
            for p in [*values, *_SQL_OUT_PATH.findall(sql)]:
                hit(p, f"{base} output")
        elif base in _READ_HEADS and not _in_place(base, argv):
            continue
        else:
            recursive = base in _RECURSIVE_HEADS and any(re.fullmatch(r"-[a-zA-Z]*[rR][a-zA-Z]*", w) or w in ("--recursive", "--delete")
                                                        for w in argv[1:])
            if "xargs" in s.words and any(_touch(w, cwd, root) for w in s.fed):
                violations.append(f"`xargs {base}` on names listed from .migration/; ledgers and the allowlist change only through a "
                                  "recorded decision")
            for o in values[-1:] if base in _WRITE_LAST_OPERAND else values:
                hit(o, base, all_kinds if recursive else ("inside", "self"), destructive=base in ("mv", *_RECURSIVE_HEADS))
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
    if base == "dbt" and seg.argv[1:2] and seg.argv[1] in ("run", "build", "seed", "snapshot", "run-operation"):
        return _check_target(f"dbt {seg.argv[1]}", seg.argv[2:], cfg)
    if base in _REST_CLIENTS:
        return _check_rest(seg)
    if base in _LEGACY_ONLY or base in _GENERIC:
        return _check_sql_client(seg, cfg, root)
    if _PYTHON.fullmatch(base):
        return _check_python(seg, cfg, root)
    return []


def _analyse(text: str, cfg: GuardConfig, root: Path, depth: int = 0) -> tuple[list[_Seg], list[str]]:
    """Segments of the text and of every shell script it runs, plus the violations of the text
    itself (opaque execution, unreadable scripts)."""
    text = re.sub(r"\\\r?\n", " ", text)
    segs = _segments(text)
    violations = _check_opaque(segs, text, cfg)
    for seg in list(segs):
        f = _shell_script(seg)
        if f is None:
            continue
        body = _read_script(f, root)
        if body is None:
            violations.append(f"shell script(s) {[f]} the command would run cannot be read in full (missing, unreadable or over "
                              f"{_MAX_SCRIPT_BYTES // (1024 * 1024)} MiB); the guard cannot clear what it cannot read")
        elif depth < 4:
            more, nested = _analyse(body, cfg, root, depth + 1)
            segs += more
            violations += nested
    return segs, violations


def evaluate(command: str, cfg: GuardConfig, root: Path | None = None) -> Verdict:
    root = root or _project_root()
    violations: list[str] = []
    if m := _PROBE.search(command):
        violations.append(f"`{m.group()}` is the factory-doctor's hook probe; it always blocks so the doctor can tell the "
                          "hook is loaded without touching Databricks")
    segs, found = _analyse(command, cfg, root)
    violations += found + _check_identity(segs) + _check_integrity(segs, root)
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
    out: list[str | None] = []
    for s in _segments(cmd):
        if s.argv0 not in ("cd", "pushd"):
            continue
        args = [w for w in s.argv[1:] if not (w.startswith("-") and len(w) > 1)]
        target = os.path.expandvars(args[0]) if args else "~"
        out.append(None if target == "-" or _expands(target) else os.path.expanduser(target))
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
        cwd = (cwd / target).resolve()
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
