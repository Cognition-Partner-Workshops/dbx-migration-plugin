#!/usr/bin/env python3
"""PreToolUse guard for the DBX migration factory: recognise the client, allow known-read shapes, block the rest.

Reads a PreToolUse event ({"tool_input": {"command"}, "cwd"}), loads the nearest `.migration/allowed_targets.json` (`catalogs`
required; `legacy_sources`, `guard_mode` block|warn, `target_hosts`, `bundle_targets`, `forbidden_bundle_targets`; hosts and
bundle targets fail closed when empty) and judges every simple command by its program: Databricks clients pass `_DBX_READ`
shapes and mutate only allowlisted securables; REST to a workspace host is GET without a body; deploys need a listed target;
identity is never changed. Legacy-only clients and generic clients naming a legacy source run read shapes only; a generic
client elsewhere writes when every host candidate is in `target_hosts` and the write resolves to a listed catalog / database.
Nothing writes `.migration/` or the running guard's tree (git there is the `_git_reads` allowlist). Scripts and SQL files are
read in the command's effective directory (event cwd, cd / pushd / env -C / git -C); what the guard cannot read blocks where a
client is involved. No `.migration/` up the tree approves everything, one without a readable allowlist blocks everything, the
doctor's `__dbx_guard_probe__<nonce>` always blocks. `hooks/tests/test_probe_table.py` pins this policy; add a row first.
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
PROBE_SENTINEL = "__dbx_guard_probe__"
_PROBE = re.compile(re.escape(PROBE_SENTINEL) + r"\w*")

_SEG = r"(?:`[^`]+`|\"[^\"]+\"|\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_$-]*)"   # one identifier part, quoted or bare
_OBJ = r"TABLE|VIEW|FUNCTION|PROCEDURE|VOLUME|INDEX|TRIGGER|SEQUENCE"
_METASTORE = (r"METASTORE|ANY\s+FILE|SHARE|RECIPIENT|PROVIDER|CONNECTION|EXTERNAL\s+LOCATION|STORAGE\s+CREDENTIAL|SERVICE\s+CREDENTIAL"
              r"|CLEAN\s+ROOM")   # catalog-less securables
# a write statement's verb phrase, ending where its target securable starts
_WRITE = re.compile(
    rf"""(?:\b(?:
        INSERT(?:\s+(?:INTO|OVERWRITE))?(?:\s+TABLE)?(?=\s+[\w`"\[])
      | UPDATE\s+(?!STATISTICS\b|SET\b|OF\b)(?:TOP\s*\([^)]*\))?
      | DELETE\s+FROM | COPY\s+INTO | MERGE\s+(?:WITH\s+SCHEMA\s+EVOLUTION\s+)?INTO | MSCK\s+REPAIR\s+TABLE
      | (?:TRUNCATE|REPLACE|RESTORE|REFRESH|REORG|ANALYZE)\s+TABLE | (?:OPTIMIZE|VACUUM)(?:\s+TABLE)? | REFRESH\s+MATERIALIZED\s+VIEW
      | SYNC\s+(?:AS\s+EXTERNAL\s+)?(?=(?:SCHEMA|TABLE)\b)(?:TABLE\s+)?
      | CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+|EXTERNAL\s+|STREAMING\s+|MATERIALIZED\s+|LIVE\s+)*
        (?=(?:{_OBJ}|SCHEMA|DATABASE|CATALOG|{_METASTORE})\b)(?:(?:{_OBJ})\s+(?:IF\s+NOT\s+EXISTS\s+)?)?
      | (?:DROP|ALTER)\s+(?=(?:{_OBJ}|SCHEMA|DATABASE|CATALOG|{_METASTORE})\b)(?:(?:{_OBJ})\s+(?:IF\s+EXISTS\s+)?)?
      | UNDROP\s+(?=(?:TABLE|SCHEMA)\b)(?:TABLE\s+)?
      | COMMENT\s+ON\s+(?:(?:{_OBJ}|MATERIALIZED\s+VIEW|COLUMN)\s+)?
      | (?:GRANT|REVOKE|DENY)\s+.+?\bON\s+(?:(?:{_OBJ}|MATERIALIZED\s+VIEW)\s+)?
      | (?:EXEC(?:UTE)?|CALL)\s+(?!IMMEDIATE\b)(?=(?:\[?[\w$]+\]?\.)+\[?[\w$]+)
    )
      | (?:^|(?<=[;\n]))\s*(?:EXEC(?:UTE)?|CALL)\s+(?!IMMEDIATE\b)(?=[\[@`\w])
    )\s*""", re.IGNORECASE | re.VERBOSE)
_TARGET = re.compile(rf"(?:(CATALOG|SCHEMA|DATABASE)\s+)?(?:IF\s+(?:NOT\s+)?EXISTS\s+)?({_SEG})((?:\.{_SEG})*)(?![\w`.])", re.IGNORECASE)
_USE_CATALOG = r"\bUSE\s+(?:(?:{c})\s+){opt}(?!SCHEMA\b)(" + _SEG + ")"   # {c}: container words; {opt}: `?` where `USE x` switches too
_IDENTIFIER_LITERAL = re.compile(r"\bIDENTIFIER\s*\(\s*'([^']*)'\s*\)", re.IGNORECASE)
_IDENTIFIER_DYNAMIC = re.compile(r"\bIDENTIFIER\s*\(", re.IGNORECASE)
_EXEC_IMMEDIATE_DYNAMIC = re.compile(r"\bEXEC(?:UTE)?\s+IMMEDIATE\s+(?!')\S", re.IGNORECASE)
_BUNDLE_TARGET = re.compile(r"(?:^|\s)(?:-t|--target)(?:=|\s+)(\S+)")
_PERMISSION = r"^\s*(?:GRANT|REVOKE|DENY)\b.*\bON\s+(?:{c})\b|^\s*(?:CREATE|ALTER|DROP)\s+(?:{c})\b"

_LEGACY_ONLY = ("bteq", "sqlplus", "sqlldr", "snowsql", "mload", "fastload", "fastexport", "tbuild", "tdload")
_LOADERS = ("sqlldr", "mload", "fastload", "tbuild", "tdload")
_WRITERS = ("pg_restore", "pgloader", "liquibase", "flyway", "sqitch")   # migration tools: every run writes
_GENERIC = ("psql", "pgcli", "sqlcmd", "osql", "isql", "tsql", "mysql", "mariadb", "sqlite3", "bcp", "beeline", "trino", "presto",
            "mssql-cli", "go-sqlcmd", "usql", *_WRITERS)
_DBX_CLIENTS = ("databricks", "dbx-recon", "spark-sql", "dbsqlcli")
_IDENTITY_CLIENTS = ("databricks", "dbx-recon", "spark-sql")
_PYTHON = re.compile(r"python[0-9.]*|spark-submit")
_REST_CLIENTS = ("curl", "wget", "http", "https", "xh")
_CLIENT_WORD = re.compile(r"(?<![\w-])(?:" + "|".join(map(re.escape, (*_DBX_CLIENTS, *_LEGACY_ONLY, *_GENERIC))) + r")(?![\w-])")
_SHELLS = ("sh", "bash", "zsh", "dash", "ksh")
_DECLARERS = ("export", "declare", "typeset", "readonly", "local")
_FIXERS = ("ruff", "black", "isort", "autopep8", "yapf", "autoflake")   # rewrite their operands in place
_DBT_RUNS = ("run", "build", "seed", "snapshot", "run-operation")
# programs the guard has a rule for; any other head in front of a client word is an unmodelled wrapper
_KNOWN = frozenset((*_DBX_CLIENTS, *_LEGACY_ONLY, *_GENERIC, *_REST_CLIENTS, *_SHELLS, *_DECLARERS, *_FIXERS, "dbt", "eval", "source",
                    ".", "docker", "podman", "nerdctl", "kubectl", "perl", "ruby", "node", "java", "patch", "xargs"))
# modelled prefixes and their value-taking options; ssh / docker exec|run / kubectl exec also consume the host or container word
_PREFIX_VALUE_FLAGS = {
    "sudo": ("-u", "-g", "-C", "-h", "-p", "-r", "-t", "-U", "-D", "-T"), "doas": ("-u", "-C"), "nohup": (),
    "env": ("-u", "--unset", "-C", "-S", "--split-string", "--chdir"), "nice": ("-n", "--adjustment"),
    "ionice": ("-c", "-n", "-p", "-P", "-u", "--class", "--classdata"), "timeout": ("-s", "--signal", "-k", "--kill-after"),
    "stdbuf": ("-i", "-o", "-e", "--input", "--output", "--error"),
    "ssh": ("-p", "-i", "-o", "-l", "-F", "-J", "-L", "-R", "-D", "-b", "-m", "-c", "-E", "-e", "-I", "-Q", "-S", "-W", "-B"),
    "docker": ("-e", "--env", "-u", "--user", "-w", "--workdir", "--name", "-v", "--volume", "-p", "--publish", "--network",
               "--entrypoint", "--platform"), "kubectl": ("-c", "--container", "-n", "--namespace"),
}
_ASSIGN = re.compile(r"^[A-Za-z_]\w*=")
_ENV_DEFAULTS = {"HOME": "~", "TMPDIR": "/tmp"}
_SHELL_VAR = re.compile(r"\$\{(\w+)\}|\$(\w+)")
# heads that only read their operands; any other head naming `.migration/` writes, and in front of a client word is a wrapper
_READERS = frozenset(("cat", "less", "more", "head", "tail", "grep", "rg", "egrep", "fgrep", "zgrep", "diff", "cmp", "ls", "stat", "wc",
                      "file", "jq", "yq", "md5sum", "sha1sum", "sha256sum", "sort", "uniq", "cut", "tr", "awk", "gawk", "mawk", "sed",
                      "tree", "du", "echo", "printf", "test", "[", "[[", "cd", "pushd", "popd", "dirname", "basename", "realpath",
                      "readlink", "which", "type", "find", "tar", "unzip", "column", "nl", "od", "xxd", "strings", "pytest", "ruff",
                      "true", "false", "sleep", "date", "env", "printenv", "set", "export", "unset", "dbx-recon", "databricks"))
_INERT = _READERS | frozenset(("man", "info", "whereis", "whatis", "apt", "apt-get", "brew", "pip", "pip3", "yum", "dnf", "apk", "conda",
                               "mamba", "git", "help", "hash", "alias", "unalias", "complete", "apropos", "tldr", "locate", "ldd",
                               "pg_dump", "pg_dumpall", "mysqldump"))
_IDENTITY_VAR = re.compile(r"^(?:DATABRICKS_\w+|ARM_CLIENT_\w+|ARM_TENANT_ID|AZURE_\w+|GOOGLE_CREDENTIALS|GOOGLE_APPLICATION_CREDENTIALS)$")
_CONFIG_HOME_VAR = ("HOME", "XDG_CONFIG_HOME")   # where ~/.databrickscfg is looked up
_FUNCTION_DEF = re.compile(r"(?:^|[;&|\n{}()]\s*)(?:function\s+[\w.-]+|[\w.-]+\s*\(\s*\))")
_SQL_VALUE_FLAGS = ("-c", "-Q", "-q", "-e", "--query", "--sql", "--statement", "--execute", "--command")
_SCRIPT_FLAGS = ("-f", "-i", "--file", "--input")
_HOST_FLAGS = ("-S", "-h", "-H", "--host", "--server", "--hostname", "--url", "-url")
_HOST_ENV = ("PGHOST", "PGHOSTADDR", "PGSERVICE", "MYSQL_HOST", "SQLCMDSERVER")
_DSN_POSITIONAL = ("isql", "usql", "pgloader")   # first positional is the DSN / URL, not a database
# reconnect meta-commands: psql `\c|\connect db [user [host]]` / conninfo / URI, mysql `connect|\r db [host]`, sqlcmd `:connect host`
_RECONNECT = re.compile(r"(?im)^[ \t]*(\\c(?:onnect)?|connect|\\r|:connect)[ \t]+([^;\n]*)")
_RUN_FILE = re.compile(r"(?<!\S)@(\S+)|^\s*\.RUN\s+FILE\s*=?\s*(\S+)", re.IGNORECASE | re.MULTILINE)
_DYNAMIC_SQL_EXECUTOR = (r"(?:\bEXEC(?:UTE)?\s+IMMEDIATE|\bsp_executesql|\bEXEC(?:UTE)?\s*\(|"
                         r"\.(?:execute|executemany|sql|run_query|execute_statement)\s*\(|\bstatement\s*=)")
_DYNAMIC_SQL_CALLER = re.compile(_DYNAMIC_SQL_EXECUTOR + r"\s*N?\s*$", re.IGNORECASE)
_PY_LITERAL = re.compile(_DYNAMIC_SQL_EXECUTOR + r"\s*[rbuf]*(['\"]{3}|['\"])(.*?)\1", re.IGNORECASE | re.DOTALL)
_PY_WRITE = re.compile(r"""['"](?:[wax]\+?|\+?>>?|\+<)['"]|\.write\w*\(|json\.dump\(|os\.(?:remove|unlink|rename|replace|chmod|rmdir|makedirs|mkdir)\(|"""
                       r"""shutil\.|\.(?:unlink|rename|rmdir|mkdir|touch|chmod)\(|\bunlink\b|\bwriteFile\w*\(""")
_MIGRATION_PATH = re.compile(r"[^\s'\"()]*\.migration(?:/[^\s'\"()]*)?")
_RMTREE = re.compile(r"rmtree\(\s*(?:['\"]([^'\"]*)['\"]|(os\.getcwd\(\)|Path\.cwd\(\)|Path\(\s*(?:['\"]\.?['\"])?\s*\)))")
_SQL_OUT_PATH = re.compile(r"(?i)(?:\bTO\s+|\\[ow]\s+|:out\s+|\bSPOOL\s+|\bFILE\s*=\s*)'?([^\s'\"]*\.migration(?:/[^\s'\"]*)?)")
_SQL_OPAQUE = re.compile(r"--[^\n]*|/\*.*?(?:\*/|\Z)|'(?:[^']|'')*(?:'|\Z)", re.DOTALL)

# read shapes: leading keyword, then no write keyword anywhere in the statement
_READ_HEAD = ("SELECT", "WITH", "SET", "USE", "DECLARE")
_DESCRIBE_HEAD = ("SHOW", "DESC", "DESCRIBE", "HELP", "GO")
_SQLPLUS_DIRECTIVE = ("SPOOL", "PROMPT", "DEFINE", "COLUMN", "WHENEVER", "EXIT", "QUIT", "TTITLE", "BTITLE", "BREAK",
                      "COMPUTE", "TIMING", "REM", "REMARK", "PAUSE", "CLEAR")
_DIRECTIVE_LINE = re.compile(r"^\s*(?:[.\\:@/]|GO\b|(?:" + "|".join(_SQLPLUS_DIRECTIVE) + r")\b)", re.IGNORECASE)
_PSQL_META = re.compile(r"\\(?:d\S*|l\S*|x|q|\?|h\S*|timing|echo|pset|set|unset|c(?:onnect)?|conninfo|encoding|z|sf|sv|a|t|H|C|f)\b")
_SQLCMD_DIRECTIVE = re.compile(r":(?:setvar|exit|quit|on\s+error|help|list\w*|reset|xml|error|out|perftrace|connect)\b", re.IGNORECASE)
# read prefixes peeled off so the statement behind them is judged on its own: EXPLAIN [...], Teradata LOCKING ... FOR ACCESS|READ
_SQL_PREFIX = re.compile(r"(?:EXPLAIN\b(?:\s+(?:ANALYZE|VERBOSE|PLAN|EXTENDED|CODEGEN|COST|FORMATTED|QUERY\s+PLAN)\b|\s*\([^)]*\)|\s+FOR\b)*\s*"
                         r"|LOCK(?:ING)?\s+(?:ROW|(?:TABLE|DATABASE|VIEW)\s+\S+)?\s*FOR\s+(?:ACCESS|READ)\b(?:\s+(?:NOWAIT|MODE))*\s*)*", re.IGNORECASE)
# `_SQL_ALLOW`: what `SET TRANSACTION` may say (the harness's consistency-window idiom); `_SQL_DENY`: takes a lock on the
# source, opens a writable transaction or switches the session (NOLOCK, READUNCOMMITTED, READPAST, READCOMMITTED stay reads)
_SQL_ALLOW = (r"(?:ISOLATION\s+LEVEL\s+(?:READ\s+(?:UNCOMMITTED|COMMITTED)|REPEATABLE\s+READ|SERIALIZABLE|SNAPSHOT)"
              r"|READ\s+ONLY|(?:NOT\s+)?DEFERRABLE)")
_SQL_DENY = re.compile(
    r"\bWITH\s*\(\s*(?:\w+(?:\s*\([^()]*\))?\s*,\s*)*(?:TABLOCKX?|XLOCK|HOLDLOCK|UPDLOCK|SERIALIZABLE|REPEATABLEREAD)\b"
    r"|\bFOR\s+(?:NO\s+KEY\s+)?UPDATE\b|\bFOR\s+(?:KEY\s+)?SHARE\b|\bLOCK(?:ING)?\b.*?\bFOR\s+(?:WRITE|EXCLUSIVE)\b"
    r"|^SET\s+(?:IDENTITY_INSERT|IMPLICIT_TRANSACTIONS|ROLE|SESSION\s+AUTHORIZATION)\b"
    r"|^SET\s+TRANSACTION\b(?!\s+" + _SQL_ALLOW + r"(?:\s*,?\s*" + _SQL_ALLOW + r")*\s*$)", re.IGNORECASE)
_NON_READ_WORD = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|CREATE|ALTER|GRANT|REVOKE|DENY|EXEC(?:UTE)?|CALL|KILL|BACKUP|RESTORE|DBCC|WAITFOR|"
    r"BEGIN|COMMIT|ROLLBACK|ENABLE|DISABLE|INTO|BULK|OPENROWSET|OPENQUERY|SHUTDOWN|RECONFIGURE|WRITETEXT|UPDATETEXT|sp_\w+|xp_\w+)\b", re.IGNORECASE)
# functions that mutate, disrupt or reach outside the source from inside a SELECT (Oracle DBMS_*/UTL_* members need no parens)
_SIDE_EFFECT_FN = re.compile(
    r"\b(?:nextval|setval|set_config|pg_sleep(?:_for|_until)?|pg_terminate_backend|pg_cancel_backend|pg_reload_conf|"
    r"pg_rotate_logfile|pg_(?:try_)?advisory_\w*lock\w*|lo_(?:import|export|unlink|creat|create|put|truncate)|dblink\w*|"
    r"opendatasource)\s*\(|\b(?:sys\.)?(?:dbms|utl)_\w+\.\w+\b|\.NEXTVAL\b", re.IGNORECASE)

# Databricks CLI: read verbs per group; mutations naming a securable, with the index of the positional carrying its catalog
_UC_READ = {"list", "get", "exists"}
_DBX_READ = {
    "current-user": {"me"}, "catalogs": _UC_READ, "schemas": _UC_READ, "tables": _UC_READ, "volumes": _UC_READ, "functions": _UC_READ,
    "metastores": _UC_READ, "external-locations": _UC_READ, "storage-credentials": _UC_READ, "connections": _UC_READ,
    "grants": {"get", "get-effective"}, "jobs": {"list", "get", "list-runs", "get-run", "get-run-output"},
    "pipelines": {"list", "get", "list-updates", "get-update", "list-pipeline-events"}, "warehouses": {"list", "get"},
    "clusters": {"list", "get", "events", "spark-versions", "list-node-types", "list-zones"},
    "workspace": {"list", "export", "get-status"}, "secrets": {"list-scopes", "list-secrets"},
    "auth": {"describe", "profiles"}, "fs": {"ls", "cat", "head"}, "api": {"get"}, "bundle": {"validate", "summary"}}
_TOKEN_PRINTERS = ("auth token", "auth env")   # print the bearer token into the session log
_LIFECYCLE = ("catalogs create", "catalogs update", "catalogs delete", "schemas delete", "grants update", "grants delete")
_CLI_CATALOG_ARG = {"schemas create": 1, "schemas update": 0, "tables delete": 0, "volumes create": 0, "volumes delete": 0,
                    "volumes update": 0, "functions delete": 0, "functions update": 0}
_DBX_VALUE_FLAGS = {"-o", "--output", "--log-level", "--log-file", "--log-format", "--progress-format", "-t", "--target", "-p",
                    "--profile", "--host", "--warehouse-id", "--catalog", "--schema", "--format", "--wait-timeout", "--json",
                    "--var", "--file", "--language", "--string-value", "--bytes-value", "-e", "--statement", "--query"}
_UC_PATH = re.compile(r"unity-catalog/(?:tables|schemas|volumes|functions)/([^/?\s]+)")
_VOLUME_PATH = re.compile(r"^(?:dbfs:)?/Volumes/([^/]+)/")
_DBX_HOST = re.compile(r"\$\{?DATABRICKS_HOST\b|\.(?:cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com)\b", re.IGNORECASE)
_DBX_API_PATH = re.compile(r"^\$[^/\s]*/api/\d")   # `$H/api/2.1/...`: an unresolved host in front of an API path
_HTTP_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")
_CURL_VALUE_SHORT = "dFTHouAebcmwxEKUyYzCDQrtPO"   # short options that take a value (end a bundled cluster)
_OP = re.compile(r"[<>]\(|<<<|<<-|&>>|<<|<>|<&|>>|>&|>\||&>|\|&|\|\||&&|[;|&()<>\n]")
_REDIRECT_OP = re.compile(r"\d*(?:<{1,3}-?|<>|<&|>{1,2}|>&|>\||&>{1,2})")
_STDIN_OP = re.compile(r"0?<")
_SEPARATORS = (";", "&&", "||", "|", "|&", "&", "(", ")", "{", "}", "\n")
_FLAG_WORD = re.compile(r"-{1,2}[\w.-]+(?:=\S*)?")
_UNREADABLE = (f"{{who}} fed script(s) {{files}} that the guard cannot read in full (missing, unreadable or over "
               f"{_MAX_SCRIPT_BYTES >> 20} MiB); inline the SQL or split it so it can be inspected")

_IN, _ALL = ("inside", "self"), ("inside", "self", "above")   # the `.migration/` relations a write blocks on
_WRITE_LAST_OPERAND = ("cp", "rsync", "install", "ln", "scp")
_RECURSIVE_HEADS = ("rm", "chmod", "chown", "chgrp", "rsync", "chattr", "setfacl")
# heads that remove, replace or rewrite their operand: block above the guard and on an unresolvable destination
_DESTRUCTIVE = ("mv", "truncate", "dd", "shred", *_WRITE_LAST_OPERAND, *_RECURSIVE_HEADS, *_FIXERS)
_IN_PLACE = {"sed": (re.compile(r"-[nEersuz]*i.*"), re.compile(r"--in-place.*")), "perl": (re.compile(r"-[a-zA-Z]*i.*"),),
             "awk": (re.compile(r"(?:--?)?inplace"),), "gawk": (re.compile(r"(?:--?)?inplace"),), "mawk": (re.compile(r"(?:--?)?inplace"),),
             "ruff": (re.compile(r"--fix|--fix-only|--unsafe-fixes|format"),)}
_IDENTITY_FILE = re.compile(r"(?:^|/)(?:\.databrickscfg|\.databricks(?:/.*)?|\.config/databricks(?:/.*)?)$")
_GUARD_TREE = Path(os.path.realpath(__file__)).parent.parent   # the running plugin
_GUARD_FILE = Path(__file__).name
_PATH_LITERAL = re.compile(r"['\"]((?:[~./$]|/)[^'\"\n]{0,300})['\"]")
_GUARD_LITERAL = re.compile(r"['\"]((?:[^'\"\n/]*/)*(?:hooks(?:/[^'\"\n]*)?|hooks\.json|" + re.escape(_GUARD_FILE) + r"))['\"]")
_OUTPUT_FLAGS = ("-o", "-O", "--output", "--out", "--out-file", "--output-file", "--outfile", "--file")
# git forms that rewrite the whole working copy: the verb alone, or with one of these flags / first operands
_GIT_DISCARDS = {"clean": (), "reset": ("--hard", "--merge", "--keep"), "checkout": ("-f", "--force"),
                 "switch": ("-f", "--force", "--discard-changes"), "stash": ("", "push", "save")}
# git on the running guard's tree: only these sub-commands and the list forms below run there
_GIT_READS = frozenset(("log", "diff", "status", "show", "fetch", "blame", "annotate", "describe", "grep", "shortlog", "reflog",
                        "rev-parse", "rev-list", "ls-files", "ls-tree", "ls-remote", "cat-file", "for-each-ref", "show-ref",
                        "merge-base", "name-rev", "diff-tree", "diff-index", "diff-files", "count-objects", "check-ignore",
                        "var", "version", "help", "whatchanged"))
_GIT_LIST_FORMS = {"stash": ("list", "show"), "worktree": ("list",), "submodule": ("status", "summary"), "remote": ("", "show", "get-url")}
_GIT_LIST_FLAGS = ("-l", "--list", "--contains", "--no-contains", "--merged", "--no-merged", "--points-at")
_GIT_SHOW_FLAGS = ("-a", "-r", "-v", "-vv", "--all", "--remotes", "--verbose", "--show-current", "--column", "--no-column")
_GIT_CONFIG_READS = ("-l", "--list", "--get", "--get-all", "--get-regexp", "--get-urlmatch")
_GIT_DIR_VALUE = re.compile(r"(?:^|/)\.git/?$")


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
            value = data.get(key, list(DEFAULT_FORBIDDEN_BUNDLE_TARGETS) if key == "forbidden_bundle_targets" else [])
            if not isinstance(value, list):
                raise ValueError(f"'{key}' must be a list")
            lists[key] = [str(x).strip() for x in value if str(x).strip()]
        mode = str(data.get("guard_mode", "block")).lower()
        if mode not in ("block", "warn"):
            raise ValueError("'guard_mode' must be 'block' or 'warn'")
        return cls([_norm(c) for c in catalogs], lists["legacy_sources"], mode, tuple(t.lower() for t in lists["forbidden_bundle_targets"]),
                   [h.lower() for h in lists["target_hosts"]], lists["bundle_targets"], path)   # DAB / dbt targets compared exactly


@dataclass
class Verdict:
    decision: str  # approve | block
    reason: str = ""
    violations: list[str] = field(default_factory=list)

    @classmethod
    def of(cls, violations: list[str], cfg: GuardConfig) -> Verdict:
        if not violations:
            return cls("approve")
        reason = ("dbx-migration-factory guard: " + "; ".join(violations) + ". Fix the command or, if the target is legitimate, add "
                  "it to .migration/allowed_targets.json via a recorded decision (.migration/06_decisions.md); never work around the guard.")
        return cls("approve", "WARN (guard_mode=warn): " + reason, violations) if cfg.mode == "warn" else cls("block", reason, violations)


def _norm(ident: str) -> str:
    return ident.strip().strip('`"[]').lower()


def load_config(start: Path) -> GuardConfig | None:
    """The allowlist of the nearest `.migration/` up the tree; None without one, OSError when the directory has no file (fail closed)."""
    start = start.resolve()
    path = next((d / CONFIG_REL for d in [start, *start.parents] if (d / CONFIG_REL).is_file() or (d / CONFIG_REL.parent).is_dir()), None)
    if path is None:
        return None
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"{path} must be a JSON object")
    return GuardConfig.from_dict(data, path)


def _sql_view(text: str) -> str:
    """SQL as the detectors read it, offsets preserved: comments and quoted literals blanked, unless the literal feeds a
    dynamic-SQL executor seen in the text already viewed (whitespace collapsed), where it is the statement."""
    out, tail, pos = [], "", 0
    for m in _SQL_OPAQUE.finditer(text):
        out.append(text[pos:m.start()])
        tail = re.sub(r"\s+", " ", tail + out[-1])[-200:]
        s, closed = m.group(), len(m.group()) > 1 and m.group().endswith("'")
        piece = (re.sub(r"[^\n]", " ", s) if s[0] != "'" else s if _DYNAMIC_SQL_CALLER.search(tail) else
                 "'" + re.sub(r"[^\n]", " ", s[1:len(s) - closed]) + "'" * closed)
        out.append(piece)
        tail = re.sub(r"\s+", " ", tail + piece)[-200:]
        pos = m.end()
    out.append(text[pos:])
    return "".join(out)


# ---------------------------------------------------------------- shell model: words, simple commands, what feeds them

def _shell_tokens(cmd: str) -> tuple[list[str], list[str]]:
    """(words, raw words): quotes removed / kept; operators and line breaks are tokens of their own, `2>&1` descriptors stay
    with their operator, a heredoc body replaces its delimiter token (raw single-quoted when the delimiter was)."""
    toks: list[list[str]] = []   # [word, raw word]
    pending: list[tuple[int, bool, bool]] = []   # (delimiter token index, strip tabs, quoted delimiter)
    word, rword, quote, plain, started, i, n = "", "", "", True, False, 0, len(cmd)
    while i < n:
        ch = cmd[i]
        if quote:
            esc = quote == '"' and ch == "\\" and i + 1 < n and cmd[i + 1] in '"\\$`\n'
            word += cmd[i + 1] if esc else "" if ch == quote else ch
            rword, i = rword + cmd[i:i + 1 + esc], i + 1 + esc
            quote = "" if ch == quote and not esc else quote
        elif ch == "\\" and i + 1 < n:
            if cmd[i + 1] != "\n":
                word, rword, plain, started = word + cmd[i + 1], rword + cmd[i:i + 2], False, True
            i += 2
        elif ch in "'\"":
            quote, plain, started, rword, i = ch, False, True, rword + ch, i + 1
        elif ch in " \t\r" or (m := _OP.match(cmd, i)):
            op = m.group() if ch not in " \t\r" else ""
            i += len(op) or 1
            if op and started and plain and word.isdigit() and op[0] in "<>":
                word, op, started = "", word + op, False
            if started:
                toks.append([word, rword])
            word, rword, plain, started = "", "", True, False
            if op:
                toks.append([op, op])
            if op in ("<<", "<<-"):
                pending.append((len(toks), op == "<<-", i < n and cmd[i:i + 1] in "'\"\\"))
            elif op == "\n":
                for idx, tabs, quoted in pending:
                    if idx >= len(toks):
                        break
                    tag, lines = toks[idx][0], cmd[i:].split("\n")
                    body = lines[:next((k for k, ln in enumerate(lines) if (ln.lstrip("\t") if tabs else ln).rstrip() == tag), len(lines))]
                    i += sum(len(ln) + 1 for ln in lines[:len(body) + 1])   # the body and its delimiter line
                    text = "\n".join(body) + "\n"
                    toks[idx] = [text, f"'{text}'" if quoted else text]
                pending.clear()
        else:
            word, rword, started, i = word + ch, rword + ch, True, i + 1
    if started:
        toks.append([word, rword])
    return [t[0] for t in toks], [t[1] for t in toks]


@dataclass
class _Seg:
    """One simple command: its words (redirections and heredoc bodies included) and their raw forms, the producers feeding its
    stdin, and -- once `_segments` has read it -- the program it runs, under which prefixes, and where."""
    words: list[str] = field(default_factory=list)
    raw: list[str] = field(default_factory=list)
    feeds: list[_Seg] = field(default_factory=list)
    argv: list[str] = field(default_factory=list)      # program and arguments after assignments, prefixes, aliases, redirections
    assigns: list[str] = field(default_factory=list)   # VAR=value prefixes (`env -u X` recorded as `X=`, `env -C d` as `PWD=d`)
    stdin: list[str] = field(default_factory=list)     # literal text piped in (echo/printf words, cat heredocs)
    opaque: str | None = None                          # a piped producer whose output the guard cannot read
    scripts: list[str] = field(default_factory=list)   # files it executes
    ctx: str = ""                                      # text of the prefixes / wrapper (`ssh host`) it runs under
    at: str | None = ""                                # directory it runs in ('' the workspace root, None unresolvable)

    argv0 = property(lambda s: s.argv[0].rsplit("/", 1)[-1] if s.argv else "")
    heredocs = property(lambda s: [f for op, f in s.redirects() if op.endswith(("<<", "<<-"))])
    herestring = property(lambda s: next((f for op, f in s.redirects() if op.endswith("<<<")), None))
    text = property(lambda s: " ".join([*s.words, *s.stdin, s.herestring or "", s.ctx]))
    args = property(lambda s: [w for i, w in enumerate(s.words)   # the words without redirections and their operands
                               if not (_REDIRECT_OP.fullmatch(w) or (i and _REDIRECT_OP.fullmatch(s.words[i - 1])))])

    def redirects(self) -> list[tuple[str, str]]:
        return [(op, f) for op, f in itertools.pairwise(self.words) if _REDIRECT_OP.fullmatch(op)]

    def raw_of(self, word: str) -> str:
        return self.raw[self.words.index(word)] if word in self.words else word


def _commands(cmd: str) -> list[_Seg]:
    """Simple commands split at operators, group delimiters and line breaks; what a `|` feeds reaches every member of a group
    on its right, and a redirection after `)`/`}` is repeated on every member of the group it closes (fail closed)."""
    toks, raws = _shell_tokens(cmd)
    out: list[_Seg] = []
    groups: list[tuple[int, list[_Seg]]] = []   # (index of the first member, stdin the group inherits)
    feed: list[_Seg] = []                        # what the next command's stdin receives
    closed: list[_Seg] = []                      # members of the group just closed
    cur, i = None, 0
    while i < len(toks):
        tok, n = toks[i], 1 + bool(_REDIRECT_OP.fullmatch(toks[i]))   # a redirection travels with its operand
        if n == 1 and tok == "\n" and cur is None and feed and not closed:
            pass                                        # a line break after `|` continues the pipeline
        elif n == 1 and tok in _SEPARATORS and (tok not in ("{", "}") or cur is None):
            if tok in ("(", "{"):
                groups.append((len(out), feed))
            elif tok in (")", "}"):
                start, feed = groups.pop() if groups else (0, [])
                closed = out[start:]
            elif tok in ("|", "|&"):
                producers = closed or ([cur] if cur else [])
                feed, closed = producers + [f for p in producers for f in p.feeds], []
            else:
                feed, closed = (groups[-1][1] if groups else []), []
            cur = None
        else:
            if cur is None and (n == 1 or not closed):
                cur, closed = _Seg(feeds=list(feed)), []
                out.append(cur)
            for c in closed if cur is None else [cur]:
                c.words.extend(toks[i:i + n])
                c.raw.extend(raws[i:i + n])
        i += n
    return [c for c in out if c.words]


def _expands(text: str, subst: bool = False) -> bool:
    """Whether the shell would expand the text: `$` (`subst`: `$(`/`<(`/`>(`) outside single quotes, or a non-identifier backtick span."""
    live = re.sub(r"\\.|'[^']*'?", lambda m: " " * len(m.group()), text, flags=re.DOTALL)
    return any(not re.fullmatch(r"[\w$-]+", m.group(1)) for m in re.finditer(r"`([^`]*)`", live)) or (
        bool(re.search(r"\$\(|(?<![\w])[<>]\(", live)) if subst else "$" in live)


def _program(words: list[str], assigns: list[str]) -> tuple[list[str], str]:
    """(argv, prefix text): the words after `VAR=value` and the modelled prefixes (`_PREFIX_VALUE_FLAGS`, `docker exec|run`,
    `kubectl exec ... --`); `env -u X` is recorded as `X=`, `env -C d` as `PWD=d`; one quoted payload is a remote command line."""
    i = 0
    while i < len(words):
        w = words[i].rsplit("/", 1)[-1]
        if _ASSIGN.match(words[i]):
            assigns.append(words[i])
            i += 1
            continue
        w = "docker" if w in ("podman", "nerdctl") else w
        if w == "docker":
            i += 1 + (words[i + 1:i + 2] == ["compose"])
            if words[i:i + 1] not in (["exec"], ["run"]):
                return words, ""
        elif w == "kubectl" and words[i + 1:i + 2] == ["exec"]:
            i += 1
        elif w not in _PREFIX_VALUE_FLAGS:
            break
        i += 1
        while i < len(words) and words[i].startswith("-") and (w != "kubectl" or words[i] != "--"):
            if w == "env" and words[i] in ("-u", "--unset", "-C") and i + 1 < len(words):
                assigns.append("PWD=" + words[i + 1] if words[i] == "-C" else words[i + 1] + "=")
            i += 2 if words[i] in _PREFIX_VALUE_FLAGS[w] else 1
        i += (w == "timeout") + (w in ("ssh", "docker")) + (w == "kubectl" and (words.index("--", i) + 1 - i if "--" in words[i:] else 1))
    rest = words[i:]
    if i and len(rest) == 1 and re.search(r"\s", rest[0]):
        rest = ["sh", "-c", rest[0]]   # `ssh host 'bteq <<EOF ...'`: the payload is a shell command line
    return rest, " ".join(words[:i])


def _scripts_of(seg: _Seg, argv: list[str] | None = None) -> list[str]:
    """Files the command executes: `< f`, `@f`, `-f/-i/--file/--input f`, `@f` / `.RUN FILE` on a heredoc line, and what a text
    producer pipes in (`cat fix.sql | bteq`, `echo @fix.sql | bteq`)."""
    def at_files(texts: list[str]) -> list[str]:
        return [(m.group(1) or m.group(2)).lstrip("@") for t in texts for m in _RUN_FILE.finditer(t)]

    files = []
    for p in seg.feeds:
        args, base = p.args, p.args[0].rsplit("/", 1)[-1] if p.args else ""
        files += at_files(p.heredocs)
        if base == "cat":   # cat reads its stdin only without an operand, or with `-`
            operands = [w for w in args[1:] if w == "-" or not w.startswith("-")]
            files += [w for w in operands if w != "-"] + ([f for op, f in p.redirects() if _STDIN_OP.fullmatch(op)]
                                                        if not operands or "-" in operands else [])
        elif base in ("echo", "printf"):
            files += at_files(args[1:])
    for tok, nxt in itertools.pairwise([*(seg.args if argv is None else argv), ""]):
        f = nxt if tok in _SCRIPT_FLAGS else tok[1:] if tok.startswith("@") else \
            tok.split("=", 1)[1] if tok.startswith(tuple(fl + "=" for fl in _SCRIPT_FLAGS)) else ""
        if f and not f.startswith("-"):
            files.append(f)
    files += [f for op, f in seg.redirects() if _STDIN_OP.fullmatch(op)]
    return files + at_files(seg.heredocs)


def _script_inputs(cmd: str, cfg: GuardConfig | None = None) -> list[str]:
    """Script files of every simple command; given a config, only of those naming a client or a legacy source."""
    return [f for c in _commands(cmd) if cfg is None or _context(" ".join(c.args), cfg) for f in _scripts_of(c)]


def _join(at: str | None, d: str) -> str | None:
    """Where a command is after moving to `d` from `at` ('' the workspace root); None when either is unresolvable."""
    if at is None or d == "-" or _expands(d):
        return None
    d = os.path.expanduser(d)
    p = os.path.normpath(d if d.startswith("/") or not at else os.path.join(at, d))
    return "" if p == "." else p


def _read_script(f: str, root: Path, at: str | None = "") -> str | None:
    """The file's text, or None when it cannot be read in full; a relative name resolves in `at` (under `root`; None: nothing resolves)."""
    if at is None:
        return None
    p = (root if not at else Path(at) if at.startswith("/") else root / at) / os.path.expandvars(os.path.expanduser(f))
    try:
        return None if p.stat().st_size > _MAX_SCRIPT_BYTES else p.read_text(errors="replace")
    except OSError:
        return None


def _shell_runs(seg: _Seg) -> tuple[str | None, str | None, bool]:
    """(text, file, built) of what a shell segment runs: the literal of `sh -c` / `eval` / a literal piped into `bash`; the
    script of `bash x.sh`, `bash < x.sh`, `source x`, `./x.sh`; `built` when the text is assembled at run time."""
    argv, base = seg.argv, seg.argv0
    if base == "eval" and len(argv) > 1:
        return (None if (built := any(_expands(seg.raw_of(w)) for w in argv[1:])) else " ".join(argv[1:])), None, built
    if base in ("source", ".") and len(argv) > 1:
        return None, argv[1], False
    if argv and (argv[0].startswith(("./", "../")) and base not in _KNOWN or base.endswith(".sh")):   # a script run by name
        return None, argv[0], False
    if base not in _SHELLS:
        return None, None, False
    for i, w in enumerate(argv[1:-1], 1):
        if re.fullmatch(r"-[A-Za-z]*c[A-Za-z]*", w):
            arg = argv[i + 1] if argv[i + 1] != "--" or i + 2 >= len(argv) else argv[i + 2]
            return (None if (built := _expands(seg.raw_of(arg))) else arg), None, built
    if len(argv) == 1 and seg.stdin and not seg.opaque:
        return "\n".join(seg.stdin), None, False
    positional = [w for w in argv[1:] if not w.startswith("-")]
    return None, positional[0] if positional else seg.scripts[0] if seg.scripts else None, False


def _segments(text: str, ctx: str = "", depth: int = 0, env: dict[str, str] | None = None, at: str | None = "") -> list[_Seg]:
    """The simple commands of the text and of every literal it hands another shell, each with its program, what feeds it and
    the directory it runs in (`at`, moved by `cd`/`pushd`/`popd`, `env -C` for its own command only)."""
    aliases: dict[str, list[str]] = {}
    env = dict(_ENV_DEFAULTS) if env is None else env
    out: list[_Seg] = []
    dirs: list[str | None] = []
    for seg in _commands(text):
        seg.words = [w if not env or "$" not in w or "$" not in re.sub(r"\\.|'[^']*'?", "", r) else
                     _SHELL_VAR.sub(lambda m: env.get(m.group(1) or m.group(2), m.group()), w) for w, r in zip(seg.words, seg.raw)]
        seg.argv, seg.ctx = _program(aliases.get(seg.args[0] if seg.args else "", seg.args[:1]) + seg.args[1:], seg.assigns)
        seg.ctx = " ".join(x for x in (ctx, seg.ctx) if x)
        env.update(a.split("=", 1) for a in (seg.assigns if not seg.argv else seg.argv[1:] if seg.argv0 in _DECLARERS else ())
                   if _ASSIGN.match(a))   # a bare or declared assignment persists for later commands
        for p in seg.feeds:
            pargs, base = p.args, p.args[0].rsplit("/", 1)[-1] if p.args else ""
            if base not in ("cat", "echo", "printf", "tee") or _expands(" ".join(p.raw)):
                seg.opaque = base or "?"
            elif base in ("echo", "printf"):
                seg.stdin.append(" ".join(w for w in pargs[1:] if not re.fullmatch(r"-[neE]+", w)))
            elif base == "cat":
                seg.stdin.extend(p.heredocs)
        seg.scripts = _scripts_of(seg, seg.argv + [w for op, f in seg.redirects() for w in (op, f)] if seg.ctx else None)
        seg.at = next((_join(at, a[4:]) for a in reversed(seg.assigns) if a.startswith("PWD=")), at)   # `env -C dir`: this command alone
        if seg.argv0 in ("cd", "pushd"):
            args = [w for w in seg.argv[1:] if not (w.startswith("-") and len(w) > 1)]
            dirs += [at] if seg.argv0 == "pushd" else []
            at = _join(at, args[0] if args else "~")
        elif seg.argv0 == "popd":
            at = dirs.pop() if dirs else None
        elif seg.argv0 == "alias":
            aliases.update((a.split("=", 1)[0], shlex.split(a.split("=", 1)[1])) for a in seg.argv[1:] if "=" in a)
        out.append(seg)
        if (nested := _shell_runs(seg)[0]) is not None and depth < 4:
            out.extend(_segments(nested, seg.ctx, depth + 1, env, seg.at))
    return out


def _context(text: str, cfg: GuardConfig, legacy_only: bool = False) -> list[str]:
    """The client words, `--target-catalog` and legacy-source names the text mentions (only the latter with `legacy_only`)."""
    hits = [tok for tok in cfg.legacy_sources if re.search(rf"(?<![\w-]){re.escape(tok)}(?![\w-])", text, re.IGNORECASE)]
    return hits if legacy_only else [m.group() for m in _CLIENT_WORD.finditer(text)] + (["--target-catalog"] if "--target-catalog" in text else []) + hits


# ---------------------------------------------------------------- policy

def _flag_values(argv: list[str], flags: tuple[str, ...]) -> list[str]:
    """Values of `flags` in argv (`-e SQL`, `--file=x`); the next word is the value unless it is itself a flag word."""
    return [argv[i + 1] if w in flags else w.split("=", 1)[1] for i, w in enumerate(argv[1:], 1)
            if (w in flags and i + 1 < len(argv) and not _FLAG_WORD.fullmatch(argv[i + 1])) or ("=" in w and w.split("=", 1)[0] in flags)]


def _sql_text(seg: _Seg, root: Path, extra: list[str] = ()) -> tuple[str, list[str]]:
    """Every piece of SQL a client segment executes (`extra`: positional SQL or `.sql` files), joined, plus unreadable scripts."""
    parts = [" ".join(w for w in extra if not w.endswith(".sql")), *_flag_values(seg.argv, _SQL_VALUE_FLAGS), *seg.stdin, *seg.heredocs,
             seg.herestring or ""]
    bodies = [(f, _read_script(f, root, seg.at)) for f in [*seg.scripts, *(w for w in extra if w.endswith(".sql"))]]
    return "\n;\n".join(filter(None, [*parts, *(b for _, b in bodies)])), [f for f, b in bodies if b is None]


def _non_reads(sql: str) -> list[str]:
    """Statements (split at `;`, directive lines on their own) that are not read shapes: after `_SQL_PREFIX` is peeled off, a
    read is a known directive / meta-command, `@file`, `/`, a describe word, or a `_READ_HEAD` statement carrying no write
    keyword, side-effecting function or denied lock / transaction token."""
    view = "\n".join(f";{line};" if _DIRECTIVE_LINE.match(line) else line for line in _sql_view(sql).split("\n"))
    bad = []
    for stmt in filter(None, (" ".join(x for x in map(str.strip, chunk.split("\n")) if x) for chunk in view.split(";"))):
        s = stmt[_SQL_PREFIX.match(stmt).end():]
        head = re.match(r"[A-Za-z_]*", s).group().upper()
        if not ({".": not re.match(r"\.os\b", s, re.IGNORECASE), "\\": bool(_PSQL_META.match(s)), ":": bool(_SQLCMD_DIRECTIVE.match(s))}[s[0]]
                if s[:1] in (".", "\\", ":") else not s or s[0] == "@" or s == "/" or head in _DESCRIBE_HEAD or head in _SQLPLUS_DIRECTIVE or (
                    head in _READ_HEAD and not (_NON_READ_WORD.search(s) or _SIDE_EFFECT_FN.search(s) or _SQL_DENY.search(s)))):
            bad.append(stmt)
    return bad


def _hosts(seg: _Seg, recon: list[list[str]] = ()) -> list[str]:
    """Every host / DSN candidate of a generic client: host flags and variables, URI / conninfo hosts on the line or in a reconnect
    (`recon`), the words a reconnect names after its database, a bare `$VAR` where a DSN stands, a `_DSN_POSITIONAL` client's first."""
    argv = seg.argv
    sql = set(_flag_values(argv, _SQL_VALUE_FLAGS)) | set(seg.scripts)
    out = list(_flag_values(argv, _HOST_FLAGS))
    out += [a.split("=", 1)[1] for a in seg.assigns if a.split("=", 1)[0] in _HOST_ENV]
    out += [w for w in argv[1:2] if seg.argv0 in _DSN_POSITIONAL and not w.startswith("-") and w not in sql]
    out += [w for i, w in enumerate(argv[1:], 1) if re.fullmatch(r"\$\{?\w+\}?", w) and w not in sql
            and (not argv[i - 1].startswith("-") or argv[i - 1] in _HOST_FLAGS)]
    out += [w for verb, *words in recon for w in words[0 if verb.lower() == ":connect" else 1:] if not re.search(r"=|://", w)]
    joined = " ".join([*(w for w in argv[1:] if w not in sql), *itertools.chain.from_iterable(recon)])
    out += re.findall(r"://(?:[^@/\s]*@)?([^:/?\s;]+)", joined)
    out += re.findall(r"(?i)\b(?:host|hostaddr|server|data source|addr)=([^;\s]+)", joined)
    return [re.split(r"[,:\\]", re.sub(r"^(?:tcp|np|lpc):|^\$\{?(\w+)\}?$", r"\1", h, flags=re.IGNORECASE), 1)[0].lower()
            for h in out if h]


def _check_opaque(segs: list[_Seg], cmd: str, cfg: GuardConfig) -> list[str]:
    """Constructs that only produce the statement at run time. Always: `eval`/`sh -c` on a `$`-built string, an opaque producer
    piped into a shell, a shell fed by process substitution. Where a client is involved: an unmodelled wrapper in front of the
    client, a variable in command position, any substitution, an expansion inside the SQL argument, an expanding heredoc, `xargs`."""
    violations, ctx = [], bool(_context(cmd, cfg))
    for s in segs:
        base, _, _, built = s.argv0, *_shell_runs(s)
        if built:
            violations.append(f"`{base}{' -c' if base in _SHELLS else ''}` on a runtime-built string; the guard cannot read what it would run")
        if base in _SHELLS:
            if s.opaque:
                violations.append(f"text piped into `{base}` comes from a decoder, download, program or expansion the guard cannot read")
            if "<(" in s.words:
                violations.append(f"`{base}` fed by process substitution; the guard cannot read what it would run")
        elif "xargs" in s.words and ctx:
            violations.append("`xargs` builds a client invocation from stdin; the guard cannot read the statement it would run")
        if not ctx:
            continue
        if s.argv and _expands(s.raw_of(s.argv[0]).rsplit("/", 1)[-1]):
            violations.append(f"variable `{s.raw_of(s.argv[0])[:40]}` in command position; the guard cannot tell which program it would "
                              "run (same class as `eval`)")
        elif s.argv and base not in _KNOWN and not _PYTHON.fullmatch(base) and (
                base not in _INERT or base == "find" and any(w.startswith(("-exec", "-ok")) for w in s.argv)) \
                and (wrapped := _context(" ".join(s.argv[1:]), cfg)):
            violations.append(f"unrecognised wrapper `{base}` in front of client `{wrapped[0]}`; the guard has no rule for `{base}`, so "
                              "it cannot tell how or where the client would run (run the client directly)")
        for i, (r, w, prev) in enumerate(zip(s.raw, s.words, ["", *s.words])):   # an expansion inside what is (or looks like) SQL
            if (prev in _SQL_VALUE_FLAGS or w.split("=", 1)[0] in _SQL_VALUE_FLAGS or re.search(r"[\s;]", w) or (
                    i >= 2 and s.words[i - 2] == "tools" and prev == "query")) and _expands(r) and not _REDIRECT_OP.fullmatch(prev):
                violations.append(f"shell expansion inside the SQL argument `{r[:60]}`; expand it in the command text so the guard can "
                                  "read the statement")
                break
        if any(re.fullmatch(r"\d*<<-?", w) and _expands(s.raw[i + 1]) for i, w in enumerate(s.words[:-1])):
            violations.append("unquoted heredoc expands `$`/backticks in its body; quote the delimiter (<<'EOF') or inline the values")
    if ctx and _expands(cmd, subst=True):
        violations.append("command/process substitution in a Databricks or legacy command; the statement is built at run time, so inline "
                          "it as text")
    if ctx and _FUNCTION_DEF.search(re.sub(r'"(?:[^"\\]|\\.)*"|\'[^\']*\'', " ", cmd)):
        violations.append("shell function defined in a Databricks or legacy command; the guard cannot follow what a call of it would run "
                          "(same class as `eval`)")
    return violations


def _check_sql_client(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    """A legacy-only client, or a generic one whose command names a legacy source, runs read shapes only; a generic client
    elsewhere may write when every host candidate (line and reconnect meta-commands) is allowlisted and the write resolves to an
    allowlisted catalog / database (default: the one database named by `-d`/`-D`/`--dbname`, URI, `dbname=` or a reconnect)."""
    base, tail = seg.argv0, " (legacy is read-only in every phase)"
    legacy, hits = base in _LEGACY_ONLY, _context(seg.text, cfg, legacy_only=True)
    if base in _LOADERS:
        return [f"`{base}` is a loader: nothing but reads ever runs against a legacy source" + tail]
    sql, unreadable = _sql_text(seg, root, [seg.argv[1]] if base == "bcp" and len(seg.argv) > 2 and "queryout" in seg.argv[2:4] else [])
    bad = [b.split("\n", 1)[0][:80] for b in [
        *([f"stdin from `{seg.opaque}`, a program or expansion the guard cannot read"] if seg.opaque else []),
        *([f"{base} (a migration tool: every run writes its target)"] if base in _WRITERS else []),
        *(["bcp ... in (loader)"] if base == "bcp" and "in" in seg.argv[1:4] else []), *_non_reads(sql)]]
    violations = [_UNREADABLE.format(who="legacy client", files=unreadable) + tail] if unreadable and (legacy or hits) else []
    if unreadable and not violations:
        bad.insert(0, f"script(s) {unreadable} the guard cannot read")
    if not bad:
        return violations
    recon = [re.sub(r"(?<!\S)-(?:\w(?:\s+[^-\s]\S*)?|\S\S+)|['\"]", " ", f"{v} {a}").split() for v, a in _RECONNECT.findall(sql)]
    if legacy:
        violations.append(f"non-read statement through a legacy-only client `{base}`: `{bad[0]}`" + tail)
    elif hits:
        violations.append(f"non-read statement against legacy source {hits}: `{bad[0]}`" + tail)
    elif not (hosts := _hosts(seg, recon)) or not all(h in cfg.target_hosts for h in hosts):
        violations.append(f"non-read statement through `{base}` to a host that is not a literal in target_hosts {cfg.target_hosts} "
                          f"(seen: {sorted(set(hosts))[:6]}; every host must be listed, an empty list blocks every write): `{bad[0]}`")
    else:
        line = "\n".join([" ".join(w for w in seg.argv[1:] if w not in set(_flag_values(seg.argv, _SQL_VALUE_FLAGS)) | set(seg.scripts)),
                          *map(" ".join, recon)])
        dbs = {_norm(a or b or c) for a, b, c in re.findall(
            r"(?i)(?<![-\w])dbname=([^;\s]+)|://[^/\s]*/([^/?\s;]+)|(?:^|\s)(?:-d|-D|--dbname|--database|\\c(?:onnect)?|connect|\\r)"
            r"[\s=](?!\S*(?:=|://))(\S+)", line)}
        violations += _catalog_violations(sql, cfg, dbs.pop() if len(dbs) == 1 else None, f"`{base}` client")
    return violations


def _catalog_violations(sql: str, cfg: GuardConfig, default: str | None, who: str) -> list[str]:
    """Writes must target an allowlisted catalog: three-part name (or `CATALOG c` / `SCHEMA c.s` right after the verb phrase;
    a qualified source further along -- CTAS, MERGE USING, INSERT SELECT -- never counts), else the last `USE CATALOG` (generic
    client: also `USE [DATABASE] x`) before the statement, else `default` (Databricks `--catalog`, a generic client's database);
    none resolving in a client (`who`; '' for SQL quoted in program text) -> block, as do `IDENTIFIER(<expr>)` / `EXECUTE IMMEDIATE <var>`."""
    allowed, in_dbx = set(cfg.catalogs), who == "Databricks client"
    cats = ("CATALOG", "DATABASE") if who and not in_dbx else ("CATALOG",)
    use = re.compile(_USE_CATALOG.format(c="|".join(cats), opt="?" * (len(cats) > 1)), re.IGNORECASE)
    text = _sql_view(_IDENTIFIER_LITERAL.sub(r"\1", sql))
    violations = []
    if in_dbx and _EXEC_IMMEDIATE_DYNAMIC.search(text):
        violations.append("EXECUTE IMMEDIATE on a non-literal; the statement is built at run time, so inline it as text")
    use_cats = [(m.start(), _norm(m.group(1))) for m in use.finditer(text)]
    for m in _WRITE.finditer(text):
        stmt = m.group() + text[m.end():].split(";", 1)[0]
        use_cat = next((c for pos, c in reversed(use_cats) if pos < m.start()), default)
        t = _TARGET.match(text, m.end())
        kind, parts = ((t.group(1) or "").upper(), 1 + t.group(3).count(".")) if t else ("", 0)
        cat = _norm(t.group(2)) if t and (kind in cats or (kind and parts >= 2) or parts >= 3) else None
        head = stmt.strip().split("\n", 1)[0][:80]
        if _IDENTIFIER_DYNAMIC.search(stmt):
            violations.append(f"IDENTIFIER(<non-literal>) names the target of a write at run time: `{head}`")
        elif who and re.match(_PERMISSION.format(c="|".join((*cats, _METASTORE))), stmt, re.IGNORECASE | re.DOTALL):
            violations.append(f"catalog / metastore lifecycle or permission change `{head}`; the allowlist authorizes object writes "
                              "inside a catalog, never grants or the containers themselves (those happen at STOP E)")
        elif cat is not None or use_cat is not None:
            c, how = (cat, f"to catalog(s) {[cat]}") if cat is not None else (use_cat, f"under USE CATALOG {use_cat!r}")
            if c not in allowed:
                violations.append(f"write {how} outside allowlist {sorted(allowed)}: `{head}`")
        elif who:
            violations.append(f"write with unresolvable catalog (not three-part qualified, no USE CATALOG, no database default) "
                              f"through a {who}: `{head}`")
    return violations


def _check_databricks(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    """The Databricks CLI (`_DBX_READ` shapes pass; a mutation needs an allowlisted securable) and the deploys --
    `databricks bundle deploy|run|destroy`, `dbt run|build|...` -- whose literal `-t/--target` must be in `bundle_targets`."""
    argv, base = seg.argv[1:], seg.argv0
    if base == "databricks" and any(w in ("--version", "-v", "-h", "--help", "version", "help") for w in argv):
        return []
    path, i = [], 0
    while i < len(argv):
        flag = argv[i] == "-" or _FLAG_WORD.fullmatch(argv[i])
        path += [] if flag else [argv[i]]
        i += 2 if flag and argv[i] in _DBX_VALUE_FLAGS else 1
    group, verb, args = *(path + ["", ""])[:2], path[2:]
    kind = f"dbt {argv[0]}" if base == "dbt" else f"databricks bundle {verb}" if group == "bundle" and verb in ("deploy", "run", "destroy") else ""
    if kind:
        t = m.group(1) if (m := _BUNDLE_TARGET.search(" " + " ".join(argv))) else ""
        bad = ("has no literal -t/--target" if not t else "is not a literal" if _expands(t) or not re.fullmatch(r"[\w.-]+", t) else
               "is a forbidden target (forbidden_bundle_targets); production deploys happen only at STOP E"
               if t.lower() in cfg.forbidden_bundle_targets else "is not in the list (exact, case-sensitive)" if t not in cfg.bundle_targets else "")
        return [f"`{kind}` target {t!r} {bad}; allowed bundle_targets {cfg.bundle_targets} (empty = every deploy blocks)"] if bad else []
    if (group, verb) == ("sql", "execute") or path[:4] == ["experimental", "aitools", "tools", "query"] or (
            not path and _flag_values(seg.argv, _SQL_VALUE_FLAGS)):
        sql, unreadable = _sql_text(seg, root, [w for w in path[4 if group == "experimental" else 2:] if w != "--"])   # positionals
        return ([_UNREADABLE.format(who="Databricks client", files=unreadable)] if unreadable else []) + _catalog_violations(
            sql, cfg, next(map(_norm, _flag_values(seg.argv, ("--catalog",))), None), "Databricks client")
    if group == "api" and verb != "get":
        if (m := _UC_PATH.search(" ".join(args))) and _norm(m.group(1).split(".")[0]) in cfg.catalogs:
            return []
    elif group == "fs" and verb in ("cp", "rm", "mkdir", "mkdirs"):
        remote = [a for a in args if a.startswith(("dbfs:", "/")) or "://" in a]
        cats = [_norm(m.group(1)) if (m := _VOLUME_PATH.match(a)) else None for a in remote]
        return [] if remote and len(args) >= (2 if verb == "cp" else 1) and all(c in cfg.catalogs for c in cats) and not any(map(_expands, args)) else [
            f"`databricks fs {verb}` with a remote path outside an allowlisted volume (dbfs:/Volumes/<catalog>/...; every remote end of a `cp`, no variables): {args}"]
    key = f"{group} {verb}"
    if key in _TOKEN_PRINTERS:
        return [f"`databricks {key}` prints the session's bearer token (credential exposure); `auth describe` shows the identity without it"]
    if key in _LIFECYCLE:
        return [(f"`databricks {key}` on {' '.join(args) or '<securable>'!r}: the allowlist authorizes object writes inside a "
                 "catalog, never catalog lifecycle or permissions (those happen at STOP E)")]
    if key in _CLI_CATALOG_ARG:
        name = (args + ["", ""])[_CLI_CATALOG_ARG[key]]
        return [] if name and _norm(name.split(".")[0]) in cfg.catalogs else [f"CLI mutation of securable {name!r} outside allowlist {sorted(cfg.catalogs)}"]
    if verb in _DBX_READ.get(group, ()):
        return []
    return [f"`databricks {group} {verb}`".rstrip() + " is not in the guard's read allowlist (fail closed); reads are "
            "list/get shapes, writes go through an allowlisted securable or the migration workflow"]


def _check_rest(seg: _Seg) -> list[str]:
    """REST calls to a Databricks host (`$DATABRICKS_HOST`, a *.databricks.com / *.azuredatabricks.net name, an alias variable the
    command set to one, or -- fail closed -- an unresolvable host in front of a Databricks API path) are GET/HEAD without a body."""
    argv = seg.argv[1:]
    if not any(_DBX_HOST.search(w) or (_DBX_API_PATH.search(w) and "$" in w and _expands(seg.raw_of(w))) for w in argv):
        return []
    method, body = "GET", False
    for i, w in enumerate(argv):
        nxt = argv[i + 1] if i + 1 < len(argv) else ""
        if seg.argv0 == "curl":
            if w in ("-X", "--request") or w.startswith(("-X", "--request=")):
                method = (nxt if w in ("-X", "--request") else w.split("=", 1)[-1][2 if w.startswith("-X") else 0:]).upper()
            elif w.startswith(("--data", "--json", "--form", "--upload-file")):
                body = True
            elif w.startswith("-") and not w.startswith("--"):   # bundled short options: `-sSd'{}'`, `-F`, `-T`
                body = body or next((ch for ch in w[1:] if ch in _CURL_VALUE_SHORT), "") in ("d", "F", "T")
        elif seg.argv0 == "wget":
            if w.startswith("--method"):
                method = (w.split("=", 1)[1] if "=" in w else nxt).upper()
            elif w.startswith(("--post-", "--body-")):
                body = True
        elif w.upper() in _HTTP_METHODS:
            method = w.upper()
        elif not w.startswith("-") and re.match(r"^[\w.-]+:?=(?!=)", w):
            body = True
    return [] if method in ("GET", "HEAD") and not body else [
        f"REST {method}{' with a request body' if body else ''} to a Databricks workspace through `{seg.argv0}`; only GET without a body passes"]


def _check_identity(segs: list[_Seg], cfg: GuardConfig | None = None) -> list[str]:
    """The session runs as the doctor-verified migration principal only: no credential / endpoint / profile variable is set,
    exported or unset (the shell persists it), no HOME/XDG_CONFIG_HOME or profile flag on a client's own segment, and no DSN /
    secret name the allowlist trusts (`target_hosts`, `legacy_sources`) reassigned: the name stands for the doctor-verified value."""
    violations = []
    tail = "; the session runs as the doctor-verified migration principal only"
    trusted = {n.lower() for n in [*cfg.target_hosts, *cfg.legacy_sources] if re.fullmatch(r"[A-Za-z_]\w*", n)} if cfg else set()
    for s in segs:
        client = s.argv0 in _IDENTITY_CLIENTS
        persistent = s.argv0 in ("export", "unset", "declare", "typeset", "setenv") or not s.argv   # outlives the command
        names = [a.split("=", 1)[0] for a in s.assigns] + ([w.split("=", 1)[0] for w in s.argv[1:] if not w.startswith("-")] if persistent else [])
        for n in names:
            if n.lower() in trusted:
                violations.append(f"`{n}` is a name the allowlist trusts (target_hosts / legacy_sources); "
                                  f"{'unsetting' if s.argv0 == 'unset' else 'assigning'} it would make that name stand for a different "
                                  "endpoint" + tail)
            elif _IDENTITY_VAR.match(n) and (client or persistent):
                violations.append(f"identity swap: `{n}=` {'around `' + s.argv0 + '`' if client else 'changed for the session'}" + tail)
            elif n in _CONFIG_HOME_VAR and client:
                violations.append(f"identity swap: `{n}=` moves the Databricks config lookup around `{s.argv0}`" + tail)
        if client and s.argv0 != "spark-sql":
            flags = [w for w in s.argv[1:] if w in ("--profile", "-p", "--host") or w.startswith(("--profile=", "--host="))]
            if flags or s.argv[1:3] == ["auth", "login"] or s.argv[1:2] == ["configure"]:
                violations.append(f"identity swap through `{s.argv0} {' '.join(flags or s.argv[1:3])}`" + tail)
    return violations


def _check_python(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    """Literal SQL handed to an executor inside a program the command runs (`-c`, heredoc, script file)."""
    argv = seg.argv
    texts = [*_flag_values(argv, ("-c",)), *seg.heredocs]
    if "-m" not in argv and not texts and (script := next((w for w in argv[1:] if w.endswith(".py")), None)):
        body = _read_script(script, root, seg.at)
        if body is None and seg.argv0 == "spark-submit":
            return [f"spark-submit script {script!r} cannot be read; the guard cannot clear a Spark job it cannot inspect"]
        texts.append(body or "")
    text = "\n".join(texts)
    hits = _context(seg.text + " " + text, cfg, legacy_only=True)
    violations = []
    for m in _PY_LITERAL.finditer(text):
        if not hits:
            violations += _catalog_violations(m.group(2), cfg, None, "")
        elif bad := _non_reads(m.group(2)):
            violations.append(f"non-read statement against legacy source {hits} in a program: `{bad[0][:80]}` "
                              "(legacy is read-only in every phase)")
    return violations


# ---------------------------------------------------------------- writes: .migration/, the credential store, the running guard

def _touch(path: str, at: str | None, root: Path) -> str:
    """How a literal path relates to what commands must not write: for the protected part of `.migration/` 'inside' (literally or
    through a glob / brace that could match a protected entry), 'self', 'above' (`.`, `..`, `$PWD`, `~`, an absolute path at or
    above the root); 'unresolved' for a path built at run time; 'identity' for the Databricks credential store; 'guard' /
    'guard-above' for the running guard's tree (symlinks resolved) -- a relative name that could be it counts whatever `at` is."""
    p = os.path.expanduser(re.sub(r"\{[^{}]*(?:,|\.\.)[^{}]*\}", "*", re.sub(r"\$\{?PWD\}?|\$\(pwd\)", ".", path)))
    if p in ("", "-") or p.isdigit():
        return ""
    if _expands(p, subst=True) or _expands(p):
        return "unresolved"
    p = os.path.normpath(p if p.startswith("/") else os.path.join(at or "", p))
    parts = [x for x in p.split("/") if x not in ("", ".")]

    def like(name: str, part: str, spelled: bool = False) -> bool:   # a glob counts when it could match (`spelled`: and spells part of) the name
        return part == name or (bool(re.search(r"[*?\[]", part)) and fnmatch.fnmatchcase(name, part)
                                and (not spelled or any(len(w) >= 3 and w in name for w in re.findall(r"\w+", part))))

    for i, part in enumerate(parts):   # a literal `.migration` anywhere; a glob only at the top of the workspace, where the ledger lives
        if part == ".migration" or (i == 0 and not p.startswith("/") and like(".migration", part)):
            rest = parts[i + 1:]
            return "self" if not rest else "" if len(rest) > 1 and rest[0] in ("recon", "waves") else "inside"
    rel = [x for x in os.path.relpath(p, root).split("/") if x not in ("", ".")] if p.startswith("/") else parts
    if all(x == ".." for x in rel):
        return "above"
    name = parts[-1] if parts else ""
    if _IDENTITY_FILE.search(re.sub(r"\$\{?HOME\}?", "~", p)) or (name.startswith(".") and like(".databrickscfg", name)):
        return "identity"
    if not p.startswith("/"):
        named = parts[next((i for i, x in enumerate(parts) if x != ".."), len(parts)):]
        if named and (like("hooks", named[0], True) or like(_GUARD_FILE, named[-1], True)
                      or (len(named) == 1 and like("hooks.json", named[0], True))):
            return "guard"
    real = os.path.realpath(p if p.startswith("/") else os.path.join(root, p)).strip("/").split("/")
    tree = [*_GUARD_TREE.parts[1:], "hooks"]
    for i, part in enumerate(real[:len(tree)]):
        if not (like(tree[i], part) or (i == len(tree) - 1 and like("hooks.json", part))):
            return ""
    return "guard" if len(real) >= len(tree) else "guard-above"


def _patch_texts(s: _Seg, files: list[str], root: Path, how: str) -> list[str]:
    """Violations of a patch applier: the guard reads every patch it is given and blocks one it cannot read or one touching `.migration/`."""
    if s.opaque or (not files and not s.stdin and not s.heredocs):
        return [f"`{how}` on a patch the guard cannot read (stdin from a program or the terminal); write it to a file first"]
    out = []
    for f in files:
        body = _read_script(f, root, s.at)
        if body is None:
            out.append(_UNREADABLE.format(who=f"`{how}`", files=[f]))
        elif _MIGRATION_PATH.search(body):
            out.append(f"`{how}` of a patch that touches .migration/ ({f}); ledgers and the allowlist change only through a recorded decision")
    if _MIGRATION_PATH.search("\n".join([*s.stdin, *s.heredocs])):
        out.append(f"`{how}` of a patch that touches .migration/; ledgers and the allowlist change only through a recorded decision")
    return out


def _git_reads(gargv: list[str]) -> bool:
    """A git sub-command that rewrites neither the working copy nor the repository: what may run on the guard's own tree."""
    verb, rest = gargv[0], gargv[1:]
    flags, ops = [w for w in rest if w.startswith("-")], [w for w in rest if not w.startswith("-")]
    if verb in _GIT_LIST_FORMS:
        return (ops[0] if ops else "") in _GIT_LIST_FORMS[verb]
    if verb in ("branch", "tag"):
        known = _GIT_LIST_FLAGS + (_GIT_SHOW_FLAGS if verb == "branch" else ())
        return all(w in known or re.fullmatch(r"--(?:sort|format|color|abbrev|column)(?:=.*)?|-n\d*", w) for w in flags) and (
            not ops or any(w in _GIT_LIST_FLAGS for w in flags))
    if verb == "config":
        return any(w in _GIT_CONFIG_READS for w in flags) and not any(
            w == "-e" or w.startswith(("--unset", "--add", "--replace", "--edit", "--rename", "--remove")) for w in flags)
    return verb in _GIT_READS


def _git_writes(s: _Seg, here: str, root: Path, out: list[str]) -> list[tuple[str, str, tuple[str, ...], bool, str | None]]:
    """Git's writes (see `_writes` for the tuple): on the running guard's tree anything but a read sub-command; the destination of
    `clone` / `init` / `worktree add`; `checkout` / `restore` / `rm` / `mv` operands; patches it applies. Git runs in the segment's
    directory (the guard process's own when nothing moved it) moved by `GIT_WORK_TREE=` / `GIT_DIR=`, `-C`, `--work-tree` / `--git-dir`."""
    argv, env, i, tree = s.argv, dict(a.split("=", 1) for a in s.assigns), 1, {}
    d = env.get("GIT_WORK_TREE") or _GIT_DIR_VALUE.sub("", env.get("GIT_DIR", ""))
    delta: str | None = _join("", d) if d else ""
    while i < len(argv) and argv[i].startswith("-"):
        key, _, d = argv[i].partition("=")
        if key in ("-C", "-c", "--git-dir", "--work-tree", "--namespace") and not d:
            d, i = (argv[i + 1] if i + 1 < len(argv) else ""), i + 2
        else:
            key, d = ("-C", key[2:]) if key.startswith("-C") and len(key) > 2 else (key, d)
            i += 1
        if key == "-C" and d and delta is not None:
            delta = _join(delta, d)
        elif key in ("--work-tree", "--git-dir") and d:
            tree[key] = d
    d = tree.get("--work-tree") or _GIT_DIR_VALUE.sub("", tree.get("--git-dir", ""))
    delta = _join(delta, d) if d and delta is not None else delta
    run_in, at = (None, None) if delta is None else (_join(_join(here, s.at or "."), delta), _join(s.at, delta))
    gargv = argv[i:]
    verb, gops = (gargv[0] if gargv else ""), [w for w in gargv[1:] if not w.startswith("-")]
    w = [(".", f"git {verb}", (), True, run_in)] if gargv and not _git_reads(gargv) else []
    if verb in ("clone", "init") or (verb == "worktree" and gops[:1] == ["add"]):
        w += [(o, f"git {verb}", _IN, True, at) for o in (gops[-1:] if verb != "worktree" else gops[1:2])]
    if verb in _GIT_DISCARDS and (not (x := _GIT_DISCARDS[verb]) or any(o in x for o in gargv[1:] + [(gops[:1] or [""])[0]])):
        out.append(f"`git {verb}` rewrites the working copy across the workspace, .migration/ included; revert a ledger only "
                   "through a recorded decision")
    elif verb in ("checkout", "restore", "rm", "mv"):
        w += [(o, f"git {verb}", _ALL if verb in ("checkout", "restore") else _IN, False, at) for o in gops]
    elif verb in ("apply", "am"):
        out += _patch_texts(s, gops + s.scripts, root, f"git {verb}")
    return w


def _writes(s: _Seg, root: Path, here: str, out: list[str]) -> list[tuple[str, str, tuple[str, ...], bool, str | None]]:
    """(path, how, the `.migration` relations that block, destructive, directory) for every operand the segment may write: `>`
    redirections, output flags, interpreter code with a write call, `find` with an action, archive extraction, a SQL client's output
    files, the operands of any head not a pure reader (`_READERS`) or run in place (`_IN_PLACE`). Violations without a path go to `out`."""
    base, argv, at = s.argv0, s.argv, s.at
    ops = [w for w in argv[1:] if not w.startswith("-")]
    values = ops + [w.split("=", 1)[1] for w in argv[1:] if "=" in w]   # `dd of=`, `--output=`
    inplace = any(rx.fullmatch(w) for w in argv[1:] for rx in _IN_PLACE.get(base, ())) and not any(w in ("--check", "--diff") for w in argv)
    w = [(f, f"{op} {f}", _IN, False, at) for op, f in s.redirects() if ">" in op]
    w += [(v, f"{base} {flag}", _IN, False, at) for flag, v in itertools.pairwise(argv) if flag in _OUTPUT_FLAGS]
    if base == "git":
        return w + _git_writes(s, here, root, out)
    if base == "patch":
        out += _patch_texts(s, s.scripts, root, "patch")
    elif _PYTHON.fullmatch(base) or base in ("perl", "ruby", "node") and not inplace:
        text = "\n".join([*_flag_values(argv, ("-c", "-e")), *s.heredocs, *s.stdin])
        if _PY_WRITE.search(text):
            w += [(p, f"{base} script", _IN, False, at)
                  for p in [*_MIGRATION_PATH.findall(text), *_PATH_LITERAL.findall(text), *_GUARD_LITERAL.findall(text)]]
            w += [(m.group(1) if m.group(1) is not None else ".", f"{base} rmtree", _ALL, False, at) for m in _RMTREE.finditer(text)]
    elif base == "find" and any(x in ("-delete", "-exec", "-execdir", "-ok", "-okdir", "-fls") or x.startswith("-fprint") for x in argv):
        w += [(o, "find with an action", _ALL, False, at) for o in ops]
    elif base in ("tar", "bsdtar") and (any(re.match(r"-?[a-zA-Z]*x", x) for x in argv[1:2]) or "--extract" in argv or "--get" in argv):
        w += [(d, f"{base} extract into", _ALL, False, at) for d in _flag_values(argv, ("-C", "--directory")) or ["."]]
    elif base == "unzip" and not any(x in argv for x in ("-l", "-t", "-p", "-z", "-Z")):
        w += [(d, "unzip into", _ALL, False, at) for d in _flag_values(argv, ("-d",)) or ["."]]
    elif base in _GENERIC or base in _LEGACY_ONLY:
        sql = " ".join([*_flag_values(argv, _SQL_VALUE_FLAGS), *s.heredocs, *s.stdin, s.herestring or ""])
        w += [(p, f"{base} output", _IN, False, at) for p in [*values, *_SQL_OUT_PATH.findall(sql)]]
    elif base not in _READERS or inplace:
        recursive = base in _RECURSIVE_HEADS and any(re.fullmatch(r"-[a-zA-Z]*[rR][a-zA-Z]*", x) or x in ("--recursive", "--delete")
                                                    for x in argv[1:])
        if "xargs" in s.words and any(_touch(x, at, root) for p in s.feeds for x in p.words):
            out.append(f"`xargs {base}` on names listed from .migration/; ledgers and the allowlist change only through a recorded decision")
        w += [(o, base, _ALL if recursive else _IN, base in _DESTRUCTIVE, at)
              for o in (values[-1:] if base in _WRITE_LAST_OPERAND else values)]
    return w


def _check_integrity(segs: list[_Seg], root: Path, here: str = "") -> list[str]:
    """Nothing but the recon harness and the workflow writes under `.migration/`, nothing writes the Databricks CLI's credential
    store, nothing edits, disables or removes the running guard: every write `_writes` finds is placed by `_touch` (`here`: the
    guard process's own directory, where git runs when neither the event nor the command says)."""
    violations: list[str] = []
    for s in segs:
        for path, how, kinds, destructive, at in _writes(s, root, here, violations):
            kind = _touch(path, at, root)
            if kind in kinds:
                violations.append(f"`{how}` writes `{path}` under .migration/ (only .migration/recon/ and .migration/waves/ are "
                                  "written by commands; ledgers and the allowlist change only through a recorded decision)")
            elif kind == "unresolved" and destructive:
                violations.append(f"`{how}` writes `{path}`, a destination built at run time that the guard cannot resolve; spell the "
                                  "path out (a variable the command itself sets is followed)")
            elif kind == "identity":
                violations.append(f"`{how}` writes `{path}`, the Databricks CLI's credential store; the session runs as the "
                                  "doctor-verified migration principal only")
            elif kind == "guard" or (kind == "guard-above" and (destructive or "above" in kinds)):
                violations.append(f"`{how}` on `{path}` inside the running guard's plugin tree ({_GUARD_TREE}); the hook is never edited, "
                                  "disabled or removed from a session (a block is a finding to report)")
    return violations


# ---------------------------------------------------------------- verdict

def _analyse(text: str, cfg: GuardConfig, root: Path, depth: int = 0, at: str | None = "") -> tuple[list[_Seg], list[str]]:
    """Segments of the text and of every shell script it runs, plus the violations of the text itself (opaque execution,
    unreadable scripts). `at` is the directory the text starts in (the event's cwd; '' for the workspace root)."""
    text = re.sub(r"\\\r?\n", " ", text)
    segs = _segments(text, at=at)
    violations = _check_opaque(segs, text, cfg)
    for seg in list(segs):
        body = _read_script(f, root, seg.at) if (f := _shell_runs(seg)[1]) is not None else ""
        if body is None:
            violations.append(f"shell script(s) {[f]} the command would run cannot be read in full (missing, unreadable or over "
                              f"{_MAX_SCRIPT_BYTES >> 20} MiB); the guard cannot clear what it cannot read")
        elif body and depth < 4:
            more, nested = _analyse(body, cfg, root, depth + 1, seg.at)
            segs += more
            violations += nested
    return segs, violations


def evaluate(command: str, cfg: GuardConfig, root: Path | None = None, cwd: str = "", here: str = "") -> Verdict:
    """The verdict on a command in the workspace at `root`, run from `cwd` (the event's; '' for the root) by a guard process in `here`."""
    root = root or Path.cwd()
    violations: list[str] = []
    if m := _PROBE.search(command):
        violations.append(f"`{m.group()}` is the factory-doctor's hook probe; it always blocks so the doctor can tell the "
                          "hook is loaded without touching Databricks")
    segs, found = _analyse(command, cfg, root, at=cwd)
    violations += found + _check_identity(segs, cfg) + _check_integrity(segs, root, here)
    for seg in segs:
        base = seg.argv0
        if base == "databricks" or (base == "dbt" and seg.argv[1:2] and seg.argv[1] in _DBT_RUNS):
            violations += _check_databricks(seg, cfg, root)
        elif base in ("spark-sql", "dbsqlcli"):
            sql, unreadable = _sql_text(seg, root)
            violations += ([_UNREADABLE.format(who="Databricks client", files=unreadable)] if unreadable else []) + _catalog_violations(
                sql, cfg, None, True)
        elif base == "dbx-recon":
            violations += [f"--target-catalog {_norm(c)!r} outside allowlist {sorted(cfg.catalogs)}"
                           for c in _flag_values(seg.argv, ("--target-catalog",)) if _norm(c) not in cfg.catalogs]
        elif base in _REST_CLIENTS:
            violations += _check_rest(seg)
        elif base in _LEGACY_ONLY or base in _GENERIC:
            violations += _check_sql_client(seg, cfg, root)
        elif _PYTHON.fullmatch(base):
            violations += _check_python(seg, cfg, root)
    return Verdict.of(list(dict.fromkeys(violations)), cfg)


def _cd_targets(cmd: str) -> list[str | None]:
    """Directories the command changes into (`cd d`, `pushd d`), in order; None for one the guard cannot resolve (`cd -`, a variable)."""
    targets = [os.path.expandvars(next((w for w in s.argv[1:] if not (w.startswith("-") and len(w) > 1)), "~"))
               for s in _segments(cmd) if s.argv0 in ("cd", "pushd")]
    return [None if t == "-" or _expands(t) else os.path.expanduser(t) for t in targets]


def evaluate_with_workdirs(command: str, cfg: GuardConfig, root: Path, cwd: str = "", here: str = "") -> Verdict:
    """`evaluate` against the starting workspace and every workspace the command `cd`s into: a write must be allowed by each
    allowlist involved, and a client command moving to a directory the guard cannot resolve is not clearable."""
    first = evaluate(command, cfg, root, cwd, here)
    violations = first.violations or ([first.reason] if first.decision == "block" else [])
    seen = {cfg.path}
    cwd = Path(cwd) if cwd else root
    for target in _cd_targets(command):
        if target is None:
            if _context(command, cfg):
                violations.append("command changes to a directory the guard cannot resolve before running a Databricks or "
                                  "legacy client; the allowlist in force there is unknown")
            break
        cwd = (cwd / target).resolve()
        try:
            other = load_config(cwd)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            violations.append(f"cannot read {CONFIG_REL} for {cwd}: {exc}")
            continue
        if other is not None and other.path not in seen:
            seen.add(other.path)
            violations += [f"[{other.path}] {x}" for x in evaluate(command, other, cwd).violations]
    return Verdict.of(list(dict.fromkeys(violations)), cfg)


def main(stdin_text: str | None = None) -> int:
    raw = stdin_text if stdin_text is not None else sys.stdin.read()
    try:
        event = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        return 0
    tool_input = event.get("tool_input") or {} if isinstance(event, dict) else {}
    command = tool_input.get("command") if isinstance(tool_input, dict) else None
    if not isinstance(command, str) or not command.strip():
        return 0
    cwd = c if isinstance(c := event.get("cwd"), str) and c.startswith("/") else ""
    root = Path(os.environ.get("CLAUDE_PROJECT_DIR") or os.environ.get("DEVIN_PROJECT_DIR") or os.getcwd())
    try:
        cfg = load_config(root)
    except (OSError, ValueError, json.JSONDecodeError) as exc:   # a broken allowlist is itself a setup violation: refuse rather than guess
        verdict = Verdict("block", f"dbx-migration-factory guard: cannot read {CONFIG_REL}: {exc}")
    else:
        if cfg is None:
            return 0
        verdict = evaluate_with_workdirs(command, cfg, root, cwd, os.getcwd())
    if verdict.reason:
        print(json.dumps({"decision": verdict.decision, "reason": verdict.reason}))
    if verdict.decision == "block":
        print(verdict.reason, file=sys.stderr)
    return 2 if verdict.decision == "block" else 0


if __name__ == "__main__":
    sys.exit(main())
