#!/usr/bin/env python3
"""Command guard for migration-safe shell and client operations.

The public API evaluates commands, edits, and working-directory policy.
"""
from __future__ import annotations

import fnmatch
import io
import itertools
import json
import os
import re
import shlex
import sys
import tokenize
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
_KNOWN = frozenset((*_DBX_CLIENTS, *_LEGACY_ONLY, *_GENERIC, *_REST_CLIENTS, *_SHELLS, *_DECLARERS, *_FIXERS, "dbt", "eval", "source",
                    ".", "docker", "podman", "nerdctl", "kubectl", "perl", "ruby", "node", "java", "patch", "xargs", "for", "select"))
_PREFIX_VALUE_FLAGS = {
    "sudo": ("-u", "-g", "-C", "-h", "-p", "-r", "-t", "-U", "-D", "-T"), "doas": ("-u", "-C"), "nohup": (),
    "env": ("-u", "--unset", "-C", "-S", "--split-string", "--chdir"), "nice": ("-n", "--adjustment"),
    "ionice": ("-c", "-n", "-p", "-P", "-u", "--class", "--classdata"), "timeout": ("-s", "--signal", "-k", "--kill-after"),
    "stdbuf": ("-i", "-o", "-e", "--input", "--output", "--error"),
    "ssh": ("-p", "-i", "-o", "-l", "-F", "-J", "-L", "-R", "-D", "-b", "-m", "-c", "-E", "-e", "-I", "-Q", "-S", "-W", "-B"),
    "docker": ("-e", "--env", "-u", "--user", "-w", "--workdir", "--name", "-v", "--volume", "-p", "--publish", "--network",
               "--entrypoint", "--platform"), "kubectl": ("-c", "--container", "-n", "--namespace"),
}
_RESERVED_PREFIXES = frozenset(("do", "then", "else", "elif", "if", "while", "until", "!", "time"))
_ASSIGN = re.compile(r"^[A-Za-z_]\w*=")
_ENV_DEFAULTS = {"HOME": "~", "TMPDIR": "/tmp"}
_SHELL_VAR = re.compile(r"\$\{(\w+)\}|\$(\w+)")
_READERS = frozenset(("cat", "less", "more", "head", "tail", "grep", "rg", "egrep", "fgrep", "zgrep", "diff", "cmp", "ls", "stat", "wc",
                      "file", "jq", "yq", "md5sum", "sha1sum", "sha256sum", "sort", "uniq", "cut", "tr", "awk", "gawk", "mawk", "sed",
                      "tree", "du", "echo", "printf", "test", "[", "[[", "cd", "pushd", "popd", "dirname", "basename", "realpath",
                      "readlink", "which", "type", "find", "tar", "unzip", "column", "nl", "od", "xxd", "strings", "pytest", "ruff",
                      "true", "false", "sleep", "date", "env", "printenv", "set", "export", "unset", "for", "select", "dbx-recon",
                      "databricks"))
_INERT = _READERS | frozenset(("man", "info", "whereis", "whatis", "apt", "apt-get", "brew", "pip", "pip3", "yum", "dnf", "apk", "conda",
                               "mamba", "git", "help", "hash", "alias", "unalias", "complete", "apropos", "tldr", "locate", "ldd",
                               "pg_dump", "pg_dumpall", "mysqldump"))
_IDENTITY_VAR = re.compile(r"^(?:DATABRICKS_\w+|ARM_CLIENT_\w+|ARM_TENANT_ID|AZURE_\w+|GOOGLE_CREDENTIALS|GOOGLE_APPLICATION_CREDENTIALS)$")
_CONFIG_HOME_VAR = ("HOME", "XDG_CONFIG_HOME")   # where ~/.databrickscfg is looked up
_FUNCTION_DEF = re.compile(r"(?:^|[;&|\n{}()]\s*)(?:function\s+[\w.-]+|[\w.-]+\s*\(\s*\))")
_SQL_VALUE_FLAGS = ("-c", "-Q", "-q", "-e", "--query", "--sql", "--statement", "--execute", "--command")
_SCRIPT_FLAGS = ("-f", "-i", "--file", "--input")
_HOST_FLAGS = ("-S", "-h", "-H", "--host", "--server", "--hostname", "--url", "-url", "--dsn")
_HOST_ENV = ("PGHOST", "PGHOSTADDR", "PGSERVICE", "MYSQL_HOST", "SQLCMDSERVER")
_DSN_POSITIONAL = ("isql", "usql", "pgloader")   # first positional is the DSN / URL, not a database
_RECONNECT = re.compile(r"(?im)^[ \t]*(\\c(?:onnect)?|connect|\\r|:connect)[ \t]+([^;\n]*)")
_RUN_FILE = re.compile(r"(?<!\S)@(\S+)|^\s*\.RUN\s+FILE\s*=?\s*(\S+)", re.IGNORECASE | re.MULTILINE)
_DYNAMIC_SQL_EXECUTOR = (r"(?:\bEXEC(?:UTE)?\s+IMMEDIATE|\bsp_executesql|\bEXEC(?:UTE)?\s*\(|"
                         r"\.(?:execute|executemany|sql|run_query|execute_statement)\s*\(|\bstatement\s*=)")
_DYNAMIC_SQL_CALLER = re.compile(_DYNAMIC_SQL_EXECUTOR + r"\s*N?\s*$", re.IGNORECASE)
_PY_LITERAL = re.compile(_DYNAMIC_SQL_EXECUTOR + r"\s*[rbuf]*(['\"]{3}|['\"])(.*?)\1", re.IGNORECASE | re.DOTALL)
_PY_WRITE = re.compile(r"""['"](?:[wax]\+?|\+?>>?|\+<)['"]|\.write\w*\(|json\.dump\(|os\.(?:remove|unlink|rename|replace|chmod|rmdir|makedirs|mkdir)\(|"""
                       r"""shutil\.|\.(?:unlink|rename|rmdir|mkdir|touch|chmod)\(|\bunlink\b|\bwriteFile\w*\(""")
_MIGRATION_PATH = re.compile(r"[^\s'\"()]*\.migration(?:/[^\s'\"()]*)?")
_DECISION_ROW = re.compile(r"(?m)^\s*(?:\|\s*|#{1,6}\s*)?(D-[A-Za-z0-9][\w.-]*)\b")
_DECISION_ID = re.compile(r"D-[A-Za-z0-9][\w.-]*")
_WRITE_OBJECT = re.compile(
    r"(?is)^\s*(?:DELETE\s+FROM|INSERT\s+INTO|MERGE\s+INTO|UPDATE(?:\s+TOP\s*\([^)]*\)(?:\s+PERCENT)?|\s+STATISTICS|\s+(?:ONLY|LOW_PRIORITY|IGNORE))*|TRUNCATE(?:\s+TABLE)?|"
    r"(?:CREATE|ALTER|DROP)(?:\s+\w+)*?\s+INDEX(?:\s+IF\s+(?:NOT\s+)?EXISTS)?\s+[\w.$\"\[\]`]+\s+ON(?:\s+ONLY)?|"
    r"CREATE(?:\s+OR\s+REPLACE)?\s+(?:\w+\s+)*?"
    r"(?:TABLE|VIEW|PROCEDURE|FUNCTION|TRIGGER|SEQUENCE|SCHEMA)(?:\s+IF\s+NOT\s+EXISTS)?|"
    r"(?:ALTER|DROP)(?:\s+\w+)*?\s+"
    r"(?:TABLE|VIEW|PROCEDURE|FUNCTION|TRIGGER|SEQUENCE|SCHEMA)(?:\s+IF\s+EXISTS)?|"
    r"(?:GRANT|REVOKE)\b.*?\bON\s+ALL\s+\w+\s+IN\s+SCHEMA|"
    r"GRANT\b.*?\bON(?:\s+\w+)?|REVOKE\b.*?\bON(?:\s+\w+)?)\s+([\w.$\"\[\]`]+)"
)
_WRITE_OBJECT_NEXT = re.compile(r"\s*,\s*([\w.$\"\[\]`]+)")
_AUTHORIZED = "authorized: decision "
_LEGACY_TAIL = " (legacy is read-only in every phase)"

class _Legacy(str):
    """A violation on a legacy source; never downgraded by warn mode."""

_RMTREE = re.compile(r"rmtree\(\s*(?:['\"]([^'\"]*)['\"]|(os\.getcwd\(\)|Path\.cwd\(\)|Path\(\s*(?:['\"]\.?['\"])?\s*\)))")
_SQL_OUT_PATH = re.compile(r"(?i)(?:\bTO\s+|\\[ow]\s+|:out\s+|\bSPOOL\s+|\bFILE\s*=\s*)'?([^\s'\"]*\.migration(?:/[^\s'\"]*)?)")
_SQL_OPAQUE = re.compile(r"--[^\n]*|/\*.*?(?:\*/|\Z)|'(?:[^']|'')*(?:'|\Z)", re.DOTALL)
_CLOUD_FAMILY = {
    "aws": (("AWS_",), re.compile(r"(?<![\w.-])(?:aws|boto3|botocore|s3fs|awscli)(?![\w-])|\bs3://")),
    "azure": (("AZURE_", "AZURITE_"), re.compile(r"(?<![\w.-])az(?![\w-])|azure[.-]storage|\babfss://")),
    "gcp": (("GOOGLE_", "GCLOUD_", "GCS_", "CLOUDSDK_", "STORAGE_EMULATOR_HOST", "PUBSUB_EMULATOR_HOST",
              "FIRESTORE_EMULATOR_HOST"),
             re.compile(r"(?<![\w.-])(?:gcloud|gsutil)(?![\w-])|google(?:[.-]cloud[.-]storage|\.cloud\s+import\s+storage)|\bgs://")),
}

_READ_HEAD = ("SELECT", "WITH", "SET", "USE", "DECLARE")
_DESCRIBE_HEAD = ("SHOW", "DESC", "DESCRIBE", "HELP", "GO")
_SQLPLUS_DIRECTIVE = ("SPOOL", "PROMPT", "DEFINE", "COLUMN", "WHENEVER", "EXIT", "QUIT", "TTITLE", "BTITLE", "BREAK",
                      "COMPUTE", "TIMING", "REM", "REMARK", "PAUSE", "CLEAR")
_DIRECTIVE_LINE = re.compile(r"^\s*(?:[.\\:@/]|GO\b|(?:" + "|".join(_SQLPLUS_DIRECTIVE) + r")\b)", re.IGNORECASE)
_PSQL_META = re.compile(r"\\(?:d\S*|l\S*|x|q|\?|h\S*|timing|echo|pset|set|unset|c(?:onnect)?|conninfo|encoding|z|sf|sv|a|t|H|C|f)\b")
_SQLCMD_DIRECTIVE = re.compile(r":(?:setvar|exit|quit|on\s+error|help|list\w*|reset|xml|error|out|perftrace|connect)\b", re.IGNORECASE)
_SQL_PREFIX = re.compile(r"(?:EXPLAIN\b(?:\s+(?:ANALYZE|VERBOSE|PLAN|EXTENDED|CODEGEN|COST|FORMATTED|QUERY\s+PLAN)\b|\s*\([^)]*\)|\s+FOR\b)*\s*"
                         r"|LOCK(?:ING)?\s+(?:ROW|(?:TABLE|DATABASE|VIEW)\s+\S+)?\s*FOR\s+(?:ACCESS|READ)\b(?:\s+(?:NOWAIT|MODE))*\s*)*", re.IGNORECASE)
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
_SIDE_EFFECT_FN = re.compile(
    r"\b(?:nextval|setval|set_config|pg_sleep(?:_for|_until)?|pg_terminate_backend|pg_cancel_backend|pg_reload_conf|"
    r"pg_rotate_logfile|pg_(?:try_)?advisory_\w*lock\w*|lo_(?:import|export|unlink|creat|create|put|truncate)|dblink\w*|"
    r"opendatasource)\s*\(|\b(?:sys\.)?(?:dbms|utl)_\w+\.\w+\b|\.NEXTVAL\b", re.IGNORECASE)

_UC_READ = {"list", "get", "exists"}
_DBX_READ = {
    "current-user": {"me"}, "catalogs": _UC_READ, "schemas": _UC_READ, "tables": _UC_READ, "volumes": _UC_READ, "functions": _UC_READ,
    "metastores": _UC_READ, "external-locations": _UC_READ, "storage-credentials": _UC_READ, "connections": _UC_READ,
    "grants": {"get", "get-effective"}, "jobs": {"list", "get", "list-runs", "get-run", "get-run-output"},
    "pipelines": {"list", "get", "list-updates", "get-update", "list-pipeline-events"}, "warehouses": {"list", "get"},
    "clusters": {"list", "get", "events", "spark-versions", "list-node-types", "list-zones"},
    "workspace": {"list", "export", "get-status"}, "secrets": {"list-scopes", "list-secrets"}, "postgres": {
        "list-branches", "list-cdf-configs", "list-cdf-statuses", "list-databases", "list-endpoints", "list-projects",
        "list-roles", "get-branch", "get-catalog", "get-cdf-config", "get-cdf-status", "get-database", "get-endpoint",
        "get-operation", "get-project", "get-role", "get-synced-table", "generate-database-credential"},
    "auth": {"describe", "profiles"}, "fs": {"ls", "cat", "head"}, "api": {"get"}, "bundle": {"validate", "summary"}}
_TOKEN_PRINTERS = ("auth token", "auth env")   # print the bearer token into the session log
_LIFECYCLE = ("catalogs create", "catalogs update", "catalogs delete", "schemas delete", "grants update", "grants delete",
              "postgres create-project", "postgres delete-project", "postgres undelete-project", "postgres update-project")
_CLI_CATALOG_ARG = {"schemas create": 1, "schemas update": 0, "tables delete": 0, "volumes create": 0, "volumes delete": 0,
                    "volumes update": 0, "functions delete": 0, "functions update": 0, "postgres create-catalog": 0,
                    "postgres delete-catalog": 0, "postgres create-synced-table": 0, "postgres delete-synced-table": 0}
_LAKEBASE_RESOURCE = re.compile(r"^projects/([^/\s]+)(?:/branches/([^/\s]+))?(?:/.*)?$")
_LAKEBASE_REFERENCE = re.compile(r"""projects/([^/\s"']+)(?:/branches/([^/\s"']+))?""")
_LAKEBASE_WRITE = {"create-branch", "delete-branch", "update-branch", "create-endpoint", "delete-endpoint", "update-endpoint",
                   "create-database", "delete-database", "update-database", "create-role", "delete-role", "update-role",
                   "create-cdf-config", "delete-cdf-config"}
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
_DESTRUCTIVE = ("mv", "truncate", "dd", "shred", *_WRITE_LAST_OPERAND, *_RECURSIVE_HEADS, *_FIXERS)
_IN_PLACE = {"sed": (re.compile(r"-[nEersuz]*i.*"), re.compile(r"--in-place.*")), "perl": (re.compile(r"-[a-zA-Z]*i.*"),),
             "awk": (re.compile(r"(?:--?)?inplace"),), "gawk": (re.compile(r"(?:--?)?inplace"),), "mawk": (re.compile(r"(?:--?)?inplace"),),
             "ruff": (re.compile(r"--fix|--fix-only|--unsafe-fixes|format"),)}

def _mutates(base: str, argv: list[str]) -> bool:
    inplace = any(rx.fullmatch(w) for w in argv[1:] for rx in _IN_PLACE.get(base, ())) and not any(w in ("--check", "--diff") for w in argv)
    return inplace or base == "find" and any(w in ("-delete", "-exec", "-execdir", "-ok", "-okdir", "-fls") or w.startswith("-fprint")
                                             for w in argv[1:])

_IDENTITY_FILE = re.compile(r"(?:^|/)(?:\.databrickscfg|\.databricks(?:/.*)?|\.config/databricks(?:/.*)?)$")
_GUARD_TREE = Path(os.path.realpath(__file__)).parent.parent   # the running plugin
_GUARD_FILE = Path(__file__).name
_PATH_LITERAL = re.compile(r"['\"]((?:[~./$]|/)[^'\"\n]{0,300})['\"]")
_GUARD_LITERAL = re.compile(r"['\"]((?:[^'\"\n/]*/)*(?:hooks(?:/[^'\"\n]*)?|hooks\.json|" + re.escape(_GUARD_FILE) + r"))['\"]")
_OUTPUT_FLAGS = ("-o", "-O", "--output", "--out", "--out-file", "--output-file", "--outfile", "--file")
_GIT_DISCARDS = {"clean": (), "reset": ("--hard", "--merge", "--keep"), "checkout": ("-f", "--force"),
                 "switch": ("-f", "--force", "--discard-changes"), "stash": ("", "push", "save")}
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
    lakebase_projects: list[str] = field(default_factory=list)
    lakebase_branches: list[str] = field(default_factory=list)
    run_mode: str = "live"
    fixture_endpoints: tuple[str, ...] = ()
    path: Path | None = None

    @classmethod
    def from_dict(cls, data: dict, path: Path | None = None) -> GuardConfig:
        catalogs = data.get("catalogs")
        if not isinstance(catalogs, list) or not catalogs:
            raise ValueError("allowed_targets.json must contain a non-empty 'catalogs' list")
        lists = {}
        for key in ("legacy_sources", "forbidden_bundle_targets", "target_hosts", "bundle_targets", "lakebase_projects", "lakebase_branches"):
            value = data.get(key, list(DEFAULT_FORBIDDEN_BUNDLE_TARGETS) if key == "forbidden_bundle_targets" else [])
            if not isinstance(value, list):
                raise ValueError(f"'{key}' must be a list")
            lists[key] = [str(x).strip() for x in value if str(x).strip()]
        lists["lakebase_projects"] = [p.removeprefix("projects/") for p in lists["lakebase_projects"]]
        mode = str(data.get("guard_mode", "block")).lower()
        if mode not in ("block", "warn"):
            raise ValueError("'guard_mode' must be 'block' or 'warn'")
        run_mode = data.get("run_mode", "live")
        if not isinstance(run_mode, str) or run_mode not in ("live", "fixture"):
            raise ValueError("'run_mode' must be 'live' or 'fixture'")
        endpoints = data.get("fixture_endpoints", [])
        if not isinstance(endpoints, list) or not all(isinstance(v, str) for v in endpoints):
            raise ValueError("'fixture_endpoints' must be a list of strings")
        for endpoint in endpoints:
            if not any(endpoint.startswith(prefix) for prefixes, _ in _CLOUD_FAMILY.values() for prefix in prefixes):
                raise ValueError(f"fixture_endpoints: cannot tell the cloud family of {endpoint}")
        return cls(catalogs=[_norm(c) for c in catalogs], legacy_sources=lists["legacy_sources"], mode=mode,
                   forbidden_bundle_targets=tuple(t.lower() for t in lists["forbidden_bundle_targets"]),
                   target_hosts=lists["target_hosts"], bundle_targets=lists["bundle_targets"],
                   lakebase_projects=lists["lakebase_projects"], lakebase_branches=lists["lakebase_branches"],
                   run_mode=run_mode, fixture_endpoints=tuple(endpoints), path=path)

@dataclass
class Verdict:
    decision: str  # approve | block
    reason: str = ""
    violations: list[str] = field(default_factory=list)

    @classmethod
    def of(cls, violations: list[str], cfg: GuardConfig) -> Verdict:
        notes = [v for v in violations if v.startswith(_AUTHORIZED)]
        real = [v for v in violations if not v.startswith(_AUTHORIZED)]
        if not real:
            reason = "dbx-migration-factory guard: " + "; ".join(notes) if notes else ""
            return cls("approve", reason, violations)
        reason = ("dbx-migration-factory guard: " + "; ".join(violations) + ". Fix the command or, if the target is legitimate, add "
                  "it to .migration/allowed_targets.json via a recorded decision (.migration/06_decisions.md); never work around the guard.")
        if cfg.mode == "warn" and not any(isinstance(v, _Legacy) for v in real):
            return cls("approve", "WARN (guard_mode=warn): " + reason, violations)
        return cls("block", reason, violations)

def _norm(ident: str) -> str:
    return ident.strip().strip('`"[]').lower()

def load_config(start: Path) -> GuardConfig | None:
    start = start.resolve()
    path = next((d / CONFIG_REL for d in [start, *start.parents] if (d / CONFIG_REL).is_file() or (d / CONFIG_REL.parent).is_dir()), None)
    if path is None:
        return None
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"{path} must be a JSON object")
    return GuardConfig.from_dict(data, path)

def _sql_view(text: str) -> str:
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

def _shell_tokens(cmd: str) -> tuple[list[str], list[str]]:
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
        elif ch == "#" and not started:
            i = cmd.find("\n", i)
            if i < 0:
                break
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
    remote: str = ""                                   # remote execution wrapper, when the command runs outside this workspace
    env: dict[str, str] = field(default_factory=dict)  # environment inherited by this command after shell assignments and unset
    multi: frozenset[str] = frozenset()
    at: str | None = ""                                # directory it runs in ('' the workspace root, None unresolvable)
    alts: list[str] = field(default_factory=list)      # the directories it may run in when `at` is None because of `x || cd d`
    sub: int = 0                                       # how many `( )` subshells enclose it
    after: str = ""                                    # the operator before it (`&&`, `||`, `;`, ...; '' for the first command)
    bg: int = 0                                        # the `&`-terminated list it belongs to (runs in a subshell); 0 for none
    lost: bool = False                                 # the possible directories overflowed _MAX_ALTS and were dropped
    unreadable: str = ""

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

_MAX_ALTS = 64   # possible directories tracked through `x || cd d` chains before the guard gives up resolving them
_MAX_DEPTH = 4

def _commands(cmd: str) -> list[_Seg]:
    toks, raws = _shell_tokens(cmd)
    out: list[_Seg] = []
    groups: list[tuple[int, list[_Seg]]] = []   # (index of the first member, stdin the group inherits)
    feed: list[_Seg] = []                        # what the next command's stdin receives
    closed: list[_Seg] = []                      # members of the group just closed
    lists: list[int] = [0]                       # index of the first member of the and/or list open at each group level
    cur, i, sub, after, bgs = None, 0, 0, "", 0
    while i < len(toks):
        tok, n = toks[i], 1 + bool(_REDIRECT_OP.fullmatch(toks[i]))   # a redirection travels with its operand
        if n == 1 and tok == "\n" and cur is None and feed and not closed:
            pass                                        # a line break after `|` continues the pipeline
        elif n == 1 and tok in _SEPARATORS and (tok not in ("{", "}") or cur is None):
            if tok == "&":                              # `&` backgrounds the whole and/or list before it
                bgs += 1
                for c in out[lists[-1]:]:
                    c.bg = c.bg or bgs
            if tok in ("&", ";", "\n"):
                lists[-1] = len(out)
            if tok in ("(", "{"):
                groups.append((len(out), feed))
                lists.append(len(out))
                sub += tok == "("
            elif tok in (")", "}"):
                start, feed = groups.pop() if groups else (0, [])
                closed = out[start:]
                lists = lists[:-1] or [0]
                sub -= tok == ")" and sub > 0
            elif tok in ("|", "|&"):
                producers = closed or ([cur] if cur else [])
                feed, closed = producers + [f for p in producers for f in p.feeds], []
            else:
                feed, closed = (groups[-1][1] if groups else []), []
            cur, after = None, tok if tok not in ("(", "{") else after
        else:
            if cur is None and (n == 1 or not closed):
                cur, closed = _Seg(feeds=list(feed), sub=sub, after=after), []
                out.append(cur)
            for c in closed if cur is None else [cur]:
                c.words.extend(toks[i:i + n])
                c.raw.extend(raws[i:i + n])
        i += n
    return [c for c in out if c.words]

def _expands(text: str, subst: bool = False) -> bool:
    live = re.sub(r"\\.|'[^']*'?", lambda m: " " * len(m.group()), text, flags=re.DOTALL)
    return any(not re.fullmatch(r"[\w$-]+", m.group(1)) for m in re.finditer(r"`([^`]*)`", live)) or (
        bool(re.search(r"\$\(|(?<![\w])[<>]\(", live)) if subst else "$" in live)

def _lakebase(verb: str, project: str | None, branch: str | None, cfg: GuardConfig) -> list[str]:
    if project is None:
        if branch == "production":
            return [f"`databricks postgres {verb}` on the `production` branch of Lakebase project ?; migration sessions "
                    "write only per-batch branches (production is repointed at STOP E)"]
        if branch is not None and cfg.lakebase_branches and not any(fnmatch.fnmatchcase(branch, pattern)
                                                                    for pattern in cfg.lakebase_branches):
            return [f"`databricks postgres {verb}` on branch {branch!r} of Lakebase project ?; allowed lakebase_branches "
                    f"{cfg.lakebase_branches}"]
        return []
    if project not in cfg.lakebase_projects:
        return [f"`databricks postgres {verb}` targets Lakebase project {project!r} outside allowed lakebase_projects "
                f"{cfg.lakebase_projects} (empty = every Lakebase write blocks)"]
    if branch == "production":
        return [f"`databricks postgres {verb}` on the `production` branch of Lakebase project {project}; migration sessions "
                "write only per-batch branches (production is repointed at STOP E)"]
    if branch is not None and cfg.lakebase_branches and not any(fnmatch.fnmatchcase(branch, pattern)
                                                                for pattern in cfg.lakebase_branches):
        return [f"`databricks postgres {verb}` on branch {branch!r} of Lakebase project {project}; allowed lakebase_branches "
                f"{cfg.lakebase_branches}"]
    return []

def _json(text: str):
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return None

def _remote_payload(words: list[str]) -> str | None:
    if words[:3] == ["aws", "ssm", "send-command"]:
        if "--cli-input-json" in words:
            return None
        params = next((words[i + 1] for i, w in enumerate(words[:-1]) if w == "--parameters"), None)
        if params is None or params.startswith(("file://", "@")):
            return None
        if params.startswith("commands="):
            value = params.split("=", 1)[1]
            if value.startswith("["):
                parsed = _json(value)
                if parsed is None:
                    return None
                return "\n".join(parsed) if isinstance(parsed, list) and all(isinstance(x, str) for x in parsed) else None
            if value.startswith('"') and value.endswith('"'):
                parsed = _json(value)
                if parsed is None:
                    return None
                return parsed if isinstance(parsed, str) else None
            return value
        if params.startswith("{"):
            parsed = _json(params)
            if parsed is None:
                return None
            value = parsed.get("commands") if isinstance(parsed, dict) else None
            return "\n".join(value) if isinstance(value, list) and all(isinstance(x, str) for x in value) else value if isinstance(value, str) else None
        return None
    if words[:4] == ["az", "vm", "run-command", "invoke"]:
        if any(w.startswith("@") for w in words):
            return None
        scripts = []
        try:
            i = words.index("--scripts") + 1
        except ValueError:
            return None
        while i < len(words) and not words[i].startswith("-"):
            scripts.append(words[i])
            i += 1
        return "\n".join(scripts) if scripts else None
    return None

def _program(words: list[str], assigns: list[str]) -> tuple[list[str], str]:
    i = 0
    while i < len(words):
        w = words[i].rsplit("/", 1)[-1]
        if _ASSIGN.match(words[i]):
            assigns.append(words[i])
            i += 1
            continue
        if w in _RESERVED_PREFIXES:
            i += 1
            continue
        w = "docker" if w in ("podman", "nerdctl") else w
        if words[i:i + 3] == ["aws", "ssm", "send-command"] or words[i:i + 4] == ["az", "vm", "run-command", "invoke"]:
            payload = _remote_payload(words[i:])
            if payload is None:
                return words, ""
            return ["sh", "-c", payload], " ".join(words)
        if w == "docker":
            i += 1
            while i < len(words) and words[i].startswith("-"):
                i += 1 if "=" in words[i] or words[i] in ("-D", "--debug", "--tls", "--tlsverify") else 2
            i += words[i:i + 1] == ["compose"]
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
    return [f for c in _commands(cmd) if cfg is None or _mentions(c, cfg) for f in _scripts_of(c)]

def _join(at: str | None, d: str) -> str | None:
    if at is None or d == "-" or _expands(d):
        return None
    d = os.path.expanduser(d)
    p = os.path.normpath(d if d.startswith("/") or not at else os.path.join(at, d))
    return "" if p == "." else p

def _read_script(f: str, root: Path, at: str | None = "") -> str | None:
    if at is None:
        return None
    p = (root if not at else Path(at) if at.startswith("/") else root / at) / os.path.expandvars(os.path.expanduser(f))
    try:
        with p.open(errors="replace") as fh:
            body = fh.read(_MAX_SCRIPT_BYTES + 1)
        return None if len(body) > _MAX_SCRIPT_BYTES else body
    except OSError:
        return None

def _shell_runs(seg: _Seg) -> tuple[str | None, str | None, bool]:
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

def _segments(text: str, ctx: str = "", depth: int = 0, env: dict[str, str] | None = None,
              at: str | None = "", multi: set[str] | None = None, root: Path | None = None) -> list[_Seg]:
    text = re.sub(r"\\\r?\n", " ", text)
    aliases: dict[str, list[str]] = {}
    env = dict(_ENV_DEFAULTS) if env is None else env
    multi = set() if multi is None else multi
    out: list[_Seg] = []
    dirs: list[str | None] = []
    alts: list[str] = []                                      # the directories `at` may be when it is None after `x || cd d`
    scopes: list[tuple[str | None, list[str], list[str | None], dict[str, str], set[str]]] = []   # state outside each open subshell
    bg, saved = 0, (at, alts, dirs, dict(env), set(multi))     # the background list being read and the state before it
    before: list[str | None] = [at]                           # where the shell was before the previous command
    lost = False
    def restore(state):
        nonlocal at, alts, dirs
        at, alts, dirs, saved_env, saved_multi = state
        env.clear(); env.update(saved_env)
        multi.clear(); multi.update(saved_multi)
    for seg in _commands(text):
        if seg.bg != bg:                                      # a `&` list runs in a subshell: what it changes ends with it
            if bg:
                restore(saved)
                before = [at] if at is not None else alts
            bg, saved = seg.bg, (at, list(alts), list(dirs), dict(env), set(multi))
        while len(scopes) < seg.sub:
            scopes.append((at, list(alts), list(dirs), dict(env), set(multi)))
        while len(scopes) > seg.sub:                          # `( ... ) || cd d`: the parent is where it was before the group
            restore(scopes.pop())
            before = [at] if at is not None else alts
        seg.words = [w if not env or "$" not in w or "$" not in re.sub(r"\\.|'[^']*'?", "", r) else
                     _SHELL_VAR.sub(lambda m: env.get(m.group(1) or m.group(2), m.group()), w) for w, r in zip(seg.words, seg.raw)]
        seg.argv, seg.ctx = _program(aliases.get(seg.args[0] if seg.args else "", seg.args[:1]) + seg.args[1:], seg.assigns)
        seg.ctx = " ".join(x for x in (ctx, seg.ctx) if x)
        wrapper = seg.ctx.split()[:1]
        seg.remote = wrapper[0] if wrapper and (
            wrapper[0] in ("ssh", "aws", "az") or
            wrapper[0] in ("docker", "podman", "nerdctl", "kubectl") and
            any(w in seg.ctx.split() for w in ("exec", "run"))
        ) else ""
        if not seg.remote and seg.argv[:3] == ["aws", "ssm", "send-command"] or not seg.remote and seg.argv[:4] == [
                "az", "vm", "run-command", "invoke"]:
            seg.remote = seg.argv0
        assignments = (seg.assigns if not seg.argv else seg.argv[1:] if seg.argv0 in _DECLARERS else ())
        for assignment in assignments:
            if _ASSIGN.match(assignment):
                name, value = assignment.split("=", 1)
                if name not in multi:
                    env[name] = value
                else:
                    env[name] = f"{env.get(name, '')} {value}".strip()
        if seg.argv0 == "unset":
            env.update((name, "") for name in seg.argv[1:]
                       if re.fullmatch(r"[A-Za-z_]\w*", name) and name not in multi)
        seg.env = dict(env)
        seg.multi = frozenset(multi)
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
        seg.alts, seg.lost = list(alts), lost
        now: list[str | None] = [at] if at is not None else alts
        if seg.argv0 in ("cd", "pushd"):
            args = [w for w in seg.argv[1:] if not (w.startswith("-") and len(w) > 1)]
            dirs += [at] if seg.argv0 == "pushd" else []
            moved = now + [_join(p, args[0] if args else "~") for p in before] if seg.after == "||" else \
                [_join(p, args[0] if args else "~") for p in now]
            moved = list(dict.fromkeys(moved))
            at = moved[0] if len(moved) == 1 else None
            alts = [] if at is not None else [p for p in moved if p is not None]
            if len(alts) > _MAX_ALTS:                         # too many to enumerate: unresolvable from here on
                alts, lost = [], True
        elif seg.argv0 == "popd":
            at, alts = (dirs.pop() if dirs else None), []
        elif seg.argv0 == "alias":
            aliases.update((a.split("=", 1)[0], shlex.split(a.split("=", 1)[1])) for a in seg.argv[1:] if "=" in a)
        before = now
        out.append(seg)
        if seg.argv0 == "for" and len(seg.argv) >= 4 and seg.argv[2] == "in":
            var, values = seg.argv[1], seg.argv[3:]
            if re.fullmatch(r"[A-Za-z_]\w*", var) and values:
                env[var] = " ".join(values)
                multi.add(var)
        nested, file, _ = _shell_runs(seg)
        if nested is not None:
            out.extend(_segments(nested, seg.ctx, depth + 1, dict(env), seg.at, set(multi), root))
        elif root is not None and file:
            if depth >= _MAX_DEPTH:
                seg.unreadable = "depth"
            elif (body := _read_script(file, root, seg.at)) is None:
                seg.unreadable = file
            else:
                shared = seg.argv0 in ("source", ".")
                out.extend(_segments(body, seg.ctx, depth + 1, env if shared else dict(env),
                                     seg.at, multi if shared else set(multi), root))
    return out

def _mentions(seg: _Seg, cfg: GuardConfig) -> bool:
    for word in seg.argv or seg.args:
        if _CLIENT_WORD.search(word) or "--target-catalog" in word:
            return True
        if any(re.search(rf"(?<![\w-]){re.escape(token)}(?![\w-])", word, re.IGNORECASE)
               for token in cfg.legacy_sources):
            return True
    return False

def _flag_values(argv: list[str], flags: tuple[str, ...]) -> list[str]:
    return [argv[i + 1] if w in flags else w.split("=", 1)[1] for i, w in enumerate(argv[1:], 1)
            if (w in flags and i + 1 < len(argv) and not _FLAG_WORD.fullmatch(argv[i + 1])) or ("=" in w and w.split("=", 1)[0] in flags)]

def _sql_text(seg: _Seg, root: Path, extra: list[str] = ()) -> tuple[str, list[str]]:
    parts = [" ".join(w for w in extra if not w.endswith(".sql")), *_flag_values(seg.argv, _SQL_VALUE_FLAGS), *seg.stdin, *seg.heredocs,
             seg.herestring or ""]
    bodies = [(f, _read_script(f, root, seg.at)) for f in [*seg.scripts, *(w for w in extra if w.endswith(".sql"))]]
    return "\n;\n".join(filter(None, [*parts, *(b for _, b in bodies)])), [f for f, b in bodies if b is None]

def _non_reads(sql: str) -> list[str]:
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

def _write_objects(statements: list[str]) -> list[list[str]]:
    objects = []
    for statement in statements:
        match = _WRITE_OBJECT.match(statement)
        names = [re.sub(r'["`\[\]]', "", match.group(1))] if match else []
        while match:
            match = _WRITE_OBJECT_NEXT.match(statement, match.end())
            if match:
                names.append(re.sub(r'["`\[\]]', "", match.group(1)))
        objects.append(names)
    return objects

def _decision(seg: _Seg, statements: list[str], root: Path) -> tuple[str | None, str | None]:
    decision_id = next((a.split("=", 1)[1] for a in reversed(seg.assigns) if a.startswith("DBX_DECISION=")), None)
    if not decision_id:
        return None, "no `DBX_DECISION=D-<id>` prefix on the command"
    if not _DECISION_ID.fullmatch(decision_id):
        return decision_id, f"`{decision_id}` is not a row in .migration/06_decisions.md"
    try:
        ledger = (root / ".migration" / "06_decisions.md").read_text(errors="replace")
    except OSError:
        return decision_id, "cannot read .migration/06_decisions.md"
    row = next((line for line in ledger.splitlines()
                if (match := _DECISION_ROW.search(line)) and match.group(1).lower() == decision_id.lower()), None)
    if row is None:
        return decision_id, f"`{decision_id}` is not a row in .migration/06_decisions.md"
    if "legacy_write_authorized" not in row.lower():
        return decision_id, f"row `{decision_id}` does not contain `legacy_write_authorized`"
    normalized_row = re.sub(r'["`\[\]]', "", row)
    objects = _write_objects(statements)
    if len(objects) != len(statements) or any(not names for names in objects):
        statement = next(statement for statement in statements if not _WRITE_OBJECT.match(statement))
        return decision_id, f"cannot tell which object `{statement[:60]}` writes"
    for names, statement in zip(objects, statements):
        for obj in names:
            if obj.startswith("$") or obj.endswith("$") or re.search(r"[$:&@]\{?\(", statement):
                return decision_id, f"`{obj}` is a run-time substitution; the decision must name the literal object"
    for names in objects:
        for obj in names:
            if not re.search(rf"(?<![\w.]){re.escape(obj)}(?![\w.])", normalized_row, re.IGNORECASE):
                return decision_id, f"row `{decision_id}` does not name `{obj}`"
    return decision_id, None

def _strip_sql_comments(sql: str) -> str:
    out = list(sql)
    quote = ""
    i = 0
    while i < len(sql):
        if quote:
            if sql[i] == quote:
                if i + 1 < len(sql) and sql[i + 1] == quote and quote == "'":
                    i += 2
                    continue
                quote = ""
            i += 1
            continue
        if sql[i] in "'\"`":
            quote = sql[i]
            i += 1
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            end = len(sql) if end < 0 else end
            for j in range(i, end):
                out[j] = " "
            i = end
        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            end = len(sql) if end < 0 else end + 2
            for j in range(i, end):
                if sql[j] != "\n":
                    out[j] = " "
            i = end
        else:
            i += 1
    return "".join(out)

def _conn(seg: _Seg, cfg: GuardConfig, sql: str) -> tuple[list[str], list[str], set[str], bool]:
    argv = seg.argv
    stripped = _strip_sql_comments(sql)
    host_values = list(_flag_values(argv, _HOST_FLAGS))
    host_values += [a.split("=", 1)[1] for a in seg.assigns if a.split("=", 1)[0] in _HOST_ENV]
    host_values += [w for w in argv[1:2] if seg.argv0 in _DSN_POSITIONAL and not w.startswith("-")]
    host_values += [
        w for i, w in enumerate(argv[1:], 1)
        if re.fullmatch(r"\$\{?\w+\}?", w) and (not argv[i - 1].startswith("-") or argv[i - 1] in _HOST_FLAGS)
    ]
    for match in _RECONNECT.finditer(stripped):
        words = match.group(2).split()
        candidate = words[:1] if match.group(1).lower() == ":connect" else words[1:2]
        host_values += [value for value in candidate if not re.search(r"=|://", value)]
    joined = " ".join(argv[1:] + list(seg.assigns))
    host_values += re.findall(r"://(?:[^@/\s]*@)?([^:/?\s;]+)", joined + " " + stripped)
    host_values += re.findall(r"(?i)\b(?:host|hostaddr|server|data source|addr)=([^;\s]+)", joined + " " + stripped)
    host_values += re.findall(r"(?im)^\s*\.LOGON\s+([^/\s;]+)", stripped)

    def host_value(value: str) -> str:
        value = re.sub(r"^(?:tcp|np|lpc):", "", value, flags=re.IGNORECASE)
        value = re.sub(r"^\$\{?(\w+)\}?$", r"\1", value)
        return re.split(r"[,:\\]", value, 1)[0].lower()

    hosts = [host_value(value) for value in host_values if value]
    literal_hosts = [host for value, host in zip(host_values, hosts)
                     if not _SHELL_VAR.fullmatch(value)]
    db_values = [value for value in _flag_values(argv, ("-d", "-D", "--dbname", "--database")) if "=" not in value]
    db_values += [
        words[0]
        for verb, value in _RECONNECT.findall(stripped)
        for words in [value.split()]
        if verb.lower() != ":connect" and words and "=" not in words[0] and "://" not in words[0]
    ]
    db_values += re.findall(r"(?i)(?<![-\w])(?:database|dbname|initial catalog)=([^;\s]+)", joined + " " + stripped)
    db_values += re.findall(r"(?i)\bUSE\s+(?:\[?DATABASE\]?\s+)?([A-Za-z_][\w$.-]*)", stripped)
    db_values += re.findall(r"://[^/\s]+/([^/?\s;]+)", joined)
    dbs = {value.strip("'\"") for value in db_values if value.strip("'\"")}

    secret_names = {
        match.group(1) or match.group(2)
        for word in [*argv, *seg.assigns]
        for match in [_SHELL_VAR.fullmatch(word.split("=", 1)[-1])]
        if match
    }
    secret_names.update(
        match.group(1) or match.group(2)
        for word in argv
        for match in [_SHELL_VAR.search(word)]
        if match
    )
    secret_names.update(word for word in argv if "jdbc:" in word.lower() or "://" in word)
    known_names = set(cfg.target_hosts) | set(cfg.legacy_sources)
    def unknown(value: str) -> bool:
        match = _SHELL_VAR.search(value)
        return bool(match and (match.group(1) or match.group(2)) not in known_names)
    unresolved = any(
        unknown(value)
        or "`" in value or "$(" in value
        for value in [*host_values, *db_values]
    )

    hits = []
    for token in cfg.legacy_sources:
        if token in secret_names:
            hits.append(token)
        if any(token.lower() == host.lower() for host in literal_hosts):
            hits.append(token)
        if any((token == db if seg.argv0 in ("psql", "pgcli", "mysql", "mariadb", "usql") else
                token.lower() == db.lower()) for db in dbs):
            hits.append(token)
    return list(dict.fromkeys(hits)), hosts, dbs, unresolved

def _check_unreadable(segs: list[_Seg], cfg: GuardConfig) -> list[str]:
    violations = []
    for index, s in enumerate(segs):
        ctx = _mentions(s, cfg)
        base, _, _, built = s.argv0, *_shell_runs(s)
        if built:
            violations.append(f"`{base}{' -c' if base in _SHELLS else ''}` on a runtime-built string; the guard cannot read what it would run")
        if base in ("cp", "ln", "mv", "install"):
            raw = " ".join(s.raw_of(word) for word in s.argv[1:])
            match = _CLIENT_WORD.search(raw)
            if not match and any("$" in word for word in s.raw):
                match = _CLIENT_WORD.search(" ".join(word for later in segs[index + 1:] for word in later.argv))
            if match:
                violations.append(f"`{base}` copies or renames client `{match.group()}` under another name; run the client by its own name so "
                                  "the guard can read the call")
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
                and (wrapped := next((m.group() for m in _CLIENT_WORD.finditer(" ".join(s.argv[1:]))), None)):
            violations.append(f"unrecognised wrapper `{base}` in front of client `{wrapped[0]}`; the guard has no rule for `{base}`, so "
                              "it cannot tell how or where the client would run (run the client directly)")
        for i, (r, w, prev) in enumerate(zip(s.raw, s.words, ["", *s.words])):   # an expansion inside what is (or looks like) SQL
            if (prev in _SQL_VALUE_FLAGS or w.split("=", 1)[0] in _SQL_VALUE_FLAGS or
                    (re.search(r"[\s;]", w) and prev not in _HOST_FLAGS) or (
                    i >= 2 and s.words[i - 2] == "tools" and prev == "query")) and _expands(r) and not _REDIRECT_OP.fullmatch(prev):
                violations.append(f"shell expansion inside the SQL argument `{r[:60]}` (substitution); expand it in the command text so the guard can "
                                  "read the statement")
                break
        if any(re.fullmatch(r"\d*<<-?", w) and _expands(s.raw[i + 1]) for i, w in enumerate(s.words[:-1])):
            violations.append("unquoted heredoc expands `$`/backticks in its body; quote the delimiter (<<'EOF') or inline the values")
        defined = {later.argv0 for later in segs if len(later.argv) == 1}
        if ctx and base not in _KNOWN and base not in _INERT and base not in defined and not _PYTHON.fullmatch(base):
            hits, _, _, _ = _conn(s, cfg, "")
            if hits:
                violations.append(f"unrecognised command `{base}` names legacy connection {hits}; the guard cannot verify its operation")
    return violations

def _check_sql_client(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    base, tail = seg.argv0, _LEGACY_TAIL
    if seg.remote:
        return []
    if base in _LOADERS:
        return [_Legacy(f"`{base}` is a loader: nothing but reads ever runs against a legacy source" + tail)]
    sql, unreadable = _sql_text(seg, root, [seg.argv[1]] if base == "bcp" and len(seg.argv) > 2 and "queryout" in seg.argv[2:4] else [])
    hits, hosts, dbs, unresolved = _conn(seg, cfg, sql)
    legacy = base in _LEGACY_ONLY
    non_reads = _non_reads(sql)
    bad = [b.split("\n", 1)[0][:80] for b in [
        *([f"stdin from `{seg.opaque}`, a program or expansion the guard cannot read"] if seg.opaque else []),
        *([f"{base} (a migration tool: every run writes its target)"] if base in _WRITERS else []),
        *(["bcp ... in (loader)"] if base == "bcp" and "in" in seg.argv[1:4] else []), *non_reads]]
    violations = [_Legacy(_UNREADABLE.format(who="legacy client", files=unreadable) + tail)] if unreadable and (legacy or hits) else []
    if unreadable and not violations:
        bad.insert(0, f"script(s) {unreadable} the guard cannot read")
    if not bad:
        return violations
    if legacy or hits:
        violation = (f"non-read statement through a legacy-only client `{base}`" if legacy
                     else f"non-read statement against legacy source {hits}") + f": `{bad[0]}`"
        plain = non_reads and not unreadable and not seg.opaque and base not in _WRITERS and not (
            base == "bcp" and "in" in seg.argv[1:4])
        did, missing = _decision(seg, non_reads, root) if plain else (None, "the statement is not a readable SQL write")
        if missing is None:
            objects = [obj for names in _write_objects(non_reads) for obj in names]
            violations.append(f"{_AUTHORIZED}{did} authorizes the legacy write of {', '.join(objects)} "
                              "(legacy_write_authorized row in .migration/06_decisions.md)")
        else:
            violations.append(_Legacy(f"{violation}; a recorded decision would allow it, but {missing}{tail}"))
    elif unresolved and bad:
        violations.append(f"non-read statement through `{base}` to a connection built at run time (`{bad[0]}`); spell the host and database "
                          f"out so the guard can resolve them: `{bad[0]}` (`target_hosts` cannot resolve it)")
    elif not hosts or not all(
        (host in cfg.target_hosts if host in {
            match.group(1) or match.group(2)
            for word in [*seg.argv, *seg.assigns]
            for match in [_SHELL_VAR.search(word)]
            if match
        } else any(host.lower() == target.lower() for target in cfg.target_hosts))
        for host in hosts
    ):
        violations.append(f"non-read statement through `{base}` to a host that is not a literal in target_hosts {cfg.target_hosts} "
                          f"(seen: {sorted(set(hosts))[:6]}; every host must be listed, an empty list blocks every write): `{bad[0]}`")
    else:
        explicit_dbs = [value for value in _flag_values(seg.argv, ("-d", "-D", "--dbname", "--database"))
                        if "=" not in value]
        use_dbs = set(re.findall(r"(?i)\bUSE\s+(?:\[?DATABASE\]?\s+)?([A-Za-z_][\w$.-]*)", _strip_sql_comments(sql)))
        extra_dbs = dbs - set(explicit_dbs) - use_dbs
        default_value = explicit_dbs[0] if explicit_dbs and not extra_dbs else next(iter(dbs)) if len(dbs) == 1 else None
        default = _norm(default_value) if default_value else None
        violations += _catalog_violations(sql, cfg, default, f"`{base}` client")
    return violations

def _catalog_violations(sql: str, cfg: GuardConfig, default: str | None, who: str) -> list[str]:
    allowed, in_dbx = set(cfg.catalogs), who in ("Databricks client", "Databricks SQL client")
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
        elif (who == "Databricks client" and re.match(r"^\s*(?:GRANT|REVOKE|DENY)\b.*\bON\s+CATALOG\b", stmt,
                                                        re.IGNORECASE | re.DOTALL)) or (
                who and who != "Databricks client" and (
                (who != "Databricks SQL client" or not re.search(r"\bON\s+TABLE\b", stmt, re.IGNORECASE)) and
                re.match(r"^\s*(?:GRANT|REVOKE|DENY)\b", stmt, re.IGNORECASE) or
                re.match(_PERMISSION.format(c="|".join((*cats, _METASTORE))), stmt, re.IGNORECASE | re.DOTALL))):
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
            sql, cfg, next(map(_norm, _flag_values(seg.argv, ("--catalog",))), None),
            "Databricks SQL client" if group == "sql" else "Databricks client")
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
                 "catalog / Lakebase-project lifecycle or permissions (those happen at STOP E)")]
    if key in _CLI_CATALOG_ARG:
        name = (args + ["", ""])[_CLI_CATALOG_ARG[key]]
        if not name or _norm(name.split(".")[0]) not in cfg.catalogs:
            return [f"CLI mutation of securable {name!r} outside allowlist {sorted(cfg.catalogs)}"]
        if group == "postgres":
            json_raw = [seg.raw_of(argv[n + 1]) for n, w in enumerate(argv[:-1]) if w == "--json"]
            json_vals = [argv[n + 1] for n, w in enumerate(argv[:-1]) if w == "--json"]
            json_raw += [seg.raw_of(w) for w in argv if w.startswith("--json=")]
            json_vals += [w.removeprefix("--json=") for w in argv if w.startswith("--json=")]
            if _expands(" ".join(json_raw)) or any(value.startswith("@") for value in json_vals):
                return [f"`databricks postgres {verb}` JSON payload must be literal (fail closed): no expansion, no `@file`"]
            def _strings(value):
                if isinstance(value, str):
                    yield "", value
                elif isinstance(value, dict):
                    for key, child in value.items():
                        if isinstance(child, str):
                            yield str(key).lower(), child
                        else:
                            yield from _strings(child)
                elif isinstance(value, list):
                    for child in value:
                        yield from _strings(child)
            for json_value in json_vals:
                parsed = _json(json_value)
                if parsed is None:
                    return [f"`databricks postgres {verb}` JSON payload is not parseable JSON (fail closed)"]
                for key, value in _strings(parsed):
                    if value.startswith("projects/"):
                        if violation := _lakebase(verb, value.removeprefix("projects/").split("/")[0], None, cfg):
                            return violation
                    if (match := _LAKEBASE_REFERENCE.search(value)) and (
                            violation := _lakebase(verb, *match.groups(), cfg)):
                        return violation
                    if "project" in key and (violation := _lakebase(verb, value, None, cfg)):
                        return violation
                    if "branch" in key and not key.startswith("source") and (
                            violation := _lakebase(verb, None, value, cfg)):
                        return violation
            for match in _LAKEBASE_REFERENCE.finditer(seg.text):
                if violation := _lakebase(verb, *match.groups(), cfg):
                    return violation
        return []
    if group == "postgres" and verb in _LAKEBASE_WRITE:
        if not args or _expands(args[0]) or not (match := _LAKEBASE_RESOURCE.fullmatch(args[0])):
            return [f"`databricks postgres {verb}` requires a literal projects/<project> resource path; allowed lakebase_projects "
                    f"{cfg.lakebase_projects} (empty = every Lakebase write blocks)"]
        project, branch = match.groups()
        if verb == "create-branch":
            if len(args) <= 1 or _expands(args[1]):
                return ["`databricks postgres create-branch` needs a literal branch id (fail closed)"]
            branch = args[1]
        return _lakebase(verb, project, branch, cfg)
    if verb in _DBX_READ.get(group, ()):
        return []
    return [f"`databricks {group} {verb}`".rstrip() + " is not in the guard's read allowlist (fail closed); reads are "
            "list/get shapes, writes go through an allowlisted securable or the migration workflow"]

def _check_rest(seg: _Seg) -> list[str]:
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

def _python_texts(seg: _Seg, root: Path) -> tuple[str, str | None]:
    argv = seg.argv
    texts = [*_flag_values(argv, ("-c",)), *seg.heredocs]
    unreadable = None
    if "-m" not in argv and not texts and (script := next((w for w in argv[1:] if w.endswith(".py")), None)):
        body = _read_script(script, root, seg.at)
        if body is None:
            unreadable = script
        else:
            texts.append(body)
    return "\n".join(texts), unreadable

def _strip_python_comments(text: str) -> str:
    lines = text.splitlines(keepends=True)
    try:
        tokens = tokenize.generate_tokens(io.StringIO(text).readline)
        for token in tokens:
            if token.type == tokenize.COMMENT:
                line, col = token.start
                end_line, end_col = token.end
                if line == end_line:
                    lines[line - 1] = lines[line - 1][:col] + " " * (end_col - col) + lines[line - 1][end_col:]
    except (IndentationError, tokenize.TokenError):
        pass
    return "".join(lines)

def _check_python(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    text, unreadable = _python_texts(seg, root)
    if unreadable and seg.argv0 == "spark-submit":
        return [f"spark-submit script {unreadable!r} cannot be read; the guard cannot clear a Spark job it cannot inspect"]
    text = _strip_python_comments(text)
    calls = list(re.finditer(r"\b(connect|create_engine|Connection|connection|WorkspaceClient|SparkSession)\s*\(([^)]*)\)",
                             text, re.IGNORECASE | re.DOTALL))
    env_refs = list(re.finditer(r"(?:environ\s*\[\s*['\"]([A-Za-z_]\w*)['\"]\s*\]|getenv\s*\(\s*['\"]([A-Za-z_]\w*)['\"])",
                                text, re.IGNORECASE))
    literals = list(_PY_LITERAL.finditer(text))
    conn_spans = [m.group(0) for m in calls]
    conn_spans += [m.group(0) for m in re.finditer(
        r"""['"][^'"]*(?:://|jdbc:|Server=|host=|Database=|Initial Catalog=)[^'"]*['"]""", text, re.IGNORECASE)]
    conn_text = " ".join(conn_spans)
    env_names = [m.group(1) or m.group(2) for m in env_refs]
    conn_text_no_env = re.sub(r"(?:environ\s*\[\s*['\"][A-Za-z_]\w*['\"]\s*\]|getenv\s*\(\s*['\"][A-Za-z_]\w*['\"])",
                              " ", conn_text, flags=re.IGNORECASE)
    argv_text = re.sub(r"(?:environ\s*\[\s*['\"][A-Za-z_]\w*['\"]\s*\]|getenv\s*\(\s*['\"][A-Za-z_]\w*['\"])",
                       " ", " ".join(seg.argv), flags=re.IGNORECASE)
    hits = [token for token in cfg.legacy_sources
            if token in env_names or any(re.search(rf"(?<![\w-]){re.escape(token)}(?![\w-])", conn_text_no_env, re.IGNORECASE)
                                         for _ in [0])
            or re.search(rf"(?<![\w-]){re.escape(token)}(?![\w-])", argv_text, re.IGNORECASE)]
    known_connection_names = set(cfg.target_hosts) | set(cfg.legacy_sources)
    unresolved = any(name not in known_connection_names for name in env_names)
    unresolved = unresolved or any(
        not re.search(r"""['"][^'"]*['"]""", m.group(2)) and not re.search(
            r"(?:environ\s*\[|getenv\s*\()", m.group(2), re.IGNORECASE)
        for m in calls if m.group(1).lower() in ("connect", "create_engine"))
    databricks = bool(re.search(r"\b(?:databricks|spark)\b", text, re.IGNORECASE) or
                      any(re.search(r"\b(?:databricks|spark)\b", word, re.IGNORECASE) for word in seg.argv))
    has_connection = bool(calls or conn_spans or env_refs)
    violations = []
    for m in literals:
        if not has_connection:
            continue
        if bad := _non_reads(m.group(2)):
            if unresolved:
                violations.append(f"non-read statement in Python uses a connection built at run time; spell the connection out so the guard "
                                  f"can resolve it: `{bad[0][:80]}`")
            elif databricks:
                violations += _catalog_violations(m.group(2), cfg, None, "Databricks client")
            elif hits:
                violations.append(_Legacy(f"non-read statement against legacy source {hits} in a program: `{bad[0][:80]}` "
                                          + _LEGACY_TAIL))
    if not literals and unresolved and _non_reads(text):
        violations.append("Python connection is built at run time; the guard cannot resolve a non-read statement")
    return violations

def _check_fixture(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    if cfg.run_mode != "fixture" or not cfg.fixture_endpoints or seg.argv0 in _INERT:
        return []
    if seg.argv0 in _SHELLS or seg.argv0 == "eval":
        text, _, _ = _shell_runs(seg)
        if text is not None:
            return []
    text = seg.text
    if _PYTHON.fullmatch(seg.argv0):
        pytext, _ = _python_texts(seg, root)
        text += "\n" + pytext
    violations = []
    for family, (prefixes, pattern) in _CLOUD_FAMILY.items():
        declared = [name for name in cfg.fixture_endpoints if any(name.startswith(prefix) for prefix in prefixes)]
        match = pattern.search(text)
        command_values: dict[str, str] = {}
        for assignment in seg.assigns:
            if _ASSIGN.match(assignment):
                name, value = assignment.split("=", 1)
                lookup = {**os.environ, **seg.env, **command_values}
                command_values[name] = _SHELL_VAR.sub(
                    lambda m: lookup.get(m.group(1) or m.group(2), m.group()), value)
        effective = {**os.environ, **seg.env, **command_values}
        for name in declared:
            if name in seg.env and name not in command_values:
                lookup = {**os.environ, **{k: v for k, v in seg.env.items() if k != name}}
                effective[name] = _SHELL_VAR.sub(
                    lambda m: lookup.get(m.group(1) or m.group(2), m.group()), seg.env[name])
        missing = [name for name in declared if not effective.get(name)]
        unresolved = [name for name in declared if (name in command_values or name in seg.env)
                      and effective.get(name) and ("$" in effective[name] or "`" in effective[name])]
        if match and unresolved:
            violations.append(f"run_mode is fixture and the command names {family} tooling (`{match.group()}`) but {unresolved} is set "
                              "to an expansion the guard cannot resolve for this command; a fixture must fail closed rather than reach "
                              f"the live {family} account (declared in fixture_endpoints)")
            continue
        if match and missing:
            violations.append(f"run_mode is fixture and the command names {family} tooling (`{match.group()}`) but {missing} is unset "
                              f"for this command; a fixture must fail closed rather than reach the live {family} account "
                              "(declared in fixture_endpoints)")
    return violations

def _check_remote(segs: list[_Seg], cfg: GuardConfig, root: Path) -> list[str]:
    violations = []
    for seg in segs:
        if not seg.remote:
            continue
        inner = seg.argv[seg.argv.index("--") + 1:] if "--" in seg.argv else seg.argv
        inner_seg = _Seg(argv=inner, words=inner, raw=inner)
        outer_words = [word.rsplit("@", 1)[-1] for word in (*seg.ctx.split(), *seg.argv)]
        hits = [token for token in cfg.legacy_sources
                if any(word.lower() == token.lower() for word in outer_words)]
        remote_sql = "\n".join((seg.text, *seg.heredocs))
        inner_hits, _, _, _ = _conn(inner_seg, cfg, remote_sql)
        hits = list(dict.fromkeys([*hits, *inner_hits]))
        if not hits:
            if inner_seg.argv0 in (*_DBX_CLIENTS, *_LEGACY_ONLY, *_GENERIC):
                violations += _check_sql_client(inner_seg, cfg, root)
            continue
        if not seg.argv:
            violations.append(f"`{seg.remote}` opens an interactive session on legacy source {hits}; the guard cannot read what would run")
            continue
        if seg.argv0 in ("aws", "az") and _remote_payload(seg.args) is None:
            violations.append(f"remote `{seg.argv0}` payload cannot be read (file://, @file, --cli-input-json); the guard cannot clear a "
                              "remote command it cannot read")
            continue
        file = _shell_runs(seg)[1]
        if seg.scripts or file or (_PYTHON.fullmatch(seg.argv0) and any(w.endswith(".py") for w in seg.argv[1:])):
            name = file or (seg.scripts[0] if seg.scripts else next(w for w in seg.argv[1:] if w.endswith(".py")))
            violations.append(f"remote command `{seg.argv0}` runs `{name}` on legacy host {hits}; a script on the remote host cannot be "
                              "read by the guard (run the statements inline)")
            continue
        base = seg.argv0
        if any(">" in op and not (op.endswith("&") and re.fullmatch(r"\d+|-", f))
               for op, f in seg.redirects()):
            violations.append(f"remote command `{base}` on legacy host {hits} redirects output to a file on the legacy host; "
                              "legacy hosts are read-only")
            continue
        if _PYTHON.fullmatch(base):
            text, _ = _python_texts(seg, root)
            if _PY_WRITE.search(text) or re.search(r"\bsubprocess\b|\bos\.(?:system|popen|exec\w*|spawn\w*)\s*\(", text):
                violations.append(f"remote python `{base}` on legacy host {hits} contains a write or subprocess; legacy hosts are read-only")
            continue
        if base in _REST_CLIENTS:
            flags = ("-o", "--output", "-O", "--remote-name", "--remote-name-all", "--output-document", "-D",
                     "--dump-header", "-c", "--cookie-jar", "-d", "--download")
            words = seg.argv[1:]
            if base == "curl":
                flags = tuple(flag for flag in flags if flag not in ("-d", "--download"))
            output = bool(_flag_values(seg.argv, flags)) or any(w in flags for w in words)
            if base == "curl":
                for word in words:
                    if match := re.match(r"^-[A-Za-z]+", word):
                        walked = match.group()[1:]
                        for letter in walked:
                            if letter in _CURL_VALUE_SHORT:
                                output = output or letter in "oODc"
                                break
            elif base in ("http", "https", "xh"):
                output = output or any(w.startswith(("-o", "-d")) and len(w) > 2 for w in words)
            stdout = any(w in ("-O-", "--output-document=-") or
                         w == "-O" and i + 1 < len(words) and words[i + 1] == "-" or
                         re.fullmatch(r"-[A-Za-z]*O-", w)
                         for i, w in enumerate(words))
            if base == "wget" and not stdout:
                output = True
            if output:
                violations.append(f"remote client `{base}` on legacy host {hits} writes output to a file; legacy hosts are read-only")
            continue
        sql_client = base in (*_LEGACY_ONLY, *_GENERIC, "spark-sql", "dbsqlcli")
        if sql_client:
            flags = ("-o", "--output", "-L", "--log-file", "--tee")
            words = seg.argv[1:]
            output = bool(_flag_values(seg.argv, flags)) or any(w in flags for w in words)
            output = output or (base == "bcp" and any(w in ("out", "queryout") for w in seg.argv[1:4]))
            sql, _ = _sql_text(seg, root)
            output = output or bool(re.search(r"(?im)(?:^|[\s;])(?::out|SPOOL(?!\s+OFF\b)|\.EXPORT)\s+\S", sql))
            if output:
                violations.append(f"remote client `{base}` on legacy host {hits} writes query output to a file on the legacy host; "
                                  "legacy hosts are read-only")
                continue
            if _non_reads(sql):
                violations.append(f"remote client `{base}` on legacy host {hits} is not read-only; legacy hosts are read-only")
                continue
        modelled = (*_DBX_CLIENTS, *_LEGACY_ONLY, *_GENERIC, *_REST_CLIENTS)
        read_shape = base in _READERS and not _mutates(base, seg.argv)
        if base == "tar" and any(re.search(r"^-.*[cxru]", w) for w in seg.argv[1:]):
            read_shape = False
        if base in ("unzip", "tee"):
            read_shape = False
        if (base not in modelled and not _PYTHON.fullmatch(base) and base not in _SHELLS
                and not read_shape):
            violations.append(f"remote command `{base}` on legacy host {hits} is not a read shape the guard models; "
                              "legacy hosts are read-only")
    return [_Legacy(v) for v in violations]

def _touch(path: str, at: str | None, root: Path) -> str:
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
    if verb == "push" and any(o in ("--force", "-f", "--force-with-lease") for o in gargv[1:]):
        out.append("`git push --force` rewrites protected remote history; force pushes are never allowed")
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

def _multi_paths(s: _Seg, path: str) -> list[str]:
    paths = [path]
    for name in s.multi:
        joined = s.env.get(name, "")
        if not joined:
            continue
        values = joined.split()
        paths = ([*values] if path == joined else
                 [value + path[len(joined):] for value in values] if path.startswith(joined + "/") else paths)
    return paths

def _writes(s: _Seg, root: Path, here: str, out: list[str]) -> list[tuple[str, str, tuple[str, ...], bool, str | None]]:
    base, argv, at = s.argv0, s.argv, s.at
    ops = [w for w in argv[1:] if not w.startswith("-")]
    values = ops + [w.split("=", 1)[1] for w in argv[1:] if "=" in w]   # `dd of=`, `--output=`
    inplace = _mutates(base, argv)
    w = [(path, f"{op} {path}", _IN, False, at) for op, f in s.redirects() if ">" in op
         for path in _multi_paths(s, f)]
    w += [(path, f"{base} {flag}", _IN, False, at) for flag, v in itertools.pairwise(argv) if flag in _OUTPUT_FLAGS
          for path in _multi_paths(s, v)]
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
    elif base == "find" and _mutates(base, argv):
        w += [(path, "find with an action", _ALL, False, at) for o in ops for path in _multi_paths(s, o)]
    elif base in ("tar", "bsdtar") and (any(re.match(r"-?[a-zA-Z]*x", x) for x in argv[1:2]) or "--extract" in argv or "--get" in argv):
        w += [(path, f"{base} extract into", _ALL, False, at)
              for d in _flag_values(argv, ("-C", "--directory")) or ["."] for path in _multi_paths(s, d)]
    elif base == "unzip" and not any(x in argv for x in ("-l", "-t", "-p", "-z", "-Z")):
        w += [(path, "unzip into", _ALL, False, at)
              for d in _flag_values(argv, ("-d",)) or ["."] for path in _multi_paths(s, d)]
    elif base in _GENERIC or base in _LEGACY_ONLY:
        sql = " ".join([*_flag_values(argv, _SQL_VALUE_FLAGS), *s.heredocs, *s.stdin, s.herestring or ""])
        w += [(path, f"{base} output", _IN, False, at)
              for p in [*values, *_SQL_OUT_PATH.findall(sql)] for path in _multi_paths(s, p)]
    elif base not in _READERS or inplace:
        recursive = base in _RECURSIVE_HEADS and any(re.fullmatch(r"-[a-zA-Z]*[rR][a-zA-Z]*", x) or x in ("--recursive", "--delete")
                                                    for x in argv[1:])
        if "xargs" in s.words and any(_touch(x, at, root) for p in s.feeds for x in p.words):
            out.append(f"`xargs {base}` on names listed from .migration/; ledgers and the allowlist change only through a recorded decision")
        w += [(path, base, _ALL if recursive else _IN, base in _DESTRUCTIVE, at)
              for o in (values[-1:] if base in _WRITE_LAST_OPERAND else values)
              for path in _multi_paths(s, o)]
    return w

def _check_integrity(segs: list[_Seg], root: Path, here: str = "") -> list[str]:
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

def evaluate(command: str, cfg: GuardConfig, root: Path | None = None, cwd: str = "", here: str = "") -> Verdict:
    root = root or Path.cwd()
    violations: list[str] = []
    if m := _PROBE.search(command):
        violations.append(f"`{m.group()}` is the factory-doctor's hook probe; it always blocks so the doctor can tell the "
                          "hook is loaded without touching Databricks")
    segs = _segments(command, at=cwd, root=root)
    violations += _check_unreadable(segs, cfg)
    for seg in segs:
        if seg.unreadable == "depth":
            violations.append(f"scripts nested more than {_MAX_DEPTH} deep; the guard cannot clear what it does not read")
        elif seg.unreadable:
            violations.append(f"shell script(s) {[seg.unreadable]} the command would run cannot be read in full (missing, unreadable or over "
                              f"{_MAX_SCRIPT_BYTES >> 20} MiB); the guard cannot clear what it cannot read")
    violations += _check_identity(segs, cfg) + _check_integrity(segs, root, here) + _check_remote(segs, cfg, root)
    for seg in segs:
        violations += _check_fixture(seg, cfg, root)
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

def evaluate_edit(tool: str, tool_input: dict, cfg: GuardConfig, root: Path, cwd: str) -> Verdict:
    file_path = tool_input.get("file_path")
    if not isinstance(file_path, str) or not file_path:
        return Verdict.of([], cfg)
    new = tool_input.get("content") or tool_input.get("new_string") or "\n".join(
        e.get("new_string", "") for e in tool_input.get("edits", []) if isinstance(e, dict)
    )
    if tool == "MultiEdit":
        old = "\n".join(e.get("old_string", "") for e in tool_input.get("edits", []) if isinstance(e, dict))
    else:
        old = tool_input.get("old_string", "")
    if tool == "write" and not old:
        path = Path(file_path) if os.path.isabs(file_path) else Path(cwd or root) / file_path
        try:
            old = path.read_text(errors="replace")
        except OSError:
            old = ""
    kind = _touch(file_path, cwd or "", root)
    violations = []
    decision = False
    if kind in ("inside", "self"):
        ledger = Path(file_path).name == "06_decisions.md"
        added = new[len(old):] if new.startswith(old) else new
        authorized = "legacy_write_authorized" in added.lower()
        decision = bool(ledger and tool != "MultiEdit" and new.startswith(old) and added and _DECISION_ROW.search(added)
                        and not authorized)
        if not decision:
            if ledger and authorized:
                violations.append("a `legacy_write_authorized` row enters the ledger only through a reviewed PR, never from a session")
            elif ledger:
                violations.append("06_decisions.md is append-only: the edit must keep the existing text and only add `D-<id>` rows")
            else:
                violations.append(f"file-edit tool `{tool}` writes `{file_path}` under .migration/ (only .migration/recon/<unit_id>/ and "
                                  ".migration/waves/ are written by a session; ledgers and the allowlist change only through a recorded "
                                  "decision — 06_decisions.md accepts only an added `D-<id>` row)")
    elif kind == "identity":
        violations.append(f"file-edit tool `{tool}` writes `{file_path}`, the Databricks CLI's credential store; the session runs as the "
                          "doctor-verified migration principal only")
    elif kind == "guard" or kind == "guard-above":
        violations.append(f"file-edit tool `{tool}` on `{file_path}` inside the running guard's plugin tree ({_GUARD_TREE}); the hook is "
                          "never edited, disabled or removed from a session (a block is a finding to report)")
    return Verdict.of(violations, cfg)

def _run_dirs(cmd: str, start: str) -> list[Path | None]:
    dirs: list[Path | None] = []
    for s in _segments(cmd, at=start):
        dirs += [Path(s.at).resolve() if s.at else None if s.at is None else Path(start)] + [Path(a).resolve() for a in s.alts]
    return list(dict.fromkeys(dirs))

def _workspace_from_cd(command: str, start: str) -> tuple[Path, GuardConfig | None]:
    directory = Path(start)
    if any(s.lost for s in _segments(command, at=start)):
        raise ValueError(f"command has more than {_MAX_ALTS} possible working directories; the allowlist in force is unknown")
    for d in _run_dirs(command, start):
        if d is None:
            continue
        directory = d
        try:
            cfg = load_config(directory)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"{exc} (workspace {directory})") from exc
        if cfg is not None:
            return directory, cfg
    return directory, None

def evaluate_with_workdirs(command: str, cfg: GuardConfig, root: Path, cwd: str = "", here: str = "") -> Verdict:
    first = evaluate(command, cfg, root, cwd, here)
    violations = first.violations or ([first.reason] if first.decision == "block" else [])
    seen = {cfg.path}
    segments = _segments(command, at=cwd or str(root))
    if any(s.lost for s in segments):
        violations.append(f"command has more than {_MAX_ALTS} possible working directories; the allowlist in force is unknown")
    unknown_clients = (*_DBX_CLIENTS, *_LEGACY_ONLY, *_GENERIC, "dbt")
    for seg in segments:
        if seg.at is None and (seg.argv0 in unknown_clients or _PYTHON.fullmatch(seg.argv0)):
            violations.append("command changes to a directory the guard cannot resolve before running a Databricks or "
                              "legacy client; the allowlist in force there is unknown")
            break
    for d in _run_dirs(command, cwd or str(root)):
        if d is None:
            continue
        try:
            other = load_config(d)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            violations.append(f"cannot read {CONFIG_REL} for {d}: {exc}")
            continue
        if other is not None and other.path not in seen:
            seen.add(other.path)
            violations += [type(x)(f"[{other.path}] {x}") for x in evaluate(command, other, d).violations
                           if not x.startswith(_AUTHORIZED)]
    return Verdict.of(list(dict.fromkeys(violations)), cfg)

def _dirs(event: dict, tool_input: dict) -> tuple[Path, str, str]:
    def abs_(v: object) -> str:
        return v if isinstance(v, str) and v.startswith("/") and v != "/" else ""
    workdir, cwd = abs_(tool_input.get("workdir")), abs_(event.get("cwd"))
    here = workdir or abs_(os.getcwd()) or str(Path.home())
    project = abs_(os.environ.get("CLAUDE_PROJECT_DIR")) or abs_(os.environ.get("DEVIN_PROJECT_DIR"))
    return Path(project or workdir or here), cwd or workdir, here

def main(stdin_text: str | None = None) -> int:
    raw = stdin_text if stdin_text is not None else sys.stdin.read()
    try:
        event = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        return 0
    tool_input = event.get("tool_input") or {} if isinstance(event, dict) else {}
    command = tool_input.get("command") if isinstance(tool_input, dict) else None
    file_path = tool_input.get("file_path") if isinstance(tool_input, dict) else None
    if (not isinstance(command, str) or not command.strip()) and (not isinstance(file_path, str) or not file_path):
        return 0
    root, cwd, here = _dirs(event, tool_input)
    try:
        cfg = load_config(root)
        if cfg is None and isinstance(file_path, str) and os.path.isabs(file_path):
            cfg = load_config(Path(file_path).parent)
        if cfg is None and isinstance(command, str) and command.strip():
            cwd = cwd or here
            root, cfg = _workspace_from_cd(command, cwd)
    except (OSError, ValueError, json.JSONDecodeError) as exc:   # a broken allowlist is itself a setup violation: refuse rather than guess
        verdict = Verdict("block", f"dbx-migration-factory guard: cannot read {CONFIG_REL}: {exc}")
    else:
        if cfg is None:
            return 0
        verdict = evaluate_edit(event.get("tool_name", ""), tool_input, cfg, root, cwd) if (
            not isinstance(command, str) or not command.strip()
        ) else evaluate_with_workdirs(command, cfg, root, cwd, here)
    if verdict.reason:
        print(json.dumps({"decision": verdict.decision, "reason": verdict.reason}))
    if verdict.decision == "block":
        print(verdict.reason, file=sys.stderr)
    return 2 if verdict.decision == "block" else 0

if __name__ == "__main__":
    sys.exit(main())
