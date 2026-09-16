#!/usr/bin/env python3
"""PreToolUse write-scope guard: tokens -> connection positions -> resolved destinations -> writes -> verdict."""
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

_SEG = r"(?:`[^`]+`|\"[^\"]+\"|\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_$-]*)"
_OBJ = r"TABLE|VIEW|FUNCTION|PROCEDURE|VOLUME|INDEX|TRIGGER|SEQUENCE"
_METASTORE = (r"METASTORE|ANY\s+FILE|SHARE|RECIPIENT|PROVIDER|CONNECTION|EXTERNAL\s+LOCATION|STORAGE\s+CREDENTIAL|SERVICE\s+CREDENTIAL"
              r"|CLEAN\s+ROOM")
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
_USE_CATALOG = r"\bUSE\s+(?:(?:{c})\s+){opt}(?!SCHEMA\b)(" + _SEG + ")"
_IDENTIFIER_LITERAL = re.compile(r"\bIDENTIFIER\s*\(\s*'([^']*)'\s*\)", re.IGNORECASE)
_IDENTIFIER_DYNAMIC = re.compile(r"\bIDENTIFIER\s*\(", re.IGNORECASE)
_EXEC_IMMEDIATE_DYNAMIC = re.compile(r"\bEXEC(?:UTE)?\s+IMMEDIATE\s+(?!')\S", re.IGNORECASE)
_BUNDLE_TARGET = re.compile(r"(?:^|\s)(?:-t|--target)(?:=|\s+)(\S+)")
_PERMISSION = r"^\s*(?:GRANT|REVOKE|DENY)\b.*\bON\s+(?:{c})\b|^\s*(?:CREATE|ALTER|DROP)\s+(?:{c})\b"

_LEGACY_ONLY = ("bteq", "sqlplus", "sqlldr", "snowsql", "mload", "fastload", "fastexport", "tbuild", "tdload")
_LOADERS = ("sqlldr", "mload", "fastload", "tbuild", "tdload")
_WRITERS = ("pg_restore", "pgloader", "liquibase", "flyway", "sqitch")
_GENERIC = ("psql", "pgcli", "sqlcmd", "osql", "isql", "tsql", "mysql", "mariadb", "sqlite3", "bcp", "beeline", "trino", "presto",
            "mssql-cli", "go-sqlcmd", "usql", *_WRITERS)
_CASE_SENSITIVE_DB = ("psql", "pgcli", "mysql", "mariadb", "usql")
_DBX_CLIENTS = ("databricks", "dbx-recon", "spark-sql", "dbsqlcli")
_SQL_CLIENTS = (*_DBX_CLIENTS, *_LEGACY_ONLY, *_GENERIC)
_IDENTITY_CLIENTS = ("databricks", "dbx-recon", "spark-sql")
_PYTHON = re.compile(r"python[0-9.]*|spark-submit")
_REST_CLIENTS = ("curl", "wget", "http", "https", "xh")
_CLIENT_WORD = re.compile(r"(?<![\w-])(?:" + "|".join(map(re.escape, _SQL_CLIENTS)) + r")(?![\w-])")
_SHELLS = ("sh", "bash", "zsh", "dash", "ksh")
_DECLARERS = ("export", "declare", "typeset", "readonly", "local")
_FIXERS = ("ruff", "black", "isort", "autopep8", "yapf", "autoflake")
_DBT_RUNS = ("run", "build", "seed", "snapshot", "run-operation")
_KNOWN = frozenset((*_SQL_CLIENTS, *_REST_CLIENTS, *_SHELLS, *_DECLARERS, *_FIXERS, "dbt", "eval", "source", ".", "docker", "podman",
                    "nerdctl", "kubectl", "perl", "ruby", "node", "java", "patch", "xargs", "for", "select"))
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
_NAME = re.compile(r"[A-Za-z_]\w*")
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
_CONFIG_HOME_VAR = ("HOME", "XDG_CONFIG_HOME")
_FUNCTION_DEF = re.compile(r"(?:^|[;&|\n{}()]\s*)(?:function\s+[\w.-]+|[\w.-]+\s*\(\s*\))\s*\{?([^}]*)")
_SQL_VALUE_FLAGS = ("-c", "-Q", "-q", "-e", "--query", "--sql", "--statement", "--execute", "--command")
_SCRIPT_FLAGS = ("-f", "-i", "--file", "--input")
_HOST_FLAGS = ("-S", "-h", "-H", "--host", "--server", "--hostname", "--url", "-url", "--dsn")
_DB_FLAGS = ("-d", "-D", "--dbname", "--database")
_HOST_ENV = ("PGHOST", "PGHOSTADDR", "PGSERVICE", "MYSQL_HOST", "SQLCMDSERVER")
_DSN_POSITIONAL = ("isql", "usql", "pgloader")
_RECONNECT = re.compile(r"(?im)^[ \t]*(\\c(?:onnect)?|connect|\\r|:connect)[ \t]+([^;\n]*)")
_RUN_FILE = re.compile(r"(?<!\S)@(\S+)|^\s*\.RUN\s+FILE\s*=?\s*(\S+)", re.IGNORECASE | re.MULTILINE)
_DYNAMIC_SQL_EXECUTOR = (r"(?:\bEXEC(?:UTE)?\s+IMMEDIATE|\bsp_executesql|\bEXEC(?:UTE)?\s*\(|"
                         r"\.(?:execute|executemany|sql|run_query|execute_statement)\s*\(|\bstatement\s*=)")
_DYNAMIC_SQL_CALLER = re.compile(_DYNAMIC_SQL_EXECUTOR + r"\s*N?\s*$", re.IGNORECASE)
_PY_LITERAL = re.compile(_DYNAMIC_SQL_EXECUTOR + r"\s*[rbuf]*(['\"]{3}|['\"])(.*?)\1", re.IGNORECASE | re.DOTALL)
_PY_CONNECT = re.compile(r"\b(connect|create_engine|Connection|connection|WorkspaceClient|SparkSession)\s*\(([^)]*)\)", re.IGNORECASE | re.DOTALL)
_PY_ENV = re.compile(r"(?:environ\s*\[\s*['\"]([A-Za-z_]\w*)['\"]\s*\]|getenv\s*\(\s*['\"]([A-Za-z_]\w*)['\"])", re.IGNORECASE)
_PY_DSN = re.compile(r"""['"][^'"]*(?:://|jdbc:|Server=|host=|Database=|Initial Catalog=)[^'"]*['"]""", re.IGNORECASE)
_PY_WRITE = re.compile(r"""['"](?:[wax]\+?|\+?>>?|\+<)['"]|\.write\w*\(|json\.dump\(|os\.(?:remove|unlink|rename|replace|chmod|rmdir|makedirs|mkdir)\(|"""
                       r"""shutil\.|\.(?:unlink|rename|rmdir|mkdir|touch|chmod)\(|\bunlink\b|\bwriteFile\w*\(""")
_MIGRATION_PATH = re.compile(r"[^\s'\"()]*\.migration(?:/[^\s'\"()]*)?")
_DECISION_ROW = re.compile(r"(?m)^\s*(?:\|\s*|#{1,6}\s*)?(D-[A-Za-z0-9][\w.-]*)\b")
_DECISION_ID = re.compile(r"D-[A-Za-z0-9][\w.-]*")
_WRITE_OBJECT = re.compile(
    r"(?is)^\s*(?:DELETE\s+FROM|INSERT\s+INTO|MERGE\s+INTO|UPDATE(?:\s+TOP\s*\([^)]*\)(?:\s+PERCENT)?|\s+STATISTICS|\s+(?:ONLY|LOW_PRIORITY|IGNORE))*|TRUNCATE(?:\s+TABLE)?|"
    r"(?:CREATE|ALTER|DROP)(?:\s+\w+)*?\s+INDEX(?:\s+IF\s+(?:NOT\s+)?EXISTS)?\s+[\w.$\"\[\]`]+\s+ON(?:\s+ONLY)?|"
    r"CREATE(?:\s+OR\s+REPLACE)?\s+(?:\w+\s+)*?(?:TABLE|VIEW|PROCEDURE|FUNCTION|TRIGGER|SEQUENCE|SCHEMA)(?:\s+IF\s+NOT\s+EXISTS)?|"
    r"(?:ALTER|DROP)(?:\s+\w+)*?\s+(?:TABLE|VIEW|PROCEDURE|FUNCTION|TRIGGER|SEQUENCE|SCHEMA)(?:\s+IF\s+EXISTS)?|"
    r"(?:GRANT|REVOKE)\b.*?\bON\s+ALL\s+\w+\s+IN\s+SCHEMA|GRANT\b.*?\bON(?:\s+\w+)?|REVOKE\b.*?\bON(?:\s+\w+)?)\s+([\w.$\"\[\]`]+)")
_WRITE_OBJECT_NEXT = re.compile(r"\s*,\s*([\w.$\"\[\]`]+)")
_AUTHORIZED = "authorized: decision "
_LEGACY_TAIL = " (legacy is read-only in every phase)"

class _Legacy(str):
    """A violation on a legacy source; never downgraded by warn mode."""

_RMTREE = re.compile(r"rmtree\(\s*(?:['\"]([^'\"]*)['\"]|(os\.getcwd\(\)|Path\.cwd\(\)|Path\(\s*(?:['\"]\.?['\"])?\s*\)))")
_SQL_OUT_PATH = re.compile(r"(?i)(?:\bTO\s+|\\[ow]\s+|:out\s+|\bSPOOL\s+|\bFILE\s*=\s*)'?([^\s'\"]*\.migration(?:/[^\s'\"]*)?)")
_SQL_OPAQUE = re.compile(r"--[^\n]*|/\*.*?(?:\*/|\Z)|'(?:[^']|'')*(?:'|\Z)", re.DOTALL)
_SQL_COMMENT = re.compile(r"'(?:[^']|'')*'|\"[^\"]*\"|`[^`]*`|(--[^\n]*|/\*.*?(?:\*/|\Z))", re.DOTALL)
_CLOUD_FAMILY = {
    "aws": (("AWS_",), re.compile(r"(?<![\w.-])(?:aws|boto3|botocore|s3fs|awscli)(?![\w-])|\bs3://")),
    "azure": (("AZURE_", "AZURITE_"), re.compile(r"(?<![\w.-])az(?![\w-])|azure[.-]storage|\babfss://")),
    "gcp": (("GOOGLE_", "GCLOUD_", "GCS_", "CLOUDSDK_", "STORAGE_EMULATOR_HOST", "PUBSUB_EMULATOR_HOST", "FIRESTORE_EMULATOR_HOST"),
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
_TOKEN_PRINTERS = ("auth token", "auth env")
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
_DBX_API_PATH = re.compile(r"^\$[^/\s]*/api/\d")
_HTTP_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")
_CURL_VALUE_SHORT = "dFTHouAebcmwxEKUyYzCDQrtPO"
_OP = re.compile(r"[<>]\(|<<<|<<-|&>>|<<|<>|<&|>>|>&|>\||&>|\|&|\|\||&&|[;|&()<>\n]")
_REDIRECT_OP = re.compile(r"\d*(?:<{1,3}-?|<>|<&|>{1,2}|>&|>\||&>{1,2})")
_STDIN_OP = re.compile(r"0?<")
_SEPARATORS = (";", "&&", "||", "|", "|&", "&", "(", ")", "{", "}", "\n")
_FLAG_WORD = re.compile(r"-{1,2}[\w.-]+(?:=\S*)?")
_UNREADABLE = (f"{{who}} fed script(s) {{files}} that the guard cannot read in full (missing, unreadable or over "
               f"{_MAX_SCRIPT_BYTES >> 20} MiB); inline the SQL or split it so it can be inspected")

_IN, _ALL = ("inside", "self"), ("inside", "self", "above")
_WRITE_LAST_OPERAND = ("cp", "rsync", "install", "ln", "scp")
_RECURSIVE_HEADS = ("rm", "chmod", "chown", "chgrp", "rsync", "chattr", "setfacl")
_DESTRUCTIVE = ("mv", "truncate", "dd", "shred", *_WRITE_LAST_OPERAND, *_RECURSIVE_HEADS, *_FIXERS)
_IN_PLACE = {"sed": (re.compile(r"-[nEersuz]*i.*"), re.compile(r"--in-place.*")), "perl": (re.compile(r"-[a-zA-Z]*i.*"),),
             "awk": (re.compile(r"(?:--?)?inplace"),), "gawk": (re.compile(r"(?:--?)?inplace"),), "mawk": (re.compile(r"(?:--?)?inplace"),),
             "ruff": (re.compile(r"--fix|--fix-only|--unsafe-fixes|format"),)}
_IDENTITY_FILE = re.compile(r"(?:^|/)(?:\.databrickscfg|\.databricks(?:/.*)?|\.config/databricks(?:/.*)?)$")
_GUARD_TREE = Path(os.path.realpath(__file__)).parent.parent
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
_MAX_ALTS = 64
_MAX_DEPTH = 4

def _mutates(base: str, argv: list[str]) -> bool:
    inplace = any(rx.fullmatch(w) for w in argv[1:] for rx in _IN_PLACE.get(base, ())) and not any(w in ("--check", "--diff") for w in argv)
    return inplace or base == "find" and any(w in ("-delete", "-exec", "-execdir", "-ok", "-okdir", "-fls") or w.startswith("-fprint")
                                             for w in argv[1:])

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
        mode, run_mode, endpoints = str(data.get("guard_mode", "block")).lower(), data.get("run_mode", "live"), data.get("fixture_endpoints", [])
        if mode not in ("block", "warn"):
            raise ValueError("'guard_mode' must be 'block' or 'warn'")
        if run_mode not in ("live", "fixture"):
            raise ValueError("'run_mode' must be 'live' or 'fixture'")
        if not isinstance(endpoints, list) or not all(isinstance(v, str) for v in endpoints):
            raise ValueError("'fixture_endpoints' must be a list of strings")
        for endpoint in endpoints:
            if not any(endpoint.startswith(prefix) for prefixes, _ in _CLOUD_FAMILY.values() for prefix in prefixes):
                raise ValueError(f"fixture_endpoints: cannot tell the cloud family of {endpoint}")
        return cls(catalogs=[_norm(c) for c in catalogs], legacy_sources=lists["legacy_sources"], mode=mode,
                   forbidden_bundle_targets=tuple(t.lower() for t in lists["forbidden_bundle_targets"]),
                   target_hosts=lists["target_hosts"], bundle_targets=lists["bundle_targets"],
                   lakebase_projects=[p.removeprefix("projects/") for p in lists["lakebase_projects"]],
                   lakebase_branches=lists["lakebase_branches"], run_mode=run_mode, fixture_endpoints=tuple(endpoints), path=path)

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
            return cls("approve", "dbx-migration-factory guard: " + "; ".join(notes) if notes else "", violations)
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
    """Blank comments and string literals, keeping a literal that is the argument of a dynamic-SQL executor."""
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

def _strip_sql_comments(sql: str) -> str:
    return _SQL_COMMENT.sub(lambda m: m.group() if m.group(1) is None else re.sub(r"[^\n]", " ", m.group(1)), sql)

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
                    i += sum(len(ln) + 1 for ln in lines[:len(body) + 1])
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
    """One simple command: words and raw words (redirections and heredoc bodies included), what feeds its stdin, and what it runs."""
    words: list[str] = field(default_factory=list)
    raw: list[str] = field(default_factory=list)
    feeds: list[_Seg] = field(default_factory=list)
    argv: list[str] = field(default_factory=list)
    assigns: list[str] = field(default_factory=list)   # VAR=value prefixes (`env -u X` as `X=`, `env -C d` as `PWD=d`)
    stdin: list[str] = field(default_factory=list)     # literal text piped in
    opaque: str | None = None                          # a piped producer the guard cannot read
    scripts: list[str] = field(default_factory=list)
    ctx: str = ""                                      # prefixes / wrapper it runs under
    remote: str = ""                                   # remote execution wrapper
    env: dict[str, str] = field(default_factory=dict)
    multi: frozenset[str] = frozenset()                # `for` variables: every value at once
    at: str | None = ""                                # directory ('' the root, None unresolvable)
    alts: list[str] = field(default_factory=list)      # candidates when `at` is None after `x || cd d`
    sub: int = 0
    after: str = ""
    bg: int = 0
    lost: bool = False
    unreadable: str = ""

    argv0 = property(lambda s: s.argv[0].rsplit("/", 1)[-1] if s.argv else "")
    heredocs = property(lambda s: [f for op, f in s.redirects() if op.endswith(("<<", "<<-"))])
    herestring = property(lambda s: next((f for op, f in s.redirects() if op.endswith("<<<")), None))
    text = property(lambda s: " ".join([*s.words, *s.stdin, s.herestring or "", s.ctx]))
    args = property(lambda s: [w for i, w in enumerate(s.words)
                               if not (_REDIRECT_OP.fullmatch(w) or (i and _REDIRECT_OP.fullmatch(s.words[i - 1])))])

    def redirects(self) -> list[tuple[str, str]]:
        return [(op, f) for op, f in itertools.pairwise(self.words) if _REDIRECT_OP.fullmatch(op)]

    def raw_of(self, word: str) -> str:
        return self.raw[self.words.index(word)] if word in self.words else word

def _commands(cmd: str) -> list[_Seg]:
    toks, raws = _shell_tokens(cmd)
    out: list[_Seg] = []
    groups: list[tuple[int, list[_Seg]]] = []   # (index of the first member, stdin the group inherits)
    feed: list[_Seg] = []                        # what the next command's stdin receives
    closed: list[_Seg] = []                      # members of the group just closed
    lists: list[int] = [0]                       # first member of the and/or list open at each group level
    cur, i, sub, after, bgs = None, 0, 0, "", 0
    while i < len(toks):
        tok, n = toks[i], 1 + bool(_REDIRECT_OP.fullmatch(toks[i]))
        if n == 1 and tok == "\n" and cur is None and feed and not closed:
            pass                                        # a line break after `|` continues the pipeline
        elif n == 1 and tok in _SEPARATORS and (tok not in ("{", "}") or cur is None):
            if tok == "&":
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
    if project is not None and project not in cfg.lakebase_projects:
        return [f"`databricks postgres {verb}` targets Lakebase project {project!r} outside allowed lakebase_projects "
                f"{cfg.lakebase_projects} (empty = every Lakebase write blocks)"]
    if branch == "production":
        return [f"`databricks postgres {verb}` on the `production` branch of Lakebase project {project or '?'}; migration sessions "
                "write only per-batch branches (production is repointed at STOP E)"]
    if branch is not None and cfg.lakebase_branches and not any(fnmatch.fnmatchcase(branch, p) for p in cfg.lakebase_branches):
        return [f"`databricks postgres {verb}` on branch {branch!r} of Lakebase project {project or '?'}; allowed lakebase_branches "
                f"{cfg.lakebase_branches}"]
    return []

def _json(text: str):
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return None

def _lines(value) -> str | None:
    return value if isinstance(value, str) else "\n".join(value) if isinstance(value, list) and all(isinstance(x, str) for x in value) else None

def _remote_payload(words: list[str]) -> str | None:
    if words[:3] == ["aws", "ssm", "send-command"] and "--cli-input-json" not in words:
        params = next((words[i + 1] for i, w in enumerate(words[:-1]) if w == "--parameters"), None)
        if params is None or params.startswith(("file://", "@")):
            return None
        if params.startswith("commands="):
            value = params.split("=", 1)[1]
            return _lines(_json(value)) if value[:1] in ('[', '"') else value
        parsed = _json(params) if params.startswith("{") else None
        return _lines(parsed.get("commands")) if isinstance(parsed, dict) else None
    if words[:4] == ["az", "vm", "run-command", "invoke"] and not any(w.startswith("@") for w in words) and "--scripts" in words:
        scripts = list(itertools.takewhile(lambda w: not w.startswith("-"), words[words.index("--scripts") + 1:]))
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
            return (words, "") if payload is None else (["sh", "-c", payload], " ".join(words))
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
    """(inline text a shell would run, script file it would run, whether the text is built at run time)."""
    argv, base = seg.argv, seg.argv0
    if base == "eval" and len(argv) > 1:
        return (None if (built := any(_expands(seg.raw_of(w)) for w in argv[1:])) else " ".join(argv[1:])), None, built
    if base in ("source", ".") and len(argv) > 1:
        return None, argv[1], False
    if argv and (argv[0].startswith(("./", "../")) and base not in _KNOWN or base.endswith(".sh")):
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
    """Every simple command the text runs (scripts it sources or runs included), with directory and environment resolved."""
    text = re.sub(r"\\\r?\n", " ", text)
    aliases: dict[str, list[str]] = {}
    env = dict(_ENV_DEFAULTS) if env is None else env
    multi = set() if multi is None else multi
    out: list[_Seg] = []
    dirs: list[str | None] = []
    alts: list[str] = []
    scopes: list[tuple] = []                                   # state outside each open subshell
    snapshot = lambda: (at, list(alts), list(dirs), dict(env), set(multi))  # noqa: E731
    bg, saved, before, lost = 0, snapshot(), [at], False

    def restore(state):
        nonlocal at, alts, dirs, before
        at, alts, dirs, saved_env, saved_multi = state
        env.clear(); env.update(saved_env)
        multi.clear(); multi.update(saved_multi)
        before = [at] if at is not None else alts
    for seg in _commands(text):
        if seg.bg != bg:                                      # a `&` list runs in a subshell: what it changes ends with it
            if bg:
                restore(saved)
            bg, saved = seg.bg, snapshot()
        while len(scopes) < seg.sub:
            scopes.append(snapshot())
        while len(scopes) > seg.sub:                          # `( ... ) || cd d`: the parent is where it was before the group
            restore(scopes.pop())
        seg.words = [w if not env or "$" not in w or "$" not in re.sub(r"\\.|'[^']*'?", "", r) else
                     _SHELL_VAR.sub(lambda m: env.get(m.group(1) or m.group(2), m.group()), w) for w, r in zip(seg.words, seg.raw)]
        seg.argv, seg.ctx = _program(aliases.get(seg.args[0] if seg.args else "", seg.args[:1]) + seg.args[1:], seg.assigns)
        seg.ctx = " ".join(x for x in (ctx, seg.ctx) if x)
        wrapper = seg.ctx.split()[:1]
        if wrapper and (wrapper[0] in ("ssh", "aws", "az") or wrapper[0] in ("docker", "podman", "nerdctl", "kubectl")
                        and any(w in seg.ctx.split() for w in ("exec", "run"))):
            seg.remote = wrapper[0]
        elif seg.argv[:3] == ["aws", "ssm", "send-command"] or seg.argv[:4] == ["az", "vm", "run-command", "invoke"]:
            seg.remote = seg.argv0
        for a in (seg.assigns if not seg.argv else seg.argv[1:] if seg.argv0 in _DECLARERS else ()):
            if _ASSIGN.match(a):
                name, value = a.split("=", 1)
                env[name] = value if name not in multi else f"{env.get(name, '')} {value}".strip()
        if seg.argv0 == "unset":
            env.update((name, "") for name in seg.argv[1:] if _NAME.fullmatch(name) and name not in multi)
        seg.env, seg.multi = dict(env), frozenset(multi)
        for p in seg.feeds:
            pargs, base = p.args, p.args[0].rsplit("/", 1)[-1] if p.args else ""
            if base not in ("cat", "echo", "printf", "tee") or _expands(" ".join(p.raw)):
                seg.opaque = base or "?"
            elif base in ("echo", "printf"):
                seg.stdin.append(" ".join(w for w in pargs[1:] if not re.fullmatch(r"-[neE]+", w)))
            elif base == "cat":
                seg.stdin.extend(p.heredocs)
        seg.scripts = _scripts_of(seg, seg.argv + [w for op, f in seg.redirects() for w in (op, f)] if seg.ctx else None)
        seg.at = next((_join(at, a[4:]) for a in reversed(seg.assigns) if a.startswith("PWD=")), at)   # `env -C dir`
        seg.alts, seg.lost = list(alts), lost
        now: list[str | None] = [at] if at is not None else alts
        if seg.argv0 in ("cd", "pushd"):
            args = [w for w in seg.argv[1:] if not (w.startswith("-") and len(w) > 1)]
            dirs += [at] if seg.argv0 == "pushd" else []
            moved = list(dict.fromkeys((now if seg.after == "||" else []) + [_join(p, args[0] if args else "~") for p in (before if seg.after == "||" else now)]))
            at = moved[0] if len(moved) == 1 else None
            alts = [] if at is not None else [p for p in moved if p is not None]
            if len(alts) > _MAX_ALTS:
                alts, lost = [], True
        elif seg.argv0 == "popd":
            at, alts = (dirs.pop() if dirs else None), []
        elif seg.argv0 == "alias":
            aliases.update((a.split("=", 1)[0], shlex.split(a.split("=", 1)[1])) for a in seg.argv[1:] if "=" in a)
        elif seg.argv0 == "for" and len(seg.argv) >= 4 and seg.argv[2] == "in" and _NAME.fullmatch(seg.argv[1]):
            env[seg.argv[1]] = " ".join(seg.argv[3:])
            multi.add(seg.argv[1])
        before = now
        out.append(seg)
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
                out.extend(_segments(body, seg.ctx, depth + 1, env if shared else dict(env), seg.at, multi if shared else set(multi), root))
    return out

def _is_token(word: str, token: str) -> bool:
    return bool(re.search(rf"(?<![\w-]){re.escape(token)}(?![\w-])", word, re.IGNORECASE))

def _mentions(seg: _Seg, cfg: GuardConfig) -> bool:
    """Whether the command names a client or a legacy source anywhere in its arguments (the coarse gate for opacity rules)."""
    return any(_CLIENT_WORD.search(w) or "--target-catalog" in w or any(_is_token(w, t) for t in cfg.legacy_sources)
               for w in (seg.argv or seg.args))

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
        match, names = _WRITE_OBJECT.match(statement), []
        while match:
            names.append(re.sub(r'["`\[\]]', "", match.group(1)))
            match = _WRITE_OBJECT_NEXT.match(statement, match.end())
        objects.append(names)
    return objects

def _decision(seg: _Seg, statements: list[str], root: Path) -> tuple[str | None, str | None]:
    """(decision id, why it does not authorize these statements; None when it does)."""
    did = next((a.split("=", 1)[1] for a in reversed(seg.assigns) if a.startswith("DBX_DECISION=")), None)
    if not did:
        return None, "no `DBX_DECISION=D-<id>` prefix on the command"
    try:
        ledger = (root / ".migration" / "06_decisions.md").read_text(errors="replace") if _DECISION_ID.fullmatch(did) else ""
    except OSError:
        return did, "cannot read .migration/06_decisions.md"
    row = next((ln for ln in ledger.splitlines() if (m := _DECISION_ROW.search(ln)) and m.group(1).lower() == did.lower()), None)
    if row is None:
        return did, f"`{did}` is not a row in .migration/06_decisions.md"
    if "legacy_write_authorized" not in row.lower():
        return did, f"row `{did}` does not contain `legacy_write_authorized`"
    objects = _write_objects(statements)
    for names, statement in zip(objects, statements):
        if not names:
            return did, f"cannot tell which object `{statement[:60]}` writes"
        for obj in names:
            if obj.startswith("$") or obj.endswith("$") or re.search(r"[$:&@]\{?\(", statement):
                return did, f"`{obj}` is a run-time substitution; the decision must name the literal object"
            if not re.search(rf"(?<![\w.]){re.escape(obj)}(?![\w.])", re.sub(r'["`\[\]]', "", row), re.IGNORECASE):
                return did, f"row `{did}` does not name `{obj}`"
    return did, None

def _host(value: str) -> str:
    value = re.sub(r"^(?:tcp|np|lpc):", "", value, flags=re.IGNORECASE)
    return re.split(r"[,:\\]", re.sub(r"^\$\{?(\w+)\}?$", r"\1", value), 1)[0]

def _conn(seg: _Seg, cfg: GuardConfig, sql: str) -> tuple[list[str], list[str], set[str], set[str], list[str]]:
    """Connection positions of a SQL client: (legacy hits, hosts, secret names among the hosts, databases, unresolved values).
    Hosts are lower-cased literals or case-preserved `$SECRET` names; databases keep their case for dialect-aware matching."""
    argv, stripped = seg.argv, _strip_sql_comments(sql)
    joined = " ".join([*argv[1:], *seg.assigns, stripped])
    recon = [(v.lower(), a.split()) for v, a in _RECONNECT.findall(stripped)]
    raw_hosts = [*_flag_values(argv, _HOST_FLAGS), *(a.split("=", 1)[1] for a in seg.assigns if a.split("=", 1)[0] in _HOST_ENV),
                 *(w for w in argv[1:2] if seg.argv0 in _DSN_POSITIONAL and not w.startswith("-")),
                 *(w for i, w in enumerate(argv[1:], 1) if re.fullmatch(r"\$\{?\w+\}?", w) and (not argv[i - 1].startswith("-") or argv[i - 1] in _HOST_FLAGS)),
                 *(w for v, ws in recon for w in (ws[:1] if v == ":connect" else ws[1:2]) if not re.search(r"=|://", w)),
                 *re.findall(r"://(?:[^@/\s]*@)?([^:/?\s;]+)", joined),
                 *re.findall(r"(?i)\b(?:host|hostaddr|server|data source|addr)=([^;\s]+)", joined),
                 *re.findall(r"(?im)^\s*\.LOGON\s+([^/\s;]+)", stripped)]
    raw_dbs = [*(v for v in _flag_values(argv, _DB_FLAGS) if "=" not in v),
               *(ws[0] for v, ws in recon if v != ":connect" and ws and not re.search(r"=|://", ws[0])),
               *re.findall(r"(?i)(?<![-\w])(?:database|dbname|initial catalog)=([^;\s]+)", joined),
               *re.findall(r"(?i)\bUSE\s+(?:\[?DATABASE\]?\s+)?([A-Za-z_][\w$.-]*)", stripped),
               *re.findall(r"://[^/\s]+/([^/?\s;]+)", " ".join(argv[1:]))]
    known = set(cfg.target_hosts) | set(cfg.legacy_sources)
    secrets = {m.group(1) or m.group(2) for w in [*argv, *seg.assigns] for m in _SHELL_VAR.finditer(w)}
    unresolved = [v for v in raw_hosts + raw_dbs if "`" in v or "$(" in v or any((m.group(1) or m.group(2)) not in known for m in _SHELL_VAR.finditer(v))]
    hosts = [(_host(v) if _SHELL_VAR.fullmatch(v) else _host(v).lower()) for v in raw_hosts if v]
    dbs = {v.strip("'\"") for v in raw_dbs if v.strip("'\"")}
    exact = seg.argv0 in _CASE_SENSITIVE_DB
    on_target = bool(hosts) and all(
        h in cfg.target_hosts if _SHELL_VAR.fullmatch(v) else h in {t.lower() for t in cfg.target_hosts}
        for h, v in zip(hosts, raw_hosts)
    )
    hits = [t for t in cfg.legacy_sources if t in secrets or any(h == t.lower() for h, v in zip(hosts, raw_hosts) if not _SHELL_VAR.fullmatch(v))
            or (not on_target and any(d == t if exact else d.lower() == t.lower() for d in dbs))]
    return hits, hosts, secrets & set(hosts), dbs, unresolved

def _check_unreadable(segs: list[_Seg], cfg: GuardConfig, text: str) -> list[str]:
    """Constructs that only produce the statement at run time, plus scripts the guard could not read."""
    violations, defined = [], {s.argv0 for s in segs if len(s.argv) == 1}
    for index, s in enumerate(segs):
        ctx = _mentions(s, cfg)
        base, _, _, built = s.argv0, *_shell_runs(s)
        if s.unreadable == "depth":
            violations.append(f"scripts nested more than {_MAX_DEPTH} deep; the guard cannot clear what it does not read")
        elif s.unreadable:
            violations.append(f"shell script(s) {[s.unreadable]} the command would run cannot be read in full (missing, unreadable or over "
                              f"{_MAX_SCRIPT_BYTES >> 20} MiB); the guard cannot clear what it cannot read")
        if built:
            violations.append(f"`{base}{' -c' if base in _SHELLS else ''}` on a runtime-built string; the guard cannot read what it would run")
        if base in ("cp", "ln", "mv", "install"):   # `cp $(which sqlcmd) x`: `$(` splits into later segments
            raw = " ".join(s.raw_of(w) for w in s.argv[1:])
            later = " ".join(w for other in segs[index + 1:] for w in other.argv) if any("$" in w for w in s.raw) else ""
            if m := _CLIENT_WORD.search(raw) or _CLIENT_WORD.search(later):
                violations.append(f"`{base}` copies or renames client `{m.group()}` under another name; run the client by its own name so "
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
                and (wrapped := _CLIENT_WORD.search(" ".join(s.argv[1:]))):
            violations.append(f"unrecognised wrapper `{base}` in front of client `{wrapped.group()}`; the guard has no rule for `{base}`, so "
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
        if base not in _KNOWN and base not in _INERT and base not in defined and not _PYTHON.fullmatch(base) and (hits := _conn(s, cfg, "")[0]):
            violations.append(f"unrecognised command `{base}` names legacy connection {hits}; the guard cannot verify its operation")
    if any(_CLIENT_WORD.search(m.group(1)) for m in _FUNCTION_DEF.finditer(re.sub(r'"(?:[^"\\]|\\.)*"|\'[^\']*\'', " ", text))):
        violations.append("shell function wrapping a SQL client; the guard cannot follow what a call of it would run (same class as `eval`)")
    return violations

def _check_sql_client(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    """A legacy-only client or a legacy connection runs read shapes only; elsewhere a write needs every host in target_hosts and an
    allowlisted catalog / database."""
    base, tail = seg.argv0, _LEGACY_TAIL
    if base in _LOADERS:
        return [_Legacy(f"`{base}` is a loader: nothing but reads ever runs against a legacy source" + tail)]
    sql, unreadable = _sql_text(seg, root, [seg.argv[1]] if base == "bcp" and len(seg.argv) > 2 and "queryout" in seg.argv[2:4] else [])
    hits, hosts, secrets, dbs, unresolved = _conn(seg, cfg, sql)
    legacy, non_reads, loading = base in _LEGACY_ONLY, _non_reads(sql), base == "bcp" and "in" in seg.argv[1:4]
    bad = [b.split("\n", 1)[0][:80] for b in [
        *([f"stdin from `{seg.opaque}`, a program or expansion the guard cannot read"] if seg.opaque else []),
        *([f"{base} (a migration tool: every run writes its target)"] if base in _WRITERS else []),
        *(["bcp ... in (loader)"] if loading else []), *non_reads]]
    violations = [_Legacy(_UNREADABLE.format(who="legacy client", files=unreadable) + tail)] if unreadable and (legacy or hits) else []
    if unreadable and not violations:
        bad.insert(0, f"script(s) {unreadable} the guard cannot read")
    if not bad:
        return violations
    if legacy or hits:
        violation = (f"non-read statement through a legacy-only client `{base}`" if legacy
                     else f"non-read statement against legacy source {hits}") + f": `{bad[0]}`"
        plain = non_reads and not unreadable and not seg.opaque and base not in _WRITERS and not loading
        did, missing = _decision(seg, non_reads, root) if plain else (None, "the statement is not a readable SQL write")
        if missing is None:
            objects = [obj for names in _write_objects(non_reads) for obj in names]
            violations.append(f"{_AUTHORIZED}{did} authorizes the legacy write of {', '.join(objects)} "
                              "(legacy_write_authorized row in .migration/06_decisions.md)")
        else:
            violations.append(_Legacy(f"{violation}; a recorded decision would allow it, but {missing}{tail}"))
    elif unresolved:
        violations.append(f"non-read statement through `{base}` to a connection built at run time ({sorted(set(unresolved))[:4]} is not a "
                          f"literal and not a name in target_hosts / legacy_sources): `{bad[0]}`; spell the host and database out so the guard "
                          "can resolve them")
    elif not hosts or not all(h in cfg.target_hosts if h in secrets else h in {t.lower() for t in cfg.target_hosts} for h in hosts):
        violations.append(f"non-read statement through `{base}` to a host that is not a literal in target_hosts {cfg.target_hosts} "
                          f"(seen: {sorted(set(hosts))[:6]}; every host must be listed, an empty list blocks every write): `{bad[0]}`")
    else:
        flagged = [v for v in _flag_values(seg.argv, _DB_FLAGS) if "=" not in v]
        used = set(re.findall(r"(?i)\bUSE\s+(?:\[?DATABASE\]?\s+)?([A-Za-z_][\w$.-]*)", _strip_sql_comments(sql)))
        default = flagged[0] if flagged and not dbs - set(flagged) - used else next(iter(dbs)) if len(dbs) == 1 else None
        violations += _catalog_violations(sql, cfg, _norm(default) if default else None, f"`{base}` client")
    return violations

def _catalog_violations(sql: str, cfg: GuardConfig, default: str | None, who: str) -> list[str]:
    """Writes whose catalog (three-part name, `USE CATALOG`, or `default`) is outside the allowlist; `who` '' checks only explicit
    catalogs, a Databricks client also blocks catalog grants, any other client blocks every grant and container change."""
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
        grant = re.match(r"^\s*(?:GRANT|REVOKE|DENY)\b", stmt, re.IGNORECASE)
        if _IDENTIFIER_DYNAMIC.search(stmt):
            violations.append(f"IDENTIFIER(<non-literal>) names the target of a write at run time: `{head}`")
        elif (who == "Databricks client" and grant and re.search(r"\bON\s+CATALOG\b", stmt, re.IGNORECASE)) or (
                who and who != "Databricks client" and (
                grant and (who != "Databricks SQL client" or not re.search(r"\bON\s+TABLE\b", stmt, re.IGNORECASE)) or
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

def _strings(value, key: str = ""):
    """(lower-cased key, string) pairs of a JSON document."""
    if isinstance(value, str):
        yield key, value
    elif isinstance(value, dict):
        for k, child in value.items():
            yield from _strings(child, str(k).lower())
    elif isinstance(value, list):
        for child in value:
            yield from _strings(child, key)

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
        sql, unreadable = _sql_text(seg, root, [w for w in path[4 if group == "experimental" else 2:] if w != "--"])
        return ([_UNREADABLE.format(who="Databricks client", files=unreadable)] if unreadable else []) + _catalog_violations(
            sql, cfg, next(map(_norm, _flag_values(seg.argv, ("--catalog",))), None), "Databricks SQL client" if group == "sql" else "Databricks client")
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
            payloads = [argv[n + 1] for n, w in enumerate(argv[:-1]) if w == "--json"] + [w.removeprefix("--json=") for w in argv if w.startswith("--json=")]
            if _expands(" ".join(map(seg.raw_of, payloads))) or any(v.startswith("@") for v in payloads):
                return [f"`databricks postgres {verb}` JSON payload must be literal (fail closed): no expansion, no `@file`"]
            for payload in payloads:
                if (parsed := _json(payload)) is None:
                    return [f"`databricks postgres {verb}` JSON payload is not parseable JSON (fail closed)"]
                for k, value in _strings(parsed):
                    ref = _LAKEBASE_REFERENCE.search(value)
                    for project, branch in ((value.removeprefix("projects/").split("/")[0], None) if value.startswith("projects/") else (None, None),
                                            ref.groups() if ref else (None, None), (value if "project" in k else None, None),
                                            (None, value if "branch" in k and not k.startswith("source") else None)):
                        if (project, branch) != (None, None) and (violation := _lakebase(verb, project, branch, cfg)):
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
    violations, tail = [], "; the session runs as the doctor-verified migration principal only"
    trusted = {n.lower() for n in [*cfg.target_hosts, *cfg.legacy_sources] if _NAME.fullmatch(n)} if cfg else set()
    for s in segs:
        client = s.argv0 in _IDENTITY_CLIENTS
        persistent = s.argv0 in ("export", "unset", "declare", "typeset", "setenv") or not s.argv
        names = [a.split("=", 1)[0] for a in s.assigns] + ([w.split("=", 1)[0] for w in s.argv[1:] if not w.startswith("-")] if persistent else [])
        for n in names:
            if n.lower() in trusted:
                violations.append(f"`{n}` is a name the allowlist trusts (target_hosts / legacy_sources); "
                                  f"{'unsetting' if s.argv0 == 'unset' else 'assigning'} it would make that name stand for a different endpoint" + tail)
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
    texts, unreadable = [*_flag_values(argv, ("-c",)), *seg.heredocs], None
    if "-m" not in argv and not texts and (script := next((w for w in argv[1:] if w.endswith(".py")), None)):
        if (body := _read_script(script, root, seg.at)) is None:
            unreadable = script
        else:
            texts.append(body)
    return "\n".join(texts), unreadable

def _strip_python_comments(text: str) -> str:
    lines = text.splitlines(keepends=True)
    try:
        for tok in tokenize.generate_tokens(io.StringIO(text).readline):
            if tok.type == tokenize.COMMENT:
                (line, col), (_, end) = tok.start, tok.end
                lines[line - 1] = lines[line - 1][:col] + " " * (end - col) + lines[line - 1][end:]
    except (IndentationError, tokenize.TokenError, SyntaxError):
        pass
    return "".join(lines)

def _check_python(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    text, unreadable = _python_texts(seg, root)
    if unreadable and seg.argv0 == "spark-submit":
        return [f"spark-submit script {unreadable!r} cannot be read; the guard cannot clear a Spark job it cannot inspect"]
    text = _strip_python_comments(text)
    calls = list(_PY_CONNECT.finditer(text))
    env_names = [m.group(1) or m.group(2) for m in _PY_ENV.finditer(text)]
    call_conn = " ".join(m.group(2) for m in calls)
    conn = _PY_ENV.sub(" ", " ".join([call_conn, *(m.group() for m in _PY_DSN.finditer(text))]))
    host_values = [m.group(1) or m.group(2) for m in re.finditer(
        r"""(?i)(?:host|server|data source)\s*=\s*['"]?([^'";,\s)]+)|://(?:[^@/\s]*@)?([^:/?\s;'"}]+)""", conn)]
    hosts = [h.lower() for h in host_values]
    foreign = [h for h in hosts if h not in {t.lower() for t in cfg.target_hosts}]
    resolved_target = bool(hosts) and not foreign or any(n in cfg.target_hosts for n in env_names)
    db_pattern = r"""(?i)(?:dbname|database|initial catalog)\s*=\s*['"]?([^'";,\s)]+)|://[^/\s'"}]+/([^?\s'";]+)"""
    db_values = [m.group(1) or m.group(2) for m in re.finditer(db_pattern, call_conn)]
    dbs = [d.strip("'\"") for d in db_values if d.strip("'\"")]
    pg = bool(re.search(r"\b(?:psycopg2?|asyncpg|pg8000)\b", text, re.IGNORECASE))
    keys = set(dbs) if pg else {d.lower() for d in dbs}
    default = _norm(dbs[0]) if len(keys) == 1 else None
    def blank_db(match):
        value = match.group(1) or match.group(2)
        start = (match.start(1) if match.group(1) else match.start(2)) - match.start()
        return match.group(0)[:start] + " " * len(value) + match.group(0)[start + len(value):]
    db_free = re.sub(db_pattern, blank_db, conn)
    argv_words = [_host(w) for w in seg.argv[1:] if not w.startswith("-")]
    hits = [t for t in cfg.legacy_sources if t in env_names or _is_token(db_free, t) or
            any(w.lower() == t.lower() for w in argv_words) or
            (not resolved_target and any(d == t if pg else d.lower() == t.lower() for d in dbs))]
    known = set(cfg.target_hosts) | set(cfg.legacy_sources)
    unresolved = any(n not in known for n in env_names) or any(
        m.group(1).lower() in ("connect", "create_engine") and not re.search(r"""['"]""", m.group(2))
        and not _PY_ENV.search(m.group(2)) for m in calls)
    databricks = bool(re.search(r"\b(?:databricks|spark)\b", text + " " + " ".join(seg.argv), re.IGNORECASE))
    literals = list(_PY_LITERAL.finditer(text))
    if not (calls or env_names or _PY_DSN.search(text)):
        return [v for m in literals for v in _catalog_violations(m.group(2), cfg, None, "")]
    statements = [m.group(2) for m in literals]
    opaque = False
    for match in re.finditer(_DYNAMIC_SQL_EXECUTOR, text, re.IGNORECASE):
        if any(match.start() == literal.start() for literal in literals):
            continue
        tail = text[match.end():].lstrip()
        name = None
        if tail.startswith("("):
            call = re.match(r"\(\s*([A-Za-z_]\w*)\s*\)", tail)
            if call:
                name = call.group(1)
            else:
                opaque = True
        elif (call := re.match(r"([A-Za-z_]\w*)\b", tail)):
            name = call.group(1)
        else:
            opaque = True
        if name:
            prefix = text[:match.start()]
            assignments = list(re.finditer(
                rf"(?<![\w.]){name}\s*(?:(?P<plain>=(?!=))|(?P<compound>[+\-*/%|&]=))", prefix))
            literals = []
            for assignment in assignments:
                if not assignment.group("plain"):
                    literals = []
                    break
                literal = re.match(
                    rf"{re.escape(name)}\s*=\s*[rbuf]*(['\"]{{3}}|['\"])(.*?)\1",
                    prefix[assignment.start():], re.S)
                if literal is None:
                    literals = []
                    break
                literals.append(literal.group(2))
            if assignments and literals and len(assignments) == len(literals):
                statements.extend(literals)
            else:
                opaque = True
    violations = []
    for statement in statements:
        if bad := _non_reads(statement):
            if hits:
                violations.append(_Legacy(f"non-read statement against legacy source {hits} in a program: `{bad[0][:80]}`" + _LEGACY_TAIL))
            elif unresolved:
                violations.append(f"non-read statement in a program on a connection built at run time: `{bad[0][:80]}`; spell the connection "
                                  "out (a literal host or a secret name in target_hosts) so the guard can resolve it")
            elif foreign or not (databricks or resolved_target):
                violations.append(f"non-read statement in a program to host(s) {sorted(set(hosts))} not in target_hosts {cfg.target_hosts}: "
                                  f"`{bad[0][:80]}`")
            elif databricks and not resolved_target:
                violations += _catalog_violations(statement, cfg, None, "Databricks client")
            else:
                violations += _catalog_violations(statement, cfg, default, "program")
    if opaque and hits:
        violations.append(_Legacy(f"statement built at run time against legacy source {hits} in a program" + _LEGACY_TAIL))
    elif opaque and not (not databricks and not unresolved and not foreign and resolved_target and default and
                         default in {_norm(c) for c in cfg.catalogs}):
        violations.append("Python statement or connection is built at run time; the guard cannot resolve a non-read statement")
    return violations

def _check_fixture(seg: _Seg, cfg: GuardConfig, root: Path) -> list[str]:
    if cfg.run_mode != "fixture" or not cfg.fixture_endpoints or seg.argv0 in _INERT or (
            seg.argv0 in (*_SHELLS, "eval") and _shell_runs(seg)[0] is not None):
        return []
    text = seg.text + ("\n" + _python_texts(seg, root)[0] if _PYTHON.fullmatch(seg.argv0) else "")
    local: dict[str, str] = {}
    for a in seg.assigns:   # `X=$Y Y=1 cmd`: a prefix sees only the prefixes before it
        if _ASSIGN.match(a):
            name, value = a.split("=", 1)
            local[name] = _SHELL_VAR.sub(lambda m: {**os.environ, **seg.env, **local}.get(m.group(1) or m.group(2), m.group()), value)
    violations = []
    for family, (prefixes, pattern) in _CLOUD_FAMILY.items():
        if not (match := pattern.search(text)):
            continue
        declared = [n for n in cfg.fixture_endpoints if n.startswith(prefixes)]
        effective = {}
        for name in declared:
            lookup = {**os.environ, **{k: v for k, v in seg.env.items() if k != name}}
            effective[name] = local[name] if name in local else _SHELL_VAR.sub(
                lambda m: lookup.get(m.group(1) or m.group(2), m.group()), seg.env.get(name, os.environ.get(name, "")))
        missing = [n for n in declared if not effective[n]]
        unresolved = [n for n in declared if (n in local or n in seg.env) and effective[n] and ("$" in effective[n] or "`" in effective[n])]
        if unresolved:
            violations.append(f"run_mode is fixture and the command names {family} tooling (`{match.group()}`) but {unresolved} is set "
                              f"to an expansion the guard cannot resolve for this command; a fixture must fail closed rather than reach "
                              f"the live {family} account (declared in fixture_endpoints)")
        elif missing:
            violations.append(f"run_mode is fixture and the command names {family} tooling (`{match.group()}`) but {missing} is unset "
                              f"for this command; a fixture must fail closed rather than reach the live {family} account "
                              "(declared in fixture_endpoints)")
    return violations

def _writes_output(seg: _Seg, root: Path) -> bool:
    """Whether a REST or SQL client run remotely writes a file where it runs."""
    base, words = seg.argv0, seg.argv[1:]
    if base in _REST_CLIENTS:
        flags = ("-o", "--output", "-O", "--remote-name", "--remote-name-all", "--output-document", "-D", "--dump-header", "-c", "--cookie-jar")
        flags += () if base == "curl" else ("-d", "--download")
        output = bool(_flag_values(seg.argv, flags)) or any(w in flags for w in words)
        if base == "curl":
            output = output or any(next((c in "oODc" for c in m.group()[1:] if c in _CURL_VALUE_SHORT), False) for w in words if (m := re.match(r"^-[A-Za-z]+", w)))
        elif base in ("http", "https", "xh"):
            output = output or any(w.startswith(("-o", "-d")) and len(w) > 2 for w in words)
        stdout = any(w in ("-O-", "--output-document=-") or w == "-O" and words[i + 1:i + 2] == ["-"] or re.fullmatch(r"-[A-Za-z]*O-", w)
                     for i, w in enumerate(words))
        return output or (base == "wget" and not stdout)
    flags = ("-o", "--output", "-L", "--log-file", "--tee")
    sql = _sql_text(seg, root)[0]
    return (bool(_flag_values(seg.argv, flags)) or any(w in flags for w in words) or (base == "bcp" and any(w in ("out", "queryout") for w in words[:3]))
            or bool(re.search(r"(?im)(?:^|[\s;])(?::out|SPOOL(?!\s+OFF\b)|\.EXPORT)\s+\S", sql)))

def _check_remote(segs: list[_Seg], cfg: GuardConfig, root: Path) -> list[str]:
    """A remote execution on a legacy host is read like a direct command; what would run only there (an interactive session, a
    script or SQL file, a payload from a file, any file written) blocks."""
    violations = []
    for seg in segs:
        if not seg.remote:
            continue
        outer = [w.rsplit("@", 1)[-1].lower() for w in seg.ctx.split()]
        if seg.remote == seg.argv0:                            # `aws ssm ... --instance-ids H` / `--targets ...,Values=H`: H is the target
            outer += [p.lower() for w in seg.argv[1:] for p in re.split(r"[=,]", w)]
        hits = list(dict.fromkeys([*(t for t in cfg.legacy_sources if t.lower() in outer), *_conn(seg, cfg, "\n".join(seg.heredocs))[0]]))
        if not hits:
            continue
        base = seg.argv0
        if not seg.argv:
            violations.append(f"`{seg.remote}` opens an interactive session on legacy source {hits}; the guard cannot read what would run")
        elif base in ("aws", "az") and _remote_payload(seg.args) is None:
            violations.append(f"remote `{base}` payload cannot be read (file://, @file, --cli-input-json); the guard cannot clear a "
                              "remote command it cannot read")
        elif seg.scripts or _shell_runs(seg)[1] or (_PYTHON.fullmatch(base) and any(w.endswith(".py") for w in seg.argv[1:])):
            name = _shell_runs(seg)[1] or next((w for w in [*seg.scripts, *seg.argv[1:]] if w in seg.scripts or w.endswith(".py")))
            violations.append(f"remote command `{base}` runs `{name}` on legacy host {hits}; a script on the remote host cannot be "
                              "read by the guard (run the statements inline)")
        elif any(">" in op and not (op.endswith("&") and re.fullmatch(r"\d+|-", f)) for op, f in seg.redirects()):
            violations.append(f"remote command `{base}` on legacy host {hits} redirects output to a file on the legacy host; legacy hosts are read-only")
        elif _PYTHON.fullmatch(base):
            text = _python_texts(seg, root)[0]
            if _PY_WRITE.search(text) or re.search(r"\bsubprocess\b|\bos\.(?:system|popen|exec\w*|spawn\w*)\s*\(", text):
                violations.append(f"remote python `{base}` on legacy host {hits} contains a write or subprocess; legacy hosts are read-only")
        elif base in _REST_CLIENTS:
            if _writes_output(seg, root):
                violations.append(f"remote client `{base}` on legacy host {hits} writes output to a file; legacy hosts are read-only")
        elif base in (*_LEGACY_ONLY, *_GENERIC, "spark-sql", "dbsqlcli") and _writes_output(seg, root):
            violations.append(f"remote client `{base}` on legacy host {hits} writes query output to a file on the legacy host; legacy hosts are read-only")
        elif base in (*_LEGACY_ONLY, *_GENERIC, "spark-sql", "dbsqlcli") and _non_reads(_sql_text(seg, root)[0]):
            violations.append(f"remote client `{base}` on legacy host {hits} is not read-only; legacy hosts are read-only")
        elif base not in (*_SQL_CLIENTS, *_REST_CLIENTS, *_SHELLS) and not _PYTHON.fullmatch(base) and not (
                base in _READERS and not _mutates(base, seg.argv) and base not in ("unzip", "tee")
                and not (base == "tar" and any(re.search(r"^-.*[cxru]", w) for w in seg.argv[1:]))):
            violations.append(f"remote command `{base}` on legacy host {hits} is not a read shape the guard models; legacy hosts are read-only")
    return [_Legacy(v) for v in violations]

def _touch(path: str, at: str | None, root: Path) -> str:
    """How a path relates to what must not be written: 'inside'/'self'/'above' `.migration/`, 'identity', 'guard', 'guard-above',
    'unresolved', or ''."""
    p = os.path.expanduser(re.sub(r"\{[^{}]*(?:,|\.\.)[^{}]*\}", "*", re.sub(r"\$\{?PWD\}?|\$\(pwd\)", ".", path)))
    if p in ("", "-") or p.isdigit():
        return ""
    if _expands(p, subst=True) or _expands(p):
        return "unresolved"
    p = os.path.normpath(p if p.startswith("/") else os.path.join(at or "", p))
    parts = [x for x in p.split("/") if x not in ("", ".")]

    def like(name: str, part: str, spelled: bool = False) -> bool:
        return part == name or (bool(re.search(r"[*?\[]", part)) and fnmatch.fnmatchcase(name, part)
                                and (not spelled or any(len(w) >= 3 and w in name for w in re.findall(r"\w+", part))))
    for i, part in enumerate(parts):   # a literal `.migration` anywhere; a glob only at the top of the workspace
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
        if named and (like("hooks", named[0], True) or like(_GUARD_FILE, named[-1], True) or (len(named) == 1 and like("hooks.json", named[0], True))):
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

_Write = tuple[str, str, tuple[str, ...], bool, str | None]   # (path, how, .migration relations that block, destructive, directory)

def _git_writes(s: _Seg, here: str, root: Path, out: list[str]) -> list[_Write]:
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
    if verb == "push" and any(o in ("--force", "-f", "--force-with-lease") or o.startswith("--force-with-lease=") for o in gargv[1:]):
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
        if joined := s.env.get(name, ""):
            values = joined.split()
            paths = values if path == joined else [v + path[len(joined):] for v in values] if path.startswith(joined + "/") else paths
    return paths

def _writes(s: _Seg, root: Path, here: str, out: list[str]) -> list[_Write]:
    base, argv, at = s.argv0, s.argv, s.at
    ops = [w for w in argv[1:] if not w.startswith("-")]
    values = ops + [w.split("=", 1)[1] for w in argv[1:] if "=" in w]   # `dd of=`, `--output=`
    inplace = _mutates(base, argv)
    w = [(path, f"{op} {path}", _IN, False, at) for op, f in s.redirects() if ">" in op for path in _multi_paths(s, f)]
    w += [(path, f"{base} {flag}", _IN, False, at) for flag, v in itertools.pairwise(argv) if flag in _OUTPUT_FLAGS for path in _multi_paths(s, v)]
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
    elif base == "find" and inplace:
        w += [(path, "find with an action", _ALL, False, at) for o in ops for path in _multi_paths(s, o)]
    elif base in ("tar", "bsdtar") and (any(re.match(r"-?[a-zA-Z]*x", x) for x in argv[1:2]) or "--extract" in argv or "--get" in argv):
        w += [(path, f"{base} extract into", _ALL, False, at) for d in _flag_values(argv, ("-C", "--directory")) or ["."] for path in _multi_paths(s, d)]
    elif base == "unzip" and not any(x in argv for x in ("-l", "-t", "-p", "-z", "-Z")):
        w += [(path, "unzip into", _ALL, False, at) for d in _flag_values(argv, ("-d",)) or ["."] for path in _multi_paths(s, d)]
    elif base in _GENERIC or base in _LEGACY_ONLY:
        sql = " ".join([*_flag_values(argv, _SQL_VALUE_FLAGS), *s.heredocs, *s.stdin, s.herestring or ""])
        w += [(path, f"{base} output", _IN, False, at) for p in [*values, *_SQL_OUT_PATH.findall(sql)] for path in _multi_paths(s, p)]
    elif base not in _READERS or inplace:
        recursive = base in _RECURSIVE_HEADS and any(re.fullmatch(r"-[a-zA-Z]*[rR][a-zA-Z]*", x) or x in ("--recursive", "--delete") for x in argv[1:])
        if "xargs" in s.words and any(_touch(x, at, root) for p in s.feeds for x in p.words):
            out.append(f"`xargs {base}` on names listed from .migration/; ledgers and the allowlist change only through a recorded decision")
        w += [(path, base, _ALL if recursive else _IN, base in _DESTRUCTIVE, at)
              for o in (values[-1:] if base in _WRITE_LAST_OPERAND else values) for path in _multi_paths(s, o)]
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
    """The verdict on a command in the workspace at `root`, run from `cwd` (the event's; '' for the root) by a guard process in `here`."""
    root = root or Path.cwd()
    violations: list[str] = []
    if m := _PROBE.search(command):
        violations.append(f"`{m.group()}` is the factory-doctor's hook probe; it always blocks so the doctor can tell the "
                          "hook is loaded without touching Databricks")
    segs = _segments(command, at=cwd, root=root)
    violations += _check_unreadable(segs, cfg, command) + _check_identity(segs, cfg) + _check_integrity(segs, root, here) + _check_remote(segs, cfg, root)
    for seg in segs:
        violations += _check_fixture(seg, cfg, root)
        base = seg.argv0
        if base == "databricks" or (base == "dbt" and seg.argv[1:2] and seg.argv[1] in _DBT_RUNS):
            violations += _check_databricks(seg, cfg, root)
        elif base in ("spark-sql", "dbsqlcli"):
            sql, unreadable = _sql_text(seg, root)
            violations += ([_UNREADABLE.format(who="Databricks client", files=unreadable)] if unreadable else []) + _catalog_violations(
                sql, cfg, None, "Databricks SQL client")
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
    """File-edit tools: `.migration/` accepts only an appended `D-<id>` row in 06_decisions.md; the credential store and the guard tree never."""
    file_path = tool_input.get("file_path")
    if not isinstance(file_path, str) or not file_path:
        return Verdict.of([], cfg)
    edits = [e for e in tool_input.get("edits", []) if isinstance(e, dict)]
    new = tool_input.get("content") or tool_input.get("new_string") or "\n".join(e.get("new_string", "") for e in edits)
    old = "\n".join(e.get("old_string", "") for e in edits) if tool == "MultiEdit" else tool_input.get("old_string", "")
    if tool == "write" and not old:
        try:
            old = (Path(file_path) if os.path.isabs(file_path) else Path(cwd or root) / file_path).read_text(errors="replace")
        except OSError:
            old = ""
    kind, violations = _touch(file_path, cwd or "", root), []
    if kind in ("inside", "self"):
        ledger, added = Path(file_path).name == "06_decisions.md", new[len(old):] if new.startswith(old) else new
        authorized = "legacy_write_authorized" in added.lower()
        if ledger and authorized:
            violations.append("a `legacy_write_authorized` row enters the ledger only through a reviewed PR, never from a session")
        elif ledger and not (tool != "MultiEdit" and new.startswith(old) and added and _DECISION_ROW.search(added)):
            violations.append("06_decisions.md is append-only: the edit must keep the existing text and only add `D-<id>` rows")
        elif not ledger:
            violations.append(f"file-edit tool `{tool}` writes `{file_path}` under .migration/ (only .migration/recon/<unit_id>/ and "
                              ".migration/waves/ are written by a session; ledgers and the allowlist change only through a recorded "
                              "decision — 06_decisions.md accepts only an added `D-<id>` row)")
    elif kind == "identity":
        violations.append(f"file-edit tool `{tool}` writes `{file_path}`, the Databricks CLI's credential store; the session runs as the "
                          "doctor-verified migration principal only")
    elif kind in ("guard", "guard-above"):
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
    """`evaluate`, plus the allowlist of every other workspace the command changes into."""
    first = evaluate(command, cfg, root, cwd, here)
    violations = first.violations or ([first.reason] if first.decision == "block" else [])
    seen = {cfg.path}
    segments = _segments(command, at=cwd or str(root))
    if any(s.lost for s in segments):
        violations.append(f"command has more than {_MAX_ALTS} possible working directories; the allowlist in force is unknown")
    if any(s.at is None and (s.argv0 in (*_SQL_CLIENTS, "dbt") or _PYTHON.fullmatch(s.argv0)) for s in segments):
        violations.append("command changes to a directory the guard cannot resolve before running a Databricks or "
                          "legacy client; the allowlist in force there is unknown")
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
            violations += [type(x)(f"[{other.path}] {x}") for x in evaluate(command, other, d).violations if not x.startswith(_AUTHORIZED)]
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
    tool_input = tool_input if isinstance(tool_input, dict) else {}
    command, file_path = tool_input.get("command"), tool_input.get("file_path")
    command = command if isinstance(command, str) and command.strip() else None
    if command is None and not (isinstance(file_path, str) and file_path):
        return 0
    root, cwd, here = _dirs(event, tool_input)
    try:
        cfg = load_config(root)
        if cfg is None and isinstance(file_path, str) and os.path.isabs(file_path):
            cfg = load_config(Path(file_path).parent)
        if cfg is None and command is not None:
            cwd = cwd or here
            root, cfg = _workspace_from_cd(command, cwd)
    except (OSError, ValueError, json.JSONDecodeError) as exc:   # a broken allowlist is itself a setup violation: refuse rather than guess
        verdict = Verdict("block", f"dbx-migration-factory guard: cannot read {CONFIG_REL}: {exc}")
    else:
        if cfg is None:
            return 0
        verdict = evaluate_with_workdirs(command, cfg, root, cwd, here) if command is not None else evaluate_edit(
            event.get("tool_name", ""), tool_input, cfg, root, cwd)
    if verdict.reason:
        print(json.dumps({"decision": verdict.decision, "reason": verdict.reason}))
    if verdict.decision == "block":
        print(verdict.reason, file=sys.stderr)
    return 2 if verdict.decision == "block" else 0

if __name__ == "__main__":
    sys.exit(main())
