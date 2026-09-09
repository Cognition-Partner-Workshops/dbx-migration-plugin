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

The guard reads text: the command, the scripts it names (SQL files handed to a client, shell
scripts handed to an interpreter or `source`) and heredoc bodies. Text it cannot read is a
violation, not a pass: an unreadable script, and any construct that would only produce the
statement at run time where a Databricks or legacy client is involved (command substitution,
`eval`, an interpreter fed a `$`-built string or decoded bytes, an expansion inside the SQL
argument or an unquoted heredoc). Programs the guard cannot read into (a Python or JDBC client
opening its own connection) are covered by the read-only source roles the doctor verifies, not
by this hook. A command that changes directory is judged against the allowlist of every
workspace it enters as well as the one it starts in.

Outside a migration workspace (no `.migration/allowed_targets.json` up the tree) the guard
approves everything. Malformed input approves (plugin hooks fail open by platform design; the
factory-doctor reports whether the hook is loaded).
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

_SEG = r"(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$-]*)"
_THREE_PART = re.compile(rf"(?<![\w`.])({_SEG})\.({_SEG})\.({_SEG})(?![\w`.])")
# the securable right after a write verb: three-part, or four-part when it is a column
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
    rf"\b(?:CREATE|DROP|ALTER|UNDROP)\s+(?:SCHEMA|DATABASE)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?({_SEG})\.({_SEG})(?![\w`.])",
    re.IGNORECASE,
)
_ON_CATALOG = re.compile(rf"\bON\s+CATALOG\s+({_SEG})", re.IGNORECASE)
_ON_SCHEMA = re.compile(rf"\bON\s+(?:SCHEMA|DATABASE)\s+({_SEG})\.({_SEG})(?![\w`.])", re.IGNORECASE)

# one match per bundle invocation, bounded by the shell separators so chained commands are each checked
_BUNDLE_DEPLOY = re.compile(r"\bdatabricks\s+bundle\b([^;&|\n]*?\b(?:deploy|run|destroy)\b[^;&|\n]*)", re.IGNORECASE)
_BUNDLE_TARGET = re.compile(r"(?:^|\s)(?:-t|--target)(?:=|\s+)(\S+)")
_TARGET_CATALOG_FLAG = re.compile(r"--target-catalog(?:=|\s+)(\S+)")
_SCRIPT_FLAGS = ("-f", "-i", "--file", "--input")
# the securable a write statement acts on sits right after its verb phrase; anything qualified
# later in the statement (CTAS `AS SELECT FROM`, MERGE `USING`, INSERT ... SELECT) is a source
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

_SHELLS = ("sh", "bash", "zsh", "dash", "ksh")
_SEPARATORS = (";", "&&", "||", "|", "&", "(", ")", "{", "}", "\n")
_PREFIX_WORDS = ("sudo", "env", "nohup", "time", "exec", "command", "nice", "xargs")
# flags whose value is the SQL text itself (psql -c, sqlcmd/isql/snowsql -Q/-q, spark-sql/dbsqlcli -e ...)
_SQL_VALUE_FLAGS = ("-c", "-Q", "-q", "-e", "--query", "--sql", "--statement", "--execute", "--command")
# producers whose output is opaque to a text scan
_OPAQUE_PRODUCER = re.compile(
    r"(?:^|[\s;&|(])(?:base64\s+(?:-[A-Za-z]*d[A-Za-z]*|--decode)|xxd\s+-r|openssl\s+enc\b|gunzip|gzip\s+-d|zcat|"
    r"uudecode|curl|wget|python[0-9.]*|perl|ruby|node)\b", re.IGNORECASE)
_HEREDOC = re.compile(r"<<-?\s*(['\"\\]?)([A-Za-z_][\w-]*)\1?[^\n]*\n")
# text ending right before the argument of `sh -c`: that argument is a shell command. Any words
# may sit between the shell and `-c` (`bash --norc -o pipefail -c`, `bash -lc`, `bash -c --`);
# over-matching only reads an argument as shell text, which never hides more than reading it as SQL
_SHELL_C_ARG = re.compile(
    r"(?:^|[\s;&|(])(?:[^\s;&|()<>'\"]*/)?(?:sh|bash|zsh|dash|ksh)(?:\s+[^\s;&|()'\"]+)*?"
    r"\s+-[A-Za-z]*c[A-Za-z]*(?:\s+--)?\s+$"
)
# characters a backslash escapes inside a double-quoted shell argument
_DQ_ESCAPABLE = '"\\$`\n'


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
    """The text as the write detector reads it, offsets preserved: SQL comments blanked, and the
    contents of single-quoted SQL literals blanked so `WHERE note = 'DROP TABLE x'` is a read.

    What counts as SQL text is decided by the enclosing shell construct, never by guessing from
    the characters: a script file or heredoc body (`sql_only`) is SQL throughout; the body of a
    top-level '...' or "..." argument is SQL (a `--` there is a comment to the end of the line or
    of the argument, whatever follows it, quotes included); the body of the argument of `sh -c`
    is shell text again, read with these same rules. Unquoted shell text has no comments and no
    literals: `--profile`, a bare `--`, and a `/tmp/*.sql` glob all stay visible. A literal that
    feeds a dynamic-SQL executor stays visible."""
    out = list(text)
    n = len(text)

    def blank(a: int, b: int) -> None:
        for k in range(a, b):
            if out[k] != "\n":
                out[k] = " "

    def sql(a: int, b: int, dq: bool = False) -> None:
        i = a
        while i < b:
            if text.startswith("--", i):
                j = text.find("\n", i, b)
                j = b if j < 0 else j
                blank(i, j)
                i = j
            elif text.startswith("/*", i):
                j = text.find("*/", i + 2, b)
                j = b if j < 0 else j + 2
                blank(i, j)
                i = j
            elif dq and text[i] == "\\" and i + 1 < b and text[i + 1] in _DQ_ESCAPABLE:
                i += 2
            elif text[i] == "'":
                j = i + 1
                while j < b:
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

    if sql_only:
        sql(0, n)
        return "".join(out)

    def argument(a: int, b: int, dq: bool) -> None:
        if _SHELL_C_ARG.search(text, 0, a - 1):
            out[a:b] = _sql_view(text[a:b])
        else:
            sql(a, b, dq)

    i = 0
    while i < n:
        c = text[i]
        if c == "\\":
            i += 2
        elif c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            argument(i + 1, j, True)
            i = j + 1
        elif c == "'":
            j = text.find("'", i + 1)
            j = n if j < 0 else j
            argument(i + 1, j, False)
            i = j + 1
        elif text.startswith("<<", i) and (m := _HEREDOC.match(text, i)):
            term = re.compile(r"^\t*" + re.escape(m.group(2)) + r"[ \t]*$", re.MULTILINE)
            t = term.search(text, m.end())
            body_end = n if t is None else t.start()
            sql(m.end(), body_end)
            i = body_end if t is None else t.end()
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


# redirection operators stay inside their simple command; every other punctuation run ends it. A
# descriptor attached to the operator (`2>&1`, `0<f`) is part of it; separated by a space it is
# an operand (`cat 2 >log` prints the file named 2)
_REDIRECT_OP = re.compile(r"\d*(?:<{1,3}|<>|<&|>{1,2}|>&|>\||&>{1,2})")
_STDIN_OP = re.compile(r"0?<")


def _raw_words(cmd: str) -> list[str]:
    """The command split at unquoted, unescaped blanks, each word kept as written (quotes and
    backslashes included)."""
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
    quoted argument (usually the SQL itself) is one word, so a `<` comparison or a line break
    inside it is never an operator. A descriptor written against its operator is one token with
    it (`2>&`, `0<`): unquoted digits opening a whitespace-delimited word, with the operator
    right behind them in the source text, is what makes the digits a descriptor; quoted or
    escaped (`'2'>log`, `\\2>log`) they are a filename, whatever follows them."""
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
    words = list(lexer(False))   # the same text, split at whitespace only
    raw = _raw_words(cmd)        # the same words with their quotes and escapes kept
    if len(raw) != len(words):   # the splits disagree: nothing can be fused
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
    """One simple command of a shell line: its words, the words of the heredoc bodies it opened,
    the operator that separated it from the command before it (`|` / `|&`: the left side's output
    is this one's stdin), how many `(`/`{` groups opened right before it and how many `)`/`}`
    closed between it and the command before it, and how many of its words it wrote itself
    (`own`): the rest were handed down by the redirections of a group it sits in."""
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
    """Whether cmds member `c` reads something other than the stdin the group closing now is
    given: a stdin redirection or heredoc of its own (or of an inner group it sits in), or a
    pipe feeding it (directly or the group it runs in). Redirections this same group handed
    down already do not count: `(bteq) < a < b` opens both, and `<&0` duplicates stdin onto
    itself, which replaces nothing."""
    words = c.words if c.own is None else c.words[:c.own]
    for w, operand in zip(words, [*words[1:], ""]):
        if _LOCAL_STDIN.fullmatch(w) and not (w.endswith("<&") and operand == "0"):
            return True
    return _pipe_into(cmds, next(k for k, x in enumerate(cmds) if x is c)) >= 0


def _commands(toks: list[str]) -> list[_Simple]:
    """The token list split into simple commands at `;`, `&&`, `||`, `|`, `|&`, `&`, group
    delimiters and line breaks. Redirections (`< f`, `>log`, `2>&1`) are part of the command
    they sit in, and the body of a `<<TAG` heredoc (read from the next line, up to the line
    holding TAG alone) belongs to the command that opened it, even when that command is followed
    by `| tee` or `&& echo` on the opening line. Punctuation between two commands folds into one
    separator: `) |`, `|` + newline and `| (` all leave the right-hand command fed by a pipe. A
    redirection written after `)` / `}` belongs to the group (`(cat f) 2>&1 | bteq`,
    `(bteq; echo done) < f`): it is repeated on every command inside the group rather than
    becoming a command of its own. The group's stdin only reaches the members that did not
    replace it themselves, with a `<` of their own or by sitting behind a pipe
    (`(bteq < r; echo) < w` and `(cat r | bteq) < w` never hand `w` to bteq)."""
    punct = re.compile(r"[();<>|&\n]+")
    out: list[_Simple] = [_Simple()]
    pending: list[tuple[str, list[str]]] = []   # (delimiter, body of the opening command), in order
    open_groups: list[int] = []          # index in `out` of each unclosed group's first command
    closed_group: list[_Simple] = []     # members of the group closed last
    operand_of: list[_Simple] = []       # commands owning the redirection whose operand comes next

    def open_group() -> None:
        open_groups.append(len(out) - 1)

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
                open_group()
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
            open_group()
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


# producers whose stdout the guard can read as text: the files `cat` names, the words `echo` /
# `printf` print, the heredoc body either opens; `tee` passes its stdin through
_TEXT_PRODUCERS = ("cat", "echo", "printf", "tee")


def _pipe_into(cmds: list[_Simple], k: int) -> int:
    """Index of the command whose separator is the pipe that feeds cmds[k]'s stdin: k itself, or
    the command opening the group that k runs in (`ls | (cd x; bteq)`: bteq reads what `ls`
    wrote); -1 when nothing is piped in."""
    # depth each command runs at: groups opened before it minus groups closed before it
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
        # cmds[j] runs inside a group: its stdin is the group's, decided at the command that
        # opened it (the one that entered `level`), which in turn may sit in an outer group
        while j >= 0 and not (cmds[j].opened and at[j] - cmds[j].opened < level <= at[j]):
            j -= 1
        if j >= 0:
            level = at[j] - cmds[j].opened if cmds[j].sep not in _PIPE_OPS else level
    return -1


def _producers(cmds: list[_Simple], k: int) -> list[_Simple]:
    """The commands whose output reaches cmds[k]'s stdin through the pipeline: the command on the
    left of each `|`, or every command of the `( ... )` / `{ ...; }` group on its left."""
    out = []
    k = _pipe_into(cmds, k)
    while k > 0 and cmds[k].sep in _PIPE_OPS:
        unwind = cmds[k].closed  # groups the left operand closes right before the pipe
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
    """Script files a text producer hands the client on its stdin: `cat fix.sql | bteq`, and
    `@fix.sql` on a line of `cat <<EOF | sqlplus` or in `echo @fix.sql | bteq`."""
    files = [tok[1:] for tok in producer.body if tok.startswith("@") and len(tok) > 1]
    base = producer.words[0].rsplit("/", 1)[-1] if producer.words else ""
    if base == "cat":
        # `cat` reads its stdin (`cat < f`, or the stdin of the group it runs in) only when it
        # names no file, or names `-`
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
                i += 2  # the operand is a log, descriptor or heredoc delimiter, not a script
            else:
                if w == "-" or not w.startswith("-"):
                    operands.append(w)
                i += 1
        files.extend(w for w in operands if w != "-")
        if not operands or "-" in operands:
            files.extend(stdin)
    elif base in ("echo", "printf"):
        files.extend(w[1:] for w in producer.words[1:] if w.startswith("@") and len(w) > 1)
    return files


def _script_inputs(cmd: str, cfg: GuardConfig | None = None) -> list[str]:
    """Files a client is told to execute: `< f`, `@f`, `-f f`, `--file f`, `-i f`, `--input f`,
    and `@f` on a line of the client's heredoc (SQL*Plus / BTEQ `.RUN`). Given a config, read
    only from the simple commands whose own words name a Databricks or legacy client or a legacy
    source: the `-f` of `rm -f x && databricks jobs list` belongs to `rm`, and a heredoc body
    that merely mentions a client (`cat <<EOF` writing a script) is data, not context. What a
    text producer pipes into such a command (`cat fix.sql | bteq`, `cat <<EOF | sqlplus` with
    `@fix.sql` in the body) is that command's script too."""
    files = []
    cmds = _commands(_shell_tokens(cmd))
    for k, c in enumerate(cmds):
        words, body = c.words, c.body
        if cfg is not None and not _has_context(" ".join(words), cfg):
            continue
        for producer in _producers(cmds, k):
            files.extend(_piped_scripts(producer))
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
        files.extend(tok[1:] for tok in body if tok.startswith("@") and len(tok) > 1)
    return files


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
    `$` and backticks is what the shell would actually expand."""
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
    """Command or process substitution, or a command in backticks."""
    live = _live(text)
    return bool(_SUBSTITUTION.search(live)) or _command_backticks(live)


def _strip_quotes(tok: str) -> str:
    if len(tok) >= 2 and tok[0] == tok[-1] and tok[0] in "'\"":
        return tok[1:-1]
    return tok


def _shell_script_inputs(cmd: str) -> list[str]:
    """Scripts run by a shell interpreter or sourced: `bash x.sh`, `sh -x x.sh`, `bash < x.sh`,
    `source x`, `. x`. `-c` forms carry their text in the command and are not files."""
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


def _has_context(text: str, cfg: GuardConfig) -> bool:
    return bool(_DATABRICKS_CONTEXT.search(text) or _LEGACY_ONLY_CLIENTS.search(text) or _mentions_legacy(text, cfg))


def _inline_scripts(cmd: str, root: Path, cfg: GuardConfig) -> tuple[str, list[str], list[str]]:
    """Command text plus the contents of every referenced script: shell scripts the command runs
    (always), then SQL scripts handed to a client (when a client or legacy source is involved).
    Scripts the guard cannot inspect in full (unreadable, or larger than `_MAX_SCRIPT_BYTES`) are
    returned as unreadable, shell and SQL separately."""
    unreadable_shell: list[str] = []
    parts = [cmd]
    for f in _shell_script_inputs(cmd):
        body = _read_script(f, root)
        if body is None:
            unreadable_shell.append(f)
        else:
            parts.append("\n;\n" + _join_continuations(body))
    text = "\n".join(parts)
    unreadable: list[str] = []
    if not _has_context(text, cfg):
        return text, unreadable, unreadable_shell
    for f in _script_inputs(text, cfg):
        body = _read_script(f, root)
        if body is None:
            unreadable.append(f)
        else:
            parts.append("\n;\n" + _sql_view(body, sql_only=True))
    return "\n".join(parts), unreadable, unreadable_shell


def _sql_bearing(toks: list[str], i: int) -> bool:
    """Whether token i is where a client reads its SQL from: the value of a SQL flag, the positional
    after `tools query`, or a composite string (whitespace or `;` inside) rather than a bare value."""
    prev = toks[i - 1] if i > 0 else ""
    if prev in _SQL_VALUE_FLAGS or toks[i].split("=", 1)[0] in _SQL_VALUE_FLAGS:
        return True
    if i >= 2 and toks[i - 2] == "tools" and prev == "query":
        return True
    return bool(re.search(r"[\s;]", _strip_quotes(toks[i])))


def _check_opaque_execution(cmd: str, cfg: GuardConfig) -> list[str]:
    """Constructs that would only produce the statement at run time, so the text scan cannot
    clear them. Always: `eval`/`sh -c` on a `$`-built string, opaque bytes piped into a shell,
    a shell fed by process substitution. In a Databricks/legacy context: any command or process
    substitution, an expansion inside the SQL argument, an unquoted heredoc that expands."""
    violations: list[str] = []
    toks = _raw_tokens(cmd)
    ctx = _has_context(cmd, cfg)

    def seg_start(i: int) -> bool:
        return i == 0 or toks[i - 1] in _SEPARATORS or toks[i - 1] in _PREFIX_WORDS

    for i, tok in enumerate(toks):
        base = tok.rsplit("/", 1)[-1]
        if tok == "eval" and seg_start(i):
            rest = " ".join(toks[i + 1:])
            if _expands(rest.split(";", 1)[0]):
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

    cmds = _commands(_shell_tokens(cmd))
    for k, c in enumerate(cmds):  # a legacy client runs whatever reaches its stdin
        if not _LEGACY_ONLY_CLIENTS.search(" ".join(c.words)):
            continue
        for producer in _producers(cmds, k):
            base = producer.words[0].rsplit("/", 1)[-1] if producer.words else ""
            if base not in _TEXT_PRODUCERS or _expands(" ".join(producer.words)):
                violations.append(f"text piped into `{c.words[0]}` comes from `{base or '?'}`, a program or expansion the "
                                  "guard cannot read; pipe from cat/echo/printf or a heredoc instead")
                break

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
                continue  # quoted delimiter: the body is literal
            end = re.search(rf"^\s*{re.escape(m.group(2))}\s*$", cmd[m.end():], re.MULTILINE)
            body = cmd[m.end(): m.end() + end.start()] if end else cmd[m.end():]
            if _expands(body):
                violations.append(f"unquoted heredoc <<{m.group(2)} expands `$`/backticks in its body; quote the delimiter "
                                  f"(<<'{m.group(2)}') or inline the values")
                break
    return violations


def _catalogs_in_segment(seg: str) -> set[str]:
    """Catalog of the statement's *write target*: the securable named directly after the verb
    phrase, when it is qualified. An unqualified target resolves to nothing here (the caller
    falls back to `USE CATALOG`), never to a qualified identifier further along, which is a
    source (CTAS `AS SELECT FROM prod...`, MERGE `USING`, INSERT ... SELECT) and may legitimately
    sit outside the allowlist."""
    start = len(seg) - len(seg.lstrip())
    for rx in (_SCHEMA_TWO_PART, _CREATE_CATALOG):
        m = rx.match(seg, start)
        if m:
            return {_norm(m.group(1))}
    for rx in (_ON_SCHEMA, _ON_CATALOG):  # GRANT/REVOKE name their securable after ON
        m = rx.search(seg)
        if m:
            return {_norm(m.group(1))}
    head = _WRITE_TARGET_HEAD.match(seg, start)
    if head:
        m = _TARGET_NAME.match(seg, head.end())
        if m:
            return {_norm(m.group(1))}
    return set()


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


def _check_uninspectable_scripts(cmd: str, cfg: GuardConfig, unreadable: list[str],
                                 unreadable_shell: list[str] = ()) -> list[str]:
    """A script the guard cannot read in full is a script it cannot clear: fail closed."""
    out = []
    if unreadable_shell:
        out.append(f"shell script(s) {list(unreadable_shell)} the command would run cannot be read in full (missing, "
                   f"unreadable or over {_MAX_SCRIPT_BYTES // (1024 * 1024)} MiB); the guard cannot clear what it cannot read")
    if not unreadable:
        return out
    if _mentions_legacy(cmd, cfg) or _LEGACY_ONLY_CLIENTS.search(cmd):
        out.append(f"legacy client fed script(s) {unreadable} that the guard cannot read in full (missing, unreadable or over "
                   f"{_MAX_SCRIPT_BYTES // (1024 * 1024)} MiB); inline the SQL or split it so it can be inspected "
                   "(legacy is read-only in every phase)")
    elif _DATABRICKS_CONTEXT.search(cmd):
        out.append(f"Databricks client fed script(s) {unreadable} that the guard cannot read in full (missing, unreadable or over "
                   f"{_MAX_SCRIPT_BYTES // (1024 * 1024)} MiB); inline the SQL or split it so its write targets can be checked")
    return out


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
    full, unreadable, unreadable_shell = _inline_scripts(command, root or _project_root(), cfg)
    violations = (_check_uninspectable_scripts(full, cfg, unreadable, unreadable_shell)
                  + _check_opaque_execution(full, cfg) + _check_databricks_writes(full, cfg)
                  + _check_legacy_writes(full, cfg))
    return _verdict(violations, cfg)


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
    """`evaluate` against the starting workspace and against every workspace the command `cd`s
    into: a write must be allowed by each allowlist involved, and a Databricks/legacy command that
    moves to a directory the guard cannot resolve is not clearable."""
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
        # A broken allowlist is itself a violation of setup step 7: refuse writes rather than guess.
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
