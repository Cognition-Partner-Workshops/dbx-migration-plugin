#!/usr/bin/env python3
"""dbx-migration-factory 0.7 PreToolUse guard (design-0.7.md section 4).

Blocks the irreversible classes the platform cannot fence itself: G1 a
legacy client or `legacy_sources` name used for anything but an inline
read, G2 a deploy to a forbidden or undeclared target, G3/G5 writes to a
credential store or the running plugin tree. Every other event is `warn`
(approve with the reason attached) and every exec event appends one line
to `.migration/guard.log`. Armed iff a `.migration/` directory exists at
or above the event's cwd, a `cd` target, or the edit's file_path.
"""
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parent.parent
ENVRC = "/opt/.devin/envrc"
CLIENTS = ("sqlplus", "sqlldr", "bteq", "mload", "fastload", "tbuild",
           "tdload", "snowsql", "sqlcmd", "psql", "mysql")
WRITE_VERB = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|ALTER|CREATE|GRANT|REVOKE|EXEC(?:UTE)?|CALL|BEGIN|COMMIT)\b", re.I)
SQUOTED = re.compile(r"'[^']*'")
DEPLOY = re.compile(r"\b(?:databricks\s+bundle\s+(?:deploy|run|destroy)|dbt\s+(?:run|build|seed))\b")
TARGET_FLAG = re.compile(r"(?:^|\s)(?:-t|--target)[=\s]+([^\s;&|]+)")
ENV_ASSIGN = re.compile(
    r"(?:^|[;&|]|\s)(?:export\s+)?DATABRICKS_(?:HOST|TOKEN|CLIENT_ID|CLIENT_SECRET)\s*=")
G3_PATH = re.compile(r"(^|/)\.databrickscfg$|\.databricks/|\.config/databricks/|" + re.escape(ENVRC))
HOOK_SELF = re.compile(r"guard\.py|hooks\.json")
REDIRECT = re.compile(r">+\s*([^\s;&|]+)")
QARG = re.compile(r"(?:^|\s)-[ceQq]\s*('[^']*'|\"[^\"]*\")")
WRITERS = ("cp", "mv", "tee", "rm", "dd", "truncate", "chmod", "chown",
           "sed", "vi", "vim", "nano", "emacs")
CARRIER = re.compile(
    r"\b(?:(?:ba|z|da|k)?sh\s+-c|eval|ssh|(?:docker|kubectl)\s+exec)\s*('[^']*'|\"[^\"]*\")")
RED = re.compile(r"(DATABRICKS_(?:HOST|TOKEN|CLIENT_ID|CLIENT_SECRET)\s*=\s*)"
                 r"(?:'[^']*'|\"[^\"]*\"|\\.|[^\s;&|'\"])+")


def _has(cmd, tok):
    return re.search(r"(?<![\w.-])" + re.escape(tok) + r"(?![\w.-])", cmd, re.I) is not None


def _g1(cmd, legacy):
    toks = CLIENTS + tuple(legacy)
    engaged = next((t for t in toks if _has(cmd, t)), None)
    if not engaged:
        return None
    if (m := _g1_write(cmd)):
        return ("G1", f"legacy client/source `{engaged}` on a line with write verb `{m}`"
                      " (no DBX_DECISION downgrade exists in 0.7)")
    bare = SQUOTED.sub(" ", cmd)
    if re.search(r"(?:^|\s)(?:-[fi]\b|@)|<", bare):
        return ("G1", f"legacy client `{engaged}` fed a script input (-f/-i/@/<); only an inline read is allowed")
    if re.search(r"\b(?:ssh|docker\s+exec)\b", cmd):
        return ("G1", f"legacy client/source `{engaged}` inside an ssh/docker exec payload")
    if any(_has(p, t) for p in re.split(r"(?<!\|)\|(?!\|)", cmd)[1:] for t in toks):
        return ("G1", f"a pipe into legacy client `{engaged}`")
    for q in _payloads(cmd):
        hit = _g1(q, legacy)
        if hit:
            return hit
    return None


def _g1_write(cmd):
    if (m := WRITE_VERB.search(SQUOTED.sub(" ", cmd))):
        return m.group(0)
    for c in SQUOTED.findall(cmd):
        if len(c.split()) > 1 and (m := WRITE_VERB.search(c)):
            return m.group(0)
    for q in QARG.findall(cmd):
        if (m := WRITE_VERB.search(SQUOTED.sub(" ", q[1:-1]))):
            return m.group(0)
    return None


def _split(cmd, seps):
    parts, cur, q = [], [], None
    for ch in cmd:
        if q:
            cur.append(ch)
            if ch == q:
                q = None
        elif ch in "'\"":
            q = ch
            cur.append(ch)
        elif ch in seps:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return parts


def _in_quotes(cmd, pos):
    stack, i = [], 0
    while i < pos:
        ch = cmd[i]
        top = stack[-1] if stack else ""
        if top in ("'", "`"):
            if ch == top:
                stack.pop()
        elif top == '"':
            if ch == '"':
                stack.pop()
            elif ch == "$" and i + 1 < pos and cmd[i + 1] == "(":
                stack.append("$(")
                i += 1
            elif ch == "`":
                stack.append("`")
        else:
            if ch == "$" and i + 1 < pos and cmd[i + 1] == "(":
                stack.append("$(")
                i += 1
            elif ch == "(":
                stack.append("(")
            elif ch == ")" and stack:
                stack.pop()
            elif ch in ("'", '"', "`"):
                stack.append(ch)
        i += 1
    return (stack or [""])[-1] in ("'", '"')


def _payloads(cmd):
    return [m.group(1)[1:-1] for m in CARRIER.finditer(cmd)
            if not _in_quotes(cmd, m.start())]


ARGFLAG = {"sudo": {"-u", "-g", "-h", "-p", "-C", "-T",
                    "--user", "--group", "--host", "--prompt"},
           "doas": {"-u"},
           "env": {"-u", "-S", "-C", "--unset", "--split-string", "--chdir"},
           "nice": {"-n", "--adjustment"},
           "time": {"-o", "-f", "-v", "--output", "--format", "--verbose"}}


def _head(words):
    skip = set(ARGFLAG) | {"command", "noglob"}
    wrapper, expect_arg = "", False
    for i, w in enumerate(words):
        if expect_arg:
            expect_arg = False
            continue
        if w in skip:
            wrapper = w
            continue
        if w.startswith("-"):
            if w in ARGFLAG.get(wrapper, ()):
                expect_arg = True
            continue
        if re.match(r"^[A-Za-z_]\w*=", w):
            continue
        return i
    return -1


def _writes_to(cmd, pred):
    for seg in _split(cmd, ";&|"):
        targets = REDIRECT.findall(seg)
        words = [w for w in _split(seg, " \t") if w]
        i = _head(words)
        if i >= 0:
            rest = words[i + 1:]
            w = os.path.basename(words[i])
            args = [a for a in rest if not a.startswith("-")]
            if w == "sed" and not any(
                    f.startswith("-i") or f.startswith("--in-place") for f in rest):
                args = []
            elif w in ("cp", "mv"):
                dest = [rest[j + 1] for j, a in enumerate(rest[:-1])
                        if a in ("-t", "--target-directory")]
                dest += [a.split("=", 1)[1] for a in rest
                         if a.startswith("--target-directory=")]
                args = dest or args[-1:]
            elif w not in WRITERS:
                args = []
            targets += args
        if any(pred(t.strip("'\"")) for t in targets):
            return True
    return False


def _g2(cmd, pol):
    forbidden = {str(t).lower() for t in (pol.get("forbidden_targets") or ("prod", "production"))}
    allowed = {str(t).lower() for t in (pol.get("bundle_targets") or ())}
    segs = _split(cmd, ";&|")
    segs += [s for q in _payloads(cmd) for s in _split(q, ";&|")]
    for seg in segs:
        if not DEPLOY.search(seg):
            continue
        targets = [t.strip("'\"").lower() for t in TARGET_FLAG.findall(seg)]
        if not targets:
            return ("G2", "deploy with no -t/--target; a deploy must name a policy.bundle_targets entry")
        for t in targets:
            if t in forbidden:
                return ("G2", f"deploy target `{t}` is in policy.forbidden_targets")
            if t not in allowed:
                return ("G2", f"deploy target `{t}` is not in policy.bundle_targets")
    if ("/branches/production" in cmd and re.search(r"\bdatabricks\s+postgres\b", cmd)
            and re.search(r"\b(?:create|delete|update)-", cmd)):
        return ("G2", "`databricks postgres` create/delete/update on `/branches/production`")
    return None


def _g3_command(cmd):
    if ENV_ASSIGN.search(cmd):
        return ("G3", "assigns DATABRICKS_HOST/TOKEN/CLIENT_ID/CLIENT_SECRET;"
                      " the migration principal's credentials are fixed")
    if _writes_to(cmd, lambda t: G3_PATH.search(t)):
        return ("G3", "writes to a Databricks credential store (.databrickscfg/.databricks/envrc)")
    if re.search(r"\bdatabricks\s+(?:configure|auth\s+login)\b", cmd):
        return ("G3", "`databricks configure`/`auth login` would rewrite the principal's credentials")
    return None


def _g5_command(cmd):
    if _writes_to(cmd, lambda t: str(PLUGIN_ROOT) in t or HOOK_SELF.search(t)):
        return ("G5", "command writes inside the running plugin tree; the hook is never modified from a session")
    return None


def _edit_block(tool, fpath, cwd):
    p = Path(fpath) if os.path.isabs(fpath) else Path(cwd) / fpath
    p = p.resolve()
    if p.is_relative_to(PLUGIN_ROOT):
        return ("G5", f"file-edit tool `{tool}` writes `{fpath}` inside the running plugin tree; "
                      "the hook is never edited from a session")
    if G3_PATH.search(str(p)):
        return ("G3", f"file-edit tool `{tool}` writes `{fpath}`, a Databricks credential store")
    return None


def _armed(start):
    d = Path(start).resolve()
    for p in (d, *d.parents):
        if (p / ".migration").is_dir():
            return p / ".migration"
    return None


def _show(ws, ref, rel):
    try:
        r = subprocess.run(["git", "-C", str(ws), "show", f"{ref}:{rel}"],
                           capture_output=True, timeout=5)
        return r.stdout.decode(errors="replace") if r.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def _policy(mdir):
    ws = mdir.parent
    base = _show(ws, "origin/HEAD", ".migration/base")
    if base is None and (mdir / "base").is_file():
        base = (mdir / "base").read_text(errors="replace")
    lines = (base or "").splitlines()
    base = lines[0].strip() if lines and lines[0].strip() else "origin/HEAD"
    try:
        return json.loads(_show(ws, base, ".migration/policy.json") or "{}")
    except json.JSONDecodeError:
        return {}


def _log(mdir, rule, decision, cmd):
    safe = RED.sub(r"\1<redacted>", cmd)[:200]
    line = json.dumps({"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                       "rule": rule, "decision": decision, "cmd": safe})
    try:
        with open(mdir / "guard.log", "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def main(stdin_text=None):
    raw = stdin_text if stdin_text is not None else sys.stdin.read()
    try:
        event = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        return 0
    ti = event.get("tool_input") or {} if isinstance(event, dict) else {}
    ti = ti if isinstance(ti, dict) else {}
    cmd = ti.get("command") if isinstance(ti.get("command"), str) and ti["command"].strip() else ""
    fpath = ti.get("file_path") if isinstance(ti.get("file_path"), str) else ""
    if not cmd and not fpath:
        return 0
    cwd = ti.get("workdir") or event.get("cwd") or os.getcwd()
    if cmd:
        dirs = [cwd] + [os.path.join(cwd, os.path.expanduser(t.strip("'\""))) for t in
                        re.findall(r"(?:^|[;&|]\s*)cd\s+(?:-[LP]\s+)?([^\s;&|]+)", cmd)]
        mdir = next((m for m in map(_armed, dirs) if m), None)
    else:
        fp = fpath if os.path.isabs(fpath) else os.path.join(cwd, fpath)
        mdir = _armed(cwd) or _armed(fp)
    if mdir is None:
        return 0
    if cmd:
        pol = _policy(mdir)
        hit = (_g1(cmd, pol.get("legacy_sources") or []) or _g2(cmd, pol)
               or _g3_command(cmd) or _g5_command(cmd))
        rule, why = hit if hit else ("none", "")
        decision = "block" if why else "warn"
        _log(mdir, rule, decision, cmd)
        reason = why or "no blocked class (G4: logged to .migration/guard.log)"
    else:
        hit = _edit_block(event.get("tool_name", ""), fpath, cwd)
        rule, why = hit if hit else ("none", "")
        decision = "block" if why else "warn"
        reason = why or "no blocked class"
    reason = "dbx-migration-factory guard: " + reason
    print(json.dumps({"decision": decision, "reason": reason}))
    if decision == "block":
        print(reason + " A block is a halt to report, never something to re-shape or retry.", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
