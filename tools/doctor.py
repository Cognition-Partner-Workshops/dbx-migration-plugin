#!/usr/bin/env python3
"""tools/doctor.py — 0.7 preflight (docs/design-0.7.md §5, schemas §6).

Six checks, each ok|warn|fail with data; `ready` is every check not fail.
Prints the report as JSON to stdout, optionally a Markdown rendering, exits 0
when ready else 1. No signature, no capabilities.json, no pointer file.

Every Databricks call goes through an injected `dbx(args)` callable wrapping
`databricks <args>`; every other subprocess through `run_cmd(argv, cwd, stdin)`;
the source_read_only adapter is `run_sql(sql) -> rows` (queries are constants;
no driver code lives here — G1 forbids pipes/scripts into legacy clients).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

POLICY_REL = ".migration/policy.json"
SLICES_REL = ".migration/slices.json"

UC_READ = {"SELECT", "READ", "READ_VOLUME", "EXECUTE", "USE_CATALOG", "USE_SCHEMA", "USE_SHARE",
           "USE_PROVIDER", "USE_RECIPIENT", "USE_CONNECTION", "BROWSE", "ACCESS"}
ORACLE_READ = {"SELECT", "READ", "CREATE SESSION", "SELECT ANY TABLE", "SELECT ANY DICTIONARY"}
MSSQL_READ = {"SELECT", "VIEW DEFINITION", "VIEW ANY DEFINITION", "VIEW DATABASE STATE",
              "VIEW SERVER STATE", "CONNECT", "SHOWPLAN"}
MSSQL_BAD_ROLE = ("SELECT IS_SRVROLEMEMBER('sysadmin') AS sysadmin, IS_MEMBER('db_owner') AS db_owner, "
                  "IS_MEMBER('db_datawriter') AS db_datawriter, IS_MEMBER('db_ddladmin') AS db_ddladmin, "
                  "IS_MEMBER('db_securityadmin') AS db_securityadmin")
MSSQL_ROLES = ("sysadmin", "db_owner", "db_datawriter", "db_ddladmin", "db_securityadmin")

class DbxError(Exception):
    def __init__(self, rc: int, stderr: str):
        super().__init__(f"databricks exited {rc}: {stderr[:300]}")
        self.rc, self.stderr = rc, stderr
def default_dbx(args: list[str]) -> object:
    argv = ["databricks", *args]
    if args and args[0] != "psql" and "--output" not in args:
        argv += ["--output", "json"]
    try:
        out = subprocess.run(argv, capture_output=True, text=True, timeout=60, stdin=subprocess.DEVNULL)
        if out.returncode != 0:
            raise DbxError(out.returncode, out.stderr or out.stdout)
        return json.loads(out.stdout)
    except FileNotFoundError as exc:
        raise DbxError(127, str(exc)) from exc
    except json.JSONDecodeError:
        return out.stdout
def default_run_cmd(argv: list[str], cwd: str | None = None, stdin: str = "") -> tuple[int, str, str]:
    try:
        out = subprocess.run(argv, cwd=cwd, input=stdin, capture_output=True, text=True, timeout=60)
        return out.returncode, out.stdout, out.stderr
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return 127, "", str(exc)
def _rows(resp) -> list:
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        return next((v for v in resp.values() if isinstance(v, list)), [])
    return []
def _rid(resource: dict) -> str:
    return str(resource.get("name") or resource.get("id") or "").rsplit("/", 1)[-1]
def _sql(dbx, policy: dict, statement: str) -> list:
    wid = str(policy["targets"]["warehouse_http_path"]).rstrip("/").rsplit("/", 1)[-1]
    resp = dbx(["api", "post", "/api/2.0/sql/statements", "--json",
                json.dumps({"warehouse_id": wid, "statement": statement, "wait_timeout": "50s"})])
    state = (resp.get("status") or {}).get("state") if isinstance(resp, dict) else None
    if state != "SUCCEEDED":
        raise DbxError(1, f"{statement}: statement state={state}")
    return (resp.get("result") or {}).get("data_array") or []
def _privs(resp) -> set[str]:
    out = set()
    for a in _rows(resp.get("privilege_assignments") if isinstance(resp, dict) else resp):
        for p in a.get("privileges") or []:
            out.add(p.get("privilege") if isinstance(p, dict) else str(p))
    return {p for p in out if p}
def _grants(dbx, securable_type: str, name: str, principal: str) -> set[str]:
    return _privs(dbx(["grants", "get-effective", securable_type, name, "--principal", principal]))
def check_identity(policy: dict, dbx, env) -> tuple[str, dict]:
    me = dbx(["current-user", "me"])
    candidates = {me.get(k) for k in ("userName", "applicationId", "servicePrincipalName", "displayName")} - {None}
    auth = env.get("DATABRICKS_AUTH_TYPE", "")
    data = {"auth_type": auth or None, "expected": policy["migration_principal"],
            "me": {k: me.get(k) for k in ("userName", "applicationId", "servicePrincipalName") if me.get(k)}}
    if auth not in ("oauth-m2m", "env-oidc"):
        return "fail", {**data, "reason": "DATABRICKS_AUTH_TYPE must be oauth-m2m or env-oidc"
                        + (" (PAT)" if auth == "pat" else "")}
    if policy["migration_principal"] not in candidates:
        return "fail", {**data, "reason": "principal mismatch"}
    return "ok", data
def check_isolation(policy: dict, dbx) -> tuple[str, dict]:
    principal, targets_cat = policy["migration_principal"], policy["targets"]["catalog"]
    lb = policy["targets"].get("lakebase") or {}
    access_cat = str(policy["source"]["access"]).split(".")[0]
    expected_reads = {access_cat} | set(policy.get("forbidden_catalogs") or [])
    writes, extra_reads, errors, bad_roles = [], [], [], []
    try:
        me = dbx(["current-user", "me"])
        groups = {str(g.get("display") or "") for g in (me.get("groups") or []) if isinstance(g, dict)}
    except DbxError as exc:
        groups, errors = set(), errors + [f"current-user: {exc}"]
    admin_groups = sorted(groups & {"admins"})
    catalogs = [r[0] for r in _sql(dbx, policy, "SHOW CATALOGS")]
    try:
        owners = {str(c.get("name")): c.get("owner") for c in _rows(dbx(["catalogs", "list"]))}
    except DbxError as exc:
        owners, errors = {}, errors + [f"catalogs: {exc}"]
    access_select = False
    for cat in catalogs:
        if cat != targets_cat and owners.get(cat) == principal:
            writes.append(f"catalog {cat}:owns")
        try:
            privs = _grants(dbx, "catalog", cat, principal)
        except DbxError as exc:
            errors.append(f"grants {cat}: {exc}")
            continue
        writes += [f"{cat}:{p}" for p in privs if p.upper() not in UC_READ and cat != targets_cat]
        extra_reads += [f"{cat}:{p}" for p in privs
                        if p.upper() in UC_READ and cat not in expected_reads and cat != targets_cat]
        access_select |= cat == access_cat and bool(privs & {"SELECT", "USE_CATALOG"})
        if cat == targets_cat:
            continue
        try:
            for sch in _sql(dbx, policy, f"SHOW SCHEMAS IN `{cat}`"):
                writes += [f"{cat}.{sch[0]}:{p}" for p in _grants(dbx, "schema", f"{cat}.{sch[0]}", principal)
                           if p.upper() not in UC_READ]
        except DbxError as exc:
            errors.append(f"schemas {cat}: {exc}")
    for plural, typ in (("external-locations", "external_location"), ("storage-credentials", "storage_credential"),
                        ("connections", "connection")):
        try:
            for res in _rows(dbx([plural, "list"])):
                if (name := _rid(res)):
                    writes += ([f"{typ} {name}:owns"] if res.get("owner") == principal else [])
                    writes += [f"{typ} {name}:{p}" for p in _grants(dbx, typ, name, principal)
                               if p.upper() not in UC_READ]
        except DbxError as exc:
            errors.append(f"{plural}: {exc}")
    try:
        for proj in _rows(dbx(["postgres", "list-projects"])):
            pid = _rid(proj)
            for br in _rows(dbx(["postgres", "list-branches", f"projects/{pid}"])):
                bid = _rid(br)
                for role in _rows(dbx(["postgres", "list-roles", f"projects/{pid}/branches/{bid}"])):
                    names = {_rid(role), (role.get("spec") or {}).get("identity") or ""}
                    if principal in names and (pid != lb.get("project") or bid == "production"):
                        bad_roles.append(f"{pid}/{bid}:{_rid(role)}")
    except DbxError as exc:
        errors.append(f"postgres: {exc}")
    data = {"writes_outside_targets": writes, "lakebase_role_violations": bad_roles, "admin_groups": admin_groups,
            "unexpected_read_grants": extra_reads, "access_select": access_select, "errors": errors}
    status = ("fail" if writes or bad_roles or admin_groups or not access_select
              else "warn" if extra_reads or errors else "ok")
    return status, data
def validate_policy(p) -> list[str]:
    if not isinstance(p, dict):
        return ["policy.json is not an object"]
    e = ["version must be 2"] if p.get("version") != 2 else []
    for k in ("repo", "base_branch", "migration_principal"):
        if not isinstance(p.get(k), str) or not p[k].strip():
            e.append(f"{k} required")
    t = p.get("targets") or {}
    if not isinstance(t, dict) or not isinstance(t.get("catalog"), str) or not t.get("catalog"):
        e.append("targets.catalog required")
    if not isinstance(t.get("warehouse_http_path") if isinstance(t, dict) else None, str):
        e.append("targets.warehouse_http_path required")
    lb = t.get("lakebase") if isinstance(t, dict) else None
    if lb is not None and (not isinstance(lb, dict) or not isinstance(lb.get("project"), str)
                           or not isinstance(lb.get("branch"), str)):
        e.append("targets.lakebase needs project and branch")
    for k in ("forbidden_catalogs", "forbidden_targets", "bundle_targets", "legacy_sources"):
        if not isinstance(p.get(k), list) or not all(isinstance(x, str) for x in p[k]):
            e.append(f"{k} must be a list of strings")
    for k in ("width", "breaker"):
        if k in p and (not isinstance(p[k], int) or p[k] < 1):
            e.append(f"{k} must be a positive int")
    s = p.get("source") or {}
    if not isinstance(s.get("family"), str) or not s.get("family"):
        e.append("source.family required")
    elif not isinstance(s.get("access"), str) or not s.get("access"):
        e.append("source.access required")
    elif s.get("kind") is not None and s["kind"] not in ("federation", "landed", "loaded_from_export", "existing"):
        e.append("source.kind must be federation|landed|loaded_from_export|existing")
    elif s.get("family") != "databricks" and not s.get("secret"):
        e.append("source.secret required for non-databricks families")
    if "tolerances" in p:
        tol = p["tolerances"]
        if not isinstance(tol, dict):
            e.append("tolerances must be an object")
        elif not isinstance(tol.get("blocking_findings", []), list):
            e.append("tolerances.blocking_findings must be a list")
    return e
def validate_slices(doc) -> list[str]:
    if not isinstance(doc, dict) or doc.get("version") != 1 or not isinstance(doc.get("slices"), list):
        return ["slices.json needs version 1 and a slices list"]
    e = []
    for i, s in enumerate(doc["slices"]):
        w = f"slices[{i}]"
        if not isinstance(s, dict):
            e.append(f"{w} not an object")
            continue
        for k in ("id", "name"):
            if not isinstance(s.get(k), str) or not s[k]:
                e.append(f"{w}.{k} required")
        if s.get("track") not in ("delta", "lakebase"):
            e.append(f"{w}.track must be delta|lakebase")
        for k in ("tables", "routines", "jobs", "consumers", "depends_on"):
            if k in s and not isinstance(s[k], list):
                e.append(f"{w}.{k} must be a list")
        if "verify_depth" in s and s["verify_depth"] not in ("sampled", "full"):
            e.append(f"{w}.verify_depth must be sampled|full")
        if "excluded" in s and not all(isinstance(x, dict) and x.get("object") and x.get("decision")
                                       for x in s.get("excluded") or []):
            e.append(f"{w}.excluded rows need object and decision")
        sw = s.get("shared_writes")
        if sw is not None and (not isinstance(sw, dict)
                               or not all(str(v).startswith(("merged:", "dual_write:")) for v in sw.values())):
            e.append(f"{w}.shared_writes values must be merged:<owner>|dual_write:<owner>")
    return e
def _base(repo: Path, run_cmd) -> str:
    rc, out, _ = run_cmd(["git", "-C", str(repo), "show", "origin/HEAD:.migration/base"])
    if rc == 0 and out.strip():
        return out.strip()
    f = repo / ".migration" / "base"
    return f.read_text().strip() if f.is_file() and f.read_text().strip() else "origin/HEAD"
def check_policy_committed(repo: Path, run_cmd) -> tuple[str, dict]:
    data = {"base": _base(repo, run_cmd)}
    if (missing := [r for r in (POLICY_REL, SLICES_REL) if not (repo / r).is_file()]):
        return "fail", {**data, "reason": f"{missing[0]} missing"}
    rc, untracked, _ = run_cmd(["git", "-C", str(repo), "ls-files", "--others", "--exclude-standard",
                                "--", POLICY_REL, SLICES_REL])
    if rc == 0 and untracked.strip():
        return "fail", {**data, "reason": "uncommitted working copy"}
    rc, _, err = run_cmd(["git", "-C", str(repo), "diff", "--quiet", data["base"], "--", POLICY_REL, SLICES_REL])
    if rc != 0:
        return "fail", {**data, "reason": err.strip()[:200] if rc > 1 else "working copy differs from base"}
    errors, docs = [], {}
    for rel in (POLICY_REL, SLICES_REL):
        try:
            docs[rel] = json.loads((repo / rel).read_text())
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"{rel}: {exc}")
    errors += validate_policy(docs.get(POLICY_REL)) + validate_slices(docs.get(SLICES_REL))
    if not errors and (docs[POLICY_REL]["targets"].get("lakebase") is None
                       and any(s.get("track") == "lakebase" for s in docs[SLICES_REL]["slices"]
                               if isinstance(s, dict))):
        errors.append("lakebase slice but no targets.lakebase")
    if errors:
        return "fail", {**data, "errors": errors}
    if (repo / ".migration" / "waves").is_dir():
        return "warn", {**data, "reason": "0.6 workspace: run policy_from_06.py"}
    return "ok", data
def _objects(slices: list) -> dict:
    schemas, tables, routines, globs = set(), [], [], set()
    for s in slices:
        for t in s.get("tables") or []:
            if isinstance(t, str):
                tables.append(t)
                if "." in t:
                    schemas.add(t.split(".", 1)[0])
        for r in s.get("routines") or []:
            if isinstance(r, str):
                (schemas.add(r[:-2]), globs.add(r[:-2])) if r.endswith(".*") else routines.append(r)
    return {"schemas": sorted(schemas), "tables": tables, "routines": routines, "globs": sorted(globs)}
def _q(value: str) -> str:
    return "'" + value.replace("'", "") + "'"
def _col0(rows: list) -> list:
    return [str(r[0] if not isinstance(r, dict) else next(iter(r.values()))) for r in rows]
def _source_queries(family: str, obj: dict) -> list[tuple[str, str]]:
    if family == "oracle":
        sch = ",".join(_q(s.upper()) for s in obj["schemas"]) or "''"
        return [("session_privs", "SELECT privilege FROM session_privs"),
                ("tab_privs", "SELECT table_schema, table_name, privilege FROM all_tab_privs WHERE grantee IN "
                              "(SELECT SYS_CONTEXT('USERENV','SESSION_USER') FROM dual UNION SELECT 'PUBLIC' FROM dual "
                              f"UNION SELECT role FROM session_roles) AND table_schema IN ({sch})"),
                ("session_user", "SELECT SYS_CONTEXT('USERENV','SESSION_USER') AS session_user FROM dual")]
    if family == "sqlserver":
        return [("server_privs", "SELECT permission_name FROM fn_my_permissions(NULL,'SERVER')"),
                ("db_privs", "SELECT permission_name FROM fn_my_permissions(NULL,'DATABASE')"),
                ("roles", MSSQL_BAD_ROLE),
                *[(f"expand:{g}", f"SELECT s.name + '.' + o.name FROM sys.objects o"
                                  f" JOIN sys.schemas s ON s.schema_id = o.schema_id"
                                  f" WHERE s.name = {_q(g)} AND o.type IN ('P','FN','IF','TF','AF','PC')")
                  for g in obj["globs"]],
                *[(f"object:{o}", f"SELECT permission_name FROM fn_my_permissions({_q(o)},'OBJECT')")
                  for o in obj["tables"] + obj["routines"]],
                *[(f"schema:{s}", f"SELECT permission_name FROM fn_my_permissions({_q(s)},'SCHEMA')")
                  for s in obj["schemas"]]]
    if family == "postgres":
        sch = ",".join(_q(s) for s in obj["schemas"]) or "''"
        glo = ",".join(_q(g) for g in obj["globs"]) or "''"
        q = [("role_flags", "SELECT rolsuper, rolcreatedb, rolcreaterole, rolbypassrls FROM pg_roles "
                            "WHERE rolname = current_user"),
             ("schema_privs", f"SELECT nspname, pg_get_userbyid(nspowner) = current_user,"
                              f" has_schema_privilege(nspname, 'CREATE') FROM pg_namespace WHERE nspname IN ({sch})"),
             ("expand:funcs", f"SELECT p.oid::regprocedure::text FROM pg_proc p"
                              f" JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname IN ({glo})")]
        if obj["tables"]:
            vals = ",".join(f"({_q(t)})" for t in obj["tables"])
            q += [("table_privs", f"SELECT v.obj, (has_table_privilege(v.obj,'INSERT') OR "
                                  f" has_table_privilege(v.obj,'UPDATE') OR has_table_privilege(v.obj,'DELETE') OR "
                                  f" has_table_privilege(v.obj,'TRUNCATE') OR has_table_privilege(v.obj,'REFERENCES') OR "
                                  f" has_table_privilege(v.obj,'TRIGGER')) AS can_write FROM (VALUES {vals}) AS v(obj)"),
                  ("table_owner", f"SELECT n.nspname || '.' || c.relname AS obj FROM pg_class c"
                                  f" JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname IN ({sch})"
                                  f" AND pg_get_userbyid(c.relowner) = current_user")]
        return q
    return []
def _source_violations(family: str, obj: dict, results: dict[str, list]) -> list[str]:
    col0 = _col0
    if family == "oracle":
        bad = [f"session_priv:{p}" for p in col0(results.get("session_privs") or []) if p.upper() not in ORACLE_READ]
        schemas = {s.upper() for s in obj["schemas"]}
        for r in results.get("tab_privs") or []:
            schema, priv = (r.get("table_schema"), r.get("privilege")) if isinstance(r, dict) else (r[0], r[2])
            if str(schema).upper() in schemas and str(priv).upper() not in ("SELECT", "READ"):
                bad.append(f"tab_priv:{r}")
        return bad + (["owns slice schema"] if set(col0(results.get("session_user") or [])) & set(obj["schemas"]) else [])
    if family == "sqlserver":
        bad = []
        for k, rows in results.items():
            if k == "roles":
                bad += [f"role:{c}" for row in rows for c, v in
                        (row.items() if isinstance(row, dict) else zip(MSSQL_ROLES, row)) if v]
            elif not k.startswith("expand:"):
                bad += [f"{k}:{p}" for p in col0(rows) if p.upper() not in MSSQL_READ]
        return bad
    if family == "postgres":
        flags = (results.get("role_flags") or [{}])[0]
        bad = [f"role:{k}" for k, v in (flags.items() if isinstance(flags, dict)
               else zip(("rolsuper", "rolcreatedb", "rolcreaterole", "rolbypassrls"), flags)) if v]
        bad += [f"table:{r[0] if not isinstance(r, dict) else r.get('obj')}" for r in results.get("table_privs") or []
                if (r.get("can_write") if isinstance(r, dict) else r[1])]
        for r in results.get("schema_privs") or []:
            name, owns, creates = (r.get("nspname"), r.get("owns"), r.get("can_create")) if isinstance(r, dict) else r
            bad += [f"owns:{name}"] if owns else []
            bad += [f"schema-create:{name}"] if creates else []
        bad += [f"owns:{o}" for o in col0(results.get("table_owner") or [])]
        bad += [f"routine:{k[7:]}" for k, rows in results.items() if k.startswith("object:")
                and any(bool(r[0] if not isinstance(r, dict) else next(iter(r.values()))) for r in rows)]
        return bad
    return []
def check_source_read_only(policy: dict, slices: list, dbx, run_sql, principal: str) -> tuple[str, dict]:
    src, family = policy.get("source") or {}, str((policy.get("source") or {}).get("family") or "").lower()
    obj = _objects(slices)
    if family == "databricks" or src.get("kind") == "existing":
        parts = str(src.get("access") or "").split(".")
        sec = [("catalog", parts[0]), *([("schema", ".".join(parts[:2]))] if len(parts) > 1 else []),
               *[("schema", f"{parts[0]}.{s}") for s in obj["schemas"] if len(parts) == 1 or s != parts[1]],
               *[("table", f"{parts[0]}.{t}") for t in obj["tables"]]]
        seen, writes, errs = set(), [], []
        for typ, name in sec:
            if name in seen:
                continue
            seen.add(name)
            try:
                writes += [f"{typ} {name}:{p}" for p in _grants(dbx, typ, name, principal)
                           if p.upper() not in UC_READ]
            except DbxError as exc:
                errs.append(f"{typ} {name}: {exc}")
        data = {"catalog": parts[0], "write_privileges": writes, "errors": errs}
        return ("fail" if writes else "warn" if errs else "ok"), data
    if family not in ("oracle", "sqlserver", "postgres"):
        return "warn", {"family": family, "reason": "no source_read_only adapter for family"}
    if run_sql is None:
        return "warn", {"family": family, "reason": "source adapter not wired (pass --source-adapter module:callable)"}
    queries = _source_queries(family, obj)
    results = {name: run_sql(sql) for name, sql in queries}
    for name in [n for n, _ in queries if n.startswith("expand:")]:
        for fn in _col0(results[name]):
            sql = (f"SELECT permission_name FROM fn_my_permissions({_q(fn)},'OBJECT')"
                   if family == "sqlserver" else f"SELECT has_function_privilege({_q(fn)},'EXECUTE')")
            results[f"object:{fn}"] = run_sql(sql)
    bad = _source_violations(family, obj, results)
    return ("fail" if bad else "ok"), {"family": family, "violations": bad}
def check_hook_live(plugin_root: Path, run_cmd) -> tuple[str, dict]:
    guard = next((p for p in (plugin_root / "hooks" / "dbx_guard.py", plugin_root / "hooks" / "guard.py")
                  if p.is_file()), None)
    if guard is None:
        return "fail", {"reason": "hooks/dbx_guard.py missing"}
    with tempfile.TemporaryDirectory(prefix="dbx-doctor-probe-") as tmp:
        (Path(tmp) / ".migration").mkdir()
        (Path(tmp) / POLICY_REL).write_text(json.dumps({"version": 2}))
        for argv in (["git", "init", "-q", "-b", "main", "."], ["git", "add", "-A"],
                     ["git", "-c", "user.email=doctor@local", "-c", "user.name=doctor", "commit", "-qm", "policy"]):
            run_cmd(argv, cwd=tmp)
        event = {"tool_name": "exec", "cwd": tmp,
                 "tool_input": {"command": "echo " + "__dbx_guard" + "_probe__"}}
        rc, out, err = run_cmd([sys.executable, str(guard)], cwd=tmp, stdin=json.dumps(event))
    data = {"probe": "sentinel", "armed_cwd": "temp checkout", "guard": guard.name, "exit": rc,
            "stdout": out.strip()[:200], "stderr": err.strip()[:200]}
    if rc != 2:
        return "fail", {**data, "reason": "guard did not block the probe"}
    try:
        data["decision"] = json.loads(out).get("decision") if out.strip() else None
    except json.JSONDecodeError:
        data["decision"] = None
    return "ok", data
def check_harness(policy: dict, slices: list, dbx, run_cmd) -> tuple[str, dict]:
    rc, out, err = run_cmd(["dbx-recon", "selftest"])
    data = {"selftest_rc": rc}
    if rc != 0:
        return "fail", {**data, "stderr": (err or out).strip()[:300]}
    try:
        _sql(dbx, policy, "SELECT 1")
        data["warehouse"] = "ok"
    except DbxError as exc:
        return "fail", {**data, "reason": f"warehouse unreachable: {exc}"}
    if any(s.get("track") == "lakebase" for s in slices if isinstance(s, dict)):
        lb = policy["targets"].get("lakebase") or {}
        try:
            dbx(["psql", "--project", str(lb.get("project")), "--branch", str(lb.get("branch")), "--", "-c", "SELECT 1"])
            data["lakebase"] = "ok"
        except DbxError as exc:
            return "fail", {**data, "reason": f"lakebase unreachable: {exc}"}
    return "ok", data
def _load(path: Path):
    try:
        doc = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return doc if isinstance(doc, dict) else None
def run_doctor(repo: Path, *, dbx=None, run_cmd=None, env=None, run_sql=None,
               plugin_root: Path | None = None, source_adapter=None) -> dict:
    """`source_adapter` is a zero-arg factory for run_sql; it runs inside guarded() so a driver
    or DSN error is a `fail` row, not a crash before the report."""
    dbx, run_cmd = dbx or default_dbx, run_cmd or default_run_cmd
    env = os.environ if env is None else env
    plugin_root = Path(plugin_root) if plugin_root else Path(__file__).resolve().parents[1]
    repo = Path(repo)
    policy = _load(repo / POLICY_REL)
    slices_doc = _load(repo / SLICES_REL)
    slices = slices_doc.get("slices") if slices_doc and isinstance(slices_doc.get("slices"), list) else None
    principal = (policy or {}).get("migration_principal", "")

    def guarded(fn):
        try:
            return fn()
        except Exception as exc:
            return "fail", {"error": str(exc)[:400]}

    checks = [
        ("identity", *guarded(lambda: check_identity(policy, dbx, env) if policy
                              else ("fail", {"reason": "policy unreadable"}))),
        ("isolation", *guarded(lambda: check_isolation(policy, dbx) if policy
                               else ("fail", {"reason": "policy unreadable"}))),
        ("policy_committed", *guarded(lambda: check_policy_committed(repo, run_cmd))),
        ("source_read_only", *guarded(lambda: check_source_read_only(
                                          policy, slices, dbx,
                                          run_sql if run_sql is not None or source_adapter is None else source_adapter(),
                                          principal)
                                      if policy and slices is not None
                                      else ("fail", {"reason": "policy/slices unreadable"}))),
        ("hook_live", *guarded(lambda: check_hook_live(plugin_root, run_cmd))),
        ("harness", *guarded(lambda: check_harness(policy, slices, dbx, run_cmd)
                             if policy and slices is not None else ("fail", {"reason": "policy/slices unreadable"}))),
    ]
    return {"ready": all(s != "fail" for _, s, _ in checks),
            "checks": [{"check": n, "status": s, "data": d} for n, s, d in checks]}
def render_markdown(report: dict) -> str:
    lines = ["| check | status | data |", "|---|---|---|"]
    for c in report["checks"]:
        cell = json.dumps(c["data"], sort_keys=True).replace("|", "\\|")
        lines.append(f"| {c['check']} | {c['status']} | {cell} |")
    return "\n".join(lines) + f"\n\n**ready**: {str(report['ready']).lower()}\n"
def main(argv: list[str] | None = None, **injected) -> int:
    p = argparse.ArgumentParser(description="dbx-migration-factory 0.7 doctor (design-0.7.md §5)")
    p.add_argument("--repo", default=".", help="run repo containing .migration/ (default: cwd)")
    p.add_argument("--markdown", metavar="PATH", help="also write the Markdown rendering to PATH")
    p.add_argument("--source-adapter", metavar="MODULE:CALLABLE",
                   help="callable(policy) -> run_sql for source_read_only (legacy drivers live in the harness)")
    args = p.parse_args(argv)
    run_sql, source_adapter = injected.get("run_sql"), None
    if run_sql is None and args.source_adapter:
        def source_adapter(spec=args.source_adapter, repo=Path(args.repo)):
            factory = __import__(spec.split(":", 1)[0], fromlist=["x"])
            for part in spec.split(":", 1)[1].split("."):
                factory = getattr(factory, part)
            return factory(_load(repo / POLICY_REL))
    report = run_doctor(Path(args.repo), dbx=injected.get("dbx"), run_cmd=injected.get("run_cmd"),
                        env=injected.get("env"), run_sql=run_sql, plugin_root=injected.get("plugin_root"),
                        source_adapter=source_adapter)
    print(json.dumps(report, indent=2, sort_keys=True))
    if args.markdown:
        Path(args.markdown).write_text(render_markdown(report))
    return 0 if report["ready"] else 1

if __name__ == "__main__":
    sys.exit(main())
