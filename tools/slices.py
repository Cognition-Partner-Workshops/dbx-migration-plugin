"""`.migration/slices.json` validation and `--check` (design-0.7 section 6).

Usage:
    python3 tools/slices.py --check [SLICES_JSON] [--deps DEPS_JSON] [--census CENSUS_JSON]

Exits 0 when the slice plan is valid; exits 1 and prints one `FAIL <code>:` line
per violation otherwise. `--deps` and `--census` are optional; the rules that
need them are skipped when the file is not given.

Dependency scan input (`--deps`), produced by the dialect skill's dependency
scan -- this tool reads it, it does not implement the scan:

    {"version": 1,
     "routines": {"<SCHEMA.NAME>": {"writes": ["<SCHEMA.TABLE>", ...],
                                  "reads":  ["<SCHEMA.TABLE>", ...]}},
     "jobs":     {"<NAME>":        {"writes": [...], "reads": [...]}}}

Object names match case-insensitively. `writes`/`reads` are transitive sets.
slices.json `routines`/`jobs` entries may be fnmatch globs (`PKG_PLANS.*`);
every declared entry must match at least one scan key.

Census input (`--census`), produced by skills/analyze:

    {"version": 1, "objects": [{"name": "<SCHEMA.OBJ>", "kind": "<free-form>"}]}

`objects` may also be a bare list of names; `kind` is optional and unused. A
census object is assigned when it matches any slice's `tables`, `routines` or
`jobs` entry (glob-aware) or an `excluded` row carrying a decision id.
"""
import argparse
import fnmatch
import json
import re
import sys

VERSION = 1
SLICE_ZERO = "slice-0"
SLICE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")  # path-safe: fanout writes recon/<id>/
TRACKS = {"delta", "lakebase"}
ROUTINGS = {"none", "jdbc_url", "synonym", "api"}
GROUPED_ROUTINGS = {"jdbc_url", "synonym"}  # not independently switchable per capability
VERIFY_DEPTHS = {"sampled", "full"}
LIST_FIELDS = ("tables", "routines", "jobs", "consumers", "depends_on")


def _norm(name):
    return str(name).strip().upper()


def _match(pattern, name):
    return fnmatch.fnmatchcase(_norm(name), _norm(pattern))


def _schema_check(doc):
    if not isinstance(doc, dict):
        return [("schema", "document is not an object")]
    fails = []
    if doc.get("version") != VERSION:
        fails.append(("schema", f"version must be {VERSION}"))
    if not isinstance(doc.get("slices"), list) or not doc["slices"]:
        fails.append(("schema", "slices must be a non-empty list"))
        return fails
    seen = set()
    for i, s in enumerate(doc["slices"]):
        if not isinstance(s, dict):
            fails.append(("schema", f"slice index {i} is not an object"))
            continue
        tag = s.get("id") or f"index {i}"
        if not isinstance(s.get("id"), str) or not s["id"]:
            fails.append(("schema", f"slice index {i} missing string id"))
        elif not SLICE_ID.fullmatch(s["id"]):
            fails.append(("schema", f"{tag}: id must match {SLICE_ID.pattern}"))
        elif s["id"] in seen:
            fails.append(("dup_slice", s["id"]))
        seen.add(s.get("id"))
        if s.get("track") not in TRACKS:
            fails.append(("schema", f"{tag}: track must be one of {sorted(TRACKS)}"))
        if s.get("routing") not in ROUTINGS:
            fails.append(("schema", f"{tag}: routing must be one of {sorted(ROUTINGS)}"))
        if "verify_depth" in s and s["verify_depth"] not in VERIFY_DEPTHS:
            fails.append(("schema", f"{tag}: verify_depth must be one of {sorted(VERIFY_DEPTHS)}"))
        if "route_group" in s and not isinstance(s["route_group"], str):
            fails.append(("schema", f"{tag}: route_group must be a string"))
        for fld in LIST_FIELDS:
            if fld in s and not (isinstance(s[fld], list)
                                 and all(isinstance(x, str) for x in s[fld])):
                fails.append(("schema", f"{tag}: {fld} must be a list of strings"))
        if "shared_writes" in s and not isinstance(s["shared_writes"], dict):
            fails.append(("schema", f"{tag}: shared_writes must be an object"))
        for row in s.get("excluded") or []:
            if not isinstance(row, dict) or not row.get("object"):
                fails.append(("excluded", f"{tag}: excluded row missing object"))
            elif not row.get("decision"):
                fails.append(("excluded_decision",
                              f"{tag}: {row['object']} excluded without a decision id"))
    return fails


def _topology_check(slices):
    fails = []
    if slices[0]["id"] != SLICE_ZERO:
        fails.append(("slice0", f"first slice must be {SLICE_ZERO}, got {slices[0]['id']}"))
    if slices[0].get("depends_on"):
        fails.append(("slice0", f"{SLICE_ZERO} must not declare depends_on"))
    ids = {s["id"] for s in slices}
    for s in slices:
        for dep in s.get("depends_on", []):
            if dep not in ids:
                fails.append(("unknown_dependency", f"{s['id']} depends on unknown {dep}"))
    graph = {s["id"]: s.get("depends_on", []) for s in slices}
    state = {}  # 1 = on the DFS stack, 2 = done

    def visit(node, stack):
        state[node] = 1
        for dep in graph.get(node, []):
            if dep not in graph:
                continue
            if state.get(dep) == 1:
                fails.append(("dependency_cycle", " -> ".join(stack + [node, dep])))
            elif state.get(dep) is None:
                visit(dep, stack + [node])
        state[node] = 2

    for node in graph:
        if state.get(node) is None:
            visit(node, [])
    return fails


def _canary_check(slices):
    fails = []
    non_zero = [s for s in slices if s["id"] != SLICE_ZERO]
    canaries = [s for s in slices if s.get("canary")]
    if len(canaries) != 1:
        fails.append(("canary", f"exactly one canary slice required, found {len(canaries)}"))
    elif non_zero and canaries[0] is not non_zero[0]:
        fails.append(("canary", f"canary must be the first non-zero slice "
                                f"({non_zero[0]['id']}), got {canaries[0]['id']}"))
    for s in slices:
        if s.get("auto_merge", True) == bool(s.get("canary")):
            fails.append(("auto_merge", f"{s['id']}: auto_merge is "
                                        f"{'false' if s.get('canary') else 'true'} "
                                        f"for a {'canary' if s.get('canary') else 'non-canary'} slice"))
    return fails


def _consumer_check(slices):
    fails = []
    owners = {}
    for s in slices:
        for c in s.get("consumers", []):
            owners.setdefault(c, []).append(s)
    for consumer, holders in owners.items():
        grouped = [s for s in holders if s.get("routing") in GROUPED_ROUTINGS]
        for i, a in enumerate(grouped):
            for b in grouped[i + 1:]:
                if not (a.get("route_group") and a["route_group"] == b.get("route_group")):
                    fails.append(("consumer_route_split",
                                  f"{consumer}: {a['id']} and {b['id']} share a consumer on "
                                  f"grouped routing but differ in route_group "
                                  f"({a.get('route_group')!r} vs {b.get('route_group')!r})"))
    return fails


def _shared_writes_check(slices):
    fails = []
    holders = {}
    for s in slices:
        for t in s.get("tables", []):
            holders.setdefault(_norm(t), []).append(s)
    for s in slices:
        for table, disp in (s.get("shared_writes") or {}).items():
            kind, sep, owner = str(disp).partition(":") if isinstance(disp, str) else ("", "", "")
            if not sep or kind not in ("merged", "dual_write") or not owner:
                fails.append(("shared_writes", f"{s['id']}: {table} -> {disp!r} must be "
                                               "merged:<slice> or dual_write:<slice>"))
                continue
            if not any(_match(table, t) for t in s.get("tables", [])):
                fails.append(("shared_writes", f"{s['id']}: {table} declared by a slice "
                                               "that does not hold it"))
                continue
            if not any(h["id"] == owner for h in holders.get(_norm(table), [])):
                fails.append(("shared_writes", f"{s['id']}: {table} owner {owner} "
                                               "does not hold the table"))
    for table, slices_holding in holders.items():
        if len(slices_holding) > 1 and not any(
                any(_match(table, t) for t in s.get("shared_writes") or {})
                for s in slices_holding):
            fails.append(("shared_writes", f"{table} appears in "
                                           f"{[s['id'] for s in slices_holding]} "
                                           "with no disposition"))
    return fails


def _scan_entries(section):
    return {name: {"writes": e.get("writes") or [], "reads": e.get("reads") or []}
            for name, e in (section or {}).items() if isinstance(e, dict)}


def _deps_check(slices, deps):
    fails = []
    scans = {fld: _scan_entries(deps.get(fld)) for fld in ("routines", "jobs")}
    tables = {s["id"]: {_norm(t) for t in s.get("tables", [])} for s in slices}
    slice0_tables = tables.get(SLICE_ZERO, set())
    readers = {}
    for s in slices:
        allowed = tables[s["id"]] | slice0_tables
        read = set()
        for fld in ("routines", "jobs"):
            for pat in s.get(fld, []):
                matched = [n for n in scans[fld] if _match(pat, n)]
                if not matched:
                    fails.append(("unscanned_routine",
                                  f"{s['id']}: {pat} matches no dependency-scan entry"))
                    continue
                for name in matched:
                    read |= {_norm(r) for r in scans[fld][name]["reads"]}
                    for w in scans[fld][name]["writes"]:
                        if _norm(w) not in allowed:
                            fails.append(("cross_slice_write",
                                          f"{s['id']}: {name} writes {w}, outside its "
                                          f"slice and {SLICE_ZERO}"))
        readers[s["id"]] = read
    for s in slices:
        if s.get("routing") == "none":
            continue
        for other in slices:
            if other is s or other["id"] == SLICE_ZERO:
                continue
            hits = sorted(readers.get(other["id"], set()) & tables[s["id"]])
            if hits and not (s.get("route_group")
                             and s["route_group"] == other.get("route_group")):
                fails.append(("cross_route_read",
                              f"{s['id']}: table(s) {hits} read by routines/jobs of "
                              f"{other['id']} outside its route_group"))
    return fails


def _census_check(slices, census):
    objects = census.get("objects") if isinstance(census, dict) else census
    if not isinstance(objects, list):
        return [("census", "objects must be a list")]
    patterns = [e for s in slices for fld in ("tables", "routines", "jobs")
                for e in s.get(fld, [])]
    excluded = [r["object"] for s in slices for r in (s.get("excluded") or [])
                if isinstance(r, dict) and r.get("object") and r.get("decision")]
    fails = []
    for obj in objects:
        name = obj.get("name") if isinstance(obj, dict) else obj
        if not isinstance(name, str):
            continue
        if any(_match(e, name) for e in excluded):
            continue
        if not any(_match(p, name) for p in patterns):
            fails.append(("unassigned_census",
                          f"{name} assigned to no slice and not excluded with a decision"))
    return fails


def check(doc, deps=None, census=None):
    """Every --check rule of section 6. Returns a list of (code, detail) failures."""
    fails = _schema_check(doc)
    if fails:
        return fails
    slices = doc["slices"]
    fails = _topology_check(slices)
    fails += _canary_check(slices)
    fails += _consumer_check(slices)
    fails += _shared_writes_check(slices)
    if deps is not None:
        fails += _deps_check(slices, deps)
    if census is not None:
        fails += _census_check(slices, census)
    return fails


def _read(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"{path}: {exc}")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="slices.py",
                                 description="validate .migration/slices.json (design-0.7 section 6)")
    ap.add_argument("--check", action="store_true", help="run every section-6 check rule")
    ap.add_argument("slices", nargs="?", default=".migration/slices.json")
    ap.add_argument("--deps", help="dependency scan JSON (dialect skill output)")
    ap.add_argument("--census", help="estate census JSON (analyze output)")
    args = ap.parse_args(argv)
    if not args.check:
        ap.error("nothing to do: pass --check")
    fails = check(_read(args.slices),
                  deps=_read(args.deps) if args.deps else None,
                  census=_read(args.census) if args.census else None)
    for code, detail in fails:
        print(f"FAIL {code}: {detail}")
    if fails:
        return 1
    print("slices.json OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
