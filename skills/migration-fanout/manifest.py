"""What a wave manifest may say: field grammar, gate shapes, write-target keys, reader predicates and
the checks a manifest passes before anything launches. Pure functions; the workflow passes what it read."""

import hashlib
import hmac
import json
import re
import subprocess
import sys
import urllib.parse
from collections import Counter
from pathlib import Path


TAG_RE = re.compile(r"[A-Za-z0-9_-]+")


PIPELINE_RE = re.compile(r"[A-Za-z0-9_]*[A-Za-z_][A-Za-z0-9_]*")


# a plan decision id: the lowercase slug of a plan.yaml decision
DECISION_ID = re.compile(r"[a-z0-9][a-z0-9_.-]*")

TARGET_DECISIONS = {
    "core": "target-core",
    "sql": "target-sql",
    "pipeline": "target-pipeline",
    "orchestration": "target-orchestration",
    "consumer": "target-consumer",
    "lakebase": "target-lakebase",
    "ml_scoring": "target-ml-scoring",
    "data_dependency": "target-data-dependency",
}


# a skill name: skills/<name>/SKILL.md under the plugin root
SKILL_NAME = re.compile(r"[a-z0-9][a-z0-9-]*")


VERIFY_DEPTHS = ("sampled", "full")


MERGE_EVIDENCE_MODES = ("live", "snapshot", "transactional")


GUARD_MODES = ("block", "warn")


GATE_KINDS = ("byte_compare", "export_file", "publish_leg", "row_parity", "structural", "custom")


GATE_STATUSES = ("pending", "passed", "failed", "waived")


UNIT_ID = re.compile(r"(?!wave-)[A-Za-z0-9_][A-Za-z0-9_.-]*")


BRIEF_MAX_CHARS = 4000


WORD = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_./-]*")


ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


PARAM_VALUE = re.compile(r"[A-Za-z0-9_\-:.T/]+(?: [0-9:.]+)?")


REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")  # host/owner/name


PR_URL = re.compile(r"https://(?P<repo>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/pull/(?P<n>[0-9]+)/?")


BARE_PATH = re.compile(r"\S+")


# a gate's evidence: the bare path, or the path with its annotation beside it, never inside it
EVIDENCE_META = {"label": str, "verdict": str, "rows": int}


def validate_gates(b):
    """A batch's acceptance gates, field by field, exactly as the plan declared them."""
    def req(cond, msg):
        if not cond:
            raise SystemExit(msg)
    gates = b.get("gates")
    req(isinstance(gates, list) and gates and all(isinstance(g, dict) for g in gates),
        f"batch {b['id']} 'gates' must be a non-empty list of {{id, kind, status, evidence, decision_id?}} rows")
    ids = Counter(g.get("id") for g in gates)
    req(all(isinstance(i, str) and UNIT_ID.fullmatch(i) for i in ids),
        f"batch {b['id']} gate 'id' must be a plain word (letters, digits, _ . -)")
    dup = [i for i, c in ids.items() if c > 1]
    req(not dup, f"batch {b['id']} gate ids must be unique: {dup}")
    for g in gates:
        req(g.get("kind") in GATE_KINDS, f"batch {b['id']} gate {g['id']} 'kind' must be one of {GATE_KINDS}")
        req(g.get("status") in GATE_STATUSES, f"batch {b['id']} gate {g['id']} 'status' must be one of {GATE_STATUSES}")
        req(isinstance(g.get("evidence"), str) and (g["status"] != "passed" or g["evidence"]),
            f"batch {b['id']} gate {g['id']} 'evidence' must be a string, non-empty once passed")
        req(not g["evidence"] or BARE_PATH.fullmatch(g["evidence"]),
            f"batch {b['id']} gate {g['id']} 'evidence' must be a bare path (no spaces or notes)")
        decision = g.get("decision_id")
        req(not ((g["status"] == "waived" and decision is None)
                 or (decision is not None and not (isinstance(decision, str) and DECISION_ID.fullmatch(decision)))),
            f"batch {b['id']} gate {g['id']} 'decision_id' must be the lowercase slug of the plan decision "
            "that set it (required for a waived gate)")


def target_key(name, namespace=""):
    """The one identity of a table however a manifest or mapping spells it: trimmed, unquoted, case-folded."""
    def segments(s):
        return [re.sub(r'^[`"\[]|[`"\]]$', "", p.strip()).casefold() for p in str(s).strip().split(".")] \
            if str(s).strip() else []
    parts, prefix = segments(name), segments(namespace)
    if parts and all(parts) and len(parts) <= len(prefix):
        parts = prefix[:len(prefix) + 1 - len(parts)] + parts
    return ".".join(parts)


def valid_namespace(value):
    return isinstance(value, str) and all(re.fullmatch(r"[a-z_][\w$]*", s) for s in target_key(value).split("."))


def reads_target(obj, table, namespace=""):
    o, t = target_key(obj, namespace), target_key(table, namespace)
    if not o:
        return False
    if not namespace and ("." not in o or "." not in t):
        return o.rsplit(".", 1)[-1] == t.rsplit(".", 1)[-1]
    return o == t


def skill_file(plugin, name):
    return plugin / "skills" / name / "SKILL.md"


def validate_manifest(m, plugin=None):
    """Fail here, in one line, instead of 20 children failing on a missing field."""
    def req(cond, msg):
        if not cond:
            raise SystemExit(msg)
    def strs(v):
        return isinstance(v, list) and all(isinstance(s, str) for s in v)
    def pos_int(key, hi):
        v = m.get(key)
        return key not in m or (isinstance(v, int) and not isinstance(v, bool)
                                and (v > 0 if hi is None else 0 < v <= hi))
    for key in ("wave", "repo", "child_skill", "verify_skill", "batches", "base_branch", "plan_step"):
        req(key in m, f"manifest is missing '{key}'")
    req(bool(m["batches"]), "manifest has no batches")
    req(isinstance(m["plan_step"], str) and DECISION_ID.fullmatch(m["plan_step"]),
        f"manifest 'plan_step' must be the lowercase slug of the plan step this wave ticket runs, got "
        f"{m['plan_step']!r}")
    for key in ("child_skill", "verify_skill"):
        req(isinstance(m[key], str) and SKILL_NAME.fullmatch(m[key]),
            f"manifest '{key}' must be a skill name (lowercase letters, digits, -): the skills/<name>/SKILL.md "
            f"the prompt embeds, got {m[key]!r}")
        req(plugin is not None, f"manifest '{key}' needs the pointer's `plugin` root to resolve "
            f"skills/{m[key]}/SKILL.md")
        req(skill_file(plugin, m[key]).is_file(),
            f"manifest '{key}' names no skill of the plugin: {skill_file(plugin, m[key])} does not exist")
    req(isinstance(m["repo"], str) and REPO_RE.fullmatch(m["repo"]),
        f"manifest 'repo' must be host/owner/name (the repo of every child's PR URL), got {m['repo']!r}")
    req(isinstance(m["wave"], int) and not isinstance(m["wave"], bool) and m["wave"] >= 0,
        "manifest key 'wave' must be a non-negative integer")
    for key in ("width", "breaker_threshold"):
        req(pos_int(key, None), f"manifest key '{key}' must be a positive integer")
    for key, hi in (("max_minutes", 60), ("close_minutes", 60), ("doctor_max_age", 1440)):
        req(pos_int(key, hi), f"manifest key '{key}' must be a positive integer of at most {hi} minutes")
    req(isinstance(m.get("degraded", False), bool), "manifest key 'degraded' must be a boolean")
    req("secrets" not in m or strs(m["secrets"]),
        "wave manifest 'secrets' (top level or per batch) must be a list of scope/key strings")
    pipelines = m.get("pipelines")
    req("pipelines" not in m or (isinstance(pipelines, dict) and pipelines
        and all(isinstance(p, str) and PIPELINE_RE.fullmatch(p) for p in pipelines)
        and all(isinstance(n, int) and not isinstance(n, bool) and n > 0 for n in pipelines.values())),
        "manifest key 'pipelines' must map each pipeline the plan split (letters, digits, '_') to a "
        "positive wave count, the <pipeline>-<N> of its wave-<pipeline>-<N>.json manifests")
    req(m["wave"] != 0 or m.get("width", 20) == 1,
        "wave 0 is the serial shared-objects wave: set width to 1")
    req(isinstance(m["base_branch"], str) and WORD.fullmatch(m["base_branch"]) and ".." not in m["base_branch"],
        "manifest 'base_branch' must be a plain branch name (letters, digits, _ . / -)")
    req("target_namespace" not in m or valid_namespace(m["target_namespace"]),
        "manifest 'target_namespace' must be the dotted catalog.schema (or schema) the harness run is "
        "given, so a bare write target or mapping object is that table and no other")
    if "target_state" in m:
        target_state = m["target_state"]
        req(isinstance(target_state, dict) and target_state,
            "manifest key 'target_state' must be a non-empty object")
        unknown_surfaces = sorted(set(target_state) - TARGET_DECISIONS.keys())
        req(not unknown_surfaces,
            f"manifest 'target_state' has unknown surface(s): {unknown_surfaces}")
        for surface, state in target_state.items():
            req(isinstance(state, dict) and isinstance(state.get("decision"), str)
                and DECISION_ID.fullmatch(state["decision"]),
                f"manifest 'target_state.{surface}.decision' must be a lowercase plan decision slug")
            req(state["decision"] == TARGET_DECISIONS[surface],
                f"manifest 'target_state.{surface}.decision' must be "
                f"'{TARGET_DECISIONS[surface]}', the plan decision for that surface")
            target_fields = {"decision", "target", "ref"}
            na_fields = {"decision", "na"}
            req(set(state) in (target_fields, na_fields),
                f"manifest 'target_state.{surface}' must have decision and either target/ref or na")
            if set(state) == target_fields:
                req(all(isinstance(state.get(key), str) and state[key].strip() for key in ("target", "ref")),
                    f"manifest 'target_state.{surface}' target and ref must be non-empty strings")
            else:
                req(isinstance(state.get("na"), str) and state["na"].strip(),
                    f"manifest 'target_state.{surface}.na' must be a non-empty reason")
    req(m["base_branch"] not in ("main", "master")
        or (isinstance(m.get("trunk_base_decision"), str) and m["trunk_base_decision"].strip()),
        "base_branch 'main' is the trunk: wave results and unit PRs land on the engagement "
        "feature branch; set base_branch to it, or record the plan decision and put its slug in "
        "'trunk_base_decision'")
    ids = Counter(b.get("id") for b in m["batches"])
    dupes = [i for i, c in ids.items() if c > 1 or not i]
    req(not dupes, f"batch ids must be unique and non-empty: {dupes}")
    bad = [i for i in ids if not (isinstance(i, str) and UNIT_ID.fullmatch(i))]
    req(not bad, f"batch ids must be a plain word (letters, digits, _ . -), like gate and unit ids: {bad}")
    ns = m.get("target_namespace", "") if isinstance(m.get("target_namespace", ""), str) else ""
    owners = {}
    for b in m["batches"]:
        for key in ("units", "brief"):
            req(bool(b.get(key)),
                f"batch {b['id']} is missing '{key}' (a child with no brief cannot be launched safely)")
        req(isinstance(b["brief"], str) and len(b["brief"]) <= BRIEF_MAX_CHARS,
            f"batch {b['id']} brief is {len(str(b['brief']))} chars; the cap is {BRIEF_MAX_CHARS}. A brief names "
            "the units, targets, gates and the files to read (skills/migration-fanout/references/brief_template.md); "
            "hosts, principals, warehouses and secret names live in the manifest and capabilities.json, "
            "which every child already reads")
        req(strs(b.get("write_targets")) and all(t.strip() for t in b["write_targets"]),
            f"batch {b['id']} needs 'write_targets', a list of table names (empty only for a batch "
            "whose dependency analysis writes nothing)")
        deploy = b.get("deploy_objects", [])
        req(strs(deploy) and all(t.strip() for t in deploy)
            and len({target_key(t, ns) for t in deploy}) == len(deploy),
            f"batch {b['id']} 'deploy_objects' must be a list of distinct names: the procedures, views "
            "and jobs the batch deploys, which no routine's DML writes")
        declared = {target_key(t, ns) for t in b["write_targets"]}
        outside = sorted(t for t in deploy if target_key(t, ns) not in declared)
        req(not outside, f"batch {b['id']} 'deploy_objects' {outside} are not in its write_targets; every object a "
            "child deploys is a write target (it collides like any other)")
        req(b.get("verify_depth", "sampled") in VERIFY_DEPTHS,
            f"batch {b['id']} 'verify_depth' must be one of {VERIFY_DEPTHS}")
        req("max_minutes" not in b or (isinstance(b["max_minutes"], int) and not isinstance(b["max_minutes"], bool)
            and 0 < b["max_minutes"] <= 60),
            f"batch {b['id']} max_minutes must be a positive integer of at most 60 minutes")
        req("secrets" not in b or strs(b["secrets"]),
            "wave manifest 'secrets' (top level or per batch) must be a list of scope/key strings")
        bad = [u for u in b["units"] if not isinstance(u, str) or not UNIT_ID.fullmatch(u)]
        req(not bad, f"batch {b['id']} unit id(s) {bad!r} are not a plain directory name (letters, digits, "
            "_ . -, not wave-*): the id names the only .migration/recon/<unit_id>/ its child may write")
        for u in b["units"]:
            owners.setdefault(u, []).append(b["id"])
        validate_gates(b)
    shared = {u: bs for u, bs in owners.items() if len(bs) > 1}
    req(not shared, f"a unit id belongs to one batch (its child alone writes .migration/recon/<unit_id>/): {shared}")
    overrides = m.get("merge_overrides", [])
    req(isinstance(overrides, list) and all(
            isinstance(e, dict) and {"decision", "units"} <= set(e) <= {"decision", "units", "blocker_classes"}
            and isinstance(e["decision"], str) and DECISION_ID.fullmatch(e["decision"])
            and strs(e["units"]) and e["units"] and ("blocker_classes" not in e or strs(e["blocker_classes"]))
            for e in overrides),
        "manifest 'merge_overrides' must be a list of {decision: <plan decision slug>, units: [unit ids], "
        "blocker_classes?: [classes it forgives]} entries: the plan decisions that let a batch merge on a PASS "
        "verdict while a unit's result.json says merge_eligible=false")
    ghosts = sorted({u for e in overrides for u in e["units"] if u not in owners})
    req(not ghosts, f"manifest 'merge_overrides' units {ghosts!r} are not units of this wave's batches")
    src = m.get("source")
    req(src is None or (isinstance(src, dict) and isinstance(src.get("params", {}), dict)
        and all(isinstance(v, str) and WORD.fullmatch(v) for v in (src.get("family"), *src.get("params", {}).keys()))
        and isinstance(src.get("secret"), str) and ENV_NAME.fullmatch(src["secret"])
        and all(isinstance(v, str) and PARAM_VALUE.fullmatch(v) for v in src.get("params", {}).values())),
        "manifest 'source' must be {family, secret (env var NAME of the DSN), params?}: family and "
        "param names one plain word (letters, digits, _ . / -), secret a shell variable name, param "
        "values what dbx-recon run --param accepts; they become the doctor's command line")
    req(m.get("verify_depth", "sampled") in VERIFY_DEPTHS, f"manifest 'verify_depth' must be one of {VERIFY_DEPTHS}")
    req("cost_estimate" not in m or isinstance(m["cost_estimate"], dict),
        "manifest 'cost_estimate' must be an object (output of `dbx-recon estimate`, summed over the wave)")
    rs = m.get("resync")
    if rs is not None:
        req(isinstance(rs, dict) and set(rs) == {"command", "units"} and isinstance(rs["command"], str)
            and rs["command"].strip() and isinstance(rs["units"], list) and rs["units"],
            "manifest 'resync' must be {command: non-empty shell command, units: [unit ids declared "
            "in this wave]} and nothing else (no SQL in the manifest); the parent-owned step runs "
            "the command once after the children report and before the verifier")
        ghosts = [u for u in rs["units"] if not isinstance(u, str) or u not in owners]
        req(not ghosts, f"manifest 'resync.units' {ghosts!r} are not units of this wave's batches")
    caps = m.get("capabilities")
    req(isinstance(caps, dict) and isinstance(caps.get("identity"), str) and caps["identity"],
        "manifest 'capabilities' must be an object with a non-empty 'identity' "
        "(the migration principal's userName from capabilities.json); no wave "
        "launches without the factory-doctor contract the children compare against")
    req(isinstance(caps.get("catalogs"), list) and caps["catalogs"]
        and all(isinstance(c, str) and c for c in caps["catalogs"]),
        "manifest 'capabilities.catalogs' must be the non-empty allowlist of catalog names")
    req(caps.get("guard_mode") in GUARD_MODES, f"manifest 'capabilities.guard_mode' must be one of {GUARD_MODES}")
    req(caps.get("ready") is True,
        "manifest 'capabilities.ready' must be true: the factory-doctor preflight "
        "did not pass; fix the D10 and re-run the doctor before launching a wave")
    req(isinstance(m.get("auto_merge", False), bool), "manifest 'auto_merge' must be a boolean")


def check_doctor_contract(m, doctor):
    """The manifest's capabilities are the doctor's, copied: a wave launches only from a signed
    preflight that saw the same identity, host, catalogs, guard mode and source."""
    def req(cond, msg):
        if not cond:
            raise SystemExit(msg)
    caps = m["capabilities"]
    req(doctor.get("ready") is True,
        f"the factory-doctor is not ready now ({doctor.get('blocking')}): fix the D10 before a wave")
    ident = doctor.get("identity")
    rows = {c.get("id"): c.get("data") or {} for c in doctor.get("checks", []) if isinstance(c, dict)}
    req(isinstance(ident, dict) and ident.get("userName") and ident.get("host"),
        "capabilities.json records no verified identity and host; a wave launches only from a "
        "doctor report that saw the migration principal")
    recorded = {"identity": ident["userName"], "host": ident["host"],
                "catalogs": sorted(rows.get("allowed_targets", {}).get("catalogs") or []),
                "guard_mode": rows.get("allowed_targets", {}).get("guard_mode")}
    for key, want in recorded.items():
        got = sorted(c.strip().strip("`").lower() for c in caps["catalogs"]) if key == "catalogs" else caps.get(key)
        req(got == want, f"manifest 'capabilities.{key}' is {got!r} but the doctor recorded {want!r} in "
            "capabilities.json; copy the doctor's values, never edit them")
    req(doctor.get("source") == m.get("source"),
        "manifest 'source' differs from the source the doctor was signed for; "
        "re-run the doctor with --wave on this manifest")


def wave_signature(body, manifest_bytes):
    ident = body.get("identity") or {}
    key = hashlib.sha256(manifest_bytes + str(ident.get("userName") or "").encode()
                         + str(ident.get("host") or "").encode()).digest()
    message = json.dumps({k: v for k, v in body.items() if k != "signature"},
                         sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(key, message, "sha256").hexdigest()


def evidence_path(evidence):
    """The path a reported gate's evidence names: the string itself, or the `path` of
    {path, label?, verdict?, rows?}. None when it is neither."""
    if isinstance(evidence, str):
        return evidence
    if (isinstance(evidence, dict) and isinstance(evidence.get("path"), str)
            and set(evidence) - {"path"} <= set(EVIDENCE_META)
            and all(isinstance(evidence[k], t) and not isinstance(evidence[k], bool)
                    for k, t in EVIDENCE_META.items() if k in evidence)):
        return evidence["path"]
    return None


def check_wave_tag(tag, manifest):
    wave = manifest["wave"]
    last = tag.rsplit("-", 1)[-1]
    if not last.isdigit() or int(last) != wave:
        raise SystemExit(f"wave-{tag}.json: the wave number in the file name must equal the manifest's 'wave' ({wave})")
    pipeline = tag.rsplit("-", 1)[0]
    count = manifest.get("pipelines", {}).get(pipeline) if pipeline != tag else None
    if pipeline != tag and (not isinstance(count, int) or isinstance(count, bool) or count < int(last)):
        raise SystemExit(f"wave-{tag}.json: wave-<pipeline>-<N>.json manifests must list every sibling pipeline in "
                         f"'pipelines' (including {pipeline}): the planning barrier reads it")


def _is_manifest(name):
    return (name.startswith("wave-") and name.endswith(".json")
            and TAG_RE.fullmatch(name[len("wave-"):-len(".json")]) is not None)


def check_pipeline_updates(script, manifest_path):
    if not Path(script).is_file():
        raise SystemExit(f"{script} is missing: the pointer's `plugin` must name the plugin root so the wave can run "
                         "skills/target-routing/pipeline_updates.py before launching")
    try:
        proc = subprocess.run([sys.executable, str(script), str(manifest_path)],
                              capture_output=True, text=True, timeout=300)
    except (OSError, subprocess.SubprocessError) as e:
        raise SystemExit(f"pipeline_updates.py did not run ({e}); the wave does not launch unchecked")
    try:
        result = json.loads(proc.stdout)
        order = result["order"]
        assert isinstance(order, dict) and all(isinstance(v, list) for v in order.values())
    except (ValueError, KeyError, TypeError, AssertionError):
        result, order = {}, None
    if proc.returncode != 0:
        why = [f"pipeline {s.get('pipeline')!r} is shared by {', '.join(map(str, s.get('batches', [])))} without a "
               f"pipeline_serialized decision" for s in result.get("shared", []) if s.get("serialized") is False]
        if result.get("unchecked_batches"):
            why.append(f"unsupported: {', '.join(map(str, result['unchecked_batches']))} declare no lakeflow_pipelines")
        raise SystemExit(f"pipeline_updates.py exit {proc.returncode}, the wave does not launch: "
                         + ("; ".join(why) or f"{proc.stdout.strip()} {proc.stderr.strip()}".strip()))
    if order is None:
        raise SystemExit(f"pipeline_updates.py printed no order: {proc.stdout.strip()[:500]}")
    return order


def other_wave_manifests(waves_dir, current):
    out = {}
    for p in sorted(waves_dir.glob("wave-*.json")):
        if p.name == current or not _is_manifest(p.name):
            continue
        tail = "every wave manifest is read for cross-wave write-target collisions, so fix it, then re-run"
        try:
            m = json.loads(p.read_text())
        except ValueError as e:
            raise SystemExit(f"{p} is not valid JSON ({e}); {tail}") from None
        batches = m.get("batches") if isinstance(m, dict) else None
        if not isinstance(batches, list) or not all(
                isinstance(b, dict) and isinstance(b.get("id"), str)
                and isinstance(b.get("units"), list) and all(isinstance(u, str) for u in b["units"])
                and isinstance(b.get("write_targets"), list) and all(isinstance(t, str) for t in b["write_targets"])
                for b in batches):
            raise SystemExit(f"{p} has no 'batches' list of {{id, units, write_targets}} rows; {tail}")
        namespace = m.get("target_namespace", "")
        if "target_namespace" in m and not valid_namespace(namespace):
            raise SystemExit(f"{p} 'target_namespace' must be the dotted catalog.schema (or schema) its harness run "
                             f"is given; {tail}")
        out[p.name] = {"target_namespace": namespace, "batches": batches}
    return out


_SEGMENT = r'(?:[A-Za-z_][\w$]*|\[[^\]]+\]|"(?:[^"]|"")+"|`[^`]+`)'


PREDICATE_TOKEN = re.compile(
    r"\s+|(?P<string>'(?:[^']|'')*')|(?P<param>\$\{\w+\})|(?P<number>-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)"
    rf"|(?P<word>{_SEGMENT}(?:\.{_SEGMENT})*)|(?P<punct><>|!=|<=|>=|[=<>(),])")


PREDICATE_WORDS = {"and", "or", "not", "in", "between", "is", "null", "like", "true", "false"}


def column_key(name):
    return re.sub(r'^[`"\[]|[`"\]]$', "", str(name).strip().split(".")[-1].strip()).casefold()


def predicate_slices(where, scope):
    if not isinstance(where, str):
        return None
    scope = {column_key(c) for c in scope}
    tokens, pos = [], 0
    while pos < len(where):
        m = PREDICATE_TOKEN.match(where, pos)
        if not m:
            return None
        if m.lastgroup:
            tokens.append((m.lastgroup, m.group()))
        pos = m.end()

    def keyword(i, *words):
        return i < len(tokens) and tokens[i][0] == "word" and tokens[i][1].lower() in words

    def value(kind, text):
        if kind == "number":
            return "n", float(text)
        if kind == "param":
            return "p", text[2:-1]
        text = text[1:-1].replace("''", "'")
        return ("p", text[2:-1]) if re.fullmatch(r"\$\{\w+\}", text) else ("s", text)

    def pins(toks):
        words = [t.lower() for k, t in toks if k == "word"]
        columns = [i for i, (k, t) in enumerate(toks) if k == "word" and t.lower() not in PREDICATE_WORDS
                   and not (t.lower() in ("date", "timestamp") and i + 1 < len(toks) and toks[i + 1][0] == "string")]
        values = [(i, value(k, t)) for i, (k, t) in enumerate(toks) if k in ("string", "number", "param")]
        ops = [t for k, t in toks if k == "punct" and t in ("=", "<", "<=", ">", ">=")]
        wildcard = "like" in words and all(re.fullmatch(r"'[%_]*'", t) for k, t in toks if k == "string")
        if not (len(columns) == 1 and column_key(toks[columns[0]][1]) in scope and values and (ops or any(
                w in ("in", "like", "between") for w in words)) and not wildcard
                and not any(w in ("is", "not") for w in words)
                and not any(k == "punct" and t in ("<>", "!=") for k, t in toks)):
            return None
        col = column_key(toks[columns[0]][1])
        if "like" in words:
            return col, ("like",)
        if "in" in words or ops == ["="]:
            return col, ("eq", frozenset(v for _, v in values))
        if "between" in words and len(values) == 2 and not ops:
            return col, ("range", (values[0][1], True), (values[1][1], True))
        if len(ops) == 1 and len(values) == 1:
            op, (at, v) = ops[0], values[0]
            if at < columns[0]:
                op = {"<": ">", "<=": ">=", ">": "<", ">=": "<="}[op]
            return col, (("range", None, (v, op == "<=")) if op in ("<", "<=") else ("range", (v, op == ">="), None))
        return col, ("like",)

    def both(a, b):
        return [{c: x.get(c, []) + y.get(c, []) for c in {*x, *y}} for x in a for y in b]

    def expr(i):
        boxes, i = term(i)
        while keyword(i, "or"):
            b, i = term(i + 1)
            boxes = None if boxes is None or b is None else boxes + b
        return boxes, i

    def term(i):
        boxes, i = factor(i)
        while keyword(i, "and"):
            b, i = factor(i + 1)
            boxes = b if boxes is None else boxes if b is None else both(boxes, b)
        return boxes, i

    def factor(i):
        if keyword(i, "not"):
            return None, factor(i + 1)[1]
        if i < len(tokens) and tokens[i] == ("punct", "("):
            boxes, i = expr(i + 1)
            if i >= len(tokens) or tokens[i] != ("punct", ")"):
                raise ValueError
            return boxes, i + 1
        start, depth, between = i, 0, False
        while (i < len(tokens)
               and not (depth == 0 and not between
                        and (keyword(i, "and", "or") or tokens[i] == ("punct", ")")))):
            depth += (tokens[i] == ("punct", "(")) - (tokens[i] == ("punct", ")"))
            if depth < 0:
                raise ValueError
            between = (between and not keyword(i, "and")) or keyword(i, "between")
            i += 1
        atom = tokens[start:i]
        if not atom or depth:
            raise ValueError
        pin = pins(atom)
        return (None if pin is None else [{pin[0]: [pin[1]]}]), i

    try:
        boxes, end = expr(0)
    except ValueError:
        return None
    return boxes if end == len(tokens) else None


def bounded_predicate(where, scope):
    """Whether a target_where bounds the rows recon reads to the unit's own slice (predicate_slices)."""
    return predicate_slices(where, scope) is not None


def disjoint_slices(a, b):
    """Whether two readers' slices can never select the same row: every box of one against every box of."""
    def literal(v, kind):
        return v[0] == kind and kind != "p"

    def below(hi, lo):
        return (hi is not None and lo is not None and hi[0][0] == lo[0][0] and literal(hi[0], hi[0][0])
                and (hi[0][1] < lo[0][1] or (hi[0][1] == lo[0][1] and not (hi[1] and lo[1]))))
    def apart(x, y):
        if x[0] == "like" or y[0] == "like":
            return False
        if x[0] == "eq" and y[0] == "eq":
            params = {v for v in x[1] | y[1] if v[0] == "p"}
            return not (x[1] & y[1]) and (not params or params == x[1] | y[1])
        if x[0] == "range" and y[0] == "range":
            return below(x[2], y[1]) or below(y[2], x[1])
        eq, rng = (x, y) if x[0] == "eq" else (y, x)
        return all(below((v, True), rng[1]) or below(rng[2], (v, True)) for v in eq[1])

    return all(any(apart(x, y) for c in set(p) & set(q) for x in p[c] for y in q[c]) for p in a for q in b)


def bounded_readers(spec, table, namespace=""):
    objects = spec.get("objects", spec.get("tables")) if isinstance(spec, dict) else None
    if not isinstance(objects, list) or not all(isinstance(o, dict) for o in objects):
        raise SystemExit("mapping spec 'objects' must be a list of object rows")
    mine = [o for o in objects if reads_target(o.get("object") or o.get("target_table") or "", table, namespace)]
    if not mine:
        return None

    def columns(row, inherited=None):
        scope = row.get("scope_columns", inherited)
        if not (isinstance(scope, list) and scope and all(isinstance(c, str) and c.strip() for c in scope)):
            return None
        return scope

    for o in mine:
        scope = columns(o)
        if scope is None:
            return "declares no scope_columns (non-empty list of the table's partition or run-date columns)"
        if not bounded_predicate(o.get("target_where"), scope):
            return "reads it without a target_where pinning one of its scope_columns"
        embeds = o.get("embeds", [])
        if not isinstance(embeds, list) or not all(isinstance(e, dict) for e in embeds):
            return "has 'embeds' that is not a list of embed rows"
        for e in embeds:
            escope = columns(e, scope)
            if escope is None or not bounded_predicate(e.get("target_where"), escope):
                return (f"embed '{e.get('array_path')}' reads it without its own target_where pinning one of its "
                        "scope_columns (or the object's)")
    return ""


def reader_slices(spec, table, namespace=""):
    objects = spec.get("objects", spec.get("tables"))
    mine = [o for o in objects if reads_target(o.get("object") or o.get("target_table") or "", table, namespace)]
    if not mine:
        return None
    return [box for o in mine for row in (o, *o.get("embeds", []))
            for box in predicate_slices(row.get("target_where"), row.get("scope_columns", o.get("scope_columns", [])))]


def transitive_writes(routines, resolve=None):
    resolve = resolve or (lambda r, t: {target_key(t)})
    by_name = {r["routine"].casefold(): r for r in routines}
    seen, writes = set(), set()
    todo = list(by_name)
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        seen.add(name)
        r = by_name[name]
        for t in r["writes"]:
            writes |= resolve(r, t)
        for callee in r["calls"]:
            if callee.casefold() not in by_name:
                raise SystemExit(f"routine {r['routine']} calls {callee}, which the dependency analysis does "
                                 "not cover, so its writes are unknown")
            todo.append(callee.casefold())
    return writes


def mapped_target(spec, table, namespace=""):
    k = target_key(table)
    objects = (spec.get("objects") or spec.get("tables") or []) if isinstance(spec, dict) else []
    return {target_key(o.get("object") or o.get("target_table") or "", namespace)
            for o in (objects if isinstance(objects, list) else [])
            if isinstance(o, dict) and target_key(o.get("root_table") or o.get("source_table") or "") == k}


def check_repo_origin(repo, origin_url):
    """The manifest's repo is where the children's PR URLs must live; when origin is a remote URL it
    has to be that repo, or every PR check fails after the child, not before it."""
    url = origin_url.strip()
    if "://" in url:
        parts = urllib.parse.urlsplit(url)
        host, path = parts.hostname, parts.path      # the port, if any, is not part of the repo
    else:
        m = re.fullmatch(r"(?:[^@/:]+@)?(?P<host>[^/:]+):(?P<path>[^/].*)", url)   # scp-style git@host:owner/name
        host, path = (m["host"], m["path"]) if m else (None, "")
    if not host:
        return  # a local mirror or an unparsed remote: nothing to compare
    path = path.strip("/")
    if path.endswith(".git"):
        path = path[:-4]
    want = repo.lower()
    if f"{host.lower()}/{path.lower()}" != want:
        raise SystemExit(f"manifest 'repo' is {repo!r} but origin is {origin_url.strip()!r}: children open PRs "
                         "on origin, so the manifest must name that host/owner/name")
