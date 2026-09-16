"""Migration fan-out workflow: run one wave of unit-migration children, then one
independent verifier, and write the wave result the orchestrator gates on. Run with the
`run_workflow` tool; the orchestrator writes `.migration/waves/current.json` at or above
the sandbox cwd. Guarantees: no two batches share a write target, at most `width`
children run at once, a circuit breaker stops new launches after `breaker_threshold`
failures of one class, this script is the single writer of the result and ledger rows,
only verifier-PASS PRs merge (and only with auto_merge or a recorded human merge_override
decision), and a resume with the same run_id replays finished children. Wave 0 runs with
`"wave": 0`, `"width": 1`. A hand-gathered wave spends its STOP C row with
`python3 workflow.py reserve` and closes it with `python3 workflow.py gates <results.json>`.
"""
import asyncio, datetime, fcntl, hashlib, hmac, json, re, shlex, subprocess, sys, tempfile
from collections import Counter
from pathlib import Path
POINTER_REL = Path(".migration/waves/current.json"); MODES = ("start", "resume", "rerun", "smoke")
HOOK_PROBE = re.compile(r"blocked:[0-9a-f]{8}|not-blocked|unknown"); TAG_RE = re.compile(r"[A-Za-z0-9_-]+")
PIPELINE_RE = re.compile(r"[A-Za-z0-9_]*[A-Za-z_][A-Za-z0-9_]*"); DOCTOR_MAX_AGE = datetime.timedelta(minutes=15)

def find_pointer(start):
    """The sandbox gives this script one thing: its cwd (the session's home directory, not the workspace)."""
    for d in (start, *start.parents):
        if (d / POINTER_REL).is_file(): return d / POINTER_REL
    raise SystemExit(f"no {POINTER_REL} at or above {start}; write the orchestrator pointer, then re-run")
POINTER_PATH = find_pointer(Path.cwd().resolve())
try: POINTER = json.loads(POINTER_PATH.read_text())
except ValueError as e: raise SystemExit(f"{POINTER_PATH} is not valid JSON: {e}") from None
if not isinstance(POINTER, dict) or POINTER.get("mode") not in MODES or not isinstance(POINTER.get("manifest"), str): raise SystemExit(f"{POINTER_PATH} must be {{manifest: 'wave-N.json', mode: start|resume|rerun|smoke, run_id, hook_probe, workspace?}}")
MODE = POINTER["mode"]; RUN_ID = POINTER.get("run_id")
if RUN_ID is not None and (not isinstance(RUN_ID, str) or not RUN_ID.strip()): raise SystemExit(f"{POINTER_PATH} run_id must be the run_workflow run_id string or null")
if RUN_ID is not None and MODE != "resume": raise SystemExit(f"{POINTER_PATH} run_id must be null unless mode is resume: run_workflow reports the run_id only once a fresh run starts; " "record it in <manifest>.run_id afterwards")
HOOK_PROBE_RESULT = POINTER.get("hook_probe")
if not isinstance(HOOK_PROBE_RESULT, str) or not HOOK_PROBE.fullmatch(HOOK_PROBE_RESULT): raise SystemExit(f"{POINTER_PATH} hook_probe must be blocked:<nonce>, not-blocked or unknown (the probe run in the " "orchestrator's shell; the doctor was given the same value)")
ROOT = Path(POINTER["workspace"]).resolve() if isinstance(POINTER.get("workspace"), str) else POINTER_PATH.parents[2]
PLUGIN = Path(POINTER["plugin"]).resolve() if isinstance(POINTER.get("plugin"), str) else None
PIPELINE_UPDATES = (PLUGIN or ROOT) / "skills" / "target-routing" / "pipeline_updates.py"
WAVES_DIR = ROOT / ".migration" / "waves"; MANIFEST_PATH = (WAVES_DIR / POINTER["manifest"]).resolve()
if not (MANIFEST_PATH.name.startswith("wave-") and TAG_RE.fullmatch(MANIFEST_PATH.stem[len("wave-"):] or "")): raise SystemExit(f"{POINTER_PATH} manifest must be named wave-<N>.json or wave-<pipeline>-<N>.json " "so every sibling wave and pipeline sees it in the collision check")
if MANIFEST_PATH.suffix != ".json" or MANIFEST_PATH.parent != WAVES_DIR.resolve() or MANIFEST_PATH.name.endswith((".result.json", ".doctor.json")): raise SystemExit(f"{POINTER_PATH} manifest must be the plain file name of a wave manifest inside {WAVES_DIR}")
if not MANIFEST_PATH.exists(): raise SystemExit(f"no wave manifest at {MANIFEST_PATH}; the plan playbook writes it, then re-run")
TAG = MANIFEST_PATH.stem[len("wave-"):]; MANIFEST_BYTES = MANIFEST_PATH.read_bytes(); MANIFEST = json.loads(MANIFEST_BYTES)
BASE_BRANCH = MANIFEST.get("base_branch", ""); MANIFEST_SHA = hashlib.sha256(MANIFEST_BYTES).hexdigest()[:12]
RESULT_PATH = MANIFEST_PATH.with_suffix(".result.json"); MERGES_PATH = MANIFEST_PATH.with_suffix(".merges.json")
RUNS_PATH = MANIFEST_PATH.with_suffix(".runs.jsonl"); BRIEF_PATH = MANIFEST_PATH.with_suffix(".brief.md")
RUN_ID_PATH = MANIFEST_PATH.with_suffix(".run_id"); BASE_SHA_PATH = MANIFEST_PATH.with_suffix(".base_sha")
DOCTOR_PATH = MANIFEST_PATH.with_suffix(".doctor.json"); DECISIONS_PATH = ROOT / ".migration" / "06_decisions.md"
resume = MODE == "resume"; SMOKE = MODE == "smoke"
if SMOKE and not (MANIFEST.get("smoke") is True and MANIFEST.get("wave") == 0 and MANIFEST.get("width") == 1): raise SystemExit("mode smoke exercises the runner only: it needs a manifest with smoke: true, wave: 0, width: 1")
if not SMOKE and MANIFEST.get("smoke") is True: raise SystemExit("a smoke manifest never runs a real wave")
prior = None
if RESULT_PATH.exists() and MODE != "rerun":
    try:
        prior = json.loads(RESULT_PATH.read_text())
        if not isinstance(prior, dict): raise ValueError("result is not a JSON object")
    except ValueError:
        if not resume: raise SystemExit(f"{RESULT_PATH} is not valid JSON (interrupted write?). Inspect it; to resume " "the same run set mode: resume with the recorded run_id, or set mode: rerun " "to redo the wave.") from None
    else:
        if prior.get("closed"): raise SystemExit(f"{RESULT_PATH} says wave {prior.get('wave')} closed clean. To redo it on " "purpose, set mode: rerun.")
        if not resume: raise SystemExit(f"{RESULT_PATH} records a halted or failed run. To continue it, set mode: resume " "with the recorded run_id (finished children replay). To redo the wave from " "scratch, set mode: rerun.")
if resume:
    if not RUN_ID: raise SystemExit("mode resume requires run_id in current.json; pass the recorded run_id")
    if not RUN_ID_PATH.exists(): raise SystemExit(f"no run record at {RUN_ID_PATH}; cannot verify the pointer run_id belongs to this wave " "— set mode: rerun for a fresh run")
    if RUN_ID_PATH.read_text().strip() != RUN_ID: raise SystemExit(f"run_id does not match {RUN_ID_PATH}; pass the recorded run_id in current.json, " "or set mode: rerun for a fresh run")
    if isinstance(prior, dict) and prior.get("run_id") and prior["run_id"] != RUN_ID: raise SystemExit(f"run_id does not match prior result at {RESULT_PATH}; pass the recorded run_id in " "current.json, or set mode: rerun for a fresh run")
REPLAYED = {
    b["id"]: b for b in (prior or {}).get("batches", [])
    if b.get("status") in ("PASS", "FAIL", "BLOCKED")
} if resume and isinstance(prior, dict) else {}

def prompt_sha(prompt):
    return hashlib.sha256(prompt.encode()).hexdigest()[:16]

def _tmp_write(path, suffix, text):
    tmp = path.with_suffix(suffix); tmp.write_text(text); tmp.replace(path)
VERIFY_DEPTHS = ("sampled", "full"); MERGE_EVIDENCE_MODES = ("live", "snapshot", "transactional"); GUARD_MODES = ("block", "warn")
STOP_MODES = ("hard", "soft"); GATE_KINDS = ("byte_compare", "export_file", "publish_leg", "row_parity", "structural", "custom")
GATE_STATUSES = ("pending", "passed", "failed", "waived"); UNIT_ID = re.compile(r"(?!wave-)[A-Za-z0-9_][A-Za-z0-9_.-]*")
WORD = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_./-]*"); ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
PARAM_VALUE = re.compile(r"[A-Za-z0-9_\-:.T/]+(?: [0-9:.]+)?")
PR_URL = re.compile(r"https://(?P<repo>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/pull/(?P<n>[0-9]+)/?")
DECISION_ID = re.compile(r"D-[0-9]+"); HUMAN_PROVENANCE = re.compile(r"(?<![\w-])user:[\w][\w.@/-]*")
DEFAULT_ACCEPTED = re.compile(r"default-accepted(?: ?\([^|]*\))?")
LEDGER_METADATA = re.compile(rf"D-[0-9]+|\d{{4}}-\d{{2}}-\d{{2}}(?:[T ][\d:.]+Z?(?:[+-]\d{{2}}:?\d{{2}})?)?" rf"|{HUMAN_PROVENANCE.pattern}|{DEFAULT_ACCEPTED.pattern}")

def decision_ledger():
    try: return DECISIONS_PATH.read_text()
    except OSError: return ""

def ledger_rows(ledger):
    """Each markdown table row of the ledger as (decision id, its cells): the id is the first cell that is."""
    for line in ledger.splitlines():
        line = line.strip()
        if "|" not in line: continue
        cells = [" ".join(c.split()) for c in line.strip("|").split("|")]
        ids = [c for c in cells if DECISION_ID.fullmatch(c)]
        if ids: yield ids[0], cells

def override_decision(decision_id, units, ledger, word="merge_override"):
    """Whether the ledger holds the D-<n> row a human wrote for these units: the row whose id cell is that."""
    if not isinstance(decision_id, str) or not DECISION_ID.fullmatch(decision_id): return False
    def token(w):
        return rf"(?<![A-Za-z0-9_.-]){re.escape(w)}(?![A-Za-z0-9_.-])"
    for row_id, cells in ledger_rows(ledger):
        if (row_id == decision_id and any(HUMAN_PROVENANCE.fullmatch(c) for c in cells) and not any(DEFAULT_ACCEPTED.fullmatch(c) for c in cells)):
            text = " | ".join(HUMAN_PROVENANCE.sub(" ", c) for c in cells if not LEDGER_METADATA.fullmatch(c))
            need = Counter((word, *units))
            if all(len(re.findall(token(w), text)) >= n for w, n in need.items()): return True
    return False

def rows_after(ledger, stop_c):
    """The ledger lines below this run's STOP C row."""
    lines = ledger.splitlines()
    for n, line in enumerate(lines):
        if any(row_id == stop_c for row_id, _ in ledger_rows(line)): return lines[n + 1:]
    return []

def ledger_waiver(gate_id, units, ledger, stop_c):
    """The D-<n> of the human row, written after this run's STOP C row, that waives this gate for every unit."""
    for line in rows_after(ledger, stop_c):
        for decision_id in dict.fromkeys(DECISION_ID.findall(line)):
            if override_decision(decision_id, [gate_id, *units], line, word="waive"): return decision_id
    return None

def declared_gates_sha(wave, batches, degraded=False):
    """What STOP C approved, whole: the wave, each batch's units and every gate row as declared (id, kind,."""
    declared = {"wave": wave, "batches": { b["id"]: {"units": sorted(b["units"]), "gates": [[g["id"], g["kind"], g["status"], g["evidence"], g.get("decision_id")] for g in b["gates"]]} for b in batches}}
    if degraded: declared["degraded"] = True
    return hashlib.sha256(json.dumps(declared, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

def gates_approved(decision_id, wave, sha, ledger, stop_mode="hard"):
    """Whether the ledger row the manifest names (stop_c) is the STOP C approval of exactly this wave's."""
    if not (isinstance(decision_id, str) and DECISION_ID.fullmatch(decision_id) and isinstance(wave, int) and not isinstance(wave, bool) and isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{64}", sha)): return False
    approval = re.compile(rf"stop c wave-{wave} gates_sha {sha}", re.I)
    def provenance(c):
        return HUMAN_PROVENANCE.fullmatch(c) or (stop_mode == "soft" and DEFAULT_ACCEPTED.fullmatch(c))
    for line in ledger.splitlines():
        line = line.strip()
        if not line.startswith("|"): continue
        cells = [" ".join(c.split()) for c in line.strip("|").split("|")]
        if decision_id in cells and any(provenance(c) for c in cells) and any(approval.fullmatch(c) for c in cells): return True
    return False

def validate_gates(b):
    """A unit's acceptance gates are manifest rows STOP C approved, not prose in a PR; every field is checked."""
    gates = b.get("gates")
    if not isinstance(gates, list) or not gates or not all(isinstance(g, dict) for g in gates): raise SystemExit(f"batch {b['id']} 'gates' must be a non-empty list of {{id, kind, status, evidence, decision_id?}} " "rows: the acceptance gates STOP C approved for its units")
    ids = Counter(g.get("id") for g in gates)
    if any(not isinstance(i, str) or not UNIT_ID.fullmatch(i) for i in ids): raise SystemExit(f"batch {b['id']} gate 'id' must be a plain word (letters, digits, _ . -)")
    if any(c > 1 for c in ids.values()): raise SystemExit(f"batch {b['id']} gate ids must be unique: {[i for i, c in ids.items() if c > 1]}")
    for g in gates:
        if g.get("kind") not in GATE_KINDS: raise SystemExit(f"batch {b['id']} gate {g['id']} 'kind' must be one of {GATE_KINDS}")
        if g.get("status") not in GATE_STATUSES: raise SystemExit(f"batch {b['id']} gate {g['id']} 'status' must be one of {GATE_STATUSES}")
        if not isinstance(g.get("evidence"), str) or (g["status"] == "passed" and not g["evidence"]): raise SystemExit(f"batch {b['id']} gate {g['id']} 'evidence' must be a string, non-empty once passed")
        decision = g.get("decision_id")
        if (g["status"] == "waived" and decision is None) or ( decision is not None and not (isinstance(decision, str) and DECISION_ID.fullmatch(decision))): raise SystemExit(f"batch {b['id']} gate {g['id']} 'decision_id' must be the D-<n> row of 06_decisions.md " "(required for a waived gate)")

def target_key(name, namespace=""):
    """The one identity of a table however a manifest or mapping spells it: trimmed, unquoted, case-folded."""
    def segments(s):
        return [re.sub(r'^[`"\[]|[`"\]]$', "", p.strip()).casefold() for p in str(s).strip().split(".")] \
            if str(s).strip() else []
    parts, prefix = segments(name), segments(namespace)
    if parts and all(parts) and len(parts) <= len(prefix): parts = prefix[:len(prefix) + 1 - len(parts)] + parts
    return ".".join(parts)

def valid_namespace(value):
    return isinstance(value, str) and all(re.fullmatch(r"[a-z_][\w$]*", s) for s in target_key(value).split("."))

def reads_target(obj, table, namespace=""):
    """A mapping object reads the target when both resolve to the same identity; with no namespace to."""
    o, t = target_key(obj, namespace), target_key(table, namespace)
    if not o: return False
    if not namespace and ("." not in o or "." not in t): return o.rsplit(".", 1)[-1] == t.rsplit(".", 1)[-1]
    return o == t

def validate_manifest(m, doctor=None):
    """Fail here, in one line, instead of 20 children failing on a missing field."""
    for key in ("wave", "repo", "child_macro", "verify_macro", "batches", "base_branch"):
        if key not in m: raise SystemExit(f"manifest is missing '{key}'")
    if not m["batches"]: raise SystemExit("manifest has no batches")
    if (isinstance(m["wave"], bool) or not isinstance(m["wave"], int) or m["wave"] < 0): raise SystemExit("manifest key 'wave' must be a non-negative integer")
    for key in ("width", "breaker_threshold"):
        if key in m and (isinstance(m[key], bool) or not isinstance(m[key], int) or m[key] <= 0): raise SystemExit(f"manifest key '{key}' must be a positive integer")
    def _bad_int(key, hi):
        return key in m and (isinstance(m[key], bool) or not isinstance(m[key], int) or not 0 < m[key] <= hi)
    def _bad_secrets(v):
        return not isinstance(v, list) or not all(isinstance(s, str) for s in v)
    for key in ("max_minutes", "close_minutes"):
        if _bad_int(key, 60): raise SystemExit(f"manifest key '{key}' must be a positive integer of at most 60 minutes")
    if _bad_int("doctor_max_age", 1440): raise SystemExit("manifest key 'doctor_max_age' must be a positive integer of at most 1440 minutes")
    if "degraded" in m and not isinstance(m["degraded"], bool): raise SystemExit("manifest key 'degraded' must be a boolean")
    if "secrets" in m and _bad_secrets(m["secrets"]): raise SystemExit("wave manifest 'secrets' (top level or per batch) must be a list of scope/key strings")
    pipelines = m.get("pipelines")
    if "pipelines" in m and (not isinstance(pipelines, dict) or not pipelines or not all(isinstance(p, str) and PIPELINE_RE.fullmatch(p) for p in pipelines) or not all(isinstance(n, int) and not isinstance(n, bool) and n > 0 for n in pipelines.values())): raise SystemExit("manifest key 'pipelines' must map each pipeline the plan split (letters, digits, '_') to a " "positive wave count, the <pipeline>-<N> of its wave-<pipeline>-<N>.json manifests")
    if m["wave"] == 0 and m.get("width", 20) != 1: raise SystemExit("wave 0 is the serial shared-objects wave: set width to 1")
    if not (isinstance(m["base_branch"], str) and WORD.fullmatch(m["base_branch"]) and ".." not in m["base_branch"]): raise SystemExit("manifest 'base_branch' must be a plain branch name (letters, digits, _ . / -)")
    if "target_namespace" in m and not valid_namespace(m["target_namespace"]): raise SystemExit("manifest 'target_namespace' must be the dotted catalog.schema (or schema) the harness run is " "given, so a bare write target or mapping object is that table and no other")
    if m["base_branch"] in ("main", "master") and not ( isinstance(m.get("trunk_base_decision"), str) and m["trunk_base_decision"].strip()): raise SystemExit("base_branch 'main' is the trunk: migration ledgers and unit PRs land on the engagement " "feature branch; set base_branch to it, or record the decision in 06_decisions.md and put " "its row reference in 'trunk_base_decision'")
    ids = Counter(b.get("id") for b in m["batches"])
    dupes = [i for i, c in ids.items() if c > 1 or not i]
    if dupes: raise SystemExit(f"batch ids must be unique and non-empty: {dupes}")
    owners = {}
    for b in m["batches"]:
        for key in ("units", "brief"):
            if not b.get(key): raise SystemExit(f"batch {b['id']} is missing '{key}' (a child with no brief cannot be launched safely)")
        if not isinstance(b.get("write_targets"), list) or not all(isinstance(t, str) and t.strip() for t in b["write_targets"]): raise SystemExit(f"batch {b['id']} needs 'write_targets', a list of table names (empty only for a batch " "whose dependency analysis writes nothing)")
        ns = m.get("target_namespace", "") if isinstance(m.get("target_namespace", ""), str) else ""
        deploy = b.get("deploy_objects", [])
        if not isinstance(deploy, list) or not all(isinstance(t, str) and t.strip() for t in deploy) \
                or len({target_key(t, ns) for t in deploy}) != len(deploy):
            raise SystemExit(f"batch {b['id']} 'deploy_objects' must be a list of distinct names: the procedures, views " "and jobs the batch deploys, which no routine's DML writes")
        declared = {target_key(t, ns) for t in b["write_targets"]}
        outside = sorted(t for t in deploy if target_key(t, ns) not in declared)
        if outside: raise SystemExit(f"batch {b['id']} 'deploy_objects' {outside} are not in its write_targets; every object a " "child deploys is a write target (it collides like any other)")
        if "verify_depth" in b and b["verify_depth"] not in VERIFY_DEPTHS: raise SystemExit(f"batch {b['id']} 'verify_depth' must be one of {VERIFY_DEPTHS}")
        if "max_minutes" in b and (isinstance(b["max_minutes"], bool) or not isinstance(b["max_minutes"], int) or not 0 < b["max_minutes"] <= 60): raise SystemExit(f"batch {b['id']} max_minutes must be a positive integer of at most 60 minutes")
        if "secrets" in b and _bad_secrets(b["secrets"]): raise SystemExit("wave manifest 'secrets' (top level or per batch) must be a list of scope/key strings")
        bad = [u for u in b["units"] if not isinstance(u, str) or not UNIT_ID.fullmatch(u)]
        if bad: raise SystemExit(f"batch {b['id']} unit id(s) {bad!r} are not a plain directory name (letters, digits, " "_ . -, not wave-*): the id names the only .migration/recon/<unit_id>/ its child may write")
        for u in b["units"]: owners.setdefault(u, []).append(b["id"])
        validate_gates(b)
    shared = {u: bs for u, bs in owners.items() if len(bs) > 1}
    if shared: raise SystemExit(f"a unit id belongs to one batch (its child alone writes .migration/recon/<unit_id>/): {shared}")
    if not (isinstance(m.get("stop_c"), str) and DECISION_ID.fullmatch(m["stop_c"])): raise SystemExit("manifest 'stop_c' must be the D-<n> row of 06_decisions.md that resolved STOP C for this wave " "(the row that records its gates_sha)")
    want = declared_gates_sha(m["wave"], m["batches"], m.get("degraded") is True)
    if m.get("gates_sha") != want: raise SystemExit(f"manifest 'gates_sha' is {m.get('gates_sha')!r} but the declared gate list hashes to {want}: " "record that value at STOP C with the approved gates; a gate renamed, added, dropped, swapped " "for another kind or given another status or evidence since, or the wave declared DEGRADED " "since, is a plan change, not a child's call, so this run halts")
    src = m.get("source")
    if src is not None and (not isinstance(src, dict) or not isinstance(src.get("params", {}), dict) or not all(isinstance(v, str) and WORD.fullmatch(v) for v in (src.get("family"), *src.get("params", {}).keys())) or not isinstance(src.get("secret"), str) or not ENV_NAME.fullmatch(src["secret"]) or not all(isinstance(v, str) and PARAM_VALUE.fullmatch(v) for v in src.get("params", {}).values())): raise SystemExit("manifest 'source' must be {family, secret (env var NAME of the DSN), params?}: family and " "param names one plain word (letters, digits, _ . / -), secret a shell variable name, param " "values what dbx-recon run --param accepts; they become the doctor's command line")
    if "verify_depth" in m and m["verify_depth"] not in VERIFY_DEPTHS: raise SystemExit(f"manifest 'verify_depth' must be one of {VERIFY_DEPTHS}")
    if "cost_estimate" in m and not isinstance(m["cost_estimate"], dict): raise SystemExit("manifest 'cost_estimate' must be an object (output of `dbx-recon estimate`, summed over the wave)")
    rs = m.get("resync")
    if rs is not None:
        if (not isinstance(rs, dict) or set(rs) != {"command", "units"} or not isinstance(rs["command"], str) or not rs["command"].strip() or not isinstance(rs["units"], list) or not rs["units"]): raise SystemExit("manifest 'resync' must be {command: non-empty shell command, units: [unit ids declared " "in this wave]} and nothing else (no SQL in the manifest); the parent-owned step runs " "the command once after the children report and before the verifier")
        ghosts = [u for u in rs["units"] if not isinstance(u, str) or u not in owners]
        if ghosts: raise SystemExit(f"manifest 'resync.units' {ghosts!r} are not units of this wave's batches")
    caps = m.get("capabilities")
    if not isinstance(caps, dict) or not isinstance(caps.get("identity"), str) or not caps["identity"]: raise SystemExit("manifest 'capabilities' must be an object with a non-empty 'identity' " "(the migration principal's userName from 09_capabilities.json); no wave " "launches without the factory-doctor contract the children compare against")
    if (not isinstance(caps.get("catalogs"), list) or not caps["catalogs"] or not all(isinstance(c, str) and c for c in caps["catalogs"])): raise SystemExit("manifest 'capabilities.catalogs' must be the non-empty allowlist of catalog names")
    for key, allowed in (("guard_mode", GUARD_MODES), ("stop_mode", STOP_MODES)):
        if caps.get(key) not in allowed: raise SystemExit(f"manifest 'capabilities.{key}' must be one of {allowed}")
    if caps.get("ready") is not True: raise SystemExit("manifest 'capabilities.ready' must be true: the factory-doctor preflight " "did not pass; fix the D10 and re-run the doctor before launching a wave")
    if "auto_merge" in m and not isinstance(m["auto_merge"], bool): raise SystemExit("manifest 'auto_merge' must be a boolean")
    if caps["stop_mode"] == "hard" and m.get("auto_merge", False): raise SystemExit("manifest 'auto_merge' must be false under capabilities.stop_mode 'hard': " "merge authority stays with a human")
    if doctor is None: return
    if doctor.get("ready") is not True: raise SystemExit(f"the factory-doctor is not ready now ({doctor.get('blocking')}): fix the D10 before a wave")
    ident = doctor.get("identity")
    rows = {c.get("id"): c.get("data") or {} for c in doctor.get("checks", []) if isinstance(c, dict)}
    if not isinstance(ident, dict) or not ident.get("userName") or not ident.get("host"): raise SystemExit("09_capabilities.json records no verified identity and host; a wave launches only from a " "doctor report that saw the migration principal")
    recorded = {"identity": ident["userName"], "host": ident["host"], "catalogs": sorted(rows.get("allowed_targets", {}).get("catalogs") or []), "guard_mode": rows.get("allowed_targets", {}).get("guard_mode"), "stop_mode": rows.get("workspace", {}).get("stop_mode")}
    for key, want in recorded.items():
        got = sorted(c.strip().strip("`").lower() for c in caps["catalogs"]) if key == "catalogs" else caps.get(key)
        if got != want: raise SystemExit(f"manifest 'capabilities.{key}' is {got!r} but the doctor recorded {want!r} in " "09_capabilities.json; copy the doctor's values, never edit them")
    if doctor.get("source") != m.get("source"): raise SystemExit("manifest 'source' differs from the source the doctor was signed for; " "re-run the doctor with --wave on this manifest")

def wave_signature(body, manifest_bytes):
    ident = body.get("identity") or {}
    key = hashlib.sha256(manifest_bytes + str(ident.get("userName") or "").encode() + str(ident.get("host") or "").encode()).digest()
    message = json.dumps({k: v for k, v in body.items() if k != "signature"}, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(key, message, "sha256").hexdigest()

def signed_doctor_report(path, manifest_bytes, now=None):
    """The doctor ran in the orchestrator's shell (credentials live there, not here) and left a record."""
    try: report = json.loads(path.read_text())
    except OSError: raise SystemExit(f"no doctor record at {path}; run factory-doctor with --wave {path.with_suffix('.json').name} " "in the orchestrator's shell, then re-run") from None
    except ValueError as e: raise SystemExit(f"{path} is not valid JSON: {e}") from None
    if not isinstance(report, dict): raise SystemExit(f"{path} is not a doctor record")
    if report.get("manifest_sha") != hashlib.sha256(manifest_bytes).hexdigest()[:12]: raise SystemExit(f"{path} was signed for another manifest (sha {report.get('manifest_sha')}); re-run the doctor with --wave")
    try: signed_at = datetime.datetime.fromisoformat(str(report.get("signed_at")))
    except ValueError: raise SystemExit(f"{path} has no valid signed_at") from None
    if signed_at.tzinfo is None: raise SystemExit(f"{path} signed_at must carry a UTC offset")
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if not (datetime.timedelta(0) <= now - signed_at <= DOCTOR_MAX_AGE): raise SystemExit(f"{path} was signed at {report['signed_at']}, more than {DOCTOR_MAX_AGE} ago (or in the future); " "re-run the doctor with --wave so the wave launches from a current preflight")
    if not isinstance(report.get("signature"), str) or not hmac.compare_digest( report["signature"], wave_signature(report, manifest_bytes)): raise SystemExit(f"{path} signature does not verify: the record was edited after the doctor wrote it; re-run the doctor")
    if report.get("hook_probe") != HOOK_PROBE_RESULT: raise SystemExit(f"{path} was signed for hook_probe {report.get('hook_probe')!r} but current.json says " f"{HOOK_PROBE_RESULT!r}; give the doctor and the pointer the same probe result")
    return report

def _base_tip():
    git = ["git", "-C", str(ROOT)]
    subprocess.run(git + ["fetch", "-q", "origin", f"+refs/heads/{BASE_BRANCH}:refs/remotes/origin/{BASE_BRANCH}"], check=True, capture_output=True, timeout=300)
    return subprocess.run(git + ["rev-parse", "--verify", f"origin/{BASE_BRANCH}^{{commit}}"], check=True, capture_output=True, text=True, timeout=300).stdout.strip()

def wave_base():
    """The base branch's commit on origin, now."""
    try: return _base_tip()
    except (OSError, subprocess.SubprocessError) as e: raise SystemExit(f"cannot resolve origin/{BASE_BRANCH} in {ROOT} ({e}); the ledger gate needs the base commit")

def launch_base():
    """The base commit when this run launched, persisted beside the manifest before the doctor, any child."""
    if resume:
        try: sha = BASE_SHA_PATH.read_text().strip()
        except OSError: raise SystemExit(f"no launch base at {BASE_SHA_PATH}; the ledger gate cannot resume without it, " "set mode: rerun for a fresh run") from None
        if not re.fullmatch(r"[0-9a-f]{40}", sha): raise SystemExit(f"{BASE_SHA_PATH} does not hold a commit sha; set mode: rerun for a fresh run")
        return sha
    sha = wave_base()
    tmp = BASE_SHA_PATH.with_suffix(".base_sha.tmp"); tmp.write_text(sha + "\n"); tmp.replace(BASE_SHA_PATH)
    return sha

def evidence_in_pr(head, path, units):
    """Whether a child's gate evidence is a file the gated PR head really carries under one of its own units'."""
    if not (isinstance(head, str) and isinstance(path, str)): return False
    parts = path.split("/")
    if (len(parts) < 4 or parts[:2] != [".migration", "recon"] or parts[2] not in units or any(p in ("", ".", "..") for p in parts[3:])): return False
    try: kind = subprocess.run(["git", "-C", str(ROOT), "cat-file", "-t", f"{head}:{path}"], check=True, capture_output=True, text=True, timeout=300).stdout.strip()
    except (OSError, subprocess.SubprocessError): return False
    return kind == "blob"

def pr_head(pr_url):
    """The head the host holds for a PR of this repo (refs/pull/N/head), fetched now; None when the URL is not."""
    m = PR_URL.fullmatch(pr_url) if isinstance(pr_url, str) else None
    if not m or m["repo"].lower() != MANIFEST["repo"].lower(): return None
    try: return fetch_ref(f"refs/pull/{m['n']}/head")
    except (OSError, subprocess.SubprocessError): return None

def fetch_ref(ref):
    """sha of `ref` on origin, fetched now into a ref only this wave writes: git's default fetch slot is one."""
    local = f"refs/migration/wave-{TAG}/{ref}"
    git = ["git", "-C", str(ROOT)]
    subprocess.run(git + ["fetch", "-q", "origin", f"+{ref}:{local}"], check=True, capture_output=True, timeout=300)
    return subprocess.run(git + ["rev-parse", "--verify", f"{local}^{{commit}}"], check=True, capture_output=True, text=True, timeout=300).stdout.strip()

def gate_outcomes(batch, reported, ledger, head):
    """The batch's gates after the child's report: every gate but a waived one takes the child's passed (with."""
    declared = {g["id"]: {**g, "decision_id": g.get("decision_id")} for g in batch.get("gates", [])}
    unmet = []
    if not isinstance(reported, list) or not all(isinstance(r, dict) for r in reported):
        if reported is not None: unmet.append("reported gates are not a list of {id, status, evidence} rows")
        reported = []
    seen = Counter(r.get("id") for r in reported)
    for r in reported:
        gid, g = r.get("id"), declared.get(r.get("id"))
        if g is None: unmet.append(f"gate {gid!r} reported but not declared for {batch['id']}")
        elif (seen[gid] > 1 or set(r) - {"id", "status", "evidence"} or r.get("status") not in ("passed", "failed") or not isinstance(r.get("evidence"), str) or (r["status"] == "passed" and not r["evidence"])): unmet.append(f"gate {gid} report must be one {{id, status: passed|failed, evidence}} row, evidence " "non-empty when passed")
        elif g["status"] == "waived": unmet.append(f"gate {gid} is waived in the plan; a child cannot change it")
        elif r["status"] == "passed" and not evidence_in_pr(head, r["evidence"], batch["units"]): unmet.append(f"gate {gid} evidence {r['evidence']!r} is not a file under .migration/recon/<unit>/ of " f"{', '.join(batch['units'])} at the gated PR head")
        else: g.update(status=r["status"], evidence=r["evidence"])
    for g in declared.values():
        if g["status"] != "waived" and (not seen[g["id"]] or g["status"] == "failed"):
            waiver = ledger_waiver(g["id"], batch["units"], ledger, MANIFEST["stop_c"])
            if waiver:
                g.update(status="waived", decision_id=waiver)
                continue
        if g["status"] != "waived" and not seen[g["id"]]: unmet.append(f"gate {g['id']} ({g['kind']}) has no child result; the plan's {g['status']} is not proof")
        elif g["status"] == "waived":
            if not override_decision(g["decision_id"], [g["id"], *batch["units"]], ledger, word="waive"): unmet.append(f"gate {g['id']} waived by {g['decision_id']} but no such row naming the gate and " f"{', '.join(batch['units'])} is in .migration/06_decisions.md")
        elif g["status"] != "passed": unmet.append(f"gate {g['id']} ({g['kind']}) is {g['status']}")
    return list(declared.values()), unmet

def gates_command(path):
    """`workflow.py gates <results.json>`: the wave-close gate rule for a wave the orchestrator gathered by."""
    try: reports = json.loads(Path(path).read_text())
    except (OSError, ValueError) as e: raise SystemExit(f"{path}: cannot read the children's results: {e}") from None
    if not isinstance(reports, list) or not all(isinstance(r, dict) and isinstance(r.get("batch"), str) for r in reports): raise SystemExit(f"{path} must be a list of {{batch, pr_url, gates}} rows, one per child")
    if not all(isinstance(r.get("pr_url"), str) and PR_URL.fullmatch(r["pr_url"]) for r in reports): raise SystemExit(f"{path}: every row needs the child's pr_url (a PR of {MANIFEST['repo']}); gate evidence is read at its head")
    by_batch = Counter(r["batch"] for r in reports)
    unknown = sorted(set(by_batch) - {b["id"] for b in MANIFEST["batches"]})
    if unknown or any(c > 1 for c in by_batch.values()): raise SystemExit(f"{path}: batches not in the manifest {unknown}, reported twice " f"{sorted(b for b, c in by_batch.items() if c > 1)}")
    ledger = decision_ledger()
    out = {}
    for b in sorted(MANIFEST["batches"], key=lambda b: b["id"]):
        report = next((r for r in reports if r["batch"] == b["id"]), None)
        if report is None: gates, unmet = [dict(g, decision_id=g.get("decision_id")) for g in b["gates"]], [f"batch {b['id']} was not gathered"]
        else:
            head = pr_head(report["pr_url"])
            gates, unmet = gate_outcomes(b, report.get("gates"), ledger, head)
            if head is None: unmet.append(f"{report['pr_url']} is not a PR of {MANIFEST['repo']} whose head git can fetch; no evidence stands")
        out[b["id"]] = {"gates": gates, "unmet": unmet}
    closed = not any(v["unmet"] for v in out.values())
    print(json.dumps({"wave": MANIFEST["wave"], "tag": TAG, "closed": closed, "batches": out}, indent=2, sort_keys=True))
    return 0 if closed else 1
STOP_MODE = MANIFEST.get("capabilities", {}).get("stop_mode") if isinstance(MANIFEST.get("capabilities"), dict) else None
if not gates_approved(MANIFEST.get("stop_c"), MANIFEST.get("wave"), MANIFEST.get("gates_sha"), decision_ledger(), STOP_MODE): raise SystemExit(f"manifest 'gates_sha' {MANIFEST.get('gates_sha')!r} is not approved by row {MANIFEST.get('stop_c')!r} of " f"{DECISIONS_PATH} (a table row with cells | D-<n> | user:<id>" f"{' or default-accepted' if STOP_MODE == 'soft' else ''} | STOP C wave-{MANIFEST.get('wave')} " "gates_sha <value> |); the manifest cannot approve its own gate list, so this run halts until STOP C " "records it")

def run_log():
    """This wave's append-only run log: one {stop_c, mode, run_id} record per launch (a workflow start or."""
    runs = []
    if RUNS_PATH.exists():
        try:
            for n, line in enumerate(RUNS_PATH.read_text().splitlines(), 1):
                run = json.loads(line) if line.strip() else None
                if run is not None:
                    if not (isinstance(run, dict) and isinstance(run.get("stop_c"), str)): raise ValueError(f"line {n} is not a {{stop_c, ...}} record")
                    runs.append(run)
        except (OSError, ValueError) as e: raise SystemExit(f"{RUNS_PATH} cannot say which STOP C rows this wave's runs spent ({e}); inspect or restore " "it before running, no approval is reusable on its word") from None
    return runs

def spent_stop_c():
    """The STOP C rows this wave's runs have spent, from its run log and the last result."""
    spent = [run["stop_c"] for run in run_log()]
    if MODE == "rerun" and RESULT_PATH.exists():
        try: previous = json.loads(RESULT_PATH.read_text())
        except (OSError, ValueError) as e: previous = e
        if not isinstance(previous, dict): raise SystemExit(f"{RESULT_PATH} cannot say which STOP C row the wave's last run spent ({previous!r}); inspect " "or restore it before rerunning, the old approval is not reusable on its word")
        spent.append(previous.get("stop_c"))
    return spent

def spent_halt():
    return SystemExit(f"{RUNS_PATH} or {RESULT_PATH} records a run this wave already made under STOP C row " f"{MANIFEST['stop_c']}; a rerun is a new run of the wave, so STOP C fires again: record its new row in " "the ledger and name it in the manifest's stop_c")

def record_run(mode, run_id=None, unspent=True):
    """Appends {stop_c, mode, run_id} to the run log under its exclusive lock, and when `unspent`, only if no."""
    with RUNS_PATH.open("a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        if unspent and MANIFEST["stop_c"] in spent_stop_c(): raise spent_halt()
        f.write(json.dumps({"stop_c": MANIFEST["stop_c"], "mode": mode, "run_id": run_id}, sort_keys=True) + "\n")
        f.flush()

def hand_run_state():
    """What the run log says of a hand run under the manifest's STOP C row: 'open', 'closed', 'workflow' or None."""
    return next(({"reserve": "open", "gates": "closed"}.get(run.get("mode"), "workflow")
                 for run in reversed(run_log()) if run["stop_c"] == MANIFEST["stop_c"]), None)

def check_wave_tag(tag, manifest):
    """wave-<tag>.json's last '-' segment is the wave number it runs, so a file renamed or mis-numbered."""
    wave = manifest["wave"]
    last = tag.rsplit("-", 1)[-1]
    if not last.isdigit() or int(last) != wave: raise SystemExit(f"wave-{tag}.json: the wave number in the file name must equal the manifest's 'wave' ({wave})")
    pipeline = tag.rsplit("-", 1)[0]
    count = manifest.get("pipelines", {}).get(pipeline) if pipeline != tag else None
    if pipeline != tag and (not isinstance(count, int) or isinstance(count, bool) or count < int(last)): raise SystemExit(f"wave-{tag}.json: wave-<pipeline>-<N>.json manifests must list every sibling pipeline in " f"'pipelines' (including {pipeline}): the planning barrier reads it")

def _is_manifest(name):
    return (name.startswith("wave-") and name.endswith(".json") and TAG_RE.fullmatch(name[len("wave-"):-len(".json")]) is not None)

def check_pipeline_updates(script, manifest_path):
    """One active update per Lakeflow pipeline per wave: the plugin's target-routing/pipeline_updates.py, run as a."""
    if not Path(script).is_file(): raise SystemExit(f"{script} is missing: the pointer's `plugin` must name the plugin root so the wave can run " "skills/target-routing/pipeline_updates.py before launching")
    try: proc = subprocess.run([sys.executable, str(script), str(manifest_path)], capture_output=True, text=True, timeout=300)
    except (OSError, subprocess.SubprocessError) as e: raise SystemExit(f"pipeline_updates.py did not run ({e}); the wave does not launch unchecked")
    try:
        result = json.loads(proc.stdout)
        order = result["order"]
        assert isinstance(order, dict) and all(isinstance(v, list) for v in order.values())
    except (ValueError, KeyError, TypeError, AssertionError): result, order = {}, None
    if proc.returncode != 0:
        why = [f"pipeline {s.get('pipeline')!r} is shared by {', '.join(map(str, s.get('batches', [])))} without a " f"pipeline_serialized decision" for s in result.get("shared", []) if s.get("serialized") is False]
        if result.get("unchecked_batches"): why.append(f"unsupported: {', '.join(map(str, result['unchecked_batches']))} declare no lakeflow_pipelines")
        raise SystemExit(f"pipeline_updates.py exit {proc.returncode}, the wave does not launch: " + ("; ".join(why) or f"{proc.stdout.strip()} {proc.stderr.strip()}".strip()))
    if order is None: raise SystemExit(f"pipeline_updates.py printed no order: {proc.stdout.strip()[:500]}")
    return order

def published_manifests():
    """{name: text} of every manifest under .migration/waves/ on origin's base branch, now."""
    git = ["git", "-C", str(ROOT)]
    try:
        tip = _base_tip()
        names = subprocess.run(git + ["ls-tree", "--name-only", tip, ".migration/waves/"], check=True, capture_output=True, text=True, timeout=300).stdout.split()
        return {Path(n).name: subprocess.run(git + ["show", f"{tip}:{n}"], check=True, capture_output=True, text=True, timeout=300).stdout for n in names if _is_manifest(Path(n).name)}
    except (OSError, subprocess.SubprocessError) as e: raise SystemExit(f"cannot read the manifests on origin/{BASE_BRANCH} ({e}); the planning barrier needs them")

def check_pipelines_published(waves_dir, m, published=None):
    """The collision check sees only manifests on disk, so a sibling pipeline whose manifests have not landed on."""
    on_disk = {f.name: f.read_text() for f in waves_dir.glob("wave-*.json") if _is_manifest(f.name)}
    if published is not None: published = {n: t for n, t in published.items() if _is_manifest(n)}
    names = on_disk if published is None else published
    pipelines = m.get("pipelines") or {}
    expected = {f"wave-{p}-{k}.json" for p, n in pipelines.items() if isinstance(n, int) and not isinstance(n, bool) for k in range(1, n + 1)}
    missing = sorted(expected - set(names))
    if missing: raise SystemExit(f"no manifest yet for {', '.join(missing)}: every pipeline in 'pipelines' commits its " "wave-<pipeline>-<N>.json before any sibling launches; pull the integration branch and " "re-run, or wait for the planning barrier")
    listed = "|".join(re.escape(p) for p in pipelines)
    extra = sorted(n for n in names if listed and re.fullmatch(rf"wave-(?:{listed})-\d+\.json", n) and n not in expected)
    if extra: raise SystemExit(f"{', '.join(extra)} is beyond the wave count in 'pipelines': the plans disagree")
    if published is None: return
    for name in sorted(expected & set(published)):
        try: theirs = json.loads(published[name])
        except ValueError: theirs = None
        if not isinstance(theirs, dict) or theirs.get("pipelines") != pipelines: raise SystemExit(f"{name} declares a different 'pipelines': the plans disagree")
    for name, text in sorted(on_disk.items()):
        if name not in published: raise SystemExit(f"{name} is not on origin/{BASE_BRANCH}: commit and push every manifest before preflight so " "sibling pipelines see it")
        if text != published[name]: raise SystemExit(f"{name} differs from origin/{BASE_BRANCH}: commit and push the edit before preflight so " "sibling pipelines check the same manifest")
    for name in sorted(set(published) - set(on_disk)): raise SystemExit(f"{name} is on origin/{BASE_BRANCH} but not on disk: pull the integration branch before " "preflight so the collision check reads it")

def other_wave_manifests(waves_dir, current):
    """{file name: {target_namespace, batches}} for every other wave-*.json in .migration/waves/: a bare."""
    out = {}
    for p in sorted(waves_dir.glob("wave-*.json")):
        if p.name == current or not _is_manifest(p.name): continue
        try: m = json.loads(p.read_text())
        except ValueError as e: raise SystemExit(f"{p} is not valid JSON ({e}); every wave manifest is read for cross-wave " "write-target collisions, so fix or remove it, then re-run") from None
        batches = m.get("batches") if isinstance(m, dict) else None
        if not isinstance(batches, list) or not all( isinstance(b, dict) and isinstance(b.get("id"), str) and isinstance(b.get("units"), list) and all(isinstance(u, str) for u in b["units"]) and isinstance(b.get("write_targets"), list) and all(isinstance(t, str) for t in b["write_targets"]) for b in batches): raise SystemExit(f"{p} has no 'batches' list of {{id, units, write_targets}} rows; every wave manifest is " "read for cross-wave write-target collisions, so fix or remove it, then re-run")
        namespace = m.get("target_namespace", "")
        if "target_namespace" in m and not valid_namespace(namespace): raise SystemExit(f"{p} 'target_namespace' must be the dotted catalog.schema (or schema) its harness run is " "given; every wave manifest is read for cross-wave write-target collisions, so fix it, " "then re-run")
        out[p.name] = {"target_namespace": namespace, "batches": batches}
    return out

def unit_mapping(unit):
    """The unit's recon mapping spec, None when the child has not written it yet."""
    p = ROOT / ".migration" / "units" / unit / "mapping_spec.json"
    if not p.is_file(): return None
    try: return json.loads(p.read_text())
    except ValueError as e: raise SystemExit(f"{p} is not valid JSON ({e})") from None
_SEGMENT = r'(?:[A-Za-z_][\w$]*|\[[^\]]+\]|"(?:[^"]|"")+"|`[^`]+`)'
PREDICATE_TOKEN = re.compile( r"\s+|(?P<string>'(?:[^']|'')*')|(?P<param>\$\{\w+\})|(?P<number>-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)" rf"|(?P<word>{_SEGMENT}(?:\.{_SEGMENT})*)|(?P<punct><>|!=|<=|>=|[=<>(),])")
PREDICATE_WORDS = {"and", "or", "not", "in", "between", "is", "null", "like", "true", "false"}

def column_key(name):
    """A column however a predicate or scope list spells it: the last segment, unquoted, case-folded."""
    return re.sub(r'^[`"\[]|[`"\]]$', "", str(name).strip().split(".")[-1].strip()).casefold()

def predicate_slices(where, scope):
    """The rows a target_where bounds recon to, as slices: a list of boxes, each a scope column -> the."""
    if not isinstance(where, str): return None
    scope = {column_key(c) for c in scope}
    tokens, pos = [], 0
    while pos < len(where):
        m = PREDICATE_TOKEN.match(where, pos)
        if not m: return None
        if m.lastgroup: tokens.append((m.lastgroup, m.group()))
        pos = m.end()
    def keyword(i, *words):
        return i < len(tokens) and tokens[i][0] == "word" and tokens[i][1].lower() in words
    def value(kind, text):
        if kind == "number": return "n", float(text)
        if kind == "param": return "p", text[2:-1]
        text = text[1:-1].replace("''", "'")
        return ("p", text[2:-1]) if re.fullmatch(r"\$\{\w+\}", text) else ("s", text)
    def pins(toks):
        words = [t.lower() for k, t in toks if k == "word"]
        columns = [i for i, (k, t) in enumerate(toks) if k == "word" and t.lower() not in PREDICATE_WORDS and not (t.lower() in ("date", "timestamp") and i + 1 < len(toks) and toks[i + 1][0] == "string")]
        values = [(i, value(k, t)) for i, (k, t) in enumerate(toks) if k in ("string", "number", "param")]
        ops = [t for k, t in toks if k == "punct" and t in ("=", "<", "<=", ">", ">=")]
        wildcard = "like" in words and all(re.fullmatch(r"'[%_]*'", t) for k, t in toks if k == "string")
        if not (len(columns) == 1 and column_key(toks[columns[0]][1]) in scope and values and (ops or any( w in ("in", "like", "between") for w in words)) and not wildcard and not any(w in ("is", "not") for w in words) and not any(k == "punct" and t in ("<>", "!=") for k, t in toks)): return None
        col = column_key(toks[columns[0]][1])
        if "like" in words: return col, ("like",)
        if "in" in words or ops == ["="]: return col, ("eq", frozenset(v for _, v in values))
        if "between" in words and len(values) == 2 and not ops: return col, ("range", (values[0][1], True), (values[1][1], True))
        if len(ops) == 1 and len(values) == 1:
            op, (at, v) = ops[0], values[0]
            if at < columns[0]: op = {"<": ">", "<=": ">=", ">": "<", ">=": "<="}[op]
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
        if keyword(i, "not"): return None, factor(i + 1)[1]
        if i < len(tokens) and tokens[i] == ("punct", "("):
            boxes, i = expr(i + 1)
            if i >= len(tokens) or tokens[i] != ("punct", ")"): raise ValueError
            return boxes, i + 1
        start, depth, between = i, 0, False
        while i < len(tokens) and not (depth == 0 and not between and (keyword(i, "and", "or") or tokens[i] == ("punct", ")"))):
            depth += (tokens[i] == ("punct", "(")) - (tokens[i] == ("punct", ")"))
            if depth < 0: raise ValueError
            between = (between and not keyword(i, "and")) or keyword(i, "between")
            i += 1
        atom = tokens[start:i]
        if not atom or depth: raise ValueError
        pin = pins(atom)
        return (None if pin is None else [{pin[0]: [pin[1]]}]), i
    try: boxes, end = expr(0)
    except ValueError: return None
    return boxes if end == len(tokens) else None

def bounded_predicate(where, scope):
    """Whether a target_where bounds the rows recon reads to the unit's own slice (predicate_slices)."""
    return predicate_slices(where, scope) is not None

def disjoint_slices(a, b):
    """Whether two readers' slices can never select the same row: every box of one against every box of."""
    def literal(v, kind):
        return v[0] == kind and kind != "p"
    def below(hi, lo):
        return (hi is not None and lo is not None and hi[0][0] == lo[0][0] and literal(hi[0], hi[0][0]) and (hi[0][1] < lo[0][1] or (hi[0][1] == lo[0][1] and not (hi[1] and lo[1]))))
    def apart(x, y):
        if x[0] == "like" or y[0] == "like": return False
        if x[0] == "eq" and y[0] == "eq":
            params = {v for v in x[1] | y[1] if v[0] == "p"}
            return not (x[1] & y[1]) and (not params or params == x[1] | y[1])
        if x[0] == "range" and y[0] == "range": return below(x[2], y[1]) or below(y[2], x[1])
        eq, rng = (x, y) if x[0] == "eq" else (y, x)
        return all(below((v, True), rng[1]) or below(rng[2], (v, True)) for v in eq[1])
    return all(any(apart(x, y) for c in set(p) & set(q) for x in p[c] for y in q[c]) for p in a for q in b)

def bounded_readers(spec, table, namespace=""):
    """Why the mapping spec's readers of `table` are not bounded to the unit's slice, '' when every one is,."""
    objects = spec.get("objects", spec.get("tables")) if isinstance(spec, dict) else None
    if not isinstance(objects, list) or not all(isinstance(o, dict) for o in objects): raise SystemExit("mapping spec 'objects' must be a list of object rows")
    mine = [o for o in objects if reads_target(o.get("object") or o.get("target_table") or "", table, namespace)]
    if not mine: return None
    def columns(row, inherited=None):
        scope = row.get("scope_columns", inherited)
        if not (isinstance(scope, list) and scope and all(isinstance(c, str) and c.strip() for c in scope)): return None
        return scope
    for o in mine:
        scope = columns(o)
        if scope is None: return "declares no scope_columns (non-empty list of the table's partition or run-date columns)"
        if not bounded_predicate(o.get("target_where"), scope): return "reads it without a target_where pinning one of its scope_columns"
        embeds = o.get("embeds", [])
        if not isinstance(embeds, list) or not all(isinstance(e, dict) for e in embeds): return "has 'embeds' that is not a list of embed rows"
        for e in embeds:
            escope = columns(e, scope)
            if escope is None or not bounded_predicate(e.get("target_where"), escope): return (f"embed '{e.get('array_path')}' reads it without its own target_where pinning one of its " "scope_columns (or the object's)")
    return ""

def reader_slices(spec, table, namespace=""):
    """The slices of `table` the spec's readers recon, every reading object's boxes and each of its embeds'."""
    objects = spec.get("objects", spec.get("tables"))
    mine = [o for o in objects if reads_target(o.get("object") or o.get("target_table") or "", table, namespace)]
    if not mine: return None
    return [box for o in mine for row in (o, *o.get("embeds", [])) for box in predicate_slices(row.get("target_where"), row.get("scope_columns", o.get("scope_columns", [])))]

def check_write_targets(batches, other_waves, mapping=None, namespace=""):
    """Two batches in one wave writing the same table means the lineage missed an edge: refuse to launch."""
    mapping = unit_mapping if mapping is None else mapping
    owners, spelled = {}, {}
    for b in batches:
        for t in b.get("write_targets", []):
            k = target_key(t, namespace)
            if k in owners: raise SystemExit(f"write-target collision before launch: '{t}' is claimed by " f"{owners[k]} and {b['id']}. Fix the wave plan, then re-run.")
            owners[k], spelled[k] = b["id"], t
    elsewhere, namespaces = {}, {None: namespace}
    for name, other in other_waves.items():
        if not (isinstance(other, dict) and isinstance(other.get("batches"), list) and "target_namespace" in other and (other["target_namespace"] == "" or valid_namespace(other["target_namespace"]))): raise SystemExit(f"{name} has no {{target_namespace, batches}} record; every wave manifest is read for " "cross-wave write-target collisions, so fix it, then re-run")
        namespaces[name] = other["target_namespace"]
        for b in other["batches"]:
            for t in b["write_targets"]: elsewhere.setdefault(target_key(t, namespaces[name]), []).append((name, b))
    for k, mine in owners.items():
        if k not in elsewhere: continue
        t = spelled[k]
        batch = next(b for b in batches if b["id"] == mine)
        shared = ", ".join(f"{name} {b['id']} (units {', '.join(b['units'])})" for name, b in elsewhere[k])
        why = (f"shared write target '{t}' is written by {mine} in this wave and by {shared}; every mapping " f"that reads it, in every wave, declares the object's scope_columns and a target_where pinning " f"one of them to the unit's own partition or run date (`1 = 1`, `col = col`, IS NOT NULL, <> and " f"columns outside scope_columns are no bound)")
        readers = []
        for wave, b in ((None, batch), *elsewhere[k]):
            where = "" if wave is None else f" ({wave} {b['id']})"
            before = len(readers)
            for u in b["units"]:
                spec = mapping(u)
                path = f".migration/units/{u}/mapping_spec.json"
                if spec is None: raise SystemExit(f"{why}: {path} is missing, so unit {u}{where} cannot be scoped")
                try: problem = bounded_readers(spec, k, namespaces[wave])
                except SystemExit as e: raise SystemExit(f"{why}: {path}: {e}") from None
                if problem is None: continue
                if problem: raise SystemExit(f"{why}: {path} (unit {u}{where}) {problem}")
                readers.append((f"unit {u}{where}", reader_slices(spec, k, namespaces[wave])))
            if len(readers) == before:
                paths = ", ".join(f".migration/units/{u}/mapping_spec.json" for u in b["units"])
                raise SystemExit(f"{why}: no unit of {b['id']}{where} reads '{t}': no object reading '{t}' in {paths}")
        for i, (a, slices_a) in enumerate(readers):
            for c, slices_c in readers[i + 1:]:
                if not disjoint_slices(slices_a, slices_c): raise SystemExit(f"shared write target '{t}': the slices {a} and {c} recon may overlap; their " f"target_where must pin a common scope column to values that cannot both hold " f"(=/IN with no value in common, differently named parameters, or literal ranges " f"that do not meet; LIKE, a parameter against a literal, or different columns " f"prove nothing). Fix the mapping specs, then re-run.")

def unit_dependencies(unit):
    """The unit's dependency analysis ({routine, reads, writes, calls} rows, shape in the source-dialect."""
    p = ROOT / ".migration" / "units" / unit / "dependencies.json"
    if not p.is_file(): return None
    try: data = json.loads(p.read_text())
    except ValueError as e: raise SystemExit(f"{p} is not valid JSON ({e})") from None
    rows = data.get("routines") if isinstance(data, dict) else None
    if not isinstance(rows, list) or not all( isinstance(r, dict) and isinstance(r.get("routine"), str) and r["routine"] and all(isinstance(r.get(k), list) and all(isinstance(t, str) and t for t in r[k]) for k in ("reads", "writes", "calls")) for r in rows): raise SystemExit(f"{p} needs a 'routines' list of {{routine, reads, writes, calls}} rows " "(string name, lists of names)")
    names = Counter(r["routine"].casefold() for r in rows)
    if any(n > 1 for n in names.values()): raise SystemExit(f"{p} names a routine twice: {', '.join(sorted(k for k, n in names.items() if n > 1))}")
    return rows

def transitive_writes(routines, resolve=None):
    """Every table written by the routines or anything they call, transitively, each through `resolve(row,."""
    resolve = resolve or (lambda r, t: {target_key(t)})
    by_name = {r["routine"].casefold(): r for r in routines}
    seen, writes = set(), set()
    todo = list(by_name)
    while todo:
        name = todo.pop()
        if name in seen: continue
        seen.add(name)
        r = by_name[name]
        for t in r["writes"]: writes |= resolve(r, t)
        for callee in r["calls"]:
            if callee.casefold() not in by_name: raise SystemExit(f"routine {r['routine']} calls {callee}, which the dependency analysis does not cover, " "so its writes are unknown")
            todo.append(callee.casefold())
    return writes

def mapped_target(spec, table, namespace=""):
    """The targets the unit's mapping spec gives a source table: every object whose root_table or."""
    k = target_key(table)
    objects = (spec.get("objects") or spec.get("tables") or []) if isinstance(spec, dict) else []
    return {target_key(o.get("object") or o.get("target_table") or "", namespace) for o in (objects if isinstance(objects, list) else []) if isinstance(o, dict) and target_key(o.get("root_table") or o.get("source_table") or "") == k}

def check_dependencies(batches, analysis=None, mapping=None, namespace=""):
    """Where a unit ships a dependency analysis, the batch's declared write_targets, less its."""
    analysis = unit_dependencies if analysis is None else analysis
    mapping = unit_mapping if mapping is None else mapping
    for b in batches:
        by_unit = {u: analysis(u) for u in b["units"]}
        complete = all(rows is not None for rows in by_unit.values())
        if not b["write_targets"] and not complete: raise SystemExit(f"batch {b['id']} declares no write_targets, which only a dependency analysis for every " f"unit ({', '.join(u for u, rows in by_unit.items() if rows is None)}) that writes " "nothing can justify. Fix the wave plan, then re-run.")
        routines = [r for rows in by_unit.values() for r in rows or []]
        if not routines and not complete: continue
        where = f"batch {b['id']} (units {', '.join(b['units'])})"
        names = Counter(r["routine"].casefold() for r in routines)
        if any(n > 1 for n in names.values()): raise SystemExit(f"{where}: two units analyse the same routine: " f"{', '.join(sorted(k for k, n in names.items() if n > 1))}")
        owner = {r["routine"].casefold(): u for u, rows in by_unit.items() for r in rows or []}
        specs = {u: mapping(u) for u, rows in by_unit.items() if rows}
        def resolve(r, t):
            u = owner[r["routine"].casefold()]
            if specs[u] is None: return {target_key(t, namespace)}
            targets = mapped_target(specs[u], t, namespace)
            if not targets: raise SystemExit(f"unit {u}'s routine {r['routine']} writes '{t}', which no object of its " "mapping_spec.json has as root_table, so its target is unknown")
            return targets
        try: actual = transitive_writes(routines, resolve)
        except SystemExit as e: raise SystemExit(f"{where}: {e}") from None
        deploy = {target_key(t, namespace) for t in b.get("deploy_objects", [])}
        written = sorted(deploy & actual)
        if written: raise SystemExit(f"batch {b['id']}: deploy_objects {written} are tables the call graph writes, not " "deployed objects. Fix the wave plan, then re-run.")
        declared = {target_key(t, namespace) for t in b["write_targets"]} - deploy
        extra = sorted(declared - actual) if complete else []
        if actual - declared or extra: raise SystemExit(f"batch {b['id']}: declared write_targets differ from the call graph's transitive writes; " f"missing from the declaration: {sorted(actual - declared) or '-'}; " f"extra in the declaration: {extra or '-'}. Fix the wave plan, then re-run.")
        called = {c.casefold() for r in routines for c in r["calls"]}
        roots = sorted(r["routine"] for r in routines if r["routine"].casefold() not in called)
        free = set(deploy)
        prefix = target_key(namespace).split(".") if namespace else []
        def take(root, fits):
            segs = target_key(root).split(".")
            found = sorted(d for d in free if d.split(".")[:len(prefix)] == prefix and fits(d.split(".")[len(prefix):], segs))
            if len(found) > 1: raise SystemExit(f"batch {b['id']}: analysed routine {root} is ambiguous: deploy_objects {found} could " "each be it. Spell the routine and its row alike, then re-run.")
            if found: free.discard(found[0])
            return bool(found)
        trailing = Counter(target_key(root).rsplit(".", 1)[-1] for root in roots)
        exact = {root for root in roots if take(root, lambda d, s: d == s if prefix and len(s) > 1 else d[-len(s):] == s)}
        undeclared = [root for root in roots if root not in exact and not take(root, lambda d, s: d[-1] == s[-1] and len(d) < len(s) and trailing[s[-1]] == 1)]
        if undeclared: raise SystemExit(f"batch {b['id']}: analysed routine(s) {undeclared} are entry points nothing in the batch " "calls, so the unit deploys them, but deploy_objects has no object of that name left for " "them (one row stands for one routine). Fix the wave plan, then re-run.")

def launch_checks(batches):
    """The cross-wave and dependency gates every launch path runs; returns the pipeline order."""
    check_pipelines_published(WAVES_DIR, MANIFEST, published_manifests() if "pipelines" in MANIFEST else None)
    check_write_targets(batches, other_wave_manifests(WAVES_DIR, MANIFEST_PATH.name), namespace=MANIFEST.get("target_namespace", ""))
    check_dependencies(batches, namespace=MANIFEST.get("target_namespace", ""))
    return check_pipeline_updates(PIPELINE_UPDATES, MANIFEST_PATH)
if sys.argv[1:2] == ["reserve"]:
    validate_manifest(MANIFEST)
    check_wave_tag(TAG, MANIFEST)
    launch_checks(sorted(MANIFEST["batches"], key=lambda b: b["id"]))
    record_run("reserve")
    print(json.dumps({"wave": MANIFEST["wave"], "stop_c": MANIFEST["stop_c"], "reserved": True}))
    sys.exit(0)
if sys.argv[1:2] == ["gates"]:
    validate_manifest(MANIFEST)
    check_wave_tag(TAG, MANIFEST)
    state = hand_run_state()
    if state == "closed": raise SystemExit(f"{RUNS_PATH} records that the hand run under STOP C row {MANIFEST['stop_c']} closed already; a new " "launch is a new run of the wave, so STOP C fires again: record its new row and name it in the " "manifest's stop_c, then `workflow.py reserve` before launching")
    if state == "workflow": raise spent_halt()
    if state is None: raise SystemExit(f"{RUNS_PATH} holds no reservation of STOP C row {MANIFEST['stop_c']}: run `python3 " "skills/migration-fanout/workflow.py reserve` before launching the children by hand, so the row " "is spent by that launch and no second launch reuses it")
    if len(sys.argv) != 3: sys.exit("usage: workflow.py gates <results.json>")
    code = gates_command(sys.argv[2])
    if code == 0: record_run("gates", unspent=False)
    sys.exit(code)
if not resume and not SMOKE and MANIFEST.get("stop_c") in spent_stop_c(): raise spent_halt()
PREFLIGHT = sys.argv[1:] == ["preflight"]
validate_manifest(MANIFEST)
check_wave_tag(TAG, MANIFEST)
BASE_SHA = None if PREFLIGHT else launch_base()
DOCTOR = signed_doctor_report(DOCTOR_PATH, MANIFEST_BYTES)
if not SMOKE:
    validate_manifest(MANIFEST, DOCTOR)

def _git_paths(*args):
    r = subprocess.run(["git", "-C", str(ROOT), "diff", "--name-only", "--no-renames", *args], check=True, capture_output=True, text=True, timeout=300)
    return r.stdout.split()

def ref_changed_paths(ref):
    """(head sha, paths) a ref on origin changes, from git: from its fork point on the base as it is now."""
    git = ["git", "-C", str(ROOT)]
    try:
        head = fetch_ref(ref)
        tip = _base_tip()
        merged = subprocess.run(git + ["merge-base", "--is-ancestor", head, tip], check=False, capture_output=True, timeout=300).returncode
        if merged not in (0, 1): raise subprocess.SubprocessError(f"merge-base rc={merged}")
        return head, _git_paths(f"{BASE_SHA if merged == 0 else tip}...{head}")
    except (OSError, subprocess.SubprocessError): return None

def unit_eligibility(head, units):
    """{unit: merge_eligible} from each unit's own .migration/recon/<unit>/result.json at the gated PR."""
    out = {}
    for u in units:
        try:
            text = subprocess.run(["git", "-C", str(ROOT), "show", f"{head}:.migration/recon/{u}/result.json"], check=True, capture_output=True, text=True, timeout=300).stdout
            got = json.loads(text).get("merge_eligible")
        except (OSError, subprocess.SubprocessError, ValueError, AttributeError): got = None
        out[u] = got if isinstance(got, bool) else None
    return out

def pr_changed_paths(pr_url):
    """What the PR really changes: the head the host holds for that PR of this repo (refs/pull/N/head),."""
    m = PR_URL.fullmatch(pr_url) if isinstance(pr_url, str) else None
    if not m or m["repo"].lower() != REPO.lower(): return None
    return ref_changed_paths(f"refs/pull/{m['n']}/head")

def replay_gate(record, pr_url):
    """The gate a replayed PASS keeps: its recorded head, while the PR still points at it or the base."""
    got = pr_changed_paths(pr_url)
    if got is None or got[0] == record["pr_head"]: return got and (got[0], [])
    try: merged = subprocess.run(["git", "-C", str(ROOT), "merge-base", "--is-ancestor", record["pr_head"], _base_tip()], check=False, capture_output=True, timeout=300).returncode
    except (OSError, subprocess.SubprocessError): return None
    return (record["pr_head"], []) if merged == 0 else got if merged == 1 else None

def verifier_changed_paths(wave, passed):
    """What the verifier itself changed on recon/wave-N."""
    got = ref_changed_paths(f"recon/wave-{wave}")
    if got is None or not all(isinstance(p.get("pr_head"), str) for p in passed): return None
    head, paths = got
    dirs = {p["pr_head"]: [f".migration/recon/{u}/" for u in p["units"]] for p in passed}
    own = {p for p in paths if not p.startswith(tuple(d for ds in dirs.values() for d in ds))}
    try:
        for pr_head, ds in dirs.items(): own.update(set(_git_paths(pr_head, head, "--", *ds)) & set(_git_paths(BASE_SHA, head, "--", *ds)))
    except (OSError, subprocess.SubprocessError): return None
    return sorted(own)

def ledger_violations(changed_paths, unit_ids, wave=None) -> list[str]:
    """Paths under .migration/ that a child (recon evidence for its own units) or the verifier (the."""
    allowed = tuple(f".migration/recon/{u}/" for u in unit_ids)
    if wave is not None: allowed += (f".migration/recon/wave-{wave}/",)
    return [p for p in changed_paths if p.startswith(".migration/") and not p.startswith(allowed)]

def batch_verdicts(verdicts, passed):
    """unit_verdicts keyed by batch id or unit id, normalised to one verdict per batch (a unit id that is also a."""
    if not isinstance(verdicts, dict): return {}
    owner = {u: p.get("batch") for p in passed for u in p.get("units") or []}
    owner.update({p.get("batch"): p.get("batch") for p in passed})
    out = {}
    for key, verdict in verdicts.items():
        batch = owner.get(key, key)
        out[batch] = None if batch in out and out[batch] != verdict else verdict
    return out

def validate_verify(verify, passed, wave=None, observed=None) -> list[str]:
    """The verifier's verdicts and its report branch."""
    problems = []
    if not isinstance(verify, dict): return ["verifier output invalid: expected an object"]
    expected = {p.get("batch") for p in passed}
    if None in expected:
        problems.append("verifier output invalid: passed batch is missing its id")
        expected.discard(None)
    verdicts = verify.get("unit_verdicts")
    if not isinstance(verdicts, dict): problems.append("verifier output invalid: unit_verdicts must be a dict"); verdicts = {}
    raw, verdicts = verdicts, batch_verdicts(verdicts, passed)
    for batch in sorted(b for b, v in verdicts.items() if v is None):
        keys = [k for k in raw if batch_verdicts({k: raw[k]}, passed) == {batch: raw[k]}]
        problems.append(f"verifier output invalid: conflicting verdicts for {batch}: " + ", ".join(f"{k}={raw[k]}" for k in keys))
    missing, extra = sorted(expected - set(verdicts)), sorted(set(verdicts) - expected)
    if missing: problems.append("verifier output invalid: missing verdicts for " + ", ".join(missing))
    if extra: problems.append("verifier output invalid: unexpected verdicts for " + ", ".join(extra))
    wave_verdict = verify.get("wave_verdict")
    if wave_verdict not in ("PASS", "FAIL"): problems.append("verifier output invalid: wave_verdict must be PASS or FAIL")
    for batch in sorted(expected - set(missing)):
        verdict = verdicts.get(batch)
        if verdict is not None and verdict not in ("PASS", "FAIL"): problems.append(f"verifier output invalid: verdict for {batch} is {verdict!r}")
        elif wave_verdict == "PASS" and verdict != "PASS": problems.append(f"verifier output invalid: wave PASS contradicts {batch}={verdict}")
    if wave_verdict == "FAIL" and expected and all(verdicts.get(b) == "PASS" for b in expected): problems.append("verifier output invalid: wave FAIL contradicts all unit verdicts PASS")
    if not isinstance(verify.get("findings"), list): problems.append("verifier output invalid: findings must be a list")
    changed = verify.get("changed_paths")
    if not isinstance(changed, list) or not all(isinstance(p, str) for p in changed): problems.append("verifier output invalid: changed_paths must be a list of paths (git diff --name-only)"); changed = []
    if wave is not None and observed is None: problems.append(f"verifier output invalid: branch recon/wave-{wave} not verifiable from git (fetch or diff " "failed), ledger integrity unverified")
    problems += [f"verifier output invalid: ledger tampered, changed {p}" for p in ledger_violations(sorted({*changed, *(observed or [])}), [], wave)]
    return problems

def validate_close(close, to_merge) -> list[str]:
    """Wave-close output problems without reading anything: every verified PR is in exactly one of."""
    problems = []
    if not isinstance(close, dict): return ["expected an object"]
    merged = close.get("merged_prs")
    if not isinstance(merged, list) or not all( isinstance(u, dict) and isinstance(u.get("pr_url"), str) and isinstance(u.get("merge_commit_sha"), str) and re.fullmatch(r"[0-9a-f]{40}", u["merge_commit_sha"]) and isinstance(u.get("merged_head"), str) and re.fullmatch(r"[0-9a-f]{40}", u["merged_head"]) for u in merged): problems.append("merged_prs rows must be {pr_url, merge_commit_sha, merged_head}"); merged = []
    merged_urls = [u["pr_url"] for u in merged]
    unmerged = close.get("unmerged")
    if not isinstance(unmerged, list) or not all( isinstance(u, dict) and isinstance(u.get("pr_url"), str) and isinstance(u.get("reason"), str) for u in unmerged): problems.append("unmerged must be a list of {pr_url, reason} rows"); unmerged = []
    want = {p.get("pr_url") for p in to_merge}
    for url in merged_urls:
        if url not in want: problems.append(f"merged a PR outside the wave ({url})")
    listed = Counter([*merged_urls, *(u["pr_url"] for u in unmerged)])
    for url in sorted(want):
        if listed.get(url, 0) != 1: problems.append(f"{url} is in {listed.get(url, 0)} of merged_prs/unmerged, expected exactly one")
    changed = close.get("changed_paths")
    if not isinstance(changed, list) or not all(isinstance(p, str) for p in changed): problems.append("changed_paths must be a list of paths (git diff --name-only)"); changed = []
    for p in changed: problems.append(f"wave-close step changed {p}; it writes nothing")
    return problems

def _applies_to(start, delta, commit):
    """Whether applying delta (a `diff-tree -p --binary --full-index` patch) onto start's tree in a."""
    git = ["git", "-C", str(ROOT)]
    with tempfile.TemporaryDirectory() as d:
        env = {"GIT_INDEX_FILE": str(Path(d) / "index")}
        subprocess.run(git + ["read-tree", start], check=True, env=env, capture_output=True, text=True, timeout=300)
        ok = subprocess.run(git + ["apply", "--cached", "--whitespace=nowarn"], input=delta, env=env, capture_output=True, text=True, timeout=300)
        if ok.returncode: return False
        tree = subprocess.run(git + ["write-tree"], check=True, env=env, capture_output=True, text=True, timeout=300).stdout.strip()
    landed = subprocess.run(git + ["rev-parse", f"{commit}^{{tree}}"], check=True, capture_output=True, text=True, timeout=300).stdout.strip()
    return tree == landed

def _same_change(parent, commit, head):
    """Whether commit is a squash or rebase of head: the gated head's exact change against its merge."""
    git = ["git", "-C", str(ROOT)]
    base = subprocess.run(git + ["merge-base", head, commit], check=True, capture_output=True, text=True, timeout=300).stdout.strip()
    n = int(subprocess.run(git + ["rev-list", "--count", f"{base}..{head}"], check=True, capture_output=True, text=True, timeout=300).stdout)
    delta = subprocess.run(git + ["diff-tree", "-p", "--binary", "--full-index", "--no-color", base, head], check=True, capture_output=True, text=True, timeout=300).stdout
    if not delta.strip(): return False
    if _applies_to(parent, delta, commit): return True
    start = subprocess.run(git + ["rev-parse", "--verify", "--quiet", f"{commit}~{n}"], check=False, capture_output=True, text=True, timeout=300)
    return n > 1 and start.returncode == 0 and _applies_to(start.stdout.strip(), delta, commit)

def proven_merged(to_merge, reported):
    """({pr_url: merge_commit_sha}, {pr_url: reason}) — a merge counts only when the PR head still equals the."""
    proven, reasons = {}, {}
    try: tip = _base_tip()
    except (OSError, subprocess.SubprocessError) as e: return proven, {p["pr_url"]: f"cannot resolve origin/{BASE_BRANCH} ({e})" for p in to_merge}
    git = ["git", "-C", str(ROOT)]
    merges = None
    for p in to_merge:
        url, head = p["pr_url"], p.get("pr_head")
        if not (isinstance(head, str) and re.fullmatch(r"[0-9a-f]{40}", head)):
            reasons[url] = "no gated PR head"
            continue
        current = pr_head(url)
        if current != head:
            reasons[url] = ("PR head unreadable" if current is None else f"PR head moved from {head} to {current} after verification")
            continue
        record = reported.get(url)
        try:
            if record is not None:
                mc = record.get("merge_commit_sha")
                if record.get("merged_head") != head:
                    reasons[url] = f"merged head {record.get('merged_head')} is not the gated head {head}"
                    continue
                on_base = (isinstance(mc, str) and re.fullmatch(r"[0-9a-f]{40}", mc) and subprocess.run(git + ["merge-base", "--is-ancestor", mc, tip], check=False, capture_output=True, timeout=300).returncode == 0)
                if not on_base:
                    reasons[url] = f"merge commit {mc} is not on origin/{BASE_BRANCH}"
                    continue
                parents = subprocess.run(git + ["rev-list", "--parents", "-n1", mc], check=True, capture_output=True, text=True, timeout=300).stdout.split()[1:]
                if len(parents) >= 2 and parents[1] != head:
                    reasons[url] = f"merge commit's PR-side parent is {parents[1]}, not the gated head"
                    continue
                if len(parents) < 2 and not (parents and _same_change(parents[0], mc, head)):
                    reasons[url] = f"commit {mc} does not carry the gated head's change"
                    continue
                proven[url] = mc
                continue
            if merges is None: merges = {p[2]: p[0] for line in subprocess.run(git + ["rev-list", "--merges", "--first-parent", "--parents", f"{BASE_SHA}..{tip}"], check=True, capture_output=True, text=True, timeout=300).stdout.splitlines() if len(p := line.split()) >= 3}
            if head in merges: proven[url] = merges[head]
            else: reasons[url] = (f"merge not recorded by the wave-close step and no merge commit on " f"origin/{BASE_BRANCH} has the gated head as its PR-side parent")
        except (OSError, subprocess.SubprocessError) as e: reasons[url] = f"merge proof failed ({e})"
    return proven, reasons
WAVE = MANIFEST["wave"]; REPO = MANIFEST["repo"]; BATCHES = sorted(MANIFEST["batches"], key=lambda b: b["id"])
WIDTH = int(MANIFEST.get("width", 20)); BREAKER = int(MANIFEST.get("breaker_threshold", 3))
AUTO_MERGE = bool(MANIFEST.get("auto_merge", False)); MAX_MINUTES = int(MANIFEST.get("max_minutes", 45))
CLOSE_MINUTES = int(MANIFEST.get("close_minutes", 10)); VERIFY_DEPTH = MANIFEST.get("verify_depth", "sampled")
RESYNC = MANIFEST.get("resync")
PRIOR_RESYNC = ((prior or {}).get("resync") or {}).get("report") if resume and isinstance(prior, dict) else None

def batch_verify_depth(batch) -> str:
    return batch.get("verify_depth", VERIFY_DEPTH)

def batch_max_minutes(batch) -> int:
    return int(batch.get("max_minutes", MAX_MINUTES))
META = { "name": f"smoke-wave-{TAG}" if SMOKE else f"migration-wave-{TAG}", "description": f"Wave {WAVE}: {len(BATCHES)} unit batches in parallel, then one independent verifier", "phases": [ {"title": "migrate", "detail": "one child per batch: convert, load, recon, open PR", "labels": [b["id"] for b in BATCHES], "soft_time_limit_minutes": max(batch_max_minutes(b) for b in BATCHES)}, *([{"title": "resync", "detail": "parent-owned identity/sequence resync on the listed units' targets", "count": 1, "soft_time_limit_minutes": 30}] if RESYNC else []), {"title": "verify", "detail": "independent recon over the wave", "count": 1, "soft_time_limit_minutes": 60}, {"title": "close", "detail": "one review round, then merge verifier-PASS PRs within the deadline", "count": 1, "soft_time_limit_minutes": CLOSE_MINUTES}, ], }
CHILD_SCHEMA = { "type": "object", "properties": { "status": {"type": "string", "enum": ["PASS", "FAIL", "BLOCKED"]}, "pr_url": {"type": "string"}, "branch": {"type": "string"}, "recon_verdict": {"type": "string", "enum": ["PASS", "FAIL", "NOT_RUN"]}, "recon_mode": {"type": "string", "description": "recon --mode of the evidence run (fixture never merges)"}, "merge_eligible": {"type": "boolean", "description": "result.json['merge_eligible'] of the evidence run"}, "merge_authority": { "type": "object", "properties": {"kind": {"type": "string", "enum": ["harness", "human_override"]}, "decision_id": {"type": "string"}}, "description": "human_override with the D-<n> row of .migration/06_decisions.md that says merge_override " "for your units; the workflow verifies the row. harness otherwise."}, "failure_class": {"type": "string"}, "write_targets": {"type": "array", "items": {"type": "string"}}, "changed_paths": {"type": "array", "items": {"type": "string"}, "description": "every path the PR changes: git diff --name-only <base>...<head>"}, "skill_feedback": {"type": "array", "items": {"type": "string"}, "description": "one line per rule you had to derive yourself"}, "gates": { "type": "array", "items": {"type": "object", "properties": {"id": {"type": "string"}, "status": {"type": "string", "enum": ["passed", "failed"]}, "evidence": {"type": "string"}}, "required": ["id", "status", "evidence"]}, "description": "outcome of each gate declared in your brief, by id; passed needs the evidence path"}, "recon_cost": {"type": "object", "description": "result.json['cost'] of the final live/snapshot/transactional run, one line"}, "one_line_summary": {"type": "string"}, }, "required": ["status", "recon_verdict", "recon_mode", "merge_eligible", "write_targets", "changed_paths", "one_line_summary"], }
VERIFY_SCHEMA = { "type": "object", "properties": { "wave_verdict": {"type": "string", "enum": ["PASS", "FAIL"]}, "unit_verdicts": {"type": "object", "description": "PASS or FAIL per batch, keyed by batch id or by unit id (a unit key is " "normalised to its batch id; one verdict per batch, a batch whose keys " "disagree is rejected)"}, "findings": {"type": "array", "items": {"type": "string"}}, "report_path": {"type": "string"}, "changed_paths": {"type": "array", "items": {"type": "string"}, "description": "every path your report branch changes: git diff --name-only <base>...<head>"}, "recon_cost": {"type": "object", "description": "summed result.json['cost'] over the verifier's re-runs"}, }, "required": ["wave_verdict", "unit_verdicts", "findings", "changed_paths"], }
CLOSE_SCHEMA = { "type": "object", "properties": { "merged_prs": {"type": "array", "items": {"type": "object", "properties": {"pr_url": {"type": "string"}, "merge_commit_sha": {"type": "string"}, "merged_head": {"type": "string"}}, "required": ["pr_url", "merge_commit_sha", "merged_head"]}, "description": "from `gh pr view <url> --json state,mergeCommit,headRefOid` after " "the merge; state must be MERGED"}, "unmerged": {"type": "array", "items": {"type": "object", "properties": {"pr_url": {"type": "string"}, "reason": {"type": "string"}}, "required": ["pr_url", "reason"]}}, "changed_paths": {"type": "array", "items": {"type": "string"}}, "review_findings": {"type": "array", "items": {"type": "string"}}, }, "required": ["merged_prs", "unmerged", "changed_paths"], }
RESYNC_SCHEMA = { "type": "object", "properties": { "status": {"type": "string", "enum": ["ok", "failed"]}, "sequences": {"type": "array", "items": {"type": "object", "properties": {"object": {"type": "string"}, "before": {}, "after": {}}, "required": ["object", "before", "after"]}}, "changed_paths": {"type": "array", "items": {"type": "string"}}, "one_line_summary": {"type": "string"}, }, "required": ["status", "sequences", "changed_paths", "one_line_summary"], }
RESYNC_CLASS = re.compile(r"identity|sequence", re.IGNORECASE)

def rerun_after_resync(record, resynced):
    """After a resync, a resumed run re-launches a child that never reported or failed on an identity/sequence."""
    if not resynced: return False
    if not isinstance(record, dict) or record.get("status") not in ("PASS", "FAIL", "BLOCKED"): return True
    return record.get("status") == "FAIL" and bool(RESYNC_CLASS.search(record.get("failure_class") or ""))

def resync_prompt(cfg):
    return ( f"You are the parent-owned identity resync step of wave {WAVE}. Repo: {REPO}. From the repo root, on the " f"base branch {BASE_BRANCH}, run exactly this command and nothing else: `{cfg['command']}`. It uses the " "target credentials by name (never print a value) and may execute only setval / identity-reseed statements " f"on the target objects of these units: {json.dumps(cfg['units'])}. The guard allowlist still applies; a " "block is a finding, not something to route around. Commit nothing and write no files; report `git status " "--porcelain` paths in changed_paths (it must be empty). For every sequence or identity column the command " "touched, report {object, before, after} in sequences; status failed if the command exited non-zero." )

def validate_resync(out) -> list[str]:
    if not isinstance(out, dict): return ["resync output invalid: expected an object"]
    problems = []
    if out.get("status") not in ("ok", "failed"): problems.append(f"resync output invalid: status {out.get('status')!r}")
    rows = out.get("sequences")
    if not isinstance(rows, list) or not all(isinstance(r, dict) and {"object", "before", "after"} <= set(r) for r in rows): problems.append("resync output invalid: sequences must be [{object, before, after}, ...]")
    changed = out.get("changed_paths")
    if not isinstance(changed, list): problems.append("resync output invalid: changed_paths must be a list")
    elif changed: problems.append("resync changed " + ", ".join(map(str, changed)) + " but must commit and write nothing")
    return problems

def child_prompt(batch):
    gates = [g for g in batch.get("gates", []) if g["status"] != "waived"]
    replay = (f"\nA parent-owned identity resync ran after your earlier attempt: " f"{json.dumps(PRIOR_RESYNC.get('sequences'), sort_keys=True)}. Start again from the merge-evidence recon.\n"
              if rerun_after_resync(REPLAYED.get(batch["id"]), PRIOR_RESYNC) else "")
    return ( f"You are one fan-out child in wave {WAVE}. Repo: {REPO}. Playbook: {MANIFEST['child_macro']}, batch " f"{batch['id']}. Anything missing from the brief stops you: report it as blocked, never improvise.\n\n" f"BRIEF:\n{batch['brief']}\n\n" f"Units: {json.dumps(batch['units'])}. Write targets you own, write nowhere else: " f"{json.dumps(batch.get('write_targets', []))}.\n" "Converted files and mapping specs: .migration/units/<unit_id>/ per the brief, spec at " ".migration/units/<unit_id>/mapping_spec.json.\n" + replay + "\n" + capability_block(batch["units"]) + f"Time budget: {batch_max_minutes(batch)} minutes; at the budget report status=BLOCKED with what landed and what blocked.\n" f"Gates STOP C declared (report each by id in gates as passed with its evidence path or failed; an unreported gate " f"fails the unit; a waived gate is the ledger's and is not listed; never rename or re-kind one): {json.dumps(gates, sort_keys=True)}\n" + "Recon: run the harness fixture-first, then the merge-evidence run; at most 3 full runs, never change a " "tolerance or 03_recon_tolerances.json; after 3 failing runs report status=FAIL with a one-word failure_class " "(timestamp_precision, decimal_rounding, sequence_behind_source, missing_rule).\n" f"The harness is the merge authority: status=PASS needs a recon PASS in one of {list(MERGE_EVIDENCE_MODES)} " "(transactional for Lakebase/operational units) with every unit's .migration/recon/<unit>/result.json saying " "merge_eligible=true; the workflow reads each file. Fixture evidence is never PASS. If a unit is not eligible and " "a human recorded a merge_override row for exactly your units in .migration/06_decisions.md, report " "merge_authority {kind: human_override, decision_id: D-<n>}; never write that row.\n" "Open exactly one PR; the first line of its description is PASS or FAIL. Do not merge it; review happens at " "wave close.\n" "Edit nothing under .migration/ except your own evidence under .migration/recon/<unit_id>/; report every path " "the PR changes in changed_paths (`git diff --name-only <base>...<head>`); any other .migration/ path turns " "PASS into FAIL ledger_tampered.\n" "Report: skill_feedback one line per rule you had to derive; recon_cost = result.json['cost'] of the final " "merge-evidence run; one_line_summary for a human skimming 20 of these: what landed, or why not." )

def capability_block(units):
    caps, src = MANIFEST["capabilities"], MANIFEST.get("source") or {}
    flags = " ".join(f"--unit {u}" for u in units) + (" " + " ".join([f"--source-family {src['family']} --source-secret {src['secret']}"] + [f"--param {shlex.quote(f'{k}={v}')}" for k, v in src.get("params", {}).items()]) if src else "")
    return ( f"Capability contract: {json.dumps(caps, sort_keys=True)}\nBefore converting anything run " f"`factory-doctor --role child --reuse-record .migration/waves/wave-{TAG}.doctor.json " f"--expect-identity {shlex.quote(caps['identity'])} --expect-host {shlex.quote(caps['host'])} {flags}` " f"(the signed record's source rows are reused while fresher than doctor_max_age {MANIFEST.get('doctor_max_age', 15)} " "minutes and bound to this manifest and identity; otherwise the doctor runs in full). Any fail row means " "status=BLOCKED naming the check id; a warn row is reported in your summary and you continue. Never " "switch identity, run `databricks auth login`, or edit .migration/allowed_targets.json.\n" )

def verify_prompt(passed):
    by_id = {b["id"]: b for b in BATCHES}
    depths = {p["batch"]: batch_verify_depth(by_id[p["batch"]]) for p in passed}
    merge_line = ("Do not merge anything; return unit_verdicts as PASS or FAIL keyed by batch id (a unit id key is " "normalised to its batch id, so one verdict per batch, whatever the batch size). The workflow's " "wave-close step merges the PRs you mark PASS (or the brief lists them for the merge owner in hard mode).")
    return ( f"You are the independent verifier for wave {WAVE}. Repo: {REPO}. You did not write " f"any of this code.\nRun the playbook {MANIFEST['verify_macro']} exactly as written over " f"these batches:\n{json.dumps(passed, sort_keys=True, indent=1)}\n\n" "Re-run the recon harness yourself. Do not trust the PR's pasted evidence, and run it with " "03_recon_tolerances.json and allowed_targets.json from the base branch, not the PR (a child that " "loosened a tolerance must fail here). For each PR run `git diff --name-only <base>...<head>`: any " ".migration/ path outside .migration/recon/<unit_id>/ is a FAIL for that unit with finding " "ledger_tampered. " + ("This wave is declared DEGRADED (no live source read): run the harness with `--mode structural` " "for every unit (Tier 0 `structural_parity` only: keys, constraints, indexes, triggers, identity " "columns, grants, read from both catalogs, no row tier) and mark the unit PASS only when that run's " "result.json says verdict=PASS and its merge_block_reasons is exactly [\"mode\"]: a structural_gap " "or warnings entry means a catalog the harness could not read or a category it does not cover, " "which is unverified structure, so FAIL with finding structure_unverifiable; verdict=FAIL is FAIL " "with finding structural_drift. Its result.json is never merge_eligible and the mode reason alone is " "expected here. Do not re-run Tier 1-3 and do not lower or raise a " "depth: the child's snapshot row parity stands. " if MANIFEST.get("degraded") is True else f"Mark a unit PASS only if you re-ran the harness in one of {list(MERGE_EVIDENCE_MODES)} " "(the same mode the child used: transactional for Lakebase/operational units) and result.json " "says merge_eligible=true. " f"Run with `--depth <d>` per batch, exactly as listed here: {json.dumps(depths, sort_keys=True)} " "(sampled = Tier 1+2 plus a stratified Tier 3 with a seed different from the child's; full = keyed " "full diff). Never lower a batch's depth; raising it is allowed and noted in findings. ") + "A batch listed with merge_authority kind human_override was cleared by the " "named D-<n> merge_override row of .migration/06_decisions.md: mark it PASS on a PASS verdict even if " "merge_eligible is false, and cite the decision id in findings. " "Each batch lists " "its acceptance gates with the evidence the child gave; open the evidence of every passed gate and FAIL " "the unit if it does not show what the gate's kind requires. " "Sum result.json['cost'] over your runs into recon_cost.\n" f"{merge_line}\nWrite the wave recon report to .migration/recon/wave-{TAG}/report.md, " f"commit it on branch recon/wave-{TAG}, push, and give '<branch>:<path>' in " "report_path. Do not edit any other file under .migration/; report your branch's " "`git diff --name-only <base>...<head>` in changed_paths. Each finding is one plain " "sentence a lead can read without opening anything." )

def close_prompt(to_merge, deadline_minutes):
    return ( f"You are the wave-close step for wave {WAVE}. Repo: {REPO}. Merge exactly these PRs, nothing else: " f"{json.dumps(to_merge, sort_keys=True)}. Each was verified PASS by the independent verifier at " "pr_head; before merging, check the PR head still equals it and the PR is open and mergeable, " "otherwise leave it and list it in unmerged with a one-sentence reason. Merge nothing whose head " "is not exactly the verified pr_head, and never push to the PR branch. After each merge run " "`gh pr view <url> --json state,mergeCommit,headRefOid`: put {pr_url, merge_commit_sha: " "mergeCommit.oid, merged_head: headRefOid} in merged_prs only when state is MERGED — anything else " "goes to unmerged. First run one Devin Review round over these PRs and report each open finding as one line " "in review_findings; a finding is not a merge blocker unless a human says so in .migration/06_decisions.md. " "Do it within " f"{deadline_minutes} minutes; when time is up, stop and list the rest as unmerged. Write nothing: " "no commits, no files, no other PR; report `git diff --name-only` of anything you changed in " "changed_paths (it must be empty). The orchestrator commits the wave's ledger artifacts in one " "wave-close PR afterwards." )

class Breaker:
    def __init__(self, threshold):
        self.threshold = threshold
        self.classes = Counter()
        self.tripped_on = None
    def record(self, failure_class):
        if not failure_class: return
        self.classes[failure_class] += 1
        if self.classes[failure_class] >= self.threshold and not self.tripped_on:
            self.tripped_on = failure_class
            log(f"CIRCUIT BREAKER: {self.threshold} children failed with '{failure_class}'. " "No new children will launch this run.")

async def run_batch(batch, sem, breaker, waits=(), done=None):
    """`waits` are the done events of the batches this one must follow (a shared, serialized pipeline); `done`."""
    try:
        for event in waits: await event.wait()
        return await _run_batch(batch, sem, breaker)
    finally:
        if done is not None: done.set()

async def _run_batch(batch, sem, breaker):
    async with sem:
        if breaker.tripped_on: return {"status": "NOT_LAUNCHED", "recon_verdict": "NOT_RUN", "one_line_summary": f"held back: breaker tripped on '{breaker.tripped_on}'"}
        log(f"launch {batch['id']} ({len(batch['units'])} units)")
        prompt = child_prompt(batch)
        try: out = await agent(prompt, phase="migrate", schema=CHILD_SCHEMA, label=batch["id"], repos=[REPO], soft_time_limit_minutes=batch_max_minutes(batch))
        except WorkflowAgentError as e: out = {"status": "FAIL", "recon_verdict": "NOT_RUN", "failure_class": "session_died", "one_line_summary": f"child session died: {e}"}
        out["prompt_sha"] = prompt_sha(prompt)
        def downgrade(failure_class, why):
            out["status"], out["failure_class"] = "FAIL", failure_class
            out["one_line_summary"] = "PASS downgraded: " + why + out["one_line_summary"]
        if out["status"] == "PASS" and (out["recon_verdict"] != "PASS" or out.get("recon_mode") not in MERGE_EVIDENCE_MODES): downgrade("non_merge_evidence", f"recon evidence was {out.get('recon_mode')}/{out.get('recon_verdict')}; ")
        if out["status"] == "PASS" and (not out.get("pr_url") or not out.get("branch")): downgrade("missing_pr", "no PR URL/branch reported; ")
        reported = out.get("changed_paths")
        usable = isinstance(reported, list) and all(isinstance(p, str) for p in reported)
        record = REPLAYED.get(batch["id"])
        gated = (replay_gate(record, out["pr_url"])
                 if isinstance(record, dict) and record.get("status") == "PASS" and isinstance(record.get("pr_head"), str)
                 and record.get("prompt_sha") == out["prompt_sha"] and record.get("pr_url") == out.get("pr_url")
                 else pr_changed_paths(out.get("pr_url")))
        observed = gated[1] if gated else None
        if gated: out["pr_head"] = gated[0]
        tampered = ledger_violations(sorted({*(reported if usable else []), *(observed or [])}), batch["units"])
        if tampered:
            if out["status"] == "PASS": downgrade("ledger_tampered", f"PR changed the ledger ({', '.join(tampered)}); ")
            else:
                out["status"], out["failure_class"] = "FAIL", "ledger_tampered"
                out["one_line_summary"] = f"PR changed the ledger ({', '.join(tampered)}); " + out["one_line_summary"]
        elif out["status"] == "PASS" and (not usable or observed is None): downgrade("ledger_tampered", "changed_paths " + ("not reported" if not usable else "not verifiable from git (not a PR of this repo, or its fetch or diff failed)") + ", ledger integrity unverified; ")
        if out["status"] == "PASS":
            claimed = out.get("merge_authority")
            decision = claimed.get("decision_id") if isinstance(claimed, dict) else None
            evidence = unit_eligibility(out["pr_head"], batch["units"])
            ineligible = sorted(u for u, e in evidence.items() if e is not True)
            if out.get("merge_eligible") is True and not ineligible: out["merge_authority"] = {"kind": "harness", "decision_id": None}
            elif (isinstance(claimed, dict) and claimed.get("kind") == "human_override" and override_decision(decision, batch["units"], decision_ledger())): out["merge_authority"] = {"kind": "human_override", "decision_id": decision}
            else:
                out.pop("merge_authority", None)
                why = "; ".join(f".migration/recon/{u}/result.json at the PR head " + ("is missing or malformed" if evidence[u] is None else f"has merge_eligible={evidence[u]!r}") for u in ineligible) or f"the child reported merge_eligible={out.get('merge_eligible')!r}"
                downgrade("merge_authority", f"recon evidence is not merge_eligible=true for every unit ({why}) and no merge_override row {decision or 'D-<n>'} naming {', '.join(batch['units'])} is in .migration/06_decisions.md; ")
        if out["status"] == "PASS":
            out["gates"], unmet = gate_outcomes(batch, out.get("gates"), decision_ledger(), out["pr_head"])
            if unmet: downgrade("gates", "; ".join(unmet) + "; ")
        if out["status"] != "PASS" and (record is None or (isinstance(record, dict) and ( record.get("status") == "PASS" or record.get("failure_class") != out.get("failure_class")))): breaker.record(out.get("failure_class") or "unclassified")
        log(f"done   {batch['id']}: {out['status']} / recon {out['recon_verdict']}: " f"{out['one_line_summary']}")
        return out
COST_KEYS = ("source_statements", "target_statements", "source_rows_fetched", "target_rows_fetched")

def sum_cost(costs) -> dict:
    """Sum result.json['cost'] dicts; a side whose adapter did not count stays None."""
    total: dict = {k: 0 for k in COST_KEYS} | {"elapsed_s": 0.0}
    for c in costs:
        if not isinstance(c, dict): continue
        for k in COST_KEYS:
            v = c.get(k)
            if v is None: total[k] = None
            elif total[k] is not None and isinstance(v, (int, float)) and not isinstance(v, bool): total[k] += v
        if isinstance(c.get("elapsed_s"), (int, float)): total["elapsed_s"] += c["elapsed_s"]
    return total

def cost_line(results, verify) -> str:
    """Estimate (STOP C) against actuals (children + verifier), so the next wave's estimate."""
    est = MANIFEST.get("cost_estimate")
    actual = sum_cost([r.get("recon_cost") for r in results] + ([verify.get("recon_cost")] if isinstance(verify, dict) else []))
    if est is None and all(actual[k] in (None, 0) for k in COST_KEYS): return "Cost: no estimate in the manifest and no recon_cost reported."
    def fmt(d):
        return ", ".join(f"{k}={d.get(k)}" for k in COST_KEYS if d.get(k) is not None) or "n/a"
    return (f"Cost: estimated {fmt(est or {})}; actual {fmt(actual)}, " f"harness time {round(actual['elapsed_s'])}s. Verifier depth {VERIFY_DEPTH}" + (", overrides: " + ", ".join(f"{b['id']}={b['verify_depth']}" for b in BATCHES if "verify_depth" in b) if any("verify_depth" in b for b in BATCHES) else "") + ".")

def waived_gates(results):
    return [{"batch": b["id"], "units": b["units"], "gate": g["id"], "decision_id": g["decision_id"]} for b, r in zip(BATCHES, results) if r["status"] == "PASS" for g in r.get("gates", []) if g["status"] == "waived"]

def merge_overrides(results):
    return [{"batch": b["id"], "units": b["units"], "decision_id": r["merge_authority"]["decision_id"]} for b, r in zip(BATCHES, results) if r["status"] == "PASS" and (r.get("merge_authority") or {}).get("kind") == "human_override"]

def write_brief(results, verify, surprises, undeclared, unreported, auto_merge, close=None, to_merge=None, resync=None):
    """Verdicts, the cost line, surprises, the resync report, and the PRs still to merge: what a lead reads in a."""
    by_status = {st: [b["id"] for b, r in zip(BATCHES, results) if r["status"] == st] for st in ("PASS", "FAIL", "BLOCKED", "NOT_LAUNCHED")}
    lines = [f"# Wave {WAVE} close", "", f"Landed: {len(by_status['PASS'])} of {len(BATCHES)} batches passed their own recon.", f"Independent verify: {verify['wave_verdict'] if verify else 'NOT RUN'}.", f"Failed: {', '.join(by_status['FAIL']) or 'none'}. Blocked: {', '.join(by_status['BLOCKED']) or 'none'}. " f"Held by circuit breaker: {', '.join(by_status['NOT_LAUNCHED']) or 'none'}.", cost_line(results, verify)]
    if surprises: lines.append(f"Merges held: two children reported the same write target ({', '.join(surprises)}).")
    if undeclared: lines.append("Merges held: children wrote outside their declared targets: " + "; ".join(f"{k}: {', '.join(v)}" for k, v in sorted(undeclared.items())) + ".")
    if unreported: lines.append(f"Merges held: {', '.join(unreported)} passed but reported no write targets.")
    waived = waived_gates(results)
    if waived: lines.append("Gates waived by ledger decision: " + "; ".join(f"{w['batch']}/{w['gate']} by {w['decision_id']}" for w in waived) + ".")
    overrides = merge_overrides(results)
    if overrides: lines.append("Human override authority (merge_eligible=false): " + "; ".join(f"{o['batch']} ({', '.join(o['units'])}) by {o['decision_id']}" for o in overrides) + ".")
    if resync:
        report = resync.get("report") or {}
        lines += ["", f"Identity resync (`{resync['command']}` over {', '.join(resync['units'])}): " f"{report.get('status', 'no report')}. {report.get('one_line_summary', '')}".rstrip()]
        lines += [f"- {r['object']}: {r['before']} -> {r['after']}" for r in report.get("sequences") or []]
        lines += [f"- problem: {p}" for p in resync.get("problems", [])]
    if close is not None:
        lines += ["", f"Wave close: {len(close.get('merged_prs', []))} of {len(to_merge or [])} verified PRs merged " f"within {CLOSE_MINUTES} min."]
        lines += [f"Not merged: {u['pr_url']} ({u['reason']})" for u in close.get("unmerged", [])]
        lines += [f"- review: {f}" for f in (close.get("review_findings") or [])]
    if not auto_merge:
        urls = [r["pr_url"] for r in results if r["status"] == "PASS" and r.get("pr_url")]
        lines.append("Awaiting manual merge: " + (", ".join(urls) or "none reported"))
    elif isinstance(close, dict) and close.get("unmerged"): lines.append("Awaiting manual merge: " + ", ".join(u["pr_url"] for u in close["unmerged"]))
    findings = (verify or {}).get("findings") or []
    lines += ["", "Verifier findings:" if findings else "Verifier findings: none."] + [f"- {f}" for f in findings]
    feedback = sorted({s for r in results for s in r.get("skill_feedback", []) if isinstance(s, str)})
    lines += ["", "Skill feedback to fold in before the next wave:" if feedback else "Skill feedback: none."]
    lines += [f"- {s}" for s in feedback]
    lines += ["", "Per batch:"] + [f"- {b['id']}: {r['status']}. {r['one_line_summary']}" + (f" {r['pr_url']}" if r.get("pr_url") else "") for b, r in zip(BATCHES, results)]
    _tmp_write(BRIEF_PATH, ".brief.md.tmp", "\n".join(lines) + "\n")

def _verify_sink(verify):
    """A verifier record with a writable findings list; a non-dict report becomes a FAIL record."""
    if not isinstance(verify, dict): verify = {"wave_verdict": "FAIL", "unit_verdicts": {}, "findings": []}
    if not isinstance(verify.get("findings"), list): verify["findings"] = []
    return verify

async def main():
    if not resume: RUN_ID_PATH.unlink(missing_ok=True)
    await register_workflow(META)
    order = launch_checks(BATCHES)
    log(f"wave {WAVE}: {len(BATCHES)} batches, width {WIDTH}, breaker at {BREAKER}" + (f", serialized {order}" if order else ""))
    sem = asyncio.Semaphore(WIDTH)
    breaker = Breaker(BREAKER)
    done = {b["id"]: asyncio.Event() for b in BATCHES}
    results = await asyncio.gather(*(run_batch(b, sem, breaker, waits=[done[d] for d in order.get(b["id"], []) if d in done], done=done[b["id"]]) for b in BATCHES))
    namespace = MANIFEST.get("target_namespace", "")
    reported = Counter(t for r in results for t in {target_key(t, namespace) for t in r.get("write_targets", [])})
    surprises = sorted(t for t, c in reported.items() if c > 1)
    undeclared = {b["id"]: extra for b, r in zip(BATCHES, results)
                  if (extra := sorted({t for t in r.get("write_targets", []) if target_key(t, namespace) not in {target_key(t, namespace) for t in b["write_targets"]}}))}
    unreported = [b["id"] for b, r in zip(BATCHES, results) if r["status"] == "PASS" and b["write_targets"] and not r.get("write_targets")]
    auto_merge = AUTO_MERGE
    for held, msg in ((surprises, f"WARNING: children reported overlapping write targets after the fact: {surprises}."),
                      (undeclared, f"HALT: children wrote outside their declared targets: {undeclared}."),
                      (unreported, f"HALT: PASS children did not report write targets: {unreported}.")):
        if held:
            auto_merge = False
            log(msg + " Auto-merge is off for this wave; a human decides at wave close.")
    resync = None
    if RESYNC:
        log(f"resync: {RESYNC['command']} over {RESYNC['units']}")
        try:
            report = await agent(resync_prompt(RESYNC), phase="resync", schema=RESYNC_SCHEMA, label=f"resync-wave-{TAG}", repos=[REPO], soft_time_limit_minutes=30)
            problems = validate_resync(report)
        except WorkflowAgentError as e: report, problems = None, [f"resync session died: {e}"]
        resync = {"command": RESYNC["command"], "units": RESYNC["units"], "report": report, "problems": problems}
        for problem in problems: log(f"WARNING: {problem}")
    passed = [{"batch": b["id"], "units": b["units"], "pr_url": r.get("pr_url", ""), "branch": r.get("branch", ""), "pr_head": r.get("pr_head"), "merge_authority": r.get("merge_authority"), "gates": r.get("gates", [])} for b, r in zip(BATCHES, results) if r["status"] == "PASS"]
    verify = None
    if passed:
        log(f"verify: {len(passed)} batches to an independent session")
        try: verify = await agent(verify_prompt(passed), phase="verify", schema=VERIFY_SCHEMA, label=f"verify-wave-{TAG}", repos=[REPO])
        except WorkflowAgentError as e: verify = {"wave_verdict": "FAIL", "unit_verdicts": {}, "findings": [f"verifier session died: {e}"]}
    else: log("verify: skipped, no batch passed")
    verify_problems = (validate_verify(verify, passed, TAG, verifier_changed_paths(TAG, passed)) if verify is not None else [])
    if verify_problems:
        verify = _verify_sink(verify)
        verify["wave_verdict"] = "FAIL"
        verify["findings"].extend(verify_problems)
    to_merge = []
    if not verify_problems and isinstance(verify, dict) and isinstance(verify.get("unit_verdicts"), dict):
        verify["unit_verdicts"] = batch_verdicts(verify["unit_verdicts"], passed)
        to_merge = [{"batch": p["batch"], "units": p["units"], "pr_url": p["pr_url"], "pr_head": p.get("pr_head")} for p in passed if verify["unit_verdicts"].get(p["batch"]) == "PASS" and p.get("pr_url")]
    close = None
    if auto_merge and to_merge:
        try: close = await asyncio.wait_for( agent(close_prompt(to_merge, CLOSE_MINUTES), phase="close", schema=CLOSE_SCHEMA, label=f"close-wave-{TAG}", repos=[REPO], soft_time_limit_minutes=CLOSE_MINUTES), timeout=CLOSE_MINUTES * 60)
        except (asyncio.TimeoutError, WorkflowAgentError) as e: close = {"merged_prs": [], "unmerged": [{"pr_url": p["pr_url"], "reason": f"wave-close step did not finish within {CLOSE_MINUTES} minutes: {e}"} for p in to_merge], "changed_paths": []}
    close_problems = validate_close(close, to_merge) if close is not None else []
    if close is not None:
        raw = close
        reported = {}
        if resume and MERGES_PATH.exists():
            try: stored = json.loads(MERGES_PATH.read_text())
            except ValueError: stored = {}
            gated = {p["pr_url"]: p.get("pr_head") for p in to_merge}
            reported.update({u: {"merge_commit_sha": mc, "merged_head": gated[u]} for u, mc in stored.items() if u in gated and isinstance(mc, str)} if isinstance(stored, dict) else {})
        rows = raw.get("merged_prs") if isinstance(raw, dict) else None
        if isinstance(rows, list): reported.update({u["pr_url"]: u for u in rows if isinstance(u, dict) and isinstance(u.get("pr_url"), str)})
        proven, proof = proven_merged(to_merge, reported)
        reasons = {u["pr_url"]: u["reason"] for u in raw.get("unmerged", []) if isinstance(u, dict) and isinstance(u.get("pr_url"), str) and isinstance(u.get("reason"), str)} if isinstance(raw, dict) else {}
        changed = raw.get("changed_paths") if isinstance(raw, dict) else None
        close = {"merged_prs": [p["pr_url"] for p in to_merge if p["pr_url"] in proven], "merges": [{"pr_url": p["pr_url"], "merge_commit_sha": proven[p["pr_url"]]} for p in to_merge if p["pr_url"] in proven], "unmerged": [{"pr_url": p["pr_url"], "reason": (proof.get(p["pr_url"]) or reasons.get(p["pr_url"]) or "wave-close output invalid")} for p in to_merge if p["pr_url"] not in proven], "changed_paths": changed if isinstance(changed, list) else [], "review_findings": [f for f in (raw.get("review_findings") if isinstance(raw, dict) else None) or [] if isinstance(f, str)]}
        _tmp_write(MERGES_PATH, ".merges.json.tmp", json.dumps(proven, indent=2, sort_keys=True) + "\n")
        if close_problems: close["invalid"] = raw
    if close_problems:
        verify = _verify_sink(verify)
        verify["findings"].extend(f"wave close invalid: {p}" for p in close_problems)
    closed = (breaker.tripped_on is None and not surprises and not undeclared and not unreported and not verify_problems and not close_problems and (close is None or not close["unmerged"]) and verify is not None and verify["wave_verdict"] == "PASS" and all(r["status"] == "PASS" for r in results))
    _tmp_write(RESULT_PATH, ".result.json.tmp", json.dumps({ "wave": WAVE, "tag": TAG, "manifest_sha": MANIFEST_SHA, "width": WIDTH, "run_id": RUN_ID, "base_sha": BASE_SHA, "mode": MODE, "stop_c": MANIFEST["stop_c"], "hook_probe": HOOK_PROBE_RESULT, "doctor_signed_at": DOCTOR.get("signed_at"), "breaker_tripped_on": breaker.tripped_on, "auto_merge": auto_merge, "closed": closed, "write_target_overlaps": surprises, "undeclared_write_targets": undeclared, "unreported_write_targets": unreported, "pipeline_order": order, "merge_overrides": merge_overrides(results), "waived_gates": waived_gates(results), "batches": [{"id": b["id"], **r} for b, r in zip(BATCHES, results)], "verify": verify, "resync": resync, "close": close, "close_minutes": CLOSE_MINUTES, }, indent=2, sort_keys=True) + "\n")
    write_brief(results, verify, surprises, undeclared, unreported, auto_merge, close, to_merge, resync)
    log(f"wrote {RESULT_PATH} and {BRIEF_PATH}")
    log(f"wave {WAVE} verdict: {verify['wave_verdict'] if verify else 'NO PASSING BATCHES'}")
if PREFLIGHT:
    order = launch_checks(BATCHES)
    print(json.dumps({"wave": WAVE, "ready": True, "batches": [b["id"] for b in BATCHES], "pipeline_order": order}, sort_keys=True))
    sys.exit(0)
if not resume and not SMOKE: record_run(MODE, RUN_ID)
asyncio.run(main())
