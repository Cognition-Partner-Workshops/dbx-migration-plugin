"""Migration fan-out workflow: run one wave of unit-migration children, then one
independent verifier, and write the wave result the orchestrator gates on.

Run with the `run_workflow` tool. The orchestrator writes `.migration/waves/current.json`
at or above the sandbox cwd; it names the wave manifest and workspace when the pointer does
not live in the workspace. Same script for every kit: the manifest names the child playbook
macro, so nothing here is DBX- or Mongo-specific.

What this script guarantees, so the orchestrator does not have to:
  - Two batches in the same wave never share a write target (checked BEFORE launch).
  - At most `width` children run at once.
  - Circuit breaker: after `breaker_threshold` children fail with the same failure class,
    no new children launch; already-running ones finish. Nothing is retried blindly.
  - Children never edit shared ledger files. This script is the single writer of
    <manifest>.result.json and the ledger rows the orchestrator appends from it.
  - The verifier is a different session from every child. Only PRs the verifier marks
    PASS are merged, and only if the manifest says auto_merge (false by default; soft stop_mode
    may set true by a recorded STOP A decision).
  - Merge authority is the harness (every unit's .migration/recon/<unit>/result.json at the PR head
    says merge_eligible=true) or a human: a batch with a unit whose evidence says otherwise is
    recorded PASS only with merge_authority {kind: human_override, decision_id: D-<n>} where that
    D-<n> row of .migration/06_decisions.md is a human's (user:<id>), names every unit of the batch and
    says merge_override. The result lists every override.
  - Re-running with the same run_id in the pointer replays finished children and only launches
    the rest.

Manifest shape (written by the plan playbook, read here):
{
  "wave": 1,
  "repo": "github.com/acme/dbx-target",
  "child_macro": "!dbx_unit_migration",       # or "!mongo_unit_migration"
  "verify_macro": "!dbx_data_reconciliation", # or "!mongo_reconciliation"
  "width": 20,
  "breaker_threshold": 3,
  "auto_merge": false,
  "child_minutes": 45,                        # soft time limit per child
  "verify_depth": "sampled",                  # optional; verifier Tier 3 depth for the wave:
                                              # sampled (default) | full. Per-batch "verify_depth"
                                              # overrides it (plan sets full on D4/finance-critical).
  "cost_estimate": {                          # optional; STOP C figures from `dbx-recon estimate`
    "source_statements": 240, "target_statements": 96,   # summed over the wave; actuals from
    "source_rows_fetched": 180000, "warehouse_hours": 1.5  # result.json["cost"] land in the brief
  },
  "capabilities": {                           # required; copied from .migration/09_capabilities.json
    "identity": "<migration SP userName>",   # (factory-doctor, ready=true) and compared field by
    "host": "https://<workspace host>",       # field with a doctor run made at launch (written to
    "catalogs": ["mig"],                       # <manifest>.doctor.json). Children run the doctor with
    "guard_mode": "block", "stop_mode": "hard", "ready": true   # --expect-identity and report BLOCKED
  },                                          # on any mismatch. hard stop_mode requires auto_merge=false.
  "source": {"family": "sqlserver",         # optional; the legacy source the doctor's
             "secret": "LEGACY_ODBC",         # source_principal_read_only row checks: engine, env var
             "params": {"db": "loans"}},      # NAME of the DSN, mapping ${params}. Passed to every
  "base_branch": "migration/loan-servicing", # engagement feature branch; PR diffs are taken against it.
  "batches": [
    {"id": "w2-b01", "units": ["orders_load", "orders_dim"],
     "write_targets": ["mig.orders", "mig.orders_dim"],
     "verify_depth": "full",                  # optional per-batch override
     "gates": [                               # required; the acceptance gates STOP C approved for these units
       {"id": "rows", "kind": "row_parity",  # kind: byte_compare | export_file | publish_leg | row_parity |
        "status": "pending", "evidence": ""},  #   structural | custom. status: pending | passed | failed | waived
       {"id": "feed", "kind": "export_file", "status": "waived", "evidence": "",
        "decision_id": "D-14"}                  # waived needs the D-<n> row of 06_decisions.md naming gate and units
     ],
     "brief": "...complete hand-off text for this batch..."}
  ],
  "stop_c": "D-21",                          # required; the 06_decisions.md row that resolved STOP C for this
                                              # wave's plan (per stop_mode). One row launches one run: a rerun after a
                                              # run recorded under it needs STOP C again (a new row, named here).
  "gates_sha": "<sha256>",                   # required; sha256 of the compact sorted JSON {wave, batches: {id:
                                              # {units (sorted), gates: [[id, kind, status, evidence, decision_id
                                              # or null], ...]}}}. The stop_c row must have the cells | D-<n> |
                                              # user:<id> (soft stop_mode: or default-accepted) | STOP C wave-<N>
                                              # gates_sha <value> |; any edit to a
                                              # unit or gate row since (a status flipped by hand included) is a halt.
  "smoke": true                              # only valid for wave 0, width 1, mode smoke
}

A child closes every gate but a waived one by reporting gates: [{id, status: passed, evidence}] where evidence
is a file under .migration/recon/<unit>/ that its PR head carries (checked from git, not the report); the
plan's passed is what STOP C expects, not proof, and a gate not reported is unmet; it cannot waive, rename
or re-kind one. A PASS with any gate not passed (or waived by a ledger row) is recorded FAIL with
failure_class gates, so the wave cannot close over it. A wave gathered by hand (the orchestrator's small-wave
path) spends the STOP C row with `python3 workflow.py reserve` before launching its children (the same run-log
record a workflow start makes, so a second launch under the row halts), then applies the same rule with
`python3 workflow.py gates <results.json>`, where the file is the children's [{batch, pr_url, gates}] reports:
it prints the overlaid gates, exits non-zero on any unmet one, and records the close once every gate is met.

Wave 0 uses the same workflow with `"wave": 0` and `"width": 1` for serial shared objects.
"""

import asyncio
import datetime
import fcntl
import hashlib
import hmac
import json
import re
import shlex
import subprocess
import sys
from collections import Counter
from pathlib import Path

POINTER_REL = Path(".migration/waves/current.json")
MODES = ("start", "resume", "rerun", "smoke")
HOOK_PROBE = re.compile(r"blocked:[0-9a-f]{8}|not-blocked|unknown")
DOCTOR_MAX_AGE = datetime.timedelta(minutes=15)


def find_pointer(start):
    """The sandbox gives this script one thing: its cwd (the session's home directory, not the workspace).
    The orchestrator writes .migration/waves/current.json at or above that cwd; `workspace` inside it
    names the repo when the pointer does not live in it."""
    for d in (start, *start.parents):
        if (d / POINTER_REL).is_file():
            return d / POINTER_REL
    raise SystemExit(f"no {POINTER_REL} at or above {start}; write the orchestrator pointer, then re-run")


POINTER_PATH = find_pointer(Path.cwd().resolve())
try:
    POINTER = json.loads(POINTER_PATH.read_text())
except ValueError as e:
    raise SystemExit(f"{POINTER_PATH} is not valid JSON: {e}") from None
if not isinstance(POINTER, dict) or POINTER.get("mode") not in MODES or not isinstance(POINTER.get("manifest"), str):
    raise SystemExit(f"{POINTER_PATH} must be {{manifest: 'wave-N.json', mode: start|resume|rerun|smoke, run_id, hook_probe, workspace?}}")
MODE = POINTER["mode"]
RUN_ID = POINTER.get("run_id")
if RUN_ID is not None and (not isinstance(RUN_ID, str) or not RUN_ID.strip()):
    raise SystemExit(f"{POINTER_PATH} run_id must be the run_workflow run_id string or null")
if RUN_ID is not None and MODE != "resume":
    raise SystemExit(f"{POINTER_PATH} run_id must be null unless mode is resume: run_workflow reports the run_id only once a fresh run starts; "
                     "record it in <manifest>.run_id afterwards")
HOOK_PROBE_RESULT = POINTER.get("hook_probe")
if not isinstance(HOOK_PROBE_RESULT, str) or not HOOK_PROBE.fullmatch(HOOK_PROBE_RESULT):
    raise SystemExit(f"{POINTER_PATH} hook_probe must be blocked:<nonce>, not-blocked or unknown (the probe run in the "
                     "orchestrator's shell; the doctor was given the same value)")
ROOT = Path(POINTER["workspace"]).resolve() if isinstance(POINTER.get("workspace"), str) else POINTER_PATH.parents[2]
WAVES_DIR = ROOT / ".migration" / "waves"
MANIFEST_PATH = (WAVES_DIR / POINTER["manifest"]).resolve()
if MANIFEST_PATH.suffix != ".json" or MANIFEST_PATH.parent != WAVES_DIR.resolve() or MANIFEST_PATH.name.endswith((".result.json", ".doctor.json")):
    raise SystemExit(f"{POINTER_PATH} manifest must be the plain file name of a wave manifest inside {WAVES_DIR}")
if not MANIFEST_PATH.exists():
    raise SystemExit(f"no wave manifest at {MANIFEST_PATH}; the plan playbook writes it, then re-run")
MANIFEST_BYTES = MANIFEST_PATH.read_bytes()
MANIFEST = json.loads(MANIFEST_BYTES)
BASE_BRANCH = MANIFEST.get("base_branch", "")
MANIFEST_SHA = hashlib.sha256(MANIFEST_BYTES).hexdigest()[:12]
RESULT_PATH = MANIFEST_PATH.with_suffix(".result.json")
RUNS_PATH = MANIFEST_PATH.with_suffix(".runs.jsonl")
BRIEF_PATH = MANIFEST_PATH.with_suffix(".brief.md")
RUN_ID_PATH = MANIFEST_PATH.with_suffix(".run_id")
BASE_SHA_PATH = MANIFEST_PATH.with_suffix(".base_sha")
DOCTOR_PATH = MANIFEST_PATH.with_suffix(".doctor.json")
DECISIONS_PATH = ROOT / ".migration" / "06_decisions.md"
resume = MODE == "resume"
SMOKE = MODE == "smoke"
if SMOKE and not (MANIFEST.get("smoke") is True and MANIFEST.get("wave") == 0 and MANIFEST.get("width") == 1):
    raise SystemExit("mode smoke exercises the runner only: it needs a manifest with smoke: true, wave: 0, width: 1")
if not SMOKE and MANIFEST.get("smoke") is True:
    raise SystemExit("a smoke manifest never runs a real wave")
prior = None

if RESULT_PATH.exists() and MODE != "rerun":
    try:
        prior = json.loads(RESULT_PATH.read_text())
        if not isinstance(prior, dict):
            raise ValueError("result is not a JSON object")
    except ValueError:
        if not resume:
            raise SystemExit(f"{RESULT_PATH} is not valid JSON (interrupted write?). Inspect it; to resume "
                             "the same run set mode: resume with the recorded run_id, or set mode: rerun "
                             "to redo the wave.") from None
    else:
        if prior.get("closed"):
            raise SystemExit(f"{RESULT_PATH} says wave {prior.get('wave')} closed clean. To redo it on "
                             "purpose, set mode: rerun.")
        if not resume:
            raise SystemExit(f"{RESULT_PATH} records a halted or failed run. To continue it, set mode: resume "
                             "with the recorded run_id (finished children replay). To redo the wave from "
                             "scratch, set mode: rerun.")
if resume:
    if not RUN_ID:
        raise SystemExit("mode resume requires run_id in current.json; pass the recorded run_id")
    if not RUN_ID_PATH.exists():
        raise SystemExit(f"no run record at {RUN_ID_PATH}; cannot verify the pointer run_id belongs to this wave "
                         "— set mode: rerun for a fresh run")
    if RUN_ID_PATH.read_text().strip() != RUN_ID:
        raise SystemExit(f"run_id does not match {RUN_ID_PATH}; pass the recorded run_id in current.json, "
                         "or set mode: rerun for a fresh run")
    if isinstance(prior, dict) and prior.get("run_id") and prior["run_id"] != RUN_ID:
        raise SystemExit(f"run_id does not match prior result at {RESULT_PATH}; pass the recorded run_id in "
                         "current.json, or set mode: rerun for a fresh run")

REPLAYED = {
    b["id"]: b for b in (prior or {}).get("batches", [])
    if b.get("status") in ("PASS", "FAIL", "BLOCKED")
} if resume and isinstance(prior, dict) else {}


def prompt_sha(prompt):
    return hashlib.sha256(prompt.encode()).hexdigest()[:16]


# Verifier Tier 3 depth. sampled: Tier 1+2 plus a differently-seeded stratified Tier 3 (catches
# a child that fabricated or misread results at a fraction of the cost). full: keyed full diff,
# for units the plan flags cutover-critical (D4 external feed, finance). Never "threshold":
# the verifier's depth is a plan decision, not a tolerance-file side effect.
VERIFY_DEPTHS = ("sampled", "full")
# Recon modes whose PASS is merge evidence (recon/report.py merge_eligible): both sides real.
# fixture never is; transactional is the operational (Lakebase) track's live run.
MERGE_EVIDENCE_MODES = ("live", "snapshot", "transactional")
# Values the child doctor compares its own findings against (hooks/dbx_guard.py, 00_context.md).
GUARD_MODES = ("block", "warn")
STOP_MODES = ("hard", "soft")
GATE_KINDS = ("byte_compare", "export_file", "publish_leg", "row_parity", "structural", "custom")
GATE_STATUSES = ("pending", "passed", "failed", "waived")
# A unit id is the one directory under .migration/recon/ its child may write, so it is a plain
# name: no separators, no leading dot, and not the verifier's wave-N.
UNIT_ID = re.compile(r"(?!wave-)[A-Za-z0-9_][A-Za-z0-9_.-]*")
# Manifest values that reach a command line (git refs here, the children's doctor flags): one plain
# word, so nothing in them is ever an option, a range or a shell operator.
WORD = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_./-]*")
# source.secret is the NAME of the environment variable holding the DSN, as a POSIX shell can set it.
ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# A source.params value: the one literal recon.cli.PARAM_RE accepts (number, identifier, date, or
# date + time, so one space at most), shell-quoted wherever it is rendered into a command line.
PARAM_VALUE = re.compile(r"[A-Za-z0-9_\-:.T/]+(?: [0-9:.]+)?")
# A PR of this repo, as the host names it; its head is refs/pull/N/head, which only the host writes.
PR_URL = re.compile(r"https://(?P<repo>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/pull/(?P<n>[0-9]+)/?")
# A row of the decision ledger (.migration/06_decisions.md).
DECISION_ID = re.compile(r"D-[0-9]+")
# Its provenance when a human wrote it (`user:<message/event id>`), as the ledger convention names it.
HUMAN_PROVENANCE = re.compile(r"(?<![\w-])user:[\w][\w.@/-]*")
# Its provenance when a soft stop elapsed and the orchestrator recorded the default, e.g. `default-accepted (soft, 60s)`.
DEFAULT_ACCEPTED = re.compile(r"default-accepted(?: ?\([^|]*\))?")
# A cell of a ledger row that is about the row, not about units: an id, a date/time or the provenance.
LEDGER_METADATA = re.compile(rf"D-[0-9]+|\d{{4}}-\d{{2}}-\d{{2}}(?:[T ][\d:.]+Z?(?:[+-]\d{{2}}:?\d{{2}})?)?"
                             rf"|{HUMAN_PROVENANCE.pattern}|{DEFAULT_ACCEPTED.pattern}")


def decision_ledger():
    try:
        return DECISIONS_PATH.read_text()
    except OSError:
        return ""


def ledger_rows(ledger):
    """Each markdown table row of the ledger as (decision id, its cells): the id is the first cell that is
    one alone, so a row that cites another decision in prose or a later column is not that decision's.
    Edge pipes are optional, as in markdown and the guard's ledger reading; a line without a pipe is prose."""
    for line in ledger.splitlines():
        line = line.strip()
        if "|" not in line:
            continue
        cells = [" ".join(c.split()) for c in line.strip("|").split("|")]
        ids = [c for c in cells if DECISION_ID.fullmatch(c)]
        if ids:
            yield ids[0], cells


def override_decision(decision_id, units, ledger, word="merge_override"):
    """Whether the ledger holds the D-<n> row a human wrote for these units: the row whose id cell is that
    id, whose provenance cell is a human's (exactly `user:<id>`, as the STOP C row's must be; a user
    mentioned in the text is not the row's author, and a default-accepted cell makes the row the
    orchestrator's), the deciding word (merge_override past merge_eligible=false; waive for a gate) and the
    id of every unit in the batch, in whatever column order the ledger keeps. Units are looked for in the
    row's text cells only: a cell that is an id, a date or the provenance alone is about the row and is
    skipped, and a provenance token inside a text cell is blanked, so none of those stands in for a unit
    the row did not name, while a unit that happens to be called like one counts when the text names it;
    nor does the deciding word stand in for a unit of that name (the row names it again)."""
    if not isinstance(decision_id, str) or not DECISION_ID.fullmatch(decision_id):
        return False

    def token(w):
        return rf"(?<![A-Za-z0-9_.-]){re.escape(w)}(?![A-Za-z0-9_.-])"

    for row_id, cells in ledger_rows(ledger):
        if (row_id == decision_id and any(HUMAN_PROVENANCE.fullmatch(c) for c in cells)
                and not any(DEFAULT_ACCEPTED.fullmatch(c) for c in cells)):
            text = " | ".join(HUMAN_PROVENANCE.sub(" ", c) for c in cells if not LEDGER_METADATA.fullmatch(c))
            need = Counter((word, *units))
            if all(len(re.findall(token(w), text)) >= n for w, n in need.items()):
                return True
    return False


def ledger_waiver(gate_id, units, ledger, stop_c):
    """The D-<n> of the human row, written after this run's STOP C row, that waives this gate for every unit
    of the batch, or None. The declaration is frozen by gates_sha, so a waiver decided after STOP C is found
    here, not in the manifest; and a run's STOP C row is its own, so a waiver above it was an earlier run's
    and does not carry. No stop_c row in the ledger, no waiver."""
    after = False
    for line in ledger.splitlines():
        if after:
            for decision_id in dict.fromkeys(DECISION_ID.findall(line)):
                if override_decision(decision_id, [gate_id, *units], line, word="waive"):
                    return decision_id
        elif any(row_id == stop_c for row_id, _ in ledger_rows(line)):
            after = True
    return None


def declared_gates_sha(wave, batches):
    """What STOP C approved, whole: the wave, each batch's units and every gate row as declared (id, kind,
    status, evidence, decision_id). Outcomes reach the result through the children's reports, never by
    editing the manifest, so any edit to it after STOP C changes the hash and halts."""
    declared = {"wave": wave, "batches": {
        b["id"]: {"units": sorted(b["units"]),
                  "gates": [[g["id"], g["kind"], g["status"], g["evidence"], g.get("decision_id")] for g in b["gates"]]}
        for b in batches}}
    return hashlib.sha256(json.dumps(declared, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def gates_approved(decision_id, wave, sha, ledger, stop_mode="hard"):
    """Whether the ledger row the manifest names (stop_c) is the STOP C approval of exactly this wave's
    declaration: a markdown table row whose cells include the decision id alone, the provenance alone, and
    `STOP C wave-<N> gates_sha <sha>` alone. STOP C resolves per stop_mode like every stop: the provenance
    is a human's `user:<id>`, or under soft stop_mode the orchestrator's `default-accepted` once the stop
    elapsed. Cells are parsed, not searched, so prose that happens to hold the tokens, another wave's
    approval or another row of the ledger does not approve."""
    if not (isinstance(decision_id, str) and DECISION_ID.fullmatch(decision_id)
            and isinstance(wave, int) and not isinstance(wave, bool)
            and isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{64}", sha)):
        return False
    approval = re.compile(rf"stop c wave-{wave} gates_sha {sha}", re.I)

    def provenance(c):
        return HUMAN_PROVENANCE.fullmatch(c) or (stop_mode == "soft" and DEFAULT_ACCEPTED.fullmatch(c))

    for line in ledger.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [" ".join(c.split()) for c in line.strip("|").split("|")]
        if decision_id in cells and any(provenance(c) for c in cells) and any(approval.fullmatch(c) for c in cells):
            return True
    return False


def validate_gates(b):
    """A unit's acceptance gates are manifest rows STOP C approved, not prose in a PR; every field is checked
    so a missing one fails here rather than closing a wave over it."""
    gates = b.get("gates")
    if not isinstance(gates, list) or not gates or not all(isinstance(g, dict) for g in gates):
        raise SystemExit(f"batch {b['id']} 'gates' must be a non-empty list of {{id, kind, status, evidence, decision_id?}} "
                         "rows: the acceptance gates STOP C approved for its units")
    ids = Counter(g.get("id") for g in gates)
    if any(not isinstance(i, str) or not UNIT_ID.fullmatch(i) for i in ids):
        raise SystemExit(f"batch {b['id']} gate 'id' must be a plain word (letters, digits, _ . -)")
    if any(c > 1 for c in ids.values()):
        raise SystemExit(f"batch {b['id']} gate ids must be unique: {[i for i, c in ids.items() if c > 1]}")
    for g in gates:
        if g.get("kind") not in GATE_KINDS:
            raise SystemExit(f"batch {b['id']} gate {g['id']} 'kind' must be one of {GATE_KINDS}")
        if g.get("status") not in GATE_STATUSES:
            raise SystemExit(f"batch {b['id']} gate {g['id']} 'status' must be one of {GATE_STATUSES}")
        if not isinstance(g.get("evidence"), str) or (g["status"] == "passed" and not g["evidence"]):
            raise SystemExit(f"batch {b['id']} gate {g['id']} 'evidence' must be a string, non-empty once passed")
        decision = g.get("decision_id")
        if (g["status"] == "waived" and decision is None) or (
                decision is not None and not (isinstance(decision, str) and DECISION_ID.fullmatch(decision))):
            raise SystemExit(f"batch {b['id']} gate {g['id']} 'decision_id' must be the D-<n> row of 06_decisions.md "
                             "(required for a waived gate)")


def validate_manifest(m, doctor=None):
    """Fail here, in one line, instead of 20 children failing on a missing field. With the doctor's
    report (.migration/09_capabilities.json) the capability contract must repeat what the doctor
    verified, field by field: a manifest cannot claim an identity, host or allowlist the doctor did
    not see."""
    for key in ("wave", "repo", "child_macro", "verify_macro", "batches", "base_branch"):
        if key not in m:
            raise SystemExit(f"manifest is missing '{key}'")
    if not m["batches"]:
        raise SystemExit("manifest has no batches")
    if (isinstance(m["wave"], bool) or not isinstance(m["wave"], int) or m["wave"] < 0):
        raise SystemExit("manifest key 'wave' must be a non-negative integer")
    for key in ("width", "breaker_threshold", "child_minutes"):
        if key in m and (isinstance(m[key], bool) or not isinstance(m[key], int) or m[key] <= 0):
            raise SystemExit(f"manifest key '{key}' must be a positive integer")
    if m["wave"] == 0 and m.get("width", 20) != 1:
        raise SystemExit("wave 0 is the serial shared-objects wave: set width to 1")
    if not (isinstance(m["base_branch"], str) and WORD.fullmatch(m["base_branch"])
            and ".." not in m["base_branch"]):
        raise SystemExit("manifest 'base_branch' must be a plain branch name (letters, digits, _ . / -)")
    if m["base_branch"] in ("main", "master") and not (
            isinstance(m.get("trunk_base_decision"), str) and m["trunk_base_decision"].strip()):
        raise SystemExit("base_branch 'main' is the trunk: migration ledgers and unit PRs land on the engagement "
                         "feature branch; set base_branch to it, or record the decision in 06_decisions.md and put "
                         "its row reference in 'trunk_base_decision'")
    ids = Counter(b.get("id") for b in m["batches"])
    dupes = [i for i, c in ids.items() if c > 1 or not i]
    if dupes:
        raise SystemExit(f"batch ids must be unique and non-empty: {dupes}")
    owners = {}
    for b in m["batches"]:
        for key in ("units", "write_targets", "brief"):
            if not b.get(key):
                raise SystemExit(f"batch {b['id']} is missing '{key}' (a child with no brief or "
                                 "no declared write targets cannot be launched safely)")
        if "verify_depth" in b and b["verify_depth"] not in VERIFY_DEPTHS:
            raise SystemExit(f"batch {b['id']} 'verify_depth' must be one of {VERIFY_DEPTHS}")
        bad = [u for u in b["units"] if not isinstance(u, str) or not UNIT_ID.fullmatch(u)]
        if bad:
            raise SystemExit(f"batch {b['id']} unit id(s) {bad!r} are not a plain directory name (letters, digits, "
                             "_ . -, not wave-*): the id names the only .migration/recon/<unit_id>/ its child may write")
        for u in b["units"]:
            owners.setdefault(u, []).append(b["id"])
        validate_gates(b)
    shared = {u: bs for u, bs in owners.items() if len(bs) > 1}
    if shared:
        raise SystemExit(f"a unit id belongs to one batch (its child alone writes .migration/recon/<unit_id>/): {shared}")
    if not (isinstance(m.get("stop_c"), str) and DECISION_ID.fullmatch(m["stop_c"])):
        raise SystemExit("manifest 'stop_c' must be the D-<n> row of 06_decisions.md that resolved STOP C for this wave "
                         "(the row that records its gates_sha)")
    want = declared_gates_sha(m["wave"], m["batches"])
    if m.get("gates_sha") != want:
        raise SystemExit(f"manifest 'gates_sha' is {m.get('gates_sha')!r} but the declared gate list hashes to {want}: "
                         "record that value at STOP C with the approved gates; a gate renamed, added, dropped, swapped "
                         "for another kind or given another status or evidence since is a plan change, not a child's "
                         "call, so this run halts")
    src = m.get("source")
    if src is not None and (not isinstance(src, dict) or not isinstance(src.get("params", {}), dict)
                            or not all(isinstance(v, str) and WORD.fullmatch(v) for v in
                                       (src.get("family"), *src.get("params", {}).keys()))
                            or not isinstance(src.get("secret"), str) or not ENV_NAME.fullmatch(src["secret"])
                            or not all(isinstance(v, str) and PARAM_VALUE.fullmatch(v)
                                       for v in src.get("params", {}).values())):
        raise SystemExit("manifest 'source' must be {family, secret (env var NAME of the DSN), params?}: family and "
                         "param names one plain word (letters, digits, _ . / -), secret a shell variable name, param "
                         "values what dbx-recon run --param accepts; they become the doctor's command line")
    if "verify_depth" in m and m["verify_depth"] not in VERIFY_DEPTHS:
        raise SystemExit(f"manifest 'verify_depth' must be one of {VERIFY_DEPTHS}")
    if "cost_estimate" in m and not isinstance(m["cost_estimate"], dict):
        raise SystemExit("manifest 'cost_estimate' must be an object (output of `dbx-recon estimate`, summed over the wave)")
    caps = m.get("capabilities")
    if not isinstance(caps, dict) or not isinstance(caps.get("identity"), str) or not caps["identity"]:
        raise SystemExit("manifest 'capabilities' must be an object with a non-empty 'identity' "
                         "(the migration principal's userName from 09_capabilities.json); no wave "
                         "launches without the factory-doctor contract the children compare against")
    if (not isinstance(caps.get("catalogs"), list) or not caps["catalogs"]
            or not all(isinstance(c, str) and c for c in caps["catalogs"])):
        raise SystemExit("manifest 'capabilities.catalogs' must be the non-empty allowlist of catalog names")
    for key, allowed in (("guard_mode", GUARD_MODES), ("stop_mode", STOP_MODES)):
        if caps.get(key) not in allowed:
            raise SystemExit(f"manifest 'capabilities.{key}' must be one of {allowed}")
    if caps.get("ready") is not True:
        raise SystemExit("manifest 'capabilities.ready' must be true: the factory-doctor preflight "
                         "did not pass; fix the D10 and re-run the doctor before launching a wave")
    if "auto_merge" in m and not isinstance(m["auto_merge"], bool):
        raise SystemExit("manifest 'auto_merge' must be a boolean")
    if caps["stop_mode"] == "hard" and m.get("auto_merge", False):
        raise SystemExit("manifest 'auto_merge' must be false under capabilities.stop_mode 'hard': "
                         "merge authority stays with a human")
    if doctor is None:
        return
    if doctor.get("ready") is not True:
        raise SystemExit(f"the factory-doctor is not ready now ({doctor.get('blocking')}): fix the D10 before a wave")
    ident = doctor.get("identity")
    rows = {c.get("id"): c.get("data") or {} for c in doctor.get("checks", []) if isinstance(c, dict)}
    if not isinstance(ident, dict) or not ident.get("userName") or not ident.get("host"):
        raise SystemExit("09_capabilities.json records no verified identity and host; a wave launches only from a "
                         "doctor report that saw the migration principal")
    recorded = {"identity": ident["userName"], "host": ident["host"],
                "catalogs": sorted(rows.get("allowed_targets", {}).get("catalogs") or []),
                "guard_mode": rows.get("allowed_targets", {}).get("guard_mode"),
                "stop_mode": rows.get("workspace", {}).get("stop_mode")}
    for key, want in recorded.items():
        # the doctor records catalogs under the guard's rule (trimmed, unquoted, case-folded)
        got = sorted(c.strip().strip("`").lower() for c in caps["catalogs"]) if key == "catalogs" else caps.get(key)
        if got != want:
            raise SystemExit(f"manifest 'capabilities.{key}' is {got!r} but the doctor recorded {want!r} in "
                             "09_capabilities.json; copy the doctor's values, never edit them")
    if doctor.get("source") != m.get("source"):
        raise SystemExit("manifest 'source' differs from the source the doctor was signed for; "
                         "re-run the doctor with --wave on this manifest")


def wave_signature(body, manifest_bytes):
    # same canonicalisation as factory-doctor's sign_wave_report: sorted keys, compact separators, HMAC-SHA256
    ident = body.get("identity") or {}
    key = hashlib.sha256(manifest_bytes + str(ident.get("userName") or "").encode()
                         + str(ident.get("host") or "").encode()).digest()
    message = json.dumps({k: v for k, v in body.items() if k != "signature"},
                         sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(key, message, "sha256").hexdigest()


def signed_doctor_report(path, manifest_bytes, now=None):
    """The doctor ran in the orchestrator's shell (credentials live there, not here) and left a record
    signed over these manifest bytes. Missing, stale (>15 min), for another manifest, or not verifying: no wave.
    Tamper-evident only; .migration/ is review-protected. The key is derivable on purpose: this sandbox holds no
    secret to verify one with, so the signature binds manifest, freshness and contents, and the gate against a lying
    orchestrator is each child's own --expect-identity doctor run plus PR review."""
    try:
        report = json.loads(path.read_text())
    except OSError:
        raise SystemExit(f"no doctor record at {path}; run factory-doctor with --wave {path.with_suffix('.json').name} "
                         "in the orchestrator's shell, then re-run") from None
    except ValueError as e:
        raise SystemExit(f"{path} is not valid JSON: {e}") from None
    if not isinstance(report, dict):
        raise SystemExit(f"{path} is not a doctor record")
    if report.get("manifest_sha") != hashlib.sha256(manifest_bytes).hexdigest()[:12]:
        raise SystemExit(f"{path} was signed for another manifest (sha {report.get('manifest_sha')}); re-run the doctor with --wave")
    try:
        signed_at = datetime.datetime.fromisoformat(str(report.get("signed_at")))
    except ValueError:
        raise SystemExit(f"{path} has no valid signed_at") from None
    if signed_at.tzinfo is None:
        raise SystemExit(f"{path} signed_at must carry a UTC offset")
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if not (datetime.timedelta(0) <= now - signed_at <= DOCTOR_MAX_AGE):
        raise SystemExit(f"{path} was signed at {report['signed_at']}, more than {DOCTOR_MAX_AGE} ago (or in the future); "
                         "re-run the doctor with --wave so the wave launches from a current preflight")
    if not isinstance(report.get("signature"), str) or not hmac.compare_digest(
            report["signature"], wave_signature(report, manifest_bytes)):
        raise SystemExit(f"{path} signature does not verify: the record was edited after the doctor wrote it; re-run the doctor")
    if report.get("hook_probe") != HOOK_PROBE_RESULT:
        raise SystemExit(f"{path} was signed for hook_probe {report.get('hook_probe')!r} but current.json says "
                         f"{HOOK_PROBE_RESULT!r}; give the doctor and the pointer the same probe result")
    return report


def _base_tip():
    git = ["git", "-C", str(ROOT)]
    subprocess.run(git + ["fetch", "-q", "origin", f"+refs/heads/{BASE_BRANCH}:refs/remotes/origin/{BASE_BRANCH}"],
                   check=True, capture_output=True, timeout=300)
    return subprocess.run(git + ["rev-parse", "--verify", f"origin/{BASE_BRANCH}^{{commit}}"],
                          check=True, capture_output=True, text=True, timeout=300).stdout.strip()


def wave_base():
    """The base branch's commit on origin, now."""
    try:
        return _base_tip()
    except (OSError, subprocess.SubprocessError) as e:
        raise SystemExit(f"cannot resolve origin/{BASE_BRANCH} in {ROOT} ({e}); the ledger gate needs the base commit")


def launch_base():
    """The base commit when this run launched, persisted beside the manifest before the doctor, any child
    or the verifier runs; a resume reuses it. It anchors the ledger diff of a head the base already
    contains (the verifier merges PRs into the base during the wave) and is what the verifier's tree of a
    unit it did not merge is held to. The run may have stopped after the verifier merged and before any
    result was written, so a base re-read on resume would already contain the heads to be diffed."""
    if resume:
        try:
            sha = BASE_SHA_PATH.read_text().strip()
        except OSError:
            raise SystemExit(f"no launch base at {BASE_SHA_PATH}; the ledger gate cannot resume without it, "
                             "set mode: rerun for a fresh run") from None
        if not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise SystemExit(f"{BASE_SHA_PATH} does not hold a commit sha; set mode: rerun for a fresh run")
        return sha
    sha = wave_base()
    tmp = BASE_SHA_PATH.with_suffix(".base_sha.tmp")
    tmp.write_text(sha + "\n")
    tmp.replace(BASE_SHA_PATH)
    return sha


def evidence_in_pr(head, path, units):
    """Whether a child's gate evidence is a file the gated PR head really carries under one of its own units'
    .migration/recon/<unit>/ directories. A path elsewhere, a claim in prose, or a file git cannot show at
    that head is no evidence; the head is the one the workflow fetched from refs/pull/N/head, never a name
    the child reported."""
    if not (isinstance(head, str) and isinstance(path, str)):
        return False
    parts = path.split("/")
    if (len(parts) < 4 or parts[:2] != [".migration", "recon"] or parts[2] not in units
            or any(p in ("", ".", "..") for p in parts[3:])):
        return False
    try:
        kind = subprocess.run(["git", "-C", str(ROOT), "cat-file", "-t", f"{head}:{path}"],
                              check=True, capture_output=True, text=True, timeout=300).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return False
    return kind == "blob"


def pr_head(pr_url):
    """The head the host holds for a PR of this repo (refs/pull/N/head), fetched now; None when the URL is not
    such a PR or git cannot answer."""
    m = PR_URL.fullmatch(pr_url) if isinstance(pr_url, str) else None
    if not m or m["repo"].lower() != MANIFEST["repo"].lower():
        return None
    git = ["git", "-C", str(ROOT)]
    try:
        subprocess.run(git + ["fetch", "-q", "origin", f"refs/pull/{m['n']}/head"], check=True, capture_output=True, timeout=300)
        return subprocess.run(git + ["rev-parse", "--verify", "FETCH_HEAD^{commit}"],
                              check=True, capture_output=True, text=True, timeout=300).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def gate_outcomes(batch, reported, ledger, head):
    """The batch's gates after the child's report: every gate but a waived one takes the child's passed (with
    evidence the gated PR head carries under the unit's recon dir) or failed; the plan's status is what
    STOP C expects, never proof, so a gate the child did not report is unmet. A waived gate is the ledger's
    and stays; a gate the child did not prove is waived if a human's row written after this run's STOP C
    row waives it for every unit. Returns the gates and what keeps the unit from closing: any gate not proven passed, or
    waived without its ledger row naming gate and units."""
    declared = {g["id"]: {**g, "decision_id": g.get("decision_id")} for g in batch.get("gates", [])}
    unmet = []
    if reported is None:
        reported = []
    if not isinstance(reported, list) or not all(isinstance(r, dict) for r in reported):
        unmet.append("reported gates are not a list of {id, status, evidence} rows")
        reported = []
    seen = Counter(r.get("id") for r in reported)
    for r in reported:
        gid, g = r.get("id"), declared.get(r.get("id"))
        if g is None:
            unmet.append(f"gate {gid!r} reported but not declared for {batch['id']}")
        elif (seen[gid] > 1 or set(r) - {"id", "status", "evidence"} or r.get("status") not in ("passed", "failed")
              or not isinstance(r.get("evidence"), str) or (r["status"] == "passed" and not r["evidence"])):
            unmet.append(f"gate {gid} report must be one {{id, status: passed|failed, evidence}} row, evidence "
                         "non-empty when passed")
        elif g["status"] == "waived":
            unmet.append(f"gate {gid} is waived in the plan; a child cannot change it")
        elif r["status"] == "passed" and not evidence_in_pr(head, r["evidence"], batch["units"]):
            unmet.append(f"gate {gid} evidence {r['evidence']!r} is not a file under .migration/recon/<unit>/ of "
                         f"{', '.join(batch['units'])} at the gated PR head")
        else:
            g.update(status=r["status"], evidence=r["evidence"])
    for g in declared.values():
        if g["status"] != "waived" and (not seen[g["id"]] or g["status"] == "failed"):
            waiver = ledger_waiver(g["id"], batch["units"], ledger, MANIFEST["stop_c"])
            if waiver:
                g.update(status="waived", decision_id=waiver)
                continue
        if g["status"] != "waived" and not seen[g["id"]]:
            unmet.append(f"gate {g['id']} ({g['kind']}) has no child result; the plan's {g['status']} is not proof")
        elif g["status"] == "waived":
            if not override_decision(g["decision_id"], [g["id"], *batch["units"]], ledger, word="waive"):
                unmet.append(f"gate {g['id']} waived by {g['decision_id']} but no such row naming the gate and "
                             f"{', '.join(batch['units'])} is in .migration/06_decisions.md")
        elif g["status"] != "passed":
            unmet.append(f"gate {g['id']} ({g['kind']}) is {g['status']}")
    return list(declared.values()), unmet


def gates_command(path):
    """`workflow.py gates <results.json>`: the wave-close gate rule for a wave the orchestrator gathered by
    hand. The file holds the children's [{batch, pr_url, gates}] reports; every manifest batch must be in it
    once, and passed evidence is checked at each PR's head as the workflow path does. Prints {closed, batches:
    {id: {gates, unmet}}} and exits 1 unless every gate is passed or waived."""
    try:
        reports = json.loads(Path(path).read_text())
    except (OSError, ValueError) as e:
        raise SystemExit(f"{path}: cannot read the children's results: {e}") from None
    if not isinstance(reports, list) or not all(isinstance(r, dict) and isinstance(r.get("batch"), str) for r in reports):
        raise SystemExit(f"{path} must be a list of {{batch, pr_url, gates}} rows, one per child")
    if not all(isinstance(r.get("pr_url"), str) and PR_URL.fullmatch(r["pr_url"]) for r in reports):
        raise SystemExit(f"{path}: every row needs the child's pr_url (a PR of {MANIFEST['repo']}); gate evidence is read at its head")
    by_batch = Counter(r["batch"] for r in reports)
    unknown = sorted(set(by_batch) - {b["id"] for b in MANIFEST["batches"]})
    if unknown or any(c > 1 for c in by_batch.values()):
        raise SystemExit(f"{path}: batches not in the manifest {unknown}, reported twice "
                         f"{sorted(b for b, c in by_batch.items() if c > 1)}")
    ledger = decision_ledger()
    out = {}
    for b in sorted(MANIFEST["batches"], key=lambda b: b["id"]):
        report = next((r for r in reports if r["batch"] == b["id"]), None)
        if report is None:
            gates, unmet = [dict(g, decision_id=g.get("decision_id")) for g in b["gates"]], [f"batch {b['id']} was not gathered"]
        else:
            head = pr_head(report["pr_url"])
            gates, unmet = gate_outcomes(b, report.get("gates"), ledger, head)
            if head is None:
                unmet.append(f"{report['pr_url']} is not a PR of {MANIFEST['repo']} whose head git can fetch; no evidence stands")
        out[b["id"]] = {"gates": gates, "unmet": unmet}
    closed = not any(v["unmet"] for v in out.values())
    print(json.dumps({"wave": MANIFEST["wave"], "closed": closed, "batches": out}, indent=2, sort_keys=True))
    return 0 if closed else 1


STOP_MODE = MANIFEST.get("capabilities", {}).get("stop_mode") if isinstance(MANIFEST.get("capabilities"), dict) else None
if not gates_approved(MANIFEST.get("stop_c"), MANIFEST.get("wave"), MANIFEST.get("gates_sha"), decision_ledger(), STOP_MODE):
    raise SystemExit(f"manifest 'gates_sha' {MANIFEST.get('gates_sha')!r} is not approved by row {MANIFEST.get('stop_c')!r} of "
                     f"{DECISIONS_PATH} (a table row with cells | D-<n> | user:<id>"
                     f"{' or default-accepted' if STOP_MODE == 'soft' else ''} | STOP C wave-{MANIFEST.get('wave')} "
                     "gates_sha <value> |); the manifest cannot approve its own gate list, so this run halts until STOP C "
                     "records it")


def run_log():
    """This wave's append-only run log: one {stop_c, mode, run_id} record per launch (a workflow start or
    rerun, a hand run's `reserve`) and per hand-run close (`gates`). A log that cannot be read is not proof
    a row is unspent and halts."""
    runs = []
    if RUNS_PATH.exists():
        try:
            for n, line in enumerate(RUNS_PATH.read_text().splitlines(), 1):
                run = json.loads(line) if line.strip() else None
                if run is not None and not (isinstance(run, dict) and isinstance(run.get("stop_c"), str)):
                    raise ValueError(f"line {n} is not a {{stop_c, ...}} record")
                if run is not None:
                    runs.append(run)
        except (OSError, ValueError) as e:
            raise SystemExit(f"{RUNS_PATH} cannot say which STOP C rows this wave's runs spent ({e}); inspect or restore "
                             "it before running, no approval is reusable on its word") from None
    return runs


def spent_stop_c():
    """The STOP C rows this wave's runs have spent, from its run log and the last result. The result holds
    one run, so the log is what remembers a row two reruns back."""
    spent = [run["stop_c"] for run in run_log()]
    if MODE == "rerun" and RESULT_PATH.exists():
        try:
            previous = json.loads(RESULT_PATH.read_text())
        except (OSError, ValueError) as e:
            previous = e
        if not isinstance(previous, dict):
            raise SystemExit(f"{RESULT_PATH} cannot say which STOP C row the wave's last run spent ({previous!r}); inspect "
                             "or restore it before rerunning, the old approval is not reusable on its word")
        spent.append(previous.get("stop_c"))
    return spent


def spent_halt():
    return SystemExit(f"{RUNS_PATH} or {RESULT_PATH} records a run this wave already made under STOP C row "
                      f"{MANIFEST['stop_c']}; a rerun is a new run of the wave, so STOP C fires again: record its new row in "
                      "the ledger and name it in the manifest's stop_c")


def record_run(mode, run_id=None, unspent=True):
    """Appends {stop_c, mode, run_id} to the run log under its exclusive lock, and when `unspent`, only if no
    record read under that same lock already spent the manifest's STOP C row: two starts that both saw the
    row free cannot both take it, since the second reads the first's record once the lock is its."""
    with RUNS_PATH.open("a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        if unspent and MANIFEST["stop_c"] in spent_stop_c():
            raise spent_halt()
        f.write(json.dumps({"stop_c": MANIFEST["stop_c"], "mode": mode, "run_id": run_id}, sort_keys=True) + "\n")
        f.flush()


def hand_run_state():
    """What the run log says of a hand run under the manifest's STOP C row: 'open' after `reserve`, 'closed'
    once `gates` closed it, 'workflow' when a workflow run spent the row, None when nothing did."""
    state = None
    for run in run_log():
        if run["stop_c"] == MANIFEST["stop_c"]:
            state = {"reserve": "open", "gates": "closed"}.get(run.get("mode"), "workflow")
    return state


if sys.argv[1:2] == ["reserve"]:
    validate_manifest(MANIFEST)
    record_run("reserve")
    print(json.dumps({"wave": MANIFEST["wave"], "stop_c": MANIFEST["stop_c"], "reserved": True}))
    sys.exit(0)
if sys.argv[1:2] == ["gates"]:
    validate_manifest(MANIFEST)
    state = hand_run_state()
    if state == "closed":
        raise SystemExit(f"{RUNS_PATH} records that the hand run under STOP C row {MANIFEST['stop_c']} closed already; a new "
                         "launch is a new run of the wave, so STOP C fires again: record its new row and name it in the "
                         "manifest's stop_c, then `workflow.py reserve` before launching")
    if state == "workflow":
        raise spent_halt()
    if state is None:
        raise SystemExit(f"{RUNS_PATH} holds no reservation of STOP C row {MANIFEST['stop_c']}: run `python3 "
                         "skills/migration-fanout/workflow.py reserve` before launching the children by hand, so the row "
                         "is spent by that launch and no second launch reuses it")
    if len(sys.argv) != 3:
        sys.exit("usage: workflow.py gates <results.json>")
    code = gates_command(sys.argv[2])
    if code == 0:
        record_run("gates", unspent=False)
    sys.exit(code)
if not resume and not SMOKE and MANIFEST.get("stop_c") in spent_stop_c():
    raise spent_halt()
validate_manifest(MANIFEST)
BASE_SHA = launch_base()
DOCTOR = signed_doctor_report(DOCTOR_PATH, MANIFEST_BYTES)
if not SMOKE:
    validate_manifest(MANIFEST, DOCTOR)


def _git_paths(*args):
    r = subprocess.run(["git", "-C", str(ROOT), "diff", "--name-only", "--no-renames", *args],
                       check=True, capture_output=True, text=True, timeout=300)
    return r.stdout.split()


def ref_changed_paths(ref):
    """(head sha, paths) a ref on origin changes, from git: from its fork point on the base as it is now
    (a child launched on a resume forked from a base the verifier had merged accepted units into; those
    are not its diff), or from the launch base when the base already contains the head (it would be its
    own merge base and diff to nothing). None when git cannot answer, and then no PASS stands. Callers
    pass refs the workflow built itself, never a name a child reported. Renames are reported as delete +
    add so a ledger file moved under recon/ still names its old path."""
    git = ["git", "-C", str(ROOT)]
    try:
        subprocess.run(git + ["fetch", "-q", "origin", ref], check=True, capture_output=True, timeout=300)
        head = subprocess.run(git + ["rev-parse", "--verify", "FETCH_HEAD^{commit}"],
                              check=True, capture_output=True, text=True, timeout=300).stdout.strip()
        tip = _base_tip()
        merged = subprocess.run(git + ["merge-base", "--is-ancestor", head, tip],
                                check=False, capture_output=True, timeout=300).returncode
        if merged not in (0, 1):
            raise subprocess.SubprocessError(f"merge-base rc={merged}")
        return head, _git_paths(f"{BASE_SHA if merged == 0 else tip}...{head}")
    except (OSError, subprocess.SubprocessError):
        return None


def unit_eligibility(head, units):
    """{unit: merge_eligible} from each unit's own .migration/recon/<unit>/result.json at the gated PR
    head. A batch reports one boolean for up to five units, so the per-unit evidence decides; a file
    git cannot show, or one without a boolean merge_eligible, is None (not eligible)."""
    out = {}
    for u in units:
        try:
            text = subprocess.run(["git", "-C", str(ROOT), "show", f"{head}:.migration/recon/{u}/result.json"],
                                  check=True, capture_output=True, text=True, timeout=300).stdout
            got = json.loads(text).get("merge_eligible")
        except (OSError, subprocess.SubprocessError, ValueError, AttributeError):
            got = None
        out[u] = got if isinstance(got, bool) else None
    return out


def pr_changed_paths(pr_url):
    """What the PR really changes: the head the host holds for that PR of this repo (refs/pull/N/head),
    so the branch name in the child's report never selects what is inspected."""
    m = PR_URL.fullmatch(pr_url) if isinstance(pr_url, str) else None
    if not m or m["repo"].lower() != REPO.lower():
        return None
    return ref_changed_paths(f"refs/pull/{m['n']}/head")


def replay_gate(record, pr_url):
    """The gate a replayed PASS keeps: its recorded head, while the PR still points at it or the base
    already contains it (the resumed run's verifier merged it; its diff now would attribute other
    accepted units to it). A PR URL names no tree: a PR that gained commits since is gated at its current
    head like a new child's. None when git cannot answer."""
    got = pr_changed_paths(pr_url)
    if got is None or got[0] == record["pr_head"]:
        return got and (got[0], [])
    try:
        merged = subprocess.run(["git", "-C", str(ROOT), "merge-base", "--is-ancestor", record["pr_head"], _base_tip()],
                                check=False, capture_output=True, timeout=300).returncode
    except (OSError, subprocess.SubprocessError):
        return None
    return (record["pr_head"], []) if merged == 0 else got if merged == 1 else None


def verifier_changed_paths(wave, passed):
    """What the verifier itself changed on recon/wave-N. A passed unit's evidence drops out only where
    the verifier's tree is byte-identical to the gated PR head's (it merged that head) or to the launch
    base's (it did not touch the unit: auto_merge off), so a result.json it rewrote (or a head re-pushed
    after the gate) stays and fails the wave. None when git cannot answer, or a passed batch has no
    gated head."""
    got = ref_changed_paths(f"recon/wave-{wave}")
    if got is None or not all(isinstance(p.get("pr_head"), str) for p in passed):
        return None
    head, paths = got
    dirs = {p["pr_head"]: [f".migration/recon/{u}/" for u in p["units"]] for p in passed}
    own = {p for p in paths if not p.startswith(tuple(d for ds in dirs.values() for d in ds))}
    try:
        for pr_head, ds in dirs.items():
            own.update(set(_git_paths(pr_head, head, "--", *ds)) & set(_git_paths(BASE_SHA, head, "--", *ds)))
    except (OSError, subprocess.SubprocessError):
        return None
    return sorted(own)


def ledger_violations(changed_paths, unit_ids, wave=None) -> list[str]:
    """Paths under .migration/ that a child (recon evidence for its own units) or the verifier (the
    wave report) may not have changed. Everything else under .migration/ is the ledger, written only
    by the workflow and the humans it stops for."""
    allowed = tuple(f".migration/recon/{u}/" for u in unit_ids)
    if wave is not None:
        allowed += (f".migration/recon/wave-{wave}/",)
    return [p for p in changed_paths
            if p.startswith(".migration/") and not p.startswith(allowed)]


def validate_verify(verify, passed, auto_merge, wave=None, observed=None) -> list[str]:
    """Return verifier-output problems without reading files or mutating input. `observed` is what git
    says the verifier itself changed on recon/wave-N (None: it could not be fetched or diffed)."""
    problems = []
    if not isinstance(verify, dict):
        return ["verifier output invalid: expected an object"]
    expected = {p.get("batch") for p in passed}
    if None in expected:
        problems.append("verifier output invalid: passed batch is missing its id")
        expected.discard(None)
    verdicts = verify.get("unit_verdicts")
    if not isinstance(verdicts, dict):
        problems.append("verifier output invalid: unit_verdicts must be a dict")
        verdicts = {}
    missing = sorted(expected - set(verdicts))
    extra = sorted(set(verdicts) - expected)
    if missing:
        problems.append("verifier output invalid: missing verdicts for " + ", ".join(missing))
    if extra:
        problems.append("verifier output invalid: unexpected verdicts for " + ", ".join(extra))
    wave_verdict = verify.get("wave_verdict")
    if wave_verdict not in ("PASS", "FAIL"):
        problems.append("verifier output invalid: wave_verdict must be PASS or FAIL")
    for batch in sorted(expected):
        verdict = verdicts.get(batch)
        if verdict not in ("PASS", "FAIL"):
            problems.append(f"verifier output invalid: verdict for {batch} is {verdict!r}")
        elif wave_verdict == "PASS" and verdict != "PASS":
            problems.append(f"verifier output invalid: wave PASS contradicts {batch}={verdict}")
    if wave_verdict == "FAIL" and expected and all(verdicts.get(b) == "PASS" for b in expected):
        problems.append("verifier output invalid: wave FAIL contradicts all unit verdicts PASS")
    merged = verify.get("merged_prs")
    if merged is not None and not isinstance(merged, list):
        problems.append("verifier output invalid: merged_prs must be a list")
        merged = []
    if auto_merge:
        if merged is None:
            problems.append("verifier output invalid: merged_prs must be a list when auto_merge is on")
            merged = []
        for batch in passed:
            url = batch.get("pr_url")
            if url and url not in merged:
                problems.append(f"verifier output invalid: merged_prs is missing {url} for {batch['batch']}")
    if not isinstance(verify.get("findings"), list):
        problems.append("verifier output invalid: findings must be a list")
    changed = verify.get("changed_paths")
    if not isinstance(changed, list) or not all(isinstance(p, str) for p in changed):
        problems.append("verifier output invalid: changed_paths must be a list of paths (git diff --name-only)")
        changed = []
    if wave is not None and observed is None:
        problems.append(f"verifier output invalid: branch recon/wave-{wave} not verifiable from git (fetch or diff "
                        "failed), ledger integrity unverified")
    problems += [f"verifier output invalid: ledger tampered, changed {p}"
                 for p in ledger_violations(sorted({*changed, *(observed or [])}), [], wave)]
    return problems

WAVE = MANIFEST["wave"]
REPO = MANIFEST["repo"]
BATCHES = sorted(MANIFEST["batches"], key=lambda b: b["id"])
WIDTH = int(MANIFEST.get("width", 20))
BREAKER = int(MANIFEST.get("breaker_threshold", 3))
AUTO_MERGE = bool(MANIFEST.get("auto_merge", False))
CHILD_MINUTES = int(MANIFEST.get("child_minutes", 45))
VERIFY_DEPTH = MANIFEST.get("verify_depth", "sampled")


def batch_verify_depth(batch) -> str:
    return batch.get("verify_depth", VERIFY_DEPTH)

META = {
    "name": f"smoke-wave-{WAVE}" if SMOKE else f"migration-wave-{WAVE}",
    "description": f"Wave {WAVE}: {len(BATCHES)} unit batches in parallel, then one independent verifier",
    "phases": [
        {"title": "migrate", "detail": "one child per batch: convert, load, recon, open PR",
         "labels": [b["id"] for b in BATCHES], "soft_time_limit_minutes": CHILD_MINUTES},
        {"title": "verify", "detail": "independent recon over the wave, merge green PRs",
         "count": 1, "soft_time_limit_minutes": 60},
    ],
}

CHILD_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["PASS", "FAIL", "BLOCKED"]},
        "pr_url": {"type": "string"},
        "branch": {"type": "string"},
        "recon_verdict": {"type": "string", "enum": ["PASS", "FAIL", "NOT_RUN"]},
        "recon_mode": {"type": "string", "description": "recon --mode of the evidence run (fixture never merges)"},
        "merge_eligible": {"type": "boolean", "description": "result.json['merge_eligible'] of the evidence run"},
        "merge_authority": {
            "type": "object",
            "properties": {"kind": {"type": "string", "enum": ["harness", "human_override"]},
                           "decision_id": {"type": "string"}},
            "description": "human_override with the D-<n> row of .migration/06_decisions.md that says merge_override "
                           "for your units; the workflow verifies the row. harness otherwise."},
        "failure_class": {"type": "string"},
        "write_targets": {"type": "array", "items": {"type": "string"}},
        "changed_paths": {"type": "array", "items": {"type": "string"},
                          "description": "every path the PR changes: git diff --name-only <base>...<head>"},
        "skill_feedback": {"type": "array", "items": {"type": "string"}},
        "gates": {
            "type": "array",
            "items": {"type": "object",
                      "properties": {"id": {"type": "string"},
                                     "status": {"type": "string", "enum": ["passed", "failed"]},
                                     "evidence": {"type": "string"}},
                      "required": ["id", "status", "evidence"]},
            "description": "outcome of each gate declared in your brief, by id; passed needs the evidence path"},
        "recon_cost": {"type": "object",
                       "description": "result.json['cost'] of the final live/snapshot/transactional run"},
        "one_line_summary": {"type": "string"},
    },
    "required": ["status", "recon_verdict", "recon_mode", "merge_eligible", "write_targets", "changed_paths",
                 "one_line_summary"],
}

VERIFY_SCHEMA = {
    "type": "object",
    "properties": {
        "wave_verdict": {"type": "string", "enum": ["PASS", "FAIL"]},
        "unit_verdicts": {"type": "object"},
        "merged_prs": {"type": "array", "items": {"type": "string"}},
        "findings": {"type": "array", "items": {"type": "string"}},
        "report_path": {"type": "string"},
        "changed_paths": {"type": "array", "items": {"type": "string"},
                          "description": "every path your report branch changes: git diff --name-only <base>...<head>"},
        "recon_cost": {"type": "object",
                       "description": "summed result.json['cost'] over the verifier's re-runs"},
    },
    "required": ["wave_verdict", "unit_verdicts", "findings", "changed_paths"],
}


def check_write_targets(batches):
    """Two batches writing the same table or collection means the lineage missed an edge.
    Refuse to launch anything; the plan must be fixed first."""
    owners = {}
    for b in batches:
        for t in b.get("write_targets", []):
            if t in owners:
                raise SystemExit(f"write-target collision before launch: '{t}' is claimed by "
                                 f"{owners[t]} and {b['id']}. Fix the wave plan, then re-run.")
            owners[t] = b["id"]


def child_prompt(batch):
    return (
        f"You are one fan-out child in wave {WAVE} of a migration. Repo: {REPO}.\n"
        f"Run the playbook {MANIFEST['child_macro']} for batch {batch['id']} exactly as written.\n\n"
        f"BATCH BRIEF (your complete hand-off; if anything is missing, report status=BLOCKED "
        f"with the missing item in one_line_summary, do not improvise):\n"
        f"{batch['brief']}\n\n"
        f"Units: {json.dumps(batch['units'], sort_keys=True)}\n"
        f"Write targets you own (never write anywhere else): "
        f"{json.dumps(batch.get('write_targets', []), sort_keys=True)}\n"
        f"Acceptance gates STOP C declared for these units (report each by id in gates as passed with the "
        f"evidence path, or failed; one you do not report fails the unit, the plan's status is what is expected, "
        f"not proof; a waived gate is the ledger's, not yours, and is not listed; never rename or re-kind a gate): "
        f"{json.dumps([g for g in batch.get('gates', []) if g['status'] != 'waived'], sort_keys=True)}\n\n"
        + capability_block(batch["units"])
        + "Rules that override anything else:\n"
        "- Do not edit files under .migration/ except your own recon evidence under "
        ".migration/recon/<unit_id>/. The workflow writes the ledger from your report.\n"
        "- Report every path your PR changes in changed_paths (`git diff --name-only <base>...<head>`); "
        "any other .migration/ path in it turns your PASS into FAIL ledger_tampered.\n"
        "- Do not merge your own PR.\n"
        f"- status=PASS requires a recon PASS in one of {list(MERGE_EVIDENCE_MODES)} (result.json "
        "merge_eligible=true; transactional is the mode for Lakebase/operational units). Fixture "
        "evidence is never PASS. Report merge_eligible=true only when every unit's "
        ".migration/recon/<unit>/result.json in your PR says so; the workflow reads each file. If any is "
        "false and a human recorded a merge_override row for exactly your units in .migration/06_decisions.md, "
        "report merge_authority {kind: human_override, decision_id: D-<n>}; the workflow checks the row and "
        "fails the unit if it is missing. Never write that row yourself.\n"
        "- If the recon harness fails 3 full runs, stop and report status=FAIL with a short "
        "failure_class (for example 'timestamp_precision', 'decimal_rounding', 'missing_rule').\n"
        "- Report every rule you had to derive yourself in skill_feedback.\n"
        "- Copy result.json['cost'] of your final merge-evidence run into recon_cost; the wave brief "
        "compares it with the STOP C estimate.\n"
        "- one_line_summary is for a human skimming 20 of these: what landed, or why not."
    )


def capability_block(units):
    caps, src = MANIFEST["capabilities"], MANIFEST.get("source") or {}
    unit_flags = " ".join(f"--unit {u}" for u in units)
    source_flags = " ".join([f"--source-family {src['family']} --source-secret {src['secret']}"]
                            + [f"--param {shlex.quote(f'{k}={v}')}" for k, v in src.get("params", {}).items()]) if src else ""
    return (
        "CAPABILITY CONTRACT (from the orchestrator's factory-doctor run): "
        f"{json.dumps(caps, sort_keys=True)}\n"
        f"Before converting anything run the factory-doctor skill with --role child "
        f"--expect-identity {caps['identity']} {unit_flags} (exactly this batch; the doctor resolves "
        "and verifies every unit's .migration/units/<unit_id>/mapping_spec.json itself), "
        f"--expect-host {shlex.quote(caps['host'])} (the workspace the contract pins; the same principal "
        "resolved against another workspace is a fail), "
        + (f"{source_flags} (the source the doctor checks for write access; the same secret your recon "
           "gate passes as --source-dsn-secret)" if source_flags else
           "--source-secret naming the secret your recon gate passes as --source-dsn-secret, and the "
           "same --param values the gate will get")
        + "; then complete its hook probe. Any 'fail' row "
        "(identity mismatch, harness missing, hooks not applied, allowlist differs from the "
        "contract, a unit's mapping missing, declared delete evidence not readable on the source) "
        "means status=BLOCKED with the check id in one_line_summary. Never continue as a different "
        "identity, never run `databricks auth login`, never edit .migration/allowed_targets.json.\n\n"
    )


def verify_prompt(passed, auto_merge):
    by_id = {b["id"]: b for b in BATCHES}
    depths = {p["batch"]: batch_verify_depth(by_id[p["batch"]]) for p in passed}
    merge_line = (
        "Merge every PR you mark PASS and list it in merged_prs, even if another unit in the wave failed; "
        "failed units are reopened next launch."
        if auto_merge else
        "Do not merge anything; return per-unit verdicts. The orchestrator surfaces the PASS PRs in the "
        "wave brief, merges them at wave close (or records the human's decision in the kit's decision log "
        "under .migration/), "
        "and the next wave does not launch until that is done.")
    return (
        f"You are the independent verifier for wave {WAVE}. Repo: {REPO}. You did not write "
        f"any of this code.\nRun the playbook {MANIFEST['verify_macro']} exactly as written over "
        f"these batches:\n{json.dumps(passed, sort_keys=True, indent=1)}\n\n"
        "Re-run the recon harness yourself. Do not trust the PR's pasted evidence, and run it with "
        "03_recon_tolerances.json and allowed_targets.json from the base branch, not the PR (a child that "
        "loosened a tolerance must fail here). For each PR run `git diff --name-only <base>...<head>`: any "
        ".migration/ path outside .migration/recon/<unit_id>/ is a FAIL for that unit with finding "
        "ledger_tampered. "
        f"Mark a unit PASS only if you re-ran the harness in one of {list(MERGE_EVIDENCE_MODES)} "
        "(the same mode the child used: transactional for Lakebase/operational units) and result.json "
        "says merge_eligible=true. A batch listed with merge_authority kind human_override was cleared by the "
        "named D-<n> merge_override row of .migration/06_decisions.md: mark it PASS on a PASS verdict even if "
        "merge_eligible is false, and cite the decision id in findings. "
        f"Run with `--depth <d>` per batch, exactly as listed here: {json.dumps(depths, sort_keys=True)} "
        "(sampled = Tier 1+2 plus a stratified Tier 3 with a seed different from the child's; full = keyed "
        "full diff). Never lower a batch's depth; raising it is allowed and noted in findings. Each batch lists "
        "its acceptance gates with the evidence the child gave; open the evidence of every passed gate and FAIL "
        "the unit if it does not show what the gate's kind requires. "
        "Sum result.json['cost'] over your runs into recon_cost.\n"
        f"{merge_line}\nWrite the wave recon report to .migration/recon/wave-{WAVE}/report.md, "
        f"commit it on branch recon/wave-{WAVE}, push, and give '<branch>:<path>' in "
        "report_path. Do not edit any other file under .migration/; report your branch's "
        "`git diff --name-only <base>...<head>` in changed_paths. Each finding is one plain "
        "sentence a lead can read without opening anything."
    )


class Breaker:
    def __init__(self, threshold):
        self.threshold = threshold
        self.classes = Counter()
        self.tripped_on = None

    def record(self, failure_class):
        if not failure_class:
            return
        self.classes[failure_class] += 1
        if self.classes[failure_class] >= self.threshold and not self.tripped_on:
            self.tripped_on = failure_class
            log(f"CIRCUIT BREAKER: {self.threshold} children failed with '{failure_class}'. "
                "No new children will launch this run.")


async def run_batch(batch, sem, breaker):
    async with sem:
        if breaker.tripped_on:
            return {"status": "NOT_LAUNCHED", "recon_verdict": "NOT_RUN",
                    "one_line_summary": f"held back: breaker tripped on '{breaker.tripped_on}'"}
        log(f"launch {batch['id']} ({len(batch['units'])} units)")
        prompt = child_prompt(batch)
        try:
            out = await agent(prompt, phase="migrate", schema=CHILD_SCHEMA,
                              label=batch["id"], repos=[REPO])
        except WorkflowAgentError as e:
            out = {"status": "FAIL", "recon_verdict": "NOT_RUN", "failure_class": "session_died",
                   "one_line_summary": f"child session died: {e}"}
        out["prompt_sha"] = prompt_sha(prompt)
        if (out["status"] == "PASS"
                and (out["recon_verdict"] != "PASS"
                     or out.get("recon_mode") not in MERGE_EVIDENCE_MODES)):
            out["status"] = "FAIL"
            out["failure_class"] = "non_merge_evidence"
            out["one_line_summary"] = (
                f"PASS downgraded: recon evidence was {out.get('recon_mode')}/"
                f"{out.get('recon_verdict')}; " + out["one_line_summary"])
        if (out["status"] == "PASS"
                and (not out.get("pr_url") or not out.get("branch"))):
            out["status"] = "FAIL"
            out["failure_class"] = "missing_pr"
            out["one_line_summary"] = (
                "PASS downgraded: no PR URL/branch reported; " + out["one_line_summary"])
        # The ledger gate reads the PR's diff from git; the child's changed_paths can only add to it. A
        # replayed PASS may keep the head gated in the run being resumed (replay_gate says whether that
        # head still stands), but only for the same result: one that answered this prompt (the runtime
        # replays unchanged prompts) and names the same PR.
        reported = out.get("changed_paths")
        usable = isinstance(reported, list) and all(isinstance(p, str) for p in reported)
        record = REPLAYED.get(batch["id"])
        if (isinstance(record, dict) and record.get("status") == "PASS" and isinstance(record.get("pr_head"), str)
                and record.get("prompt_sha") == out["prompt_sha"] and record.get("pr_url") == out.get("pr_url")):
            gated = replay_gate(record, out["pr_url"])
        else:
            gated = pr_changed_paths(out.get("pr_url"))
        observed = gated[1] if gated else None
        if gated:
            out["pr_head"] = gated[0]
        tampered = ledger_violations(sorted({*(reported if usable else []), *(observed or [])}), batch["units"])
        if tampered:
            prefix = "PASS downgraded: " if out["status"] == "PASS" else ""
            out["status"] = "FAIL"
            out["failure_class"] = "ledger_tampered"
            out["one_line_summary"] = f"{prefix}PR changed the ledger ({', '.join(tampered)}); " + out["one_line_summary"]
        elif out["status"] == "PASS" and (not usable or observed is None):
            out["status"] = "FAIL"
            out["failure_class"] = "ledger_tampered"
            out["one_line_summary"] = (
                "PASS downgraded: changed_paths "
                + ("not reported" if not usable else "not verifiable from git (not a PR of this repo, or its fetch or diff failed)")
                + ", ledger integrity unverified; " + out["one_line_summary"])
        if out["status"] == "PASS":
            claimed = out.get("merge_authority")
            decision = claimed.get("decision_id") if isinstance(claimed, dict) else None
            evidence = unit_eligibility(out["pr_head"], batch["units"])
            ineligible = sorted(u for u, e in evidence.items() if e is not True)
            if out.get("merge_eligible") is True and not ineligible:
                out["merge_authority"] = {"kind": "harness", "decision_id": None}
            elif (isinstance(claimed, dict) and claimed.get("kind") == "human_override"
                  and override_decision(decision, batch["units"], decision_ledger())):
                out["merge_authority"] = {"kind": "human_override", "decision_id": decision}
            else:
                out["status"] = "FAIL"
                out["failure_class"] = "merge_authority"
                out.pop("merge_authority", None)
                why = "; ".join(f".migration/recon/{u}/result.json at the PR head "
                                + ("is missing or malformed" if evidence[u] is None else f"has merge_eligible={evidence[u]!r}")
                                for u in ineligible) or f"the child reported merge_eligible={out.get('merge_eligible')!r}"
                out["one_line_summary"] = (
                    f"PASS downgraded: recon evidence is not merge_eligible=true for every unit ({why}) "
                    f"and no merge_override row {decision or 'D-<n>'} naming {', '.join(batch['units'])} is in "
                    ".migration/06_decisions.md; " + out["one_line_summary"])
        if out["status"] == "PASS":
            out["gates"], unmet = gate_outcomes(batch, out.get("gates"), decision_ledger(), out["pr_head"])
            if unmet:
                out["status"] = "FAIL"
                out["failure_class"] = "gates"
                out["one_line_summary"] = "PASS downgraded: " + "; ".join(unmet) + "; " + out["one_line_summary"]
        # a replayed failure of this class was counted by the run being resumed; a replayed PASS (or FAIL of
        # another class) that the gate fails now was not
        if out["status"] != "PASS" and (record is None or (isinstance(record, dict) and (
                record.get("status") == "PASS" or record.get("failure_class") != out.get("failure_class")))):
            breaker.record(out.get("failure_class") or "unclassified")
        log(f"done   {batch['id']}: {out['status']} / recon {out['recon_verdict']}: "
            f"{out['one_line_summary']}")
        return out


COST_KEYS = ("source_statements", "target_statements", "source_rows_fetched", "target_rows_fetched")


def sum_cost(costs) -> dict:
    """Sum result.json['cost'] dicts; a side whose adapter did not count stays None."""
    total: dict = {k: 0 for k in COST_KEYS} | {"elapsed_s": 0.0}
    for c in costs:
        if not isinstance(c, dict):
            continue
        for k in COST_KEYS:
            v = c.get(k)
            if v is None:
                total[k] = None
            elif total[k] is not None and isinstance(v, (int, float)) and not isinstance(v, bool):
                total[k] += v
        if isinstance(c.get("elapsed_s"), (int, float)):
            total["elapsed_s"] += c["elapsed_s"]
    return total


def cost_line(results, verify) -> str:
    """Estimate (STOP C) against actuals (children + verifier), so the next wave's estimate
    can be corrected instead of guessed again."""
    est = MANIFEST.get("cost_estimate")
    actual = sum_cost([r.get("recon_cost") for r in results]
                      + ([verify.get("recon_cost")] if isinstance(verify, dict) else []))
    if est is None and all(actual[k] in (None, 0) for k in COST_KEYS):
        return "Cost: no estimate in the manifest and no recon_cost reported."
    def fmt(d):
        return ", ".join(f"{k}={d.get(k)}" for k in COST_KEYS if d.get(k) is not None) or "n/a"
    return (f"Cost: estimated {fmt(est or {})}; actual {fmt(actual)}, "
            f"harness time {round(actual['elapsed_s'])}s. Verifier depth {VERIFY_DEPTH}"
            + (", overrides: " + ", ".join(f"{b['id']}={b['verify_depth']}" for b in BATCHES if "verify_depth" in b)
               if any("verify_depth" in b for b in BATCHES) else "") + ".")






def waived_gates(results):
    return [{"batch": b["id"], "units": b["units"], "gate": g["id"], "decision_id": g["decision_id"]}
            for b, r in zip(BATCHES, results) if r["status"] == "PASS"
            for g in r.get("gates", []) if g["status"] == "waived"]


def merge_overrides(results):
    return [{"batch": b["id"], "units": b["units"], "decision_id": r["merge_authority"]["decision_id"]}
            for b, r in zip(BATCHES, results)
            if r["status"] == "PASS" and (r.get("merge_authority") or {}).get("kind") == "human_override"]


def write_brief(results, verify, surprises, undeclared, unreported, auto_merge):
    """Ten lines a lead reads in one minute. The orchestrator posts this at wave close."""
    n = len(BATCHES)
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = [b["id"] for b, r in zip(BATCHES, results) if r["status"] == "FAIL"]
    blocked = [b["id"] for b, r in zip(BATCHES, results) if r["status"] == "BLOCKED"]
    held = [b["id"] for b, r in zip(BATCHES, results) if r["status"] == "NOT_LAUNCHED"]
    feedback = sorted({s for r in results for s in r.get("skill_feedback", [])})
    lines = [
        f"# Wave {WAVE} close",
        "",
        f"Landed: {passed} of {n} batches passed their own recon.",
        f"Independent verify: {verify['wave_verdict'] if verify else 'NOT RUN'}"
        + (f", {len(verify.get('merged_prs', []))} PRs merged." if verify else "."),
        f"Failed: {', '.join(failed) or 'none'}.",
        f"Blocked on missing inputs: {', '.join(blocked) or 'none'}.",
        f"Held back by circuit breaker: {', '.join(held) or 'none'}.",
    ]
    if surprises:
        lines.append(f"Merges held: two children reported the same write target "
                     f"({', '.join(surprises)}). A human decides which PR lands.")
    if undeclared:
        lines.append("Merges held: children wrote outside their declared targets: "
                     + "; ".join(f"{k}: {', '.join(v)}" for k, v in sorted(undeclared.items()))
                     + ". A human decides which PR lands.")
    if unreported:
        lines.append(f"Merges held: {', '.join(unreported)} passed but reported no write targets; "
                     "a human confirms what they wrote before any PR lands.")
    waived = waived_gates(results)
    if waived:
        lines.append("Gates waived by ledger decision: "
                     + "; ".join(f"{w['batch']}/{w['gate']} by {w['decision_id']}" for w in waived) + ".")
    overrides = merge_overrides(results)
    if overrides:
        lines.append("Human override authority (merge_eligible=false; merged only if listed above): "
                     + "; ".join(f"{o['batch']} ({', '.join(o['units'])}) by {o['decision_id']}" for o in overrides) + ".")
    if not auto_merge:
        urls = [r["pr_url"] for r in results
                if r["status"] == "PASS" and r.get("pr_url")]
        lines.append("Awaiting manual merge: " + (", ".join(urls) or "none reported"))
    lines.append(cost_line(results, verify))
    lines += [
        "",
        "Verifier findings:" if verify and verify["findings"] else "Verifier findings: none.",
    ]
    lines += [f"- {f}" for f in (verify or {}).get("findings", [])]
    lines += ["", "Skill feedback to fold in before the next wave:" if feedback
              else "Skill feedback: none."]
    lines += [f"- {s}" for s in feedback]
    lines += ["", "Per batch:"]
    lines += [f"- {b['id']}: {r['status']}. {r['one_line_summary']}"
              + (f" {r['pr_url']}" if r.get("pr_url") else "")
              for b, r in zip(BATCHES, results)]
    brief_tmp = BRIEF_PATH.with_suffix(".brief.md.tmp")
    brief_tmp.write_text("\n".join(lines) + "\n")
    brief_tmp.replace(BRIEF_PATH)


async def main():
    if not resume:
        RUN_ID_PATH.unlink(missing_ok=True)
    await register_workflow(META)
    check_write_targets(BATCHES)
    log(f"wave {WAVE}: {len(BATCHES)} batches, width {WIDTH}, breaker at {BREAKER}")

    sem = asyncio.Semaphore(WIDTH)
    breaker = Breaker(BREAKER)
    results = await asyncio.gather(*(run_batch(b, sem, breaker) for b in BATCHES))

    reported = Counter(t for r in results for t in r.get("write_targets", []))
    surprises = [t for t, c in reported.items() if c > 1]
    undeclared = {}
    for b, r in zip(BATCHES, results):
        extra = sorted(set(r.get("write_targets", [])) - set(b["write_targets"]))
        if extra:
            undeclared[b["id"]] = extra
    unreported = [b["id"] for b, r in zip(BATCHES, results)
                  if r["status"] == "PASS" and not r.get("write_targets")]
    auto_merge = AUTO_MERGE
    if surprises:
        auto_merge = False
        log(f"WARNING: children reported overlapping write targets after the fact: {surprises}. "
            "Auto-merge is off for this wave; a human decides at wave close.")
    if undeclared:
        auto_merge = False
        log(f"HALT: children wrote outside their declared targets: {undeclared}. "
            "Auto-merge is off for this wave; a human decides at wave close.")
    if unreported:
        auto_merge = False
        log(f"HALT: PASS children did not report write targets: {unreported}. "
            "Auto-merge is off for this wave; a human decides at wave close.")

    passed = [{"batch": b["id"], "units": b["units"], "pr_url": r.get("pr_url", ""),
               "branch": r.get("branch", ""), "pr_head": r.get("pr_head"), "merge_authority": r.get("merge_authority"),
               "gates": r.get("gates", [])}
              for b, r in zip(BATCHES, results) if r["status"] == "PASS"]
    verify = None
    if passed:
        log(f"verify: {len(passed)} batches to an independent session")
        try:
            verify = await agent(verify_prompt(passed, auto_merge), phase="verify", schema=VERIFY_SCHEMA,
                                 label=f"verify-wave-{WAVE}", repos=[REPO])
        except WorkflowAgentError as e:
            verify = {"wave_verdict": "FAIL", "unit_verdicts": {},
                      "findings": [f"verifier session died: {e}"]}
    else:
        log("verify: skipped, no batch passed")

    verify_problems = (validate_verify(verify, passed, auto_merge, WAVE, verifier_changed_paths(WAVE, passed))
                       if verify is not None else [])
    if verify_problems:
        if not isinstance(verify, dict):
            verify = {"wave_verdict": "FAIL", "unit_verdicts": {}, "findings": []}
        verify["wave_verdict"] = "FAIL"
        if not isinstance(verify.get("findings"), list):
            verify["findings"] = []
        verify["findings"].extend(verify_problems)
    closed = (breaker.tripped_on is None and not surprises and not undeclared and not unreported
              and not verify_problems and verify is not None and verify["wave_verdict"] == "PASS"
              and all(r["status"] == "PASS" for r in results))
    result_tmp = RESULT_PATH.with_suffix(".result.json.tmp")
    result_tmp.write_text(json.dumps({
        "wave": WAVE, "manifest_sha": MANIFEST_SHA, "width": WIDTH,
        "run_id": RUN_ID, "base_sha": BASE_SHA, "mode": MODE, "stop_c": MANIFEST["stop_c"],
        "hook_probe": HOOK_PROBE_RESULT, "doctor_signed_at": DOCTOR.get("signed_at"),
        "breaker_tripped_on": breaker.tripped_on, "auto_merge": auto_merge,
        "closed": closed,
        "write_target_overlaps": surprises,
        "undeclared_write_targets": undeclared,
        "unreported_write_targets": unreported,
        "merge_overrides": merge_overrides(results),
        "waived_gates": waived_gates(results),
        "batches": [{"id": b["id"], **r} for b, r in zip(BATCHES, results)],
        "verify": verify,
    }, indent=2, sort_keys=True) + "\n")
    result_tmp.replace(RESULT_PATH)
    write_brief(results, verify, surprises, undeclared, unreported, auto_merge)
    log(f"wrote {RESULT_PATH} and {BRIEF_PATH}")
    log(f"wave {WAVE} verdict: {verify['wave_verdict'] if verify else 'NO PASSING BATCHES'}")


if not resume and not SMOKE:
    record_run(MODE, RUN_ID)
asyncio.run(main())
