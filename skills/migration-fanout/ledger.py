"""The decision ledger (`06_decisions.md`): rows, structured cells, STOP C approval of a gates hash,
overrides and their blocker-class scope, and the approved-plan hash a plumbing relaunch may keep."""

import hashlib
import json
import re
from collections import Counter


DECISION_ID = re.compile(r"D-[0-9]+")


HUMAN_PROVENANCE = re.compile(r"(?<![\w-])user:[\w][\w.@/-]*")


DEFAULT_ACCEPTED = re.compile(r"default-accepted(?: ?\([^|]*\))?")


LEDGER_METADATA = re.compile(rf"D-[0-9]+|\d{{4}}-\d{{2}}-\d{{2}}(?:[T ][\d:.]+Z?(?:[+-]\d{{2}}:?\d{{2}})?)?"
                             rf"|{HUMAN_PROVENANCE.pattern}|{DEFAULT_ACCEPTED.pattern}")


def ledger_rows(ledger):
    for line in ledger.splitlines():
        line = line.strip()
        if "|" not in line:
            continue
        cells = [" ".join(c.split()) for c in line.strip("|").split("|")]
        ids = [c for c in cells if DECISION_ID.fullmatch(c)]
        if ids:
            yield ids[0], cells


def structured_decision(cells):
    """The one machine cell a ledger row may carry: a JSON object {kind, units, gate?, blocker_classes?}.
    None when the row is prose only; ValueError when a cell looks like one and is not."""
    found = None
    for c in cells:
        if not c.startswith("{"):
            continue
        try:
            obj = json.loads(c)
        except ValueError:
            raise ValueError(f"cell {c!r} is not JSON") from None
        if not (isinstance(obj, dict) and isinstance(obj.get("kind"), str)
                and isinstance(obj.get("units"), list) and all(isinstance(u, str) for u in obj["units"])
                and isinstance(obj.get("gate", ""), str)
                and (obj.get("blocker_classes") is None
                     or (isinstance(obj["blocker_classes"], list)
                         and all(isinstance(b, str) for b in obj["blocker_classes"])))):
            raise ValueError(f"cell {c!r} must be {{kind, units: [..], gate?, blocker_classes?: [..]}}")
        if found is not None:
            raise ValueError("more than one machine cell")
        found = obj
    return found


def human_decision(decision_id, ledger):
    """The cells of the human row D-<n> names (never a default-accepted one), or None."""
    if not isinstance(decision_id, str) or not DECISION_ID.fullmatch(decision_id):
        return None
    for row_id, cells in ledger_rows(ledger):
        if (row_id == decision_id and any(HUMAN_PROVENANCE.fullmatch(c) for c in cells)
                and not any(DEFAULT_ACCEPTED.fullmatch(c) for c in cells)):
            return cells
    return None


def override_decision(decision_id, units, ledger, word="merge_override", gate=None):
    """True when the human row D-<n> grants `word` for every unit (and the gate, for a waiver). A row
    with a machine cell is read from that cell alone; a prose row must name the word, the gate and
    each unit as whole tokens outside its metadata cells."""
    cells = human_decision(decision_id, ledger)
    if cells is None:
        return False
    try:
        machine = structured_decision(cells)
    except ValueError:
        return False
    if machine is not None:
        return (machine["kind"] == word and set(units) <= set(machine["units"])
                and (gate is None or machine.get("gate") == gate))

    def token(w):
        return rf"(?<![A-Za-z0-9_.-]){re.escape(w)}(?![A-Za-z0-9_.-])"

    text = " | ".join(HUMAN_PROVENANCE.sub(" ", c) for c in cells if not LEDGER_METADATA.fullmatch(c))
    need = Counter((word, *([gate] if gate else []), *units))
    return all(len(re.findall(token(w), text)) >= n for w, n in need.items())


def override_scope(decision_id, ledger):
    """The blocker classes a structured override row says it covers; None when the row sets no scope."""
    cells = human_decision(decision_id, ledger)
    try:
        machine = structured_decision(cells) if cells else None
    except ValueError:
        return None
    return sorted(machine["blocker_classes"]) if machine and machine.get("blocker_classes") is not None else None


def rows_after(ledger, stop_c):
    """The ledger lines below this run's STOP C row."""
    lines = ledger.splitlines()
    for n, line in enumerate(lines):
        if any(row_id == stop_c for row_id, _ in ledger_rows(line)):
            return lines[n + 1:]
    return []


def ledger_waiver(gate_id, units, ledger, stop_c):
    for line in rows_after(ledger, stop_c):
        for decision_id in dict.fromkeys(DECISION_ID.findall(line)):
            if override_decision(decision_id, units, line, word="waive", gate=gate_id):
                return decision_id
    return None


# what a merge_override row that sets no blocker_classes forgives: every policy class, never data.
# Rows that differ are fixed in converted code; only a row that names "data" says otherwise
UNSCOPED_OVERRIDE = frozenset({"structural", "privilege_visibility", "rerun_policy", "evidence"})


def override_forgives(scope):
    return UNSCOPED_OVERRIDE if scope is None else set(scope)


def scope_covers(scope, classes):
    """Whether an override's blocker-class scope covers what each unit recorded. An unrecorded class
    list (a harness before blocker classes, or a malformed result) fits no override: it cannot show
    the blockers are not data."""
    return all(c is not None and set(c) <= override_forgives(scope) for c in classes.values())


def ledger_override(units, ledger, stop_c, classes=None):
    """The merge_override row a human wrote for these units below this run's STOP C row whose scope
    covers the units' recorded blocker classes, whether or not the child thought to report it: the
    decision is the ledger's, not the child's memory. The first applicable row wins; a narrower one
    above it is not a refusal, and is returned only when no row covers the classes (so the halt names it)."""
    named = None
    for line in rows_after(ledger, stop_c):
        for decision_id in dict.fromkeys(DECISION_ID.findall(line)):
            if override_decision(decision_id, units, line):
                if scope_covers(override_scope(decision_id, line), classes or {}):
                    return decision_id
                named = named or decision_id
    return named


# manifest keys a plumbing relaunch may change without a new STOP C: how a child is briefed and
# connected, and estimates. Everything else is the plan the row approved (contract: order,
# dependencies, width, gates, write targets). Of `source`, only the secret name is plumbing:
# the family and the params select the slice that is reconciled, which is scope
PLAN_PLUMBING = frozenset({"brief", "repo", "secrets", "cost_estimate", "max_minutes", "stop_c", "gates_sha"})


SOURCE_PLUMBING = frozenset({"secret"})


def approved_plan_sha(manifest):
    def strip(x):
        if isinstance(x, dict):
            return {k: strip(v) for k, v in x.items() if k not in PLAN_PLUMBING}
        if isinstance(x, list):
            return [strip(v) for v in x]
        return x
    plan = strip(manifest)
    if isinstance(plan.get("source"), dict):
        plan["source"] = {k: v for k, v in plan["source"].items() if k not in SOURCE_PLUMBING}
    return hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def declared_gates_sha(wave, batches, degraded=False):
    declared = {"wave": wave, "batches": {
        b["id"]: {"units": sorted(b["units"]),
                  "gates": [[g["id"], g["kind"], g["status"], g["evidence"], g.get("decision_id")] for g in b["gates"]]}
        for b in batches}}
    if degraded:
        declared["degraded"] = True
    return hashlib.sha256(json.dumps(declared, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def gates_approved(decision_id, wave, sha, ledger, stop_mode="hard"):
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
