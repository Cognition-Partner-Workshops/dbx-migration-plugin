"""The plan decisions a wave runs under: which committed `merge_override` authorization clears a batch,
the blocker classes it forgives, and the plan-scope hash that tells a plan change from a plumbing edit."""

import hashlib
import json


# what a merge_override entry that sets no blocker_classes forgives: every policy class, never data.
# Rows that differ are fixed in converted code; an entry that names "data" authorizes nothing
UNSCOPED_OVERRIDE = frozenset({"structural", "privilege_visibility", "rerun_policy", "evidence"})


def override_forgives(scope):
    """The blocker classes an override's scope forgives: every policy class when unscoped, else the policy
    classes it names; `data` is never one of them."""
    return UNSCOPED_OVERRIDE if scope is None else set(scope) & UNSCOPED_OVERRIDE


def scope_covers(scope, classes):
    """Whether an override's blocker-class scope covers what each unit recorded. An unrecorded class
    list (a harness before blocker classes, or a malformed result) fits no override: it cannot show
    the blockers are not data."""
    return all(c is not None and set(c) <= override_forgives(scope) for c in classes.values())


# the committed authorization file's entry kinds a wave runs under: a plan decision the human
# selected is mirrored there by reviewed PR, with `objects` naming the units it covers. A waived
# gate cites its `gate_waived` entry from the manifest; a `merge_override` entry names its wave
# itself, so the manifest never changes (same plan_sha) when the human authorizes one
AUTHORIZED_KINDS = frozenset({"gate_waived", "merge_override"})


def plan_authorizations(text):
    """The committed `.migration/authorizations.json` entries by id; no file is no entries. Raises
    ValueError when the file is not {"version": 1, "authorizations": [{id, kind, objects, by}, ...]}."""
    if text is None:
        return {}
    doc = json.loads(text)
    entries = doc.get("authorizations") if isinstance(doc, dict) else None
    if not (isinstance(entries, list) and all(isinstance(e, dict) and isinstance(e.get("id"), str) for e in entries)):
        raise ValueError("is not {version, authorizations: [{id, kind, objects, by}]}")
    ids = [e["id"] for e in entries]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        raise ValueError(f"repeats authorization ids {dupes!r}; which entry a decision cites must not depend on order")
    return dict(zip(ids, entries))


def unauthorized_decisions(manifest, entries):
    """What the manifest waives without a committed authorization: each waived gate's `decision_id`
    must be a `gate_waived` entry, by a user, whose objects name every unit it covers. `entries` may
    be a callable, read only when the manifest cites a decision."""
    cited = [(g.get("decision_id"), b["units"], f"batch {b['id']} gate {g['id']}")
             for b in manifest.get("batches", []) for g in b.get("gates", []) if g.get("status") == "waived"]
    if not cited:
        return []
    entries = entries() if callable(entries) else entries
    problems = []
    for decision, units, what in cited:
        e = entries.get(decision)
        objects = e.get("objects") if isinstance(e, dict) else None
        if not (isinstance(e, dict) and e.get("kind") == "gate_waived" and str(e.get("by", "")).startswith("user:")
                and isinstance(objects, list) and all(isinstance(o, str) for o in objects)):
            problems.append(f"{what} cites {decision!r}, not a `gate_waived` entry by a user in the committed authorization file")
        elif not set(units) <= set(objects):
            problems.append(f"{what} cites {decision!r}, whose objects do not name {sorted(set(units) - set(objects))}")
    return problems


def _strs(v):
    return isinstance(v, list) and all(isinstance(s, str) for s in v)


def wave_overrides(entries, wave):
    """The committed `merge_override` authorizations for one wave, as `{decision, units, blocker_classes?}`
    rows: entries by a user whose `wave` is the manifest's file stem (`wave-<tag>`), with `objects` naming
    units and `blocker_classes`, when set, policy classes only. An entry naming `data`, another wave, no
    wave, or a non-user author is not an override of this wave."""
    if not isinstance(entries, dict):
        return []
    rows = []
    for decision, e in entries.items():
        if not (isinstance(e, dict) and e.get("kind") == "merge_override" and str(e.get("by", "")).startswith("user:")
                and e.get("wave") == wave and _strs(e.get("objects")) and e["objects"]
                and ("blocker_classes" not in e or (_strs(e["blocker_classes"])
                                                    and set(e["blocker_classes"]) <= UNSCOPED_OVERRIDE))):
            continue
        row = {"decision": decision, "units": list(e["objects"])}
        if "blocker_classes" in e:
            row["blocker_classes"] = list(e["blocker_classes"])
        rows.append(row)
    return rows


def merge_override_for(units, entries, wave):
    """The single committed `merge_override` authorization of this wave covering every unit of a batch
    (wave_overrides), else None: two entries that both cover it, or none, clear nothing."""
    covering = [e for e in wave_overrides(entries, wave) if set(units) <= set(e["units"])]
    return covering[0] if len(covering) == 1 else None


# manifest keys a plumbing edit may change without changing the plan: how a child is briefed and
# connected, and estimates. Everything else is the plan the human approved (order, dependencies,
# width, gates, write targets). Merge overrides are not in the manifest at all: they are committed
# authorization entries, so authorizing one keeps the plan_sha and the wave reruns under its plan
# step. Of `source`, only the secret name is plumbing: the family and the params select the slice
# that is reconciled, which is scope
PLAN_PLUMBING = frozenset({"brief", "repo", "secrets", "cost_estimate", "max_minutes"})


SOURCE_PLUMBING = frozenset({"secret"})


def plan_sha(manifest):
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
