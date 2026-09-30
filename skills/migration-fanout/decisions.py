"""The plan decisions a committed wave manifest carries: which `merge_overrides` entry clears a batch,
the blocker classes it forgives, and the plan-scope hash that tells a plan change from a plumbing edit."""

import hashlib
import json


# what a merge_overrides entry that sets no blocker_classes forgives: every policy class, never data.
# Rows that differ are fixed in converted code; only an entry that names "data" says otherwise
UNSCOPED_OVERRIDE = frozenset({"structural", "privilege_visibility", "rerun_policy", "evidence"})


def override_forgives(scope):
    return UNSCOPED_OVERRIDE if scope is None else set(scope)


def scope_covers(scope, classes):
    """Whether an override's blocker-class scope covers what each unit recorded. An unrecorded class
    list (a harness before blocker classes, or a malformed result) fits no override: it cannot show
    the blockers are not data."""
    return all(c is not None and set(c) <= override_forgives(scope) for c in classes.values())


# the committed authorization file's entry kinds a manifest may cite: a plan decision the human
# selected is mirrored there by reviewed PR, with `objects` naming the units it covers
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
    return {e["id"]: e for e in entries}


def unauthorized_decisions(manifest, entries):
    """What the manifest waives or overrides without a committed authorization: each waived gate's
    `decision_id` and each merge_overrides entry's `decision` must be an entry of that kind, by a
    user, whose objects name every unit it covers. `entries` may be a callable, read only when
    the manifest cites a decision."""
    cited = [(g.get("decision_id"), "gate_waived", b["units"], f"batch {b['id']} gate {g['id']}")
             for b in manifest.get("batches", []) for g in b.get("gates", []) if g.get("status") == "waived"]
    cited += [(e.get("decision"), "merge_override", e.get("units") or [],
               f"merge_overrides entry for {', '.join(e.get('units') or [])}")
              for e in manifest.get("merge_overrides") or [] if isinstance(e, dict)]
    if not cited:
        return []
    entries = entries() if callable(entries) else entries
    problems = []
    for decision, kind, units, what in cited:
        e = entries.get(decision)
        objects = e.get("objects") if isinstance(e, dict) else None
        if not (isinstance(e, dict) and e.get("kind") == kind and str(e.get("by", "")).startswith("user:")
                and isinstance(objects, list) and all(isinstance(o, str) for o in objects)):
            problems.append(f"{what} cites {decision!r}, not a `{kind}` entry by a user in the committed authorization file")
        elif not set(units) <= set(objects):
            problems.append(f"{what} cites {decision!r}, whose objects do not name {sorted(set(units) - set(objects))}")
    return problems


def merge_override_for(units, entries):
    """The single merge_overrides entry covering every unit of a batch, else None: two entries that
    both cover it, or none, clear nothing."""
    if not isinstance(entries, list):
        return None
    covering = [e for e in entries
                if isinstance(e, dict) and isinstance(e.get("decision"), str)
                and isinstance(e.get("units"), list) and set(units) <= set(e["units"])]
    return covering[0] if len(covering) == 1 else None


# manifest keys a plumbing edit may change without changing the plan: how a child is briefed and
# connected, and estimates. Everything else is the plan the human approved (order, dependencies,
# width, gates, write targets, overrides). Of `source`, only the secret name is plumbing: the family
# and the params select the slice that is reconciled, which is scope
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
