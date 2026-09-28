"""What a child, the verifier, the close step and the resync step report back: the JSON schemas and
the validators the workflow runs on every report before it counts."""

import re
from collections import Counter


def ledger_violations(changed_paths, unit_ids, wave=None) -> list[str]:
    allowed = tuple(f".migration/recon/{u}/" for u in unit_ids)
    if wave is not None:
        allowed += (f".migration/recon/wave-{wave}/",)
    return [p for p in changed_paths
            if p.startswith(".migration/") and not p.startswith(allowed)]


def batch_verdicts(verdicts, passed):
    if not isinstance(verdicts, dict):
        return {}
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
    raw, verdicts = verdicts, batch_verdicts(verdicts, passed)
    for batch in sorted(b for b, v in verdicts.items() if v is None):
        keys = [k for k in raw if batch_verdicts({k: raw[k]}, passed) == {batch: raw[k]}]
        problems.append(f"verifier output invalid: conflicting verdicts for {batch}: "
                        + ", ".join(f"{k}={raw[k]}" for k in keys))
    missing = sorted(expected - set(verdicts))
    extra = sorted(set(verdicts) - expected)
    if missing:
        problems.append("verifier output invalid: missing verdicts for " + ", ".join(missing))
    if extra:
        problems.append("verifier output invalid: unexpected verdicts for " + ", ".join(extra))
    wave_verdict = verify.get("wave_verdict")
    if wave_verdict not in ("PASS", "FAIL"):
        problems.append("verifier output invalid: wave_verdict must be PASS or FAIL")
    for batch in sorted(expected - set(missing)):
        verdict = verdicts.get(batch)
        if verdict is not None and verdict not in ("PASS", "FAIL"):
            problems.append(f"verifier output invalid: verdict for {batch} is {verdict!r}")
        elif wave_verdict == "PASS" and verdict != "PASS":
            problems.append(f"verifier output invalid: wave PASS contradicts {batch}={verdict}")
    if wave_verdict == "FAIL" and expected and all(verdicts.get(b) == "PASS" for b in expected):
        problems.append("verifier output invalid: wave FAIL contradicts all unit verdicts PASS")
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


def validate_close(close, to_merge, merge=True) -> list[str]:
    problems = []
    if not isinstance(close, dict):
        return ["expected an object"]
    merged = close.get("merged_prs")
    if not isinstance(merged, list) or not all(
            isinstance(u, dict) and isinstance(u.get("pr_url"), str)
            and isinstance(u.get("merge_commit_sha"), str)
            and re.fullmatch(r"[0-9a-f]{40}", u["merge_commit_sha"])
            and isinstance(u.get("merged_head"), str)
            and re.fullmatch(r"[0-9a-f]{40}", u["merged_head"])
            for u in merged):
        problems.append("merged_prs rows must be {pr_url, merge_commit_sha, merged_head}")
        merged = []
    merged_urls = [u["pr_url"] for u in merged]
    if not merge and merged:
        problems.append("merged with auto_merge off")
    unmerged = close.get("unmerged")
    if not isinstance(unmerged, list) or not all(
            isinstance(u, dict) and isinstance(u.get("pr_url"), str) and isinstance(u.get("reason"), str)
            for u in unmerged):
        problems.append("unmerged must be a list of {pr_url, reason} rows")
        unmerged = []
    want = {p.get("pr_url") for p in to_merge}
    for url in merged_urls:
        if url not in want:
            problems.append(f"merged a PR outside the wave ({url})")
    listed = Counter([*merged_urls, *(u["pr_url"] for u in unmerged)])
    for url in sorted(want):
        if listed.get(url, 0) != 1:
            problems.append(f"{url} is in {listed.get(url, 0)} of merged_prs/unmerged, expected exactly one")
    changed = close.get("changed_paths")
    if not isinstance(changed, list) or not all(isinstance(p, str) for p in changed):
        problems.append("changed_paths must be a list of paths (git diff --name-only)")
        changed = []
    for p in changed:
        problems.append(f"wave-close step changed {p}; it writes nothing")
    return problems


CHILD_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["PASS", "FAIL", "BLOCKED"]},
        "pr_url": {"type": "string"},
        "branch": {"type": "string"},
        "recon_verdict": {"type": "string", "enum": ["PASS", "FAIL", "NOT_RUN"]},
        "recon_mode": {"type": "string", "description": "recon --mode of the evidence run (fixture never merges)"},
        "merge_eligible": {"type": "boolean", "description": "result.json['merge_eligible'] of the evidence run"},
        "parity": {"type": "string", "enum": ["PASS", "FAIL", "NOT_RUN"],
                   "description": "result.json['parity'] when the harness records it: the row and routine tiers alone"},
        "blocker_classes": {"type": "array", "items": {"type": "string"},
                            "description": "result.json['blocker_classes'] when the harness records it: why merge "
                                           "policy is blocked, never why rows differ"},
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
        "skill_feedback": {"type": "array", "items": {"type": "string"},
                           "description": "one line per rule you had to derive yourself"},
        "gates": {
            "type": "array",
            "items": {"type": "object",
                      "properties": {"id": {"type": "string"},
                                     "status": {"type": "string", "enum": ["passed", "failed"]},
                                     "evidence": {"anyOf": [
                                         {"type": "string"},
                                         {"type": "object",
                                          "properties": {"path": {"type": "string"}, "label": {"type": "string"},
                                                         "verdict": {"type": "string"}, "rows": {"type": "integer"}},
                                          "required": ["path"]}]}},
                      "required": ["id", "status", "evidence"]},
            "description": "outcome of each gate declared in your brief, by id; passed needs the evidence: the bare "
                           "path under .migration/recon/<unit>/, or {path, label?, verdict?, rows?} to annotate it"},
        "recon_cost": {"type": "object",
                       "description": "result.json['cost'] of the final live/snapshot/transactional run, one line"},
        "one_line_summary": {"type": "string"},
    },
    "required": ["status", "recon_verdict", "recon_mode", "merge_eligible", "write_targets", "changed_paths",
                 "one_line_summary"],
}


VERIFY_SCHEMA = {
    "type": "object",
    "properties": {
        "wave_verdict": {"type": "string", "enum": ["PASS", "FAIL"]},
        "unit_verdicts": {"type": "object",
                          "description": "PASS or FAIL per batch, keyed by batch id or by unit id (a unit key is "
                                         "normalised to its batch id; one verdict per batch, a batch whose keys "
                                         "disagree is rejected)"},
        "findings": {"type": "array", "items": {"type": "string"}},
        "report_path": {"type": "string"},
        "changed_paths": {"type": "array", "items": {"type": "string"},
                          "description": "every path your report branch changes: git diff --name-only <base>...<head>"},
        "recon_cost": {"type": "object",
                       "description": "summed result.json['cost'] over the verifier's re-runs"},
    },
    "required": ["wave_verdict", "unit_verdicts", "findings", "changed_paths"],
}


CLOSE_SCHEMA = {
    "type": "object",
    "properties": {
        "merged_prs": {"type": "array",
                       "items": {"type": "object",
                                 "properties": {"pr_url": {"type": "string"},
                                                "merge_commit_sha": {"type": "string"},
                                                "merged_head": {"type": "string"}},
                                 "required": ["pr_url", "merge_commit_sha", "merged_head"]},
                       "description": "from `gh pr view <url> --json state,mergeCommit,headRefOid` after "
                                      "the merge; state must be MERGED"},
        "unmerged": {"type": "array",
                     "items": {"type": "object",
                               "properties": {"pr_url": {"type": "string"}, "reason": {"type": "string"}},
                               "required": ["pr_url", "reason"]}},
        "changed_paths": {"type": "array", "items": {"type": "string"}},
        "review_findings": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["merged_prs", "unmerged", "changed_paths"],
}


RESYNC_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["ok", "failed"]},
        "sequences": {"type": "array",
                      "items": {"type": "object",
                                "properties": {"object": {"type": "string"}, "before": {}, "after": {}},
                                "required": ["object", "before", "after"]}},
        "changed_paths": {"type": "array", "items": {"type": "string"}},
        "one_line_summary": {"type": "string"},
    },
    "required": ["status", "sequences", "changed_paths", "one_line_summary"],
}


def validate_resync(out) -> list[str]:
    if not isinstance(out, dict):
        return ["resync output invalid: expected an object"]
    problems = []
    if out.get("status") not in ("ok", "failed"):
        problems.append(f"resync output invalid: status {out.get('status')!r}")
    elif out["status"] == "failed":
        problems.append(f"resync command failed: {out.get('one_line_summary')}")
    rows = out.get("sequences")
    if (not isinstance(rows, list)
            or not all(isinstance(r, dict) and {"object", "before", "after"} <= set(r) for r in rows)):
        problems.append("resync output invalid: sequences must be [{object, before, after}, ...]")
    changed = out.get("changed_paths")
    if not isinstance(changed, list):
        problems.append("resync output invalid: changed_paths must be a list")
    elif changed:
        problems.append("resync changed " + ", ".join(map(str, changed)) + " but must commit and write nothing")
    return problems


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


def _verify_sink(verify, problems, fail=False):
    """Fold problems into the verify record's findings, or into a fresh FAIL one when the record is unusable."""
    if not isinstance(verify, dict):
        verify = {"wave_verdict": "FAIL", "unit_verdicts": {}, "findings": []}
    if fail:
        verify["wave_verdict"] = "FAIL"
    if not isinstance(verify.get("findings"), list):
        verify["findings"] = []
    verify["findings"].extend(problems)
    return verify
