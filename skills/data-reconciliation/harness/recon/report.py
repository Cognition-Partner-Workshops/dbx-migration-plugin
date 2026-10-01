"""Result rendering: one machine-readable result.json (the workflow gates on it), one
human report.md (read at wave close), and a ~30-line recon.summary.md sized for a PR body.
Every report cites mode, mapping version, and tolerance version so evidence is
re-runnable."""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

from .config import RERUN_POSTURES, ConfigError
from .rerun import rerun_first_run_baseline, rerun_gap, rerun_missing, rerun_unsupported
from .routines import parity_missing, routine_gap
from .tiers import TierResult

MAX_FINDINGS_IN_REPORT = 50

STRUCTURAL_TIERS = ("structural_parity", "schema_parity")
# tiers that grade the evidence, not the rows: a failure here is a run to repeat, not drift
EVIDENCE_TIERS = ("consistency_window",)
# findings in a data tier that say the rows are not all there or not gradable yet, not that they
# differ: the feed's lag, an ungradable watermark, unusable delete evidence, aggregates left
# ungraded because too many keys are in flight. A row applied out of order or replayed is data
EVIDENCE_CHECKS = frozenset({"cdc_lag_exceeded", "cdc_in_flight_exceeded", "cdc_watermark_incomparable",
                             "cdc_lag_ungraded", "delete_lag_exceeded", "delete_evidence_retention_gap",
                             "delete_evidence_unusable", "aggregates_ungraded_in_flight"})


def data_failed(tier: TierResult) -> bool:
    """A failed data tier whose findings say the rows differ; a tier that failed without a finding
    is not explained and counts as data."""
    return not tier.passed and (not tier.findings or any(f.check not in EVIDENCE_CHECKS for f in tier.findings))


# Why a merge is blocked, by what a human has to do about it:
#   data                  rows or routine behaviour differ: fix the converted code
#   structural            the catalogs differ (a constraint, trigger, index, identity or grant)
#   privilege_visibility  the principal could not read a dictionary: fix its grants or declare
#                         the category blind; nothing is known to differ
#   rerun_policy          the rerun proof is missing, failed or unsupported under the posture
#   evidence              the run is not merge evidence (mode, snapshot manifest, unlisted
#                         routines, ungraded embeds, provenance)
BLOCKER_CLASSES = ("data", "structural", "privilege_visibility", "rerun_policy", "evidence")

MODE_NOTES = {
    "snapshot": " (PASS scoped to the snapshot watermark)",
    "fixture": " (fixture data: NOT a merge verdict, run live once before merging)",
    "transactional": " (both sides live: PASS scoped to the consistency window that held and the "
                     "target's applied CDC watermark)",
}


def _mode_note(mode: str) -> str:
    return MODE_NOTES.get(mode, "")


def _rerun_line(result: dict) -> str | None:
    proof = result.get("rerun_proof")
    if proof is None:
        return None
    line = f"- Rerun proof: fresh `{proof.get('fresh')}`, evolved `{proof.get('evolved')}`"
    if proof.get("unsupported_reason"):
        line += f" ({proof['unsupported_reason']})"
    for f in proof.get("findings", [])[:MAX_FINDINGS_IN_SUMMARY]:
        line += (f"\n  - {f.get('run')} `{f.get('table')}` {f.get('check')}"
                 + (f" `{f['column']}`" if f.get("column") else "") + f": {f.get('detail', '')}")
    return line


def status_line(result: dict) -> str:
    """One line a reader can act on: parity first, then the merge policy with its blocker
    classes. `parity PASS, merge blocked (rerun_policy)` never reads as a data failure."""
    if result.get("merge_eligible"):
        policy = "merge eligible"
    else:
        classes = result.get("blocker_classes") or sorted({b["class"] for b in result.get("blockers", [])})
        policy = "merge blocked" + (f" ({', '.join(classes)})" if classes else "")
    return f"parity {result.get('parity', result['verdict'])}, {policy}"


def _blocker_lines(result: dict) -> list[str]:
    blockers = result.get("blockers") or []
    if not blockers:
        return []
    by_class: dict[str, list[str]] = {}
    for b in blockers:
        by_class.setdefault(b["class"], []).append(b["reason"])
    return ["- Blockers: " + "; ".join(f"{cls}: {', '.join(reasons)}" for cls, reasons in by_class.items())]


def _authority_line(result: dict) -> str:
    """The harness is the only authority this file writes. A merge past merge_eligible=false needs a
    human_override the workflow checks against a merge_overrides entry of the committed wave manifest."""
    authority = result.get("merge_authority") or {"kind": "harness", "decision_id": None}
    return (f"- Merge authority: `{authority['kind']}`"
            + (f" ({authority['decision_id']})" if authority.get("decision_id") else "")
            + " (human_override needs a merge_overrides entry in the committed wave manifest naming the unit)")


def build_result(unit: str, mode: str, mapping_version: str, tolerance_version: str,
                 tiers: list[TierResult], seed: int = 0,
                 params: dict[str, str] | None = None,
                 snapshot: dict | None = None,
                 provenance_warnings: list[str] | None = None,
                 depth: str = "threshold", cost: dict | None = None,
                 type_map: dict | None = None,
                 routine_parity: list[dict] | None = None,
                 routine_writers: list[str] | None = None,
                 routine_analysis_missing: bool = False,
                 routine_dependencies: str | None = None,
                 rerun_proof: dict | None = None,
                 rerun_posture: str = "required") -> dict:
    """`verdict` is every tier's pass/fail and `merge_eligible` the harness's merge answer, as
    the workflow reads them. Alongside them the result separates what a reader has to act on:
    `parity` (did the rows and routines match), `merge_policy` (eligible/blocked) and
    `blockers`, each block reason with the class of work it needs (BLOCKER_CLASSES)."""
    if rerun_posture not in RERUN_POSTURES:
        raise ConfigError(f"rerun_posture must be one of {RERUN_POSTURES}, got {rerun_posture!r}")
    warnings = []
    for t in tiers:
        for path in t.stats.get("embeds_ungraded", []):
            warnings.append(f"UNGRADED embedded values: {path} (cardinality checked only; "
                            "declare embed key/fields in the mapping spec to grade values)")
        for note in t.stats.get("unverified", []):
            warnings.append(f"UNVERIFIED {t.name}: {note}")
        for note in t.stats.get("dictionary_unavailable", []):
            warnings.append(f"UNVERIFIED {t.name}: structure unavailable: {note}")
    warnings.extend(provenance_warnings or [])
    verdict = "PASS" if all(t.passed for t in tiers) else "FAIL"
    structural = next((t for t in tiers if t.name in STRUCTURAL_TIERS), None)
    data_tiers = [t for t in tiers if t.name not in STRUCTURAL_TIERS + EVIDENCE_TIERS]
    checks = (structural.stats.get("structural_checks") or {}) if structural else {}
    structural_blind = any(v == "unsupported" for c, v in checks.items() if c != "indexes")
    unlisted_writers = parity_missing(routine_parity, routine_writers)
    parity_gap = bool(unlisted_writers) or routine_analysis_missing
    if not data_tiers:
        parity = "NOT_RUN"
    elif not any(data_failed(t) for t in data_tiers) and not routine_gap(routine_parity):
        parity = "PASS"
    else:
        parity = "FAIL"
    # a hole the principal was refused is a visibility gap, not a mismatch; a hole of any other
    # kind (or a finding) is structural
    structural_class = "structural"
    if structural is not None and not structural.findings \
            and set(structural.stats.get("hole_kinds") or []) == {"privilege"}:
        structural_class = "privilege_visibility"
    structural_warning = tuple(f"UNVERIFIED {t}" for t in STRUCTURAL_TIERS)
    blockers: list[dict] = []

    def block(reason: str, cls: str) -> None:
        blockers.append({"reason": reason, "class": cls})

    if structural is not None and (structural.findings or structural.stats.get("unverified")
                                   or structural.stats.get("dictionary_unavailable")
                                   or structural_blind):
        block("structural_gap", structural_class)
    if verdict == "FAIL":  # every failed tier names its class: a structural miss does not hide a moved window
        failed_classes = []
        if any(data_failed(t) for t in tiers if t.name not in STRUCTURAL_TIERS + EVIDENCE_TIERS):
            failed_classes.append("data")
        if structural is not None and not structural.passed:
            failed_classes.append(structural_class)
        if any(not t.passed and (t.name in EVIDENCE_TIERS or not data_failed(t)) for t in data_tiers
               + [t for t in tiers if t.name in EVIDENCE_TIERS]):
            failed_classes.append("evidence")  # the window moved, or the feed is behind
        for cls in failed_classes or ["evidence"]:
            block("tier_failed", cls)
    if routine_gap(routine_parity):
        block("routine_gap", "data")
    if parity_gap:
        block("routine_parity_missing", "evidence")
    if warnings:
        block("warnings", structural_class if all(w.startswith(structural_warning) for w in warnings)
              else "evidence")
    if mode not in ("live", "snapshot", "transactional"):
        block("mode", "evidence")
    if mode == "snapshot" and snapshot is None:
        block("snapshot_missing", "evidence")
    if mode != "structural":  # no row tier ran, so there is no rerun to prove
        if rerun_missing(rerun_proof) and rerun_posture != "not_applicable":
            block("rerun_missing", "rerun_policy")
        if rerun_gap(rerun_proof):
            block("rerun_gap", "rerun_policy")
        if rerun_unsupported(rerun_proof) and (
                rerun_posture == "required"
                or (rerun_posture == "first_run_baseline"
                    and not rerun_first_run_baseline(rerun_proof))):
            block("rerun_unsupported", "rerun_policy")
    reasons = list(dict.fromkeys(b["reason"] for b in blockers))
    merge_eligible = not blockers
    return {
        "unit": unit,
        "mode": mode,
        "mapping_version": mapping_version,
        "tolerance_version": tolerance_version,
        "seed": seed,
        "depth": depth,
        "params": params or {},
        "snapshot": snapshot,
        "cost": cost or {},
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "tiers": [t.as_dict() for t in tiers],
        "warnings": warnings,
        "verdict": verdict,
        "parity": parity,
        "merge_eligible": merge_eligible,
        "merge_policy": "eligible" if merge_eligible else "blocked",
        "blockers": blockers,
        "blocker_classes": sorted({b["class"] for b in blockers}),
        "merge_authority": {"kind": "harness", "decision_id": None},
        "type_map": type_map,
        "rerun_proof": rerun_proof,
        "rerun_posture": rerun_posture,
        "merge_block_reasons": reasons,
        "routine_parity": routine_parity,
        "routine_writers": routine_writers,
        "routine_dependencies": routine_dependencies,
        **({"routine_analysis_missing": True} if routine_analysis_missing else {}),
    }


def _parity_lines(result: dict) -> list[str]:
    parity = result.get("routine_parity")
    if not parity:
        if result.get("routine_analysis_missing"):
            return ["", "## Routine parity: routine_parity_missing (no dependency analysis; commit "
                        f".migration/units/{result['unit']}/dependencies.json, an empty `routines` list "
                        "for a unit that writes nothing, or pass `--routine-dependencies`)"]
        if "routine_parity_missing" in result.get("merge_block_reasons", []):
            return ["", "## Routine parity: routine_parity_missing (the unit has writing routines and no "
                        "parity list; run `dbx-recon routine-parity` and pass `--routine-parity`)", ""] + [
                        f"- `{w}` no row" for w in result.get("routine_writers") or []]
        return []
    counts = {s: sum(1 for r in parity if r["status"] == s) for s in ("proven", "unproven", "failed")}
    lines = ["", f"## Routine parity: {counts['proven']} proven, {counts['unproven']} unproven, "
                 f"{counts['failed']} failed", ""]
    for r in parity:
        if r["status"] == "proven":
            continue
        why = r.get("reason") or "; ".join(f"{f['table']} {f['check']}: {f['detail']}"
                                           for f in r.get("findings", []))
        lines.append(f"- `{r['routine']}` {r['status']}: {why}")
    return lines


def render_report(result: dict) -> str:
    lines = [
        f"# Recon report: unit `{result['unit']}`",
        "",
        f"- **{status_line(result)}**",
        f"- Verdict: {result['verdict']} (all tiers)",
        *_blocker_lines(result),
        f"- Mode: `{result['mode']}`" + _mode_note(result["mode"]),
        (f"- Merge eligible: {'yes' if result['merge_eligible'] else 'no'} "
         "(fixture/continuous evidence never merges)"),
        f"- Rerun posture: `{result.get('rerun_posture', 'required')}`",
        _authority_line(result),
        f"- Mapping version: `{result['mapping_version']}`",
        f"- Tolerance version: `{result['tolerance_version']}`",
        f"- Seed: `{result.get('seed', 0)}`" + (f" | Params: `{result['params']}`"
                                                if result.get("params") else ""),
        f"- Tier 3 depth: `{result.get('depth', 'threshold')}`",
        f"- Generated: {result['generated_at']}",
    ]
    if result.get("snapshot") is not None:
        lines.append(f"- Snapshot provenance: `{json.dumps(result['snapshot'], default=str)}`")
    lines += _parity_lines(result)
    if result.get("cost"):
        lines.append(f"- Cost: `{json.dumps(result['cost'], default=str)}`")
    if _rerun_line(result):
        lines.append(_rerun_line(result))
    for w in result.get("warnings", []):
        lines.append(f"- **WARNING: {w}**")
    lines += [
        "",
        "| Tier | Name | Checks | Result |",
        "|---|---|---|---|",
    ]
    for t in result["tiers"]:
        lines.append(f"| {t['tier']} | {t['name']} | {t['checks_run']} | "
                     f"{'PASS' if t['passed'] else 'FAIL (' + str(len(t['findings'])) + ' findings)'} |")
    for t in result["tiers"]:
        if t.get("stats"):
            lines += ["", f"## Tier {t['tier']} coverage", "```json",
                      json.dumps(t["stats"], indent=2, default=str), "```"]
        if t["findings"]:
            lines += ["", f"## Tier {t['tier']} findings ({len(t['findings'])})"]
            for f in t["findings"][:MAX_FINDINGS_IN_REPORT]:
                lines.append(f"- `{f['object']}` {f['check']}: {f['detail']}"
                             + (f" | source={f['source_value']} target={f['target_value']}"
                                f" | rules={f['rules_applied']}"
                                if f["check"] in ("field_diff", "embed_field_diff",
                                                  "aggregate_min", "aggregate_max",
                                                  "aggregate_sum", "aggregate_null_rate",
                                                  "aggregate_distinct_count") else ""))
            if len(t["findings"]) > MAX_FINDINGS_IN_REPORT:
                lines.append(f"- ... {len(t['findings']) - MAX_FINDINGS_IN_REPORT} more in result.json")
    return "\n".join(lines) + "\n"


MAX_FINDINGS_IN_SUMMARY = 5


def render_summary(result: dict) -> str:
    """The tier-A evidence surface: what a unit PR renders. Full detail stays in
    result.json / report.md, which the PR links."""
    lines = [
        f"# Recon summary: `{result['unit']}` - **{status_line(result)}**",
        "",
        *_blocker_lines(result),
        f"- Verdict: {result['verdict']} / mode `{result['mode']}`" + _mode_note(result["mode"]),
        (f"- Merge eligible: {'yes' if result['merge_eligible'] else 'no'} "
         "(fixture/continuous evidence never merges)"),
        f"- Rerun posture: `{result.get('rerun_posture', 'required')}`",
        _authority_line(result),
        f"- Mapping `{result['mapping_version']}` / tolerances `{result['tolerance_version']}`"
        f" / seed `{result.get('seed', 0)}` / depth `{result.get('depth', 'threshold')}`"
        + (f" / params `{result['params']}`" if result.get("params") else ""),
        f"- Generated: {result['generated_at']}",
    ]
    cost = result.get("cost") or {}
    if cost.get("source_statements") is not None:
        lines.append(f"- Cost: source {cost['source_statements']} statements / "
                     f"{cost['source_rows_fetched']} rows fetched; target {cost['target_statements']} "
                     f"statements / {cost['target_rows_fetched']} rows; {cost['elapsed_s']}s")
    lines += _parity_lines(result)
    structural = next((t for t in result["tiers"] if t["name"] in ("structural_parity", "schema_parity")), None)
    if structural is not None and structural["stats"].get("structural_checks"):
        checks = ", ".join(f"{k}={v}" for k, v in structural["stats"]["structural_checks"].items())
        lines.append(f"- Structural checks: {checks}")
    if result.get("snapshot") is not None:
        lines.append(f"- Snapshot provenance: `{json.dumps(result['snapshot'], default=str)}`")
    window = next((t for t in result["tiers"] if t["name"] == "consistency_window"), None)
    if window is not None:
        iso = window["stats"].get("isolation", {})
        in_flight = {o: m["in_flight_at_open"] for o, m in window["stats"].get("markers", {}).items()
                     if m.get("in_flight_at_open")}
        strength = window["stats"].get("strength", {})
        def side(name):
            how = strength.get(name)
            return f"`{iso.get(name)}`" + (f" ({how})" if how and how != "snapshot" else "")
        codes = {f["check"] for f in window["findings"]}
        state = "held" if window["passed"] else ("MOVED" if "window_unstable" in codes else "UNPROVEN")
        lines.append(f"- Consistency window: source isolation {side('source')}, target isolation "
                     f"{side('target')}, {state}"
                     + (f"; in flight at open: `{json.dumps(in_flight)}`" if in_flight else ""))
    if _rerun_line(result):
        lines.append(_rerun_line(result))
    for w in result.get("warnings", []):
        lines.append(f"- **WARNING: {w}**")
    lines += [
        "",
        "| Tier | Checks | Result |",
        "|---|---|---|",
    ]
    for t in result["tiers"]:
        lines.append(f"| {t['tier']} {t['name']} | {t['checks_run']} | "
                     f"{'PASS' if t['passed'] else 'FAIL (' + str(len(t['findings'])) + ')'} |")
    failing = [(t["tier"], f) for t in result["tiers"] for f in t["findings"]]
    if failing:
        lines += ["", f"Top findings ({min(len(failing), MAX_FINDINGS_IN_SUMMARY)} of {len(failing)}; full list in result.json):"]
        for tier, f in failing[:MAX_FINDINGS_IN_SUMMARY]:
            lines.append(f"- T{tier} `{f['object']}` {f['check']}: {f['detail']}")
    lines += ["", "Full evidence: result.json, report.md (linked from the PR, not pasted)."]
    return "\n".join(lines) + "\n"


def write_outputs(out_dir: Path, result: dict) -> tuple[Path, Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    rj = out_dir / "result.json"
    rm = out_dir / "report.md"
    rs = out_dir / "recon.summary.md"
    def atomic_write(path: Path, content: str) -> None:
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(content)
        tmp.replace(path)
    atomic_write(rj, json.dumps(result, indent=2, default=str) + "\n")
    atomic_write(rm, render_report(result))
    atomic_write(rs, render_summary(result))
    if result.get("mode") == "continuous":
        cycles = out_dir / "cycles"
        cycles.mkdir(parents=True, exist_ok=True)
        stamp = re.sub(r"[^A-Za-z0-9_.-]", "_", result["generated_at"])
        atomic_write(cycles / f"{stamp}.result.json",
                     json.dumps(result, indent=2, default=str) + "\n")
    return rj, rm, rs
