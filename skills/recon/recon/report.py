"""The result.json contract (design §8): parity PASS|FAIL|NOT_RUN, mode, tiers,
routine_parity, findings[] classified structural|privilege_visibility|evidence|rerun_policy,
cost. A finding blocks only when its class is listed in tolerances.blocking_findings; row
differences are parity FAIL, not a finding."""
from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

from .rerun import (rerun_first_run_baseline, rerun_gap, rerun_missing, rerun_unsupported)
from .routines import parity_missing, routine_gap
from .tiers import TierResult, blocking

MAX_FINDINGS_IN_REPORT = 50

STRUCTURAL_TIERS = ("structural_parity", "schema_parity")
EVIDENCE_TIERS = ("consistency_window",)
EVIDENCE_CHECKS = frozenset({"cdc_lag_exceeded", "cdc_in_flight_exceeded", "cdc_watermark_incomparable",
                             "cdc_lag_ungraded", "delete_lag_exceeded", "delete_evidence_retention_gap",
                             "delete_evidence_unusable", "aggregates_ungraded_in_flight"})
PRIVILEGE_CHECKS = frozenset({"trigger_extra", "grant_extra"})
MERGE_MODES = ("live", "snapshot", "transactional")


def data_failed(tier: TierResult) -> bool:
    return not tier.passed and (not tier.findings or any(f.check not in EVIDENCE_CHECKS
                                                       for f in tier.findings))


def _find(cls: str, code: str, obj: str, detail: str, blocking_classes: tuple,
          severity: str = "error") -> dict:
    return {"class": cls, "code": code, "object": obj, "detail": detail,
            "blocking": severity != "info" and cls in blocking_classes}


def _unverified_keys(stats: dict) -> list[str]:
    return [k for k in stats
            if k.endswith("_unverified") or k.endswith("_unverifiable") or k == "index_unsupported"]


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
                 rerun_posture: str = "required",
                 grading: dict[str, str] | None = None,
                 blocking_findings: tuple = ()) -> dict:
    grading = dict(grading or {})
    warnings: list[str] = []
    findings: list[dict] = []
    blocking_classes = tuple(blocking_findings or ())

    def add(cls, code, obj, detail, severity="error"):
        findings.append(_find(cls, code, obj, detail, blocking_classes, severity))

    structural = next((t for t in tiers if t.name in STRUCTURAL_TIERS), None)
    data_tiers = [t for t in tiers if t.name not in STRUCTURAL_TIERS + EVIDENCE_TIERS]

    for t in tiers:
        for path in t.stats.get("embeds_ungraded", []):
            note = (f"{path} (cardinality checked only; declare embed key/fields in the "
                    "mapping spec to grade values)")
            warnings.append(f"UNGRADED embedded values: {note}")
            add("evidence", "embeds_ungraded", str(path), note)
        if t.name in STRUCTURAL_TIERS:
            for f in t.findings:
                cls = "privilege_visibility" if f.check in PRIVILEGE_CHECKS else "structural"
                add(cls, f.check, f.object, f.detail, f.severity)
            hole_kinds = set(t.stats.get("hole_kinds") or [])
            for key in ("unverified", "dictionary_unavailable"):
                for note in t.stats.get(key, []):
                    cls = "privilege_visibility" if hole_kinds == {"privilege"} else "evidence"
                    add(cls, key, t.name, str(note))
            for obj, sides in t.stats.items():
                if not isinstance(sides, dict):
                    continue
                for side in ("source", "target"):
                    cats = (sides.get(side) or {}).get("privilege_denied") if isinstance(sides.get(side), dict) else None
                    if cats:
                        add("privilege_visibility", f"{side}_privilege_denied", str(obj),
                            f"{side} dictionary refused {sorted(cats)}")
            for key in _unverified_keys(t.stats):
                for note in t.stats.get(key) or []:
                    add("evidence", key, t.name, str(note))
            checks_map = t.stats.get("structural_checks") or {}
            blind = [c for c, v in checks_map.items() if v == "unsupported" and c != "indexes"]
            if blind:
                add("evidence", "structural_blind", t.name,
                    f"structural categories not checkable: {blind}")
        else:
            for f in t.findings:
                if t.name in EVIDENCE_TIERS or f.check in EVIDENCE_CHECKS:
                    add("evidence", f.check, f.object, f.detail, f.severity)

    unlisted = parity_missing(routine_parity, routine_writers)
    unproven = [r.get("routine") for r in (routine_parity or []) if r.get("status") == "unproven"]
    failed_routines = [r.get("routine") for r in (routine_parity or []) if r.get("status") == "failed"]

    if routine_analysis_missing:
        add("evidence", "routine_analysis_missing", unit,
            "no routine dependency analysis committed for the slice")
    for w in unlisted:
        add("evidence", "routine_parity_missing", str(w), "writing routine has no routine_parity row")
    for r in unproven:
        add("evidence", "routine_unproven", str(r), "routine_parity row is unproven")
    for r in failed_routines:
        add("evidence", "routine_failed", str(r), "routine_parity row failed")

    for obj, g in grading.items():
        if g == "by_design_unverified":
            add("evidence", "by_design_unverified", str(obj),
                "object graded by_design_unverified: values not graded")

    for w in provenance_warnings or []:
        warnings.append(str(w))
        add("evidence", "provenance", unit, str(w))

    if mode not in MERGE_MODES:
        add("evidence", "mode", unit, f"mode {mode!r} is not merge evidence")
    if mode == "snapshot" and snapshot is None:
        add("evidence", "snapshot_missing", unit, "mode snapshot without a snapshot manifest")

    if mode != "structural":
        if rerun_missing(rerun_proof) and rerun_posture != "not_applicable":
            add("rerun_policy", "rerun_missing", unit, "no rerun proof for this slice")
        if rerun_gap(rerun_proof):
            add("rerun_policy", "rerun_gap", unit, "rerun proof failed")
        if rerun_unsupported(rerun_proof) and (
                rerun_posture == "required"
                or (rerun_posture == "first_run_baseline"
                    and not rerun_first_run_baseline(rerun_proof))):
            add("rerun_policy", "rerun_unsupported", unit,
                f"rerun proof unsupported under posture {rerun_posture}")

    if not data_tiers or routine_analysis_missing or unlisted or unproven:
        parity = "NOT_RUN"
    elif any(data_failed(t) for t in data_tiers) or routine_gap(routine_parity):
        parity = "FAIL"
    else:
        parity = "PASS"

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
        "parity": parity,
        "findings": findings,
        "routine_parity": routine_parity,
        "routine_writers": routine_writers,
        "routine_dependencies": routine_dependencies,
        **({"routine_analysis_missing": True} if routine_analysis_missing else {}),
        "grading": grading,
        "type_map": type_map,
        "rerun_proof": rerun_proof,
        "rerun_posture": rerun_posture,
    }


MODE_NOTES = {
    "snapshot": " (PASS scoped to the snapshot watermark)",
    "fixture": " (fixture data: development evidence, the verifier refuses it)",
    "transactional": " (both sides live: PASS scoped to the consistency window that held and the "
                     "target's applied CDC watermark)",
}


def _mode_note(mode: str) -> str:
    return MODE_NOTES.get(mode, "")


def status_line(result: dict) -> str:
    return f"PARITY {result['parity']} (mode {result['mode']})"


def _rerun_line(result: dict) -> str | None:
    proof = result.get("rerun_proof")
    if not isinstance(proof, dict):
        return None
    return (f"- Rerun proof: passed={proof.get('passed')} fresh={proof.get('fresh')} "
            f"evolved={proof.get('evolved')}")


def _parity_lines(result: dict) -> list[str]:
    parity = result.get("routine_parity")
    if not parity:
        if result.get("routine_analysis_missing"):
            return ["", "## Routine parity: no dependency analysis "
                        "(.migration/recon/<slice>/inputs/dependencies.json)"]
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


def _finding_lines(result: dict) -> list[str]:
    findings = result.get("findings") or []
    if not findings:
        return []
    lines = ["", "## Findings", ""]
    for f in findings[:MAX_FINDINGS_IN_REPORT]:
        lines.append(f"- [{f['class']}{' BLOCKING' if f['blocking'] else ''}] "
                     f"`{f['object']}` {f['code']}: {f['detail']}")
    if len(findings) > MAX_FINDINGS_IN_REPORT:
        lines.append(f"- ... {len(findings) - MAX_FINDINGS_IN_REPORT} more in result.json")
    return lines


def render_report(result: dict) -> str:
    lines = [
        f"# Recon report: unit `{result['unit']}`",
        "",
        f"- **{status_line(result)}**",
        f"- Mode: `{result['mode']}`" + _mode_note(result["mode"]),
        f"- Rerun posture: `{result.get('rerun_posture', 'required')}`",
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
    lines += _finding_lines(result)
    for w in result.get("warnings", []):
        lines.append(f"- **WARNING: {w}**")
    lines += [
        "",
        "| Tier | Name | Checks | Result |",
        "|---|---|---|---|",
    ]
    for t in result["tiers"]:
        info = sum(f.get("severity") == "info" for f in t["findings"])
        failing = len(t["findings"]) - info
        lines.append(f"| {t['tier']} | {t['name']} | {t['checks_run']} | "
                     f"{'PASS' if t['passed'] else 'FAIL (' + str(failing) + ' findings)'}"
                     f"{' + ' + str(info) + ' info' if info else ''} |")
    for t in result["tiers"]:
        if t.get("stats"):
            lines += ["", f"## Tier {t['tier']} coverage", "```json",
                      json.dumps(t["stats"], indent=2, default=str), "```"]
        if t["findings"]:
            lines += ["", f"## Tier {t['tier']} findings ({len(t['findings'])})"]
            for f in t["findings"][:MAX_FINDINGS_IN_REPORT]:
                lines.append(f"- `{f['object']}` {f['check']}"
                             f"{' [info]' if f.get('severity') == 'info' else ''}: {f['detail']}"
                             + (f" | source={f['source_value']} target={f['target_value']}"
                                f" | rules={f['rules_applied']}"
                                if f["check"] in ("field_diff", "embed_field_diff",
                                                  "aggregate_min", "aggregate_max",
                                                  "aggregate_sum", "aggregate_null_rate",
                                                  "aggregate_distinct_count") else ""))
            if len(t["findings"]) > MAX_FINDINGS_IN_REPORT:
                lines.append(f"- ... {len(t['findings']) - MAX_FINDINGS_IN_REPORT} more in result.json")
    return "\n".join(lines) + "\n"


def write_outputs(out_dir: Path, result: dict) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    rj = out_dir / "result.json"
    rm = out_dir / "report.md"

    def atomic_write(path: Path, content: str) -> None:
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(content)
        tmp.replace(path)

    atomic_write(rj, json.dumps(result, indent=2, default=str) + "\n")
    atomic_write(rm, render_report(result))
    if result.get("mode") == "continuous":
        cycles = out_dir / "cycles"
        cycles.mkdir(parents=True, exist_ok=True)
        stamp = re.sub(r"[^A-Za-z0-9_.-]", "_", result["generated_at"])
        atomic_write(cycles / f"{stamp}.result.json",
                     json.dumps(result, indent=2, default=str) + "\n")
    return rj, rm
