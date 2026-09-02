"""Result rendering: one machine-readable result.json (the workflow gates on it), one
human report.md (read at wave close), and a ~30-line recon.summary.md sized for a PR body.
Every report cites mode, mapping version, and tolerance version so evidence is
re-runnable."""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

from .tiers import TierResult

MAX_FINDINGS_IN_REPORT = 50

MODE_NOTES = {
    "snapshot": " (PASS scoped to the snapshot watermark)",
    "fixture": " (fixture data: NOT a merge verdict, run live once before merging)",
}


def _mode_note(mode: str) -> str:
    return MODE_NOTES.get(mode, "")


def build_result(unit: str, mode: str, mapping_version: str, tolerance_version: str,
                 tiers: list[TierResult], seed: int = 0,
                 params: dict[str, str] | None = None,
                 snapshot: dict | None = None,
                 provenance_warnings: list[str] | None = None) -> dict:
    warnings = []
    for t in tiers:
        for path in t.stats.get("embeds_ungraded", []):
            warnings.append(f"UNGRADED embedded values: {path} (cardinality checked only; "
                            "declare embed key/fields in the mapping spec to grade values)")
    warnings.extend(provenance_warnings or [])
    verdict = "PASS" if all(t.passed for t in tiers) else "FAIL"
    merge_eligible = (verdict == "PASS" and mode in ("live", "snapshot")
                      and not warnings and (mode != "snapshot" or snapshot is not None))
    return {
        "unit": unit,
        "mode": mode,
        "mapping_version": mapping_version,
        "tolerance_version": tolerance_version,
        "seed": seed,
        "params": params or {},
        "snapshot": snapshot,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "tiers": [t.as_dict() for t in tiers],
        "warnings": warnings,
        "verdict": verdict,
        "merge_eligible": merge_eligible,
    }


def render_report(result: dict) -> str:
    lines = [
        f"# Recon report: unit `{result['unit']}`",
        "",
        f"- **Verdict: {result['verdict']}**",
        f"- Mode: `{result['mode']}`" + _mode_note(result["mode"]),
        f"- Merge eligible: {'yes' if result['merge_eligible'] else 'no'} "
        "(fixture/continuous evidence never merges)",
        f"- Mapping version: `{result['mapping_version']}`",
        f"- Tolerance version: `{result['tolerance_version']}`",
        f"- Seed: `{result.get('seed', 0)}`" + (f" | Params: `{result['params']}`"
                                                if result.get("params") else ""),
        f"- Generated: {result['generated_at']}",
    ]
    if result.get("snapshot") is not None:
        lines.append(f"- Snapshot provenance: `{json.dumps(result['snapshot'], default=str)}`")
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
        f"# Recon summary: `{result['unit']}` - **{result['verdict']}**",
        "",
        f"- Mode: `{result['mode']}`" + _mode_note(result["mode"]),
        f"- Merge eligible: {'yes' if result['merge_eligible'] else 'no'} "
        "(fixture/continuous evidence never merges)",
        f"- Mapping `{result['mapping_version']}` / tolerances `{result['tolerance_version']}`"
        f" / seed `{result.get('seed', 0)}`"
        + (f" / params `{result['params']}`" if result.get("params") else ""),
        f"- Generated: {result['generated_at']}",
    ]
    if result.get("snapshot") is not None:
        lines.append(f"- Snapshot provenance: `{json.dumps(result['snapshot'], default=str)}`")
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
