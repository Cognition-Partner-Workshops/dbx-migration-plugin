"""One active update per Lakeflow pipeline in a wave (rule in SKILL.md, "Write scope").

    python3 skills/target-routing/pipeline_updates.py .migration/waves/wave-N.json [--decisions FILE] [--out FILE]

Reads the wave manifest's batches, each with the `lakeflow_pipelines` it updates, and reports every pipeline
two batches of the same wave declare. A shared pipeline passes only when the wave is serial (`width` 1) or
`serialized_pipelines` maps it to the D-<n> row of .migration/06_decisions.md (beside the waves directory
unless --decisions says otherwise) tagged `pipeline_serialized` that names it; the result's `order` then
lists, per batch, the earlier batches of the manifest it must wait for, which the workflow enforces.
Standard library only; exit 0 pass, 1 halt, 2 unsupported.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

DECISION_ID = re.compile(r"D-[0-9]+")
SERIALIZED_WORD = "pipeline_serialized"


def _fail(msg: str) -> None:
    raise SystemExit(f"pipeline_updates: {msg}")


def _pipelines(batch: dict) -> list[str] | None:
    """Declared pipelines, lower-cased and validated; None when the batch declares nothing."""
    if "lakeflow_pipelines" not in batch:
        return None
    names = batch["lakeflow_pipelines"]
    if not isinstance(names, list) or not all(isinstance(n, str) and n.strip() for n in names):
        _fail(f"batch {batch['id']}: lakeflow_pipelines must be a list of non-empty names")
    low = [n.strip().lower() for n in names]
    if len(set(low)) != len(low):
        _fail(f"batch {batch['id']}: lakeflow_pipelines repeats a name")
    return low


def _serialized(manifest: dict) -> dict[str, str]:
    raw = manifest.get("serialized_pipelines", {})
    if not isinstance(raw, dict) or not all(
            isinstance(k, str) and isinstance(v, str) and DECISION_ID.fullmatch(v.strip()) for k, v in raw.items()):
        _fail("serialized_pipelines must map each pipeline to the D-<n> row of 06_decisions.md that serialized it")
    return {k.strip().lower(): v.strip() for k, v in raw.items()}


def _ledger_rows(ledger: str):
    """Each markdown table row of the decision ledger as (decision id, its cells); a line without a pipe is prose."""
    for line in ledger.splitlines():
        line = line.strip()
        if "|" not in line:
            continue
        cells = [" ".join(c.split()) for c in line.strip("|").split("|")]
        ids = [c for c in cells if DECISION_ID.fullmatch(c)]
        if ids:
            yield ids[0], cells


def _decision_serializes(decision_id: str, pipeline: str, ledger: str) -> bool:
    """Whether the ledger holds that row, tagged pipeline_serialized and naming this pipeline."""
    def token(word: str) -> str:
        return rf"(?<![A-Za-z0-9_.-]){re.escape(word)}(?![A-Za-z0-9_.-])"
    for row_id, cells in _ledger_rows(ledger):
        text = " | ".join(cells)
        if (row_id == decision_id and re.search(token(SERIALIZED_WORD), text)
                and re.search(token(pipeline), text, re.IGNORECASE)):
            return True
    return False


def check_manifest(manifest: object, ledger: str = "") -> dict:
    if not isinstance(manifest, dict) or not isinstance(manifest.get("batches"), list):
        _fail("manifest must be an object with a batches list")
    batches = manifest["batches"]
    ids = [b.get("id") if isinstance(b, dict) else None for b in batches]
    if not all(isinstance(i, str) and i for i in ids) or len(set(ids)) != len(ids):
        _fail("every batch needs a distinct string id")
    width = manifest.get("width", len(batches))
    serialized = _serialized(manifest)
    owners: dict[str, list[str]] = {}
    unchecked = []
    for b in batches:
        names = _pipelines(b)
        if names is None:
            unchecked.append(b["id"])
            continue
        for n in names:
            owners.setdefault(n, []).append(b["id"])
    shared, order = [], {}
    for name, bs in owners.items():
        if len(bs) < 2:
            continue
        ok = True if width == 1 else serialized.get(name, False)
        if ok is not True and ok is not False and not _decision_serializes(ok, name, ledger):
            _fail(f"serialized_pipelines names {ok} for {name!r} but 06_decisions.md has no such row tagged "
                  f"{SERIALIZED_WORD} that names the pipeline")
        shared.append({"pipeline": name, "batches": bs, "serialized": ok})
        if ok is not False:
            for earlier, later in zip(bs, bs[1:]):
                order.setdefault(later, set()).add(earlier)
    halt = any(s["serialized"] is False for s in shared)
    status = "halt" if halt else "unsupported" if unchecked else "pass"
    return {"wave": manifest.get("wave"), "width": width, "status": status, "shared": shared,
            "order": {b: sorted(deps) for b, deps in sorted(order.items())}, "unchecked_batches": unchecked}


_EXIT = {"pass": 0, "halt": 1, "unsupported": 2}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("manifest")
    ap.add_argument("--decisions", help="the decision ledger; default .migration/06_decisions.md beside the waves directory")
    ap.add_argument("--out")
    args = ap.parse_args(argv)
    manifest_path = Path(args.manifest)
    decisions = Path(args.decisions) if args.decisions else manifest_path.resolve().parent.parent / "06_decisions.md"
    with open(manifest_path, encoding="utf-8") as fh:
        manifest = json.load(fh)
    try:
        ledger = decisions.read_text(encoding="utf-8")
    except OSError:
        ledger = ""
    result = check_manifest(manifest, ledger)
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
    print(text)
    return _EXIT[result["status"]]


if __name__ == "__main__":
    sys.exit(main())
