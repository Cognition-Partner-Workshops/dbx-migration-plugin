"""One active update per Lakeflow pipeline in a wave (rule in SKILL.md, "Write scope").

    python3 skills/target-routing/pipeline_updates.py .migration/waves/wave-N.json [--out FILE]

Reads the wave manifest's batches, each with the `lakeflow_pipelines` it updates, and reports every pipeline
two batches of the same wave declare. A shared pipeline passes only when the wave is serial (`width` 1) or
`serialized_pipelines` maps it to the plan decision id (a slug like `ser-p`) whose selected option records the
serialization order in plan.yaml; the result's `order` then lists, per batch, the earlier batches of the
manifest it must wait for, which the workflow enforces.
Standard library only; exit 0 pass, 1 halt, 2 unsupported.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

DECISION_ID = re.compile(r"[a-z0-9][a-z0-9_.-]*")


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
        _fail("serialized_pipelines must map each pipeline to the plan decision id slug that serialized it")
    return {k.strip().lower(): v.strip() for k, v in raw.items()}


def check_manifest(manifest: object) -> dict:
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
    ap.add_argument("--out")
    args = ap.parse_args(argv)
    manifest_path = Path(args.manifest)
    with open(manifest_path, encoding="utf-8") as fh:
        manifest = json.load(fh)
    result = check_manifest(manifest)
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
    print(text)
    return _EXIT[result["status"]]


if __name__ == "__main__":
    sys.exit(main())
