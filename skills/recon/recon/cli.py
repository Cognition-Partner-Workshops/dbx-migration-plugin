"""dbx-recon: the slice recon gate (design §8, §9).

`run` reads `.migration/recon/<unit>/inputs/` (mapping, dependencies, canonicalization,
grading, rerun_posture) plus `.migration/policy.json` (source.access, targets, tolerances)
and `.migration/slices.json` (the slice's declared routines and track), and writes only
under `--out .migration/recon/<unit>/out/` (result.json, report.md, cycles/ for continuous).
The Databricks adapter reads `source.access` in every mode except `live`, where the native
family adapter reads the legacy engine through `source.secret`. Exit 0 PASS, 1 FAIL,
3 NOT_RUN."""
from __future__ import annotations

import argparse
import datetime as dt
import decimal
import json
import os
import re
import sys
import uuid as uuid_mod
from dataclasses import replace
from pathlib import Path

from . import canon
from .adapters import (DatabricksSourceAdapter, DatabricksTargetAdapter,
                       LakebaseTargetAdapter, OracleSourceAdapter, PostgresSourceAdapter,
                       SqlServerSourceAdapter, TargetIdentityError)
from .config import (CanonRule, ConfigError, load_policy, load_slice_inputs, load_slices,
                     slice_entry, source_table_name, tolerances_from_dict, validate_identifier)
from .engine import DEPTHS, MODES, run_recon
from .report import status_line
from .rerun import check_proof, grade_rerun, load_prior, load_record, source_digest
from .routines import check_parity, git_committed, grade_routines, load_runs, writers

SOURCE_FAMILIES = ("oracle", "sqlserver", "postgres", "databricks")
MIGRATION = Path(".migration")


def parse_params(items: list[str]) -> dict[str, str]:
    params = {}
    for item in items or []:
        name, sep, value = item.partition("=")
        if not sep:
            raise ConfigError(f"--param must be NAME=VALUE, got {item!r}")
        params[name] = value
    return params


def selftest() -> int:
    samples = {
        "decimal_round": decimal.Decimal("1.23456789012"),
        "datetime_utc_truncate_ms": dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc),
        "datetime_grid_333": dt.datetime(2000, 1, 1, 0, 0, 0, 3000, tzinfo=dt.timezone.utc),
        "rstrip_spaces": "x  ",
        "empty_string_is_null": "",
        "null_missing_equiv": canon.MISSING,
        "collation_casefold": "ABC",
        "uuid_normalize": uuid_mod.uuid4(),
        "identity": 1,
    }
    c = canon.Canonicalizer([CanonRule(rule=name, applies_to="*", params={})
                             for name in samples])
    for name, value in samples.items():
        c.apply(value, [name])
    print(f"dbx-recon selftest PASS: {len(samples)} canonicalization rules exercised")
    return 0


def _slice_paths(unit: str) -> tuple[Path, Path]:
    return MIGRATION / "recon" / unit / "inputs", MIGRATION / "recon" / unit / "out"


def _out_dir(unit: str, arg: Path) -> Path:
    _, out = _slice_paths(unit)
    resolved = arg.resolve()
    want = out.resolve()
    if resolved != want and want not in resolved.parents:
        raise ConfigError(f"--out must be under {out}/, got {arg}")
    return arg


def _target_schema(unit: str, targets: dict) -> str:
    return (targets.get("schema_prefix") or "") + re.sub(r"[^A-Za-z0-9_]", "_", unit)


def _target(policy: dict, entry: dict | None, unit: str):
    targets = policy["targets"]
    track = (entry or {}).get("track", "delta")
    schema = _target_schema(unit, targets)
    if track == "lakebase":
        lb = targets.get("lakebase") or {}
        database = lb.get("database", "databricks_postgres")
        return "lakebase", LakebaseTargetAdapter("LAKEBASE_MIGRATION_DSN", database, schema)
    catalog = targets.get("catalog")
    if not catalog:
        raise ConfigError("policy.targets.catalog is required for the delta track")
    return "databricks", DatabricksTargetAdapter(
        targets.get("warehouse_http_path") or os.environ.get("DATABRICKS_HTTP_PATH"),
        catalog, schema)


def _source(policy: dict, family: str | None, mode: str):
    src = policy["source"]
    if mode == "live":
        fam = family or src["family"]
        if fam == "databricks":
            return DatabricksSourceAdapter(
                policy["targets"].get("warehouse_http_path")
                or os.environ.get("DATABRICKS_HTTP_PATH")), fam
        adapters = {"oracle": OracleSourceAdapter, "sqlserver": SqlServerSourceAdapter,
                    "postgres": PostgresSourceAdapter}
        if fam not in adapters:
            raise ConfigError(f"mode live has no native adapter for family {fam!r}")
        return adapters[fam](src["secret"]), fam
    return DatabricksSourceAdapter(
        policy["targets"].get("warehouse_http_path")
        or os.environ.get("DATABRICKS_HTTP_PATH")), family or src["family"]


def _qualify_source(spec, access: str):
    objects = []
    for c in spec.objects:
        objects.append(replace(
            c, root_table=source_table_name(access, c.root_table),
            embeds=[replace(e, child_table=source_table_name(access, e.child_table))
                    for e in c.embeds]))
    return replace(spec, objects=objects)


def _validate_ops(ops) -> None:
    for op in ops:
        if not isinstance(op, dict) or not op.get("source_sql") or not op.get("target_sql"):
            raise ConfigError(
                f"ops entry missing required keys: {op.get('name', '?')}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="dbx-recon")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("selftest")

    rp = sub.add_parser("routine-parity")
    rp.add_argument("--unit", required=True)
    rp.add_argument("--runs", type=Path, help="run records dir (default <out>/runs)")
    rp.add_argument("--repo", type=Path, default=Path("."))
    rp.add_argument("--out", type=Path)

    pf = sub.add_parser("rerun-proof")
    pf.add_argument("--unit", required=True)
    pf.add_argument("--source", action="append", default=[], type=Path)
    pf.add_argument("--ddl", type=Path)
    pf.add_argument("--prior-proof", type=Path)
    pf.add_argument("--prior-shape", type=Path)
    pf.add_argument("--fresh", required=True, type=Path)
    pf.add_argument("--evolved", type=Path)
    pf.add_argument("--out", type=Path)

    sh = sub.add_parser("shape")
    sh.add_argument("--unit", required=True)
    sh.add_argument("--table", action="append", required=True)
    sh.add_argument("--out", required=True, type=Path)

    r = sub.add_parser("run")
    r.add_argument("--unit", required=True)
    r.add_argument("--family", choices=SOURCE_FAMILIES)
    r.add_argument("--mode", required=True, choices=MODES)
    r.add_argument("--depth", choices=DEPTHS, default="threshold")
    r.add_argument("--seed", type=int, default=0)
    r.add_argument("--param", action="append", default=[], metavar="NAME=VALUE")
    r.add_argument("--out", required=True, type=Path)

    args = p.parse_args(argv)

    try:
        return _dispatch(args)
    except (ConfigError, TargetIdentityError, ValueError, OSError, json.JSONDecodeError) as exc:
        print(f"dbx-recon: {exc}", file=sys.stderr)
        return 3


def _dispatch(args) -> int:
    if args.cmd == "selftest":
        return selftest()

    inputs, default_out = _slice_paths(args.unit)

    if args.cmd == "routine-parity":
        out = _out_dir(args.unit, args.out or default_out)
        deps = None
        deps_path = inputs / "dependencies.json"
        if deps_path.exists():
            deps = json.loads(deps_path.read_text())
        else:
            raise ConfigError(f"{deps_path} not found")
        runs = args.runs or (out / "runs")
        result = grade_routines(deps, load_runs(runs, args.repo), git_committed(args.repo))
        out.mkdir(parents=True, exist_ok=True)
        (out / "routine_parity.json").write_text(json.dumps(result, indent=2) + "\n")
        parity = result["routine_parity"]
        counts = {s: sum(1 for r in parity if r["status"] == s)
                  for s in ("proven", "unproven", "failed")}
        print(f"dbx-recon routine-parity: {counts['proven']} proven, {counts['unproven']} "
              f"unproven, {counts['failed']} failed -> {out}/routine_parity.json")
        return 1 if counts["failed"] else 2 if counts["unproven"] else 0

    if args.cmd == "rerun-proof":
        out = _out_dir(args.unit, args.out or default_out)
        if not args.source:
            raise ConfigError("rerun-proof needs --source <file> (repeatable)")
        if args.prior_proof is not None and args.prior_shape is not None:
            raise ConfigError("rerun-proof takes --prior-proof or --prior-shape, not both")
        prior_path = args.prior_proof or args.prior_shape
        prior = (load_prior(prior_path, args.unit, proof=args.prior_proof is not None)
                 if prior_path is not None else None)
        proof = grade_rerun(load_record(args.fresh, "fresh"),
                            load_record(args.evolved, "evolved") if args.evolved else None,
                            prior, digest=source_digest(args.source),
                            ddl=args.ddl.read_text() if args.ddl is not None else None)
        proof = {"unit": args.unit, "sources": [str(s) for s in args.source],
                 **({"prior_from": str(prior_path)} if prior is not None else {}),
                 **proof}
        out.mkdir(parents=True, exist_ok=True)
        (out / "rerun_proof.json").write_text(json.dumps(proof, indent=2) + "\n")
        print(json.dumps(proof))
        return 0 if proof["passed"] else 1

    policy = load_policy(MIGRATION / "policy.json")
    entry = slice_entry(load_slices(MIGRATION / "slices.json"), args.unit)

    if args.cmd == "shape":
        _, target = _target(policy, entry, args.unit)
        tables = {}
        for t in args.table:
            validate_identifier(t)
            tables[t] = target.column_shape(t)
        absent = [t for t, cols in tables.items() if not cols and not target.table_exists(t)]
        if absent:
            raise ConfigError(f"table {', '.join(absent)} not found in the slice target")
        shape = {"target_kind": target.target_kind,
                 "read_at": dt.datetime.now(dt.timezone.utc).isoformat(), "tables": tables}
        out = _out_dir(args.unit, args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(shape, indent=2) + "\n")
        print(json.dumps({"tables": {t: len(c) for t, c in tables.items()}, "out": str(out)}))
        return 0

    # run
    params = parse_params(args.param)
    spec, deps, rules, ops, snapshot = load_slice_inputs(inputs, params)
    out = _out_dir(args.unit, args.out)
    source, family = _source(policy, args.family, args.mode)
    if args.mode != "live" or family == "databricks":
        spec = _qualify_source(spec, policy["source"]["access"])
    kind, target = _target(policy, entry, args.unit)
    if ops:
        _validate_ops(ops)
        run_source = lambda op: source.run_query(op["source_sql"])
        run_target = lambda op: target.run_query(op["target_sql"])
    else:
        run_source = run_target = None
    tol = tolerances_from_dict(policy.get("tolerances") or {}, MIGRATION / "policy.json")

    routine_parity = None
    rp_path = out / "routine_parity.json"
    if rp_path.exists():
        parity_data = json.loads(rp_path.read_text())
        rows = parity_data.get("routine_parity") if isinstance(parity_data, dict) else parity_data
        routine_parity = check_parity(
            rows, str(rp_path), deps,
            committed=git_committed(Path(".")), repo=Path("."))
    routine_writers = sorted(set((entry or {}).get("routines") or []) |
                             set(writers(deps))) if deps else (entry or {}).get("routines") or []
    rerun_proof = None
    proof_path = out / "rerun_proof.json"
    if proof_path.exists():
        proof = json.loads(proof_path.read_text())
        sources = proof.get("sources") or []
        digest = source_digest([Path(s) for s in sources]) if sources else ""
        if not sources:
            raise ConfigError(f"{proof_path}: the proof records no job sources to re-digest")
        rerun_proof = check_proof(proof, args.unit, str(proof_path), digest)

    result = run_recon(args.unit, args.mode, spec, tol, rules, source, target,
                       ops=ops, run_source=run_source, run_target=run_target,
                       out_dir=out, seed=args.seed,
                       params=params, snapshot=snapshot, source_family=family, depth=args.depth,
                       routine_parity=routine_parity,
                       routine_writers=routine_writers,
                       routine_analysis_missing=deps is None,
                       routine_dependencies=str(inputs / "dependencies.json") if deps else None,
                       rerun_proof=rerun_proof)
    print(status_line(result))
    return {"PASS": 0, "FAIL": 1, "NOT_RUN": 3}[result["parity"]]
