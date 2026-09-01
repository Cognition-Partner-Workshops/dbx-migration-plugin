"""CLI: dbx-recon run --unit <id> --family <source> --mapping ... --tolerances ...

Secrets are passed by environment-variable NAME (--source-dsn-secret, --target-secret);
the harness reads the value from the environment and never accepts literals.

Exit code 0 = PASS, 1 = FAIL. The workflow script and the wave gate read result.json,
never this stdout line.
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import json
import sys
import uuid as uuid_mod
from pathlib import Path

from . import canon, engine, report  # noqa: F401
from .adapters import SOURCE_ADAPTERS, DatabricksTargetAdapter
from .config import CanonRule, load_canon_rules, load_mapping_spec, load_tolerances
from .engine import MODES, run_recon

SOURCE_FAMILIES = ("redshift", "snowflake", "teradata", "oracle", "sqlserver", "databricks")


def selftest() -> int:
    """Blueprint post-setup check: exercises every canonicalization rule on sample values
    and verifies the engine and report modules import. No database connections."""
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


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="dbx-recon")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("selftest", help="verify the harness install (no connections needed)")
    r = sub.add_parser("run", help="run the recon gate for one unit")
    r.add_argument("--unit", required=True)
    r.add_argument("--family", required=True, choices=SOURCE_FAMILIES)
    r.add_argument("--mapping", required=True, type=Path,
                   help="mapping spec JSON: source table -> target table, keys, fields")
    r.add_argument("--tolerances", required=True, type=Path,
                   help=".migration/03_tolerances.json, versioned")
    r.add_argument("--canonicalization", required=True, type=Path,
                   help="the source-dialect skill's recon_canonicalization rules, as JSON")
    r.add_argument("--mode", required=True, choices=MODES)
    r.add_argument("--source-dsn-secret", required=True,
                   help="ENV VAR NAME holding the source connection (read-only principal)")
    r.add_argument("--target-secret", required=True,
                   help="ENV VAR NAME holding Databricks SQL JSON "
                        "(convention: DATABRICKS_MIGRATION_SQL)")
    r.add_argument("--target-catalog", required=True, help="the migration catalog, never prod")
    r.add_argument("--target-schema", required=True)
    r.add_argument("--ops", type=Path, help="recorded representative queries for Tier 4")
    r.add_argument("--seed", type=int, default=0,
                   help="sampling seed (recorded in result.json for re-runnability)")
    r.add_argument("--param", action="append", default=[], metavar="NAME=VALUE",
                   help="resolve a ${name} placeholder in the mapping spec's where clauses "
                        "(e.g. partition/date scoping); repeatable; recorded in result.json")
    r.add_argument("--out", required=True, type=Path)
    args = p.parse_args(argv)

    if args.cmd == "selftest":
        return selftest()

    params = {}
    for item in args.param:
        name, sep, value = item.partition("=")
        if not sep or not name:
            raise SystemExit(f"--param must be NAME=VALUE, got '{item}'")
        params[name] = value
    spec = load_mapping_spec(args.mapping, params)
    tol = load_tolerances(args.tolerances)
    rules = load_canon_rules(args.canonicalization)

    source = SOURCE_ADAPTERS[args.family](args.source_dsn_secret)
    target = DatabricksTargetAdapter(args.target_secret, args.target_catalog, args.target_schema)

    ops = json.loads(args.ops.read_text()) if args.ops else None
    result = run_recon(args.unit, args.mode, spec, tol, rules, source, target,
                       ops=ops, out_dir=args.out, seed=args.seed, params=params)
    print(f"dbx-recon {result['verdict']}: unit={args.unit} mode={args.mode} "
          f"mapping={spec.version} tolerances={tol.version} -> {args.out}/result.json")
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
