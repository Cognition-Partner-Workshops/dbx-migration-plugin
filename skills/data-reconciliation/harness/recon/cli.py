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
import re
import sys
import uuid as uuid_mod
from pathlib import Path

from . import canon, engine, report  # noqa: F401
from .config import (CanonRule, ConfigError, load_canon_rules, load_mapping_spec,
                     load_tolerances, validate_identifier)
from .engine import MODES, run_recon

SOURCE_FAMILIES = ("redshift", "snowflake", "teradata", "oracle", "sqlserver", "databricks")
PARAM_RE = re.compile(r"^[A-Za-z0-9_\-:.T /]*$")


def _single_identifier(value: str, option: str) -> str:
    try:
        validate_identifier(value)
    except ConfigError as exc:
        raise SystemExit(str(exc)) from None
    if "." in value:
        raise SystemExit(f"--{option} must be a single identifier segment")
    return value


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
    r.add_argument("--target-catalog", required=True)
    r.add_argument("--allowed-catalogs", required=True,
                   help="the migration catalog(s) recorded in .migration/00_context.md; "
                        "the run refuses any other --target-catalog")
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

    allowed_catalogs = [_single_identifier(value.strip(), "allowed-catalogs")
                        for value in args.allowed_catalogs.split(",") if value.strip()]
    if not allowed_catalogs:
        raise SystemExit("--allowed-catalogs must contain at least one catalog")
    target_catalog = _single_identifier(args.target_catalog, "target-catalog")
    target_schema = _single_identifier(args.target_schema, "target-schema")
    if target_catalog not in allowed_catalogs:
        raise SystemExit(f"--target-catalog {target_catalog!r} is not in --allowed-catalogs")

    params = {}
    for item in args.param:
        name, sep, value = item.partition("=")
        if not sep or not name:
            raise SystemExit(f"--param must be NAME=VALUE, got '{item}'")
        if not PARAM_RE.fullmatch(value):
            raise SystemExit(f"invalid --param value for {name}")
        params[name] = value
    spec = load_mapping_spec(args.mapping, params)
    tol = load_tolerances(args.tolerances)
    rules = load_canon_rules(args.canonicalization)

    ops = json.loads(args.ops.read_text()) if args.ops else None
    if ops:
        for op in ops:
            if not all(op.get(k) for k in ("name", "source_sql", "target_sql")):
                raise SystemExit(
                    f"ops entry missing required keys: {op.get('name', '?')}")
            for key in ("source_sql", "target_sql"):
                if not op[key].lstrip().lower().startswith(("select", "with")):
                    raise SystemExit(
                        f"op {op.get('name', '?')} SQL must be SELECT or WITH")
    from .adapters import SOURCE_ADAPTERS, DatabricksTargetAdapter

    source = SOURCE_ADAPTERS[args.family](args.source_dsn_secret)
    target = DatabricksTargetAdapter(args.target_secret, target_catalog, target_schema)
    run_source = (lambda op: source.run_query(op["source_sql"])) if ops else None
    run_target = (lambda op: target.run_query(op["target_sql"])) if ops else None
    result = run_recon(args.unit, args.mode, spec, tol, rules, source, target,
                       ops=ops, run_source=run_source, run_target=run_target,
                       out_dir=args.out, seed=args.seed, params=params)
    print(f"dbx-recon {result['verdict']}: unit={args.unit} mode={args.mode} "
          f"mapping={spec.version} tolerances={tol.version} merge_eligible={result['merge_eligible']} "
          f"-> {args.out}/result.json")
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
