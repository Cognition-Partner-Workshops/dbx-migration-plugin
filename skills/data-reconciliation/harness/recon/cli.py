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
import os
import re
import sys
import uuid as uuid_mod
from pathlib import Path

from . import canon, engine, report  # noqa: F401
from .config import (
    READ_ONLY_SQL_KEYWORDS,
    CanonRule,
    ConfigError,
    load_canon_rules,
    load_mapping_spec,
    load_tolerances,
    validate_identifier,
)
from .cost import estimate_cost
from .engine import DEPTHS, MODES, PLANNED_MODES, run_recon
from .fixture_shape import compare_fixture
from .rerun import check_proof, grade_rerun, load_prior, load_record, source_digest
from .typemap import apply_type_map, load_type_map

SOURCE_FAMILIES = ("redshift", "snowflake", "teradata", "oracle", "sqlserver", "databricks", "postgres")
# databricks: Delta under Unity Catalog (analytical track). lakebase: a schema in a Lakebase
# branch database (operational track); --target-catalog then names the branch database and
# must still appear in .migration/allowed_targets.json.
TARGET_KINDS = ("databricks", "lakebase")
# --mode transactional grades two live sides; a Delta target is loaded, not replicated, so
# only the operational target accepts it.
TRANSACTIONAL_TARGET_KINDS = ("lakebase",)
# a --param value is one literal: a number, identifier, date or date + time; never an expression
PARAM_RE = re.compile(r"^[A-Za-z0-9_\-:.T/]+(?: [0-9:.]+)?$")


def _single_identifier(value: str, option: str) -> str:
    try:
        validate_identifier(value)
    except ConfigError as exc:
        raise SystemExit(str(exc)) from None
    if "." in value:
        raise SystemExit(f"--{option} must be a single identifier segment")
    return value


def _load_allowed_targets(path: Path) -> list[str]:
    path = path.resolve()
    if path != Path(".migration/allowed_targets.json").resolve():
        raise SystemExit("--allowed-targets-file must resolve to .migration/allowed_targets.json")
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read allowlist file {path}: {exc}") from None
    values = data.get("catalogs") if isinstance(data, dict) else None
    if not isinstance(values, list) or not values:
        raise SystemExit(f"{path} must contain a non-empty 'catalogs' list")
    return [_single_identifier(value, "allowed-targets-file") for value in values]


def _validate_sql(sql: str, name: str) -> None:
    stripped = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    stripped = re.sub(r"--[^\r\n]*", " ", stripped)
    if ";" in stripped or READ_ONLY_SQL_KEYWORDS.search(stripped):
        raise SystemExit(f"op {name} SQL must be a single read-only SELECT or WITH query")
    if not stripped.lstrip().lower().startswith(("select", "with")):
        raise SystemExit(f"op {name} SQL must be SELECT or WITH")


def _load_snapshot(path: Path | None, mode: str) -> dict | None:
    if mode == "snapshot" and path is None:
        raise SystemExit("--snapshot-manifest is required when --mode snapshot")
    if path is None:
        return None
    path = path.resolve()
    if path.suffix != ".json" or not path.is_relative_to(Path(".migration/snapshots").resolve()):
        raise SystemExit("--snapshot-manifest must be a .json file inside .migration/snapshots")
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read snapshot manifest {path}: {exc}") from None
    if not isinstance(data, dict) or not all(key in data for key in ("source", "extracted_at", "row_counts")):
        raise SystemExit(f"{path} must contain source, extracted_at, and row_counts")
    try:
        dt.datetime.fromisoformat(str(data["extracted_at"]).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise SystemExit(f"{path} extracted_at must be ISO-8601") from None
    if not isinstance(data["row_counts"], dict) or any(
            isinstance(value, bool) or not isinstance(value, int)
            for value in data["row_counts"].values()):
        raise SystemExit(f"{path} row_counts must be a dictionary of integer counts")
    return {key: data[key] for key in ("source", "extracted_at", "row_counts")}


def parse_params(items: list[str]) -> dict[str, str]:
    params = {}
    for item in items:
        name, sep, value = item.partition("=")
        if not sep or not name:
            raise SystemExit(f"--param must be NAME=VALUE, got '{item}'")
        if not PARAM_RE.fullmatch(value):
            raise SystemExit(f"invalid --param value for {name}")
        params[name] = value
    return params


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


def _load_spec(mapping, canonicalization, family, target_kind, params):
    """Mapping spec with the family's type map applied for the given target kind — the same
    shape `run` reconciles, so `estimate` counts the statements the run will issue."""
    spec = load_mapping_spec(mapping, params)
    type_map = None
    if canonicalization and family:
        try:
            tm = load_type_map(canonicalization, family, target_kind)
            if tm:
                spec, type_map = apply_type_map(tm, spec)
        except ConfigError as exc:
            raise SystemExit(f"type map: {exc}") from None
    return spec, type_map


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="dbx-recon")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("selftest", help="verify the harness install (no connections needed)")
    sub.add_parser("families", help="print the live-tested vs refused source families as JSON")
    d = sub.add_parser("dictionary-objects", help="print the catalog objects a family's "
                       "dictionary readers probe as JSON (doctor's dictionary_readable table)")
    d.add_argument("--family", required=True)
    t = sub.add_parser("type-map-audit", help="audit a spec's declared target types against the "
                       "family type_map (JSON, no connections)")
    t.add_argument("--spec", required=True, type=Path)
    t.add_argument("--family", required=True)
    t.add_argument("--target-kind", default="databricks", choices=TARGET_KINDS)
    t.add_argument("--canonicalization", action="append", type=Path, default=[])
    t.add_argument("--param", action="append", default=[])
    e = sub.add_parser("estimate", help="statements/rows a run would cost (no connections); "
                                        "summed per wave for the STOP C cost line")
    e.add_argument("--mapping", required=True, type=Path)
    e.add_argument("--tolerances", required=True, type=Path)
    e.add_argument("--depth", choices=DEPTHS, default="threshold")
    e.add_argument("--row-counts", type=Path,
                   help="JSON {root_table: rows} from the analysis inventory; without it row "
                        "transfer is reported as unknown")
    e.add_argument("--ops-count", type=int, default=0, help="number of Tier 4 recorded ops")
    e.add_argument("--mode", choices=MODES, default="live",
                   help="transactional adds the window, PK-set and schema-parity statements")
    e.add_argument("--family", choices=SOURCE_FAMILIES,
                   help="source engine; with --canonicalization its type_map is applied so the "
                        "estimate counts the statements the run will actually issue")
    e.add_argument("--canonicalization", type=Path,
                   help="the source-dialect skill's canonicalization.json (optional; fills "
                        "undeclared target types like `run` does)")
    e.add_argument("--target-kind", choices=TARGET_KINDS, default="databricks")
    e.add_argument("--param", action="append", default=[], metavar="NAME=VALUE")
    fs = sub.add_parser("fixture-shape", help="wave 0: compare the fixture copy's column shape "
                        "and sample cardinality with the real source (read-only, capped); "
                        "writes <out>/fixture_shape.json")
    fs.add_argument("--family", required=True, choices=SOURCE_FAMILIES)
    fs.add_argument("--mapping", required=True, type=Path)
    fs.add_argument("--source-dsn-secret", required=True,
                    help="ENV VAR NAME holding the real source connection (read-only principal)")
    fs.add_argument("--fixture-dsn-secret", required=True,
                    help="ENV VAR NAME holding the fixture copy's connection (same engine)")
    fs.add_argument("--source-statement-cap", required=True, type=int,
                    help="most statements this check may issue against the real source; the "
                         "wave's legacy-query cap share for wave 0")
    fs.add_argument("--param", action="append", default=[], metavar="NAME=VALUE")
    fs.add_argument("--out", required=True, type=Path)
    rp = sub.add_parser("rerun-proof", help="grade the schema-evolution rerun proof from the "
                        "child's two run records (no connections); writes <out>/rerun_proof.json")
    rp.add_argument("--unit", required=True)
    rp.add_argument("--source", action="append", default=[], type=Path,
                    help="a source file of the job under proof (DDL, notebook, SQL); repeatable; the "
                         "proof digests them so any later edit makes it stale")
    rp.add_argument("--ddl", type=Path,
                    help="optional DDL hint: tables it creates that the fresh run did not record are notes")
    rp.add_argument("--prior-proof", type=Path,
                    help="the previously committed rerun_proof.json; its observed shape is what the "
                         "evolved run's pre_shape must equal")
    rp.add_argument("--prior-shape", type=Path,
                    help="shape JSON instead of --prior-proof (first run: the manifest-declared old shape)")
    rp.add_argument("--fresh", required=True, type=Path,
                    help="run record from the fresh-target run (dbx-recon shape after the job)")
    rp.add_argument("--evolved", type=Path,
                    help="run record from the run against the table pre-created in its previous "
                         "committed shape, with pre_shape read before the job; omitted = unsupported")
    rp.add_argument("--out", required=True, type=Path)
    sh = sub.add_parser("shape", help="read the observed column shape of target tables into a "
                        "shape JSON (read-only; the rerun proof's record input)")
    sh.add_argument("--target-kind", choices=TARGET_KINDS, default="databricks")
    sh.add_argument("--target-secret", required=True)
    sh.add_argument("--target-catalog", required=True)
    sh.add_argument("--allowed-targets-file", type=Path,
                    default=Path(".migration/allowed_targets.json"))
    sh.add_argument("--target-schema", required=True)
    sh.add_argument("--table", action="append", required=True)
    sh.add_argument("--out", required=True, type=Path)
    r = sub.add_parser("run", help="run the recon gate for one unit")
    r.add_argument("--unit", required=True)
    r.add_argument("--family", required=True, choices=SOURCE_FAMILIES)
    r.add_argument("--mapping", required=True, type=Path,
                   help="mapping spec JSON: source table -> target table, keys, fields")
    r.add_argument("--tolerances", required=True, type=Path,
                   help=".migration/03_tolerances.json, versioned")
    r.add_argument("--canonicalization", required=True, type=Path,
                   help="the source-dialect skill's recon_canonicalization rules, as JSON")
    r.add_argument("--mode", required=True, choices=MODES + PLANNED_MODES)
    r.add_argument("--source-dsn-secret", required=True,
                   help="ENV VAR NAME holding the source connection (read-only principal)")
    r.add_argument("--target-kind", choices=TARGET_KINDS, default="databricks")
    r.add_argument("--target-secret", required=True,
                   help="ENV VAR NAME holding Databricks SQL JSON (convention: "
                        "DATABRICKS_MIGRATION_SQL) or, for --target-kind lakebase, the branch "
                        "endpoint's libpq DSN (convention: LAKEBASE_MIGRATION_DSN)")
    r.add_argument("--target-catalog", required=True,
                   help="Unity Catalog catalog, or the Lakebase branch database name")
    r.add_argument("--allowed-targets-file", type=Path,
                   default=Path(".migration/allowed_targets.json"))
    r.add_argument("--target-schema", required=True)
    r.add_argument("--ops", type=Path, help="recorded representative queries for Tier 4")
    r.add_argument("--snapshot-manifest", type=Path)
    r.add_argument("--source-dictionary", type=Path,
                   help="fixture dictionary JSON (harness/fixtures/example_<family>/dictionary.json): "
                        "structural facts read from the file, not the live catalog; never merge-eligible")
    r.add_argument("--target-dictionary", type=Path,
                   help="same, for the target side")
    r.add_argument("--seed", type=int, default=0,
                   help="sampling seed (recorded in result.json for re-runnability)")
    r.add_argument("--depth", choices=DEPTHS, default="threshold",
                   help="Tier 3 depth: threshold (tolerance file decides), sampled (verifier "
                        "default), full (cutover-critical units per the wave manifest's verify_depth)")
    r.add_argument("--param", action="append", default=[], metavar="NAME=VALUE",
                   help="resolve a ${name} placeholder in the mapping spec's where clauses "
                        "(e.g. partition/date scoping); repeatable; recorded in result.json")
    r.add_argument("--rerun-proof", type=Path,
                   help="rerun_proof.json from `dbx-recon rerun-proof`; a failed leg blocks merge "
                        "with reason rerun_gap; an unsupported evolved leg with rerun_unsupported")
    r.add_argument("--rerun-source", action="append", default=[], type=Path,
                   help="with --rerun-proof: the job's source files as committed now (the same set "
                        "rerun-proof was given); a proof of other files is stale and refused")
    r.add_argument("--out", required=True, type=Path)
    args = p.parse_args(argv)

    if args.cmd == "selftest":
        return selftest()

    if args.cmd == "families":
        from .adapters import SOURCE_ADAPTERS, is_untested_source_family
        print(json.dumps({
            "live_tested": sorted(f for f in SOURCE_ADAPTERS if not is_untested_source_family(f)),
            "untested": sorted(f for f in SOURCE_ADAPTERS if is_untested_source_family(f)),
        }))
        return 0

    if args.cmd == "dictionary-objects":
        from .adapters import DICTIONARY_OBJECTS
        print(json.dumps({
            "family": args.family,
            "family_known": args.family in DICTIONARY_OBJECTS,
            "objects": [list(x) for x in DICTIONARY_OBJECTS.get(args.family, ())],
        }))
        return 0

    if args.cmd == "type-map-audit":
        from .typemap import audit_spec, load_type_map, type_map_families, type_map_targets
        out = {"family_known": False, "target_known": False, "map": None,
               "findings": [], "error": None}
        try:
            maps = []
            for c in args.canonicalization:
                if args.family in type_map_families(c):
                    out["family_known"] = True
                    if args.target_kind in type_map_targets(c, args.family):
                        out["target_known"] = True
                tm = load_type_map(c, args.family, args.target_kind)
                if tm:
                    maps.append((c, tm))
            if len(maps) > 1:
                out["error"] = (f"multiple canonicalization files carry a type_map for "
                                f"{args.family}")
            elif maps:
                out["map"] = str(maps[0][0])
                spec = load_mapping_spec(args.spec, parse_params(args.param))
                for row in audit_spec(maps[0][1], spec):
                    out["findings"].append({"field": f"{row['object']}.{row['source']}",
                                            "verdict": row["status"], "detail": row["expected"],
                                            "source_type": row["source_type"],
                                            "target_type": row["target_type"]})
        except Exception as e:
            out["error"] = f"{type(e).__name__}: {e}"
        print(json.dumps(out))
        return 0

    if args.cmd == "fixture-shape":
        from .adapters import SOURCE_ADAPTERS, is_untested_source_family
        if is_untested_source_family(args.family):
            raise SystemExit(f"--family {args.family}: {args.family} source adapter is untested; "
                             "see SKILL.md")
        if args.source_statement_cap < 1:
            raise SystemExit("--source-statement-cap must be at least 1")
        src_secret, fix_secret = args.source_dsn_secret, args.fixture_dsn_secret
        if src_secret == fix_secret or (os.environ.get(src_secret) is not None
                                        and os.environ.get(src_secret) == os.environ.get(fix_secret)):
            raise SystemExit(f"fixture-shape: --source-dsn-secret {src_secret} and --fixture-dsn-secret "
                             f"{fix_secret} resolve to the same connection; the fixture copy must live "
                             "apart from the legacy source")
        spec = load_mapping_spec(args.mapping, parse_params(args.param))
        source = SOURCE_ADAPTERS[args.family](src_secret)
        fixture = SOURCE_ADAPTERS[args.family](fix_secret)
        try:
            check = compare_fixture(spec, source, fixture, args.source_statement_cap)
        except ConfigError as exc:
            raise SystemExit(f"fixture-shape: {exc}") from None
        check = {"family": args.family, "mapping_version": spec.version,
                 "source_statement_cap": args.source_statement_cap, **check}
        args.out.mkdir(parents=True, exist_ok=True)
        (args.out / "fixture_shape.json").write_text(json.dumps(check, indent=2) + "\n")
        print(f"dbx-recon fixture-shape {check['status']}: {len(check['findings'])} finding(s), "
              f"{check['source_statements']}/{args.source_statement_cap} source statements "
              f"-> {args.out}/fixture_shape.json")
        return 0 if check["status"] == "pass" else 1

    if args.cmd == "rerun-proof":
        if not args.source:
            raise SystemExit("rerun-proof needs --source <file> (the job's DDL, notebook or SQL; repeatable)")
        if args.prior_proof is not None and args.prior_shape is not None:
            raise SystemExit("rerun-proof takes --prior-proof or --prior-shape, not both")
        prior_path = args.prior_proof or args.prior_shape
        try:
            prior = (load_prior(prior_path, args.unit, proof=args.prior_proof is not None)
                     if prior_path is not None else None)
            proof = grade_rerun(load_record(args.fresh, "fresh"),
                                load_record(args.evolved, "evolved") if args.evolved else None, prior,
                                digest=source_digest(args.source),
                                ddl=args.ddl.read_text() if args.ddl is not None else None)
        except (OSError, ConfigError) as exc:
            raise SystemExit(f"rerun-proof: {exc}") from None
        proof = {"unit": args.unit, "sources": [str(s) for s in args.source],
                 **({"prior_from": str(prior_path)} if prior is not None else {}),
                 **proof}
        args.out.mkdir(parents=True, exist_ok=True)
        (args.out / "rerun_proof.json").write_text(json.dumps(proof, indent=2) + "\n")
        print(json.dumps(proof))
        return 0 if proof["passed"] else 1

    if args.cmd == "run" and args.mode in PLANNED_MODES:
        raise SystemExit(f"--mode {args.mode} is not implemented in this harness version")
    if args.cmd == "run" and args.mode == "transactional" \
            and args.target_kind not in TRANSACTIONAL_TARGET_KINDS:
        raise SystemExit(
            f"--mode transactional is not implemented for --target-kind {args.target_kind}: it "
            "grades two live sides and only the operational target (--target-kind lakebase) is "
            "one. Analytical-track units reconcile with --mode snapshot or live at a stated "
            "consistency point. See 14-front_door_oltp.")

    params = parse_params(args.param) if args.cmd != "shape" else {}
    if args.cmd == "estimate":
        if args.canonicalization and not args.family:
            raise SystemExit("--canonicalization needs --family so its type_map is selected")
        spec, _ = _load_spec(args.mapping, args.canonicalization, args.family,
                             args.target_kind, params)
        tol = load_tolerances(args.tolerances)
        row_counts = None
        if args.row_counts is not None:
            try:
                row_counts = json.loads(args.row_counts.read_text())
            except (OSError, json.JSONDecodeError) as exc:
                raise SystemExit(f"cannot read row counts {args.row_counts}: {exc}") from None
            if not isinstance(row_counts, dict) or any(
                    isinstance(v, bool) or not isinstance(v, int) for v in row_counts.values()):
                raise SystemExit(f"{args.row_counts} must be a JSON object of integer row counts")
        print(json.dumps(estimate_cost(spec, tol, args.depth, row_counts, args.ops_count,
                                       mode=args.mode), indent=2))
        return 0

    allowed_catalogs = _load_allowed_targets(args.allowed_targets_file)
    target_catalog = _single_identifier(args.target_catalog, "target-catalog")
    target_schema = _single_identifier(args.target_schema, "target-schema")
    if target_catalog not in allowed_catalogs:
        raise SystemExit(f"--target-catalog {target_catalog!r} is not in {args.allowed_targets_file}")
    from .adapters import (
        SOURCE_ADAPTERS,
        DatabricksTargetAdapter,
        LakebaseTargetAdapter,
        TargetIdentityError,
        is_untested_source_family,
    )
    if args.cmd == "shape":
        try:
            if args.target_kind == "lakebase":
                target = LakebaseTargetAdapter(args.target_secret, target_catalog, target_schema)
            else:
                target = DatabricksTargetAdapter(args.target_secret, target_catalog, target_schema)
            tables = {_single_identifier(t, "table"): target.column_shape(t) for t in args.table}
            absent = [t for t, cols in tables.items() if not cols and not target.table_exists(t)]
            if absent:
                raise ConfigError(f"table {', '.join(absent)} not found in {target_catalog}.{target_schema}; "
                                  "a shape read before the run must name tables that exist "
                                  "(pre-create the previous shape first)")
        except (TargetIdentityError, ConfigError) as exc:
            raise SystemExit(f"shape: {exc}") from None
        shape = {"target_kind": args.target_kind, "catalog": target_catalog, "schema": target_schema,
                 "read_at": dt.datetime.now(dt.timezone.utc).isoformat(), "tables": tables}
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(shape, indent=2) + "\n")
        print(json.dumps({"tables": {t: len(c) for t, c in tables.items()}, "out": str(args.out)}))
        return 0
    if is_untested_source_family(args.family):  # refused before any input file is read
        raise SystemExit(f"--family {args.family}: {args.family} source adapter is untested; "
                         "see SKILL.md")

    spec, type_map = _load_spec(args.mapping, args.canonicalization, args.family,
                                args.target_kind, params)
    tol = load_tolerances(args.tolerances)
    rules = load_canon_rules(args.canonicalization)

    snapshot = _load_snapshot(args.snapshot_manifest, args.mode)
    rerun_proof = None
    if args.rerun_proof is not None:
        if not args.rerun_source:
            raise SystemExit("--rerun-proof needs --rerun-source <file> (the job's source files as they "
                             "are now; the proof must bind to them)")
        try:
            rerun_proof = check_proof(json.loads(args.rerun_proof.read_text()), args.unit,
                                      str(args.rerun_proof), source_digest(args.rerun_source))
        except (OSError, json.JSONDecodeError, ConfigError) as exc:
            raise SystemExit(f"--rerun-proof: {exc}") from None
    try:
        ops = json.loads(args.ops.read_text()) if args.ops else None
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read ops file {args.ops}: {exc}") from None
    if ops:
        for op in ops:
            if not all(op.get(k) for k in ("name", "source_sql", "target_sql")):
                raise SystemExit(
                    f"ops entry missing required keys: {op.get('name', '?')}")
            for key in ("source_sql", "target_sql"):
                _validate_sql(op[key], op.get("name", "?"))
    source = SOURCE_ADAPTERS[args.family](args.source_dsn_secret)
    if args.target_kind == "lakebase":
        # --target-catalog names the Lakebase database; the adapter refuses a DSN that lands
        # anywhere else, so the allowlist binds the connection and not just the label
        try:
            target = LakebaseTargetAdapter(args.target_secret, target_catalog, target_schema)
        except TargetIdentityError as exc:
            raise SystemExit(str(exc)) from None
    else:
        target = DatabricksTargetAdapter(args.target_secret, target_catalog, target_schema)
    if args.source_dictionary or args.target_dictionary:
        from .structure import DictionaryOverlay, load_dictionary
        try:
            if args.source_dictionary:
                source = DictionaryOverlay(source, load_dictionary(args.source_dictionary))
            if args.target_dictionary:
                target = DictionaryOverlay(target, load_dictionary(args.target_dictionary))
        except ConfigError as exc:
            raise SystemExit(f"dictionary: {exc}") from None
    run_source = (lambda op: source.run_query(op["source_sql"])) if ops else None
    run_target = (lambda op: target.run_query(op["target_sql"])) if ops else None
    result = run_recon(args.unit, args.mode, spec, tol, rules, source, target,
                       ops=ops, run_source=run_source, run_target=run_target,
                       out_dir=args.out, seed=args.seed, params=params, snapshot=snapshot,
                       source_family=args.family, depth=args.depth, type_map=type_map,
                       rerun_proof=rerun_proof)
    print(f"dbx-recon {result['verdict']}: unit={args.unit} mode={args.mode} depth={result['depth']} "
          f"mapping={spec.version} tolerances={tol.version} merge_eligible={result['merge_eligible']} "
          f"-> {args.out}/result.json")
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
