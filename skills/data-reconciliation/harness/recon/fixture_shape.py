"""Wave 0 fixture shape check: is the fixture copy shaped like the real source?

Children develop against a fixture, so a fixture that merely has the tables (a column spelled
differently, a looser type, every row in one status) lets a wrong conversion reach review. This
compares, per mapped table, the source's observed column shape and a sample cardinality with the
fixture's. Source reads are catalog queries plus one aggregate per mapped column, counted
against a stated cap; nothing here writes. What could not be read is `unsupported`.

    check = {status: pass|fail|unsupported, findings: [...], tables: {root_table: {...}},
             source_statements: n}
"""

from __future__ import annotations

from .config import ConfigError, MappingSpec
from .rerun import normalize_type

STATUSES = ("pass", "fail", "unsupported")
CHECKS = ("table_missing", "column_missing", "column_extra", "type_mismatch",
          "nullable_mismatch", "empty_fixture", "cardinality_collapsed", "null_profile")


def _shape(adapter, table: str) -> dict[str, dict] | str:
    """Observed columns by lower-cased name, or the reason they could not be read."""
    try:
        cols = adapter.column_shape(table)
    except NotImplementedError as exc:
        return f"column_shape unsupported: {exc}"
    except AttributeError:
        return f"column_shape unsupported: {type(adapter).__name__} has no catalog reader"
    except KeyError:
        return f"no {table}"
    if not cols:
        return f"no {table}"
    return {str(c["name"]).lower(): {"type": normalize_type(c["type"]),
                                     "nullable": bool(c["nullable"])} for c in cols}


def _find(table: str, check: str, detail: str, column: str | None = None) -> dict:
    row = {"table": table, "check": check}
    if column is not None:
        row["column"] = column
    row["detail"] = detail
    return row


def _compare_shape(table: str, mapped: list[str], src: dict, fix: dict) -> list[dict]:
    out = []
    for col in mapped:
        s, f = src.get(col), fix.get(col)
        if s is None:
            continue  # the source lacks a mapped column: the spec's problem, tier 7's finding
        if f is None:
            out.append(_find(table, "column_missing", f"source {s['type']}, absent in fixture", col))
            continue
        if s["type"] != f["type"]:
            out.append(_find(table, "type_mismatch", f"source {s['type']}, fixture {f['type']}", col))
        if s["nullable"] != f["nullable"]:
            out.append(_find(table, "nullable_mismatch",
                             f"source {'nullable' if s['nullable'] else 'not null'}, "
                             f"fixture {'nullable' if f['nullable'] else 'not null'}", col))
    for col in fix:
        if col not in src:
            out.append(_find(table, "column_extra", f"fixture {fix[col]['type']}, absent in source", col))
    return out


def _compare_cardinality(table: str, columns: list[str], source, fixture) -> list[dict]:
    out, s_rows, f_rows = [], None, None
    for col in columns:
        s, f = source.field_aggregates(table, col), fixture.field_aggregates(table, col)
        s_rows, f_rows = int(s["count"]), int(f["count"])
        if f_rows == 0:
            out.append(_find(table, "empty_fixture", f"source {s_rows} rows, fixture 0 rows"))
            break
        if int(s["distinct_count"]) > 1 and int(f["distinct_count"]) <= 1:
            out.append(_find(table, "cardinality_collapsed",
                             f"source {s['distinct_count']} distinct in {s_rows} rows, "
                             f"fixture {f['distinct_count']} distinct in {f_rows} rows", col))
        s_null, f_null = float(s["null_rate"]), float(f["null_rate"])
        if (s_null == 0.0) != (f_null == 0.0) or (s_null == 1.0) != (f_null == 1.0):
            out.append(_find(table, "null_profile",
                             f"source null rate {s_null:.1f}, fixture null rate {f_null:.1f}", col))
    return out


def compare_fixture(spec: MappingSpec, source, fixture,
                    source_statement_cap: int | None = None) -> dict:
    """Shapes first for every table (one source read each), then cardinality table by table
    until the source cap is reached; tables past the cap are `unsupported`, never clean."""
    if source_statement_cap is not None and source_statement_cap < len(spec.objects):
        raise ConfigError(f"source statement cap {source_statement_cap} is below the "
                          f"{len(spec.objects)} catalog reads the shapes need")
    start = source.statements
    findings: list[dict] = []
    tables: dict[str, dict] = {}
    plans: dict[str, tuple[list[str], dict | None]] = {}
    for obj in spec.objects:
        table = obj.root_table
        mapped = list(dict.fromkeys(f.source.lower() for f in obj.fields))
        row = {"status": "pass", "shape": "checked", "cardinality": "checked"}
        tables[table] = row
        fix = _shape(fixture, table)
        if isinstance(fix, str) and fix.startswith("no "):
            findings.append(_find(table, "table_missing", f"fixture has no {table}"))
            row.update(status="fail", shape="unsupported", cardinality="unsupported",
                       reason=f"fixture has no {table}")
            plans[table] = (mapped, None)
            continue
        src = _shape(source, table)
        if isinstance(src, str) or isinstance(fix, str):
            reason = src if isinstance(src, str) else fix
            row.update(status="unsupported", shape="unsupported",
                       reason=("source " if isinstance(src, str) else "fixture ") + reason)
        else:
            found = _compare_shape(table, mapped, src, fix)
            findings.extend(found)
            if found:
                row["status"] = "fail"
            # a column the fixture lacks is already a finding; aggregating it would only error
            mapped = [c for c in mapped if c in fix]
        plans[table] = (mapped, row)
    for obj in spec.objects:
        table = obj.root_table
        mapped, row = plans[table]
        if row is None:
            continue
        used = source.statements - start
        if source_statement_cap is not None and used + len(mapped) > source_statement_cap:
            row["cardinality"] = "unsupported"
            row.setdefault("reason", f"source statement cap {source_statement_cap} reached "
                                     f"after {used} statements")
            if row["status"] == "pass":
                row["status"] = "unsupported"
            continue
        found = _compare_cardinality(table, mapped, source, fixture)
        findings.extend(found)
        if found:
            row["status"] = "fail"
    statuses = {r["status"] for r in tables.values()}
    status = ("fail" if findings or "fail" in statuses
              else "unsupported" if "unsupported" in statuses else "pass")
    return {"status": status, "findings": findings, "tables": tables,
            "source_statements": source.statements - start}


def load_check(data: object, where: str) -> dict:
    """Validate a written fixture_shape.json before anything reads a verdict from it."""
    if not isinstance(data, dict):
        raise ConfigError(f"{where}: fixture shape check must be a JSON object")
    if data.get("status") not in STATUSES:
        raise ConfigError(f"{where}: status must be one of {', '.join(STATUSES)}")
    findings = data.get("findings")
    if not isinstance(findings, list):
        raise ConfigError(f"{where}: findings must be a list")
    for f in findings:
        if not isinstance(f, dict) or f.get("check") not in CHECKS or not f.get("table"):
            raise ConfigError(f"{where}: each finding names its table and a known check")
    if not isinstance(data.get("tables"), dict):
        raise ConfigError(f"{where}: tables must be an object")
    return data
