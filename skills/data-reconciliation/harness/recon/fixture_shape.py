"""Wave 0 fixture shape check: is the fixture copy shaped like the real source?

Children develop against a fixture, so a fixture that merely has the tables (a column spelled
differently, a looser type, every row in one status) lets a wrong conversion reach review. This
compares, per mapped table, the source's observed column shape and a sample cardinality (within
the object's `root_where`) with the fixture's. Source reads are catalog queries plus one profile
statement per mapped column, counted against a stated cap on the adapter's own statement counter;
nothing here writes. What could not be read is `unsupported`.

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
    except Exception as exc:  # a live catalog read that failed (denied, dropped, dialect)
        return f"column_shape failed: {type(exc).__name__}: {exc}"
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


class _Budget:
    """The source statement cap, enforced at the source-read boundary on the adapter's own
    counter: a read is issued only when one more statement fits, and the counter is re-read
    after it so an adapter that spends more than one statement stops the reads too."""

    def __init__(self, source, cap: int | None):
        self.source, self.cap, self.start = source, cap, source.statements

    @property
    def used(self) -> int:
        return self.source.statements - self.start

    def fits(self, n: int = 1) -> bool:
        return self.cap is None or self.used + n <= self.cap

    def reason(self) -> str:
        return f"source statement cap {self.cap} reached after {self.used} statements"


def _compare_cardinality(table: str, columns: list[str], where: str | None, source, fixture,
                         budget: _Budget) -> tuple[list[dict], str | None]:
    """Findings, and the reason cardinality stopped short of the cap (None when every column
    was profiled)."""
    out = []
    for col in columns:
        if not budget.fits():
            return out, budget.reason()
        try:
            s = source.column_profile(table, col, where)
            if not budget.fits(0):
                return out, budget.reason() + f" (profiling {col} overspent it)"
            f = fixture.column_profile(table, col, where)
        except Exception as exc:  # a profile the side cannot run (no equality operator, denied)
            return out, f"profiling {col} failed: {type(exc).__name__}: {exc}"
        s_rows, f_rows = int(s["count"]), int(f["count"])
        if s_rows == 0:
            return out, "source has 0 rows in scope, nothing to compare the fixture's profile with"
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
    return out, None


def _table_shapes(table: str, source, fixture, row: dict, findings: list[dict]) -> tuple:
    """Both sides' shapes for one table (one catalog read each); a side that could not be read
    marks the row and leaves a reason in its place."""
    fix = _shape(fixture, table)
    if isinstance(fix, str) and fix.startswith("no "):
        findings.append(_find(table, "table_missing", f"fixture has no {table}"))
        row.update(status="fail", shape="unsupported", cardinality="unsupported",
                   reason=f"fixture has no {table}")
        return None, fix
    src = _shape(source, table)
    if isinstance(src, str) or isinstance(fix, str):
        reason = src if isinstance(src, str) else fix
        row.update(status="unsupported", shape="unsupported", cardinality="unsupported",
                   reason=("source " if isinstance(src, str) else "fixture ") + reason)
    return src, fix


def compare_fixture(spec: MappingSpec, source, fixture,
                    source_statement_cap: int | None = None) -> dict:
    """Shapes first for every table (one source read each), then cardinality table by table,
    scoped by the object's `root_where`, until the source cap is reached; a table whose shape
    could not be read on either side, or that lies past the cap, is `unsupported`, never clean."""
    shape_reads = len({obj.root_table for obj in spec.objects})
    if source_statement_cap is not None and source_statement_cap < shape_reads:
        raise ConfigError(f"source statement cap {source_statement_cap} is below the "
                          f"{shape_reads} catalog reads the shapes need")
    budget = _Budget(source, source_statement_cap)
    findings: list[dict] = []
    tables: dict[str, dict] = {}
    shapes: dict[str, tuple] = {}  # one catalog read per side per table, shared by its objects
    plans: list[list[str] | None] = []  # per object: the columns to profile (as mapped), or nothing
    for obj in spec.objects:
        table = obj.root_table
        # shapes compare on the lower-cased name; the profile statement gets the mapping's spelling
        # (a case-sensitive source knows `OrderId`, not `orderid`)
        spelled: dict[str, str] = {}
        for c in [*obj.key_source, *(f.source for f in obj.fields)]:
            spelled.setdefault(c.lower(), c)
        mapped = list(spelled)
        row = tables.setdefault(table, {"status": "pass", "shape": "checked",
                                        "cardinality": "checked"})
        plans.append(None)
        if table not in shapes:
            shapes[table] = _table_shapes(table, source, fixture, row, findings)
        src, fix = shapes[table]
        if not isinstance(src, dict) or not isinstance(fix, dict):
            continue
        found = _compare_shape(table, mapped, src, fix)
        findings.extend(found)
        if found:
            row["status"] = "fail"
        # a column the fixture lacks is already a finding, one the source lacks is the mapping's
        # defect (tier 7): neither can be profiled, and the second leaves the table unproven
        plans[-1] = [spelled[c] for c in mapped if c in fix and c in src]
        unmapped = [c for c in mapped if c not in src]
        if unmapped:
            row["cardinality"] = "partial"
            row.setdefault("reason", f"source has no mapped column {unmapped[0]} (a mapping defect)")
            if row["status"] == "pass":
                row["status"] = "unsupported"
    for obj, mapped in zip(spec.objects, plans):
        table = obj.root_table
        row = tables[table]
        if mapped is None:
            continue
        if not budget.fits(len(mapped)):
            short = budget.reason()
        else:
            found, short = _compare_cardinality(table, mapped, obj.root_where, source, fixture, budget)
            findings.extend(found)
            if found:
                row["status"] = "fail"
        if short:
            row["cardinality"] = "unsupported"
            row.setdefault("reason", short)
            if row["status"] == "pass":
                row["status"] = "unsupported"
    statuses = {r["status"] for r in tables.values()}
    status = ("fail" if findings or "fail" in statuses
              else "unsupported" if "unsupported" in statuses else "pass")
    return {"status": status, "findings": findings, "tables": tables,
            "source_statements": budget.used}


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
