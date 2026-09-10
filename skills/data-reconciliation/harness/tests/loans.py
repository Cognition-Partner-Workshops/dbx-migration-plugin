"""The loans estate the transactional tests reconcile: rows, catalog facts, the mapping, and
the stub connections the SQL adapters are exercised against."""

import dataclasses
import datetime as dt

from recon.adapters import (
    SchemaFacts,
    SqlServerSourceAdapter,
    _PostgresBase,
    _SqlAdapterBase,
)
from recon.config import CanonRule, FieldMapping, MappingSpec, ObjectMapping, Tolerances
from recon.engine import run_recon

from tests.fakes import FakeSource, FakeTarget

T0 = dt.datetime(2026, 9, 1, 12, 0, 0)  # noqa: DTZ001  naive = UTC by contract
EPOCH = dt.datetime(1970, 1, 1)  # noqa: DTZ001
UTC = dt.timezone.utc
PLUS2 = dt.timezone(dt.timedelta(hours=2))
EVEN_KEYS = [2 * i for i in range(1, 41)]


def _ts(seconds: int) -> dt.datetime:
    return T0 + dt.timedelta(seconds=seconds)


def _loan(i: int, changed: int = 0, **over) -> dict:
    row = {"loan_id": i, "loan_number": f"LN{i:05d}", "current_balance": 1000 + i,
           "modified_date": _ts(changed)}
    row.update(over)
    return row


LOANS_FACTS = SchemaFacts(
    primary_key=("loan_id",), unique={("loan_number",)},
    foreign_keys={(("borrower_id",), "dbo.borrowers", ("borrower_id",))},
    not_null={"loan_id", "loan_number", "current_balance", "modified_date", "borrower_id"},
    indexes={("borrower_id",), ("loan_status", "days_past_due")}, check_count=2,
    checks={"([Current_Balance]>=(0))", "([Loan_Status]='FC' OR [Loan_Status]='DL' OR [Loan_Status]='AC')"},
    identity_columns={"loan_id"})

TARGET_LOANS_FACTS = SchemaFacts(
    primary_key=("loan_id",), unique={("loan_number",)},
    foreign_keys={(("borrower_id",), "loan_servicing.borrowers", ("borrower_id",))},
    not_null={"loan_id", "loan_number", "current_balance", "modified_date", "borrower_id"},
    indexes={("borrower_id",), ("loan_status", "days_past_due", "loan_id")}, check_count=2,
    checks={"CHECK ((current_balance >= (0)::numeric))",
            ("CHECK (((loan_status)::text = ANY ((ARRAY['AC'::character varying, 'DL'::character varying, "
             "'FC'::character varying])::text[])))")},
    identity_columns={"loan_id"})

BORROWER_FACTS = SchemaFacts(primary_key=("borrower_id",), not_null={"borrower_id"},
                             identity_columns={"borrower_id"})


def _facts(base: SchemaFacts, **over) -> SchemaFacts:
    return dataclasses.replace(base, **over)


def _tightened(**over) -> SchemaFacts:
    return _facts(TARGET_LOANS_FACTS, **over)


def _spec(with_watermark: bool = True, with_identity: bool = True) -> MappingSpec:
    loans = ObjectMapping(
        object="loans", root_table="dbo.loans", key_source=["loan_id"], key_target=["loan_id"],
        fields=[FieldMapping("loan_number", "loan_number", "varchar", "string"),
                FieldMapping("current_balance", "current_balance", "money", "decimal(19,4)"),
                FieldMapping("borrower_id", "borrower_id", "int", "int")],
        watermark_source="modified_date" if with_watermark else None,
        watermark_target="modified_date" if with_watermark else None,
        identity_source="loan_id" if with_identity else None,
        identity_target="loan_id" if with_identity else None)
    borrowers = ObjectMapping(
        object="borrowers", root_table="dbo.borrowers", key_source=["borrower_id"],
        key_target=["borrower_id"], fields=[FieldMapping("name", "name", "varchar", "string")])
    return MappingSpec("m1", [loans, borrowers])


def _keyed(column: str) -> MappingSpec:
    """The loans mapping keyed on `column` instead of loan_id."""
    spec = _spec()
    spec.objects[0].key_source[:] = [column]
    spec.objects[0].key_target[:] = [column]
    return spec


def _counter_spec(unit: str | None) -> MappingSpec:
    """The loans mapping keyed on a numeric change column (rowversion / version_no)."""
    spec = _spec()
    loans = dataclasses.replace(spec.objects[0], watermark_source="version_no",
                                watermark_target="version_no", watermark_unit=unit)
    return MappingSpec("m1", [loans, spec.objects[1]])


def _rows(n: int = 12, keys=None) -> tuple[list[dict], list[dict]]:
    keys = list(keys) if keys is not None else range(1, n + 1)
    loans = [_loan(k, changed=i, borrower_id=1 + i % 3) for i, k in enumerate(keys, 1)]
    borrowers = [{"borrower_id": b, "name": f"B{b}"} for b in (1, 2, 3)]
    return loans, borrowers


def _counter_rows(behind: int, step: int = 1):
    """12 source rows versioned 1*step..12*step; the target lacks the last `behind` of them."""
    loans, borrowers = _rows(12)
    for i, r in enumerate(loans, 1):
        r["version_no"] = i * step
    tgt = [dict(r) for r in loans[:12 - behind]]
    return loans, tgt, borrowers


def _rowversion(n: int) -> bytes:
    """A SQL Server rowversion as pyodbc returns it: 8 bytes, unsigned big-endian."""
    return n.to_bytes(8, "big")


def _rowversion_rows(behind: int):
    loans, tgt, borrowers = _counter_rows(behind=behind, step=1)
    for r in loans:
        r["version_no"] = _rowversion(1000 + r["version_no"])
    tgt = [dict(r) for r in loans[:12 - behind]]
    return loans, tgt, borrowers


def _sides(loans_src, loans_tgt, borrowers, *, src_seq=None, tgt_seq=None, tgt_facts=None):
    borrowers_tgt = [dict(b) for b in borrowers]
    source = FakeSource({"dbo.loans": loans_src, "dbo.borrowers": borrowers},
                        schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
                        sequences={("dbo.loans", "loan_id"): src_seq if src_seq is not None
                                   else max(r["loan_id"] for r in loans_src) + 1})
    target = FakeTarget({"loans": loans_tgt, "borrowers": borrowers_tgt},
                        schema={"loans": tgt_facts or TARGET_LOANS_FACTS, "borrowers": BORROWER_FACTS},
                        sequences={("loans", "loan_id"): tgt_seq if tgt_seq is not None
                                   else max(r["loan_id"] for r in loans_src) + 1})
    return source, target


def _run(source, target, spec=None, tol=None, **kw):
    return run_recon("u1", "transactional", spec or _spec(), tol or Tolerances("t1"),
                     [CanonRule("decimal_round", "money", {"places": 4})], source, target, **kw)


def _tier(result, name):
    return next(t for t in result["tiers"] if t["name"] == name)


def _codes(result, name):
    return sorted(f["check"] for f in _tier(result, name)["findings"])


def _details(result, name) -> dict[str, str]:
    return {f["check"]: f["detail"] for f in _tier(result, name)["findings"]}


class _StubConn:
    """DB-API stand-in that records every (sql, params) and answers each fetch with `rows`."""

    def __init__(self, rows=((3, 3, 1, 9, 3, 12),)):
        self.rows, self.closed, self.executed = list(rows), False, []

    def cursor(self):
        conn = self

        class Cur:
            def execute(self, sql, params=()):
                conn.executed.append((sql, params))

            def fetchall(self):
                return conn.rows
        return Cur()

    def close(self):
        self.closed = True


def _db(name):
    return _StubConn([(name,)])


class _NoSnapshotAdapter(_SqlAdapterBase):
    change_token_sql = "TOKEN {table}"


class _PostgresLike(_PostgresBase):
    pass


class _SqlServerLike(SqlServerSourceAdapter):
    def __init__(self, conn):
        _SqlAdapterBase.__init__(self, conn)
