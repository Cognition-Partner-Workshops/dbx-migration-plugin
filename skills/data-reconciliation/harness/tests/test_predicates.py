"""A9: mapping predicates are interpolated raw into SQL on both sides, so the loader admits
only a small allowlisted grammar and names the token it refused."""

import json
import os
from pathlib import Path

import pytest
from recon.config import ConfigError, load_mapping_spec, validate_predicate

ACCEPTED = [
    "source_table = 'raw.loans'",
    "batch=a",
    "BATCH = '${batch}'",
    "order_date >= '${from_date}'",
    "Amount > ${floor}",
    "dbo.loans.status <> 'X' AND [Loan_Status] IN ('AC', 'DL', 'FC')",
    '"Region" = \'eu\' OR NOT (qty BETWEEN 1 AND 10)',
    "modified_date >= '2026-09-08 18:43:52.164113' AND memo IS NOT NULL",
    "deleted_at IS NULL AND (rate <= 0.25 OR rate >= -1.5e3)",
    "code LIKE 'LN%' AND note NOT LIKE '%x''y%'",
    "loan_id != 7 AND version_no > 2001",
    "hire_date < DATE '2026-01-01' AND ts <= TIMESTAMP '2026-01-01 00:00:00'",
    "  status = 'A'  ",
]

REJECTED = [
    ("pg_terminate_backend(pg_backend_pid())", "pg_terminate_backend("),
    ("1=1 AND pg_sleep(1e9) IS NULL", "pg_sleep("),
    ("set_config('x', 'y', false) = 'y'", "set_config("),
    ("EXISTS (SELECT 1 FROM other_db.dbo.x)", "EXISTS"),
    ("id IN (SELECT id FROM dbo.x)", "SELECT"),
    ("CASE WHEN a = 1 THEN 1 ELSE 0 END = 1", "CASE"),
    ("ID = 1 -- injected", "--"),
    ("ID = 1 /* c */", "/*"),
    ("1=1; DELETE FROM cdc_checkpoint", ";"),
    ("a = 1 UNION SELECT 1", "UNION"),
    ("a = 1 OR 1 = 1 INTO x", "INTO"),
    ("a = b + 1", "+"),
    ("a = 'unterminated", "'unterminated"),
    ("a = [unterminated", "[unterminated"),
    ("a IN (b)", "b"),
    ("a IN (1, (2))", "("),
    ("a BETWEEN 1", ""),
    ("(a = 1", ""),
    ("a = 1)", ")"),
    ("a = 1 AND", ""),
    ("a =", ""),
    ("", ""),
    ("a", ""),
    ("a IS 1", "1"),
    ("a b", "b"),
    ("a = $x", "$x"),
]

SLOTS = ("root_where", "target_where", "child_where", "applied_where")

REHEARSAL_MAPPING = Path(os.environ.get(
    "REHEARSAL_MAPPING",
    Path(__file__).resolve().parents[5] / "ts-tsql-sybase-legacy-db/rehearsals/lakebase/mapping.json"))
REHEARSAL_PREDICATES = [f"source_table = 'raw.{t}'"
                        for t in ("borrowers", "loans", "payments", "escrow_accounts", "loan_modifications")]


def _mapping(tmp_path: Path, slot: str, where: str) -> Path:
    obj = {"object": "c", "root_table": "T", "key": {"source": ["ID"], "target": "id"},
           "fields": [{"source": "ID", "target": "id"}]}
    if slot in ("root_where", "target_where"):
        obj[slot] = where
    elif slot == "child_where":
        obj["embeds"] = [{"array_path": "items", "child_table": "I", "child_where": where,
                          "parent_key": ["ID"], "key": {"source": ["ID"], "target": "id"}, "fields": []}]
    else:
        obj["delete_evidence"] = {"kind": "sqlserver_cdc", "capture": "dbo_T",
                                  "applied_position": {"table": "ck", "column": "lsn", "where": where}}
    path = tmp_path / "map.json"
    path.write_text(json.dumps({"version": "m", "objects": [obj]}))
    return path


@pytest.mark.parametrize("where", ACCEPTED)
def test_the_allowlisted_grammar_loads(where):
    assert validate_predicate(where) == where


@pytest.mark.parametrize("where, token", REJECTED)
def test_everything_else_is_refused_naming_the_token(where, token):
    with pytest.raises(ConfigError, match="predicates must be a single expression") as info:
        validate_predicate(where)
    if token:
        assert repr(token) in str(info.value), str(info.value)


@pytest.mark.parametrize("slot", SLOTS)
def test_every_predicate_slot_is_validated_after_substitution(tmp_path, slot):
    params = {"batch": "demo", "src": "dbo.loans"}
    spec = load_mapping_spec(_mapping(tmp_path, slot, "batch = '${batch}'"), params)
    c = spec.objects[0]
    loaded = (c.embeds[0].child_where if slot == "child_where"
              else c.delete_evidence.applied_where if slot == "applied_where" else getattr(c, slot))
    assert loaded == "batch = 'demo'"
    with pytest.raises(ConfigError, match="predicates must be a single expression.*'--'"):
        load_mapping_spec(_mapping(tmp_path, slot, "batch = 1 -- x"), params)
    # a parameter value is part of the predicate: it cannot smuggle in what the grammar refuses
    with pytest.raises(ConfigError, match="predicates must be a single expression"):
        load_mapping_spec(_mapping(tmp_path, slot, "batch = ${batch}"), {"batch": "1; DROP TABLE x"})


@pytest.mark.parametrize("where", REHEARSAL_PREDICATES)
def test_the_rehearsal_mapping_predicates_load(where):
    assert validate_predicate(where) == where


def test_the_rehearsal_mapping_file_loads_when_the_fixture_repo_is_checked_out():
    if not REHEARSAL_MAPPING.exists():
        pytest.skip(f"{REHEARSAL_MAPPING} not found; set REHEARSAL_MAPPING")
    spec = load_mapping_spec(REHEARSAL_MAPPING)
    wheres = {o.delete_evidence.applied_where for o in spec.objects if o.delete_evidence}
    wheres |= {w for o in spec.objects for w in (o.root_where, o.target_where) if w}
    assert wheres >= set(REHEARSAL_PREDICATES)
