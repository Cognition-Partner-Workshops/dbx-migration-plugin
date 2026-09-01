"""Every canonicalization rule fires on its seeded mismatch class."""

import datetime as dt
import decimal
import uuid

import pytest

from recon.canon import MISSING, CanonError, Canonicalizer
from recon.config import CanonRule


def canon(*names_params):
    return Canonicalizer([CanonRule(rule=n, applies_to="*", params=p) for n, p in names_params])


def test_decimal_round_half_even():
    c = canon(("decimal_round", {"mode": "half_even", "places": 2}))
    ok, fired = c.equal(decimal.Decimal("1.005"), 1.005, ["decimal_round"])
    assert ok and "decimal_round" in fired
    ok, _ = c.equal(decimal.Decimal("1.005"), 1.01, ["decimal_round"])
    assert not ok  # half_even rounds 1.005 to 1.00


def test_datetime_utc_truncate_ms():
    c = canon(("datetime_utc_truncate_ms", {}))
    src = dt.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    tgt = dt.datetime(2026, 1, 2, 1, 4, 5, 123000)  # UTC, ms precision
    ok, fired = c.equal(src, tgt, ["datetime_utc_truncate_ms"])
    assert ok and "datetime_utc_truncate_ms" in fired


def test_datetime_grid_333():
    c = canon(("datetime_grid_333", {}))
    # SQL Server snapped .997 -> BSON stored .997; app wrote 1.000 on the mongo side
    src = dt.datetime(2026, 1, 2, 3, 4, 5, 997000)
    tgt = dt.datetime(2026, 1, 2, 3, 4, 6, 0)
    ok, fired = c.equal(src, tgt, ["datetime_grid_333"])
    assert ok and "datetime_grid_333" in fired
    assert not c.equal(src, dt.datetime(2026, 1, 2, 3, 4, 5, 950000), ["datetime_grid_333"])[0]


def test_rstrip_spaces():
    c = canon(("rstrip_spaces", {}))
    ok, fired = c.equal("ABC   ", "ABC", ["rstrip_spaces"])
    assert ok and "rstrip_spaces" in fired


def test_empty_string_is_null():
    c = canon(("empty_string_is_null", {}))
    ok, fired = c.equal("", None, ["empty_string_is_null"])
    assert ok and "empty_string_is_null" in fired


def test_null_missing_equiv():
    c = canon(("null_missing_equiv", {}))
    ok, fired = c.equal(None, MISSING, ["null_missing_equiv"])
    assert ok and "null_missing_equiv" in fired


def test_collation_casefold():
    c = canon(("collation_casefold", {}))
    ok, fired = c.equal("McDonald", "MCDONALD", ["collation_casefold"])
    assert ok and "collation_casefold" in fired


def test_uuid_normalize():
    c = canon(("uuid_normalize", {}))
    u = uuid.uuid4()
    ok, fired = c.equal(str(u).upper(), u.bytes, ["uuid_normalize"])
    assert ok and "uuid_normalize" in fired


def test_identity():
    c = canon(("identity", {}))
    assert c.equal({"a": 1}, {"a": 1}, ["identity"])[0]
    assert not c.equal({"a": 1}, {"a": 2}, ["identity"])[0]


def test_numeric_abs_tol():
    c = canon(("identity", {}))
    assert c.equal(100.0, 100.4, ["identity"], numeric_abs_tol=0.5)[0]
    assert not c.equal(100.0, 100.6, ["identity"], numeric_abs_tol=0.5)[0]


def test_unknown_rule_rejected():
    with pytest.raises(CanonError):
        Canonicalizer([CanonRule(rule="made_up", applies_to="*")])


def test_field_referencing_absent_rule_rejected():
    c = canon(("identity", {}))
    with pytest.raises(CanonError):
        c.apply("x", ["rstrip_spaces"])
