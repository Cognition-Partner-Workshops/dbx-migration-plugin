"""The range-fingerprint digest contract lives in one module; the SQL adapters render it per
engine and the fakes evaluate it in Python, so neither can drift from the other."""

import datetime as dt
from decimal import Decimal

from recon import adapters
from recon.fingerprint import DIGEST_MODULUS, digest, moments, normalise

from tests import fakes


def test_adapters_and_fakes_share_one_digest_module():
    assert adapters.DIGEST_MODULUS is DIGEST_MODULUS == 2**31 - 1
    assert adapters._digest_value is normalise
    assert fakes.moments is moments
    assert not any(name in vars(fakes._TransactionalMixin) for name in ("_digest", "_moments"))
    assert "DIGEST_MODULUS" not in vars(fakes)


def test_digest_is_the_whole_number_the_sql_casts_to():
    assert digest(7) == digest(Decimal("7.0")) == Decimal(7)
    assert digest(dt.datetime(1970, 1, 1, 0, 0, 1)) == Decimal(1_000_000)  # noqa: DTZ001
    assert digest(dt.date(1970, 1, 2)) == Decimal(86_400_000_000)
    assert digest(True) is digest(None) is digest("7") is None
    sentinel = dt.datetime(9999, 12, 31, 23, 59, 59, 999_999)  # noqa: DTZ001  SCD open-row marker
    assert digest(sentinel) == Decimal(253_402_300_799_999_999)  # exact, not float-rounded


def test_moments_are_the_exact_sum_and_the_sum_of_squared_residues():
    total, squares = moments([DIGEST_MODULUS + 2, 3, None])
    assert total == DIGEST_MODULUS + 5 and squares == 2 * 2 + 3 * 3
    assert moments([]) == (Decimal(0), 0)


def test_normalise_makes_driver_sums_comparable():
    assert normalise(Decimal("5.000000")) == normalise(5.0) == normalise(5) == 5
    assert normalise(None) == 0 and normalise(True) == 1
    assert normalise(Decimal("5.5")) == Decimal("5.5") and normalise(5.5) == 5.5
