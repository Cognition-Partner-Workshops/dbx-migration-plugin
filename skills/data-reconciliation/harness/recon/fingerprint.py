"""The range-fingerprint digest contract, defined once. `_SqlAdapterBase._digest_sql` renders it
per engine; the in-memory fakes evaluate `moments` over Python values; both agree with this
module or neither is the harness's fingerprint."""

from __future__ import annotations

import datetime as dt
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

# Range fingerprints carry two moments per digested column: the exact sum and the sum of
# squared residues modulo this Mersenne prime (2^31 - 1). Two multisets that agree on count,
# sum and sum of squares must differ in at least three elements, so any one- or two-key
# substitution inside a range is provably visible; the modulus keeps a bigint key's or an
# epoch-microsecond datetime's square inside DECIMAL(38,0) on every engine.
DIGEST_MODULUS = 2_147_483_647

# Kinds with an exact portable digest; everything else (strings, uuids, fractional numbers)
# streams its ranges instead.
DIGESTIBLE_KINDS = ("integer", "datetime")

_EPOCH = dt.datetime(1970, 1, 1)  # noqa: DTZ001  naive = UTC by contract


def digest(value: Any) -> Decimal | None:
    """The whole number an engine's digest expression yields for one value: integers and
    whole decimals as they are, datetimes as microseconds since the epoch, dates as whole days
    of microseconds; None for kinds that do not digest."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        delta = value - _EPOCH  # integer arithmetic: total_seconds() is a float and rounds past ~2255
        return Decimal((delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds)
    if isinstance(value, dt.date):
        return Decimal((value - _EPOCH.date()).days * 86_400_000_000)
    return None


def moments(values) -> tuple[Decimal, int]:
    """(sum, sum of squared residues) as `SUM(digest), SUM(square_digest_sql)` compute them:
    NULL digests contribute nothing and the residue is rounded to a whole number first."""
    digests = [digest(v) or Decimal(0) for v in values]
    squares = 0
    for d in digests:
        residue = abs(int(d.to_integral_value(rounding=ROUND_HALF_UP))) % DIGEST_MODULUS
        squares += residue * residue
    return sum(digests, Decimal(0)), squares


def normalise(value: Any) -> Any:
    """Normalise an engine's SUM result so equal digests compare equal across drivers
    (Decimal('5.000000') vs int 5 vs float 5.0)."""
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, float):
        return int(value) if value.is_integer() else value
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else value
    return value
