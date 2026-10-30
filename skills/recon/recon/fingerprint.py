
from __future__ import annotations

import datetime as dt
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

DIGEST_MODULUS = 2_147_483_647

DIGESTIBLE_KINDS = ("integer", "datetime")

_EPOCH = dt.datetime(1970, 1, 1)  # noqa: DTZ001  naive = UTC by contract

def digest(value: Any) -> Decimal | None:
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
    digests = [digest(v) or Decimal(0) for v in values]
    squares = 0
    for d in digests:
        residue = abs(int(d.to_integral_value(rounding=ROUND_HALF_UP))) % DIGEST_MODULUS
        squares += residue * residue
    return sum(digests, Decimal(0)), squares

def normalise(value: Any) -> Any:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, float):
        return int(value) if value.is_integer() else value
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else value
    return value
