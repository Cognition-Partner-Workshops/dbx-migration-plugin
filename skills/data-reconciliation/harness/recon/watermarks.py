"""One comparison rule for watermarks, shared by tiers 3, 5 and 6 and the in-flight predicate.

Watermarks are datetimes or numbers. Datetimes are compared as instants: an aware value is
converted to UTC, a naive value is read as UTC already. That is the only reading consistent
with the range digests (epoch microseconds of the stored value) and with a CDC feed that
copies a source `datetime`/`datetime2` column into a Lakebase `timestamptz`; a source that
stores local wall-clock time must declare a UTC-normalising expression as its watermark.
Two watermarks of different families (a datetime against a number) are never ordered by
string fallback; they are a mapping error and are refused before the window opens.
"""

from __future__ import annotations

import datetime as dt
import decimal
from typing import Any

from .config import ConfigError

_NUMBER = (int, float, decimal.Decimal)


def instant(value: Any) -> Any:
    """The comparable form of a watermark: naive UTC datetime for any datetime/date, the value
    itself otherwise."""
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time())
    return value


def family(value: Any) -> str:
    if isinstance(value, bool):
        return "other"
    if isinstance(value, _NUMBER):
        return "number"
    if isinstance(value, (dt.datetime, dt.date)):
        return "datetime"
    return "other"


def check_comparable(a: Any, b: Any, what: str) -> None:
    """Refuse two non-null watermarks that cannot be ordered against each other."""
    if a is None or b is None:
        return
    fa, fb = family(a), family(b)
    if fa != fb or fa == "other":
        raise ConfigError(f"{what}: watermarks are not comparable across sides "
                          f"({type(a).__name__} vs {type(b).__name__}); both must be datetimes "
                          "or both numbers")


def later(a: Any, b: Any) -> bool:
    """a is strictly after b, as instants."""
    check_comparable(a, b, "watermark order")
    return instant(a) > instant(b)


def same(a: Any, b: Any) -> bool:
    """Equal as instants (a naive UTC and the equivalent offset-aware value are the same)."""
    if a is None or b is None:
        return a is None and b is None
    if family(a) != family(b):
        return False
    return instant(a) == instant(b)


def lag_seconds(src: Any, tgt: Any) -> float | None:
    """src - tgt in seconds, or None when either is null or the two are not comparable."""
    if src is None or tgt is None:
        return None
    if family(src) == family(tgt) == "datetime":
        return (instant(src) - instant(tgt)).total_seconds()
    if family(src) == family(tgt) == "number":
        return float(decimal.Decimal(str(src)) - decimal.Decimal(str(tgt)))
    return None
