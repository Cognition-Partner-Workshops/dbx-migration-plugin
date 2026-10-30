
from __future__ import annotations

import datetime as dt
import decimal
from typing import Any

from .config import ConfigError

_NUMBER = (int, float, decimal.Decimal)
_BINARY = (bytes, bytearray, memoryview)

def counter_value(value: bytes | bytearray | memoryview) -> int:
    return int.from_bytes(bytes(value), "big", signed=False)

def instant(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time())
    if isinstance(value, _BINARY):
        return counter_value(value)
    return value

def in_form_of(value: Any, like: Any) -> Any:
    if isinstance(like, _BINARY) and isinstance(value, _NUMBER) and not isinstance(value, bool):
        whole = int(value)
        if whole == value and 0 <= whole < 256 ** len(like):
            return whole.to_bytes(len(like), "big")
    elif isinstance(value, _BINARY) and isinstance(like, _NUMBER) and not isinstance(like, bool):
        return counter_value(value)
    return value

def family(value: Any) -> str:
    if isinstance(value, bool):
        return "other"
    if isinstance(value, (*_NUMBER, *_BINARY)):
        return "number"
    if isinstance(value, (dt.datetime, dt.date)):
        return "datetime"
    return "other"

def check_comparable(a: Any, b: Any, what: str) -> None:
    if a is None or b is None:
        return
    fa, fb = family(a), family(b)
    if fa != fb or fa == "other":
        raise ConfigError(f"{what}: watermarks are not comparable across sides "
                          f"({type(a).__name__} vs {type(b).__name__}); both must be datetimes "
                          "or both numbers")

def later(a: Any, b: Any) -> bool:
    check_comparable(a, b, "watermark order")
    return instant(a) > instant(b)

def same(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if family(a) != family(b):
        return False
    return instant(a) == instant(b)

def literal(value: Any, utc_offset: bool = False, binary: str = "0x{hex}") -> str:
    if isinstance(value, dt.datetime):
        text = instant(value).isoformat(sep=" ", timespec="microseconds")
        return f"'{text}+00:00'" if utc_offset else f"'{text}'"
    if isinstance(value, dt.date):
        return f"'{value.isoformat()}'"
    if isinstance(value, _BINARY):
        return binary.format(hex=bytes(value).hex())
    if isinstance(value, bool) or not isinstance(value, _NUMBER):
        raise ConfigError(f"watermark values must be datetimes or numbers, got {type(value).__name__}")
    return str(value)

EPOCH_SCALE = {"epoch_s": decimal.Decimal(1), "epoch_ms": decimal.Decimal("0.001"),
               "epoch_us": decimal.Decimal("0.000001")}

def lag_units(src: Any, tgt: Any) -> decimal.Decimal | None:
    if src is None or tgt is None or not (family(src) == family(tgt) == "number"):
        return None
    return decimal.Decimal(str(instant(src))) - decimal.Decimal(str(instant(tgt)))

def lag_seconds(src: Any, tgt: Any, unit: str | None = None) -> float | None:
    if src is None or tgt is None:
        return None
    if family(src) == family(tgt) == "datetime":
        return (instant(src) - instant(tgt)).total_seconds()
    if family(src) == family(tgt) == "number" and unit in EPOCH_SCALE:
        return float(lag_units(src, tgt) * EPOCH_SCALE[unit])
    return None
