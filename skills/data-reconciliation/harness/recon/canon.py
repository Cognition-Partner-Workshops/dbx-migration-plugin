"""Canonicalization engine.

Rules arrive AS DATA from the source profile's recon_canonicalization section, so profile
improvements upgrade recon without touching this code. Each rule is a pure function
(value -> value); a field's rule list is applied in order to BOTH sides before comparison.
"""

from __future__ import annotations

import datetime as dt
import decimal
import uuid
from collections.abc import Callable
from typing import Any

from .config import CanonRule

_MISSING = object()  # sentinel distinct from None: field absent on the document


class CanonError(Exception):
    pass


def _decimal_round(value: Any, params: dict) -> Any:
    if value is None or value is _MISSING:
        return value
    mode = {"half_even": decimal.ROUND_HALF_EVEN,
            "half_up": decimal.ROUND_HALF_UP}[params.get("mode", "half_even")]
    places = int(params.get("places", 10))
    d = value if isinstance(value, decimal.Decimal) else decimal.Decimal(str(value))
    with decimal.localcontext() as ctx:
        ctx.prec = max(28, len(d.as_tuple().digits) + places + 2)
        return d.quantize(decimal.Decimal(1).scaleb(-places), rounding=mode)


def _datetime_utc_truncate_ms(value: Any, params: dict) -> Any:
    if not isinstance(value, dt.datetime):
        return value
    if value.tzinfo is not None:
        value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return value.replace(microsecond=(value.microsecond // 1000) * 1000)


def _datetime_grid_333(value: Any, params: dict) -> Any:
    """SQL Server datetime snaps to 1/300s. Round both sides to a 10ms grid, which is the
    coarsest grid both the .000/.003/.007 pattern and true ms values collapse onto."""
    if not isinstance(value, dt.datetime):
        return value
    value = _datetime_utc_truncate_ms(value, {})
    ms = value.microsecond // 1000
    return value.replace(microsecond=(round(ms / 10) * 10 % 1000) * 1000) + dt.timedelta(
        seconds=1 if round(ms / 10) == 100 else 0)


def _rstrip_spaces(value: Any, params: dict) -> Any:
    return value.rstrip(" ") if isinstance(value, str) else value


def _empty_string_is_null(value: Any, params: dict) -> Any:
    if isinstance(value, str) and value == "":
        return None
    return value


def _null_missing_equiv(value: Any, params: dict) -> Any:
    return None if value is _MISSING else value


def _collation_casefold(value: Any, params: dict) -> Any:
    return value.casefold() if isinstance(value, str) else value


def _uuid_normalize(value: Any, params: dict) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value).lower()
    if isinstance(value, (bytes, bytearray)) and len(value) == 16:
        return str(uuid.UUID(bytes=bytes(value))).lower()
    if isinstance(value, str):
        try:
            return str(uuid.UUID(value)).lower()
        except ValueError:
            return value
    return value


def _identity(value: Any, params: dict) -> Any:
    return value


_RULES: dict[str, Callable[[Any, dict], Any]] = {
    "decimal_round": _decimal_round,
    "datetime_utc_truncate_ms": _datetime_utc_truncate_ms,
    "datetime_grid_333": _datetime_grid_333,
    "rstrip_spaces": _rstrip_spaces,
    "empty_string_is_null": _empty_string_is_null,
    "null_missing_equiv": _null_missing_equiv,
    "collation_casefold": _collation_casefold,
    "uuid_normalize": _uuid_normalize,
    "identity": _identity,
}

MISSING = _MISSING


class Canonicalizer:
    """Applies a field's ordered rule list; records which rules fired for evidence."""

    def __init__(self, rules: list[CanonRule]):
        self._by_name = {}
        for r in rules:
            if r.rule not in _RULES:
                raise CanonError(f"unknown canonicalization rule '{r.rule}'; "
                                 "add it to the harness before delegating it to a profile")
            self._by_name[r.rule] = r

    def apply(self, value: Any, rule_names: list[str]) -> tuple[Any, list[str]]:
        fired: list[str] = []
        for name in rule_names:
            r = self._by_name.get(name)
            if r is None:
                raise CanonError(f"field references rule '{name}' not present in the profile rules file")
            before = value
            value = _RULES[name](value, r.params)
            if value is not before and value != before:
                fired.append(name)
        return value, fired

    def equal(self, source_value: Any, target_value: Any, rule_names: list[str],
              numeric_abs_tol: float = 0.0) -> tuple[bool, list[str]]:
        s, fired_s = self.apply(source_value, rule_names)
        t, fired_t = self.apply(target_value, rule_names)
        fired = sorted(set(fired_s) | set(fired_t))
        if s is None and t is None:
            return True, fired
        if isinstance(s, (int, float, decimal.Decimal)) and isinstance(t, (int, float, decimal.Decimal)) \
                and not isinstance(s, bool) and not isinstance(t, bool):
            return abs(decimal.Decimal(str(s)) - decimal.Decimal(str(t))) <= decimal.Decimal(str(numeric_abs_tol)), fired
        return s == t, fired
