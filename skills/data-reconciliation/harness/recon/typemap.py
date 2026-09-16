"""Source-family type maps: the dialect skill's canonicalization.json can carry a
`type_map.<family>` machine table the harness applies to a mapping spec at load time
(filling empty target_type, refusing a contradicting one) and the doctor audits against.
Types parse to (name, args) under one rule for source types, spec target types and map
patterns: lowercase, whitespace collapsed, every (...) group lifted out into args in order
(so INTERVAL DAY(2) TO SECOND(6) and TIMESTAMP(6) WITH LOCAL TIME ZONE both parse).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from pathlib import Path

from .config import ConfigError, MappingSpec

_PAREN = re.compile(r"\(([^)]*)\)")
_WS = re.compile(r"\s+")

# Delta-side spellings folded to one name before comparison. timestamptz is NOT `timestamp`:
# Delta's TIMESTAMP is the session-zoned type, so a zoned spelling never aliases to it.
_ALIASES = {
    "numeric": "decimal", "dec": "decimal", "integer": "int", "long": "bigint",
    "real": "float", "double precision": "double",
    "timestamp without time zone": "timestamp_ntz",
    "timestamp with time zone": "timestamp_tz", "timestamptz": "timestamp_tz",
    "timestamp_ltz": "timestamp",
}


@dataclass(frozen=True)
class TypeRule:
    source: str
    target: str
    accepts: tuple[str, ...] = ()
    p_max: int | None = None


@dataclass(frozen=True)
class TypeMap:
    family: str
    note: str
    rules: tuple[TypeRule, ...]


def _parse(text: str) -> tuple[str, tuple]:
    t = _WS.sub(" ", str(text).strip().lower())
    args = tuple(_arg(a) for group in _PAREN.findall(t) for a in group.split(","))
    name = _WS.sub(" ", _PAREN.sub(" ", t)).strip()
    return _ALIASES.get(name, name), args


def _arg(token: str):
    first = token.strip().split(" ")[0] if token.strip() else ""
    return int(first) if first.isdigit() else first


def _match(rule: TypeRule, name: str, args: tuple) -> dict | None:
    pname, pargs = _parse(rule.source)
    if pname != name or len(pargs) != len(args):
        return None
    bound = {}
    for pat, arg in zip(pargs, args):
        if pat == "*":
            if arg != "*":
                return None
        elif isinstance(pat, int):
            if arg != pat:
                return None
        elif isinstance(arg, int):
            bound[pat] = arg
        else:
            return None
    if rule.p_max is not None and bound.get("p", 0) > rule.p_max:
        return None
    return bound


def _render(pattern: str, bound: dict) -> str:
    name, args = _parse(pattern)
    if not args:
        return name
    return f"{name}({','.join(str(bound.get(a, a)) for a in args)})"


def _target_matches(dname: str, dargs: tuple, pattern: str) -> bool:
    pname, pargs = _parse(pattern)
    if pname != dname or len(pargs) != len(dargs):
        return False
    for pat, arg in zip(pargs, dargs):
        if pat == "*":
            if not isinstance(arg, int) or arg < 1:  # a * scale keeps fractions; 0 truncates
                return False
        elif pat != arg:
            return False
    return True


def load_type_map(path: Path, family: str) -> TypeMap | None:
    try:
        data = json.loads(Path(path).read_text())
    except OSError:
        return None  # load_canon_rules reports the unreadable file itself
    if not isinstance(data, dict):
        return None
    entry = (data.get("type_map") or {}).get(family)
    if entry is None:
        return None
    rules = []
    for r in entry.get("types", []):
        if not isinstance(r, dict) or not r.get("source") or not r.get("target"):
            raise ConfigError(f"{path}: type_map.{family} entry missing source/target: {r}")
        rules.append(TypeRule(source=r["source"], target=r["target"],
                              accepts=tuple(r.get("accepts", ())), p_max=r.get("p_max")))
    return TypeMap(family=family, note=entry.get("note", ""), rules=tuple(rules))


def type_map_families(path: Path) -> list[str]:
    data = json.loads(Path(path).read_text())
    if not isinstance(data, dict):
        return []
    return sorted((data.get("type_map") or {}).keys())


def expected_target(tm: TypeMap, source_type: str) -> tuple[str, tuple[str, ...]] | None:
    name, args = _parse(source_type)
    if not name:
        return None
    for rule in tm.rules:
        bound = _match(rule, name, args)
        if bound is not None:
            return _render(rule.target, bound), tuple(_render(a, bound) for a in rule.accepts)
    return None


def audit_field(tm: TypeMap, source_type: str, target_type: str) -> tuple[str, str | None]:
    found = expected_target(tm, source_type)
    if found is None:
        return "unmapped", None
    expected, accepts = found
    if not (target_type or "").strip():
        return "undeclared", expected
    dname, dargs = _parse(target_type)
    for pat in (expected, *accepts):
        if _target_matches(dname, dargs, pat):
            return "ok", expected
    return "contradiction", expected


def _each_field(spec: MappingSpec):
    for o in spec.objects:
        yield o, o.object, o.fields
        for e in o.embeds:
            yield e, f"{o.object}.{e.array_path}", e.fields


def audit_spec(tm: TypeMap, spec: MappingSpec) -> list[dict]:
    rows = []
    for _container, label, fields in _each_field(spec):
        for f in fields:
            status, expected = audit_field(tm, f.source_type, f.target_type)
            rows.append({"object": label, "source": f.source, "source_type": f.source_type,
                         "target_type": f.target_type, "expected": expected, "status": status})
    return rows


def apply_type_map(tm: TypeMap, spec: MappingSpec) -> tuple[MappingSpec, dict]:
    filled, unmapped, errors = [], [], []

    def fix(label: str, f):
        name = f"{label}.{f.source}"
        found = expected_target(tm, f.source_type)
        if found is None:
            unmapped.append(name)
            return f
        expected, accepts = found
        status, _ = audit_field(tm, f.source_type, f.target_type)
        if status == "undeclared":
            filled.append(name)
            return replace(f, target_type=expected)
        if status == "contradiction":
            acc = f" (accepted: {', '.join(accepts)})" if accepts else ""
            errors.append(f"{name}: {f.source_type} -> declared {f.target_type}, "
                          f"map says {expected}{acc}")
        return f

    objects = []
    for o in spec.objects:
        objects.append(replace(o,
                               fields=[fix(o.object, f) for f in o.fields],
                               embeds=[replace(e, fields=[fix(f"{o.object}.{e.array_path}", f)
                                                          for f in e.fields])
                                       for e in o.embeds]))
    if errors:
        raise ConfigError(f"{len(errors)} field(s) contradict the {tm.family} type map: "
                          + "; ".join(errors))
    return replace(spec, objects=objects), {"family": tm.family, "filled": filled, "unmapped": unmapped}
