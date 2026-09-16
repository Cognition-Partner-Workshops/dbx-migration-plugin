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
    "int8": "bigint", "int4": "int", "real": "float", "float4": "float",
    "double precision": "double", "float8": "double",
    "bool": "boolean", "character": "char", "character varying": "varchar",
    "timestamp without time zone": "timestamp_ntz",
    "timestamp with time zone": "timestamp_tz", "timestamptz": "timestamp_tz",
    "timestamp_ltz": "timestamp",
}

# spellings that mean something else on a specific target kind, applied before _ALIASES
# when parsing types spelled for that kind (declared targets and map patterns)
_KIND_ALIASES = {
    "lakebase": {"timestamp without time zone": "timestamp"},
}

# decimal-like source types whose (p,s) edge shapes normalise before matching
_DECIMAL_SOURCES = {"number", "decimal", "numeric"}


@dataclass(frozen=True)
class TypeRule:
    source: str
    target: str
    accepts: tuple[str, ...] = ()
    # alternative spellings accepted only when the field carries the named canonicalization
    # rule: (alternative, required rule name)
    conditional: tuple[tuple[str, str], ...] = ()
    arg_max: tuple[tuple[str, int], ...] = ()  # bound pattern arg -> max value


@dataclass(frozen=True)
class TypeMap:
    family: str
    target_kind: str
    note: str
    rules: tuple[TypeRule, ...]
    decimal_max_precision: int | None = None


def _parse(text: str, aliases: bool = True, kind: str | None = None) -> tuple[str, tuple]:
    t = _WS.sub(" ", str(text).strip().lower())
    args = tuple(_arg(a) for group in _PAREN.findall(t) for a in group.split(","))
    name = _WS.sub(" ", _PAREN.sub(" ", t)).strip()
    if not aliases:
        return name, args
    return _KIND_ALIASES.get(kind, {}).get(name, _ALIASES.get(name, name)), args


def _arg(token: str):
    first = token.strip().split(" ")[0] if token.strip() else ""
    return int(first) if first.lstrip("-").isdigit() else first


def _norm_decimal(name: str, args: tuple) -> tuple:
    """Oracle NUMBER(p,s) edge shapes, as decimal arithmetic, not Oracle specifics:
    s < 0 rounds to whole 10^-s so (p+|s|, 0) is exact; s > p means |x| < 0.1 with
    s fractional digits, so (s, s)."""
    if name in _DECIMAL_SOURCES and len(args) == 2 and args[0] == "*" and isinstance(args[1], int):
        args = (38, args[1])  # an open precision is decimal-max wide
    if name in _DECIMAL_SOURCES and len(args) == 2 and all(isinstance(a, int) for a in args):
        p, s = args
        if s < 0:
            return (p + abs(s), 0)
        if s > p:
            return (s, s)
    return args


def read_as(source_type: str) -> str | None:
    """The normalised spelling when _norm_decimal rewrote the args (e.g. NUMBER(7,0))."""
    name, args = _parse(source_type)
    norm = _norm_decimal(name, args)
    if norm == args:
        return None
    return f"{name}({','.join(str(a) for a in norm)})".upper()


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
    for key, mx in rule.arg_max:
        if bound.get(key, 0) > mx:
            return None
    return bound


def _render(pattern: str, bound: dict) -> str:
    # the kind's own spelling, args substituted where the pattern put them (numeric stays
    # numeric, `timestamp(n) with time zone` keeps its mid-name paren group)
    if not _PAREN.search(pattern):
        return _parse(pattern, aliases=False)[0]
    return _WS.sub(" ", _PAREN.sub(
        lambda m: "(" + ",".join(str(bound.get(a.strip(), a.strip()))
                                 for a in m.group(1).split(",")) + ")",
        pattern.strip().lower()))


def _target_matches(dname: str, dargs: tuple, pattern: str, kind: str | None = None) -> bool:
    pname, pargs = _parse(pattern, kind=kind)
    if pname != dname or len(pargs) != len(dargs):
        return False
    for pat, arg in zip(pargs, dargs):
        if pat == "*":
            if not isinstance(arg, int) or arg < 1:  # a * scale keeps fractions; 0 truncates
                return False
        elif pat != arg:
            return False
    return True


def load_type_map(path: Path, family: str, target_kind: str) -> TypeMap | None:
    try:
        data = json.loads(Path(path).read_text())
    except OSError:
        return None  # load_canon_rules reports the unreadable file itself
    if not isinstance(data, dict):
        return None
    entry = (data.get("type_map") or {}).get(family)
    if not isinstance(entry, dict):
        return None
    entry = entry.get(target_kind)
    if entry is None:
        return None
    rules = []
    for r in entry.get("types", []):
        if not isinstance(r, dict) or not r.get("source") or not r.get("target"):
            raise ConfigError(f"{path}: type_map.{family}.{target_kind} entry missing source/target: {r}")
        arg_max = dict(r.get("arg_max") or {})
        if r.get("p_max") is not None:
            arg_max.setdefault("p", r["p_max"])
        rules.append(TypeRule(source=r["source"], target=r["target"],
                              accepts=tuple(r.get("accepts", ())),
                              conditional=tuple(sorted((r.get("conditional") or {}).items())),
                              arg_max=tuple(sorted(arg_max.items()))))
    return TypeMap(family=family, target_kind=target_kind, note=entry.get("note", ""),
                   rules=tuple(rules), decimal_max_precision=entry.get("decimal_max_precision"))


def type_map_families(path: Path) -> list[str]:
    data = json.loads(Path(path).read_text())
    if not isinstance(data, dict):
        return []
    return sorted((data.get("type_map") or {}).keys())


def type_map_targets(path: Path, family: str) -> list[str]:
    data = json.loads(Path(path).read_text())
    if not isinstance(data, dict):
        return []
    entry = (data.get("type_map") or {}).get(family)
    return sorted(entry.keys()) if isinstance(entry, dict) else []


def _precision_error(tm: TypeMap, source_type: str) -> str | None:
    """A normalised decimal wider than the target kind can hold is a defect, not a fill."""
    if tm.decimal_max_precision is None:
        return None
    name, args = _parse(source_type)
    args = _norm_decimal(name, args)
    if (name in _DECIMAL_SOURCES and len(args) == 2 and isinstance(args[0], int)
            and args[0] > tm.decimal_max_precision):
        return (f"{source_type} needs decimal({args[0]},{args[1]}); "
                f"{tm.target_kind} decimals stop at {tm.decimal_max_precision}")
    return None


def expected_target(tm: TypeMap, source_type: str) -> tuple[str, tuple[str, ...], tuple] | None:
    name, args = _parse(source_type)
    if not name:
        return None
    args = _norm_decimal(name, args)
    for rule in tm.rules:
        bound = _match(rule, name, args)
        if bound is not None:
            return (_render(rule.target, bound),
                    tuple(_render(a, bound) for a in rule.accepts), rule.conditional)
    return None


def audit_field(tm: TypeMap, source_type: str, target_type: str,
                evidence: tuple | list = ()) -> tuple[str, str | None]:
    if (err := _precision_error(tm, source_type)) is not None:
        return "unrepresentable", err
    found = expected_target(tm, source_type)
    if found is None:
        return "unmapped", None
    expected, accepts, conditional = found
    if not (target_type or "").strip():
        return "undeclared", expected
    dname, dargs = _parse(target_type, kind=tm.target_kind)
    # a declared decimal-family (p,s) that can't exist on the kind is a contradiction even
    # when a wildcard pattern would swallow it
    if (dname in _DECIMAL_SOURCES and len(dargs) == 2
            and all(isinstance(a, int) for a in dargs)):
        p_, s_ = dargs
        bad = "s > p" if s_ > p_ else ("s < 0" if s_ < 0 else
              ("p < 1" if p_ < 1 else
               f"precision past {tm.target_kind}'s {tm.decimal_max_precision}"
               if tm.decimal_max_precision is not None and p_ > tm.decimal_max_precision else None))
        if bad:
            cap = (f"; {tm.target_kind} decimals stop at {tm.decimal_max_precision}"
                   if tm.decimal_max_precision is not None else "")
            return "contradiction", (f"declared {dname}({p_},{s_}) is not a valid "
                                     f"{tm.target_kind} decimal: {bad}{cap}")
    for pat in (expected, *accepts):
        if _target_matches(dname, dargs, pat, tm.target_kind):
            return "ok", expected
    for alt, token in conditional:
        if _target_matches(dname, dargs, alt, tm.target_kind):
            if token in evidence:
                return "ok", expected
            return "contradiction", f"{expected} ({alt} needs rule {token} on the field)"
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
            status, expected = audit_field(tm, f.source_type, f.target_type, f.evidence)
            rows.append({"object": label, "source": f.source, "source_type": f.source_type,
                         "read_as": read_as(f.source_type),
                         "target_type": f.target_type, "expected": expected, "status": status})
    return rows


def apply_type_map(tm: TypeMap, spec: MappingSpec) -> tuple[MappingSpec, dict]:
    filled, unmapped, errors = [], [], []

    def fix(label: str, f):
        name = f"{label}.{f.source}"
        if (err := _precision_error(tm, f.source_type)) is not None:
            errors.append(f"{name}: {err}")
            return f
        found = expected_target(tm, f.source_type)
        if found is None:
            unmapped.append(name)
            return f
        expected, accepts, _cond = found
        status, detail = audit_field(tm, f.source_type, f.target_type, f.evidence)
        if status == "undeclared":
            filled.append(name)
            return replace(f, target_type=expected)
        if status == "contradiction":
            acc = f" (accepted: {', '.join(accepts)})" if accepts else ""
            ra = read_as(f.source_type)
            src = f"{f.source_type} -> read as {ra}" if ra else f.source_type
            errors.append(f"{name}: {src} -> declared {f.target_type}, "
                          f"map says {detail}{acc}")
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
    return replace(spec, objects=objects), {"family": tm.family, "target_kind": tm.target_kind,
                                            "filled": filled, "unmapped": unmapped}
