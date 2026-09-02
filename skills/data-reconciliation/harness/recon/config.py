"""Versioned inputs: mapping spec, tolerance record, canonicalization rules.

All three are loaded from JSON files and cited in every report. The harness refuses to
run if any is missing a version field, because an unversioned input cannot be cited.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class ConfigError(Exception):
    pass


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)*$")
READ_ONLY_SQL_KEYWORDS = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|truncate|grant|revoke|call|"
    r"exec|execute|copy|unload|into)\b", re.IGNORECASE)
READ_ONLY_PREDICATE_KEYWORDS = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|truncate|grant|revoke|call|"
    r"exec|execute|copy|unload|into|union)\b", re.IGNORECASE)


def validate_identifier(name: str) -> str:
    if not isinstance(name, str) or not IDENTIFIER_RE.fullmatch(name):
        raise ConfigError(f"invalid identifier: {name!r}")
    return name


def _validate_predicate(value: str | None) -> str | None:
    if (value is not None and
            (any(token in value for token in (";", "--", "/*"))
             or READ_ONLY_PREDICATE_KEYWORDS.search(value))):
        raise ConfigError("predicates must be a single expression")
    return value


@dataclass(frozen=True)
class FieldMapping:
    source: str
    target: str
    source_type: str
    target_type: str
    rules: list[str] = field(default_factory=list)  # canonicalization rule names, in order


@dataclass(frozen=True)
class EmbedMapping:
    """A child table folded into an ARRAY<STRUCT> column on the target. Rare in warehouse
    migrations; most specs have no embeds."""
    array_path: str
    child_table: str
    # Optional filter on the child table when only a subset embeds.
    child_where: str | None = None
    target_where: str | None = None
    # Value grading (Tier 3). Without these, only cardinality is checked (Tier 1) and the
    # embed is reported as UNGRADED in every result: an ungraded embed is never silent.
    parent_key: list[str] = field(default_factory=list)  # child cols joining to the root key
    key_source: list[str] = field(default_factory=list)  # child cols identifying an element
    key_target: str = ""                                 # element field carrying that key
    fields: list[FieldMapping] = field(default_factory=list)


@dataclass(frozen=True)
class ObjectMapping:
    object: str
    root_table: str
    key_source: list[str]
    key_target: list[str]
    fields: list[FieldMapping]
    embeds: list[EmbedMapping] = field(default_factory=list)
    root_where: str | None = None
    target_where: str | None = None

    def __post_init__(self):
        if isinstance(self.key_target, str):
            object.__setattr__(self, "key_target", [self.key_target])


@dataclass(frozen=True)
class MappingSpec:
    version: str
    objects: list[ObjectMapping]


@dataclass(frozen=True)
class Tolerances:
    version: str
    full_diff_row_threshold: int = 100_000
    sample_size: int = 1_000
    numeric_abs_tol: float = 0.0
    aggregate_rel_tol: float = 0.0
    source_concurrency: int = 1


@dataclass(frozen=True)
class CanonRule:
    rule: str
    applies_to: str
    params: dict[str, Any] = field(default_factory=dict)


def substitute_params(text: str | None, params: dict[str, str], path: Path) -> str | None:
    """Resolve ${name} placeholders (e.g. batch/namespace scoping in root_where) from
    runner parameters, so a new batch is a parameter, not a mapping-spec version bump."""
    if text is None:
        return None
    def repl(m):
        name = m.group(1)
        if name not in params:
            raise ConfigError(f"{path}: unresolved placeholder '${{{name}}}'; pass --param {name}=<value>")
        return str(params[name])
    return re.sub(r"\$\{(\w+)\}", repl, text)


def _require_version(data: dict, path: Path) -> str:
    version = data.get("version")
    if not version:
        raise ConfigError(f"{path}: missing 'version'; unversioned inputs cannot be cited in evidence")
    return str(version)


def _field_mappings(items: list[dict]) -> list[FieldMapping]:
    return [FieldMapping(
        source=f["source"], target=f["target"],
        source_type=f.get("source_type", ""), target_type=f.get("target_type", ""),
        rules=list(f.get("rules", [])),
    ) for f in items]


def _validate_mapping_identifiers(c: dict) -> None:
    validate_identifier(c.get("object") or c.get("target_table") or c.get("source_table"))
    validate_identifier(c.get("root_table") or c.get("source_table"))
    key = c.get("key") or {}
    for name in key.get("source", []):
        validate_identifier(name)
    targets = key.get("target", "")
    if isinstance(targets, str):
        targets = [targets]
    if not isinstance(targets, list) or not targets:
        raise ConfigError("comparison key target must be a string or list of strings")
    for name in targets:
        validate_identifier(name)
    for f in c.get("fields", []):
        validate_identifier(f["source"])
        validate_identifier(f["target"])
    for e in c.get("embeds", []):
        validate_identifier(e["array_path"])
        validate_identifier(e["child_table"])
        for name in e.get("parent_key", []):
            validate_identifier(name)
        ekey = e.get("key") or {}
        for name in ekey.get("source", []):
            validate_identifier(name)
        if ekey.get("target"):
            validate_identifier(ekey["target"])
        for f in e.get("fields", []):
            validate_identifier(f["source"])
            validate_identifier(f["target"])


def load_mapping_spec(path: Path, params: dict[str, str] | None = None) -> MappingSpec:
    data = json.loads(path.read_text())
    version = _require_version(data, path)
    params = params or {}
    objects = []
    for c in data.get("objects") or data.get("tables") or []:
        _validate_mapping_identifiers(c)
        fields_ = _field_mappings(c.get("fields", []))
        embeds = []
        for e in c.get("embeds", []):
            ekey = e.get("key") or {}
            embeds.append(EmbedMapping(
                array_path=e["array_path"], child_table=e["child_table"],
                child_where=_validate_predicate(substitute_params(e.get("child_where"), params, path)),
                target_where=_validate_predicate(substitute_params(e.get("target_where"), params, path)),
                parent_key=list(e.get("parent_key", [])),
                key_source=list(ekey.get("source", [])), key_target=ekey.get("target", ""),
                fields=_field_mappings(e.get("fields", [])),
            ))
        key = c.get("key") or {}
        if not key.get("source") or not key.get("target"):
            raise ConfigError(
                f"{path}: object '{c.get('object')}' has no comparison key; "
                "every target table must declare a business key in the mapping spec")
        key_targets = [key["target"]] if isinstance(key["target"], str) else list(key["target"])
        if len(key["source"]) != len(key_targets):
            raise ConfigError(
                f"{path}: object '{c.get('object')}' comparison key source/target lengths differ")
        objects.append(ObjectMapping(
            object=c.get("object") or c["target_table"], root_table=c.get("root_table") or c["source_table"],
            key_source=list(key["source"]), key_target=key_targets,
            fields=fields_, embeds=embeds,
            root_where=_validate_predicate(substitute_params(c.get("root_where"), params, path)),
            target_where=_validate_predicate(substitute_params(c.get("target_where"), params, path)),
        ))
    if not objects:
        raise ConfigError(f"{path}: mapping spec has no objects")
    return MappingSpec(version=version, objects=objects)


def load_tolerances(path: Path) -> Tolerances:
    data = json.loads(path.read_text())
    version = _require_version(data, path)
    return Tolerances(
        version=version,
        full_diff_row_threshold=int(data.get("full_diff_row_threshold", 100_000)),
        sample_size=int(data.get("sample_size", 1_000)),
        numeric_abs_tol=float(data.get("numeric_abs_tol", 0.0)),
        aggregate_rel_tol=float(data.get("aggregate_rel_tol", 0.0)),
        source_concurrency=int(data.get("source_concurrency", 1)),
    )


def load_canon_rules(path: Path) -> list[CanonRule]:
    data = json.loads(path.read_text())
    rules = data if isinstance(data, list) else data.get("rules", [])
    return [CanonRule(rule=r["rule"], applies_to=r.get("applies_to", "*"),
                      params=dict(r.get("params", {}))) for r in rules]
