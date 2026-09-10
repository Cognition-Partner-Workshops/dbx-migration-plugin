"""Versioned inputs: mapping spec, tolerance record, canonicalization rules.

All three are loaded from JSON files and cited in every report. The harness refuses to
run if any is missing a version field, because an unversioned input cannot be cited.
"""

from __future__ import annotations

import json
import math
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


# Numeric watermarks carry no time by themselves. epoch_* scale the difference to seconds so
# tier 6 grades it against cdc_lag_max_s; counter (rowversion, a version column) has no time
# meaning, so lag is graded as the number of unapplied source rows against cdc_in_flight_max_rows.
WATERMARK_UNITS = ("datetime", "epoch_s", "epoch_ms", "epoch_us", "counter")

# Change streams the harness can read committed deletes from. sqlserver_cdc: the capture
# instance's change table via cdc.fn_cdc_get_all_changes_<capture>, LSN positions.
DELETE_EVIDENCE_KINDS = ("sqlserver_cdc",)


@dataclass(frozen=True)
class DeleteEvidenceSpec:
    """Where tier 5 finds tombstones for one object: the source capture (CDC capture instance)
    and the target column that records the last source position the feed applied (a checkpoint
    row, or MAX over a landing table), read as MAX(applied_column) FROM applied_table WHERE
    applied_where. Without this block every target-only key stays a finding."""
    kind: str
    capture: str
    applied_table: str
    applied_column: str
    applied_where: str | None = None


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
    # Transactional mode (operational track). watermark: the change column on each side; rows
    # whose source watermark is newer than the target's applied high-watermark are in flight,
    # not defects. unit: what a numeric watermark measures (WATERMARK_UNITS); a datetime column
    # needs none. identity: the source identity/sequence column and its target column, whose
    # owned sequence must be ahead of every migrated key.
    watermark_source: str | None = None
    watermark_target: str | None = None
    watermark_unit: str | None = None
    identity_source: str | None = None
    identity_target: str | None = None
    delete_evidence: DeleteEvidenceSpec | None = None

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
    # Transactional mode: tolerated CDC lag between max(source watermark) and max(target
    # watermark), and the number of key ranges the PK-set diff counts before streaming keys.
    cdc_lag_max_s: float = 0.0
    # Unapplied source rows tolerated when the watermark is a counter (no time unit to grade).
    cdc_in_flight_max_rows: int = 0
    pk_set_ranges: int = 64
    # Tier 5 streams every key range instead of trusting equal range fingerprints (count, sum,
    # sum of squares): the complete comparison for units where a three-or-more-key substitution
    # that preserves both moments must be ruled out, at the cost of pulling every key.
    pk_set_stream_every_range: bool = False
    # A side with no pinned snapshot and no engine change token proves stillness only by
    # (count, max watermark) markers, which miss updates below the max and balanced
    # insert+delete pairs. False (default): such a run is not merge-eligible. True records the
    # STOP A decision to accept marker-only evidence (Sybase ASE, logins without VIEW SERVER STATE).
    accept_marker_only_window: bool = False
    # Tier 7 fails a target that enforces a NOT NULL, unique, foreign-key or CHECK constraint the
    # source does not: such a target rejects writes the legacy application makes today. True
    # records the decision that the tightening is intended and demotes those findings to stats.
    accept_target_only_constraints: bool = False
    # Tier 7 fails when a source CHECK predicate and a target CHECK predicate are both unmatched
    # after canonicalisation (dialect functions, different shapes): the harness can prove neither
    # equivalence nor difference. True records that a human compared the listed pairs by hand.
    accept_unverified_check_constraints: bool = False


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
    for block in ("watermark", "identity"):
        pair = c.get(block)
        if pair is None:
            continue
        if not isinstance(pair, dict) or not pair.get("source") or not pair.get("target"):
            raise ConfigError(f"{block} must be an object with source and target column names")
        validate_identifier(pair["source"])
        validate_identifier(pair["target"])
        if block == "watermark" and pair.get("unit") is not None and pair["unit"] not in WATERMARK_UNITS:
            raise ConfigError(f"watermark unit must be one of {', '.join(WATERMARK_UNITS)}, "
                              f"got {pair['unit']!r}")
    if c.get("delete_evidence") is not None:
        _validate_delete_evidence(c["delete_evidence"])
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


def _validate_segment(what: str, name: Any) -> None:
    """One bare identifier: a capture instance is a sysname the adapter splices into a CDC function
    name, and the applied-position table/column are quoted as single segments inside the target
    schema the run was given. A qualified name would pass preflight and abort the run."""
    validate_identifier(name)
    if "." in name:
        raise ConfigError(f"{what} must be a single identifier segment, got {name!r}")


def _validate_delete_evidence(block: Any) -> None:
    if not isinstance(block, dict):
        raise ConfigError("delete_evidence must be an object")
    if block.get("kind") not in DELETE_EVIDENCE_KINDS:
        raise ConfigError(f"delete_evidence.kind must be one of {', '.join(DELETE_EVIDENCE_KINDS)}, "
                          f"got {block.get('kind')!r}")
    if not isinstance(block.get("capture"), str) or not block["capture"]:
        raise ConfigError("delete_evidence.capture must name the source capture instance")
    _validate_segment("delete_evidence.capture", block["capture"])
    applied = block.get("applied_position")
    if not isinstance(applied, dict) or not applied.get("table") or not applied.get("column"):
        raise ConfigError("delete_evidence.applied_position must be an object with the target "
                          "table and column holding the last applied source position")
    for key in ("table", "column"):
        _validate_segment(f"delete_evidence.applied_position.{key}", applied[key])
    where = applied.get("where")
    if where is not None and not isinstance(where, str):
        raise ConfigError("delete_evidence.applied_position.where must be a string predicate")
    _validate_predicate(where)


def _delete_evidence(c: dict, params: dict[str, str], path: Path) -> DeleteEvidenceSpec | None:
    block = c.get("delete_evidence")
    if block is None:
        return None
    applied = block["applied_position"]
    return DeleteEvidenceSpec(
        kind=block["kind"], capture=block["capture"],
        applied_table=applied["table"], applied_column=applied["column"],
        applied_where=_validate_predicate(substitute_params(applied.get("where"), params, path)))


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
            watermark_source=(c.get("watermark") or {}).get("source"),
            watermark_target=(c.get("watermark") or {}).get("target"),
            watermark_unit=(c.get("watermark") or {}).get("unit"),
            identity_source=(c.get("identity") or {}).get("source"),
            identity_target=(c.get("identity") or {}).get("target"),
            delete_evidence=_delete_evidence(c, params, path),
        ))
    if not objects:
        raise ConfigError(f"{path}: mapping spec has no objects")
    return MappingSpec(version=version, objects=objects)


def _flag(data: dict, key: str, path: Path) -> bool:
    """A tolerance switch is a JSON boolean and nothing else: "false", 0 or null would
    otherwise be coerced and silently widen what the run accepts."""
    value = data.get(key, False)
    if not isinstance(value, bool):
        raise ConfigError(f"{path}: {key} must be a JSON boolean (true/false), got {value!r}")
    return value


def _bound(data: dict, key: str, default: float, path: Path) -> float:
    """A tolerance bound is a finite, non-negative JSON number. NaN compares false against
    everything, so `lag > NaN` would never fail; infinity and negatives widen or invert the
    check; booleans and numeric strings are refused rather than coerced."""
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{path}: {key} must be a JSON number, got {value!r}")
    if not math.isfinite(value) or value < 0:
        raise ConfigError(f"{path}: {key} must be finite and >= 0, got {value!r}")
    return float(value)


def _count(data: dict, key: str, default: int, path: Path) -> int:
    """A tolerance count is a non-negative JSON integer; booleans and fractions are refused."""
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigError(f"{path}: {key} must be a non-negative JSON integer, got {value!r}")
    return value


def _positive_count(data: dict, key: str, default: int, path: Path) -> int:
    """A tolerance count that sizes a plan (ranges, samples, concurrency) must be >= 1: zero
    would silently collapse the plan and a coerced string or boolean would hide a typo."""
    value = _count(data, key, default, path)
    if value < 1:
        raise ConfigError(f"{path}: {key} must be a positive JSON integer, got {value!r}")
    return value


def load_tolerances(path: Path) -> Tolerances:
    data = json.loads(path.read_text())
    version = _require_version(data, path)
    return Tolerances(
        version=version,
        full_diff_row_threshold=_count(data, "full_diff_row_threshold", 100_000, path),
        sample_size=_positive_count(data, "sample_size", 1_000, path),
        numeric_abs_tol=_bound(data, "numeric_abs_tol", 0.0, path),
        aggregate_rel_tol=_bound(data, "aggregate_rel_tol", 0.0, path),
        source_concurrency=_positive_count(data, "source_concurrency", 1, path),
        cdc_lag_max_s=_bound(data, "cdc_lag_max_s", 0.0, path),
        cdc_in_flight_max_rows=_count(data, "cdc_in_flight_max_rows", 0, path),
        pk_set_ranges=_positive_count(data, "pk_set_ranges", 64, path),
        pk_set_stream_every_range=_flag(data, "pk_set_stream_every_range", path),
        accept_marker_only_window=_flag(data, "accept_marker_only_window", path),
        accept_target_only_constraints=_flag(data, "accept_target_only_constraints", path),
        accept_unverified_check_constraints=_flag(data, "accept_unverified_check_constraints", path),
    )


def load_canon_rules(path: Path) -> list[CanonRule]:
    data = json.loads(path.read_text())
    rules = data if isinstance(data, list) else data.get("rules", [])
    return [CanonRule(rule=r["rule"], applies_to=r.get("applies_to", "*"),
                      params=dict(r.get("params", {}))) for r in rules]
