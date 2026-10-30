
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

class ConfigError(Exception):
    pass

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)*$")
READ_ONLY_SQL_KEYWORDS = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|truncate|grant|revoke|call|"
    r"exec|execute|copy|unload|into)\b", re.IGNORECASE)

def validate_identifier(name: str) -> str:
    if not isinstance(name, str) or not IDENTIFIER_RE.fullmatch(name):
        raise ConfigError(f"invalid identifier: {name!r}")
    return name

_SEGMENT = r'(?:[A-Za-z_][\w$]*|\[[^\]]+\]|"(?:[^"]|"")+")'
_PREDICATE_TOKEN = re.compile(
    r"\s+|(?P<string>'(?:[^']|'')*')|(?P<param>\$\{\w+\})"
    r"|(?P<number>-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)"
    rf"|(?P<word>{_SEGMENT}(?:\.{_SEGMENT})*)|(?P<punct><>|!=|<=|>=|[=<>(),])")
_KEYWORDS = {"and", "or", "not", "in", "between", "is", "null", "like"}
_RESERVED = {"select", "from", "where", "exists", "case", "when", "then", "else", "end", "union",
             "join", *READ_ONLY_SQL_KEYWORDS.pattern[3:-3].split("|")}

class _Predicate:
    def __init__(self, text: str):
        self.text, self.tokens, pos = text, [], 0
        while pos < len(text):
            m = _PREDICATE_TOKEN.match(text, pos)
            if not m:
                self.fail(re.match(r"\S+", text[pos:]).group())
            if m.lastgroup:
                self.tokens.append((m.lastgroup, m.group()))
            pos = m.end()
        self.i = 0

    def fail(self, token: str | None = None):
        got = repr(token if token is not None else self.tokens[self.i][1]) \
            if token is not None or self.i < len(self.tokens) else "end of predicate"
        raise ConfigError(f"predicates must be a single expression: unexpected {got} in {self.text!r}")

    def take(self, *wanted: str) -> bool:
        if self.i < len(self.tokens) and self.tokens[self.i][1].lower() in wanted:
            self.i += 1
            return True
        return False

    def expect(self, *wanted: str) -> None:
        if not self.take(*wanted):
            self.fail()

    def parse(self) -> None:
        self.predicate()
        if self.i < len(self.tokens):
            self.fail()

    def predicate(self) -> None:
        self.term()
        while self.take("and", "or"):
            self.term()

    def term(self) -> None:
        if self.take("not"):
            return self.term()
        if self.take("("):
            self.predicate()
            return self.expect(")")
        self.operand()
        if self.take("=", "<>", "!=", "<", "<=", ">", ">="):
            return self.operand()
        negated = self.take("not")
        if self.take("in"):
            self.expect("(")
            self.literal()
            while self.take(","):
                self.literal()
            self.expect(")")
        elif self.take("between"):
            self.operand()
            self.expect("and")
            self.operand()
        elif self.take("like"):
            self.operand()
        elif not negated and self.take("is"):
            self.take("not")
            self.expect("null")
        else:
            self.fail()

    def _typed_literal(self) -> bool:
        return (self.i + 1 < len(self.tokens) and self.tokens[self.i][1].lower() in ("date", "timestamp")
                and self.tokens[self.i + 1][0] == "string")

    def operand(self) -> None:
        kind, text = self.tokens[self.i] if self.i < len(self.tokens) else (None, "")
        if kind == "word" and text.lower() not in _KEYWORDS and not self._typed_literal():
            if text.lower() in _RESERVED:
                self.fail()
            if self.i + 1 < len(self.tokens) and self.tokens[self.i + 1][1] == "(":
                self.fail(text + "(")
            self.i += 1
        else:
            self.literal()

    def literal(self) -> None:
        if self._typed_literal():
            self.i += 1
        kind = self.tokens[self.i][0] if self.i < len(self.tokens) else None
        if kind not in ("string", "number", "param"):
            self.fail()
        self.i += 1

def validate_predicate(value: str | None) -> str | None:
    if value is not None:
        _Predicate(value).parse()
    return value

@dataclass(frozen=True)
class FieldMapping:
    source: str
    target: str
    source_type: str
    target_type: str
    rules: list[str] = field(default_factory=list)  # canonicalization rule names, in order
    evidence: list[str] = field(default_factory=list)

@dataclass(frozen=True)
class EmbedMapping:
    array_path: str
    child_table: str
    child_where: str | None = None
    target_where: str | None = None
    parent_key: list[str] = field(default_factory=list)  # child cols joining to the root key
    key_source: list[str] = field(default_factory=list)  # child cols identifying an element
    key_target: str = ""                                 # element field carrying that key
    fields: list[FieldMapping] = field(default_factory=list)

WATERMARK_UNITS = ("datetime", "epoch_s", "epoch_ms", "epoch_us", "counter")

DELETE_EVIDENCE_KINDS = ("sqlserver_cdc",)

@dataclass(frozen=True)
class DeleteEvidenceSpec:
    kind: str
    capture: str
    applied_table: str
    applied_column: str
    applied_where: str | None = None

GRADINGS = ("full", "by_design_unverified")

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
    grading: str = "full"
    watermark_source: str | None = None
    watermark_target: str | None = None
    watermark_unit: str | None = None
    identity_source: str | None = None
    identity_target: str | None = None
    delete_evidence: DeleteEvidenceSpec | None = None

    def __post_init__(self):
        if isinstance(self.key_target, str):
            object.__setattr__(self, "key_target", [self.key_target])

RERUN_POSTURES = ("required", "first_run_baseline", "not_applicable")

@dataclass(frozen=True)
class MappingSpec:
    version: str
    objects: list[ObjectMapping]
    principal_map: dict[str, str] = field(default_factory=dict)
    rerun_posture: str = "required"

FINDING_CLASSES = ("structural", "privilege_visibility", "evidence", "rerun_policy")

@dataclass(frozen=True)
class Tolerances:
    version: str
    full_diff_row_threshold: int = 100_000
    sample_size: int = 1_000
    numeric_abs_tol: float = 0.0
    aggregate_rel_tol: float = 0.0
    source_concurrency: int = 1
    cdc_lag_max_s: float = 0.0
    cdc_in_flight_max_rows: int = 0
    pk_set_ranges: int = 64
    pk_set_stream_every_range: bool = False
    accept_marker_only_window: bool = False
    accept_target_only_constraints: bool = False
    accept_unverified_check_constraints: bool = False
    exact: bool = True
    per_type: dict = field(default_factory=dict)
    source_lag_s: float = 0.0
    mirror_lag_s: float = 0.0
    green_cycles: int = 3
    blocking_findings: tuple = ()

@dataclass(frozen=True)
class CanonRule:
    rule: str
    applies_to: str
    params: dict[str, Any] = field(default_factory=dict)

def substitute_params(text: str | None, params: dict[str, str], path: Path) -> str | None:
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
    fields = []
    for f in items:
        evidence = f.get("evidence", [])
        if not isinstance(evidence, list) or any(not isinstance(e, str) for e in evidence):
            raise ConfigError(f"field {f.get('source')}: 'evidence' must be a list of strings")
        fields.append(FieldMapping(
            source=f["source"], target=f["target"],
            source_type=f.get("source_type", ""), target_type=f.get("target_type", ""),
            rules=list(f.get("rules", [])), evidence=list(evidence)))
    return fields

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
    validate_predicate(where)

def _delete_evidence(c: dict, params: dict[str, str], path: Path) -> DeleteEvidenceSpec | None:
    block = c.get("delete_evidence")
    if block is None:
        return None
    applied = block["applied_position"]
    return DeleteEvidenceSpec(
        kind=block["kind"], capture=block["capture"],
        applied_table=applied["table"], applied_column=applied["column"],
        applied_where=validate_predicate(substitute_params(applied.get("where"), params, path)))

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
                child_where=validate_predicate(substitute_params(e.get("child_where"), params, path)),
                target_where=validate_predicate(substitute_params(e.get("target_where"), params, path)),
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
        grading = c.get("grading", "full")
        if grading not in GRADINGS:
            raise ConfigError(f"{path}: object '{c.get('object')}' grading must be one of {GRADINGS}, got {grading!r}")
        objects.append(ObjectMapping(
            object=c.get("object") or c["target_table"], root_table=c.get("root_table") or c["source_table"],
            key_source=list(key["source"]), key_target=key_targets,
            fields=fields_, embeds=embeds,
            root_where=validate_predicate(substitute_params(c.get("root_where"), params, path)),
            target_where=validate_predicate(substitute_params(c.get("target_where"), params, path)),
            watermark_source=(c.get("watermark") or {}).get("source"),
            watermark_target=(c.get("watermark") or {}).get("target"),
            watermark_unit=(c.get("watermark") or {}).get("unit"),
            identity_source=(c.get("identity") or {}).get("source"),
            identity_target=(c.get("identity") or {}).get("target"),
            delete_evidence=_delete_evidence(c, params, path),
            grading=grading,
        ))
    if not objects:
        raise ConfigError(f"{path}: mapping spec has no objects")
    principal_map = data.get("principal_map") or {}
    if not isinstance(principal_map, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in principal_map.items()):
        raise ConfigError(f"{path}: principal_map must be a string -> string object")
    posture = data.get("rerun_posture", "required")
    if posture not in RERUN_POSTURES:
        raise ConfigError(f"{path}: rerun_posture must be one of {RERUN_POSTURES}, got {posture!r}")
    return MappingSpec(version=version, objects=objects,
                       principal_map={k.lower(): v.lower() for k, v in principal_map.items()},
                       rerun_posture=posture)

def _flag(data: dict, key: str, path: Path) -> bool:
    value = data.get(key, False)
    if not isinstance(value, bool):
        raise ConfigError(f"{path}: {key} must be a JSON boolean (true/false), got {value!r}")
    return value

def _bound(data: dict, key: str, default: float, path: Path) -> float:
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{path}: {key} must be a JSON number, got {value!r}")
    if not math.isfinite(value) or value < 0:
        raise ConfigError(f"{path}: {key} must be finite and >= 0, got {value!r}")
    return float(value)

def _count(data: dict, key: str, default: int, path: Path) -> int:
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigError(f"{path}: {key} must be a non-negative JSON integer, got {value!r}")
    return value

def _positive_count(data: dict, key: str, default: int, path: Path) -> int:
    value = _count(data, key, default, path)
    if value < 1:
        raise ConfigError(f"{path}: {key} must be a positive JSON integer, got {value!r}")
    return value

def load_tolerances(path: Path) -> Tolerances:
    return tolerances_from_dict(json.loads(path.read_text()), path)

def tolerances_from_dict(data: dict, path: Path) -> Tolerances:
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
        exact=_flag(data, "exact", path) if "exact" in data else True,
        per_type=_per_type(data, path),
        source_lag_s=_bound(data, "source_lag_s", 0.0, path),
        mirror_lag_s=_bound(data, "mirror_lag_s", 0.0, path),
        green_cycles=_positive_count(data, "green_cycles", 3, path),
        blocking_findings=_blocking_findings(data, path),
    )

def _per_type(data: dict, path: Path) -> dict:
    raw = data.get("per_type") or {}
    if not isinstance(raw, dict) or not all(
            isinstance(k, str) and isinstance(v, (int, float)) and not isinstance(v, bool)
            and math.isfinite(v) and v >= 0 for k, v in raw.items()):
        raise ConfigError(f"{path}: per_type must be an object of type name -> non-negative tolerance")
    per_type = {k.lower().split("(")[0].strip(): float(v) for k, v in raw.items()}
    if data.get("exact", True) and per_type:
        raise ConfigError(f"{path}: tolerances declare exact=true but carry per_type tolerances")
    return per_type

def _blocking_findings(data: dict, path: Path) -> tuple:
    raw = data.get("blocking_findings") or []
    if not isinstance(raw, list) or not all(isinstance(c, str) for c in raw):
        raise ConfigError(f"{path}: blocking_findings must be a list of finding classes")
    unknown = [c for c in raw if c not in FINDING_CLASSES]
    if unknown:
        raise ConfigError(f"{path}: blocking_findings names unknown classes {unknown}; "
                          f"classes are {list(FINDING_CLASSES)}")
    return tuple(raw)

def load_canon_rules(path: Path) -> list[CanonRule]:
    text = path.read_text()
    if not text.strip():
        raise ConfigError(f"{path}: empty canonicalization file; a file with no rules is {{\"rules\": []}}")
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{path}: canonicalization is not valid JSON ({exc})") from None
    rules = data if isinstance(data, list) else data.get("rules", []) if isinstance(data, dict) else None
    if not isinstance(rules, list) or not all(isinstance(r, dict) and isinstance(r.get("rule"), str) for r in rules):
        raise ConfigError(f"{path}: canonicalization must be {{\"rules\": [...]}} or a list of rules, each an "
                          "object with a string 'rule'")
    return [CanonRule(rule=r["rule"], applies_to=r.get("applies_to", "*"),
                      params=dict(r.get("params", {}))) for r in rules]

def load_json(path: Path, what: str):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"{what}: cannot read {path}: {exc}") from None

def load_policy(path: Path) -> dict:
    data = load_json(path, "policy")
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: policy must be an object")
    for key in ("source", "targets"):
        if not isinstance(data.get(key), dict):
            raise ConfigError(f"{path}: missing '{key}' object")
    source = data["source"]
    access = source.get("access")
    if not isinstance(access, str) or not access:
        raise ConfigError(f"{path}: source.access is required: a <catalog> or <catalog>.<schema> "
                          "reference the harness reads the source through")
    parts = access.split(".")
    if not 1 <= len(parts) <= 2:
        raise ConfigError(f"{path}: source.access must be <catalog> or <catalog>.<schema>, "
                          f"got {access!r}")
    for p in parts:
        validate_identifier(p)
    for key in ("family", "secret"):
        if not isinstance(source.get(key), str) or not source[key]:
            raise ConfigError(f"{path}: source.{key} is required")
    return data

def load_slices(path: Path) -> list:
    if not path.exists():
        return []
    data = load_json(path, "slices")
    slices = data.get("slices", data) if isinstance(data, dict) else data
    if not isinstance(slices, list):
        raise ConfigError(f"{path}: slices must be a list")
    return slices

def slice_entry(slices: list, unit: str) -> dict | None:
    for s in slices:
        if isinstance(s, dict) and (s.get("id") == unit or s.get("name") == unit):
            return s
    return None

def source_table_name(access: str, root_table: str) -> str:
    if len(root_table.split(".")) >= 3:
        return root_table
    parts = access.split(".")
    if len(parts) == 1:
        return f"{access}.{root_table}"
    return f"{access}.{root_table.rsplit('.', 1)[-1]}"

def load_slice_inputs(inputs: Path, params: dict | None = None):
    spec = load_mapping_spec(inputs / "mapping.json", params)
    grading_path = inputs / "grading.json"
    if grading_path.exists():
        data = load_json(grading_path, "grading input")
        if not isinstance(data, dict):
            raise ConfigError(f"{grading_path}: must be an object of object name -> grading")
        objects = []
        for c in spec.objects:
            g = data.get(c.object, c.grading)
            if g not in GRADINGS:
                raise ConfigError(f"{grading_path}: {c.object} grading must be one of "
                                  f"{GRADINGS}, got {g!r}")
            objects.append(replace(c, grading=g))
        spec = replace(spec, objects=objects)
    posture_path = inputs / "rerun_posture.json"
    if posture_path.exists():
        data = load_json(posture_path, "rerun_posture input")
        value = data.get("posture") if isinstance(data, dict) else data
        if value not in RERUN_POSTURES:
            raise ConfigError(f"{posture_path}: posture must be one of {RERUN_POSTURES}, "
                              f"got {value!r}")
        spec = replace(spec, rerun_posture=value)
    deps = None
    deps_path = inputs / "dependencies.json"
    if deps_path.exists():
        deps = load_json(deps_path, "dependencies input")
    rules = []
    canon_path = inputs / "canonicalization.json"
    if canon_path.exists():
        rules = load_canon_rules(canon_path)
    ops = None
    ops_path = inputs / "ops.json"
    if ops_path.exists():
        ops = load_json(ops_path, "ops input")
    snapshot = None
    snap_path = inputs / "snapshot_manifest.json"
    if snap_path.exists():
        snapshot = load_json(snap_path, "snapshot manifest")
    return spec, deps, rules, ops, snapshot
