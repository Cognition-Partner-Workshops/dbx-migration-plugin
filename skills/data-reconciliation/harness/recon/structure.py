"""Structural parity: the non-row evidence (constraints, triggers, indexes, identity, grants)
shared by tier 0 (live/snapshot/fixture runs) and tier 7 (transactional), plus the fixture
dictionaries (--source-dictionary/--target-dictionary) a run can read structure from when the
live catalog is out of reach. What a reader cannot deliver is recorded per category
("unsupported"), never silently clean."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from .adapters import IdentityState, SchemaFacts
from .config import ConfigError
import dataclasses

from .tiers import Finding, TierResult

CATEGORIES = ("constraints", "triggers", "indexes", "sequences_identity", "grants")

# finding.check -> category, for the per-object structural diff
_CHECK_CATEGORY = {
    "primary_key_mismatch": "constraints",
    "primary_key_informational_only": "constraints",
    "primary_key_informational_missing": "constraints",
    "unique_missing": "constraints", "unique_extra": "constraints",
    "unique_nulls_equal_missing": "constraints", "unique_nulls_equal_extra": "constraints",
    "foreign_key_missing": "constraints", "foreign_key_extra": "constraints",
    "foreign_key_action_mismatch": "constraints",
    "foreign_key_informational_only": "constraints",
    "foreign_key_informational_missing": "constraints",
    "not_null_missing": "constraints", "not_null_extra": "constraints",
    "check_constraint_missing": "constraints", "check_constraint_extra": "constraints",
    "check_constraint_unverified": "constraints",
    "check_constraint_count_lower": "constraints", "check_constraint_count_higher": "constraints",
    "expression_unique_missing": "constraints", "expression_unique_extra": "constraints",
    "index_missing": "indexes",
    "sequence_missing": "sequences_identity", "sequence_behind_source": "sequences_identity",
    "sequence_direction_mismatch": "sequences_identity",
    "sequence_increment_mismatch": "sequences_identity",
    "identity_missing": "sequences_identity", "identity_extra": "sequences_identity",
    "trigger_missing": "triggers", "trigger_extra": "triggers",
    "trigger_granularity_mismatch": "triggers",
    "grant_missing": "grants", "grant_extra": "grants",
}


def mask_unsupported(facts: SchemaFacts, categories) -> SchemaFacts:
    """The same facts with the named categories emptied, so comparators can never grade a
    hole as an absence."""
    fields = {}
    if "constraints" in categories:
        fields.update(primary_key=(), primary_key_informational=(), unique=frozenset(), unique_nulls_equal=frozenset(),
                      foreign_keys=frozenset(), foreign_keys_informational=frozenset(),
                      foreign_key_actions={}, not_null=frozenset(), check_count=0,
                      checks=frozenset(), expression_unique=frozenset())
    if "indexes" in categories:
        fields.update(indexes=frozenset(), partial=frozenset(), expression_indexes=frozenset())
    if "sequences_identity" in categories:
        fields["identity_columns"] = frozenset()
    if "triggers" in categories:
        fields["triggers"] = {}
    if "grants" in categories:
        fields["grants"] = {}
    return dataclasses.replace(facts, **fields) if fields else facts


def structural_checks(pairs: list[tuple[SchemaFacts, SchemaFacts]]) -> dict[str, str]:
    """category -> "checked" when at least one object was read and no facts on either side
    marks it unsupported; "unsupported" is a hole, not a pass. Grants are "effective"
    when every reader expanded role membership, "direct_only" when any could only see
    direct grants (Unity Catalog)."""
    def label(cat: str) -> str:
        if not pairs or any(cat in f.unsupported for pair in pairs for f in pair):
            return "unsupported"
        if cat == "grants":
            return "effective" if all(f.grants_effective for pair in pairs for f in pair) \
                else "direct_only"
        return "checked"
    return {cat: label(cat) for cat in CATEGORIES}


def _trigger_cover(facts: SchemaFacts) -> Counter:
    cov = Counter()
    for timing, events, gran in facts.triggers.values():
        for ev in events:
            cov[(timing, ev, gran)] += 1
    return cov


def compare_triggers(obj: str, s: SchemaFacts, t: SchemaFacts) -> tuple[list[Finding], list[Finding]]:
    """(findings, extra): trigger names and bodies are not comparable across dialects, so parity
    is by shape count — a multiset of (timing, event, granularity). A source shape with fewer
    target triggers is trigger_missing; more is trigger_extra. When a (timing, event) pair is
    present on both sides with disjoint granularity sets it is trigger_granularity_mismatch
    instead — the shapes differ in kind, not count."""
    s_cov, t_cov = _trigger_cover(s), _trigger_cover(t)
    s_pairs = {(tm, ev) for tm, ev, _ in s_cov}
    t_pairs = {(tm, ev) for tm, ev, _ in t_cov}

    def _grans(cov: Counter, pair: tuple[str, str]) -> set[str]:
        return {g for (tm, ev, g) in cov if (tm, ev) == pair}

    def _disjoint(pair: tuple[str, str]) -> bool:
        return pair in s_pairs and pair in t_pairs and \
            _grans(s_cov, pair).isdisjoint(_grans(t_cov, pair))

    name_of = {(tm, ev): n for n, (tm, evs, _g) in sorted(s.triggers.items()) for ev in evs}
    findings, extra = [], []
    for (timing, ev, gran), n in sorted(s_cov.items()):
        if _disjoint((timing, ev)):
            continue
        have = t_cov.get((timing, ev, gran), 0)
        if n > have:
            findings.append(Finding(obj, "trigger_missing",
                                    f"{timing} {ev} {gran}: source {n}, target {have}"))
    for pair in sorted(s_pairs & t_pairs):
        if _grans(s_cov, pair).isdisjoint(_grans(t_cov, pair)):
            findings.append(Finding(obj, "trigger_granularity_mismatch",
                                    f"source trigger {name_of.get(pair, '?')} fires {pair[0]} "
                                    f"{pair[1]} per {'/'.join(sorted(_grans(s_cov, pair)))}, "
                                    f"target fires it per {'/'.join(sorted(_grans(t_cov, pair)))}"))
    for (timing, ev, gran), n in sorted(t_cov.items()):
        if _disjoint((timing, ev)):
            continue
        have = s_cov.get((timing, ev, gran), 0)
        if n > have:
            extra.append(Finding(obj, "trigger_extra",
                                 f"{timing} {ev} {gran}: target {n}, source {have}: writes the "
                                 "legacy app makes today behave differently"))
    return findings, extra


_PRIVILEGE_CAPABILITIES = {"modify": ("insert", "update", "delete")}


def _capabilities(privs) -> set[str]:
    out = set()
    for priv in privs:
        out.update(_PRIVILEGE_CAPABILITIES.get(priv.lower(), (priv.lower(),)))
    return out


def compare_grants(obj: str, s: SchemaFacts, t: SchemaFacts,
                   principal_map: dict[str, str]) -> list[Finding]:
    """Source grantee g is expected on the target as principal_map.get(g, g). Missing grantee or
    privileges -> grant_missing; a target grantee outside the mapped set, or extra privileges on
    a mapped one, -> grant_extra (a finding, not tightened: a wider target grant is a blast-radius
    defect, and the allowlist forbids grants beyond it)."""
    findings = []
    mapped: dict[str, list[str]] = {}
    for g in s.grants:
        mapped.setdefault(principal_map.get(g, g), []).append(g)
    for tg in sorted(mapped):
        names = ",".join(sorted(mapped[tg]))
        union = set().union(*(_capabilities(s.grants[g]) for g in mapped[tg]))
        if tg not in t.grants:
            findings.append(Finding(obj, "grant_missing",
                                    f"source grant {names} ({','.join(sorted(union))}) has no "
                                    f"target grant for {tg}"))
        elif missing := sorted(union - _capabilities(t.grants[tg])):
            findings.append(Finding(obj, "grant_missing",
                                    f"source grant {names} is missing {','.join(missing)} on "
                                    f"target grantee {tg}"))
    for tg in sorted(t.grants):
        if tg in mapped:
            union = set().union(*(_capabilities(s.grants[g]) for g in mapped[tg]))
            if extra := sorted(_capabilities(t.grants[tg]) - union):
                findings.append(Finding(obj, "grant_extra",
                                        f"target grant {tg} carries {','.join(extra)} the source "
                                        f"grant {','.join(sorted(mapped[tg]))} lacks"))
        else:
            findings.append(Finding(obj, "grant_extra",
                                    f"target grant {tg} ({','.join(sorted(t.grants[tg]))}) has no "
                                    "source counterpart"))
    return findings


def compare_identity_columns(obj: str, s: SchemaFacts, t: SchemaFacts,
                             colmap: dict[str, str]) -> tuple[list[Finding], list[Finding]]:
    """(findings, tightened): a mapped source identity column missing on the target is
    identity_missing; a target identity whose mapped source column is not identity is a
    tightened identity_extra."""
    inv = {v: k for k, v in colmap.items()}
    findings, tight = [], []
    for col in sorted(s.identity_columns):
        if col in colmap and colmap[col] not in t.identity_columns:
            findings.append(Finding(obj, "identity_missing",
                                    f"source identity {col} -> target {colmap[col]} is not identity"))
    for col in sorted(t.identity_columns):
        if col in inv and inv[col] not in s.identity_columns:
            tight.append(Finding(obj, "identity_extra",
                                 f"target {col} is identity but source {inv[col]} is not: "
                                 "target-generated keys diverge from the source's"))
    return findings, tight


def diff_by_object(findings: list[Finding]) -> dict[str, dict[str, list[str]]]:
    """object -> category -> [detail], the structural part of the verdict at a glance."""
    out: dict[str, dict[str, list[str]]] = {}
    for f in findings:
        cat = _CHECK_CATEGORY.get(f.check)
        if cat:
            out.setdefault(f.object, {}).setdefault(cat, []).append(f.detail)
    return out


@dataclass(frozen=True)
class FixtureDictionary:
    family: str
    path: str
    tables: dict[str, SchemaFacts]
    identity: dict[tuple[str, str], IdentityState]
    unsupported: frozenset[str]


def load_dictionary(path: Path) -> FixtureDictionary:
    """A fixture dictionary: {"family": ..., "tables": {name: <schema facts keys>}}."""
    try:
        data = json.loads(Path(path).read_text())
    except (OSError, ValueError) as e:
        raise ConfigError(f"{path}: cannot read dictionary: {e}") from None
    if not isinstance(data, dict) or not isinstance(data.get("tables"), dict):
        raise ConfigError(f"{path}: a fixture dictionary is an object with a `tables` map")
    unsupported = frozenset(str(c) for c in (data.get("unsupported") or []))
    # UC group membership is not expanded: databricks fixture facts are direct-only
    grants_effective = str(data.get("family") or "") != "databricks"
    tables: dict[str, SchemaFacts] = {}
    identity: dict[tuple[str, str], IdentityState] = {}
    for name, t in data["tables"].items():
        if not isinstance(t, dict):
            raise ConfigError(f"{path}: table {name}: facts must be an object")
        try:
            fks = {(tuple(fk["columns"]), fk["references"], tuple(fk["referenced_columns"]))
                   for fk in t.get("foreign_keys", [])}
            info_fks = {(tuple(fk["columns"]), fk["references"], tuple(fk["referenced_columns"]))
                        for fk in t.get("foreign_keys_informational", [])}
            fk_actions = {(cols, ref, rcols): (str(fk.get("on_update", "no action")),
                                             str(fk.get("on_delete", "no action")))
                          for fk in t.get("foreign_keys", [])
                          for cols, ref, rcols in [(tuple(fk["columns"]), fk["references"],
                                                    tuple(fk["referenced_columns"]))]}
            checks = set(t.get("checks") or [])
            for col, st in (t.get("identity_state") or {}).items():
                identity[(name, col)] = IdentityState(int(st["next"]), int(st.get("increment", 1)))
            tables[name] = SchemaFacts(
                table=name,
                primary_key=tuple(t.get("primary_key") or ()),
                primary_key_informational=tuple(t.get("primary_key_informational") or ()),
                unique={tuple(u) for u in (t.get("unique") or [])},
                unique_nulls_equal={tuple(u) for u in (t.get("unique_nulls_equal") or [])},
                foreign_keys=fks, foreign_key_actions=fk_actions,
                foreign_keys_informational=info_fks,
                not_null=set(t.get("not_null") or []),
                indexes={tuple(i) for i in (t.get("indexes") or [])},
                check_count=len(checks) if checks else int(t.get("check_count") or 0),
                checks=checks,
                identity_columns=set(t.get("identity_columns") or []),
                triggers={n: (str(v["timing"]), tuple(sorted(v["events"])),
                              str(v.get("granularity") or "row"))
                          for n, v in (t.get("triggers") or {}).items()},
                grants={str(g).lower(): frozenset(str(p).lower() for p in ps)
                        for g, ps in (t.get("grants") or {}).items()},
                unsupported=unsupported | frozenset(str(c) for c in (t.get("unsupported") or [])),
                grants_effective=grants_effective)
        except (KeyError, TypeError, ValueError) as e:
            raise ConfigError(f"{path}: table {name}: bad shape: {e}") from None
    return FixtureDictionary(family=str(data.get("family") or ""), path=str(path),
                             tables=tables, identity=identity, unsupported=unsupported)


class DictionaryOverlay:
    """Wraps a live adapter: schema_facts comes from the fixture dictionary, identity_state when
    the dictionary carries it, everything else delegates. The `dictionary_label` is how the tier
    records that structure was read from a fixture, not the live catalog."""

    def __init__(self, adapter, dictionary: FixtureDictionary):
        self._adapter, self._dictionary = adapter, dictionary
        self.dictionary_label = f"fixture:{dictionary.path}"

    def __getattr__(self, name):
        return getattr(self._adapter, name)

    def schema_facts(self, table) -> SchemaFacts:
        try:
            return self._dictionary.tables[table]
        except KeyError:
            raise NotImplementedError(
                f"{self._dictionary.path} has no facts for {table}") from None

    def identity_state(self, table, column) -> IdentityState | None:
        try:
            return self._dictionary.identity[(table, column)]
        except KeyError:
            raise NotImplementedError(
                f"{self._dictionary.path} has no identity state for {table}.{column}") from None


def tier0_structural_parity(spec, tol, source, target, catalog_only: bool = False) -> TierResult:
    """Structural parity outside a consistency window: the same grading as tier 7 but honest
    about holes — a catalog that cannot be read leaves categories unsupported, never clean.
    `catalog_only` (structural mode) skips the row-backed identity bounds."""
    from .transactional import schema_parity  # lazy: transactional imports this module
    return schema_parity(0, "structural_parity", spec, tol, source, target, strict=False,
                         catalog_only=catalog_only)
