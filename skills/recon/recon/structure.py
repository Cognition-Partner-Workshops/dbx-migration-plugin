
from __future__ import annotations

from collections import Counter

import dataclasses

from .adapters import SchemaFacts

from .tiers import Finding, TierResult

CATEGORIES = ("constraints", "triggers", "indexes", "sequences_identity", "grants")

_CHECK_CATEGORY = {
    "primary_key_mismatch": "constraints",
    "primary_key_informational_only": "constraints",
    "primary_key_informational_missing": "constraints",
    "primary_key_not_applicable": "constraints",
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
    "identity_not_applicable": "sequences_identity",
    "trigger_missing": "triggers", "trigger_extra": "triggers",
    "trigger_granularity_mismatch": "triggers",
    "grant_missing": "grants", "grant_extra": "grants",
}

def mask_unsupported(facts: SchemaFacts, categories) -> SchemaFacts:
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
                   principal_map: dict[str, str],
                   own_principals: frozenset[str] = frozenset()) -> list[Finding]:
    findings = []
    own = {p.lower() for p in own_principals}

    def extra(detail: str, grantee: str) -> Finding:
        return Finding(obj, "grant_extra", detail, severity="info" if grantee in own else "error")
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
            if more := sorted(_capabilities(t.grants[tg]) - union):
                findings.append(extra(f"target grant {tg} carries {','.join(more)} the source "
                                      f"grant {','.join(sorted(mapped[tg]))} lacks", tg))
        else:
            findings.append(extra(f"target grant {tg} ({','.join(sorted(t.grants[tg]))}) has no "
                                  "source counterpart", tg))
    return findings

def compare_identity_columns(obj: str, s: SchemaFacts, t: SchemaFacts,
                             colmap: dict[str, str]) -> tuple[list[Finding], list[Finding]]:
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
    out: dict[str, dict[str, list[str]]] = {}
    for f in findings:
        cat = _CHECK_CATEGORY.get(f.check)
        if cat:
            out.setdefault(f.object, {}).setdefault(cat, []).append(f.detail)
    return out

def tier0_structural_parity(spec, tol, source, target, catalog_only: bool = False,
                            own_principals: frozenset[str] = frozenset()) -> TierResult:
    from .transactional import schema_parity  # lazy: transactional imports this module
    return schema_parity(0, "structural_parity", spec, tol, source, target, strict=False,
                         catalog_only=catalog_only, own_principals=own_principals)
