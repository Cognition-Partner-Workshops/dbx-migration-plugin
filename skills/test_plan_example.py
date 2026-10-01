"""plan.example.yaml must stay a valid plan by the rules that are checkable without
devin-webapp's `migration_plan.py` (PyYAML only): unique ids, `depends_on` resolving to step
ids, `selected` null or an option id, blocker checks only the three machine kinds, no
`approved`, and `important` decisions undecided."""
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[1]
PLAN = yaml.safe_load((ROOT / "skills/migration-planning/references/plan.example.yaml").read_text())

CHECK_KINDS = {"repo", "secret", "mcp"}
ID_RE = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"


def _nodes():
    for phase in PLAN["phases"]:
        yield phase
        yield from phase.get("steps", [])


def _ids():
    for node in _nodes():
        yield node["id"]
        for group in ("decisions", "gates", "blockers"):
            for child in node.get(group, []):
                yield child["id"]


def _steps():
    return {step["id"] for phase in PLAN["phases"] for step in phase.get("steps", [])}


def test_every_id_is_a_unique_slug():
    ids = list(_ids())
    assert len(ids) == len(set(ids)), [i for i in ids if ids.count(i) > 1]
    import re
    assert all(re.fullmatch(ID_RE, i) for i in ids)


def test_depends_on_resolves_to_step_ids():
    for step in (s for phase in PLAN["phases"] for s in phase.get("steps", [])):
        for dep in step.get("depends_on", []):
            assert dep in _steps(), f"{step['id']} depends on unknown step {dep!r}"


def test_selected_is_null_or_an_option_id_and_important_stays_undecided():
    for node in _nodes():
        for d in node.get("decisions", []):
            options = {o["id"] for o in d.get("options", [])}
            assert d.get("selected") is None or d["selected"] in options
            if d.get("important"):
                assert d["selected"] is None, f"{d['id']} is important but already decided"


def test_blocker_checks_are_only_the_machine_kinds():
    for node in _nodes():
        for b in node.get("blockers", []):
            check = b.get("check")
            assert check is None or check.get("kind") in CHECK_KINDS
            if check:
                field = {"repo": "repo", "secret": "secret", "mcp": "server"}[check["kind"]]
                assert isinstance(check.get(field), str) and check[field]


def test_nothing_writes_approved():
    def walk(node):
        if isinstance(node, dict):
            assert "approved" not in node
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)
    walk(PLAN)
