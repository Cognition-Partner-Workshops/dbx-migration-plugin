"""Tests for tools/slices.py --check rules (design-0.7 section 6)."""
import pytest

from tools import slices as sl


def mk_slice(sid, **kw):
    s = {"id": sid, "name": sid, "track": "delta", "tables": [], "routines": [],
         "jobs": [], "consumers": [], "depends_on": [], "routing": "none"}
    s.update(kw)
    return s


def mk_doc(*extra):
    """A valid slices doc: slice-0, canary slice-1, then `extra` slices."""
    doc = {"version": 1, "slices": [
        mk_slice("slice-0"),
        mk_slice("slice-1", canary=True, auto_merge=False,
                 depends_on=["slice-0"], routing="api"),
        *extra]}
    return doc


def mk_deps(routines=None, jobs=None):
    return {"version": 1, "routines": routines or {}, "jobs": jobs or {}}


def codes(fails):
    return {c for c, _ in fails}


# --- schema -----------------------------------------------------------------

def test_valid_base_doc():
    assert sl.check(mk_doc()) == []


def test_bad_version_and_empty():
    assert "schema" in codes(sl.check({"version": 2, "slices": []}))
    assert "schema" in codes(sl.check({"version": 1, "slices": []}))
    assert "schema" in codes(sl.check("nope"))


def test_slice0_must_be_first():
    doc = mk_doc()
    doc["slices"][0], doc["slices"][1] = doc["slices"][1], doc["slices"][0]
    assert "slice0" in codes(sl.check(doc))


def test_dup_and_bad_fields():
    doc = mk_doc(mk_slice("slice-1", track="tape", routing="pigeon",
                          tables="not-a-list"))
    fails = codes(sl.check(doc))
    assert {"dup_slice", "schema"} <= fails


@pytest.mark.parametrize("sid", ["../escape", "a/b", "bad id", "..", ".hidden"])
def test_slice_id_must_be_path_safe(sid):
    doc = mk_doc(mk_slice(sid, depends_on=["slice-1"]))
    assert "schema" in codes(sl.check(doc))


# --- canary / auto_merge ----------------------------------------------------

def test_canary_not_first():
    doc = mk_doc(mk_slice("slice-2", canary=True, auto_merge=False,
                          depends_on=["slice-1"]))
    # two canaries and misplacement: the slice-1 base canary is still first
    doc["slices"][1]["canary"] = False
    doc["slices"][1]["auto_merge"] = True
    assert "canary" in codes(sl.check(doc))


def test_zero_and_two_canaries():
    doc = mk_doc()
    doc["slices"][1]["canary"] = False
    assert "canary" in codes(sl.check(doc))
    doc2 = mk_doc(mk_slice("slice-2", canary=True, auto_merge=False,
                           depends_on=["slice-1"]))
    assert "canary" in codes(sl.check(doc2))


def test_auto_merge_must_match_canary():
    doc = mk_doc()
    doc["slices"][1]["auto_merge"] = True  # canary with auto_merge
    assert "auto_merge" in codes(sl.check(doc))
    doc2 = mk_doc(mk_slice("slice-2", auto_merge=False, depends_on=["slice-1"]))
    assert "auto_merge" in codes(sl.check(doc2))


# --- depends_on -------------------------------------------------------------

def test_dependency_cycle():
    doc = mk_doc(mk_slice("slice-2", depends_on=["slice-3"]),
                 mk_slice("slice-3", depends_on=["slice-2"]))
    assert "dependency_cycle" in codes(sl.check(doc))


def test_unknown_dependency():
    doc = mk_doc(mk_slice("slice-2", depends_on=["slice-9"]))
    assert "unknown_dependency" in codes(sl.check(doc))


# --- consumers / route_group ------------------------------------------------

def test_consumer_route_split():
    doc = mk_doc(
        mk_slice("slice-2", routing="jdbc_url", consumers=["app"],
                 depends_on=["slice-1"]),
        mk_slice("slice-3", routing="jdbc_url", consumers=["app"],
                 depends_on=["slice-1"]))
    assert "consumer_route_split" in codes(sl.check(doc))


def test_consumer_same_route_group_ok():
    doc = mk_doc(
        mk_slice("slice-2", routing="jdbc_url", consumers=["app"],
                 route_group="g", depends_on=["slice-1"]),
        mk_slice("slice-3", routing="synonym", consumers=["app"],
                 route_group="g", depends_on=["slice-1"]))
    assert "consumer_route_split" not in codes(sl.check(doc))


def test_consumer_independent_routing_ok():
    doc = mk_doc(
        mk_slice("slice-2", routing="api", consumers=["app"],
                 depends_on=["slice-1"]),
        mk_slice("slice-3", routing="jdbc_url", consumers=["app"],
                 depends_on=["slice-1"]))
    assert "consumer_route_split" not in codes(sl.check(doc))


# --- shared_writes ----------------------------------------------------------

def test_shared_table_needs_disposition():
    doc = mk_doc(
        mk_slice("slice-2", tables=["A.T1"], depends_on=["slice-1"]),
        mk_slice("slice-3", tables=["A.T1"], depends_on=["slice-1"]))
    assert "shared_writes" in codes(sl.check(doc))


@pytest.mark.parametrize("disp", ["merged:slice-2", "dual_write:slice-2"])
def test_shared_table_with_disposition_ok(disp):
    doc = mk_doc(
        mk_slice("slice-2", tables=["A.T1"], depends_on=["slice-1"]),
        mk_slice("slice-3", tables=["A.T1"], depends_on=["slice-1"],
                 shared_writes={"A.T1": disp}))
    assert "shared_writes" not in codes(sl.check(doc))


def test_shared_writes_owner_must_hold_table():
    doc = mk_doc(
        mk_slice("slice-2", tables=["A.T1"], depends_on=["slice-1"]),
        mk_slice("slice-3", tables=["A.T1"], depends_on=["slice-1"],
                 shared_writes={"A.T1": "merged:slice-9"}))
    assert "shared_writes" in codes(sl.check(doc))


def test_shared_writes_declared_by_non_holder():
    doc = mk_doc(
        mk_slice("slice-2", tables=["A.T1"], depends_on=["slice-1"]),
        mk_slice("slice-3", tables=["A.T1"], depends_on=["slice-1"]),
        mk_slice("slice-4", depends_on=["slice-1"],
                 shared_writes={"A.T1": "merged:slice-2"}))
    assert "shared_writes" in codes(sl.check(doc))


def test_shared_writes_bad_format():
    doc = mk_doc(
        mk_slice("slice-2", tables=["A.T1"], depends_on=["slice-1"]),
        mk_slice("slice-3", tables=["A.T1"], depends_on=["slice-1"],
                 shared_writes={"A.T1": "eventually"}))
    assert "shared_writes" in codes(sl.check(doc))


# --- dependency scan rules --------------------------------------------------

def deps_for(mapping):
    return mk_deps(routines=mapping)


def test_cross_slice_write():
    doc = mk_doc(
        mk_slice("slice-2", tables=["A.T1"], routing="synonym",
                 route_group="g", depends_on=["slice-1"]),
        mk_slice("slice-3", routines=["PKG.R1"], routing="synonym",
                 route_group="g", depends_on=["slice-1"]))
    deps = deps_for({"PKG.R1": {"writes": ["A.T1"], "reads": []}})
    assert "cross_slice_write" in codes(sl.check(doc, deps=deps))


def test_write_to_slice0_ok():
    doc = mk_doc(mk_slice("slice-2", routines=["PKG.R1"], depends_on=["slice-1"]))
    doc["slices"][0]["tables"] = ["A.SHARED"]
    deps = deps_for({"PKG.R1": {"writes": ["A.SHARED"], "reads": []}})
    assert "cross_slice_write" not in codes(sl.check(doc, deps=deps))


def test_unscanned_routine():
    doc = mk_doc(mk_slice("slice-2", routines=["PKG.MISSING.*"],
                          depends_on=["slice-1"]))
    assert "unscanned_routine" in codes(sl.check(doc, deps=deps_for({})))


def test_cross_route_read():
    doc = mk_doc(
        mk_slice("slice-2", tables=["A.T1"], routing="jdbc_url",
                 route_group="g1", depends_on=["slice-1"]),
        mk_slice("slice-3", routines=["PKG.R1"], routing="jdbc_url",
                 route_group="g2", depends_on=["slice-1"]))
    deps = deps_for({"PKG.R1": {"writes": [], "reads": ["A.T1"]}})
    assert "cross_route_read" in codes(sl.check(doc, deps=deps))


def test_cross_route_read_same_group_ok():
    doc = mk_doc(
        mk_slice("slice-2", tables=["A.T1"], routing="jdbc_url",
                 route_group="g", depends_on=["slice-1"]),
        mk_slice("slice-3", routines=["PKG.R1"], routing="jdbc_url",
                 route_group="g", depends_on=["slice-1"]))
    deps = deps_for({"PKG.R1": {"writes": [], "reads": ["A.T1"]}})
    assert "cross_route_read" not in codes(sl.check(doc, deps=deps))


def test_slice0_table_readable_by_all():
    doc = mk_doc(mk_slice("slice-2", routines=["PKG.R1"],
                          depends_on=["slice-1"]))
    doc["slices"][0]["tables"] = ["A.SHARED"]
    deps = deps_for({"PKG.R1": {"writes": [], "reads": ["A.SHARED"]}})
    assert "cross_route_read" not in codes(sl.check(doc, deps=deps))


def test_job_same_name_does_not_hide_routine():
    doc = mk_doc(mk_slice("slice-2", routines=["PKG.R1"], jobs=["PKG.R1"],
                          tables=["A.OWN"], depends_on=["slice-1"]))
    deps = mk_deps(routines={"PKG.R1": {"writes": ["A.OTHER"], "reads": []}},
                   jobs={"PKG.R1": {"writes": ["A.OWN"], "reads": []}})
    fails = codes(sl.check(doc, deps=deps))
    assert "cross_slice_write" in fails
    assert "unscanned_routine" not in fails


def test_glob_routine_expansion():
    doc = mk_doc(mk_slice("slice-2", routines=["PKG.*"], depends_on=["slice-1"]))
    deps = deps_for({"PKG.R1": {"writes": ["A.T1"], "reads": []},
                     "PKG.R2": {"writes": [], "reads": []}})
    fails = sl.check(doc, deps=deps)
    assert "cross_slice_write" in codes(fails)
    assert "unscanned_routine" not in codes(fails)


# --- census -----------------------------------------------------------------

def test_unassigned_census():
    doc = mk_doc(mk_slice("slice-2", tables=["A.T1"], depends_on=["slice-1"]))
    census = {"version": 1, "objects": [{"name": "A.T1"}, {"name": "A.T2"}]}
    assert "unassigned_census" in codes(sl.check(doc, census=census))


def test_census_excluded_with_decision_ok():
    doc = mk_doc(mk_slice("slice-2", tables=["A.T1"], depends_on=["slice-1"],
                          excluded=[{"object": "A.T2", "decision": "d-x"}]))
    census = {"version": 1, "objects": [{"name": "A.T1"}, {"name": "A.T2"}]}
    assert sl.check(doc, census=census) == []


def test_census_excluded_without_decision_fails():
    doc = mk_doc(mk_slice("slice-2", tables=["A.T1"], depends_on=["slice-1"],
                          excluded=[{"object": "A.T2"}]))
    census = {"version": 1, "objects": [{"name": "A.T2"}]}
    fails = codes(sl.check(doc, census=census))
    assert "excluded_decision" in fails or "unassigned_census" in fails


def test_census_glob_assignment():
    doc = mk_doc(mk_slice("slice-2", routines=["PKG.*"], depends_on=["slice-1"]))
    census = {"version": 1, "objects": [{"name": "PKG.R9", "kind": "routine"}]}
    assert "unassigned_census" not in codes(sl.check(doc, census=census))
