"""Migration fan-out: one wave of unit-migration children, then one independent verifier,
and the wave result the wave ticket's worker hands back to the manager. Guarantees: no two
batches share a write target, at most `width` children at once, a circuit breaker on repeated
failure classes, this script is the single writer of the result, only verifier-PASS PRs merge
(auto_merge or a merge_overrides entry of the committed manifest). Each invocation launches one
run of one manifest through `run_workflow`; the run log refuses the same manifest bytes twice.
"""

import asyncio
import datetime
import fcntl
import hashlib
import hmac
import json
import re
import shlex
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

POINTER_REL = Path(".migration/waves/current.json")
HOOK_PROBE = re.compile(r"blocked:[0-9a-f]{8}|not-blocked|unknown")
DOCTOR_MAX_AGE = datetime.timedelta(minutes=15)


def find_pointer(start):
    """The sandbox gives this script one thing: its cwd (the session's home directory, not the workspace)."""
    for d in (start, *start.parents):
        if (d / POINTER_REL).is_file():
            return d / POINTER_REL
    raise SystemExit(f"no {POINTER_REL} at or above {start}; write the wave ticket's pointer, then re-run")


POINTER_PATH = find_pointer(Path.cwd().resolve())
try:
    POINTER = json.loads(POINTER_PATH.read_text())
except ValueError as e:
    raise SystemExit(f"{POINTER_PATH} is not valid JSON: {e}") from None
if isinstance(POINTER, dict) and ("mode" in POINTER or "run_id" in POINTER):
    raise SystemExit(
        f"{POINTER_PATH} no longer takes mode or run_id; a rerun is a new ticket: delete wave-<N>.result.json, "
        "edit the manifest's plumbing, re-sign it with the doctor and re-dispatch the ticket for its plan step; "
        "a changed plan is a new plan step"
    )
if not isinstance(POINTER, dict) or not isinstance(POINTER.get("manifest"), str):
    raise SystemExit(f"{POINTER_PATH} must be {{manifest: 'wave-N.json', hook_probe, workspace, plugin}}")
HOOK_PROBE_RESULT = POINTER.get("hook_probe")
if not isinstance(HOOK_PROBE_RESULT, str) or not HOOK_PROBE.fullmatch(HOOK_PROBE_RESULT):
    raise SystemExit(f"{POINTER_PATH} hook_probe must be blocked:<nonce>, not-blocked or unknown (the probe run in the "
                     "worker's shell; the doctor was given the same value)")
ROOT = Path(POINTER["workspace"]).resolve() if isinstance(POINTER.get("workspace"), str) else POINTER_PATH.parents[2]
PLUGIN = Path(POINTER["plugin"]).resolve() if isinstance(POINTER.get("plugin"), str) else None
if PLUGIN:
    sys.path.insert(0, str(PLUGIN / "skills" / "migration-fanout"))
try:  # the sandbox runs a copy of this script; the pointer's plugin root names its siblings
    import cards
    from decisions import UNSCOPED_OVERRIDE, merge_override_for, override_forgives, plan_sha
    from manifest import (BARE_PATH, MERGE_EVIDENCE_MODES, PR_URL, TAG_RE, _is_manifest, bounded_readers,
                          check_doctor_contract, check_pipeline_updates, check_repo_origin, check_wave_tag,
                          disjoint_slices, evidence_path, mapped_target, other_wave_manifests, reader_slices,
                          skill_file, target_key, transitive_writes, valid_namespace, validate_manifest,
                          wave_signature)
    from report import (CHILD_SCHEMA, CLOSE_SCHEMA, COST_KEYS, RESYNC_SCHEMA, VERIFY_SCHEMA, _verify_sink,
                        batch_verdicts, protected_files_violations, sum_cost, validate_close, validate_resync,
                        validate_verify)
except ImportError:
    raise SystemExit(f"{POINTER_PATH} names no plugin root with skills/migration-fanout/"
                     "{cards,decisions,manifest,report}.py and skills/target-routing/pipeline_updates.py; "
                     "nothing was written") from None
PIPELINE_UPDATES = (PLUGIN or ROOT) / "skills" / "target-routing" / "pipeline_updates.py"
WAVES_DIR = ROOT / ".migration" / "waves"
MANIFEST_PATH = (WAVES_DIR / POINTER["manifest"]).resolve()
if not (MANIFEST_PATH.name.startswith("wave-")
        and TAG_RE.fullmatch(MANIFEST_PATH.stem[len("wave-"):] or "")):
    raise SystemExit(f"{POINTER_PATH} manifest must be named wave-<N>.json or wave-<pipeline>-<N>.json "
                     "so every sibling wave and pipeline sees it in the collision check")
if (MANIFEST_PATH.suffix != ".json" or MANIFEST_PATH.parent != WAVES_DIR.resolve()
        or MANIFEST_PATH.name.endswith((".result.json", ".doctor.json"))):
    raise SystemExit(f"{POINTER_PATH} manifest must be the plain file name of a wave manifest inside {WAVES_DIR}")
if not MANIFEST_PATH.exists():
    raise SystemExit(f"no wave manifest at {MANIFEST_PATH}; the manager's plan step commits it, then re-run")
TAG = MANIFEST_PATH.stem[len("wave-"):]
MANIFEST_BYTES = MANIFEST_PATH.read_bytes()
MANIFEST = json.loads(MANIFEST_BYTES)
BASE_BRANCH = MANIFEST.get("base_branch", "")
MANIFEST_SHA = hashlib.sha256(MANIFEST_BYTES).hexdigest()[:12]
RESULT_PATH = MANIFEST_PATH.with_suffix(".result.json")
RUNS_PATH = MANIFEST_PATH.with_suffix(".runs.jsonl")
LOCK_PATH = MANIFEST_PATH.with_name(f".{MANIFEST_PATH.stem}.lock")
CARD_PATH = MANIFEST_PATH.with_suffix(".card.md")
DOCTOR_PATH = MANIFEST_PATH.with_suffix(".doctor.json")
SMOKE = MANIFEST.get("smoke") is True
if sys.argv[1:]:
    raise SystemExit("workflow.py takes no arguments; it runs through run_workflow")

def _tmp_write(path, text):
    """Write `text` to `path` via a sibling tmp file, so the real path is replaced atomically."""
    tmp = path.with_suffix("".join(path.suffixes) + ".tmp")
    tmp.write_text(text)
    tmp.replace(path)


async def smoke_main():
    await register_workflow({
        "name": f"smoke-wave-{TAG}",
        "description": "credential-free runner check: no child launches",
        "phases": [{"title": "smoke", "detail": "pointer, manifest and result writer only"}],
    })
    log(f"smoke wave {TAG}: launched nothing")
    _tmp_write(RESULT_PATH, json.dumps({
        "wave": 0,
        "tag": TAG,
        "smoke": True,
        "manifest_sha": MANIFEST_SHA,
        "width": 1,
        "hook_probe": HOOK_PROBE_RESULT,
        "closed": True,
        "batches": [],
        "verify": None,
        "auto_merge": False,
        "breaker_tripped_on": None,
        "brief": ["launched nothing; runner, pointer and result writer work"],
    }, indent=2, sort_keys=True) + "\n")


if RESULT_PATH.exists():
    try:
        prior = json.loads(RESULT_PATH.read_text())
        closed = prior.get("closed") if isinstance(prior, dict) else None
    except (OSError, ValueError):
        closed = "unreadable"
    raise SystemExit(
        f"{RESULT_PATH} exists: this wave already ran (closed={closed} when readable). "
        "A rerun is a new ticket: delete that file, edit the manifest's plumbing (the run log refuses the same "
        "bytes and a changed plan), re-sign it with the doctor and re-dispatch the ticket for its plan step"
    )

if SMOKE:
    if not (MANIFEST.get("wave") == 0 and MANIFEST.get("width") == 1 and MANIFEST.get("batches") == []):
        raise SystemExit("a smoke manifest exercises the runner only: {smoke: true, wave: 0, width: 1, batches: []}")
    asyncio.run(smoke_main())
    sys.exit(0)


def prompt_sha(prompt):
    return hashlib.sha256(prompt.encode()).hexdigest()[:16]


def skill_text(name):
    """(<abs path>, frontmatter-stripped body) of the plugin's skills/<name>/SKILL.md, embedded verbatim."""
    path = skill_file(PLUGIN, name)
    body = path.read_text()
    if body.startswith("---"):
        end = body.find("\n---", 3)
        if end != -1:
            body = body[end + len("\n---"):].lstrip("\n")
    return path, body


def signed_doctor_report(path, manifest_bytes, manifest_sha, now=None):
    try:
        report = json.loads(path.read_text())
    except OSError:
        raise SystemExit(f"no doctor record at {path}; run factory-doctor with --wave {path.with_suffix('.json').name} "
                         "in the worker's shell, then re-run") from None
    except ValueError as e:
        raise SystemExit(f"{path} is not valid JSON: {e}") from None
    if not isinstance(report, dict):
        raise SystemExit(f"{path} is not a doctor record")
    if report.get("manifest_sha") != manifest_sha:
        raise SystemExit(f"{path} was signed for another manifest (sha {report.get('manifest_sha')}); "
                         "re-run the doctor with --wave")
    try:
        signed_at = datetime.datetime.fromisoformat(str(report.get("signed_at")))
    except ValueError:
        raise SystemExit(f"{path} has no valid signed_at") from None
    if signed_at.tzinfo is None:
        raise SystemExit(f"{path} signed_at must carry a UTC offset")
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if not (datetime.timedelta(0) <= now - signed_at <= DOCTOR_MAX_AGE):
        raise SystemExit(f"{path} was signed at {report['signed_at']}, more than {DOCTOR_MAX_AGE} ago "
                         "(or in the future); re-run the doctor with --wave so the wave launches from a "
                         "current preflight")
    if not isinstance(report.get("signature"), str) or not hmac.compare_digest(
            report["signature"], wave_signature(report, manifest_bytes)):
        raise SystemExit(f"{path} signature does not verify: the record was edited after the doctor wrote it; "
                         "re-run the doctor")
    if report.get("hook_probe") != HOOK_PROBE_RESULT:
        raise SystemExit(f"{path} was signed for hook_probe {report.get('hook_probe')!r} but current.json "
                         f"says {HOOK_PROBE_RESULT!r}; give the doctor and the pointer the same probe result")
    return report


def _base_tip():
    git = ["git", "-C", str(ROOT)]
    subprocess.run(git + ["fetch", "-q", "origin", f"+refs/heads/{BASE_BRANCH}:refs/remotes/origin/{BASE_BRANCH}"],
                   check=True, capture_output=True, timeout=300)
    return subprocess.run(git + ["rev-parse", "--verify", f"origin/{BASE_BRANCH}^{{commit}}"],
                          check=True, capture_output=True, text=True, timeout=300).stdout.strip()


def wave_base():
    """The base branch's commit on origin, now."""
    try:
        return _base_tip()
    except (OSError, subprocess.SubprocessError) as e:
        raise SystemExit(f"cannot resolve origin/{BASE_BRANCH} in {ROOT} ({e}); the protected-files gate needs the base commit")


def evidence_in_pr(head, path, units):
    if not (isinstance(head, str) and isinstance(path, str)):
        return False
    parts = path.split("/")
    if (len(parts) < 4 or parts[:2] != [".migration", "recon"] or parts[2] not in units
            or any(p in ("", ".", "..") for p in parts[3:])):
        return False
    try:
        kind = subprocess.run(["git", "-C", str(ROOT), "cat-file", "-t", f"{head}:{path}"],
                              check=True, capture_output=True, text=True, timeout=300).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return False
    return kind == "blob"


def pr_head(pr_url):
    m = PR_URL.fullmatch(pr_url) if isinstance(pr_url, str) else None
    if not m or m["repo"].lower() != MANIFEST["repo"].lower():
        return None
    try:
        return fetch_ref(f"refs/pull/{m['n']}/head")
    except (OSError, subprocess.SubprocessError):
        return None


def fetch_ref(ref):
    local = f"refs/migration/wave-{TAG}/{ref}"
    git = ["git", "-C", str(ROOT)]
    subprocess.run(git + ["fetch", "-q", "origin", f"+{ref}:{local}"], check=True, capture_output=True, timeout=300)
    return subprocess.run(git + ["rev-parse", "--verify", f"{local}^{{commit}}"],
                          check=True, capture_output=True, text=True, timeout=300).stdout.strip()


def gate_outcomes(batch, reported, head):
    """The declared gates with what the child proved: a waived gate stands on the plan decision the
    manifest names; a pending or failed one needs a passed report whose evidence is in the PR."""
    declared = {g["id"]: {**g, "decision_id": g.get("decision_id")} for g in batch.get("gates", [])}
    unmet = []
    if reported is None:
        reported = []
    if not isinstance(reported, list) or not all(isinstance(r, dict) for r in reported):
        unmet.append("reported gates are not a list of {id, status, evidence} rows")
        reported = []
    seen = Counter(r.get("id") for r in reported)
    for r in reported:
        gid, g = r.get("id"), declared.get(r.get("id"))
        path = evidence_path(r.get("evidence"))
        if g is None:
            unmet.append(f"gate {gid!r} reported but not declared for {batch['id']}")
        elif (seen[gid] > 1 or set(r) - {"id", "status", "evidence"} or r.get("status") not in ("passed", "failed")
              or path is None or (r["status"] == "passed" and not path)):
            unmet.append(f"gate {gid} report must be one {{id, status: passed|failed, evidence}} row; evidence is "
                         "the bare path or {path, label?, verdict?, rows?}, non-empty when passed")
        elif g["status"] == "waived":
            unmet.append(f"gate {gid} is waived in the plan; a child cannot change it")
        elif r["status"] == "passed" and not BARE_PATH.fullmatch(path):
            unmet.append(f"gate {gid} evidence {path!r} is not a bare path: report {{path, label}} to annotate it")
        elif r["status"] == "passed" and not evidence_in_pr(head, path, batch["units"]):
            unmet.append(f"gate {gid} evidence {path!r} is not a file under .migration/recon/<unit>/ of "
                         f"{', '.join(batch['units'])} at the gated PR head")
        else:
            g.update(status=r["status"], evidence=path)
            if isinstance(r["evidence"], dict) and len(r["evidence"]) > 1:
                g["evidence_meta"] = {k: v for k, v in r["evidence"].items() if k != "path"}
    for g in declared.values():
        if g["status"] == "waived":
            continue
        if not seen[g["id"]]:
            unmet.append(f"gate {g['id']} ({g['kind']}) has no child result; the plan's {g['status']} is not proof")
        elif g["status"] != "passed":
            unmet.append(f"gate {g['id']} ({g['kind']}) is {g['status']}")
    return list(declared.values()), unmet


def run_log():
    runs = []
    if RUNS_PATH.exists():
        try:
            for n, line in enumerate(RUNS_PATH.read_text().splitlines(), 1):
                if not line.strip():
                    continue
                run = json.loads(line)
                if not (isinstance(run, dict) and all(isinstance(run.get(k), str) for k in RUN_RECORD)):
                    raise ValueError(f"line {n} is not a {{{', '.join(RUN_RECORD)}}} record")
                runs.append(run)
        except (OSError, ValueError) as e:
            raise SystemExit(f"{RUNS_PATH} cannot say which plan steps this wave's runs recorded ({e}); "
                             "inspect or restore it before running") from None
    return runs


RUN_RECORD = ("plan_step", "plan_sha", "manifest_sha", "started")
_RUN_LOCK = None


def record_run():
    """One run of a wave at a time, one run per manifest, one plan per plan step. The wave lock is
    held until this process exits, so a run that died holds nothing and a running wave cannot be
    launched twice, whatever its manifest bytes; the result is rechecked under it. The run log is read
    and appended under its own lock: the same bytes, concurrent or later, halt on the first record;
    a plumbing edit (brief, repo, secret name, estimates) is a new manifest the doctor signed and
    launches a new run; a manifest whose plan differs from what this plan step already ran (units,
    write targets, gates, width, source scope, overrides) halts: scope changes through a plan
    decision the human selects, which is a new plan step, never a rerun of this one."""
    global _RUN_LOCK
    _RUN_LOCK = LOCK_PATH.open("a")
    try:
        fcntl.flock(_RUN_LOCK, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit(f"another run of wave {TAG} holds {LOCK_PATH} (plan step {MANIFEST['plan_step']}): two runs "
                         "would launch the same batches twice; wait for it to write its result or halt it") from None
    if RESULT_PATH.exists():
        raise SystemExit(f"{RESULT_PATH} was written while this run was starting: this wave already ran")
    with RUNS_PATH.open("a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        runs = run_log()
        step, plan = MANIFEST["plan_step"], plan_sha(MANIFEST)
        if MANIFEST_SHA in {run["manifest_sha"] for run in runs}:
            raise SystemExit(
                f"{RUNS_PATH} records a run of this wave's manifest already (plan step {step}); "
                f"a rerun is a new ticket: delete wave-{TAG}.result.json, edit the manifest's plumbing, re-sign it "
                "with the doctor and re-dispatch the ticket for its plan step"
            )
        if any(run["plan_step"] == step and run["plan_sha"] != plan for run in runs):
            raise SystemExit(
                f"{RUNS_PATH} records a run of plan step {step} over a different plan: this manifest changes the "
                "units, write targets, gates, width, source scope or overrides that step ran, not just its plumbing. "
                "A plan change is a plan decision the human selects, so it runs as a new plan step of the approved "
                "plan, never as a rerun of this one"
            )
        f.write(json.dumps({"plan_step": step, "plan_sha": plan, "manifest_sha": MANIFEST_SHA,
                            "started": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")},
                           sort_keys=True) + "\n")
        f.flush()


def published_manifests():
    git = ["git", "-C", str(ROOT)]
    try:
        tip = _base_tip()
        names = subprocess.run(git + ["ls-tree", "--name-only", tip, ".migration/waves/"],
                               check=True, capture_output=True, text=True, timeout=300).stdout.split()
        return {Path(n).name: subprocess.run(git + ["show", f"{tip}:{n}"], check=True, capture_output=True, text=True,
                                             timeout=300).stdout
                for n in names if _is_manifest(Path(n).name)}
    except (OSError, subprocess.SubprocessError) as e:
        raise SystemExit(f"cannot read the manifests on origin/{BASE_BRANCH} ({e}); the planning barrier needs them")


def check_pipelines_published(waves_dir, m, published=None):
    on_disk = {f.name: f.read_text() for f in waves_dir.glob("wave-*.json") if _is_manifest(f.name)}
    if published is not None:
        published = {n: t for n, t in published.items() if _is_manifest(n)}
    names = on_disk if published is None else published
    pipelines = m.get("pipelines") or {}
    expected = {f"wave-{p}-{k}.json" for p, n in pipelines.items()
                if isinstance(n, int) and not isinstance(n, bool) for k in range(1, n + 1)}
    missing = sorted(expected - set(names))
    if missing:
        raise SystemExit(f"no manifest yet for {', '.join(missing)}: every pipeline in 'pipelines' commits its "
                         "wave-<pipeline>-<N>.json before any sibling launches; pull the integration branch "
                         "and re-run, or wait for the planning barrier")
    listed = "|".join(re.escape(p) for p in pipelines)
    extra = sorted(n for n in names if listed and re.fullmatch(rf"wave-(?:{listed})-\d+\.json", n)
                   and n not in expected)
    if extra:
        raise SystemExit(f"{', '.join(extra)} is beyond the wave count in 'pipelines': the plans disagree")
    if published is None:
        return
    for name in sorted(expected & set(published)):
        try:
            theirs = json.loads(published[name])
        except ValueError:
            theirs = None
        if not isinstance(theirs, dict) or theirs.get("pipelines") != pipelines:
            raise SystemExit(f"{name} declares a different 'pipelines': the plans disagree")
    for name, text in sorted(on_disk.items()):
        if name not in published:
            raise SystemExit(f"{name} is not on origin/{BASE_BRANCH}: commit and push every manifest before "
                             "preflight so "
                             "sibling pipelines see it")
        if text != published[name]:
            raise SystemExit(f"{name} differs from origin/{BASE_BRANCH}: commit and push the edit before preflight so "
                             "sibling pipelines check the same manifest")
    for name in sorted(set(published) - set(on_disk)):
        raise SystemExit(f"{name} is on origin/{BASE_BRANCH} but not on disk: pull the integration branch before "
                         "preflight so the collision check reads it")


def unit_mapping(unit):
    """The unit's recon mapping spec, None when the child has not written it yet."""
    p = ROOT / ".migration" / "units" / unit / "mapping_spec.json"
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text())
    except ValueError as e:
        raise SystemExit(f"{p} is not valid JSON ({e})") from None


def check_write_targets(batches, other_waves, mapping=None, namespace=""):
    """Two batches in one wave writing the same table means the lineage missed an edge: refuse to launch."""
    mapping = unit_mapping if mapping is None else mapping
    owners, spelled = {}, {}
    for b in batches:
        for t in b.get("write_targets", []):
            k = target_key(t, namespace)
            if k in owners:
                raise SystemExit(f"write-target collision before launch: '{t}' is claimed by "
                                 f"{owners[k]} and {b['id']}. Fix the wave plan, then re-run.")
            owners[k], spelled[k] = b["id"], t
    elsewhere, namespaces = {}, {None: namespace}
    for name, other in other_waves.items():
        if not (isinstance(other, dict) and isinstance(other.get("batches"), list) and "target_namespace" in other
                and (other["target_namespace"] == "" or valid_namespace(other["target_namespace"]))):
            raise SystemExit(f"{name} has no {{target_namespace, batches}} record; every wave manifest is read for "
                             "cross-wave write-target collisions, so fix it, then re-run")
        namespaces[name] = other["target_namespace"]
        for b in other["batches"]:
            for t in b["write_targets"]:
                elsewhere.setdefault(target_key(t, namespaces[name]), []).append((name, b))
    for k, mine in owners.items():
        if k not in elsewhere:
            continue
        t = spelled[k]
        batch = next(b for b in batches if b["id"] == mine)
        shared = ", ".join(f"{name} {b['id']} (units {', '.join(b['units'])})" for name, b in elsewhere[k])
        why = (f"shared write target '{t}' is written by {mine} in this wave and by {shared}; every mapping "
               "that reads it, in every wave, declares the object's scope_columns and a target_where pinning "
               "one of them to the unit's own partition or run date")
        readers = []
        for wave, b in ((None, batch), *elsewhere[k]):
            where = "" if wave is None else f" ({wave} {b['id']})"
            before = len(readers)
            for u in b["units"]:
                spec = mapping(u)
                path = f".migration/units/{u}/mapping_spec.json"
                if spec is None:
                    raise SystemExit(f"{why}: {path} is missing, so unit {u}{where} cannot be scoped")
                try:
                    problem = bounded_readers(spec, k, namespaces[wave])
                except SystemExit as e:
                    raise SystemExit(f"{why}: {path}: {e}") from None
                if problem is None:
                    continue
                if problem:
                    raise SystemExit(f"{why}: {path} (unit {u}{where}) {problem}")
                readers.append((f"unit {u}{where}", reader_slices(spec, k, namespaces[wave])))
            if len(readers) == before:
                paths = ", ".join(f".migration/units/{u}/mapping_spec.json" for u in b["units"])
                raise SystemExit(f"{why}: no unit of {b['id']}{where} reads '{t}': no object reading '{t}' in {paths}")
        for i, (a, slices_a) in enumerate(readers):
            for c, slices_c in readers[i + 1:]:
                if not disjoint_slices(slices_a, slices_c):
                    raise SystemExit(f"shared write target '{t}': the slices {a} and {c} recon may overlap; "
                                     "their target_where must pin a common scope column to values that cannot "
                                     "both hold. Fix the mapping specs, then re-run.")


def unit_dependencies(unit):
    p = ROOT / ".migration" / "units" / unit / "dependencies.json"
    if not p.is_file():
        return None
    try:
        data = json.loads(p.read_text())
    except ValueError as e:
        raise SystemExit(f"{p} is not valid JSON ({e})") from None
    rows = data.get("routines") if isinstance(data, dict) else None
    if not isinstance(rows, list) or not all(
            isinstance(r, dict) and isinstance(r.get("routine"), str) and r["routine"]
            and all(isinstance(r.get(k), list) and all(isinstance(t, str) and t for t in r[k])
                    for k in ("reads", "writes", "calls"))
            for r in rows):
        raise SystemExit(f"{p} needs a 'routines' list of {{routine, reads, writes, calls}} rows "
                         "(string name, lists of names)")
    names = Counter(r["routine"].casefold() for r in rows)
    if any(n > 1 for n in names.values()):
        raise SystemExit(f"{p} names a routine twice: {', '.join(sorted(k for k, n in names.items() if n > 1))}")
    return rows


def check_dependencies(batches, analysis=None, mapping=None, namespace=""):
    analysis = unit_dependencies if analysis is None else analysis
    mapping = unit_mapping if mapping is None else mapping
    for b in batches:
        by_unit = {u: analysis(u) for u in b["units"]}
        complete = all(rows is not None for rows in by_unit.values())
        if not b["write_targets"] and not complete:
            raise SystemExit(f"batch {b['id']} declares no write_targets, which only a dependency analysis for "
                             f"every unit ({', '.join(u for u, rows in by_unit.items() if rows is None)}) that "
                             "writes nothing can justify. Fix the wave plan, then re-run.")
        routines = [r for rows in by_unit.values() for r in rows or []]
        if not routines and not complete:
            continue
        where = f"batch {b['id']} (units {', '.join(b['units'])})"
        names = Counter(r["routine"].casefold() for r in routines)
        if any(n > 1 for n in names.values()):
            raise SystemExit(f"{where}: two units analyse the same routine: "
                             f"{', '.join(sorted(k for k, n in names.items() if n > 1))}")
        owner = {r["routine"].casefold(): u for u, rows in by_unit.items() for r in rows or []}
        specs = {u: mapping(u) for u, rows in by_unit.items() if rows}

        def resolve(r, t):
            u = owner[r["routine"].casefold()]
            if specs[u] is None:
                return {target_key(t, namespace)}
            targets = mapped_target(specs[u], t, namespace)
            if not targets:
                raise SystemExit(f"unit {u}'s routine {r['routine']} writes '{t}', which no object of its "
                                 "mapping_spec.json has as root_table, so its target is unknown")
            return targets

        try:
            actual = transitive_writes(routines, resolve)
        except SystemExit as e:
            raise SystemExit(f"{where}: {e}") from None
        deploy = {target_key(t, namespace) for t in b.get("deploy_objects", [])}
        written = sorted(deploy & actual)
        if written:
            raise SystemExit(f"batch {b['id']}: deploy_objects {written} are tables the call graph writes, not "
                             "deployed objects. Fix the wave plan, then re-run.")
        declared = {target_key(t, namespace) for t in b["write_targets"]} - deploy
        extra = sorted(declared - actual) if complete else []
        if actual - declared or extra:
            raise SystemExit(f"batch {b['id']}: declared write_targets differ from the call graph's transitive "
                             f"writes; missing from the declaration: {sorted(actual - declared) or '-'}; extra in "
                             f"the declaration: {extra or '-'}. Fix the wave plan, then re-run.")
        called = {c.casefold() for r in routines for c in r["calls"]}
        roots = sorted(r["routine"] for r in routines if r["routine"].casefold() not in called)
        free = set(deploy)
        prefix = target_key(namespace).split(".") if namespace else []

        def take(root, fits):
            segs = target_key(root).split(".")
            found = sorted(d for d in free if d.split(".")[:len(prefix)] == prefix
                           and fits(d.split(".")[len(prefix):], segs))
            if len(found) > 1:
                raise SystemExit(f"batch {b['id']}: analysed routine {root} is ambiguous: deploy_objects {found} could "
                                 "each be it. Spell the routine and its row alike, then re-run.")
            if found:
                free.discard(found[0])
            return bool(found)

        trailing = Counter(target_key(root).rsplit(".", 1)[-1] for root in roots)
        exact = {root for root in roots
                 if take(root, lambda d, s: d == s if prefix and len(s) > 1 else d[-len(s):] == s)}
        undeclared = [root for root in roots if root not in exact
                      and not take(root, lambda d, s: d[-1] == s[-1] and len(d) < len(s) and trailing[s[-1]] == 1)]
        if undeclared:
            raise SystemExit(f"batch {b['id']}: analysed routine(s) {undeclared} are entry points nothing in "
                             "the batch calls, so the unit deploys them, but deploy_objects has no object of that "
                             "name left for them (one row stands for one routine). Fix the wave plan, then re-run.")


def launch_checks():
    """The launch-time checks; returns the pipeline order."""
    try:
        origin = subprocess.run(["git", "-C", str(ROOT), "remote", "get-url", "origin"], check=True,
                                capture_output=True, text=True, timeout=60).stdout
    except (OSError, subprocess.SubprocessError):
        raise SystemExit(f"{ROOT} has no git remote 'origin'; children push PRs to it") from None
    check_repo_origin(MANIFEST["repo"], origin)
    check_pipelines_published(WAVES_DIR, MANIFEST, published_manifests() if "pipelines" in MANIFEST else None)
    check_write_targets(sorted(MANIFEST["batches"], key=lambda b: b["id"]),
                        other_wave_manifests(WAVES_DIR, MANIFEST_PATH.name),
                        namespace=MANIFEST.get("target_namespace", ""))
    check_dependencies(sorted(MANIFEST["batches"], key=lambda b: b["id"]),
                       namespace=MANIFEST.get("target_namespace", ""))
    return check_pipeline_updates(PIPELINE_UPDATES, MANIFEST_PATH)


validate_manifest(MANIFEST, PLUGIN)
check_wave_tag(TAG, MANIFEST)
BASE_SHA = wave_base()
DOCTOR = signed_doctor_report(DOCTOR_PATH, MANIFEST_BYTES, MANIFEST_SHA)
check_doctor_contract(MANIFEST, DOCTOR)


def _git_paths(*args):
    r = subprocess.run(["git", "-C", str(ROOT), "diff", "--name-only", "--no-renames", *args],
                       check=True, capture_output=True, text=True, timeout=300)
    return r.stdout.split()


def ref_changed_paths(ref):
    git = ["git", "-C", str(ROOT)]
    try:
        head = fetch_ref(ref)
        tip = _base_tip()
        merged = subprocess.run(git + ["merge-base", "--is-ancestor", head, tip],
                                check=False, capture_output=True, timeout=300).returncode
        if merged not in (0, 1):
            raise subprocess.SubprocessError(f"merge-base rc={merged}")
        return head, _git_paths(f"{BASE_SHA if merged == 0 else tip}...{head}")
    except (OSError, subprocess.SubprocessError):
        return None


def unit_recon(head, units):
    """Each unit's .migration/recon/<unit>/result.json at the PR head, read once: (merge_eligible, blocker
    classes). Eligibility is None unless the file says true or false; classes are `blocker_classes`, else
    the `class` of each `blockers` entry, else None (a harness that predates blocker classes cannot be
    matched against a scoped override)."""
    def read(u):
        try:
            result = json.loads(subprocess.run(
                ["git", "-C", str(ROOT), "show", f"{head}:.migration/recon/{u}/result.json"],
                check=True, capture_output=True, text=True, timeout=300).stdout)
        except (OSError, subprocess.SubprocessError, ValueError):
            return None, None
        if not isinstance(result, dict):
            return None, None
        eligible = result.get("merge_eligible")
        classes = result.get("blocker_classes")
        if classes is None and isinstance(result.get("blockers"), list):
            classes = [b.get("class") for b in result["blockers"] if isinstance(b, dict)]
        return (eligible if isinstance(eligible, bool) else None,
                sorted(set(classes)) if isinstance(classes, list) and all(isinstance(c, str) for c in classes)
                else None)
    return {u: read(u) for u in units}


def pr_changed_paths(pr_url):
    m = PR_URL.fullmatch(pr_url) if isinstance(pr_url, str) else None
    if not m or m["repo"].lower() != REPO.lower():
        return None
    return ref_changed_paths(f"refs/pull/{m['n']}/head")


def verifier_changed_paths(wave, passed):
    """What the verifier itself changed on recon/wave-N."""
    got = ref_changed_paths(f"recon/wave-{wave}")
    if got is None or not all(isinstance(p.get("pr_head"), str) for p in passed):
        return None
    head, paths = got
    dirs = {p["pr_head"]: [f".migration/recon/{u}/" for u in p["units"]] for p in passed}
    own = {p for p in paths if not p.startswith(tuple(d for ds in dirs.values() for d in ds))}
    try:
        for pr_head, ds in dirs.items():
            own.update(set(_git_paths(pr_head, head, "--", *ds)) & set(_git_paths(BASE_SHA, head, "--", *ds)))
    except (OSError, subprocess.SubprocessError):
        return None
    return sorted(own)


def _applies_to(start, delta, commit):
    git = ["git", "-C", str(ROOT)]
    with tempfile.TemporaryDirectory() as d:
        env = {"GIT_INDEX_FILE": str(Path(d) / "index")}
        subprocess.run(git + ["read-tree", start], check=True, env=env, capture_output=True,
                       text=True, timeout=300)
        ok = subprocess.run(git + ["apply", "--cached", "--whitespace=nowarn"], input=delta, env=env,
                            capture_output=True, text=True, timeout=300)
        if ok.returncode:
            return False
        tree = subprocess.run(git + ["write-tree"], check=True, env=env, capture_output=True,
                              text=True, timeout=300).stdout.strip()
    landed = subprocess.run(git + ["rev-parse", f"{commit}^{{tree}}"], check=True, capture_output=True,
                            text=True, timeout=300).stdout.strip()
    return tree == landed


def _same_change(parent, commit, head):
    git = ["git", "-C", str(ROOT)]
    base = subprocess.run(git + ["merge-base", head, commit],
                          check=True, capture_output=True, text=True, timeout=300).stdout.strip()
    n = int(subprocess.run(git + ["rev-list", "--count", f"{base}..{head}"],
                           check=True, capture_output=True, text=True, timeout=300).stdout)
    delta = subprocess.run(git + ["diff-tree", "-p", "--binary", "--full-index", "--no-color", base, head],
                           check=True, capture_output=True, text=True, timeout=300).stdout
    if not delta.strip():
        return False
    if _applies_to(parent, delta, commit):
        return True
    start = subprocess.run(git + ["rev-parse", "--verify", "--quiet", f"{commit}~{n}"],
                           check=False, capture_output=True, text=True, timeout=300)
    return n > 1 and start.returncode == 0 and _applies_to(start.stdout.strip(), delta, commit)


def proven_merged(to_merge, reported):
    proven, reasons = {}, {}
    try:
        tip = _base_tip()
    except (OSError, subprocess.SubprocessError) as e:
        return proven, {p["pr_url"]: f"cannot resolve origin/{BASE_BRANCH} ({e})" for p in to_merge}
    git = ["git", "-C", str(ROOT)]
    merges = None
    for p in to_merge:
        url, head = p["pr_url"], p.get("pr_head")
        if not (isinstance(head, str) and re.fullmatch(r"[0-9a-f]{40}", head)):
            reasons[url] = "no gated PR head"
            continue
        current = pr_head(url)
        if current != head:
            reasons[url] = ("PR head unreadable" if current is None
                            else f"PR head moved from {head} to {current} after verification")
            continue
        record = reported.get(url)
        try:
            if record is not None:
                mc = record.get("merge_commit_sha")
                if record.get("merged_head") != head:
                    reasons[url] = f"merged head {record.get('merged_head')} is not the gated head {head}"
                    continue
                on_base = (isinstance(mc, str) and re.fullmatch(r"[0-9a-f]{40}", mc)
                           and subprocess.run(git + ["merge-base", "--is-ancestor", mc, tip],
                                              check=False, capture_output=True,
                                              timeout=300).returncode == 0)
                if not on_base:
                    reasons[url] = f"merge commit {mc} is not on origin/{BASE_BRANCH}"
                    continue
                parents = subprocess.run(git + ["rev-list", "--parents", "-n1", mc],
                                         check=True, capture_output=True, text=True,
                                         timeout=300).stdout.split()[1:]
                if len(parents) >= 2 and parents[1] != head:
                    reasons[url] = f"merge commit's PR-side parent is {parents[1]}, not the gated head"
                    continue
                if len(parents) < 2 and not (parents and _same_change(parents[0], mc, head)):
                    reasons[url] = f"commit {mc} does not carry the gated head's change"
                    continue
                proven[url] = mc
                continue
            if merges is None:
                merges = {parts[2]: parts[0]
                          for line in subprocess.run(
                              git + ["rev-list", "--merges", "--first-parent", "--parents", f"{BASE_SHA}..{tip}"],
                              check=True, capture_output=True, text=True, timeout=300).stdout.splitlines()
                          for parts in [line.split()] if len(parts) >= 3}
            if head in merges:
                proven[url] = merges[head]
            else:
                reasons[url] = (f"merge not recorded by the wave-close step and no merge commit on "
                                f"origin/{BASE_BRANCH} has the gated head as its PR-side parent")
        except (OSError, subprocess.SubprocessError) as e:
            reasons[url] = f"merge proof failed ({e})"
    return proven, reasons


WAVE = MANIFEST["wave"]
REPO = MANIFEST["repo"]
BATCHES = sorted(MANIFEST["batches"], key=lambda b: b["id"])
WIDTH = int(MANIFEST.get("width", 20))
BREAKER = int(MANIFEST.get("breaker_threshold", 3))
AUTO_MERGE = bool(MANIFEST.get("auto_merge", False))
MAX_MINUTES = int(MANIFEST.get("max_minutes", 45))
CLOSE_MINUTES = int(MANIFEST.get("close_minutes", 10))
VERIFY_DEPTH = MANIFEST.get("verify_depth", "sampled")
RESYNC = MANIFEST.get("resync")


def batch_verify_depth(batch) -> str:
    return batch.get("verify_depth", VERIFY_DEPTH)


def batch_max_minutes(batch) -> int:
    return int(batch.get("max_minutes", MAX_MINUTES))


META = {
    "name": f"migration-wave-{TAG}",
    "description": f"Wave {WAVE}: {len(BATCHES)} unit batches in parallel, then one independent verifier",
    "phases": [
        {"title": "migrate", "detail": "one child per batch: convert, load, recon, open PR",
         "labels": [b["id"] for b in BATCHES],
         "soft_time_limit_minutes": max(batch_max_minutes(b) for b in BATCHES)},
        *([{"title": "resync", "detail": "parent-owned identity/sequence resync on the listed units' targets",
            "count": 1, "soft_time_limit_minutes": 30}] if RESYNC else []),
        {"title": "verify", "detail": "independent recon over the wave",
         "count": 1, "soft_time_limit_minutes": 60},
        {"title": "close", "detail": "review round over verifier-PASS PRs; merges them when auto_merge is on",
         "count": 1, "soft_time_limit_minutes": CLOSE_MINUTES},
    ],
}


def resync_prompt(cfg):
    return (
        f"You are the parent-owned identity resync step of wave {WAVE}. Repo: {REPO}. From the repo root, on the "
        f"base branch {BASE_BRANCH}, run exactly this command and nothing else: `{cfg['command']}`. It uses the "
        "target credentials by name (never print a value) and may execute only setval / identity-reseed statements "
        f"on the target objects of these units: {json.dumps(cfg['units'])}. The guard allowlist still applies; a "
        "block is a finding, not something to route around. Commit nothing and write no files; report `git status "
        "--porcelain` paths in changed_paths (it must be empty). For every sequence or identity column the command "
        "touched, report {object, before, after} in sequences; status failed if the command exited non-zero."
    )


def child_prompt(batch):
    gates = [g for g in batch.get("gates", []) if g["status"] != "waived"]
    skill_path, skill_body = skill_text(MANIFEST["child_skill"])
    return (
        f"You are one fan-out child in wave {WAVE}. Repo: {REPO}. Skill: {MANIFEST['child_skill']} ({skill_path}). "
        f"Its full text follows; follow it exactly. Batch {batch['id']}.\n\n{skill_body}\n\nBRIEF:\n{batch['brief']}\n\n"
        f"Units: {json.dumps(batch['units'])}. Write targets you own: {json.dumps(batch.get('write_targets', []))}.\n"
        "Unit handoffs: .migration/units/<unit_id>/mapping_spec.json.\n"
        + "\n" + capability_block(batch["units"])
        + f"Time budget: {batch_max_minutes(batch)} minutes; at the budget report status=BLOCKED with what landed.\n"
        "Declared gates (report each id in gates as passed with its evidence, a bare path or {path, label, verdict, "
        "rows}, never a path with a note appended, or failed; unreported fails; waived gates are plan decisions and "
        f"not listed; never rename or re-kind one): {json.dumps(gates, sort_keys=True)}\n"
        + "Recon: fixture-first, then the merge-evidence run; at most 3 full runs, never change a tolerance; after 3 "
        "failing runs report status=FAIL with a one-word failure_class (timestamp_precision, decimal_rounding, "
        "sequence_behind_source, missing_rule).\n"
        f"The harness is the merge authority: status=PASS needs a recon PASS in one of {list(MERGE_EVIDENCE_MODES)} "
        "(transactional for Lakebase/operational units) and merge_eligible=true in every unit's "
        ".migration/recon/<unit>/result.json. Fixture evidence is never PASS. If a unit is not eligible and exactly "
        "one committed merge_overrides entry covers your units, report merge_authority {kind: human_override, "
        "decision_id: <its decision>}; never edit the manifest.\n"
        "Open exactly one PR, first line PASS or FAIL. Do not merge it; wave close reviews.\n"
        "Edit nothing under .migration/ except .migration/recon/<unit_id>/; report every path the PR changes in "
        "changed_paths (`git diff --name-only <base>...<head>`); any other .migration/ path is FAIL "
        "protected_files_tampered.\n"
        "Report: skill_feedback one line per rule you had to derive; recon_cost = result.json['cost'] of the final "
        "merge-evidence run; parity and blocker_classes from that result.json when present; "
        "one_line_summary: what landed, or why not."
    )


def capability_block(units):
    caps, src = MANIFEST["capabilities"], MANIFEST.get("source") or {}
    flags = " ".join(f"--unit {u}" for u in units)
    if src:
        flags += " " + " ".join([f"--source-family {src['family']} --source-secret {src['secret']}"]
                                + [f"--param {shlex.quote(f'{k}={v}')}" for k, v in src.get("params", {}).items()])
    return (
        f"Capability contract: {json.dumps(caps, sort_keys=True)}\nBefore converting run "
        f"`factory-doctor --role child --reuse-record .migration/waves/wave-{TAG}.doctor.json "
        f"--expect-identity {shlex.quote(caps['identity'])} --expect-host {shlex.quote(caps['host'])} {flags}` "
        f"(its signed source rows are reused while fresher than doctor_max_age {MANIFEST.get('doctor_max_age', 15)} "
        "minutes and bound to this manifest and identity; otherwise the doctor runs in full). A fail row means "
        "status=BLOCKED naming the check id; a warn row goes in your summary and you continue. Never switch "
        "identity, run `databricks auth login`, or edit .migration/allowed_targets.json.\n"
    )


def verify_prompt(passed):
    by_id = {b["id"]: b for b in BATCHES}
    depths = {p["batch"]: batch_verify_depth(by_id[p["batch"]]) for p in passed}
    merge_line = ("Do not merge anything; return unit_verdicts as PASS or FAIL keyed by batch id (a unit id key is "
                  "normalised to its batch id, so one verdict per batch, whatever the batch size). The workflow's "
                  "wave-close step merges the PRs you mark PASS (or the result's brief lists them for the human "
                  "who merges when auto_merge is off).")
    skill_path, skill_body = skill_text(MANIFEST["verify_skill"])
    return (
        f"You are the independent verifier for wave {WAVE}. Repo: {REPO}. You did not write any of this code. "
        f"Skill: {MANIFEST['verify_skill']} ({skill_path}). Its full text follows; follow it exactly.\n\n"
        f"{skill_body}\n\nVerify these batches:\n{json.dumps(passed, sort_keys=True, indent=1)}\n\n"
        "Re-run the recon harness yourself. Do not trust the PR's pasted evidence, and run it with the "
        "tolerances and allowed_targets.json from the base branch, not the PR (a child that loosened a "
        "tolerance must fail here). For each PR run `git diff --name-only <base>...<head>`: any .migration/ "
        "path outside .migration/recon/<unit_id>/ is a FAIL for that unit with finding protected_files_tampered. "
        + ("This wave is declared DEGRADED (no live source read): run the harness with `--mode structural` "
           "for every unit (Tier 0 `structural_parity` only: keys, constraints, indexes, triggers, identity "
           "columns, grants, read from both catalogs, no row tier) and mark the unit PASS only when that run's "
           "result.json says verdict=PASS and its merge_block_reasons is exactly [\"mode\"]: a structural_gap "
           "or warnings entry means a catalog the harness could not read or a category it does not cover, "
           "which is unverified structure, so FAIL with finding structure_unverifiable; verdict=FAIL is FAIL "
           "with finding structural_drift. Its result.json is never merge_eligible and the mode reason alone is "
           "expected here. Do not re-run Tier 1-3 and do not lower or raise a "
           "depth: the child's snapshot row parity stands. "
           if MANIFEST.get("degraded") is True else
           f"Mark a unit PASS only if you re-ran the harness in one of {list(MERGE_EVIDENCE_MODES)} "
           "(the same mode the child used: transactional for Lakebase/operational units) and result.json "
           "says merge_eligible=true. "
           f"Run with `--depth <d>` per batch, exactly as listed here: {json.dumps(depths, sort_keys=True)} "
           "(sampled = Tier 1+2 plus a stratified Tier 3 with a seed different from the child's; full = keyed "
           "full diff). Never lower a batch's depth; raising it is allowed and noted in findings. ")
        + "A batch listed with merge_authority kind human_override was cleared by the named entry of the "
        f"committed manifest's merge_overrides {json.dumps(MANIFEST.get('merge_overrides', []), sort_keys=True)}: "
        "mark it PASS on a PASS verdict even if merge_eligible is false, and cite the decision id in findings. "
        "Each batch lists "
        "its acceptance gates with the evidence the child gave; open the evidence of every passed gate and FAIL "
        "the unit if it does not show what the gate's kind requires. "
        "Sum result.json['cost'] over your runs into recon_cost.\n"
        f"{merge_line}\nWrite the wave recon report to .migration/recon/wave-{TAG}/report.md, "
        f"commit it on branch recon/wave-{TAG}, push, and give '<branch>:<path>' in "
        "report_path. Do not edit any other file under .migration/; report your branch's "
        "`git diff --name-only <base>...<head>` in changed_paths. Each finding is one plain "
        "sentence a lead can read without opening anything."
    )


def close_prompt(to_merge, deadline_minutes, merge=True):
    review = ("First run one Devin Review round over these PRs and report each open finding as one line "
              "in review_findings; a finding is not a merge blocker unless a plan decision says so. ")
    tail = (f"Do it within {deadline_minutes} minutes; when time is up, stop and list the rest as unmerged. "
            "Write nothing: no commits, no files, no other PR; report `git diff --name-only` of anything you "
            "changed in changed_paths (it must be empty). The wave ticket's worker commits the wave result "
            "afterwards.")
    if not merge:
        return (f"You are the wave-close review step for wave {WAVE}. Repo: {REPO}. Do not merge anything: "
                "auto_merge is off and a human merges at wave close. Review exactly these PRs, nothing else: "
                f"{json.dumps(to_merge, sort_keys=True)}. " + review +
                "merged_prs must be []; list every PR in unmerged with the reason "
                "'auto_merge off; human merges at wave close'. " + tail)
    return (
        f"You are the wave-close step for wave {WAVE}. Repo: {REPO}. Merge exactly these PRs, nothing else: "
        f"{json.dumps(to_merge, sort_keys=True)}. Each was verified PASS by the independent verifier at "
        "pr_head; before merging, check the PR head still equals it and the PR is open and mergeable, "
        "otherwise leave it and list it in unmerged with a one-sentence reason. Merge nothing whose head "
        "is not exactly the verified pr_head, and never push to the PR branch. After each merge run "
        "`gh pr view <url> --json state,mergeCommit,headRefOid`: put {pr_url, merge_commit_sha: "
        "mergeCommit.oid, merged_head: headRefOid} in merged_prs only when state is MERGED — anything else "
        "goes to unmerged. " + review + tail
    )


class Breaker:
    def __init__(self, threshold):
        self.threshold = threshold
        self.classes = Counter()
        self.tripped_on = None

    def record(self, failure_class):
        if not failure_class:
            return
        self.classes[failure_class] += 1
        if self.classes[failure_class] >= self.threshold and not self.tripped_on:
            self.tripped_on = failure_class
            log(f"CIRCUIT BREAKER: {self.threshold} children failed with '{failure_class}'. "
                "No new children will launch this run.")


async def run_batch(batch, sem, breaker, waits=(), done=None):
    try:
        for event in waits:
            await event.wait()
        return await _run_batch(batch, sem, breaker)
    finally:
        if done is not None:
            done.set()


async def _run_batch(batch, sem, breaker):
    async with sem:
        if breaker.tripped_on:
            return {"status": "NOT_LAUNCHED", "recon_verdict": "NOT_RUN",
                    "one_line_summary": f"held back: breaker tripped on '{breaker.tripped_on}'"}
        log(f"launch {batch['id']} ({len(batch['units'])} units)")
        prompt = child_prompt(batch)
        try:
            out = await agent(prompt, phase="migrate", schema=CHILD_SCHEMA,
                              label=batch["id"], repos=[REPO],
                              soft_time_limit_minutes=batch_max_minutes(batch))
        except WorkflowAgentError as e:
            out = {"status": "FAIL", "recon_verdict": "NOT_RUN", "failure_class": "session_died",
                   "one_line_summary": f"child session died: {e}"}
        out["prompt_sha"] = prompt_sha(prompt)

        def downgrade(cls, why, drop=None):
            if out["status"] == "PASS":
                why = "PASS downgraded: " + why
            out["status"], out["failure_class"] = "FAIL", cls
            out["one_line_summary"] = why + "; " + out["one_line_summary"]
            if drop:
                out.pop(drop, None)
        if out["status"] == "PASS" and (out["recon_verdict"] != "PASS"
                                        or out.get("recon_mode") not in MERGE_EVIDENCE_MODES):
            downgrade("non_merge_evidence",
                      f"recon evidence was {out.get('recon_mode')}/{out.get('recon_verdict')}")
        if out["status"] == "PASS" and not (out.get("pr_url") and out.get("branch")):
            downgrade("missing_pr", "no PR URL/branch reported")
        reported = out.get("changed_paths")
        usable = isinstance(reported, list) and all(isinstance(p, str) for p in reported)
        gated = pr_changed_paths(out.get("pr_url"))
        observed = gated[1] if gated else None
        if gated:
            out["pr_head"] = gated[0]
        tampered = protected_files_violations(sorted({*(reported if usable else []), *(observed or [])}),
                                              batch["units"])
        if tampered:
            downgrade("protected_files_tampered", f"PR changed protected files ({', '.join(tampered)})")
        elif out["status"] == "PASS" and (not usable or observed is None):
            downgrade("protected_files_tampered",
                      "changed_paths "
                      + ("not reported" if not usable else
                          "not verifiable from git (not a PR of this repo, or its fetch or diff failed)")
                      + ", protected files unverified")
        if out["status"] == "PASS":
            claimed = out.get("merge_authority")
            recon = unit_recon(out["pr_head"], batch["units"])
            ineligible = sorted(u for u, (eligible, _) in recon.items() if eligible is not True)
            if out.get("merge_eligible") is True and not ineligible:
                out["merge_authority"] = {"kind": "harness", "decision_id": None}
            else:
                entry = merge_override_for(batch["units"], MANIFEST.get("merge_overrides"))
                scope = entry.get("blocker_classes") if entry else None
                uncovered = {u: c for u, (_, c) in recon.items() if c is None or not set(c) <= override_forgives(scope)}
                why = "; ".join(
                    f".migration/recon/{u}/result.json at the PR head "
                    + ("is missing or malformed" if recon[u][0] is None else f"has merge_eligible={recon[u][0]!r}")
                    for u in ineligible) or f"the child reported merge_eligible={out.get('merge_eligible')!r}"
                if entry is None:
                    downgrade("merge_authority",
                              f"recon evidence is not merge_eligible=true for every unit ({why}) and no single "
                              f"merge_overrides entry of the committed manifest covers {', '.join(batch['units'])}",
                              drop="merge_authority")
                elif not (isinstance(claimed, dict) and claimed.get("kind") == "human_override"
                          and claimed.get("decision_id") == entry["decision"]):
                    downgrade("merge_authority",
                              f"recon evidence is not merge_eligible=true for every unit ({why}); the manifest's "
                              f"merge_overrides entry {entry['decision']} covers the batch but the child claimed "
                              f"{claimed!r}: a PASS on an override names exactly that entry's decision",
                              drop="merge_authority")
                elif uncovered:
                    what = "; ".join(f"{u} has blockers {c if c is not None else 'unrecorded'}"
                                     for u, c in sorted(uncovered.items()))
                    downgrade("merge_authority",
                              f"recon evidence is not merge_eligible=true for every unit ({why}); "
                              f"merge_overrides entry {entry['decision']} covers blocker classes "
                              f"{scope if scope is not None else sorted(UNSCOPED_OVERRIDE) + ['(unscoped)']} "
                              f"only, but {what}",
                              drop="merge_authority")
                else:
                    out["merge_authority"] = {"kind": "human_override", "decision_id": entry["decision"]}
        if out["status"] == "PASS":
            out["gates"], unmet = gate_outcomes(batch, out.get("gates"), out["pr_head"])
            if unmet:
                downgrade("gates", "; ".join(unmet))
        if out["status"] != "PASS":
            breaker.record(out.get("failure_class") or "unclassified")
        log(f"done   {batch['id']}: {out['status']} / recon {out['recon_verdict']}: "
            f"{out['one_line_summary']}")
        return out


def cost_line(results, verify) -> str:
    est = MANIFEST.get("cost_estimate")
    actual = sum_cost([r.get("recon_cost") for r in results]
                      + ([verify.get("recon_cost")] if isinstance(verify, dict) else []))
    if est is None and all(actual[k] in (None, 0) for k in COST_KEYS):
        return "Cost: no estimate in the manifest and no recon_cost reported."
    def fmt(d):
        return ", ".join(f"{k}={d.get(k)}" for k in COST_KEYS if d.get(k) is not None) or "n/a"
    return (f"Cost: estimated {fmt(est or {})}; actual {fmt(actual)}, "
            f"harness time {round(actual['elapsed_s'])}s. Verifier depth {VERIFY_DEPTH}"
            + (", overrides: " + ", ".join(f"{b['id']}={b['verify_depth']}" for b in BATCHES if "verify_depth" in b)
               if any("verify_depth" in b for b in BATCHES) else "") + ".")


def waived_gates(results):
    return [{"batch": b["id"], "units": b["units"], "gate": g["id"], "decision_id": g["decision_id"]}
            for b, r in zip(BATCHES, results) if r["status"] == "PASS"
            for g in r.get("gates", []) if g["status"] == "waived"]


def merge_overrides(results):
    return [{"batch": b["id"], "units": b["units"], "decision_id": r["merge_authority"]["decision_id"]}
            for b, r in zip(BATCHES, results)
            if r["status"] == "PASS" and (r.get("merge_authority") or {}).get("kind") == "human_override"]


def brief_lines(results, verify, surprises, undeclared, unreported, auto_merge, close=None, to_merge=None, resync=None):
    """The wave brief a human reads in the result: what landed, what is held and why, what to merge."""
    by_status = {st: [b["id"] for b, r in zip(BATCHES, results) if r["status"] == st]
                 for st in ("PASS", "FAIL", "BLOCKED", "NOT_LAUNCHED")}
    lines = [f"Wave {WAVE} close, plan step {MANIFEST['plan_step']}.",
             f"Landed: {len(by_status['PASS'])} of {len(BATCHES)} batches passed their own recon.",
             f"Independent verify: {verify['wave_verdict'] if verify else 'NOT RUN'}.",
             f"Failed: {', '.join(by_status['FAIL']) or 'none'}. Blocked: {', '.join(by_status['BLOCKED']) or 'none'}. "
             f"Held by circuit breaker: {', '.join(by_status['NOT_LAUNCHED']) or 'none'}.",
             cost_line(results, verify)]
    if surprises:
        lines.append(f"Merges held: two children reported the same write target ({', '.join(surprises)}).")
    if undeclared:
        lines.append("Merges held: children wrote outside their declared targets: "
                     + "; ".join(f"{k}: {', '.join(v)}" for k, v in sorted(undeclared.items())) + ".")
    if unreported:
        lines.append(f"Merges held: {', '.join(unreported)} passed but reported no write targets.")
    waived = waived_gates(results)
    if waived:
        lines.append("Gates waived by plan decision: "
                     + "; ".join(f"{w['batch']}/{w['gate']} by {w['decision_id']}" for w in waived) + ".")
    overrides = merge_overrides(results)
    if overrides:
        lines.append("Human override authority (merge_eligible=false): "
                     + "; ".join(f"{o['batch']} ({', '.join(o['units'])}) by {o['decision_id']}"
                                for o in overrides) + ".")
    if resync:
        report = resync.get("report") if isinstance(resync.get("report"), dict) else {}
        lines += ["", f"Identity resync (`{resync['command']}` over {', '.join(resync['units'])}): "
                  f"{report.get('status', 'no report')}. {report.get('one_line_summary', '')}".rstrip()]
        rows = report.get("sequences") if isinstance(report.get("sequences"), list) else []
        lines += [f"- {r['object']}: {r['before']} -> {r['after']}"
                  for r in rows if isinstance(r, dict) and {"object", "before", "after"} <= set(r)]
        lines += [f"- problem: {p}" for p in resync.get("problems", [])]
        if resync.get("held_batches"):
            lines.append("- held from merge this run: " + ", ".join(resync["held_batches"]))
    if close is not None:
        if auto_merge:
            lines += ["", f"Wave close: {len(close.get('merged_prs', []))} of {len(to_merge or [])} verified "
                      f"PRs merged within {CLOSE_MINUTES} min."]
            lines += [f"Not merged: {u['pr_url']} ({u['reason']})" for u in close.get("unmerged", [])]
        else:
            lines += ["", f"Wave-close review over {len(to_merge or [])} verified PRs "
                      "(auto-merge off, nothing merged)."]
        lines += [f"- review: {f}" for f in (close.get("review_findings") or [])]
    if not auto_merge:
        urls = [r["pr_url"] for r in results if r["status"] == "PASS" and r.get("pr_url")]
        lines.append("Awaiting manual merge: " + (", ".join(urls) or "none reported"))
    else:
        held = set((resync or {}).get("held_batches") or [])
        urls = ([u["pr_url"] for u in close["unmerged"]] if isinstance(close, dict) else [])
        urls += [r["pr_url"] for b, r in zip(BATCHES, results)
                 if b["id"] in held and r["status"] == "PASS" and r.get("pr_url") and r["pr_url"] not in urls]
        if urls:
            lines.append("Awaiting manual merge: " + ", ".join(urls))
    findings = (verify or {}).get("findings") or []
    lines += ["", "Verifier findings:" if findings else "Verifier findings: none."] + [f"- {f}" for f in findings]
    feedback = sorted({s for r in results for s in r.get("skill_feedback", []) if isinstance(s, str)})
    lines += ["", "Skill feedback to fold in before the next wave:" if feedback else "Skill feedback: none."]
    lines += [f"- {s}" for s in feedback]
    lines += ["", "Per batch:"] + [f"- {b['id']}: {r['status']}. {r['one_line_summary']}"
                                   + (f" {r['pr_url']}" if r.get("pr_url") else "") for b, r in zip(BATCHES, results)]
    return lines


async def main():
    await register_workflow(META)
    order = ORDER
    log(f"wave {WAVE} (plan step {MANIFEST['plan_step']}, manifest {MANIFEST_SHA}): {len(BATCHES)} batches, "
        f"width {WIDTH}, breaker at {BREAKER}" + (f", serialized {order}" if order else ""))

    sem = asyncio.Semaphore(WIDTH)
    breaker = Breaker(BREAKER)
    done = {b["id"]: asyncio.Event() for b in BATCHES}
    results = await asyncio.gather(
        *(run_batch(b, sem, breaker, waits=[done[d] for d in order.get(b["id"], []) if d in done],
                    done=done[b["id"]]) for b in BATCHES))

    namespace = MANIFEST.get("target_namespace", "")
    reported = Counter(t for r in results for t in {target_key(t, namespace) for t in r.get("write_targets", [])})
    surprises = sorted(t for t, c in reported.items() if c > 1)
    undeclared = {}
    for b, r in zip(BATCHES, results):
        declared = {target_key(t, namespace) for t in b["write_targets"]}
        extra = sorted({t for t in r.get("write_targets", []) if target_key(t, namespace) not in declared})
        if extra:
            undeclared[b["id"]] = extra
    unreported = [b["id"] for b, r in zip(BATCHES, results)
                  if r["status"] == "PASS" and b["write_targets"] and not r.get("write_targets")]
    auto_merge = AUTO_MERGE
    halts = [(surprises, "WARNING", f"children reported overlapping write targets after the fact: {surprises}"),
             (undeclared, "HALT", f"children wrote outside their declared targets: {undeclared}"),
             (unreported, "HALT", f"PASS children did not report write targets: {unreported}")]
    for flag, level, what in halts:
        if flag:
            auto_merge = False
            log(f"{level}: {what}. Auto-merge is off for this wave; a human decides at wave close.")

    resync = None
    if RESYNC:
        log(f"resync: {RESYNC['command']} over {RESYNC['units']}")
        try:
            report = await agent(resync_prompt(RESYNC), phase="resync", schema=RESYNC_SCHEMA,
                                 label=f"resync-wave-{TAG}", repos=[REPO], soft_time_limit_minutes=30)
            problems = validate_resync(report)
        except WorkflowAgentError as e:
            report, problems = None, [f"resync session died: {e}"]
        held = [b["id"] for b in BATCHES if set(b["units"]) & set(RESYNC["units"])] if problems else []
        resync = {"command": RESYNC["command"], "units": RESYNC["units"], "report": report,
                  "problems": problems, "held_batches": held}
        for problem in problems:
            log(f"WARNING: {problem}")
        if held:
            log("HALT: identity resync did not complete cleanly (" + "; ".join(problems)
                + f"): {held} are held from merge this run.")

    passed = [{"batch": b["id"], "units": b["units"], "pr_url": r.get("pr_url", ""),
               "branch": r.get("branch", ""), "pr_head": r.get("pr_head"), "merge_authority": r.get("merge_authority"),
               "gates": r.get("gates", [])}
              for b, r in zip(BATCHES, results) if r["status"] == "PASS"]
    verify = None
    if passed:
        log(f"verify: {len(passed)} batches to an independent session")
        try:
            verify = await agent(verify_prompt(passed), phase="verify", schema=VERIFY_SCHEMA,
                                 label=f"verify-wave-{TAG}", repos=[REPO])
        except WorkflowAgentError as e:
            verify = {"wave_verdict": "FAIL", "unit_verdicts": {},
                      "findings": [f"verifier session died: {e}"]}
    else:
        log("verify: skipped, no batch passed")

    verify_problems = (validate_verify(verify, passed, TAG, verifier_changed_paths(TAG, passed))
                       if verify is not None else [])
    if verify_problems:
        verify = _verify_sink(verify, verify_problems, fail=True)
    to_merge = []
    if not verify_problems and isinstance(verify, dict) and isinstance(verify.get("unit_verdicts"), dict):
        verify["unit_verdicts"] = batch_verdicts(verify["unit_verdicts"], passed)
        held = set((resync or {}).get("held_batches") or [])
        to_merge = [{"batch": p["batch"], "units": p["units"], "pr_url": p["pr_url"], "pr_head": p.get("pr_head")}
                    for p in passed if (verify["unit_verdicts"].get(p["batch"]) == "PASS" and p.get("pr_url")
                                        and p["batch"] not in held)]
    close = None
    if to_merge:
        try:
            close = await asyncio.wait_for(
                agent(close_prompt(to_merge, CLOSE_MINUTES, merge=auto_merge), phase="close", schema=CLOSE_SCHEMA,
                      label=f"close-wave-{TAG}", repos=[REPO], soft_time_limit_minutes=CLOSE_MINUTES),
                timeout=CLOSE_MINUTES * 60)
        except (asyncio.TimeoutError, WorkflowAgentError) as e:
            close = {"merged_prs": [],
                     "unmerged": [{"pr_url": p["pr_url"],
                                   "reason": f"wave-close step did not finish within {CLOSE_MINUTES} minutes: {e}"}
                                  for p in to_merge],
                     "changed_paths": []}
    close_problems = validate_close(close, to_merge, merge=auto_merge) if close is not None else []
    if close is not None:
        raw = close
        reported = {}
        rows = raw.get("merged_prs") if isinstance(raw, dict) else None
        if isinstance(rows, list):
            reported.update({u["pr_url"]: u for u in rows
                             if isinstance(u, dict) and isinstance(u.get("pr_url"), str)})
        proven, proof = proven_merged(to_merge, reported)
        reasons = {u["pr_url"]: u["reason"] for u in raw.get("unmerged", [])
                   if isinstance(u, dict) and isinstance(u.get("pr_url"), str)
                   and isinstance(u.get("reason"), str)} if isinstance(raw, dict) else {}
        changed = raw.get("changed_paths") if isinstance(raw, dict) else None
        close = {"merged_prs": [p["pr_url"] for p in to_merge if p["pr_url"] in proven],
                 "merges": [{"pr_url": p["pr_url"], "merge_commit_sha": proven[p["pr_url"]]}
                            for p in to_merge if p["pr_url"] in proven],
                 "unmerged": [{"pr_url": p["pr_url"],
                               "reason": (proof.get(p["pr_url"]) or reasons.get(p["pr_url"])
                                          or "wave-close output invalid")}
                              for p in to_merge if p["pr_url"] not in proven],
                 "changed_paths": changed if isinstance(changed, list) else [],
                 "review_findings": [f for f in (raw.get("review_findings") if isinstance(raw, dict) else None) or []
                                     if isinstance(f, str)]}
        if close_problems:
            close["invalid"] = raw
    if close_problems:
        verify = _verify_sink(verify, [f"wave close invalid: {p}" for p in close_problems])
    closed = (breaker.tripped_on is None and not surprises and not undeclared and not unreported
              and not verify_problems and not close_problems and not (resync or {}).get("held_batches")
              and (close is None or not auto_merge or not close["unmerged"])
              and verify is not None and verify["wave_verdict"] == "PASS"
              and all(r["status"] == "PASS" for r in results))
    result = {
        "wave": WAVE, "tag": TAG, "plan_step": MANIFEST["plan_step"], "manifest_sha": MANIFEST_SHA,
        "plan_sha": plan_sha(MANIFEST), "width": WIDTH, "base_sha": BASE_SHA,
        "hook_probe": HOOK_PROBE_RESULT, "doctor_signed_at": DOCTOR.get("signed_at"),
        "breaker_tripped_on": breaker.tripped_on, "auto_merge": auto_merge,
        "closed": closed,
        "write_target_overlaps": surprises,
        "undeclared_write_targets": undeclared,
        "unreported_write_targets": unreported,
        "pipeline_order": order,
        "merge_overrides": merge_overrides(results),
        "waived_gates": waived_gates(results),
        "batches": [{"id": b["id"], **r} for b, r in zip(BATCHES, results)],
        "verify": verify, "resync": resync, "close": close, "close_minutes": CLOSE_MINUTES,
        "brief": brief_lines(results, verify, surprises, undeclared, unreported, auto_merge, close, to_merge, resync),
    }
    card = cards.wave_card(result)
    _tmp_write(RESULT_PATH, json.dumps(result, indent=2, sort_keys=True) + "\n")
    _tmp_write(CARD_PATH, card)
    log(f"wrote {RESULT_PATH} and {CARD_PATH}")
    log(f"wave {WAVE} verdict: {verify['wave_verdict'] if verify else 'NO PASSING BATCHES'}")


ORDER = launch_checks()
record_run()
asyncio.run(main())
