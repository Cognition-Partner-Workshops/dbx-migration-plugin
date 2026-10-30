"""Slice fan-out orchestrator (design-0.7 section 7).

Reads `.migration/policy.json` and `.migration/slices.json`, then for every
slice whose `depends_on` are all merged launches one `migrate` child, a `verify`
child on PASS, and merges the slice PR at the verifier's `verified_head` when
`auto_merge` holds. Per-slice state lives in `.migration/recon/<slice>/fanout.json`
(manager-committed on base_branch); re-running is idempotent: merged slices are
skipped, open PRs are refreshed via `pr_state`, and everything else is
re-driven from its recorded state.

All external effects sit behind the `Ops` interface so tests run offline:

    launch(brief) -> report        run one child to its report
    pr_state(url) -> {"state": "open"|"merged"|"closed", "head": sha}
    merge(url, sha)                merge the PR only at that head
    changed_paths(url) -> [paths]  git diff --name-only <base>...<pr_head>

`GhOps` is the thin real adapter: `gh` CLI for PR state, diff and merge; the
Devin sessions API for `launch`.

Child report schema (structured output):
    {status: PASS|FAIL|BLOCKED, slice, pr_url, branch, parity: PASS|FAIL,
     findings: [{class, code, detail}], failure_class?, changed_paths: [...],
     recon_cost, one_line_summary}
A verify child's report additionally carries `verified_head` and `blocking`.
A non-PASS/FAIL/BLOCKED status is treated as "child still in flight" and is
picked up on the next run once its PR exists.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

BRIEF_LIMIT = 4000
RUNNING = {"launched", "verify_pending", "merge_ready", "awaiting_human"}
IN_FLIGHT = {"launched", "verify_pending", "merge_ready"}


def _norm(name):
    return str(name).strip().upper()


def _record_path(migration_dir, sid):
    return Path(migration_dir) / "recon" / sid / "fanout.json"


def _load_record(migration_dir, s):
    rec = {"slice": s["id"], "status": "new"}
    path = _record_path(migration_dir, s["id"])
    if path.exists():
        try:
            rec.update(json.loads(path.read_text()))
        except (OSError, json.JSONDecodeError):
            rec["note"] = "fanout.json unreadable; starting over"
    return rec


def _write_record(migration_dir, rec):
    path = _record_path(migration_dir, rec["slice"])
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(rec, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)


def _topo(slices):
    """Dependency-first order, stable by slices.json order. A cycle (already a
    --check refusal) is appended in declared order."""
    ids = {s["id"] for s in slices}
    done, order, pending = set(), [], list(slices)
    while pending:
        ready = [s for s in pending
                 if all(d in done or d not in ids for d in s.get("depends_on", []))]
        if not ready:
            return order + pending
        for s in ready:
            order.append(s)
            done.add(s["id"])
        pending = [s for s in pending if s not in ready]
    return order


def _allowed_slice_path(path, sid):
    return (path.startswith(f".migration/recon/{sid}/out/")
            or path.startswith(f".migration/authorized/{sid}/"))


def _protected_paths(paths, sid):
    return [p for p in paths
            if p.startswith(".migration/") and not _allowed_slice_path(p, sid)]


def _has_disposition(doc, table):
    for s in doc["slices"]:
        for t, disp in (s.get("shared_writes") or {}).items():
            if (_norm(t) == table and isinstance(disp, str)
                    and disp.partition(":")[0] in ("merged", "dual_write")):
                return True
    return False


def _collision(doc, s, running_ids):
    """Tables shared between `s` and a running slice with no shared_writes
    disposition. Cannot happen after slices.py --check; kept as defence."""
    mine = {_norm(t) for t in s.get("tables", [])}
    hits = set()
    for other in doc["slices"]:
        if other["id"] not in running_ids:
            continue
        for t in mine & {_norm(x) for x in other.get("tables", [])}:
            if not _has_disposition(doc, t):
                hits.add(t)
    return sorted(hits)


def _brief(kind, s, policy, pr_url=None):
    sid = s["id"]
    base = policy["base_branch"]
    if kind == "verify":
        text = (f"Verify {sid} of {policy['repo']} on base {base}: run the verify skill "
                f"against PR {pr_url}. Check out the PR head with policy.json, slices.json "
                f"and .migration/recon/{sid}/inputs/ taken from {base}; re-run dbx-recon "
                f"at the slice's verify_depth with a fresh seed; return parity, findings, "
                f"blocking and verified_head per the child report schema.")
    else:
        text = (f"Migrate {sid} ({s.get('name', '')}) of {policy['repo']} on base {base}: "
                f"run the migrate skill per .migration/slices.json[{sid}] and "
                f".migration/policy.json. Under .migration/ write only "
                f".migration/recon/{sid}/out/ and .migration/authorized/{sid}/; open one "
                f"PR against {base} and report the child report schema.")
    return text[:BRIEF_LIMIT]


def run(ops, policy, doc, *, migration_dir=".migration", width=None, breaker=None,
        clock=time.time):
    """One fan-out pass. Returns a summary dict; records persist per slice."""
    slices = doc.get("slices", [])
    width = int(width if width is not None else policy.get("width", 4))
    breaker = int(breaker if breaker is not None else policy.get("breaker", 3))
    order = _topo(slices)
    by_id = {s["id"]: s for s in slices}
    records = {s["id"]: _load_record(migration_dir, s) for s in order}
    failures = Counter()
    events = []

    def save(rec):
        rec["updated_at"] = int(clock())
        _write_record(migration_dir, rec)

    def fail(rec, cls, note):
        rec.update(status="failed", failure_class=cls, note=note)
        failures[cls] += 1
        events.append(f"{rec['slice']}: failed {cls} ({note})")
        save(rec)

    def tripped():
        return any(count >= breaker for count in failures.values())

    def merged(sid):
        return sid in records and records[sid]["status"] == "merged"

    def in_flight():
        return sum(1 for r in records.values() if r["status"] in IN_FLIGHT)

    def try_merge(s, rec):
        sid = s["id"]
        if not s.get("auto_merge", True):
            rec.update(status="awaiting_human",
                       note="STOP: non-auto-merge slice awaits human PR review")
            events.append(f"{sid}: verified, awaiting human (auto_merge false)")
            save(rec)
            return
        st = ops.pr_state(rec["pr_url"])
        if st["state"] == "merged":
            rec["status"] = "merged"
            save(rec)
            return
        if st["state"] == "closed":
            fail(rec, "pr", f"PR closed unmerged: {rec['pr_url']}")
            return
        if st.get("head") != rec.get("verified_head"):
            rec.update(status="verify_pending",
                       note=f"PR head moved past verified_head {rec.get('verified_head')}")
            events.append(f"{sid}: head moved, re-verifying")
            save(rec)
            return
        ops.merge(rec["pr_url"], rec["verified_head"])
        rec["status"] = "merged"
        events.append(f"{sid}: merged at {rec['verified_head']}")
        save(rec)

    def verify(s, rec):
        sid = s["id"]
        if tripped():
            events.append(f"{sid}: breaker tripped, verify not launched")
            return
        report = ops.launch(_brief("verify", s, policy, rec["pr_url"])) or {}
        status = report.get("status")
        if status == "PASS":
            rec["verified_head"] = report.get("verified_head")
            rec["findings"] = report.get("findings", rec.get("findings", []))
            if report.get("blocking") or not rec["verified_head"]:
                fail(rec, report.get("failure_class") or "verify",
                     "verifier reported blocking findings or no verified_head")
                return
            bad = _protected_paths(ops.changed_paths(rec["pr_url"]), sid)
            if bad:
                fail(rec, "protected_files_tampered",
                     f"PR diff touches protected paths: {bad}")
                return
            rec["status"] = "merge_ready"
            save(rec)
            try_merge(s, rec)
        elif status in ("FAIL", "BLOCKED"):
            rec["findings"] = report.get("findings", rec.get("findings", []))
            fail(rec, report.get("failure_class") or status.lower(),
                 report.get("one_line_summary") or f"verify child {status}")
        else:
            rec.update(status="verify_pending", note="verifier in flight",
                       child=report.get("session_id") or report.get("child"))
            save(rec)

    def migrate(s, rec):
        sid = s["id"]
        shared = _collision(doc, s, {r["slice"] for r in records.values()
                                     if r["status"] in RUNNING} - {s["id"]})
        if shared:
            fail(rec, "collision",
                 f"shares write table(s) {shared} with running slice(s), no disposition")
            return
        if in_flight() >= width:
            rec["status"] = "waiting"
            events.append(f"{sid}: width {width} reached")
            save(rec)
            return
        report = ops.launch(_brief("migrate", s, policy)) or {}
        status = report.get("status")
        if status == "PASS" and report.get("pr_url"):
            rec.update(status="verify_pending", pr_url=report["pr_url"],
                       branch=report.get("branch"),
                       findings=report.get("findings", []),
                       recon_cost=report.get("recon_cost"))
            save(rec)
            verify(s, rec)
        elif status == "PASS":
            fail(rec, "report", "migrate child PASS without pr_url")
        elif status in ("FAIL", "BLOCKED"):
            rec["findings"] = report.get("findings", rec.get("findings", []))
            fail(rec, report.get("failure_class") or status.lower(),
                 report.get("one_line_summary") or f"migrate child {status}")
        else:
            rec.update(status="launched", note="migrate child in flight",
                       child=report.get("session_id") or report.get("child"))
            save(rec)

    for s in order:
        sid = s["id"]
        rec = records[sid]
        if rec.get("pr_url") and rec["status"] in RUNNING:
            st = ops.pr_state(rec["pr_url"])
            if st["state"] == "merged":
                rec["status"] = "merged"
                save(rec)
            elif st["state"] == "closed":
                fail(rec, "pr", f"PR closed unmerged: {rec['pr_url']}")
        if rec["status"] in ("merged", "awaiting_human"):
            continue
        if not all(merged(d) for d in s.get("depends_on", []) if d in by_id):
            if rec["status"] != "waiting":
                rec["status"] = "waiting"
                save(rec)
            continue
        if rec["status"] == "merge_ready":
            try_merge(s, rec)
        elif rec["status"] in ("launched", "verify_pending") and rec.get("pr_url"):
            verify(s, rec)
        elif rec["status"] != "halted":
            if tripped():
                rec["status"] = "halted"
                events.append(f"{sid}: breaker tripped, migrate not launched")
                save(rec)
            else:
                migrate(s, rec)

    return {"slices": {sid: {k: r[k] for k in ("status", "pr_url", "verified_head",
                                             "failure_class", "note") if k in r}
                       for sid, r in records.items()},
            "failures": dict(failures),
            "halted": tripped(),
            "events": events}


class GhOps:
    """Thin real adapter: `gh` CLI for PR operations; the Devin sessions API
    (DEVIN_API_KEY, optional DEVIN_API_BASE) for `launch`."""

    def __init__(self, poll_s=60, timeout_s=6 * 3600):
        self.poll_s = poll_s
        self.timeout_s = timeout_s

    @staticmethod
    def _gh(*args):
        out = subprocess.run(["gh", *args], capture_output=True, text=True)
        if out.returncode:
            raise RuntimeError(f"gh {' '.join(args)}: {out.stderr.strip()}")
        return out.stdout

    def pr_state(self, pr_url):
        data = json.loads(self._gh("pr", "view", pr_url, "--json", "state,headRefOid"))
        return {"state": {"MERGED": "merged", "OPEN": "open", "CLOSED": "closed"}[data["state"]],
                "head": data.get("headRefOid")}

    def merge(self, pr_url, sha):
        self._gh("pr", "merge", pr_url, "--merge", "--match-head-commit", sha)

    def changed_paths(self, pr_url):
        return [line for line in self._gh("pr", "diff", pr_url, "--name-only").splitlines()
                if line.strip()]

    def launch(self, brief):
        import urllib.request
        base = os.environ.get("DEVIN_API_BASE", "https://api.devin.ai")
        key = os.environ["DEVIN_API_KEY"]

        def call(method, path, body=None):
            req = urllib.request.Request(
                f"{base}{path}", data=json.dumps(body).encode() if body else None,
                method=method, headers={"Authorization": f"Bearer {key}",
                                        "Content-Type": "application/json"})
            return json.loads(urllib.request.urlopen(req).read())

        session_id = call("POST", "/v3/sessions", {"prompt": brief})["session_id"]
        deadline = time.time() + self.timeout_s
        while time.time() < deadline:
            sess = call("GET", f"/v3/sessions/{session_id}")
            if sess.get("structured_output") is not None:
                return sess["structured_output"]
            if sess.get("status") in ("finished", "stopped", "expired", "blocked"):
                return {"status": "BLOCKED",
                        "one_line_summary": f"session {session_id} ended {sess.get('status')}"}
            time.sleep(self.poll_s)
        return {"status": "BLOCKED", "one_line_summary": f"session {session_id} timed out"}


def _read(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"{path}: {exc}")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="fanout.py",
                                 description="slice fan-out (design-0.7 section 7)")
    ap.add_argument("--policy", default=".migration/policy.json")
    ap.add_argument("--slices", default=".migration/slices.json")
    ap.add_argument("--migration-dir", default=".migration")
    ap.add_argument("--width", type=int, help="parallel children (default policy.width, else 4)")
    ap.add_argument("--breaker", type=int,
                    help="same-class failures that stop launching (default policy.breaker, else 3)")
    args = ap.parse_args(argv)
    summary = run(GhOps(), _read(args.policy), _read(args.slices),
                  migration_dir=args.migration_dir,
                  width=args.width, breaker=args.breaker)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 1 if summary["halted"] or summary["failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
