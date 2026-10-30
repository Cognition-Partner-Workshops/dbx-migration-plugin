"""Table-driven tests for hooks/guard.py (design-0.7.md section 4).

Runs the guard as a subprocess against temporary git repos: one row per
G1-G5 shape, positive and negative, plus arming and .migration/base
resolution via `git show`.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

GUARD = Path(__file__).resolve().parents[1] / "hooks" / "guard.py"
PLUGIN_ROOT = GUARD.parents[1]
POLICY = {"version": 2,
          "bundle_targets": ["migration"],
          "forbidden_targets": ["prod", "production"],
          "legacy_sources": ["LEGACY_DSN", "legacy-db-01"]}


def git(ws, *args):
    return subprocess.run(["git", "-C", str(ws), *args], check=True,
                          capture_output=True, text=True).stdout.strip()


def run_guard(event, cwd, env=None):
    e = dict(os.environ)
    e.update(env or {})
    return subprocess.run([sys.executable, str(GUARD)], input=json.dumps(event),
                          capture_output=True, text=True, cwd=str(cwd), env=e)


def exec_event(cmd):
    return {"tool_name": "exec", "tool_input": {"command": cmd}}


def edit_event(path, tool="write"):
    return {"tool_name": tool, "tool_input": {"file_path": str(path), "content": "x"}}


def decision(r):
    assert r.returncode in (0, 2), r.stderr
    return json.loads(r.stdout)["decision"] if r.stdout.strip() else None


@pytest.fixture
def plain(tmp_path):
    return tmp_path


@pytest.fixture
def armed(tmp_path):
    (tmp_path / ".migration").mkdir()
    return tmp_path


@pytest.fixture
def policy_ws(tmp_path):
    """Repo with .migration/base + policy.json committed on base ref mig/run."""
    (tmp_path / ".migration").mkdir()
    git(tmp_path, "init", "-b", "main")
    git(tmp_path, "config", "user.email", "t@example.com")
    git(tmp_path, "config", "user.name", "t")
    (tmp_path / ".migration/base").write_text("mig/run\n")
    (tmp_path / ".migration/policy.json").write_text(json.dumps(POLICY))
    git(tmp_path, "add", ".migration")
    git(tmp_path, "commit", "-qm", "policy")
    git(tmp_path, "branch", "mig/run")
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    git(tmp_path, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
    return tmp_path


G1 = [
    ("psql -c 'select 1 from t'", "warn"),
    ("psql -c 'select a<b from t'", "warn"),
    ("psql --version", "warn"),
    ("sqlplus u@dsn -c 'select sysdate from dual'", "warn"),
    ("psql -c \"drop table t\"", "block"),
    ("sqlcmd -Q \"DELETE FROM t\"", "block"),
    ("sqlcmd -Q 'COMMIT'", "block"),
    ("psql -f run.sql", "block"),
    ("sqlcmd -i load.sql", "block"),
    ("sqlplus u@dsn @script.sql", "block"),
    ("mysql -u x db < dump.sql", "block"),
    ("cat q.sql | bteq", "block"),
    ("echo 'DROP TABLE t' | psql", "block"),
    ("ssh db-01 \"bteq < q.sql\"", "block"),
    ("docker exec dbx psql -c 'select 1'", "block"),
    ("psql -c 'select 1' | grep x", "warn"),
    ("psql -c 'DROP TABLE t'", "block"),
    ("psql -c 'COMMIT'", "block"),
    ("psql -c \"SELECT * FROM audit WHERE action = 'DELETE'\"", "warn"),
    ("grep -r psql src/", "warn"),
    ("git commit -m 'note about psql'", "block"),  # COMMIT is a G1 verb and the token is engaged
]

G1_LEGACY = [
    ("sqlplus $LEGACY_DSN -c 'select 1'", "warn"),
    ("ssh legacy-db-01 'rm -rf /x'", "block"),
    ("ssh LEGACY-DB-01 'rm -rf /x'", "block"),
    ("echo LEGACY_DSN is set", "warn"),
    ("sqlldr $LEGACY_DSN control=l.ctl < in.dat", "block"),
]

G2 = [
    ("databricks bundle deploy -t migration", "warn"),
    ("databricks bundle deploy -t prod", "block"),
    ("databricks bundle deploy --target production", "block"),
    ("dbt run --target dev", "block"),
    ("dbt build -t migration", "warn"),
    ("databricks bundle run -t prod", "block"),
    ("databricks bundle destroy --target=prod", "block"),
    ("databricks bundle deploy", "block"),
    ("databricks bundle deploy -t 'migration'", "warn"),
    ("databricks bundle deploy -t 'prod'", "block"),
    ("databricks bundle deploy --target=\"prod\"", "block"),
    ("echo -t migration; databricks bundle deploy", "block"),
    ("databricks bundle validate -t prod", "warn"),
    ("databricks postgres create-branch projects/p/branches/production/x", "block"),
    ("databricks postgres delete-endpoint projects/p/branches/production/ep", "block"),
    ("databricks postgres list-branches projects/p/branches/production", "warn"),
    ("databricks postgres create-branch projects/p/branches/mig-run/x", "warn"),
]

G3_EXEC = [
    ("export DATABRICKS_TOKEN=x", "block"),
    ("DATABRICKS_HOST=h databricks clusters list", "block"),
    ("export DATABRICKS_CLIENT_SECRET=x", "block"),
    ("echo $DATABRICKS_TOKEN", "warn"),
    ("echo x > /opt/.devin/envrc", "block"),
    ("sed -i s/a/b/ /opt/.devin/envrc", "block"),
    ("echo hi | tee /opt/.devin/envrc", "block"),
    ("cat /opt/.devin/envrc", "warn"),
    ("cat /opt/.devin/envrc && echo hi > /tmp/report", "warn"),
    ("sed -n '1p' /opt/.devin/envrc", "warn"),
    ("sed -i 's;a;b;' /opt/.devin/envrc", "block"),
    ("sed -i 's|a|b|' /opt/.devin/envrc", "block"),
    ("source /opt/.devin/envrc", "warn"),
    ("echo x > ~/.databrickscfg", "block"),
    ("echo x > /home/u/.databricks/cfg", "block"),
    ("cat ~/.databrickscfg", "warn"),
    ("databricks configure --profile x", "block"),
    ("databricks auth login", "block"),
    ("databricks auth describe", "warn"),
]

G5_EXEC = [
    ("sed -i s/x/y/ hooks/guard.py", "block"),
    ("rm hooks/guard.py", "block"),
    ("echo x > " + str(PLUGIN_ROOT) + "/hooks.json", "block"),
    ("cp " + str(PLUGIN_ROOT) + "/hooks/guard.py /tmp/g.py", "warn"),
    ("cp /tmp/x " + str(PLUGIN_ROOT) + "/hooks/", "block"),
    ("cp -t /tmp " + str(PLUGIN_ROOT) + "/hooks/guard.py", "warn"),
    ("cp -t " + str(PLUGIN_ROOT) + "/hooks/ /tmp/x", "block"),
    ("mv --target-directory=" + str(PLUGIN_ROOT) + " /tmp/x", "block"),
    ("cat hooks/guard.py", "warn"),
    ("cat hooks/guard.py && echo hi > /tmp/report", "warn"),
    (sys.executable + " hooks/guard.py", "warn"),
]

@pytest.mark.parametrize("cmd,want", G1 + G1_LEGACY + G2 + G3_EXEC + G5_EXEC,
                         ids=[c[0] for c in G1 + G1_LEGACY + G2 + G3_EXEC + G5_EXEC])
def test_exec(policy_ws, cmd, want):
    assert decision(run_guard(exec_event(cmd), policy_ws)) == want


G3_G5_EDIT = [
    ("/home/u/.databrickscfg", "block"),
    ("/home/u/.databricks/cfg", "block"),
    ("/home/u/.config/databricks/profiles", "block"),
    ("/opt/.devin/envrc", "block"),
    ("src/app.py", "warn"),
    (".migration/policy.json", "warn"),
    (".migration/recon/slice-0/inputs/mapping.json", "warn"),
]


@pytest.mark.parametrize("path,want", G3_G5_EDIT, ids=[p for p, _ in G3_G5_EDIT])
def test_edit(policy_ws, path, want):
    p = Path(path) if Path(path).is_absolute() else policy_ws / path
    assert decision(run_guard(edit_event(p), policy_ws)) == want


def test_g5_edit_inside_plugin_tree(policy_ws):
    r = run_guard(edit_event(PLUGIN_ROOT / "hooks" / "guard.py"), policy_ws)
    assert r.returncode == 2
    assert "plugin tree" in json.loads(r.stdout)["reason"]


def test_g5_edit_other_plugin_files(policy_ws):
    for rel in ("AGENTS.md", "skills/plan/SKILL.md", "tools/fanout.py"):
        assert decision(run_guard(edit_event(PLUGIN_ROOT / rel), policy_ws)) == "block"


def test_unarmed(plain):
    r = run_guard(exec_event("psql -f run.sql"), plain)
    assert r.returncode == 0
    assert r.stdout == ""


def test_unarmed_edit(plain):
    r = run_guard(edit_event(plain / "x.txt"), plain)
    assert r.returncode == 0
    assert r.stdout == ""


def test_armed_no_policy_deploy_blocks_read_passes(armed):
    assert decision(run_guard(exec_event("databricks bundle deploy -t migration"), armed)) == "block"
    assert decision(run_guard(exec_event("psql -c 'select 1'"), armed)) == "warn"


def test_armed_not_a_git_repo(armed):
    assert decision(run_guard(exec_event("databricks bundle deploy -t migration"), armed)) == "block"
    assert decision(run_guard(exec_event("ls"), armed)) == "warn"


def test_base_resolution_via_git_show(policy_ws):
    """policy.json comes from `git show <base>:...`, never the working copy."""
    (policy_ws / ".migration/policy.json").write_text(
        json.dumps({**POLICY, "bundle_targets": ["evil"], "legacy_sources": []}))
    assert decision(run_guard(exec_event("databricks bundle deploy -t evil"), policy_ws)) == "block"
    assert decision(run_guard(exec_event("databricks bundle deploy -t migration"), policy_ws)) == "warn"
    assert decision(run_guard(exec_event("ssh legacy-db-01 'ls'"), policy_ws)) == "block"


def test_base_file_working_copy_fallback(tmp_path):
    """No origin/HEAD: the working-copy .migration/base names the base ref."""
    (tmp_path / ".migration").mkdir()
    git(tmp_path, "init", "-b", "main")
    git(tmp_path, "config", "user.email", "t@example.com")
    git(tmp_path, "config", "user.name", "t")
    (tmp_path / ".migration/base").write_text("mig/run\n")
    (tmp_path / ".migration/policy.json").write_text(json.dumps(POLICY))
    git(tmp_path, "add", ".migration")
    git(tmp_path, "commit", "-qm", "policy")
    git(tmp_path, "branch", "mig/run")
    assert decision(run_guard(exec_event("databricks bundle deploy -t migration"), tmp_path)) == "warn"
    assert decision(run_guard(exec_event("databricks bundle deploy -t prod"), tmp_path)) == "block"


def test_no_base_no_policy_every_deploy_blocks(armed):
    git(armed, "init", "-b", "main")
    git(armed, "config", "user.email", "t@example.com")
    git(armed, "config", "user.name", "t")
    assert decision(run_guard(exec_event("databricks bundle deploy -t migration"), armed)) == "block"
    assert decision(run_guard(exec_event("databricks bundle deploy"), armed)) == "block"


def test_g4_log_line_per_exec_event(armed):
    run_guard(exec_event("ls"), armed)
    run_guard(exec_event("psql -f run.sql"), armed)
    log = (armed / ".migration/guard.log").read_text().splitlines()
    assert len(log) == 2
    l0, l1 = (json.loads(x) for x in log)
    assert (l0["rule"], l0["decision"], l0["cmd"]) == ("none", "warn", "ls")
    assert (l1["rule"], l1["decision"]) == ("G1", "block")
    assert l0["ts"]


def test_g4_log_redacts_secret_values(armed):
    run_guard(exec_event("export DATABRICKS_TOKEN=sekrit123"), armed)
    (logged,) = (armed / ".migration/guard.log").read_text().splitlines()
    entry = json.loads(logged)
    assert entry["decision"] == "block" and "sekrit123" not in entry["cmd"]
    assert "<redacted>" in entry["cmd"]
    run_guard(exec_event("export DATABRICKS_TOKEN='sekrit with spaces'"), armed)
    entry = json.loads((armed / ".migration/guard.log").read_text().splitlines()[-1])
    assert "sekrit" not in entry["cmd"]


def test_arming_via_cd_target(tmp_path):
    ws, outside = tmp_path / "ws", tmp_path / "outside"
    (ws / ".migration").mkdir(parents=True)
    outside.mkdir()
    r = run_guard(exec_event("cd ../ws && psql -f run.sql"), outside)
    assert r.returncode == 2
    assert (ws / ".migration/guard.log").exists()


def test_arming_cd_into_unarmed_dir_stays_silent(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "empty").mkdir()
    r = run_guard(exec_event("cd ../empty && psql -f run.sql"), outside)
    assert r.returncode == 0 and r.stdout == ""


def test_arming_cd_home_relative(tmp_path):
    home, outside = tmp_path / "home", tmp_path / "outside"
    (home / "ws" / ".migration").mkdir(parents=True)
    outside.mkdir()
    r = run_guard(exec_event("cd ~/ws && psql -f run.sql"), outside,
                  env={"HOME": str(home)})
    assert r.returncode == 2
    assert (home / "ws" / ".migration/guard.log").exists()


def test_no_log_for_edit_events(armed):
    run_guard(edit_event(armed / "x.txt"), armed)
    assert not (armed / ".migration/guard.log").exists()


def test_block_output_contract(policy_ws):
    r = run_guard(exec_event("psql -f run.sql"), policy_ws)
    out = json.loads(r.stdout)
    assert r.returncode == 2
    assert out["decision"] == "block" and out["reason"]
    assert r.stderr.strip()


def test_warn_output_contract(policy_ws):
    r = run_guard(exec_event("ls"), policy_ws)
    out = json.loads(r.stdout)
    assert r.returncode == 0
    assert out["decision"] == "warn" and out["reason"]


def test_malformed_event(plain):
    r = subprocess.run([sys.executable, str(GUARD)], input="not json",
                       capture_output=True, text=True, cwd=str(plain))
    assert r.returncode == 0 and r.stdout == ""


def test_empty_event(armed):
    assert decision(run_guard({"tool_name": "exec", "tool_input": {}}, armed)) is None
