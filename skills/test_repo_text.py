from pathlib import Path

import pytest

from skills.repo_text import read_text


def test_read_text_decodes_utf16_bom(tmp_path):
    path = tmp_path / "utf16.txt"
    path.write_bytes("legacy text".encode("utf-16"))

    assert read_text(path) == "legacy text"


@pytest.mark.parametrize("encoding", ["utf-32-le", "utf-32-be"])
def test_read_text_decodes_utf32_bom(tmp_path, encoding):
    path = tmp_path / "utf32.txt"
    path.write_bytes("legacy text".encode("utf-32"))
    assert read_text(path) == "legacy text"
    path.write_bytes(("\ufeff" + "legacy text").encode(encoding))
    assert read_text(path) == "legacy text"


def test_read_text_skips_nul_bytes(tmp_path):
    path = tmp_path / "binary.dat"
    path.write_bytes(b"header\x00payload")

    assert read_text(path) is None


def test_read_text_rejects_non_utf8_without_bom(tmp_path):
    path = tmp_path / "latin1.txt"
    path.write_bytes(b"\xe9")

    with pytest.raises(ValueError, match="not decodable text"):
        read_text(path)


def test_read_text_decodes_utf8(tmp_path):
    path = tmp_path / "utf8.txt"
    path.write_text("plain text \N{SNOWMAN}", encoding="utf-8")

    assert read_text(path) == "plain text \N{SNOWMAN}"


ROOT = Path(__file__).resolve().parents[1]


def test_platform_5xx_retry_rule_lives_in_target_routing():
    text = (ROOT / "skills/target-routing/SKILL.md").read_text(encoding="utf-8")
    for needle in ("bundle deploy", "bundle run", "5xx", "three attempts", "platform_5xx"):
        assert needle in text, needle


@pytest.mark.parametrize("path", ["skills/install-dbx-factory/playbooks/5-unit_migration.md",
                                  "skills/install-dbx-factory/playbooks/12-front_door_code.md"])
def test_deploy_playbooks_point_at_the_5xx_retry_rule(path):
    text = (ROOT / path).read_text(encoding="utf-8")
    assert "target-routing/SKILL.md" in text


def test_the_signed_doctor_record_is_committed_beside_the_manifest():
    """Children reuse the orchestrator's wave-<N>.doctor.json only if it reaches their checkout: the
    fan-out skill says to commit it (it carries no secret values) and the doctor skill says what the
    child checks before trusting it."""
    fanout = (ROOT / "skills/migration-fanout/SKILL.md").read_text(encoding="utf-8")
    assert "do not commit it" not in fanout
    step = fanout[fanout.index("wave-<N>.doctor.json"):]
    assert "commit" in step[:600].lower() and "no secret" in step[:600]
    doctor = (ROOT / "skills/factory-doctor/SKILL.md").read_text(encoding="utf-8")
    for needle in ("signature", "doctor_max_age", "--expect-identity", "inputs_sha", "`reusable`",
                   "source_principal_read_only", "named_secrets_exist"):
        assert needle in doctor, needle


def test_structural_mode_is_documented_once_in_the_harness_skill():
    text = (ROOT / "skills/data-reconciliation/SKILL.md").read_text(encoding="utf-8")
    assert "`structural`" in text and "structural|" in text or "|structural" in text
    fanout = (ROOT / "skills/migration-fanout/SKILL.md").read_text(encoding="utf-8")
    assert "--mode structural" in fanout
