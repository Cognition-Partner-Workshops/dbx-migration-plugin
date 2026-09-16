import pytest

from skills.repo_text import read_text


def test_read_text_decodes_utf16_bom(tmp_path):
    path = tmp_path / "utf16.txt"
    path.write_bytes("legacy text".encode("utf-16"))

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
