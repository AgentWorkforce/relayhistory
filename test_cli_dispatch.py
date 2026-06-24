import json
import os
import sqlite3
import subprocess
from pathlib import Path


ROOT = Path(__file__).parent
CLI = ROOT / "ai-hist"
RUST_CLI = ROOT / "ai-hist-rust"


def run_cli(args, env, check=False):
    result = subprocess.run(
        [str(CLI), *args],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check:
        assert result.returncode == 0, result.stderr
    return result


def isolated_env(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    db = tmp_path / "ai-history.db"
    env = os.environ.copy()
    env.update(
        {
            "HOME": str(home),
            "AI_HIST_DB": str(db),
            "OPENCODE_DB": str(tmp_path / "missing-opencode.db"),
            "TRAJECTORY_ROOT": str(tmp_path / "trajectories"),
        }
    )
    return env, db, home


def write_history(home):
    claude = home / ".claude"
    codex = home / ".codex"
    claude.mkdir()
    codex.mkdir()
    claude.joinpath("history.jsonl").write_text(
        json.dumps(
            {
                "display": "dispatch unique claude prompt",
                "timestamp": 1700000000000,
                "project": "/tmp/dispatch/project",
                "sessionId": "claude-dispatch",
            }
        )
        + "\n"
    )
    codex.joinpath("history.jsonl").write_text(
        json.dumps(
            {
                "text": "dispatch unique codex prompt",
                "ts": 1700000001,
                "session_id": "codex-dispatch",
            }
        )
        + "\n"
    )


def seed_via_wrapper(tmp_path):
    env, db, home = isolated_env(tmp_path)
    write_history(home)
    result = run_cli(["sync"], env)
    assert result.returncode == 0, result.stderr
    assert "using deprecated Python fallback" not in result.stderr
    assert "Total:" in result.stdout
    return env, db


def test_sync_routes_to_rust_and_preserves_db(tmp_path):
    env, db = seed_via_wrapper(tmp_path)
    conn = sqlite3.connect(db)
    rows = conn.execute(
        "SELECT source, session_id, prompt FROM history ORDER BY source"
    ).fetchall()
    conn.close()
    assert rows == [
        ("claude", "claude-dispatch", "dispatch unique claude prompt"),
        ("codex", "codex-dispatch", "dispatch unique codex prompt"),
    ]


def test_search_json_routes_to_rust_with_python_compatible_shape(tmp_path):
    env, _db = seed_via_wrapper(tmp_path)
    result = run_cli(["search", "dispatch", "--json"], env)
    assert result.returncode == 0, result.stderr
    assert "deprecated Python fallback" not in result.stderr
    rows = json.loads(result.stdout)
    assert len(rows) == 2
    assert set(rows[0]) == {
        "id",
        "source",
        "session_id",
        "project",
        "prompt",
        "timestamp_ms",
    }


def test_search_tag_without_query_routes_to_rust(tmp_path):
    env, _db = seed_via_wrapper(tmp_path)
    tag = run_cli(["tag", "claude-dispatch", "relayfile-migration", "--source", "claude"], env)
    assert tag.returncode == 0, tag.stderr
    result = run_cli(["search", "--tag", "relayfile-migration", "--json"], env)
    assert result.returncode == 0, result.stderr
    assert "deprecated Python fallback" not in result.stderr
    rows = json.loads(result.stdout)
    assert [row["session_id"] for row in rows] == ["claude-dispatch"]


def test_session_full_routes_to_rust(tmp_path):
    env, _db = seed_via_wrapper(tmp_path)
    result = run_cli(["session", "claude-dispatch", "--full"], env)
    assert result.returncode == 0, result.stderr
    assert "using deprecated Python fallback" not in result.stderr
    assert "dispatch unique claude prompt" in result.stdout


def test_session_missing_json_has_no_extra_stderr(tmp_path):
    env, _db = seed_via_wrapper(tmp_path)
    result = run_cli(["session", "missing-session", "--json"], env)
    assert result.returncode == 1
    assert result.stdout == "[]\n"
    assert result.stderr == ""


def test_previously_fallback_commands_route_to_rust(tmp_path):
    env, _db = seed_via_wrapper(tmp_path)
    export_path = tmp_path / "export.jsonl"
    for args in (
        ["stats", "--json"],
        ["tag", "claude-dispatch", "release", "--source", "claude", "--json"],
        ["resume", "dispatch", "--json"],
        ["tags", "--sessions"],
        ["show", "1", "--json"],
        ["context", "1"],
        ["pack", "dispatch", "--json"],
    ):
        result = run_cli(args, env)
        assert result.returncode == 0, (args, result.stderr)
        assert "using deprecated Python fallback" not in result.stderr

    export_result = run_cli(["export", str(export_path), "--source", "claude"], env)
    assert export_result.returncode == 0, export_result.stderr
    assert export_path.exists()

    import_result = run_cli(["import", str(export_path), "--dry-run"], env)
    assert import_result.returncode == 0, import_result.stderr
    assert "dry-run" in import_result.stdout


def test_escape_hatches_force_python_or_rust(tmp_path):
    env, _db = seed_via_wrapper(tmp_path)
    python_env = env | {"AI_HIST_CLI": "python"}
    result = run_cli(["search", "dispatch", "--json"], python_env)
    assert result.returncode == 0, result.stderr
    assert "deprecated Python fallback" not in result.stderr

    rust_env = env | {"AI_HIST_CLI": "rust"}
    result = run_cli(["stats"], rust_env)
    assert result.returncode == 0, result.stderr
    assert "Total entries:" in result.stdout


def test_explicit_rust_binary_escape_hatch(tmp_path):
    env, _db = seed_via_wrapper(tmp_path)
    built = ROOT / "target" / "debug" / "ai-hist"
    if not built.exists():
        subprocess.run(["cargo", "build", "-q", "-p", "ai-hist-cli"], cwd=ROOT, check=True)
    result = run_cli(
        ["search", "dispatch", "--json"],
        env | {"AI_HIST_RUST_BIN": str(built)},
    )
    assert result.returncode == 0, result.stderr
    assert len(json.loads(result.stdout)) == 2


def test_top_level_help_lists_rust_and_fallback_commands(tmp_path):
    env, _db, _home = isolated_env(tmp_path)
    result = run_cli(["--help"], env)
    assert result.returncode == 0
    assert "Rust-default commands:" in result.stdout
    assert "Python fallback commands:" in result.stdout
    assert "search" in result.stdout
    assert "sync" in result.stdout
    assert "show" in result.stdout
    assert "DISPATCH_MATRIX.md" in result.stdout


def test_rust_default_validates_source_choices(tmp_path):
    env, _db = seed_via_wrapper(tmp_path)
    result = run_cli(["search", "dispatch", "--source", "not-a-source"], env)
    assert result.returncode != 0
    assert "invalid source" in result.stderr


def test_rust_default_uses_xdg_data_home_when_ai_hist_db_is_unset(tmp_path):
    env = os.environ.copy()
    home = tmp_path / "home"
    xdg = tmp_path / "xdg"
    home.mkdir()
    env.update({"HOME": str(home), "XDG_DATA_HOME": str(xdg)})
    env.pop("AI_HIST_DB", None)
    result = subprocess.run(
        [str(RUST_CLI), "recent", "--json"],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert result.returncode == 0, result.stderr
    assert (xdg / "ai-hist" / "ai-history.db").exists()
