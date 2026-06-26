import json
import os
import sqlite3
import subprocess
from pathlib import Path


ROOT = Path(__file__).parent


def test_install_script_installs_working_launchers_from_source(tmp_path):
    bin_dir = tmp_path / "bin"
    install_dir = tmp_path / "share" / "ai-hist"
    env = os.environ.copy()
    env.update(
        {
            "AI_HIST_SOURCE_DIR": str(ROOT),
            "AI_HIST_BIN_DIR": str(bin_dir),
            "AI_HIST_INSTALL_DIR": str(install_dir),
            "AI_HIST_BUILD_PROFILE": "debug",
        }
    )

    result = subprocess.run(
        ["sh", str(ROOT / "install.sh")],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert result.returncode == 0, result.stderr

    ai_hist = bin_dir / "ai-hist"
    ai_hist_python = bin_dir / "ai-hist-python"
    ai_hist_rust = bin_dir / "ai-hist-rust"
    assert ai_hist.exists()
    assert ai_hist_python.exists()
    assert ai_hist_rust.exists()
    assert install_dir.joinpath("ai-hist-rust-bin").exists()

    help_result = subprocess.run(
        [str(ai_hist), "--help"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert help_result.returncode == 0
    assert "Rust-default commands:" in help_result.stdout
    assert "Python fallback" not in help_result.stdout
    assert "sync" in help_result.stdout

    home = tmp_path / "home"
    home.joinpath(".claude").mkdir(parents=True)
    home.joinpath(".codex").mkdir(parents=True)
    home.joinpath(".claude", "history.jsonl").write_text(
        json.dumps(
            {
                "display": "installer claude prompt",
                "timestamp": 1700000000000,
                "project": "/tmp/install",
                "sessionId": "install-claude",
            }
        )
        + "\n"
    )
    home.joinpath(".codex", "history.jsonl").write_text(
        json.dumps(
            {
                "text": "installer codex prompt",
                "ts": 1700000001,
                "session_id": "install-codex",
            }
        )
        + "\n"
    )
    db_path = tmp_path / "history.db"
    run_env = os.environ.copy()
    run_env.update(
        {
            "HOME": str(home),
            "AI_HIST_DB": str(db_path),
            "OPENCODE_DB": str(tmp_path / "missing-opencode.db"),
            "TRAJECTORY_ROOT": str(tmp_path / "trajectories"),
            "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
        }
    )

    sync_result = subprocess.run(
        [str(ai_hist), "sync"],
        env=run_env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert sync_result.returncode == 0, sync_result.stderr
    assert "using deprecated Python fallback" not in sync_result.stderr

    search_result = subprocess.run(
        [str(ai_hist), "search", "installer", "--json"],
        env=run_env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert search_result.returncode == 0, search_result.stderr
    rows = json.loads(search_result.stdout)
    assert len(rows) == 2

    rust_result = subprocess.run(
        [str(ai_hist_rust), "--db", str(db_path), "recent", "--json"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert rust_result.returncode == 0, rust_result.stderr
    assert len(json.loads(rust_result.stdout)) == 2

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM history").fetchone()[0] == 2
    finally:
        conn.close()


def test_install_script_binary_mode_does_not_require_cargo(tmp_path):
    bin_dir = tmp_path / "bin"
    install_dir = tmp_path / "share" / "ai-hist"
    fake_tools = tmp_path / "fake-tools"
    fake_tools.mkdir()
    fake_cargo = fake_tools / "cargo"
    fake_cargo.write_text("#!/bin/sh\necho cargo should not run >&2\nexit 99\n")
    fake_cargo.chmod(0o755)

    fake_binary = tmp_path / "ai-hist-darwin-arm64"
    fake_binary.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = \"--version\" ]; then echo 'ai-hist 9.9.9'; exit 0; fi\n"
        "if [ \"$1\" = \"recent\" ]; then echo '[]'; exit 0; fi\n"
        "echo \"fake ai-hist: $*\"\n"
    )
    fake_binary.chmod(0o755)

    env = os.environ.copy()
    env.update(
        {
            "AI_HIST_INSTALL_METHOD": "binary",
            "AI_HIST_BINARY_URL": fake_binary.as_uri(),
            "AI_HIST_BIN_DIR": str(bin_dir),
            "AI_HIST_INSTALL_DIR": str(install_dir),
            "AI_HIST_WRAPPER_SOURCE_DIR": str(ROOT),
            "PATH": f"{fake_tools}:{os.environ.get('PATH', '')}",
        }
    )

    result = subprocess.run(
        ["sh", str(ROOT / "install.sh")],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert result.returncode == 0, result.stderr
    assert "cargo should not run" not in result.stderr

    ai_hist = bin_dir / "ai-hist"
    ai_hist_python = bin_dir / "ai-hist-python"
    ai_hist_rust = bin_dir / "ai-hist-rust"
    assert ai_hist.exists()
    assert ai_hist_python.exists()
    assert ai_hist_rust.exists()
    assert install_dir.joinpath("ai-hist-rust-bin").exists()
    assert install_dir.joinpath("ai-hist-python").exists()

    run_env = os.environ.copy()
    run_env.pop("AI_HIST_RUST_BIN", None)

    version_result = subprocess.run(
        [str(ai_hist), "--version"],
        env=run_env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert version_result.returncode == 0, version_result.stderr
    assert version_result.stdout.strip() == "ai-hist 9.9.9"

    rust_result = subprocess.run(
        [str(ai_hist_rust), "recent"],
        env=run_env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert rust_result.returncode == 0, rust_result.stderr
    assert rust_result.stdout.strip() == "[]"
