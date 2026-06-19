"""Tests for ai-hist — 100% coverage target."""

import importlib.machinery
import importlib.util
import json
import os
import sqlite3
import sys
import time
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

# Import the ai-hist script (no .py extension)
_path = str(Path(__file__).parent / "ai-hist")
_loader = importlib.machinery.SourceFileLoader("ai_hist", _path)
_spec = importlib.util.spec_from_loader("ai_hist", _loader, origin=_path)
ai_hist = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ai_hist)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_env(tmp_path, monkeypatch):
    """Set up isolated DB, state, and source files."""
    db_path = tmp_path / "test.db"
    state_path = tmp_path / ".sync-state.json"
    claude_hist = tmp_path / "claude_history.jsonl"
    codex_hist = tmp_path / "codex_history.jsonl"

    monkeypatch.setattr(ai_hist, "DB_PATH", db_path)
    monkeypatch.setattr(ai_hist, "STATE_PATH", state_path)
    monkeypatch.setattr(ai_hist, "SOURCES", {
        "claude": claude_hist,
        "codex": codex_hist,
    })

    # Point cursor at an empty tmp dir so it never reads real ~/.cursor.
    cursor_root = tmp_path / "cursor_projects"
    monkeypatch.setattr(ai_hist, "CURSOR_ROOT", cursor_root)
    trajectory_root = tmp_path / ".trajectories"
    monkeypatch.setattr(ai_hist, "TRAJECTORY_ROOT", str(trajectory_root))
    monkeypatch.setattr(ai_hist, "DEFAULT_TRAJECTORY_SEARCH_ROOT", tmp_path / "Projects")

    # Isolate Claude per-session JSONL scanning from real ~/.claude/projects.
    claude_projects_root = tmp_path / "claude_projects"
    monkeypatch.setattr(ai_hist, "CLAUDE_PROJECTS_ROOT", claude_projects_root)

    # Isolate Codex rollout dirs from real ~/.codex/sessions.
    monkeypatch.setattr(ai_hist, "CODEX_SESSIONS_DIRS", [])

    return SimpleNamespace(
        db_path=db_path,
        state_path=state_path,
        claude_hist=claude_hist,
        codex_hist=codex_hist,
        cursor_root=cursor_root,
        trajectory_root=trajectory_root,
        claude_projects_root=claude_projects_root,
        tmp_path=tmp_path,
    )


def make_cursor_session(cursor_root: Path, project: str, session_id: str, prompts: list,
                        wrap_user_query: bool = True) -> Path:
    """Create a fake cursor agent-transcripts jsonl. Returns the file path."""
    session_dir = cursor_root / project / "agent-transcripts" / session_id
    session_dir.mkdir(parents=True, exist_ok=True)
    jsonl = session_dir / f"{session_id}.jsonl"
    lines = []
    for p in prompts:
        text = f"<user_query>\n{p}\n</user_query>" if wrap_user_query else p
        lines.append(json.dumps({
            "role": "user",
            "message": {"content": [{"type": "text", "text": text}]},
        }))
        # Add an assistant response so we exercise the role filter.
        lines.append(json.dumps({
            "role": "assistant",
            "message": {"content": [{"type": "text", "text": "ok"}]},
        }))
    jsonl.write_text("\n".join(lines) + "\n")
    return jsonl


def make_claude_entry(display, timestamp=1700000000000, project="/proj", session_id="s1"):
    return json.dumps({
        "display": display,
        "timestamp": timestamp,
        "project": project,
        "sessionId": session_id,
        "pastedContents": {},
    })


def make_codex_entry(text, ts=1700000000, session_id="cs1"):
    return json.dumps({
        "text": text,
        "ts": ts,
        "session_id": session_id,
    })


def seed_db(env, claude_lines=None, codex_lines=None):
    """Write history files and run sync."""
    if claude_lines:
        env.claude_hist.write_text("\n".join(claude_lines) + "\n")
    if codex_lines:
        env.codex_hist.write_text("\n".join(codex_lines) + "\n")
    ai_hist.cmd_sync()


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------

class TestParseClaude:
    def test_valid_entry(self):
        line = make_claude_entry("hello world", 1700000000000, "/my/project", "sess1")
        result = ai_hist.parse_claude(line)
        assert result == {
            "source": "claude",
            "session_id": "sess1",
            "project": "/my/project",
            "prompt": "hello world",
            "prompt_hash": ai_hist._prompt_hash("hello world"),
            "timestamp_ms": 1700000000000,
            "git_branch": None,
        }

    def test_git_branch_captured(self):
        line = json.dumps({"display": "hi", "timestamp": 1, "gitBranch": "feat/x"})
        result = ai_hist.parse_claude(line)
        assert result["git_branch"] == "feat/x"

    def test_empty_display_returns_none(self):
        line = json.dumps({"display": "", "timestamp": 123})
        assert ai_hist.parse_claude(line) is None

    def test_whitespace_display_returns_none(self):
        line = json.dumps({"display": "   ", "timestamp": 123})
        assert ai_hist.parse_claude(line) is None

    def test_missing_display_returns_none(self):
        line = json.dumps({"timestamp": 123})
        assert ai_hist.parse_claude(line) is None

    def test_missing_optional_fields(self):
        line = json.dumps({"display": "test"})
        result = ai_hist.parse_claude(line)
        assert result["session_id"] is None
        assert result["project"] is None
        assert result["timestamp_ms"] == 0


class TestParseCodex:
    def test_valid_entry(self):
        line = make_codex_entry("fix the bug", 1700000000, "cs1")
        result = ai_hist.parse_codex(line)
        assert result == {
            "source": "codex",
            "session_id": "cs1",
            "project": None,
            "prompt": "fix the bug",
            "prompt_hash": ai_hist._prompt_hash("fix the bug"),
            "timestamp_ms": 1700000000000,
            "git_branch": None,
        }

    def test_empty_text_returns_none(self):
        line = json.dumps({"text": "", "ts": 123})
        assert ai_hist.parse_codex(line) is None

    def test_whitespace_text_returns_none(self):
        line = json.dumps({"text": "  ", "ts": 100})
        assert ai_hist.parse_codex(line) is None

    def test_missing_text_returns_none(self):
        line = json.dumps({"ts": 123})
        assert ai_hist.parse_codex(line) is None

    def test_missing_optional_fields(self):
        line = json.dumps({"text": "hello"})
        result = ai_hist.parse_codex(line)
        assert result["session_id"] is None
        assert result["timestamp_ms"] == 0


# ---------------------------------------------------------------------------
# Core function tests
# ---------------------------------------------------------------------------

class TestInitDb:
    def test_creates_tables(self, tmp_path):
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        ai_hist.init_db(conn)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='history'"
        ).fetchone()
        assert tables is not None
        fts = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='history_fts'"
        ).fetchone()
        assert fts is not None
        conn.close()

    def test_idempotent(self, tmp_path):
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        ai_hist.init_db(conn)
        ai_hist.init_db(conn)  # should not raise
        conn.close()


class TestLoadSaveState:
    def test_load_empty(self, tmp_env):
        state = ai_hist.load_state()
        assert state == {}

    def test_save_and_load(self, tmp_env):
        ai_hist.save_state({"claude": 100, "codex": 200})
        state = ai_hist.load_state()
        assert state == {"claude": 100, "codex": 200}

    def test_save_creates_parent_dir(self, tmp_env, monkeypatch):
        new_state = tmp_env.tmp_path / "sub" / "dir" / ".sync-state.json"
        monkeypatch.setattr(ai_hist, "STATE_PATH", new_state)
        ai_hist.save_state({"x": 1})
        assert new_state.exists()


class TestFmtRow:
    def test_with_project(self):
        result = ai_hist.fmt_row(1, "claude", "/my/project", "hello", 1700000000000)
        assert "(claude)" in result
        assert "[/my/project]" in result
        assert "hello" in result
        assert "#1" in result

    def test_without_project(self):
        result = ai_hist.fmt_row(2, "codex", None, "world", 1700000000000)
        assert "(codex)" in result
        assert "[" not in result

    def test_long_prompt_truncated(self):
        long_prompt = "x" * 200
        result = ai_hist.fmt_row(3, "claude", None, long_prompt, 1700000000000)
        assert result.endswith("...")
        assert "x" * 120 in result

    def test_newlines_replaced(self):
        result = ai_hist.fmt_row(4, "claude", None, "line1\nline2", 1700000000000)
        assert "\n" not in result
        assert "line1 line2" in result

    def test_short_prompt_not_truncated(self):
        result = ai_hist.fmt_row(5, "claude", None, "short", 1700000000000)
        assert "..." not in result

    def test_verbose_no_truncation(self):
        long_prompt = "x" * 200
        result = ai_hist.fmt_row(6, "claude", None, long_prompt, 1700000000000, verbose=True)
        assert "..." not in result
        assert "x" * 200 in result

    def test_verbose_preserves_newlines(self):
        result = ai_hist.fmt_row(7, "claude", None, "line1\nline2", 1700000000000, verbose=True)
        assert "line1\nline2" in result


# ---------------------------------------------------------------------------
# Command tests
# ---------------------------------------------------------------------------

class TestCmdSync:
    def test_sync_claude_entries(self, tmp_env, capsys):
        tmp_env.claude_hist.write_text(
            make_claude_entry("first prompt", 1700000001000) + "\n"
            + make_claude_entry("second prompt", 1700000002000) + "\n"
        )
        ai_hist.cmd_sync()
        captured = capsys.readouterr()
        assert "+2" in captured.out
        assert "Total: 2" in captured.out

    def test_sync_codex_entries(self, tmp_env, capsys):
        tmp_env.codex_hist.write_text(
            make_codex_entry("codex prompt", 1700000001) + "\n"
        )
        ai_hist.cmd_sync()
        captured = capsys.readouterr()
        assert "+1" in captured.out

    def test_sync_both_sources(self, tmp_env, capsys):
        tmp_env.claude_hist.write_text(make_claude_entry("c1", 1700000001000) + "\n")
        tmp_env.codex_hist.write_text(make_codex_entry("x1", 1700000001) + "\n")
        ai_hist.cmd_sync()
        captured = capsys.readouterr()
        assert "Total: 2" in captured.out

    def test_incremental_sync(self, tmp_env, capsys):
        tmp_env.claude_hist.write_text(make_claude_entry("first", 1700000001000) + "\n")
        ai_hist.cmd_sync()
        with open(tmp_env.claude_hist, "a") as f:
            f.write(make_claude_entry("second", 1700000002000) + "\n")
        ai_hist.cmd_sync()
        captured = capsys.readouterr()
        assert "Total: 2" in captured.out

    def test_sync_up_to_date(self, tmp_env, capsys):
        tmp_env.claude_hist.write_text(make_claude_entry("first", 1700000001000) + "\n")
        ai_hist.cmd_sync()
        capsys.readouterr()
        ai_hist.cmd_sync()
        captured = capsys.readouterr()
        assert "up to date" in captured.out

    def test_sync_missing_source(self, tmp_env, capsys):
        ai_hist.cmd_sync()
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_sync_skips_empty_lines(self, tmp_env, capsys):
        tmp_env.claude_hist.write_text(
            make_claude_entry("one", 1700000001000) + "\n\n\n"
            + make_claude_entry("two", 1700000002000) + "\n"
        )
        ai_hist.cmd_sync()
        captured = capsys.readouterr()
        assert "Total: 2" in captured.out

    def test_sync_handles_invalid_json(self, tmp_env, capsys):
        tmp_env.claude_hist.write_text(
            "not valid json\n"
            + make_claude_entry("valid", 1700000001000) + "\n"
        )
        ai_hist.cmd_sync()
        captured = capsys.readouterr()
        assert "+1" in captured.out
        assert "1 errors" in captured.out

    def test_sync_skips_none_rows(self, tmp_env, capsys):
        tmp_env.claude_hist.write_text(
            json.dumps({"display": "", "timestamp": 123}) + "\n"
            + make_claude_entry("real", 1700000001000) + "\n"
        )
        ai_hist.cmd_sync()
        captured = capsys.readouterr()
        assert "+1" in captured.out

    def test_sync_dedup_on_reinsert(self, tmp_env, capsys):
        tmp_env.claude_hist.write_text(make_claude_entry("dupe", 1700000001000) + "\n")
        ai_hist.cmd_sync()
        ai_hist.save_state({})
        ai_hist.cmd_sync()
        conn = sqlite3.connect(str(tmp_env.db_path))
        count = conn.execute("SELECT COUNT(*) FROM history").fetchone()[0]
        conn.close()
        assert count == 1

    def test_sync_creates_db_parent_dir(self, tmp_env, monkeypatch):
        nested = tmp_env.tmp_path / "a" / "b" / "test.db"
        monkeypatch.setattr(ai_hist, "DB_PATH", nested)
        ai_hist.cmd_sync()
        assert nested.exists()

    def test_sync_handles_sqlite_error_on_insert(self, tmp_env, capsys, monkeypatch):
        tmp_env.claude_hist.write_text(
            make_claude_entry("will fail", 1700000001000) + "\n"
            + make_claude_entry("also fails", 1700000002000) + "\n"
        )
        original_connect = sqlite3.connect

        class FaultyConnection:
            def __init__(self, conn):
                self._conn = conn
                self._initialized = False

            def executescript(self, sql):
                return self._conn.executescript(sql)

            def execute(self, sql, params=None):
                if sql.startswith("INSERT OR IGNORE INTO history") and self._initialized:
                    raise sqlite3.OperationalError("simulated error")
                result = self._conn.execute(sql, params) if params else self._conn.execute(sql)
                if "PRAGMA" in sql:
                    self._initialized = True
                return result

            def commit(self):
                return self._conn.commit()

            def close(self):
                return self._conn.close()

        def patched_connect(path):
            real_conn = original_connect(path)
            return FaultyConnection(real_conn)

        monkeypatch.setattr(sqlite3, "connect", patched_connect)
        ai_hist.cmd_sync()
        captured = capsys.readouterr()
        assert "2 errors" in captured.out


class TestCmdSearch:
    def test_search_finds_match(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("fix authentication bug", 1700000001000, "/proj"),
            make_claude_entry("add new feature", 1700000002000, "/proj"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(query=["authentication"], source=None, project=None, limit=20)
        ai_hist.cmd_search(args)
        captured = capsys.readouterr()
        assert "authentication" in captured.out
        assert "#" in captured.out  # ID is shown

    def test_search_no_results(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[make_claude_entry("hello", 1700000001000)])
        capsys.readouterr()
        args = SimpleNamespace(query=["zzzznonexistent"], source=None, project=None, limit=20)
        with pytest.raises(SystemExit) as exc_info:
            ai_hist.cmd_search(args)
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "No results." in captured.out

    def test_search_filter_by_source(self, tmp_env, capsys):
        seed_db(tmp_env,
            claude_lines=[make_claude_entry("shared term", 1700000001000)],
            codex_lines=[make_codex_entry("shared term", 1700000002)],
        )
        capsys.readouterr()
        args = SimpleNamespace(query=["shared"], source="codex", project=None, limit=20)
        ai_hist.cmd_search(args)
        captured = capsys.readouterr()
        assert "(codex)" in captured.out
        assert "(claude)" not in captured.out

    def test_search_filter_by_project(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("in relay", 1700000001000, "/proj/relay"),
            make_claude_entry("in dashboard", 1700000002000, "/proj/dashboard"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(query=["in"], source=None, project="relay", limit=20)
        ai_hist.cmd_search(args)
        captured = capsys.readouterr()
        assert "relay" in captured.out
        assert "dashboard" not in captured.out

    def test_search_respects_limit(self, tmp_env, capsys):
        lines = [make_claude_entry(f"test query {i}", 1700000000000 + i * 1000) for i in range(10)]
        seed_db(tmp_env, claude_lines=lines)
        capsys.readouterr()
        args = SimpleNamespace(query=["test"], source=None, project=None, limit=3)
        ai_hist.cmd_search(args)
        captured = capsys.readouterr()
        result_lines = [l for l in captured.out.strip().split("\n") if l.strip()]
        assert len(result_lines) == 3

    def test_search_multi_word_query(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("fix the authentication bug", 1700000001000),
        ])
        capsys.readouterr()
        args = SimpleNamespace(query=["fix", "bug"], source=None, project=None, limit=20)
        ai_hist.cmd_search(args)
        captured = capsys.readouterr()
        assert "authentication" in captured.out

    def test_search_hyphenated_term(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("deploy agent-relay to prod", 1700000001000, "/proj"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(query=["agent-relay"], source=None, project=None, limit=20)
        ai_hist.cmd_search(args)
        captured = capsys.readouterr()
        assert "agent-relay" in captured.out

    def test_search_excludes_leading_dash_term(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("deploy agent relay", 1700000001000, "/proj"),
            make_claude_entry("deploy dashboard", 1700000002000, "/proj"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(query=["deploy", "-relay"], source=None, project=None, limit=20)
        ai_hist.cmd_search(args)
        captured = capsys.readouterr()
        assert "dashboard" in captured.out
        assert "agent relay" not in captured.out

    def test_search_only_excluded_term_returns_no_results(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("deploy agent relay", 1700000001000, "/proj"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(query=["-relay"], source=None, project=None, limit=20)
        with pytest.raises(SystemExit) as exc_info:
            ai_hist.cmd_search(args)
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "No results." in captured.out

    def test_search_quotes_embedded_quotes(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry('fix foo"bar input', 1700000001000, "/proj"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(query=['foo"bar'], source=None, project=None, limit=20)
        ai_hist.cmd_search(args)
        captured = capsys.readouterr()
        assert 'foo"bar' in captured.out


class TestCmdRecent:
    def test_recent_default(self, tmp_env, capsys):
        lines = [make_claude_entry(f"prompt {i}", 1700000000000 + i * 1000) for i in range(5)]
        seed_db(tmp_env, claude_lines=lines)
        capsys.readouterr()
        args = SimpleNamespace(n=20, source=None, project=None)
        ai_hist.cmd_recent(args)
        captured = capsys.readouterr()
        result_lines = [l for l in captured.out.strip().split("\n") if l.strip()]
        assert len(result_lines) == 5

    def test_recent_limited(self, tmp_env, capsys):
        lines = [make_claude_entry(f"prompt {i}", 1700000000000 + i * 1000) for i in range(10)]
        seed_db(tmp_env, claude_lines=lines)
        capsys.readouterr()
        args = SimpleNamespace(n=3, source=None, project=None)
        ai_hist.cmd_recent(args)
        captured = capsys.readouterr()
        result_lines = [l for l in captured.out.strip().split("\n") if l.strip()]
        assert len(result_lines) == 3

    def test_recent_order_newest_first(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("old prompt", 1700000001000),
            make_claude_entry("new prompt", 1700000099000),
        ])
        capsys.readouterr()
        args = SimpleNamespace(n=2, source=None, project=None)
        ai_hist.cmd_recent(args)
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        assert "new prompt" in lines[0]
        assert "old prompt" in lines[1]

    def test_recent_empty_db(self, tmp_env, capsys):
        seed_db(tmp_env)
        capsys.readouterr()
        args = SimpleNamespace(n=10, source=None, project=None)
        ai_hist.cmd_recent(args)
        captured = capsys.readouterr()
        assert captured.out.strip() == ""

    def test_recent_filter_by_source(self, tmp_env, capsys):
        seed_db(tmp_env,
            claude_lines=[make_claude_entry("claude msg", 1700000001000)],
            codex_lines=[make_codex_entry("codex msg", 1700000002)],
        )
        capsys.readouterr()
        args = SimpleNamespace(n=20, source="claude", project=None)
        ai_hist.cmd_recent(args)
        captured = capsys.readouterr()
        assert "(claude)" in captured.out
        assert "(codex)" not in captured.out

    def test_recent_filter_by_project(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("in relay", 1700000001000, "/proj/relay"),
            make_claude_entry("in dash", 1700000002000, "/proj/dashboard"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(n=20, source=None, project="dashboard")
        ai_hist.cmd_recent(args)
        captured = capsys.readouterr()
        assert "dash" in captured.out
        assert "relay" not in captured.out

    def test_recent_filter_by_source_and_project(self, tmp_env, capsys):
        seed_db(tmp_env,
            claude_lines=[make_claude_entry("c relay", 1700000001000, "/proj/relay")],
            codex_lines=[make_codex_entry("x msg", 1700000002)],
        )
        capsys.readouterr()
        args = SimpleNamespace(n=20, source="claude", project="relay")
        ai_hist.cmd_recent(args)
        captured = capsys.readouterr()
        assert "c relay" in captured.out
        assert len([l for l in captured.out.strip().split("\n") if l.strip()]) == 1


class TestCmdShow:
    def test_show_existing_entry(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("full prompt text here\nwith newlines", 1700000001000, "/proj/x", "sess-abc"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(id=1)
        ai_hist.cmd_show(args)
        captured = capsys.readouterr()
        assert "ID:" in captured.out
        assert "Source:    claude" in captured.out
        assert "Session:   sess-abc" in captured.out
        assert "Project:   /proj/x" in captured.out
        assert "full prompt text here\nwith newlines" in captured.out
        # Resume hint
        assert "claude --resume sess-abc" in captured.out
        assert "cd /proj/x" in captured.out
        # Context hint
        assert "ai-hist context 1" in captured.out

    def test_show_claude_session_count(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("first", 1700000001000, "/proj", "sess-cnt"),
            make_claude_entry("second", 1700000002000, "/proj", "sess-cnt"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(id=1)
        ai_hist.cmd_show(args)
        captured = capsys.readouterr()
        assert "Session has 2 entries" in captured.out
        assert "ai-hist session sess-cnt" in captured.out

    def test_show_codex_resume_hint(self, tmp_env, capsys):
        seed_db(tmp_env, codex_lines=[make_codex_entry("codex prompt", 1700000001, "cx-sess")])
        capsys.readouterr()
        args = SimpleNamespace(id=1)
        ai_hist.cmd_show(args)
        captured = capsys.readouterr()
        assert "codex resume cx-sess" in captured.out

    def test_show_nonexistent_entry(self, tmp_env, capsys):
        seed_db(tmp_env)
        capsys.readouterr()
        args = SimpleNamespace(id=999)
        with pytest.raises(SystemExit) as exc_info:
            ai_hist.cmd_show(args)
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "No entry with id 999" in captured.out

    def test_show_entry_without_session_or_project(self, tmp_env, capsys):
        tmp_env.codex_hist.write_text(
            json.dumps({"text": "nosession", "ts": 1700000001}) + "\n"
        )
        ai_hist.cmd_sync()
        capsys.readouterr()
        args = SimpleNamespace(id=1)
        ai_hist.cmd_show(args)
        captured = capsys.readouterr()
        assert "Session:   (none)" in captured.out
        assert "Project:   (none)" in captured.out
        assert "ai-hist context 1" in captured.out
        # No resume hint when no session
        assert "resume" not in captured.out.lower().split("context")[0]


class TestCmdContext:
    def test_context_same_session(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("before", 1700000001000, "/proj", "sess-ctx"),
            make_claude_entry("target", 1700000002000, "/proj", "sess-ctx"),
            make_claude_entry("after", 1700000003000, "/proj", "sess-ctx"),
            make_claude_entry("other session", 1700000002500, "/proj", "sess-other"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(id=2, window=5)
        ai_hist.cmd_context(args)
        captured = capsys.readouterr()
        assert "sess-ctx" in captured.out
        assert "3 entries" in captured.out
        assert "before" in captured.out
        assert "target" in captured.out
        assert "after" in captured.out
        # Current entry marked with >>>
        assert ">>>" in captured.out

    def test_context_nearby_other_sessions(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("mine", 1700000001000, "/proj", "sess-a"),
            make_claude_entry("nearby", 1700000002000, "/proj", "sess-b"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(id=1, window=5)
        ai_hist.cmd_context(args)
        captured = capsys.readouterr()
        assert "Nearby" in captured.out
        assert "nearby" in captured.out

    def test_context_nonexistent(self, tmp_env, capsys):
        seed_db(tmp_env)
        capsys.readouterr()
        args = SimpleNamespace(id=999, window=5)
        with pytest.raises(SystemExit) as exc_info:
            ai_hist.cmd_context(args)
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "No entry with id 999" in captured.out

    def test_context_no_session(self, tmp_env, capsys):
        tmp_env.codex_hist.write_text(
            json.dumps({"text": "lone wolf", "ts": 1700000001}) + "\n"
        )
        ai_hist.cmd_sync()
        capsys.readouterr()
        args = SimpleNamespace(id=1, window=5)
        ai_hist.cmd_context(args)
        captured = capsys.readouterr()
        # No session section, but no crash
        assert "Session" not in captured.out or "Nearby" in captured.out

    def test_context_custom_window(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("target", 1700000001000, "/proj", "sess-w"),
            make_claude_entry("far away", 1700000601000, "/proj", "sess-other"),  # 10 min later
        ])
        capsys.readouterr()
        # 5 min window — should NOT include the far entry
        args = SimpleNamespace(id=1, window=5)
        ai_hist.cmd_context(args)
        captured = capsys.readouterr()
        assert "far away" not in captured.out
        capsys.readouterr()
        # 15 min window — should include it
        args = SimpleNamespace(id=1, window=15)
        ai_hist.cmd_context(args)
        captured = capsys.readouterr()
        assert "far away" in captured.out

    def test_context_no_nearby(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("alone", 1700000001000, "/proj", "sess-alone"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(id=1, window=5)
        ai_hist.cmd_context(args)
        captured = capsys.readouterr()
        # Session shown, no nearby section
        assert "sess-alone" in captured.out
        assert "Nearby" not in captured.out


class TestCmdSession:
    def test_session_shows_all_prompts(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("first in session", 1700000001000, "/proj", "sess-xyz"),
            make_claude_entry("second in session", 1700000002000, "/proj", "sess-xyz"),
            make_claude_entry("different session", 1700000003000, "/proj", "sess-other"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(session_id="sess-xyz", full=False)
        ai_hist.cmd_session(args)
        captured = capsys.readouterr()
        assert "sess-xyz" in captured.out
        assert "2 entries" in captured.out
        assert "first in session" in captured.out
        assert "second in session" in captured.out
        assert "different session" not in captured.out

    def test_session_not_found(self, tmp_env, capsys):
        seed_db(tmp_env)
        capsys.readouterr()
        args = SimpleNamespace(session_id="nonexistent", full=False)
        with pytest.raises(SystemExit) as exc_info:
            ai_hist.cmd_session(args)
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "No entries for session nonexistent" in captured.out

    def test_session_full_flag(self, tmp_env, capsys):
        long_prompt = "x" * 200
        seed_db(tmp_env, claude_lines=[
            make_claude_entry(long_prompt, 1700000001000, "/proj", "sess-full"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(session_id="sess-full", full=True)
        ai_hist.cmd_session(args)
        captured = capsys.readouterr()
        assert "x" * 200 in captured.out
        assert "..." not in captured.out

    def test_session_chronological_order(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("later", 1700000099000, "/proj", "sess-order"),
            make_claude_entry("earlier", 1700000001000, "/proj", "sess-order"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(session_id="sess-order", full=False)
        ai_hist.cmd_session(args)
        captured = capsys.readouterr()
        lines = [l for l in captured.out.strip().split("\n") if l.strip() and "(" in l and "#" in l]
        assert "earlier" in lines[0]
        assert "later" in lines[1]


class TestCmdStats:
    def test_stats_with_data(self, tmp_env, capsys):
        seed_db(tmp_env,
            claude_lines=[
                make_claude_entry("c1", 1700000001000, "/proj/a"),
                make_claude_entry("c2", 1700000002000, "/proj/b"),
            ],
            codex_lines=[
                make_codex_entry("x1", 1700000003),
            ],
        )
        capsys.readouterr()
        ai_hist.cmd_stats()
        captured = capsys.readouterr()
        assert "Total entries: 3" in captured.out
        assert "claude: 2" in captured.out
        assert "codex: 1" in captured.out
        assert "Date range:" in captured.out
        assert "/proj/a" in captured.out or "/proj/b" in captured.out

    def test_stats_empty_db(self, tmp_env, capsys):
        seed_db(tmp_env)
        capsys.readouterr()
        ai_hist.cmd_stats()
        captured = capsys.readouterr()
        assert "Total entries: 0" in captured.out
        assert "Date range:" not in captured.out

    def test_stats_no_projects(self, tmp_env, capsys):
        seed_db(tmp_env, codex_lines=[make_codex_entry("x1", 1700000001)])
        capsys.readouterr()
        ai_hist.cmd_stats()
        captured = capsys.readouterr()
        assert "Top 10 projects:" in captured.out


class TestCmdWatch:
    def test_watch_runs_sync_and_stops(self, tmp_env, capsys):
        call_count = 0

        def mock_sleep(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 1:
                raise KeyboardInterrupt()

        args = SimpleNamespace(interval=1)
        with patch.object(time, "sleep", mock_sleep):
            with pytest.raises(KeyboardInterrupt):
                ai_hist.cmd_watch(args)
        captured = capsys.readouterr()
        assert "Watching every 1s" in captured.out

    def test_watch_handles_sync_error(self, tmp_env, capsys):
        call_count = 0

        def failing_sync(args=None):
            raise RuntimeError("test error")

        def mock_sleep(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 1:
                raise KeyboardInterrupt()

        args = SimpleNamespace(interval=5)
        with patch.object(ai_hist, "cmd_sync", failing_sync):
            with patch.object(time, "sleep", mock_sleep):
                with pytest.raises(KeyboardInterrupt):
                    ai_hist.cmd_watch(args)
        captured = capsys.readouterr()
        assert "Error: test error" in captured.err


# ---------------------------------------------------------------------------
# CLI / main tests
# ---------------------------------------------------------------------------

class TestMain:
    def test_no_args_prints_help(self, capsys):
        with patch("sys.argv", ["ai-hist"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "usage:" in captured.out.lower() or "Sync & search" in captured.out

    def test_sync_command(self, tmp_env, capsys):
        with patch("sys.argv", ["ai-hist", "sync"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "Total:" in captured.out

    def test_search_command(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[make_claude_entry("hello world", 1700000001000)])
        capsys.readouterr()
        with patch("sys.argv", ["ai-hist", "search", "hello"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "hello world" in captured.out

    def test_recent_command(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[make_claude_entry("recent test", 1700000001000)])
        capsys.readouterr()
        with patch("sys.argv", ["ai-hist", "recent", "5"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "recent test" in captured.out

    def test_stats_command(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[make_claude_entry("stats test", 1700000001000)])
        capsys.readouterr()
        with patch("sys.argv", ["ai-hist", "stats"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "Total entries: 1" in captured.out

    def test_search_with_source_flag(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[make_claude_entry("flagtest", 1700000001000)])
        capsys.readouterr()
        with patch("sys.argv", ["ai-hist", "search", "flagtest", "--source", "claude", "--limit", "5"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "flagtest" in captured.out

    def test_search_with_project_flag(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("in relay", 1700000001000, "/proj/relay"),
            make_claude_entry("in dash", 1700000002000, "/proj/dash"),
        ])
        capsys.readouterr()
        with patch("sys.argv", ["ai-hist", "search", "in", "--project", "relay"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "relay" in captured.out
        assert "dash" not in captured.out

    def test_show_command(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[make_claude_entry("show me", 1700000001000)])
        capsys.readouterr()
        with patch("sys.argv", ["ai-hist", "show", "1"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "show me" in captured.out

    def test_session_command(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[make_claude_entry("sess prompt", 1700000001000, "/p", "s1")])
        capsys.readouterr()
        with patch("sys.argv", ["ai-hist", "session", "s1"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "sess prompt" in captured.out

    def test_session_command_with_full(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[make_claude_entry("x" * 200, 1700000001000, "/p", "s2")])
        capsys.readouterr()
        with patch("sys.argv", ["ai-hist", "session", "s2", "--full"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "x" * 200 in captured.out

    def test_watch_command_dispatches(self, tmp_env):
        def mock_watch(args):
            assert args.interval == 60

        with patch.object(ai_hist, "cmd_watch", mock_watch):
            with patch("sys.argv", ["ai-hist", "watch"]):
                ai_hist.main()

    def test_recent_with_source_and_project(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[make_claude_entry("filtered", 1700000001000, "/proj/x")])
        capsys.readouterr()
        with patch("sys.argv", ["ai-hist", "recent", "5", "--source", "claude", "--project", "proj"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "filtered" in captured.out

    def test_context_command(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("ctx target", 1700000001000, "/proj", "sess-c"),
        ])
        capsys.readouterr()
        with patch("sys.argv", ["ai-hist", "context", "1"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "ctx target" in captured.out

    def test_context_command_with_window(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("target", 1700000001000, "/proj", "sess-w"),
        ])
        capsys.readouterr()
        with patch("sys.argv", ["ai-hist", "context", "1", "--window", "10"]):
            ai_hist.main()
        captured = capsys.readouterr()
        assert "target" in captured.out


# ---------------------------------------------------------------------------
# FTS trigger integration test
# ---------------------------------------------------------------------------

class TestFTSIntegration:
    def test_fts_index_populated_on_insert(self, tmp_env):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("unique searchable term xyzzy", 1700000001000, "/proj"),
        ])
        conn = sqlite3.connect(str(tmp_env.db_path))
        rows = conn.execute(
            "SELECT rowid FROM history_fts WHERE history_fts MATCH 'xyzzy'"
        ).fetchall()
        conn.close()
        assert len(rows) == 1

    def test_fts_searches_project_field(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("some prompt", 1700000001000, "/unique/project/path"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(query=["unique"], source=None, project=None, limit=20)
        ai_hist.cmd_search(args)
        captured = capsys.readouterr()
        assert "some prompt" in captured.out


# ---------------------------------------------------------------------------
# Relaycast tests
# ---------------------------------------------------------------------------

class TestIsoToMs:
    def test_basic_iso(self):
        ms = ai_hist._iso_to_ms("2026-03-07T20:13:00Z")
        assert ms > 0

    def test_iso_with_fractional(self):
        ms = ai_hist._iso_to_ms("2026-03-07T20:13:00.123Z")
        assert ms % 1000 == 123

    def test_iso_with_short_frac(self):
        ms = ai_hist._iso_to_ms("2026-03-07T20:13:00.5Z")
        assert ms % 1000 == 500

    def test_iso_with_timezone_offset(self):
        ms = ai_hist._iso_to_ms("2026-03-07T20:13:00+00:00")
        assert ms > 0

    def test_iso_invalid(self):
        assert ai_hist._iso_to_ms("not a date") == 0

    def test_iso_empty(self):
        assert ai_hist._iso_to_ms("") == 0


class TestRelayMsgToRow:
    def test_channel_message(self, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")
        msg = {
            "id": "msg1",
            "from_name": "Lead",
            "text": "deploy to prod",
            "created_at": "2026-03-07T20:13:00.000Z",
            "thread_id": "thread1",
        }
        row = ai_hist._relay_msg_to_row(msg, "#general")
        assert row["source"] == "relay"
        assert row["prompt"] == "[Lead] deploy to prod"
        assert row["session_id"] == "thread1"
        assert row["project"] == "ws_test"
        assert row["timestamp_ms"] > 0

    def test_message_without_thread(self, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")
        msg = {"from_name": "Bot", "text": "hello", "created_at": "2026-03-07T20:13:00Z"}
        row = ai_hist._relay_msg_to_row(msg, "#ops")
        assert row["session_id"] == "#ops"

    def test_message_with_from_id_fallback(self, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")
        msg = {"from_id": "agent-123", "text": "working", "created_at": "2026-03-07T20:13:00Z"}
        row = ai_hist._relay_msg_to_row(msg, "#ch")
        assert "[agent-123]" in row["prompt"]

    def test_empty_text_returns_none(self, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")
        msg = {"from_name": "Bot", "text": "", "created_at": "2026-03-07T20:13:00Z"}
        assert ai_hist._relay_msg_to_row(msg, "#ch") is None

    def test_missing_text_returns_none(self, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")
        msg = {"from_name": "Bot", "created_at": "2026-03-07T20:13:00Z"}
        assert ai_hist._relay_msg_to_row(msg, "#ch") is None

    def test_no_sender(self, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")
        msg = {"text": "anonymous msg", "created_at": "2026-03-07T20:13:00Z"}
        row = ai_hist._relay_msg_to_row(msg, "#ch")
        assert row["prompt"] == "anonymous msg"


class TestSyncRelaycast:
    def test_skips_when_no_env_vars(self, tmp_env, capsys, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "")
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_relaycast(conn, state)
        conn.close()
        captured = capsys.readouterr()
        # Should produce no output — silently skipped
        assert "[relay]" not in captured.out

    def test_sync_channel_messages(self, tmp_env, capsys, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "rk_test_123")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")

        call_log = []
        def mock_get(path, params=None):
            call_log.append(path)
            if path == "channels":
                return {"ok": True, "data": [{"name": "general"}]}
            if path == "channels/general/messages":
                return {"ok": True, "data": [
                    {"id": "m1", "from_name": "Lead", "text": "hello team",
                     "created_at": "2026-03-07T10:00:00Z"},
                    {"id": "m2", "from_name": "Worker", "text": "on it",
                     "created_at": "2026-03-07T10:01:00Z"},
                ]}
            if path == "dm/conversations/all":
                return {"ok": True, "data": []}
            return {"ok": True, "data": []}

        monkeypatch.setattr(ai_hist, "relaycast_get", mock_get)
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_relaycast(conn, state)
        total = conn.execute("SELECT COUNT(*) FROM history WHERE source = 'relay'").fetchone()[0]
        conn.close()
        assert total == 2
        captured = capsys.readouterr()
        assert "+2" in captured.out
        assert "relay" in state

    def test_sync_dm_messages(self, tmp_env, capsys, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "rk_test_123")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")

        def mock_get(path, params=None):
            if path == "channels":
                return {"ok": True, "data": []}
            if path == "dm/conversations/all":
                return {"ok": True, "data": [{"id": "conv1"}]}
            if path == "dm/conversations/conv1/messages":
                return {"ok": True, "data": [
                    {"id": "dm1", "from_name": "Alice", "text": "hey",
                     "created_at": "2026-03-07T10:00:00Z"},
                ]}
            return {"ok": True, "data": []}

        monkeypatch.setattr(ai_hist, "relaycast_get", mock_get)
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_relaycast(conn, state)
        total = conn.execute("SELECT COUNT(*) FROM history WHERE source = 'relay'").fetchone()[0]
        conn.close()
        assert total == 1
        assert "dm:conv1" in state.get("relay", {})

    def test_sync_incremental_with_after(self, tmp_env, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "rk_test_123")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")

        calls = []
        def mock_get(path, params=None):
            calls.append((path, params))
            if path == "channels":
                return {"ok": True, "data": [{"name": "ops"}]}
            if path == "channels/ops/messages":
                return {"ok": True, "data": []}
            if path == "dm/conversations/all":
                return {"ok": True, "data": []}
            return {"ok": True, "data": []}

        monkeypatch.setattr(ai_hist, "relaycast_get", mock_get)
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {"relay": {"ch:ops": "last-known-id"}}
        ai_hist.sync_relaycast(conn, state)
        conn.close()
        # Check that after param was passed
        msg_calls = [(p, pr) for p, pr in calls if "messages" in p]
        assert any(pr and pr.get("after") == "last-known-id" for _, pr in msg_calls)

    def test_sync_handles_api_error(self, tmp_env, capsys, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "rk_test_123")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")

        def mock_get(path, params=None):
            raise urllib.error.URLError("connection refused")

        monkeypatch.setattr(ai_hist, "relaycast_get", mock_get)
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_relaycast(conn, state)
        conn.close()
        captured = capsys.readouterr()
        assert "API error" in captured.out

    def test_sync_handles_dm_403(self, tmp_env, capsys, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "rk_test_123")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")

        def mock_get(path, params=None):
            if path == "channels":
                return {"ok": True, "data": []}
            if path == "dm/conversations/all":
                raise urllib.error.HTTPError(
                    "url", 403, "Forbidden", {}, None)
            return {"ok": True, "data": []}

        monkeypatch.setattr(ai_hist, "relaycast_get", mock_get)
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_relaycast(conn, state)
        conn.close()
        captured = capsys.readouterr()
        assert "+0" in captured.out  # Gracefully handled

    def test_sync_pagination(self, tmp_env, capsys, monkeypatch):
        """Test that pagination continues when 100 messages returned."""
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "rk_test_123")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")

        page1 = [{"id": f"m{i}", "from_name": "Bot", "text": f"msg {i}",
                   "created_at": "2026-03-07T10:00:00Z"} for i in range(100)]
        page2 = [{"id": "m100", "from_name": "Bot", "text": "last msg",
                   "created_at": "2026-03-07T10:01:00Z"}]
        call_count = {"ch": 0}

        def mock_get(path, params=None):
            if path == "channels":
                return {"ok": True, "data": [{"name": "big"}]}
            if path == "channels/big/messages":
                call_count["ch"] += 1
                if call_count["ch"] == 1:
                    return {"ok": True, "data": page1}
                return {"ok": True, "data": page2}
            if path == "dm/conversations/all":
                return {"ok": True, "data": []}
            return {"ok": True, "data": []}

        monkeypatch.setattr(ai_hist, "relaycast_get", mock_get)
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_relaycast(conn, state)
        total = conn.execute("SELECT COUNT(*) FROM history WHERE source = 'relay'").fetchone()[0]
        conn.close()
        assert total == 101
        assert call_count["ch"] == 2

    def test_sync_dm_pagination(self, tmp_env, monkeypatch):
        """Test DM pagination and incremental after."""
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "rk_test_123")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")

        page1 = [{"id": f"dm{i}", "from_name": "A", "text": f"dm {i}",
                   "created_at": "2026-03-07T10:00:00Z"} for i in range(100)]
        page2 = [{"id": "dm100", "from_name": "A", "text": "last",
                   "created_at": "2026-03-07T10:01:00Z"}]
        call_count = {"dm": 0}

        def mock_get(path, params=None):
            if path == "channels":
                return {"ok": True, "data": []}
            if path == "dm/conversations/all":
                return {"ok": True, "data": [{"id": "c1"}]}
            if path == "dm/conversations/c1/messages":
                call_count["dm"] += 1
                if call_count["dm"] == 1:
                    return {"ok": True, "data": page1}
                return {"ok": True, "data": page2}
            return {"ok": True, "data": []}

        monkeypatch.setattr(ai_hist, "relaycast_get", mock_get)
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {"relay": {"dm:c1": "old-id"}}  # incremental
        ai_hist.sync_relaycast(conn, state)
        conn.close()
        assert call_count["dm"] == 2

    def test_sync_skips_empty_conv_id(self, tmp_env, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "rk_test_123")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")

        def mock_get(path, params=None):
            if path == "channels":
                return {"ok": True, "data": []}
            if path == "dm/conversations/all":
                return {"ok": True, "data": [{"id": ""}, {"id": "valid"}]}
            if path == "dm/conversations/valid/messages":
                return {"ok": True, "data": [
                    {"id": "x1", "from_name": "A", "text": "hi",
                     "created_at": "2026-03-07T10:00:00Z"}
                ]}
            return {"ok": True, "data": []}

        monkeypatch.setattr(ai_hist, "relaycast_get", mock_get)
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_relaycast(conn, state)
        total = conn.execute("SELECT COUNT(*) FROM history WHERE source = 'relay'").fetchone()[0]
        conn.close()
        assert total == 1  # Only the valid conv

    def test_sync_handles_sqlite_error_relay(self, tmp_env, capsys, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "rk_test_123")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")

        def mock_get(path, params=None):
            if path == "channels":
                return {"ok": True, "data": [{"name": "ch"}]}
            if path == "channels/ch/messages":
                return {"ok": True, "data": [
                    {"id": "m1", "from_name": "X", "text": "fail",
                     "created_at": "2026-03-07T10:00:00Z"}
                ]}
            if path == "dm/conversations/all":
                return {"ok": True, "data": []}
            return {"ok": True, "data": []}

        monkeypatch.setattr(ai_hist, "relaycast_get", mock_get)
        original_connect = sqlite3.connect

        class FaultyConn:
            def __init__(self, conn):
                self._conn = conn
                self._ready = False
            def executescript(self, sql):
                return self._conn.executescript(sql)
            def execute(self, sql, params=None):
                if sql.startswith("INSERT OR IGNORE") and self._ready and "relay" in str(params):
                    raise sqlite3.OperationalError("simulated")
                result = self._conn.execute(sql, params) if params else self._conn.execute(sql)
                if "PRAGMA" in sql:
                    self._ready = True
                return result
            def commit(self):
                return self._conn.commit()
            def close(self):
                return self._conn.close()

        conn = FaultyConn(sqlite3.connect(str(tmp_env.db_path)))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_relaycast(conn, state)
        conn.close()
        captured = capsys.readouterr()
        assert "1 errors" in captured.out

    def test_sync_dm_empty_after_cursor(self, tmp_env, monkeypatch):
        """Cover the break when DM messages return empty after cursor."""
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "rk_test_123")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")

        def mock_get(path, params=None):
            if path == "channels":
                return {"ok": True, "data": []}
            if path == "dm/conversations/all":
                return {"ok": True, "data": [{"id": "c2"}]}
            if path == "dm/conversations/c2/messages":
                return {"ok": True, "data": []}  # empty → break
            return {"ok": True, "data": []}

        monkeypatch.setattr(ai_hist, "relaycast_get", mock_get)
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {"relay": {"dm:c2": "prev-id"}}
        ai_hist.sync_relaycast(conn, state)
        conn.close()

    def test_sync_dm_sqlite_error(self, tmp_env, capsys, monkeypatch):
        """Cover the sqlite error branch in DM insert path."""
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "rk_test_123")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")

        def mock_get(path, params=None):
            if path == "channels":
                return {"ok": True, "data": []}
            if path == "dm/conversations/all":
                return {"ok": True, "data": [{"id": "c3"}]}
            if path == "dm/conversations/c3/messages":
                return {"ok": True, "data": [
                    {"id": "d1", "from_name": "X", "text": "fail dm",
                     "created_at": "2026-03-07T10:00:00Z"}
                ]}
            return {"ok": True, "data": []}

        monkeypatch.setattr(ai_hist, "relaycast_get", mock_get)

        class FaultyDmConn:
            def __init__(self, conn):
                self._conn = conn
                self._ready = False
            def executescript(self, sql):
                return self._conn.executescript(sql)
            def execute(self, sql, params=None):
                if sql.startswith("INSERT OR IGNORE") and self._ready and "relay" in str(params):
                    raise sqlite3.OperationalError("dm insert fail")
                result = self._conn.execute(sql, params) if params else self._conn.execute(sql)
                if "PRAGMA" in sql:
                    self._ready = True
                return result
            def commit(self):
                return self._conn.commit()
            def close(self):
                return self._conn.close()

        conn = FaultyDmConn(sqlite3.connect(str(tmp_env.db_path)))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_relaycast(conn, state)
        conn.close()
        captured = capsys.readouterr()
        assert "errors" in captured.out

    def test_sync_handles_generic_exception(self, tmp_env, capsys, monkeypatch):
        monkeypatch.setattr(ai_hist, "RELAYCAST_API_KEY", "rk_test_123")
        monkeypatch.setattr(ai_hist, "RELAYCAST_WORKSPACE_ID", "ws_test")

        def mock_get(path, params=None):
            raise RuntimeError("unexpected")

        monkeypatch.setattr(ai_hist, "relaycast_get", mock_get)
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_relaycast(conn, state)
        conn.close()
        captured = capsys.readouterr()
        assert "error" in captured.out.lower()


# ---------------------------------------------------------------------------
# Cursor tests
# ---------------------------------------------------------------------------

class TestParseCursorLine:
    def test_strips_user_query_wrapper(self):
        line = json.dumps({
            "role": "user",
            "message": {"content": [{"type": "text",
                                       "text": "<user_query>\nfix the bug\n</user_query>"}]},
        })
        assert ai_hist.parse_cursor_line(line) == "fix the bug"

    def test_returns_text_without_wrapper(self):
        line = json.dumps({
            "role": "user",
            "message": {"content": [{"type": "text", "text": "plain prompt"}]},
        })
        assert ai_hist.parse_cursor_line(line) == "plain prompt"

    def test_string_content(self):
        line = json.dumps({"role": "user", "message": {"content": "raw string"}})
        assert ai_hist.parse_cursor_line(line) == "raw string"

    def test_skips_assistant_role(self):
        line = json.dumps({
            "role": "assistant",
            "message": {"content": [{"type": "text", "text": "hi"}]},
        })
        assert ai_hist.parse_cursor_line(line) is None

    def test_skips_empty_text(self):
        line = json.dumps({
            "role": "user",
            "message": {"content": [{"type": "text", "text": "   "}]},
        })
        assert ai_hist.parse_cursor_line(line) is None

    def test_skips_when_only_wrapper(self):
        line = json.dumps({
            "role": "user",
            "message": {"content": [{"type": "text",
                                       "text": "<user_query></user_query>"}]},
        })
        assert ai_hist.parse_cursor_line(line) is None

    def test_skips_non_text_content(self):
        line = json.dumps({
            "role": "user",
            "message": {"content": [{"type": "image", "url": "x"}]},
        })
        assert ai_hist.parse_cursor_line(line) is None

    def test_missing_message(self):
        line = json.dumps({"role": "user"})
        assert ai_hist.parse_cursor_line(line) is None


class TestDecodeCursorProject:
    def test_basic(self):
        assert ai_hist._decode_cursor_project(
            "Users-khaliq-Projects-AgentWorkforce"
        ) == "/Users/khaliq/Projects/AgentWorkforce"


class TestSyncTrajectories:
    def _write_runtime_compact(self, tmp_env, run_id="run-1"):
        compacted = tmp_env.trajectory_root / "planner" / "compacted"
        compacted.mkdir(parents=True, exist_ok=True)
        path = compacted / f"{run_id}.json"
        path.write_text(json.dumps({
            "id": run_id,
            "version": 1,
            "personaId": "planner",
            "projectId": "agent-workforce",
            "task": {
                "title": "Retry strategy",
                "description": "Choose retry behavior for API calls.",
            },
            "status": "completed",
            "startedAt": "2026-06-06T10:00:00.000Z",
            "completedAt": "2026-06-06T10:05:00.000Z",
            "decisions": [{
                "question": "How should retries behave?",
                "chosen": "capped exponential backoff",
                "reasoning": "Protect downstream services.",
                "alternatives": ["fixed delay", "no retry"],
            }],
            "retrospective": {
                "summary": "Retry policy selected.",
                "approach": "Compared failure modes.",
                "learnings": ["Bound retries by elapsed time."],
                "confidence": 0.82,
            },
        }))
        return path

    def test_parse_runtime_compacted_trajectory(self, tmp_env):
        path = self._write_runtime_compact(tmp_env)
        row = ai_hist.parse_trajectory_file(path)
        assert row["id"] == "run-1"
        assert row["persona_id"] == "planner"
        assert row["project_id"] == "agent-workforce"
        assert "capped exponential backoff" in row["search_text"]
        assert row["timestamp_ms"] > 0

    def test_sync_imports_runtime_files_and_skips_aggregate_compaction(self, tmp_env, capsys):
        self._write_runtime_compact(tmp_env)
        aggregate_dir = tmp_env.trajectory_root / "compacted"
        aggregate_dir.mkdir(parents=True, exist_ok=True)
        (aggregate_dir / "aggregate.json").write_text(json.dumps({
            "id": "aggregate-1",
            "type": "compacted",
            "sourceTrajectories": ["run-1"],
            "decisionGroups": [],
        }))

        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_trajectories(conn, state)
        trajectory_rows = conn.execute(
            "SELECT id, persona_id, project_id FROM trajectories"
        ).fetchall()
        history_rows = conn.execute(
            "SELECT source, session_id, project, prompt FROM history WHERE source='trajectory'"
        ).fetchall()
        conn.close()

        assert trajectory_rows == [("run-1", "planner", "agent-workforce")]
        assert len(history_rows) == 1
        assert history_rows[0][1] == "run-1"
        assert "Retry strategy" in history_rows[0][3]
        assert "aggregate-1" not in state.get("trajectory", {})
        captured = capsys.readouterr()
        assert "[trajectory] +1 rows" in captured.out


class TestSyncCursor:
    def test_no_cursor_dir(self, tmp_env):
        # cursor_root does not exist by default — should silently no-op
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        ai_hist.sync_cursor(conn, {})
        conn.close()

    def test_imports_user_prompts(self, tmp_env, capsys):
        make_cursor_session(
            tmp_env.cursor_root,
            "Users-me-Projects-foo",
            "75042b11-e498-44a1-a37c-635924134bf2",
            ["first prompt", "second prompt"],
        )
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_cursor(conn, state)
        rows = conn.execute(
            "SELECT session_id, project, prompt FROM history WHERE source='cursor' "
            "ORDER BY prompt"
        ).fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0][0] == "75042b11-e498-44a1-a37c-635924134bf2"
        assert rows[0][1] == "/Users/me/Projects/foo"
        assert rows[0][2] == "first prompt"
        captured = capsys.readouterr()
        assert "[cursor] +2 rows from 1 files" in captured.out

    def test_skips_non_user_messages(self, tmp_env):
        # `make_cursor_session` interleaves assistant lines — verify they're filtered.
        make_cursor_session(tmp_env.cursor_root, "P", "s1", ["only one"])
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        ai_hist.sync_cursor(conn, {})
        count = conn.execute(
            "SELECT COUNT(*) FROM history WHERE source='cursor'"
        ).fetchone()[0]
        conn.close()
        assert count == 1

    def test_incremental_offset(self, tmp_env):
        jsonl = make_cursor_session(tmp_env.cursor_root, "P", "s1", ["one"])
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {}
        ai_hist.sync_cursor(conn, state)
        # Append a new user line.
        with open(jsonl, "a") as f:
            f.write(json.dumps({
                "role": "user",
                "message": {"content": [{"type": "text", "text": "two"}]},
            }) + "\n")
        ai_hist.sync_cursor(conn, state)
        rows = conn.execute(
            "SELECT prompt FROM history WHERE source='cursor' ORDER BY prompt"
        ).fetchall()
        conn.close()
        assert [r[0] for r in rows] == ["one", "two"]

    def test_skips_when_offset_at_eof(self, tmp_env):
        jsonl = make_cursor_session(tmp_env.cursor_root, "P", "s1", ["x"])
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        state = {"cursor": {str(jsonl): jsonl.stat().st_size}}
        ai_hist.sync_cursor(conn, state)
        count = conn.execute(
            "SELECT COUNT(*) FROM history WHERE source='cursor'"
        ).fetchone()[0]
        conn.close()
        assert count == 0

    def test_handles_invalid_json(self, tmp_env, capsys):
        session_dir = tmp_env.cursor_root / "P" / "agent-transcripts" / "s1"
        session_dir.mkdir(parents=True)
        jsonl = session_dir / "s1.jsonl"
        jsonl.write_text(
            "not valid json\n"
            + json.dumps({"role": "user",
                          "message": {"content": [{"type": "text", "text": "ok"}]}}) + "\n"
        )
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        ai_hist.sync_cursor(conn, {})
        rows = conn.execute(
            "SELECT prompt FROM history WHERE source='cursor'"
        ).fetchall()
        conn.close()
        assert rows == [("ok",)]
        captured = capsys.readouterr()
        assert "1 errors" in captured.out

    def test_skips_files_without_matching_jsonl(self, tmp_env):
        # session dir without the expected jsonl file — should be silently ignored.
        (tmp_env.cursor_root / "P" / "agent-transcripts" / "empty").mkdir(parents=True)
        # Also a non-dir entry at the project level.
        tmp_env.cursor_root.mkdir(parents=True, exist_ok=True)
        (tmp_env.cursor_root / "stray-file").write_text("nope")
        # And a project dir with no agent-transcripts subdir.
        (tmp_env.cursor_root / "Q").mkdir()
        # And a non-dir under agent-transcripts.
        (tmp_env.cursor_root / "P" / "agent-transcripts" / "loose.txt").write_text("x")

        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        ai_hist.sync_cursor(conn, {})  # should not raise
        conn.close()

    def test_show_cursor_resume_hint(self, tmp_env, capsys):
        make_cursor_session(
            tmp_env.cursor_root,
            "Users-me-Projects-foo",
            "abc-123",
            ["hello"],
        )
        ai_hist.cmd_sync()
        capsys.readouterr()
        # Find the entry id
        conn = sqlite3.connect(str(tmp_env.db_path))
        eid = conn.execute(
            "SELECT id FROM history WHERE source='cursor'"
        ).fetchone()[0]
        conn.close()
        ai_hist.cmd_show(SimpleNamespace(id=eid))
        captured = capsys.readouterr()
        assert "cursor-agent --resume=abc-123" in captured.out
        assert "cd /Users/me/Projects/foo" in captured.out

    def test_show_cursor_resume_without_project(self, tmp_env, capsys, monkeypatch):
        # Insert a cursor row directly with no project set.
        conn = sqlite3.connect(str(tmp_env.db_path))
        ai_hist.init_db(conn)
        conn.execute(
            "INSERT INTO history (source, session_id, project, prompt, timestamp_ms) "
            "VALUES ('cursor', 'sess-x', NULL, 'q', 1700000000000)"
        )
        conn.commit()
        eid = conn.execute("SELECT id FROM history").fetchone()[0]
        conn.close()
        ai_hist.cmd_show(SimpleNamespace(id=eid))
        captured = capsys.readouterr()
        assert "cursor-agent --resume=sess-x" in captured.out
        assert "cd " not in captured.out


# ---------------------------------------------------------------------------
# Export / Import tests
# ---------------------------------------------------------------------------

class TestCmdExport:
    def test_export_jsonl_to_stdout(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("hello world", 1700000001000, "/proj", "sess1"),
            make_claude_entry("fix the bug", 1700000002000, "/proj", "sess1"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(output=None, format="jsonl", source=None, project=None, since=None)
        ai_hist.cmd_export(args)
        captured = capsys.readouterr()
        lines = [l for l in captured.out.strip().splitlines() if l]
        assert len(lines) == 2
        row = json.loads(lines[0])
        assert row["source"] == "claude"
        assert row["prompt"] == "hello world"
        assert row["session_id"] == "sess1"
        assert "timestamp_ms" in row
        assert "prompt_hash" in row

    def test_export_jsonl_to_file(self, tmp_env, tmp_path, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("export me", 1700000001000, "/proj", "s1"),
        ])
        capsys.readouterr()
        out_file = str(tmp_path / "out.jsonl")
        args = SimpleNamespace(output=out_file, format="jsonl", source=None, project=None, since=None)
        ai_hist.cmd_export(args)
        rows = [json.loads(l) for l in Path(out_file).read_text().splitlines() if l]
        assert len(rows) == 1
        assert rows[0]["prompt"] == "export me"

    def test_export_jsonl_gz(self, tmp_env, tmp_path, capsys):
        import gzip
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("compressed", 1700000001000, "/proj", "s1"),
        ])
        capsys.readouterr()
        out_file = str(tmp_path / "out.jsonl.gz")
        args = SimpleNamespace(output=out_file, format="jsonl", source=None, project=None, since=None)
        ai_hist.cmd_export(args)
        with gzip.open(out_file, "rt", encoding="utf-8") as f:
            rows = [json.loads(l) for l in f if l.strip()]
        assert len(rows) == 1
        assert rows[0]["prompt"] == "compressed"

    def test_export_sqlite(self, tmp_env, tmp_path, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("sqlite export", 1700000001000, "/proj", "s1"),
        ])
        capsys.readouterr()
        out_file = str(tmp_path / "export.db")
        args = SimpleNamespace(output=out_file, format="sqlite", source=None, project=None, since=None)
        ai_hist.cmd_export(args)
        captured = capsys.readouterr()
        assert "1" in captured.out
        conn = sqlite3.connect(out_file)
        rows = conn.execute("SELECT prompt FROM history").fetchall()
        conn.close()
        assert len(rows) == 1
        assert rows[0][0] == "sqlite export"

    def test_export_sqlite_overwrites_existing_file(self, tmp_env, tmp_path, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("current export", 1700000001000, "/proj", "s1"),
        ])
        out_file = str(tmp_path / "export.db")
        stale = sqlite3.connect(out_file)
        ai_hist.init_db(stale)
        stale.execute(
            "INSERT INTO history (source, session_id, project, prompt, prompt_hash, timestamp_ms) "
            "VALUES ('claude', 'old', '/old', 'stale row', 'hash', 1)"
        )
        stale.commit()
        stale.close()
        capsys.readouterr()

        args = SimpleNamespace(output=out_file, format="sqlite", source=None, project=None, since=None)
        ai_hist.cmd_export(args)

        conn = sqlite3.connect(out_file)
        rows = conn.execute("SELECT prompt FROM history ORDER BY prompt").fetchall()
        conn.close()
        assert rows == [("current export",)]

    def test_export_sqlite_refuses_active_db_path(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("do not overwrite active db", 1700000001000, "/proj", "s1"),
        ])
        capsys.readouterr()
        args = SimpleNamespace(
            output=str(tmp_env.db_path), format="sqlite", source=None, project=None, since=None
        )
        with pytest.raises(SystemExit) as exc_info:
            ai_hist.cmd_export(args)
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Refusing to export SQLite over the active AI_HIST_DB" in captured.err

    def test_export_filter_by_source(self, tmp_env, capsys):
        seed_db(tmp_env,
            claude_lines=[make_claude_entry("claude prompt", 1700000001000)],
            codex_lines=[make_codex_entry("codex prompt", 1700000001, "cs1")],
        )
        capsys.readouterr()
        args = SimpleNamespace(output=None, format="jsonl", source="claude", project=None, since=None)
        ai_hist.cmd_export(args)
        captured = capsys.readouterr()
        rows = [json.loads(l) for l in captured.out.strip().splitlines() if l]
        assert all(r["source"] == "claude" for r in rows)
        assert len(rows) == 1

    def test_export_migrates_old_database_without_prompt_hash(self, tmp_env, capsys):
        conn = sqlite3.connect(str(tmp_env.db_path))
        conn.executescript("""\
CREATE TABLE history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT,
    project TEXT,
    prompt TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    UNIQUE(source, timestamp_ms, prompt)
);
CREATE VIRTUAL TABLE history_fts USING fts5(
    prompt, project, content='history', content_rowid='id'
);
CREATE TRIGGER history_ai AFTER INSERT ON history BEGIN
    INSERT INTO history_fts(rowid, prompt, project)
    VALUES (new.id, new.prompt, new.project);
END;
""")
        conn.execute(
            "INSERT INTO history (source, session_id, project, prompt, timestamp_ms) "
            "VALUES ('claude', 's1', '/proj', 'old db export', 1700000001000)"
        )
        conn.commit()
        conn.close()

        args = SimpleNamespace(output=None, format="jsonl", source=None, project=None, since=None)
        ai_hist.cmd_export(args)
        captured = capsys.readouterr()
        row = json.loads(captured.out.strip())
        assert row["prompt"] == "old db export"
        assert row["prompt_hash"] is None

    def test_export_filter_by_since(self, tmp_env, capsys):
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("old entry", 1000000000000, "/p"),   # 2001
            make_claude_entry("new entry", 1700000000000, "/p"),   # 2023
        ])
        capsys.readouterr()
        args = SimpleNamespace(output=None, format="jsonl", source=None, project=None, since="2020-01-01")
        ai_hist.cmd_export(args)
        captured = capsys.readouterr()
        rows = [json.loads(l) for l in captured.out.strip().splitlines() if l]
        assert len(rows) == 1
        assert rows[0]["prompt"] == "new entry"

    def test_export_no_db(self, tmp_env, capsys):
        # Remove the DB
        if tmp_env.db_path.exists():
            tmp_env.db_path.unlink()
        args = SimpleNamespace(output=None, format="jsonl", source=None, project=None, since=None)
        with pytest.raises(SystemExit) as exc_info:
            ai_hist.cmd_export(args)
        assert exc_info.value.code == 1


class TestCmdImport:
    def test_import_jsonl(self, tmp_env, tmp_path, capsys):
        # Write a JSONL export file
        export_file = tmp_path / "import.jsonl"
        rows = [
            {"source": "claude", "session_id": "s1", "project": "/proj",
             "prompt": "imported prompt", "prompt_hash": "abc123", "timestamp_ms": 1700000001000},
        ]
        export_file.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
        capsys.readouterr()
        args = SimpleNamespace(file=str(export_file), dry_run=False)
        ai_hist.cmd_import(args)
        captured = capsys.readouterr()
        assert "+1" in captured.out
        # Verify it's in the DB
        conn = sqlite3.connect(str(tmp_env.db_path))
        found = conn.execute("SELECT prompt FROM history WHERE prompt = 'imported prompt'").fetchone()
        conn.close()
        assert found is not None

    def test_import_dedup(self, tmp_env, tmp_path, capsys):
        export_file = tmp_path / "import.jsonl"
        row = {"source": "claude", "session_id": "s1", "project": "/proj",
               "prompt": "dedup me", "prompt_hash": "abc", "timestamp_ms": 1700000001000}
        export_file.write_text(json.dumps(row) + "\n")
        args = SimpleNamespace(file=str(export_file), dry_run=False)
        # Import twice
        capsys.readouterr()
        ai_hist.cmd_import(args)
        capsys.readouterr()
        ai_hist.cmd_import(args)
        captured = capsys.readouterr()
        assert "+0" in captured.out or "already existed" in captured.out

    def test_import_sqlite(self, tmp_env, tmp_path, capsys):
        # Create a source SQLite DB with entries
        src_db = str(tmp_path / "src.db")
        conn = sqlite3.connect(src_db)
        ai_hist.init_db(conn)
        conn.execute(
            "INSERT INTO history (source, session_id, project, prompt, prompt_hash, timestamp_ms) "
            "VALUES ('codex', 'cs1', '/myproj', 'from sqlite import', 'hash1', 1700000001000)"
        )
        conn.commit()
        conn.close()
        capsys.readouterr()
        args = SimpleNamespace(file=src_db, dry_run=False)
        ai_hist.cmd_import(args)
        captured = capsys.readouterr()
        assert "+1" in captured.out
        conn = sqlite3.connect(str(tmp_env.db_path))
        found = conn.execute("SELECT prompt FROM history WHERE prompt = 'from sqlite import'").fetchone()
        conn.close()
        assert found is not None

    def test_import_dry_run(self, tmp_env, tmp_path, capsys):
        export_file = tmp_path / "dry.jsonl"
        row = {"source": "claude", "session_id": "s1", "project": "/p",
               "prompt": "dry run prompt", "prompt_hash": "x", "timestamp_ms": 1700000001000}
        export_file.write_text(json.dumps(row) + "\n")
        capsys.readouterr()
        args = SimpleNamespace(file=str(export_file), dry_run=True)
        ai_hist.cmd_import(args)
        captured = capsys.readouterr()
        assert "dry-run" in captured.out
        # Nothing written — DB should not have been created
        assert not tmp_env.db_path.exists()

    def test_import_jsonl_gz(self, tmp_env, tmp_path, capsys):
        import gzip
        export_file = str(tmp_path / "import.jsonl.gz")
        row = {"source": "cursor", "session_id": "cs1", "project": "/p",
               "prompt": "gzip import", "prompt_hash": "y", "timestamp_ms": 1700000001000}
        with gzip.open(export_file, "wt", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")
        capsys.readouterr()
        args = SimpleNamespace(file=export_file, dry_run=False)
        ai_hist.cmd_import(args)
        captured = capsys.readouterr()
        assert "+1" in captured.out

    def test_import_missing_prompt_hash_backfilled(self, tmp_env, tmp_path, capsys):
        # Rows without prompt_hash (older export format) should still import
        export_file = tmp_path / "old.jsonl"
        row = {"source": "claude", "session_id": "s1", "project": "/p",
               "prompt": "no hash row", "timestamp_ms": 1700000001000}
        export_file.write_text(json.dumps(row) + "\n")
        capsys.readouterr()
        args = SimpleNamespace(file=str(export_file), dry_run=False)
        ai_hist.cmd_import(args)
        conn = sqlite3.connect(str(tmp_env.db_path))
        found = conn.execute(
            "SELECT prompt_hash FROM history WHERE prompt = 'no hash row'"
        ).fetchone()
        conn.close()
        assert found is not None
        assert found[0] is not None  # hash was backfilled

    def test_roundtrip_jsonl(self, tmp_env, tmp_path, capsys):
        """Export then import into a fresh DB produces identical entries."""
        seed_db(tmp_env, claude_lines=[
            make_claude_entry("roundtrip test", 1700000001000, "/proj", "sess-rt"),
        ])
        export_file = str(tmp_path / "rt.jsonl")
        capsys.readouterr()
        ai_hist.cmd_export(SimpleNamespace(
            output=export_file, format="jsonl", source=None, project=None, since=None
        ))
        # Import into a fresh DB
        fresh_db = str(tmp_path / "fresh.db")
        monkeypatch_db = patch.object(ai_hist, "DB_PATH", Path(fresh_db))
        with monkeypatch_db:
            capsys.readouterr()
            ai_hist.cmd_import(SimpleNamespace(file=export_file, dry_run=False))
            captured = capsys.readouterr()
            assert "+1" in captured.out
            conn = sqlite3.connect(fresh_db)
            row = conn.execute("SELECT source, session_id, project, prompt FROM history").fetchone()
            conn.close()
        assert row[0] == "claude"
        assert row[1] == "sess-rt"
        assert row[2] == "/proj"
        assert row[3] == "roundtrip test"


# ---------------------------------------------------------------------------
# Session helpers and Claude session scanning
# ---------------------------------------------------------------------------

def make_claude_session_jsonl(project_dir: Path, session_id: str, git_branch: str,
                               cwd: str, prompts: list, last_assistant: str = None) -> Path:
    """Create a fake Claude per-session JSONL file."""
    project_dir.mkdir(parents=True, exist_ok=True)
    jsonl = project_dir / f"{session_id}.jsonl"
    lines = []
    # First line: a typical attachment entry that carries session metadata.
    lines.append(json.dumps({
        "type": "attachment",
        "sessionId": session_id,
        "gitBranch": git_branch,
        "cwd": cwd,
        "timestamp": "2024-01-01T00:00:00.000Z",
    }))
    for i, prompt in enumerate(prompts):
        lines.append(json.dumps({
            "type": "user",
            "sessionId": session_id,
            "gitBranch": git_branch,
            "cwd": cwd,
            "timestamp": f"2024-01-01T00:0{i}:01.000Z",
            "message": {"role": "user", "content": prompt},
        }))
    if last_assistant:
        lines.append(json.dumps({
            "type": "assistant",
            "sessionId": session_id,
            "gitBranch": git_branch,
            "cwd": cwd,
            "timestamp": "2024-01-01T01:00:00.000Z",
            "message": {"role": "assistant", "content": [{"type": "text", "text": last_assistant}]},
        }))
    jsonl.write_text("\n".join(lines) + "\n")
    return jsonl


class TestUpsertSession:
    def test_insert(self, tmp_path):
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        ai_hist.init_db(conn)
        ai_hist._upsert_session(conn, "sid1", "claude", "/proj", "main", 1000)
        conn.commit()
        row = conn.execute(
            "SELECT session_id, source, cwd, git_branch, first_activity_ms, last_activity_ms "
            "FROM sessions WHERE session_id='sid1'"
        ).fetchone()
        conn.close()
        assert row == ("sid1", "claude", "/proj", "main", 1000, 1000)

    def test_upsert_preserves_existing_cwd(self, tmp_path):
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        ai_hist.init_db(conn)
        ai_hist._upsert_session(conn, "sid1", "claude", "/proj", "main", 1000)
        # Second upsert with NULL cwd should keep existing.
        ai_hist._upsert_session(conn, "sid1", "claude", None, None, 2000)
        conn.commit()
        row = conn.execute("SELECT cwd, git_branch FROM sessions WHERE session_id='sid1'").fetchone()
        conn.close()
        assert row == ("/proj", "main")

    def test_upsert_updates_last_activity_ms(self, tmp_path):
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        ai_hist.init_db(conn)
        ai_hist._upsert_session(conn, "sid1", "claude", "/proj", "main", 1000)
        ai_hist._upsert_session(conn, "sid1", "claude", "/proj", "main", 5000)
        conn.commit()
        row = conn.execute(
            "SELECT first_activity_ms, last_activity_ms FROM sessions WHERE session_id='sid1'"
        ).fetchone()
        conn.close()
        assert row[0] == 1000  # first stays
        assert row[1] == 5000  # last updated

    def test_upsert_last_assistant_text(self, tmp_path):
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        ai_hist.init_db(conn)
        ai_hist._upsert_session(conn, "sid1", "claude", "/proj", "main", 1000,
                                 last_assistant_text="done!")
        conn.commit()
        row = conn.execute("SELECT last_assistant_text FROM sessions WHERE session_id='sid1'").fetchone()
        conn.close()
        assert row[0] == "done!"


class TestScanClaudeSessionFile:
    def test_basic_metadata(self, tmp_path):
        proj_dir = tmp_path / "proj"
        jsonl = make_claude_session_jsonl(proj_dir, "sess-abc", "feat/auth", "/app", ["fix bug"])
        meta = ai_hist._scan_claude_session_file(jsonl)
        assert meta is not None
        assert meta["session_id"] == "sess-abc"
        assert meta["git_branch"] == "feat/auth"
        assert meta["cwd"] == "/app"
        assert meta["raw_path"] == str(jsonl)

    def test_last_assistant_text(self, tmp_path):
        proj_dir = tmp_path / "proj"
        jsonl = make_claude_session_jsonl(proj_dir, "sess-abc", "main", "/app",
                                          ["do X"], last_assistant="Done, I updated auth.ts")
        meta = ai_hist._scan_claude_session_file(jsonl)
        assert meta is not None
        assert "auth.ts" in meta["last_assistant_text"]

    def test_no_session_id_returns_none(self, tmp_path):
        f = tmp_path / "unknown.jsonl"
        f.write_text(json.dumps({"type": "mode", "mode": "normal"}) + "\n")
        assert ai_hist._scan_claude_session_file(f) is None

    def test_missing_file_returns_none(self, tmp_path):
        assert ai_hist._scan_claude_session_file(tmp_path / "missing.jsonl") is None


class TestSyncClaudeSessions:
    def test_populates_sessions_table(self, tmp_env, capsys):
        proj_dir = tmp_env.claude_projects_root / "proj"
        make_claude_session_jsonl(proj_dir, "sess-1", "main", "/app", ["do work"])
        ai_hist.cmd_sync()
        conn = sqlite3.connect(str(tmp_env.db_path))
        row = conn.execute(
            "SELECT session_id, source, cwd, git_branch FROM sessions WHERE session_id='sess-1'"
        ).fetchone()
        conn.close()
        assert row == ("sess-1", "claude", "/app", "main")

    def test_incremental_scan_skips_unchanged_files(self, tmp_env, capsys):
        proj_dir = tmp_env.claude_projects_root / "proj"
        make_claude_session_jsonl(proj_dir, "sess-1", "main", "/app", ["do work"])
        ai_hist.cmd_sync()
        capsys.readouterr()
        ai_hist.cmd_sync()
        captured = capsys.readouterr()
        # Second sync should report no new files scanned.
        assert "scanned 1" not in captured.out

    def test_last_assistant_text_stored(self, tmp_env):
        proj_dir = tmp_env.claude_projects_root / "proj"
        make_claude_session_jsonl(proj_dir, "sess-2", "feat/x", "/app",
                                  ["do Y"], last_assistant="Updated src/auth.ts")
        ai_hist.cmd_sync()
        conn = sqlite3.connect(str(tmp_env.db_path))
        row = conn.execute(
            "SELECT last_assistant_text FROM sessions WHERE session_id='sess-2'"
        ).fetchone()
        conn.close()
        assert row[0] is not None
        assert "auth.ts" in row[0]


class TestReadCodexSessionMetaBranch:
    def test_returns_branch(self, tmp_path):
        rollout = tmp_path / "rollout-1.jsonl"
        rollout.write_text(json.dumps({
            "type": "session_meta",
            "payload": {
                "id": "codex-sess-1",
                "cwd": "/my/project",
                "git": {"branch": "feat/payments"},
            },
        }) + "\n")
        result = ai_hist._read_codex_session_meta(rollout)
        assert result == ("codex-sess-1", "/my/project", "feat/payments")

    def test_no_git_returns_none_branch(self, tmp_path):
        rollout = tmp_path / "rollout-2.jsonl"
        rollout.write_text(json.dumps({
            "type": "session_meta",
            "payload": {"id": "codex-sess-2", "cwd": "/proj"},
        }) + "\n")
        result = ai_hist._read_codex_session_meta(rollout)
        assert result == ("codex-sess-2", "/proj", None)

    def test_non_session_meta_returns_none(self, tmp_path):
        rollout = tmp_path / "rollout-3.jsonl"
        rollout.write_text(json.dumps({"type": "other", "payload": {}}) + "\n")
        assert ai_hist._read_codex_session_meta(rollout) is None
