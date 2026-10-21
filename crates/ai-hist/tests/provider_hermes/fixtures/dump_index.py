"""Dump the captured pre-fix ledger, preserving its native evidence and schema."""
import pathlib
import sqlite3
import sys

capture = pathlib.Path(sys.argv[1]).resolve()
output = pathlib.Path(sys.argv[2])
with sqlite3.connect(capture / "legacy-index.db") as connection:
    # Python's iterdump emits both virtual FTS content and the shadow index.
    # Only shadow storage belongs in a restore; inserting virtual rows before
    # reopening the writable_schema declarations fails and duplicates content.
    statements = [
        statement for statement in connection.iterdump()
        if not statement.startswith((
            'INSERT INTO "history_fts" VALUES',
            'INSERT INTO "session_events_fts" VALUES',
        ))
    ]
text = "PRAGMA foreign_keys=OFF;\n" + "\n".join(statements) + "\n"
text = text.replace(str(capture / "home"), "/synthetic/hermes-pre-intake-home")
output.write_text(text)
