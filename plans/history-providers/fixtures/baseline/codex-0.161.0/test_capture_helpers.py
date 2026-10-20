"""Offline regressions for capture transport and sanitized artifact links."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from rpc_reader import RpcReplies

ROOT = Path(__file__).resolve().parent


class CaptureHelpers(unittest.TestCase):
    def test_coalesced_notification_and_replies_are_drained(self):
        # One underlying write can fill TextIOWrapper's buffer with several
        # records. No further pipe readability event is required to read them.
        process = subprocess.Popen(
            [sys.executable, "-c", "import os,time; os.write(1,b'"
             '{"method":"thread/started"}\\n{"id":1,"result":"first"}\\n'
             '{"id":2,"result":"second"}\\n' "'); time.sleep(1)"],
            stdout=subprocess.PIPE, text=True,
        )
        try:
            replies = RpcReplies(process.stdout)
            observed = []
            self.assertEqual(replies.response(1, 0.5, observed.append), "first")
            self.assertEqual(replies.response(2, 0.5, observed.append), "second")
            self.assertEqual(observed[0]["method"], "thread/started")
        finally:
            process.terminate()
            process.wait(timeout=5)
            process.stdout.close()

    def sanitize_reader(self, capture_date):
        native = "11111111-1111-4111-8111-111111111111"
        with tempfile.TemporaryDirectory() as temporary:
            source, output = Path(temporary) / "raw", Path(temporary) / "clean"
            source.mkdir()
            (source / "native-id.txt").write_text(native)
            row = {"timestamp": f"{capture_date}T06:30:00.123+05:30", "type": "session_meta",
                   "payload": {"id": native, "cwd": "/tmp/synthetic/workspace"}}
            (source / "rollout-0.jsonl").write_text(json.dumps(row) + "\n")
            filename = f"rollout-{capture_date}T06-30-00-{native}.jsonl"
            date_path = capture_date.replace("-", "/")
            reader = {"id": native,
                      "path": f"/tmp/synthetic/codex/sessions/{date_path}/{filename}",
                      "serverName": "test-machine.invalid", "current_date": capture_date,
                      "timestamp": f"{capture_date}T06:30:00.123+05:30"}
            for name in ("reader", "items", "turns", "reopened-items", "resumed-items", "rpc-transcript"):
                (source / f"{name}.json").write_text(json.dumps(reader))
            script = Path(os.environ.get("SANITIZER_SCRIPT", ROOT / "sanitize.py"))
            subprocess.run([sys.executable, str(script), str(source), str(output)],
                           check=True, capture_output=True)
            return (json.loads((output / "reader.json").read_text()),
                    json.loads((output / "session.jsonl").read_text()))

    def test_sanitized_reader_preserves_rollout_filename_and_identity(self):
        stable = "019b76da-a800-7000-8000-000000000161"
        actual, row = self.sanitize_reader("2026-10-15")
        self.assertEqual(actual["id"], stable)
        self.assertEqual(Path(actual["path"]).name,
                         f"rollout-2026-01-01T00-00-00-{stable}.jsonl")
        self.assertIn("/sessions/2026/01/01/", actual["path"])
        self.assertEqual(row["payload"]["id"], stable)

    def test_arbitrary_recapture_dates_and_hostname_are_synthetic(self):
        for date in ("2026-10-15", "2026-11-02", "2027-02-03"):
            with self.subTest(date=date):
                actual, row = self.sanitize_reader(date)
                self.assertEqual(actual["serverName"], "synthetic-host.local")
                self.assertEqual(actual["current_date"], "2026-01-01")
                self.assertEqual(actual["timestamp"], "2026-01-01T00:00:00.000Z")
                self.assertEqual(row["timestamp"], "2026-01-01T00:00:00.000Z")
                self.assertIn("/sessions/2026/01/01/", actual["path"])
                self.assertNotIn(date, actual["path"])


if __name__ == "__main__":
    unittest.main()
