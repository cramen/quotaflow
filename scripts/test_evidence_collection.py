#!/usr/bin/env python3
"""Exercise the real evidence collector's source and report freshness boundary."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from candidate_identity import source_identity

ROOT = Path(__file__).resolve().parents[1]


class CollectorTest(unittest.TestCase):
    def setUp(self):
        parent = ROOT / "build/reports/collector-controls"
        parent.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=parent)
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.report = self.directory / "input.json"
        self.candidate = {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                          "sourceSha256": source_identity(ROOT)[1], "version": "control", "artifacts": {"control.jar": "a" * 64}}
        self.identity = self.directory / "candidate.json"

    def collect(self, command):
        self.identity.write_text(json.dumps(self.candidate))
        return subprocess.run([sys.executable, str(ROOT / "scripts/run_evidence_stage.py"),
            "--candidate", str(self.identity), "--kind", "negative-controls",
            "--output", str(self.directory / "stage"), "--report", str(self.report.relative_to(ROOT)), "--", *command],
            cwd=ROOT, capture_output=True, text=True)

    def test_captures_a_fresh_successful_invocation(self):
        result = self.collect([sys.executable, "-c", "from pathlib import Path; import sys; Path(sys.argv[1]).write_text('{}')", str(self.report)])
        self.assertEqual(0, result.returncode, result.stderr)
        stage = json.loads((self.directory / "stage/stage.json").read_text())
        self.assertEqual(self.candidate, stage["candidate"])
        self.assertEqual(0, stage["exitCode"])
        self.assertEqual(1, len(stage["reports"]))

    def test_refuses_report_left_by_an_earlier_run(self):
        import os
        self.report.write_text("{}"); os.utime(self.report, (1, 1))
        result = self.collect([sys.executable, "-c", "pass"])
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Stale report", result.stderr)
        self.assertFalse((self.directory / "stage/stage.json").exists())

    def test_source_mismatch_refuses_to_start_the_command(self):
        self.candidate["sourceSha256"] = "0" * 64
        result = self.collect([sys.executable, "-c", "from pathlib import Path; import sys; Path(sys.argv[1]).write_text('ran')", str(self.report)])
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Source changed", result.stderr)
        self.assertFalse(self.report.exists())

    def test_absent_reports_cannot_be_collected(self):
        result = self.collect([sys.executable, "-c", "pass"])
        self.assertNotEqual(0, result.returncode)
        self.assertIn("no reports", result.stderr)


if __name__ == "__main__":
    unittest.main()
