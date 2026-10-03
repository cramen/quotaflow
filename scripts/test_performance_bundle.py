#!/usr/bin/env python3
"""Synthetic negative controls for the local-only performance evidence gate."""
import hashlib
import os
import subprocess
import sys
import json
from pathlib import Path
import tempfile
import unittest
from verify_performance_bundle import ALGORITHMS, PATHS, digest, verify


class BundleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.identity = {"commit": "a" * 40, "sourceSha256": "a" * 64, "version": "1.0-test",
                         "artifacts": {"core.jar": "b" * 64}}
        profile = {"machine": "synthetic-negative-control"}
        profile_hash = hashlib.sha256(json.dumps(profile, sort_keys=True).encode()).hexdigest()
        baseline = dict(self.identity, commit="b" * 40, sourceSha256="c" * 64)
        self.bundle = {"schemaVersion": 1, "execution": "local-only", "candidate": self.identity,
                       "baseline": baseline, "profile": profile_hash, "workloadSha256": "d" * 64,
                       "startedAt": "2026-01-02T00:00:00Z", "runs": {"baseline": [], "candidate": []}}
        self.bundle["profileData"] = self.write("profile.json", profile)
        self.bundle["baselineReview"] = self.write("review.json", {
            "baseline": baseline, "profile": profile_hash, "workloadSha256": "d" * 64,
            "reviewer": "maintainer", "reason": "Last accepted runtime version on the same profile",
            "reviewedAt": "2026-01-01T00:00:00Z"})
        self.review_digest = self.bundle["baselineReview"]["sha256"]
        for group_index, group in enumerate(("baseline", "candidate")):
            for index in range(3):
                number = group_index * 3 + index
                data = []
                for path in PATHS:
                    for algorithm in ALGORITHMS:
                        for mode in ("thrpt", "sample"):
                            data.append({"benchmark": "io.quotaflow.verification.CertificationBenchmark." + ("throughput" if mode == "thrpt" else "latency"), "mode": mode,
                                "params": {"path": path, "algorithm": algorithm}, "jmhVersion": "1.37",
                                "jdkVersion": "21", "vmName": "OpenJDK", "vmVersion": "21", "jvmArgs": [],
                                "threads": 1, "forks": 1, "warmupIterations": 2, "warmupTime": "1 s",
                                "measurementIterations": 3, "measurementTime": "1 s",
                                "primaryMetric": {"scoreUnit": "ops/s" if mode == "thrpt" else "ms/op",
                                    "score": 100, "scorePercentiles": {"99.0": 1}}})
                self.bundle["runs"][group].append({"profile": profile_hash, "workloadSha256": "d" * 64,
                    "identity": self.bundle[group], "pid": number + 1,
                    "startedAt": f"2026-01-02T00:{number:02}:00Z", "endedAt": f"2026-01-02T00:{number:02}:59Z",
                    "jmh": self.write(f"{number}.json", data), "log": self.write(f"{number}.log", "complete"),
                    "before": self.write(f"{number}-before.json", {"profile": profile_hash, "stable": True}),
                    "after": self.write(f"{number}-after.json", {"profile": profile_hash, "stable": True})})

    def write(self, name, content):
        path = self.root / name
        path.write_text(json.dumps(content))
        return {"path": name, "sha256": digest(path)}

    def check(self, **kwargs):
        manifest = self.write("manifest.json", self.bundle)
        return verify(self.root, manifest["sha256"], self.identity, self.review_digest, **kwargs)

    def modify(self, run, function):
        path = self.root / run["jmh"]["path"]
        content = json.loads(path.read_text())
        function(content)
        run["jmh"] = self.write(path.name, content)

    def test_complete_comparable_bundle_passes(self):
        self.assertEqual("PASS", self.check()["comparison"])

    def test_repeatable_regression_blocks(self):
        for run in self.bundle["runs"]["candidate"]:
            self.modify(run, lambda data: data[0]["primaryMetric"].update(score=89))
        self.assertEqual("REGRESSION", self.check()["comparison"])

    def test_exact_ten_percent_is_allowed(self):
        for run in self.bundle["runs"]["candidate"]:
            self.modify(run, lambda data: data[0]["primaryMetric"].update(score=90))
        self.assertEqual("PASS", self.check()["comparison"])

    def test_noisy_boundary_is_inconclusive(self):
        self.modify(self.bundle["runs"]["candidate"][0], lambda data: data[0]["primaryMetric"].update(score=80))
        self.assertEqual("INCONCLUSIVE", self.check()["comparison"])

    def test_diagnostic_never_certifies(self):
        self.assertEqual("NOT ASSESSED", self.check(diagnostic=True)["comparison"])

    def test_missing_runs_and_paths_fail(self):
        self.bundle["runs"]["candidate"].pop()
        with self.assertRaises(ValueError): self.check()

    def test_missing_metric_fails(self):
        self.modify(self.bundle["runs"]["candidate"][0], lambda data: data.pop())
        with self.assertRaises(ValueError): self.check()

    def test_profile_mismatch_fails(self):
        self.bundle["runs"]["candidate"][0]["profile"] = "different"
        with self.assertRaises(ValueError): self.check()

    def test_tampered_raw_result_fails(self):
        (self.root / self.bundle["runs"]["candidate"][0]["jmh"]["path"]).write_text("[]")
        with self.assertRaises(ValueError): self.check()

    def test_stale_identity_fails(self):
        self.bundle["candidate"] = dict(self.identity, commit="f" * 40)
        with self.assertRaises(ValueError): self.check()

    def test_baseline_replacement_fails(self):
        self.review_digest = "0" * 64
        with self.assertRaises(ValueError): self.check()

    def test_in_release_review_fails(self):
        review = json.loads((self.root / "review.json").read_text())
        review["reviewedAt"] = "2026-01-03T00:00:00Z"
        self.bundle["baselineReview"] = self.write("review.json", review)
        self.review_digest = self.bundle["baselineReview"]["sha256"]
        with self.assertRaises(ValueError): self.check()

    def test_overlapping_runs_fail(self):
        self.bundle["runs"]["candidate"][0]["startedAt"] = "2026-01-02T00:00:30Z"
        with self.assertRaises(ValueError): self.check()

    def test_path_escape_fails(self):
        self.bundle["runs"]["candidate"][0]["log"]["path"] = "../outside"
        with self.assertRaises(ValueError): self.check()

    def test_missing_artifact_fails(self):
        (self.root / self.bundle["runs"]["candidate"][0]["log"]["path"]).unlink()
        with self.assertRaises(ValueError): self.check()

    def test_jmh_configuration_mismatch_fails(self):
        self.modify(self.bundle["runs"]["candidate"][0], lambda data: data[0].update(threads=2))
        with self.assertRaises(ValueError): self.check()

    def test_github_cannot_start_the_local_runner(self):
        command = [sys.executable, str(Path(__file__).with_name("run_local_benchmarks.py")),
                   "--profile", "missing", "--baseline-review", "missing", "--candidate", "missing",
                   "--baseline-jar", "missing", "--candidate-jar", "missing", "--java", "missing",
                   "--host-probe", "missing", "--redis-url", "redis://localhost", "--output", str(self.root / "must-not-exist")]
        result = subprocess.run(command, env=dict(os.environ, GITHUB_ACTIONS="true"), capture_output=True, text=True)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("prohibited", result.stderr)
        self.assertFalse((self.root / "must-not-exist").exists())


if __name__ == "__main__":
    unittest.main()
