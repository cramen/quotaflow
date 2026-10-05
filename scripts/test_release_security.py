"""Negative controls for current database evidence and exact reachability decisions."""
import copy
import datetime as dt
from pathlib import Path
import tempfile
import unittest

from release_common import sha256
from scan_release import artifact_set, evaluate


class SecurityGateTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="quotaflow-security-"); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); (self.root / "proof.txt").write_text("Reviewed call graph and executable reachability evidence")
        self.now = dt.datetime(2026, 10, 5, 12, tzinfo=dt.timezone.utc)
        self.policy = {"maximumDatabaseAgeHours": 24, "maximumReportAgeHours": 24, "maximumTriageDays": 30}
        self.candidate = {"commit": "a" * 40, "version": "1.0.0", "sourceSha256": "b" * 64, "artifacts": {"core.jar": "c" * 64}}
        self.database = {"valid": True, "built": (self.now - dt.timedelta(hours=1)).isoformat(), "schemaVersion": "v6"}
        self.raw = {"descriptor": {"name": "grype", "version": "0.120.0", "timestamp": self.now.isoformat(),
                    "configuration": {}, "db": {"status": copy.deepcopy(self.database)}}, "matches": []}
        self.finding = {"vulnerability": {"id": "GHSA-abcd-1234-abcd", "severity": "Low"}, "artifact": {"purl": "pkg:maven/example/dependency@1.0.0"}}
        self.review = {"advisory": self.finding["vulnerability"]["id"], "purl": self.finding["artifact"]["purl"],
            "candidateArtifactsSha256": artifact_set(self.candidate), "decision": "not_reachable", "owner": "Security reviewer",
            "rationale": "The affected entry point is absent from every supported public execution path.",
            "reviewedAt": self.now.isoformat(), "expiresAt": (self.now + dt.timedelta(days=7)).isoformat(),
            "evidence": [{"path": "proof.txt", "sha256": sha256(self.root / "proof.txt")} ]}

    def gate(self, reviews=()):
        return evaluate(self.raw, self.database, self.candidate, reviews, self.policy, self.now, self.root)

    def test_fresh_empty_scan_passes(self):
        self.assertEqual("PASS", self.gate()["status"])

    def test_every_severity_blocks_without_exact_review(self):
        for severity in ("Critical", "High", "Medium", "Low", "Negligible", "Unknown"):
            self.finding["vulnerability"]["severity"] = severity; self.raw["matches"] = [self.finding]
            self.assertEqual("BLOCKED", self.gate()["status"])

    def test_reachable_and_unknown_reviews_do_not_waive_findings(self):
        self.raw["matches"] = [self.finding]
        for decision in ("reachable", "unknown"):
            self.review["decision"] = decision
            self.assertEqual("BLOCKED", self.gate([self.review])["status"])

    def test_current_exact_evidenced_nonreachability_passes(self):
        self.raw["matches"] = [self.finding]
        self.assertEqual("PASS", self.gate([self.review])["status"])

    def test_expired_review_and_different_library_bytes_fail(self):
        self.raw["matches"] = [self.finding]
        review = copy.deepcopy(self.review); review["expiresAt"] = (self.now - dt.timedelta(seconds=1)).isoformat()
        with self.assertRaisesRegex(ValueError, "Expired"): self.gate([review])
        review = copy.deepcopy(self.review); review["candidateArtifactsSha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "different library"): self.gate([review])

    def test_review_for_other_version_does_not_apply(self):
        self.raw["matches"] = [self.finding]; self.review["purl"] = "pkg:maven/example/dependency@2.0.0"
        self.assertEqual("BLOCKED", self.gate([self.review])["status"])

    def test_blanket_rule_and_tampered_evidence_fail(self):
        self.raw["matches"] = [self.finding]
        review = copy.deepcopy(self.review); review["purl"] = "pkg:maven/example/dependency@*"
        with self.assertRaisesRegex(ValueError, "exact package"): self.gate([review])
        (self.root / "proof.txt").write_text("changed")
        with self.assertRaisesRegex(ValueError, "digest"): self.gate([self.review])

    def test_stale_unavailable_future_and_mismatched_databases_fail(self):
        original = copy.deepcopy(self.database)
        for value in (self.now - dt.timedelta(hours=25), self.now + dt.timedelta(hours=1)):
            self.database["built"] = value.isoformat()
            with self.assertRaises(ValueError): self.gate()
        self.database = copy.deepcopy(original); self.database["valid"] = False
        with self.assertRaises(ValueError): self.gate()
        self.database = original; self.raw["descriptor"]["db"]["status"]["schemaVersion"] = "other"
        with self.assertRaisesRegex(ValueError, "identity"): self.gate()

    def test_absent_filtered_or_stale_report_is_not_empty_findings(self):
        del self.raw["matches"]
        with self.assertRaisesRegex(ValueError, "Missing vulnerability"): self.gate()
        self.raw["matches"] = []; self.raw["ignoredMatches"] = [self.finding]
        with self.assertRaisesRegex(ValueError, "Suppressed"): self.gate()
        del self.raw["ignoredMatches"]; self.raw["descriptor"]["configuration"]["only-fixed"] = True
        with self.assertRaisesRegex(ValueError, "filtering"): self.gate()
        self.raw["descriptor"]["configuration"] = {}; self.raw["descriptor"]["timestamp"] = (self.now - dt.timedelta(days=2)).isoformat()
        with self.assertRaisesRegex(ValueError, "Stale"): self.gate()


if __name__ == "__main__":
    unittest.main()
