"""Exercise the real Gradle verifier using synthetic evidence, without running JMH.

Run: python3 scripts/test_benchmark_verification.py
Python is only required for this verifier test utility, not the library/build.
"""
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class BenchmarkVerificationTest(unittest.TestCase):
    def test_evidence_contract(self):
        with tempfile.TemporaryDirectory(prefix="quotaflow-benchmark-verifier-") as folder:
            directory = Path(folder)
            results = directory / "results.json"
            profile = directory / "profile.json"
            baseline = directory / "baseline.json"
            fields = ("machine", "cpu", "memory", "os", "jdk", "server", "network", "workload")
            profile_data = {field: "synthetic-" + field for field in fields}
            profile.write_text(json.dumps(profile_data))
            common = dict(jmhVersion="1.36", jdkVersion="17", vmName="test", vmVersion="17",
                          jvmArgs=[], threads=8, forks=1, warmupIterations=2, warmupTime="10 s",
                          warmupBatchSize=1, measurementIterations=3, measurementTime="10 s",
                          measurementBatchSize=1)
            entries = []
            for method, mode, unit, score in [("chainAllowThroughput", "thrpt", "ops/s", 100),
                                               ("chainAllowLatency", "sample", "ms/op", 50)]:
                entries.append(dict(common, benchmark="io.quotaflow.store.redis.RedisChainBenchmark." + method,
                                    mode=mode, primaryMetric=dict(score=score, scoreUnit=unit,
                                                                 scorePercentiles={"99.0": score})))

            def run(label, data, options=(), expected=None):
                results.write_text(json.dumps(data))
                command = [str(ROOT / "gradlew"), ":quotaflow-store-redis:benchmarkGate",
                           "-x", ":quotaflow-store-redis:jmh", "--console=plain",
                           f"-PbenchmarkResultsFile={results}", f"-PbenchmarkProfileFile={profile}", *options]
                completed = subprocess.run(command, cwd=ROOT, text=True, stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT, timeout=120)
                if expected:
                    self.assertNotEqual(completed.returncode, 0, completed.stdout)
                    self.assertIn(expected, completed.stdout)
                else:
                    self.assertEqual(completed.returncode, 0, completed.stdout)
                print(label + ": passed", flush=True)
                return json.loads((directory / "verification.json").read_text())

            report = run("Low throughput/high latency are diagnostic", entries)
            self.assertEqual(report["comparison"], "NOT ASSESSED")
            baseline.write_text(json.dumps(report))
            comparison = [f"-PbenchmarkBaselineFile={baseline}", "-PrequireBenchmarkComparison"]
            report = run("Matching evidence passes", entries, comparison)
            self.assertEqual(report["comparison"], "PASS")
            changed = copy.deepcopy(entries)
            changed[0]["primaryMetric"]["score"] = 89
            run("Throughput regression blocks", changed, comparison, "regression check failed")
            changed = copy.deepcopy(entries)
            changed[1]["primaryMetric"]["scorePercentiles"]["99.0"] = 56
            run("Latency regression blocks", changed, comparison, "regression check failed")
            changed = copy.deepcopy(entries)
            changed[0]["threads"] = 4
            run("Workload mismatch blocks", changed, comparison, "configuration mismatch")
            profile.write_text(json.dumps(dict(profile_data, machine="another-machine")))
            run("Hardware mismatch blocks", entries, comparison, "configuration mismatch")
            profile.write_text(json.dumps(profile_data))
            run("Missing required baseline blocks", entries, ["-PrequireBenchmarkComparison"], "no explicit baseline")
            changed = copy.deepcopy(entries)
            changed[0]["primaryMetric"]["scoreUnit"] = "ops/ms"
            run("Wrong units block", changed, expected="Wrong JMH mode/unit")
            changed = copy.deepcopy(entries)
            changed[0]["primaryMetric"]["score"] = "NaN"
            run("Non-finite data blocks", changed, expected="Invalid JMH measurement")
            run("Missing metric blocks", entries[:1], expected="Missing or duplicate")
            final = run("Duplicate metric blocks", entries + entries[:1], expected="Missing or duplicate")
            self.assertEqual(final["comparison"], "FAILED")


if __name__ == "__main__":
    unittest.main()
