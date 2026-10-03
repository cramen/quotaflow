"""Negative controls for build-only verification; invokes real JVMs and Gradle."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class RuntimeGuardTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="quotaflow-runtime-")
        cls.root = Path(cls.directory.name)
        cls.classes = cls.root / "classes"
        cls.classes.mkdir()
        subprocess.run(["javac", "--release", "17", "-d", str(cls.classes),
                        str(ROOT / "gradle/verification/RuntimeGuard.java")], check=True)
        manifest = cls.root / "MANIFEST.MF"
        manifest.write_text("Manifest-Version: 1.0\nPremain-Class: io.quotaflow.verification.RuntimeGuard\n\n")
        cls.jar = cls.root / "guard.jar"
        subprocess.run(["jar", "--create", "--file", str(cls.jar), "--manifest", str(manifest),
                        "-C", str(cls.classes), "."], check=True)
        properties = subprocess.run(["java", "-XshowSettings:properties", "-version"],
                                    capture_output=True, text=True, check=True)
        cls.actual = int(re.search(r"java.specification.version = (\d+)", properties.stderr).group(1))

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def invoke(self, expected, name):
        report = self.root / name
        result = subprocess.run(["java", f"-javaagent:{self.jar}={expected},{report}",
                                 "-cp", str(self.jar), "io.quotaflow.verification.RuntimeGuard"],
                                capture_output=True, text=True)
        reports = list(report.glob("worker-*.json"))
        self.assertEqual(1, len(reports), result.stderr)
        return result, json.loads(reports[0].read_text())

    def test_actual_worker_identity_is_recorded(self):
        result, report = self.invoke(self.actual, "matching")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("PASS", report["status"])
        self.assertEqual(self.actual, report["actualJdk"])
        self.assertTrue(report["javaHome"])

    def test_mislabeled_worker_fails_before_tests(self):
        expected = 17 if self.actual != 17 else 25
        result, report = self.invoke(expected, "mismatch")
        self.assertEqual(78, result.returncode, result.stderr)
        self.assertEqual("FAIL", report["status"])
        self.assertEqual(self.actual, report["actualJdk"])
        self.assertEqual(expected, report["expectedJdk"])


class GradleAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Populate actual allowed runtime dependencies before offline fault injection.
        result = subprocess.run([str(ROOT / "gradlew"), "dependencyAudit", "--console=plain"],
                                cwd=ROOT, capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError(result.stdout + result.stderr)

    def gradle(self, *arguments, init=None, environment=None, offline=False):
        with tempfile.TemporaryDirectory(prefix="quotaflow-gradle-control-") as directory:
            command = [str(ROOT / "gradlew"), "--console=plain"]
            if offline:
                command.append("--offline")
            if init:
                script = Path(directory) / "control.gradle"
                script.write_text(init)
                command += ["-I", str(script)]
            return subprocess.run(command + list(arguments), cwd=ROOT, env=environment,
                                  capture_output=True, text=True)

    def test_unresolved_dependency_cannot_pass_audit(self):
        result = self.gradle(":quotaflow-core:dependencyAudit", offline=True, init='''
gradle.beforeProject { p ->
    if (p.path == ':quotaflow-core') p.plugins.withId('java-library') {
        p.dependencies.add('runtimeOnly', 'io.quotaflow.invalid:missing-fixture:0')
    }
}
''')
        self.assertNotEqual(0, result.returncode)
        self.assertIn("missing-fixture", result.stdout + result.stderr)

    def test_disallowed_resolved_dependency_cannot_pass_audit(self):
        result = self.gradle(":quotaflow-core:dependencyAudit", init='''
gradle.beforeProject { p ->
    if (p.path == ':quotaflow-core') p.plugins.withId('java-library') {
        p.dependencies.add('runtimeOnly', 'org.reactivestreams:reactive-streams:1.0.4')
        // This fault injection targets the independent allowlist, after the lock gate.
        p.afterEvaluate { p.configurations.runtimeClasspath.resolutionStrategy.deactivateDependencyLocking() }
    }
}
''')
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Unexpected modules on runtime classpath", result.stdout + result.stderr)

    def test_ci_cannot_opt_out_of_complete_verification(self):
        result = self.gradle("help", "-PallowIncompleteVerification=true", environment={**os.environ, "CI": "true"})
        self.assertNotEqual(0, result.returncode)
        self.assertIn("CI cannot opt out", result.stdout + result.stderr)

    def test_github_cannot_schedule_benchmark_execution(self):
        result = self.gradle(":quotaflow-core:benchmarkGate", "--dry-run",
                             environment={**os.environ, "GITHUB_ACTIONS": "true"})
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Benchmark execution is local-only", result.stdout + result.stderr)

    def test_unsupported_test_runtime_is_rejected(self):
        result = self.gradle("help", "-PtestJdk=19")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("testJdk must be one of", result.stdout + result.stderr)


class RequiredScenarioTest(unittest.TestCase):
    def run_fixture(self, allow_incomplete):
        with tempfile.TemporaryDirectory(prefix="quotaflow-skipped-control-") as directory:
            root = Path(directory)
            (root / "settings.gradle").write_text("rootProject.name = 'verification-fixture'\n")
            guard = root / "gradle/verification/RuntimeGuard.java"
            guard.parent.mkdir(parents=True)
            shutil.copy2(ROOT / "gradle/verification/RuntimeGuard.java", guard)
            (root / "build.gradle").write_text("""
plugins { id 'java-library' }
repositories { mavenCentral() }
java { toolchain { languageVersion = JavaLanguageVersion.of(17) } }
dependencies {
    testImplementation platform('org.junit:junit-bom:5.11.4')
    testImplementation 'org.junit.jupiter:junit-jupiter'
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
}
test { useJUnitPlatform() }
""" + "apply from: " + json.dumps(str(ROOT / "gradle/test-runtime.gradle")) + "\n")
            source = root / "src/test/java/SkippedScenarioTest.java"
            source.parent.mkdir(parents=True)
            source.write_text("""
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
class SkippedScenarioTest {
    @Test @Disabled("negative control: missing required scenario") void requiredScenario() { }
}
""")
            result = subprocess.run([str(ROOT / "gradlew"), "-p", str(root), "test",
                                     "-PallowIncompleteVerification=" + str(allow_incomplete).lower()],
                                    cwd=ROOT, env={**os.environ, "CI": "false"}, capture_output=True, text=True)
            report = json.loads((root / "build/reports/runtime/test-jdk17/test-result.json").read_text())
            return result, report

    def test_skipped_required_scenario_fails(self):
        result, report = self.run_fixture(False)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(1, report["skipped"])
        self.assertEqual("INCOMPLETE", report["status"])

    def test_explicit_local_opt_out_remains_incomplete(self):
        result, report = self.run_fixture(True)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("INCOMPLETE", report["status"])


if __name__ == "__main__":
    unittest.main()
