#!/usr/bin/env python3
"""Prove real JaCoCo and PIT tasks reject an intentionally under-tested fixture."""
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class QualityThresholdTest(unittest.TestCase):
    def test_under_tested_code_cannot_pass_either_gate(self):
        with tempfile.TemporaryDirectory(prefix="quotaflow-threshold-control-") as temporary:
            root = Path(temporary)
            (root / "settings.gradle").write_text("rootProject.name = 'threshold-control'\n")
            (root / "build.gradle").write_text('''
plugins { id 'java'; id 'jacoco'; id 'info.solidsoft.pitest' version '1.19.0' }
repositories { mavenCentral() }
java { toolchain { languageVersion = JavaLanguageVersion.of(17) } }
dependencies {
    testImplementation 'org.junit.jupiter:junit-jupiter:5.11.4'
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher:1.11.4'
    testImplementation 'org.pitest:pitest-junit5-plugin:1.2.1'
}
test { useJUnitPlatform() }
jacoco { toolVersion = '0.8.14' }
jacocoTestCoverageVerification {
    dependsOn test
    violationRules { rule { limit { counter = 'BRANCH'; minimum = 0.90 } } }
}
pitest {
    targetClasses = ['fixture.Guard']; mutationThreshold = 80
    outputFormats = ['XML']; threads = 1
}
''')
            main = root / "src/main/java/fixture/Guard.java"
            test = root / "src/test/java/fixture/GuardTest.java"
            main.parent.mkdir(parents=True); test.parent.mkdir(parents=True)
            main.write_text('''package fixture;
public class Guard {
    public boolean acquire(int weight, int balance) {
        if (weight <= 0) throw new IllegalArgumentException("positive weight required");
        if (weight > balance) return false;
        return true;
    }
}
''')
            test.write_text('''package fixture;
class GuardTest {
    @org.junit.jupiter.api.Test void onlyOneHappyPath() {
        org.junit.jupiter.api.Assertions.assertTrue(new Guard().acquire(1, 2));
    }
}
''')
            output = ROOT / "build/reports/quality-threshold-controls"
            output.mkdir(parents=True, exist_ok=True)
            for task, marker in (("jacocoTestCoverageVerification", "branches covered ratio"),
                                 ("pitest", "Mutation score")):
                result = subprocess.run([str(ROOT / "gradlew"), "-p", str(root), task, "--console=plain"],
                                        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                (output / (task + ".log")).write_text(result.stdout)
                self.assertNotEqual(0, result.returncode, "Under-tested fixture unexpectedly certified")
                self.assertIn(marker, result.stdout, "Failure must come from the threshold, not missing tooling")


if __name__ == "__main__":
    unittest.main()
