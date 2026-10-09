#!/usr/bin/env python3
"""Synthetic positive and negative controls for exact-candidate release acceptance."""
import copy
import json
from pathlib import Path
import unittest
from test_performance_bundle import BundleTest
from verify_performance_bundle import digest
from verify_release_evidence import REQUIRED, verify


class ReleaseEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.performance = BundleTest()
        self.performance.setUp()
        self.addCleanup(self.performance.doCleanups)
        self.root = self.performance.root
        self.candidate = self.performance.identity
        self.candidate["artifacts"] = {"quotaflow-core-1.0-test.jar": "b" * 64}
        self.performance.check()
        self.performance_ref = {"path": "manifest.json", "sha256": digest(self.root / "manifest.json")}
        # Keep performance paths relative to its manifest without rewriting raw evidence.
        import shutil
        performance = self.root / "performance"
        performance.mkdir()
        for path in list(self.root.iterdir()):
            if path.is_file(): shutil.move(path, performance / path.name)
        self.manifest = {"schemaVersion": 1, "candidate": self.candidate, "stages": {}}
        for kind in REQUIRED:
            reports = []
            if kind.startswith(("runtime-", "topology-")):
                names = ["GeneratedNumericModelTest", "GeneratedRaceModelTckTest", "RecoveryProcessTckTest",
                         "StaggeredRecoveryEnvelopeTckTest", "ObservationIsolationTest", "ClusterTopologyTckTest", "SentinelTopologyTckTest"]
                xml = '<testsuite tests="7" failures="0" errors="0" skipped="0">' + ''.join(
                    f'<testcase classname="fixture.{name}" name="scenario"/>' for name in names) + '</testsuite>'
                reports.append(self.write(f"{kind}/TEST-fixture.xml", xml))
                if kind.startswith("runtime-"):
                    jdk = int(kind.split('-')[1])
                    reports.append(self.write(f"{kind}/worker-1.json", {"expectedJdk": jdk, "actualJdk": jdk, "status": "PASS"}))
                    if jdk >= 21:
                        for number, scenario in enumerate(("healthy", "degraded", "throttle")):
                            for offset, algorithm in enumerate(("TOKEN_BUCKET", "GCRA")):
                                for phase, callers in (("cold", 512), ("warm", 100000)):
                                    name = f"{kind}/{algorithm}-{scenario}-{phase}"
                                    reports.append(self.write(name + ".json", {"pid": 10 + number * 2 + offset, "jdk": jdk,
                                        "callers": callers, "allowed": callers, "rejected": 0, "libraryPins": 0,
                                        "unattributedPins": 0, "totalPins": 0}))
                                    reports.append(self.write(name + ".jfr", "synthetic recording control"))
            elif kind == "core-coverage":
                reports.append(self.write(kind + "/coverage.xml", '<report><counter type="BRANCH" covered="90" missed="10"/></report>'))
            elif kind.endswith("-mutation"):
                xml = '<mutations>' + '<mutation status="KILLED"/>' * 8 + '<mutation status="SURVIVED"/>' * 2 + '</mutations>'
                reports.append(self.write(kind + "/mutations.xml", xml))
            elif kind.startswith("native-"):
                marker = "native-smoke OK" if kind == "native-store" else "NATIVE VERIFIED"
                log = self.write(kind + "/native-run.log", marker + " stack=" + kind.removeprefix("native-"))
                reports.append(self.write(kind + "/native.json", {"native": True, "exitCode": 0, "fixture": kind.removeprefix("native-"), "executableSha256": "a" * 64,
                    "expectedMarker": marker, "runLog": dict(log, path="native-run.log")}))
                reports.append(log)
            elif kind == "soak":
                report = dict.fromkeys(("TOKEN_BUCKETAllowed", "GCRAAllowed", "rejected", "cancelled", "reloads", "fallbackDecisions", "transitions"), 1)
                report.update(status="PASS", durationSeconds=3600, elapsedNanos=3600000000000, owners=4,
                              unexpectedExceptions=0, outages=2, recoveries=2, maximumRecoveryMillis=500)
                reports.append(self.write(kind + "/soak.json", report))
            elif kind == "reproducibility":
                reports.append(self.write(kind + "/repro.json", {"status": "PASS", "version": "1.0-test", "builds": [{"quotaflow-core-1.0-test.jar": "b" * 64, "quotaflow-core-1.0-test-sources.jar": "c" * 64, "pom-default.xml": "d" * 64}] * 2}))
            elif kind == "performance":
                reports.append({"path": "performance/manifest.json", "sha256": digest(self.root / "performance/manifest.json")})
            elif kind == "negative-controls":
                reports.append(self.write(kind + "/controls.json", {"status": "PASS", "suites": ["build", "thresholds", "performance", "release"]}))
            elif kind == "consumers":
                runs, logs = [], []
                for boot in ("4.1.1", "4.0.8", "3.5.16"):
                    for jdk in (17, 21, 25):
                        for tool in ("maven", "gradle"):
                            for stack in ("servlet", "reactive"):
                                name = f"{boot}-{jdk}-{tool}-{stack}.log"
                                logs.append(self.write(kind + "/" + name, f"CONSUMER VERIFIED boot={boot} stack={stack} jdk={jdk} redisVerified=true"))
                                runs.append(dict(boot=boot, jdk=jdk, tool=tool, stack=stack, log=name, status="PASS"))
                reports.append(self.write(kind + "/consumers.json", runs)); reports.extend(logs)
            stage = {"kind": kind, "candidate": copy.deepcopy(self.candidate), "exitCode": 0, "startedAt": "2026-01-01T00:00:00Z", "endedAt": "2026-01-02T00:00:00Z",
                     "log": self.write(kind + "/stage.log", "synthetic command complete"), "reports": reports}
            self.manifest["stages"][kind] = self.write(kind + "/stage.json", stage)

    def write(self, name, value):
        path = self.root / name; path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value if isinstance(value, str) else json.dumps(value))
        return {"path": name, "sha256": digest(path)}

    def check(self):
        ref = self.write("manifest.json", self.manifest)
        return verify(self.root, ref["sha256"], self.candidate, self.performance.review_digest)

    def alter_stage(self, kind, change):
        ref = self.manifest["stages"][kind]
        stage = json.loads((self.root / ref["path"]).read_text())
        change(stage)
        self.manifest["stages"][kind] = self.write(ref["path"], stage)

    def approve_performance_exception(self):
        from unittest.mock import patch
        from verify_release_evidence import performance_exception
        self.candidate['artifacts'].update({f'fixture-{i}.jar': str(i) * 64 for i in range(6)})
        policy = self.root / 'trusted-policy.json'
        policy.write_text(json.dumps({'performanceException': {
            'version': self.candidate['version'], 'artifacts': self.candidate['artifacts'],
            'reason': 'Explicit synthetic operator exception'}}))
        self.policy_patch = patch('verify_release_evidence.RELEASE_POLICY', policy)
        self.policy_patch.start(); self.addCleanup(self.policy_patch.stop)
        for kind in REQUIRED:
            self.alter_stage(kind, lambda stage: stage.update(candidate=copy.deepcopy(self.candidate)))
        report = performance_exception(self.candidate)
        reference = self.write('performance/exception.json', report)
        self.alter_stage('performance', lambda stage: stage.update(reports=[reference]))
        return policy

    def test_approved_exception_is_waived_not_performance_pass(self):
        self.approve_performance_exception()
        result = self.check()
        self.assertEqual('PASS', result['status'])
        self.assertEqual('WAIVED', result['stages']['performance']['status'])
        self.assertFalse(result['stages']['performance']['performanceCertified'])

    def test_readiness_surfaces_the_performance_limitation(self):
        from release_readiness import performance_limitations
        self.approve_performance_exception()
        quality = self.check()
        limitations = performance_limitations({'gates': {'quality': quality}})
        self.assertEqual([{'check': 'performance', 'status': 'WAIVED',
                           'performanceCertified': False,
                           'reason': 'Explicit synthetic operator exception'}], limitations)
        self.assertEqual([], performance_limitations(None))

    def test_exception_cannot_skip_any_other_gate(self):
        self.approve_performance_exception()
        for kind in REQUIRED - {'performance'}:
            with self.subTest(kind=kind):
                saved = self.manifest['stages'].pop(kind)
                with self.assertRaises(ValueError): self.check()
                self.manifest['stages'][kind] = saved

    def test_exception_requires_exact_binaries_and_version(self):
        from verify_release_evidence import performance_exception
        self.approve_performance_exception()
        for field, value in [('version', 'other'), ('artifacts', {'other.jar': 'f' * 64})]:
            candidate = copy.deepcopy(self.candidate); candidate[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): performance_exception(candidate)
        candidate = copy.deepcopy(self.candidate)
        candidate['artifacts'][next(iter(candidate['artifacts']))] = 'f' * 64
        with self.assertRaises(ValueError): performance_exception(candidate)

    def test_exception_rejects_unapproved_or_changed_report(self):
        policy = self.approve_performance_exception()
        original = (self.root / 'performance/exception.json').read_text()
        for field, value in [('reason', 'Changed'), ('policySha256', 'f' * 64),
                             ('performanceCertified', True), ('candidate', {})]:
            report = json.loads(original); report[field] = value
            ref = self.write('performance/exception.json', report)
            self.alter_stage('performance', lambda stage: stage.update(reports=[ref]))
            with self.subTest(field=field), self.assertRaises(ValueError): self.check()
        ref = self.write('performance/exception.json', json.loads(original))
        self.alter_stage('performance', lambda stage: stage.update(reports=[ref]))
        policy.write_text('{}')
        with self.assertRaises(ValueError): self.check()

    def test_complete_bundle_passes(self):
        self.assertEqual("PASS", self.check()["status"])

    def test_every_required_gate_is_mandatory(self):
        for kind in REQUIRED:
            saved = self.manifest["stages"].pop(kind)
            with self.assertRaises(ValueError): self.check()
            self.manifest["stages"][kind] = saved

    def test_stale_stage_fails(self):
        self.alter_stage("soak", lambda s: s["candidate"].update(sourceSha256="f" * 64))
        with self.assertRaises(ValueError): self.check()

    def test_failed_invocation_fails(self):
        self.alter_stage("core-mutation", lambda s: s.update(exitCode=1))
        with self.assertRaises(ValueError): self.check()

    def test_tampered_report_fails(self):
        (self.root / "core-coverage/coverage.xml").write_text("tampered")
        with self.assertRaises(ValueError): self.check()

    def test_skipped_required_scenario_fails(self):
        replacement = self.write("runtime-17/TEST-fixture.xml", '<testsuite tests="1" skipped="1"><testcase><skipped/></testcase></testsuite>')
        self.alter_stage("runtime-17", lambda s: s["reports"].__setitem__(0, replacement))
        with self.assertRaises(ValueError): self.check()

    def test_short_soak_fails(self):
        report = json.loads((self.root / "soak/soak.json").read_text()); report["durationSeconds"] = 60
        replacement = self.write("soak/soak.json", report)
        self.alter_stage("soak", lambda s: s["reports"].__setitem__(0, replacement))
        with self.assertRaises(ValueError): self.check()

    def test_nonpassing_independent_mutation_module_fails(self):
        replacement = self.write("fallback-mutation/mutations.xml", '<mutations><mutation status="SURVIVED"/></mutations>')
        self.alter_stage("fallback-mutation", lambda s: s["reports"].__setitem__(0, replacement))
        with self.assertRaises(ValueError): self.check()

    def test_native_compile_without_runtime_proof_fails(self):
        report = json.loads((self.root / "native-servlet/native.json").read_text()); report["native"] = False
        replacement = self.write("native-servlet/native.json", report)
        self.alter_stage("native-servlet", lambda s: s["reports"].__setitem__(0, replacement))
        with self.assertRaises(ValueError): self.check()

    def test_archive_traversal_and_symlinks_are_rejected(self):
        import io, zipfile, stat
        from import_release_bundle import extract
        for name, mode in (("../outside", 0), ("link", stat.S_IFLNK | 0o777)):
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w") as archive:
                info = zipfile.ZipInfo(name); info.external_attr = mode << 16
                archive.writestr(info, "payload")
            buffer.seek(0)
            with self.assertRaises(ValueError): extract(buffer, self.root / "invalid-import")
        self.assertFalse((self.root / "invalid-import").exists())

    def test_archive_without_root_manifest_fails(self):
        import io, zipfile
        from import_release_bundle import extract
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive: archive.writestr("nested/manifest.json", "{}")
        buffer.seek(0)
        with self.assertRaises(ValueError): extract(buffer, self.root / "missing-manifest-import")


if __name__ == "__main__":
    unittest.main()
