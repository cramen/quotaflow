"""Release preparation controls: real Git histories, publication faults and credential mapping."""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import zipfile

from prepare_release import inspect_repository, production_sbom
from release_common import GROUP, MODULES, credential_environment, release_version, sha256, tag_identity

ROOT = Path(__file__).resolve().parents[1]


class IdentityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="quotaflow-release-git-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.run_git("init", "-b", "main")
        (self.root / "source").write_text("first")
        self.run_git("add", "source"); self.run_git("commit", "-m", "First")
        self.first = self.run_git("rev-parse", "HEAD")
        (self.root / "source").write_text("second")
        self.run_git("commit", "-am", "Second")
        self.run_git("update-ref", "refs/remotes/origin/main", "HEAD")
        self.run_git("tag", "v1.2.3", self.first)

    def run_git(self, *args):
        return subprocess.check_output(["git", "-c", "user.name=Release Test", "-c", "user.email=release@example.invalid",
                                        "-c", "commit.gpgSign=false", *args], cwd=self.root, text=True, stderr=subprocess.DEVNULL).strip()

    def test_older_main_commit_is_valid_with_complete_history(self):
        self.run_git("checkout", "--detach", self.first)
        self.assertEqual(self.first, tag_identity(self.root, "v1.2.3")["commit"])

    def test_head_must_equal_tag(self):
        with self.assertRaisesRegex(ValueError, "Checkout"):
            tag_identity(self.root, "v1.2.3")

    def test_annotated_tag_resolves_to_its_commit(self):
        self.run_git("tag", "-a", "v1.2.4", "-m", "Release", self.first)
        self.run_git("checkout", "--detach", self.first)
        self.assertEqual(self.first, tag_identity(self.root, "v1.2.4", self.first)["commit"])

    def test_off_main_tag_is_refused(self):
        self.run_git("checkout", "-b", "side", self.first)
        (self.root / "source").write_text("side")
        self.run_git("commit", "-am", "Side"); self.run_git("tag", "v9.0.0")
        with self.assertRaisesRegex(ValueError, "outside trusted main"):
            tag_identity(self.root, "v9.0.0")

    def test_dirty_tree_and_wrong_authorized_commit_are_refused(self):
        self.run_git("checkout", "--detach", self.first)
        with self.assertRaisesRegex(ValueError, "authorized"):
            tag_identity(self.root, "v1.2.3", "0" * 40)
        (self.root / "source").write_text("changed")
        with self.assertRaisesRegex(ValueError, "clean tracked"):
            tag_identity(self.root, "v1.2.3")

    def test_shallow_clone_cannot_claim_ancestry(self):
        with tempfile.TemporaryDirectory(prefix="quotaflow-shallow-") as destination:
            subprocess.run(["git", "clone", "--depth=1", self.root.as_uri(), destination], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            with self.assertRaisesRegex(ValueError, "Complete Git history"):
                tag_identity(Path(destination), "v1.2.3")

    def test_strict_semantic_version_and_snapshot_refusal(self):
        for value in ("0.1.0", "1.2.3-rc.1", "2.0.0+build.8"):
            self.assertEqual(value, release_version(value))
        for value in ("v1.2.3", "01.2.3", "1.2", "1.2.3-SNAPSHOT", "1.2.3-snapshot.1", "1.2.3-01", "1.2.3/other", "1.2.3 "):
            with self.subTest(value=value), self.assertRaises(ValueError):
                release_version(value)


class PublicationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="quotaflow-publication-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.version = "1.2.3"
        self.graph = {"version": self.version, "modules": list(MODULES), "components": {}, "dependencies": {}}
        for module in MODULES:
            directory = self.root / "io/quotaflow" / module / self.version
            directory.mkdir(parents=True)
            stem = module + "-" + self.version
            for suffix, entries in ((".jar", {"io/quotaflow/Example.class": b"\xca\xfe\xba\xbe\x00"}),
                                    ("-sources.jar", {"Example.java": b"class Example {}"}),
                                    ("-javadoc.jar", {"index.html": b"reference " * 20, "Example.html": b"API documentation " * 20})):
                with zipfile.ZipFile(directory / (stem + suffix), "w") as jar:
                    for name, data in entries.items(): jar.writestr(name, data)
            dependencies = "" if module == "quotaflow-core" else f"<dependencies><dependency><groupId>{GROUP}</groupId><artifactId>quotaflow-core</artifactId><version>{self.version}</version></dependency></dependencies>"
            (directory / (stem + ".pom")).write_text(f'''<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>{GROUP}</groupId><artifactId>{module}</artifactId><version>{self.version}</version><name>Quotaflow</name><description>Public API</description><url>https://example.invalid</url>
<licenses><license><name>Apache-2.0</name><url>https://example.invalid/license</url></license></licenses>
<developers><developer><id>maintainer</id><name>Maintainer</name></developer></developers><scm><url>https://example.invalid/source</url><connection>scm:git:https://example.invalid/source</connection></scm>{dependencies}</project>''')
            binary = directory / (stem + ".jar")
            (directory / (stem + ".module")).write_text(json.dumps({"component": {"group": GROUP, "module": module, "version": self.version},
                "variants": [{"name": "runtime", "files": [{"url": binary.name, "sha256": sha256(binary)}]}]}))
            ref = GROUP + ":" + module + ":" + self.version
            self.graph["components"][ref] = {"group": GROUP, "name": module, "version": self.version, "artifacts": []}
            self.graph["dependencies"][ref] = []
        self.slf4j = "org.slf4j:slf4j-api:2.0.17"
        self.graph["components"][self.slf4j] = {"group": "org.slf4j", "name": "slf4j-api", "version": "2.0.17", "artifacts": [{"filename": "slf4j-api-2.0.17.jar", "sha256": "a" * 64}]}
        self.graph["dependencies"][GROUP + ":quotaflow-core:" + self.version] = [self.slf4j]

    def asset(self, module, suffix):
        return self.root / "io/quotaflow" / module / self.version / (module + "-" + self.version + suffix)

    def test_complete_seven_module_candidate_and_runtime_sbom(self):
        binaries, assets = inspect_repository(self.root, self.version, self.graph)
        sbom = production_sbom(self.graph, self.version, binaries)
        self.assertEqual(7, len(binaries)); self.assertEqual(35, len(assets)); self.assertEqual(8, len(sbom["components"]))
        self.assertEqual(self.version, sbom["metadata"]["component"]["version"])

    def test_missing_and_internal_modules_fail(self):
        shutil.rmtree(self.root / "io/quotaflow/quotaflow-config")
        with self.assertRaises(ValueError): inspect_repository(self.root, self.version)
        (self.root / "io/quotaflow/quotaflow-tck").mkdir()
        with self.assertRaises(ValueError): inspect_repository(self.root, self.version)

    def test_empty_documentation_is_not_publishable(self):
        with zipfile.ZipFile(self.asset("quotaflow-kotlin", "-javadoc.jar"), "w") as jar:
            jar.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0")
        with self.assertRaisesRegex(ValueError, "documentation"):
            inspect_repository(self.root, self.version)

    def test_snapshot_inter_module_dependency_fails(self):
        p = self.asset("quotaflow-config", ".pom")
        p.write_text(p.read_text().replace("quotaflow-core</artifactId><version>1.2.3", "quotaflow-core</artifactId><version>1.2.3-SNAPSHOT"))
        with self.assertRaises(ValueError): inspect_repository(self.root, self.version)

    def test_changed_binary_and_wrong_checksum_fail(self):
        p = self.asset("quotaflow-core", ".jar.sha256"); p.write_text("0" * 64)
        with self.assertRaisesRegex(ValueError, "checksum"): inspect_repository(self.root, self.version)
        p.unlink()
        with zipfile.ZipFile(self.asset("quotaflow-core", ".jar"), "a") as jar: jar.writestr("modified.txt", "changed")
        with self.assertRaisesRegex(ValueError, "digest"): inspect_repository(self.root, self.version)

    def test_test_dependencies_and_wrong_project_version_fail_sbom(self):
        binaries, _ = inspect_repository(self.root, self.version)
        graph = copy.deepcopy(self.graph)
        graph["components"]["org.junit:junit:5"] = {"group": "org.junit", "name": "junit", "version": "5", "artifacts": [{"sha256": "b" * 64}]}
        with self.assertRaisesRegex(ValueError, "shipped runtime"): production_sbom(graph, self.version, binaries)
        graph = copy.deepcopy(self.graph); graph["version"] = "1.2.3-SNAPSHOT"
        with self.assertRaises(ValueError): production_sbom(graph, self.version, binaries)


class CredentialTest(unittest.TestCase):
    def test_canonical_properties_are_accepted_but_conflicting_aliases_fail(self):
        environment = {"ORG_GRADLE_PROJECT_mavenCentralUsername": "fixture-user", "ORG_GRADLE_PROJECT_mavenCentralPassword": "fixture-token"}
        self.assertEqual(environment, credential_environment("central", environment))
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            credential_environment("central", {**environment, "CENTRAL_PORTAL_TOKEN": "different"})

    def test_required_properties_and_empty_pgp_passphrase(self):
        result = credential_environment("all", {"CENTRAL_PORTAL_USERNAME": "fixture-user", "CENTRAL_PORTAL_TOKEN": "fixture-token", "SIGNING_KEY": "fixture-key"})
        self.assertEqual("fixture-user", result["ORG_GRADLE_PROJECT_mavenCentralUsername"])
        self.assertEqual("fixture-token", result["ORG_GRADLE_PROJECT_mavenCentralPassword"])
        self.assertEqual("", result["ORG_GRADLE_PROJECT_signingInMemoryKeyPassword"])
        with self.assertRaisesRegex(ValueError, "CENTRAL_PORTAL_TOKEN"):
            credential_environment("central", {"CENTRAL_PORTAL_USERNAME": "fixture-user"})


class GradleBoundaryTest(unittest.TestCase):
    def test_plugin_receives_only_supported_credential_properties_in_isolated_home(self):
        with tempfile.TemporaryDirectory(prefix="quotaflow-release-gradle-") as directory:
            root = Path(directory); home = root / "gradle-home"; home.mkdir()
            # Reuse immutable/download caches, never user properties or init scripts.
            shared = Path(os.environ.get("GRADLE_USER_HOME", str(Path.home() / ".gradle")))
            for name in ("caches", "wrapper"):
                (home / name).symlink_to(shared / name, target_is_directory=True)
            init = root / "mapping.gradle"
            init.write_text('''gradle.projectsEvaluated {
 rootProject.tasks.register('verifyReleaseCredentialMapping') { doLast {
  def params = gradle.sharedServices.registrations.getByName('maven-central-build-service').parameters
  if (params.repositoryUsername.orNull != 'fixture-user' || params.repositoryPassword.orNull != 'fixture-token')
   throw new GradleException('Credential mapping mismatch')
  println('Publishing plugin credential mapping verified')
 } }
}''')
            env = {k:v for k,v in os.environ.items() if not k.startswith('ORG_GRADLE_PROJECT_')}
            env.update(credential_environment("central", {"CENTRAL_PORTAL_USERNAME": "fixture-user", "CENTRAL_PORTAL_TOKEN": "fixture-token"}))
            result = subprocess.run([str(ROOT / "gradlew"), "-g", str(home), "-I", str(init), "verifyReleaseCredentialMapping", "--offline", "--no-daemon", "--console=plain"],
                                    cwd=ROOT, env=env, capture_output=True, text=True)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertIn("Publishing plugin credential mapping verified", result.stdout)
            self.assertNotIn("fixture-token", result.stdout + result.stderr)

    def test_direct_central_graph_is_refused_even_in_dry_run(self):
        result = subprocess.run([str(ROOT / "gradlew"), "publishAndReleaseToMavenCentral", "--dry-run", "--offline", "--console=plain"],
                                cwd=ROOT, capture_output=True, text=True)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Direct Gradle Central upload is disabled", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
