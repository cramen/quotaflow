"""Actual ephemeral PGP signing controls and Sigstore identity boundary controls."""
import contextlib
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from release_common import sha256
from release_signatures import gpg, gpg_home, sign_assets, sign_sigstore, sigstore_environment, verify_detached, verify_pgp, verify_sigstore


class PgpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.stack = contextlib.ExitStack(); cls.home = cls.stack.enter_context(gpg_home())
        gpg(cls.home, ["--pinentry-mode", "loopback", "--passphrase", "", "--quick-generate-key",
                       "Quotaflow Test Only <release-tests@example.invalid>", "ed25519", "sign", "1d"])
        cls.identity = next(line.split(":")[9] for line in gpg(cls.home, ["--with-colons", "--list-keys"]).splitlines() if line.startswith("fpr:"))
        cls.private = gpg(cls.home, ["--pinentry-mode", "loopback", "--passphrase", "", "--armor", "--export-secret-keys", cls.identity])
        cls.public = cls.home / "public.asc"; cls.public.write_text(gpg(cls.home, ["--armor", "--export", cls.identity]))

    @classmethod
    def tearDownClass(cls): cls.stack.close()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="quotaflow-signed-candidate-"); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.jar = self.root / "example.jar"; self.jar.write_bytes(b"immutable artifact bytes")
        self.pom = self.root / "example.pom"; self.pom.write_text("<project>candidate</project>")
        self.manifest = {"mode": "dry-run", "candidate": {"commit": "a" * 40, "version": "1.0.0", "sourceSha256": "b" * 64,
            "artifacts": {"example.jar": sha256(self.jar)}}, "assets": [{"path": p.name, "sha256": sha256(p)} for p in (self.jar, self.pom)]}
        (self.root / "candidate-manifest.json").write_text(json.dumps(self.manifest))

    def sign(self): return sign_assets(self.root, self.private, "", self.public, self.identity, test_only=True)

    def test_every_asset_is_signed_and_verified_with_an_unprotected_test_key(self):
        report = self.sign(); self.assertEqual(2, len(report["signedAssets"]))
        verify_pgp(self.root, report, self.manifest, self.identity, production=False)
        self.assertFalse(report["production"])

    def test_test_signatures_cannot_authorize_real_publication(self):
        report = self.sign()
        with self.assertRaisesRegex(ValueError, "Test signatures"):
            verify_pgp(self.root, report, self.manifest, self.identity)
        with self.assertRaisesRegex(ValueError, "Dry-run"):
            sign_assets(self.root, self.private, "", self.public, self.identity)

    def test_modified_bytes_fail_real_cryptographic_verification(self):
        self.sign(); self.jar.write_bytes(b"different bytes")
        with self.assertRaises(ValueError): verify_detached(self.jar, str(self.jar) + ".asc", self.public, self.identity)

    def test_missing_signature_wrong_identity_and_incomplete_set_fail(self):
        report = self.sign()
        with self.assertRaises(ValueError): verify_detached(self.jar, str(self.jar) + ".asc", self.public, "0" * 40)
        missing = copy.deepcopy(report); missing["signedAssets"].pop()
        with self.assertRaisesRegex(ValueError, "Incomplete"): verify_pgp(self.root, missing, self.manifest, self.identity, production=False)
        Path(str(self.jar) + ".asc").unlink()
        with self.assertRaises(ValueError): verify_pgp(self.root, report, self.manifest, self.identity, production=False)

    def test_existing_valid_signature_is_reused_but_corruption_is_not_replaced(self):
        first = self.sign(); second = self.sign(); self.assertEqual(first, second)
        Path(str(self.jar) + ".asc").write_text("corrupted")
        with self.assertRaises(ValueError): self.sign()

    def test_valid_signature_at_another_path_cannot_replace_required_sidecar(self):
        report=self.sign()
        original=self.root/report['signedAssets'][0]['signature']['path']
        renamed=self.root/'renamed.asc'; renamed.write_bytes(original.read_bytes())
        report['signedAssets'][0]['signature']['path']='renamed.asc'
        with self.assertRaisesRegex(ValueError,'required Maven sidecar'):
            verify_pgp(self.root,report,self.manifest,self.identity,production=False)


class SigstoreBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="quotaflow-sigstore-control-"); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.blob = self.root / "manifest.json"; self.bundle = self.root / "signature.json"
        self.blob.write_text('{"repository":"cramen/quotaflow","candidate":{"commit":"' + 'a' * 40 + '","version":"1.0.0"},"mode":"release"}')
        self.bundle.write_text("{}"); self.policy = {"repository": "cramen/quotaflow", "certificateIdentity": "release@example.invalid", "oidcIssuer": "https://oauth2.sigstore.dev/auth"}

    def test_verifier_requires_exact_local_identity_and_manifest_context(self):
        with patch('release_signatures.tool', return_value=(Path('/approved/cosign'), {"version": "test"})), patch('release_signatures.subprocess.run', return_value=subprocess.CompletedProcess([], 0)) as run:
            verify_sigstore(self.blob,self.bundle,self.policy,'v1.0.0','a'*40,self.root)
            command=run.call_args.args[0]
            self.assertIn('release@example.invalid',command)
            self.assertIn(self.policy['oidcIssuer'],command)
            self.assertNotIn('--certificate-github-workflow-sha',command)
            for tag,commit in [('v2.0.0','a'*40),('v1.0.0','b'*40)]:
                with self.assertRaisesRegex(ValueError,'manifest'):
                    verify_sigstore(self.blob,self.bundle,self.policy,tag,commit,self.root)
            changed=json.loads(self.blob.read_text());changed['repository']='other/project'
            self.blob.write_text(json.dumps(changed))
            with self.assertRaisesRegex(ValueError,'manifest'):
                verify_sigstore(self.blob,self.bundle,self.policy,'v1.0.0','a'*40,self.root)

    def test_missing_bundle_or_failed_crypto_verification_blocks(self):
        with patch('release_signatures.tool', return_value=(Path('/approved/cosign'), {})), patch('release_signatures.subprocess.run', return_value=subprocess.CompletedProcess([], 1)):
            with self.assertRaises(ValueError): verify_sigstore(self.blob, self.bundle, self.policy, 'v1.0.0', 'a' * 40, self.root)
        self.bundle.unlink()
        with self.assertRaisesRegex(ValueError, 'Missing'): verify_sigstore(self.blob, self.bundle, self.policy, 'v1.0.0', 'a' * 40, self.root)

    def test_ambient_trust_overrides_and_non_release_signing_are_refused(self):
        with patch.dict('os.environ', {'SIGSTORE_ROOT_FILE': '/untrusted/root', 'TUF_MIRROR': 'https://untrusted.invalid', 'GITHUB_ACTIONS': 'true'}):
            environment = sigstore_environment(self.root)
            self.assertNotIn('SIGSTORE_ROOT_FILE', environment); self.assertNotIn('TUF_MIRROR', environment)
            with self.assertRaisesRegex(ValueError, 'local-only'):
                sign_sigstore(self.blob, self.root/'new-bundle.json', self.policy, self.root)


if __name__ == '__main__': unittest.main()
