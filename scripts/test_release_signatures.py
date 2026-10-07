"""Actual ephemeral PGP asset and manifest signature controls."""
import contextlib
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from release_common import sha256
from release_signatures import gpg, gpg_home, sign_assets, sign_manifest, verify_manifest, verify_detached, verify_pgp


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


class ManifestTest(PgpTest):
    def setUp(self):
        super().setUp()
        self.blob=self.root/'release-manifest.json'
        self.document={'repository':'cramen/quotaflow','mode':'release','candidate':{'version':'1.0.0','commit':'a'*40}}
        self.blob.write_text(json.dumps(self.document))
        self.policy={'repository':'cramen/quotaflow','pgp':{'fingerprint':self.identity,'publicKey':str(self.public)}}
    def sign_manifest(self):return sign_manifest(self.blob,self.private,'',self.policy)
    def check_manifest(self,tag='v1.0.0',commit='a'*40):
        return verify_manifest(self.blob,str(self.blob)+'.asc',self.policy,tag,commit)
    def test_manifest_signature_is_real_and_reusable_without_external_identity(self):
        first=self.sign_manifest();self.assertEqual('PASS',first['status'])
        self.assertEqual(first,self.sign_manifest());self.assertEqual(first,self.check_manifest())
    def test_wrong_manifest_context_or_key_and_tampered_bytes_fail(self):
        self.sign_manifest()
        for tag,commit in [('v2.0.0','a'*40),('v1.0.0','b'*40)]:
            with self.assertRaises(ValueError):self.check_manifest(tag,commit)
        self.policy['pgp']['fingerprint']='0'*40
        with self.assertRaises(ValueError):self.check_manifest()
        self.policy['pgp']['fingerprint']=self.identity
        self.blob.write_text(self.blob.read_text()+' ')
        with self.assertRaises(ValueError):self.check_manifest()
    def test_missing_and_corrupt_signatures_are_not_replaced(self):
        with self.assertRaisesRegex(ValueError,'Missing'):self.check_manifest()
        self.sign_manifest();Path(str(self.blob)+'.asc').write_text('corrupt')
        with self.assertRaises(ValueError):self.sign_manifest()
    def test_rehearsal_wrong_repository_and_ci_are_refused(self):
        for key,value in [('mode','rehearsal'),('repository','other/project')]:
            document=dict(self.document);document[key]=value;self.blob.write_text(json.dumps(document))
            with self.assertRaises(ValueError):self.sign_manifest()
        self.blob.write_text(json.dumps(self.document))
        with patch.dict('os.environ',{'GITHUB_ACTIONS':'true'}):
            with self.assertRaisesRegex(ValueError,'local-only'):self.sign_manifest()


if __name__ == '__main__': unittest.main()
