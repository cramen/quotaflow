"""Failure injection for irreversible release transitions, without external APIs."""
import copy
import hashlib
import tempfile
from pathlib import Path
import unittest
import zipfile

from release_common import GROUP, MODULES, purl, sha256
from release_promotion import advance, encoded, RecoveryRequired


class Journal:
    def __init__(self): self.state = None; self.history = []; self.fail_phase = None
    def load(self): return copy.deepcopy(self.state)
    def save(self, state):
        if state['phase'] == self.fail_phase: raise OSError('durable journal unavailable')
        self.state = copy.deepcopy(state); self.history.append(copy.deepcopy(state))


class GitHub:
    def __init__(self): self.files = {}; self.complete = False; self.fail_publish = False
    def ensure_draft(self, tag, commit): return 123
    def assets(self, release): return [{'name': name} for name in self.files]
    def upload_asset(self, release, name, data):
        if name in self.files: raise ValueError('collision')
        self.files[name] = data
    def download_asset(self, release, name): return self.files[name]
    def publish(self, release, tag, commit):
        if self.fail_publish: raise OSError('GitHub unavailable after Central publication')
        self.complete = True
    def is_complete(self, release, tag, commit): return self.complete


class Portal:
    def __init__(self):
        self.uploads = 0; self.promotions = 0; self.files = {}; self.state = 'VALIDATED'
        self.lose_upload = False; self.lose_promote = False; self.corrupt = False
    def upload(self, bundle, name):
        self.uploads += 1
        with zipfile.ZipFile(bundle) as archive: self.files = {name: archive.read(name) for name in archive.namelist()}
        if self.lose_upload: raise OSError('lost upload response')
        return 'deployment-1'
    def status(self, deployment):
        return {'deploymentId': deployment, 'deploymentState': self.state,
                'purls': [purl(GROUP, module, '1.0.0') for module in MODULES]}
    def download(self, deployment, name, published=False): return b'wrong' if self.corrupt else self.files[name]
    def promote(self, deployment):
        self.promotions += 1; self.state = 'PUBLISHED'
        if self.lose_promote: raise OSError('lost promotion response')


class PromotionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.bundle = self.root/'maven.zip'
        with zipfile.ZipFile(self.bundle, 'w') as archive:
            for module in MODULES:
                archive.writestr('io/quotaflow/' + module + '/1.0.0/' + module + '-1.0.0.jar', module.encode())
        self.manifest = {'candidate': {'version': '1.0.0', 'commit': 'a'*40},
                         'mavenBundle': {'path': 'maven.zip', 'sha256': sha256(self.bundle)}}
        self.digest = hashlib.sha256(encoded(self.manifest)).hexdigest()
        self.journal = Journal(); self.portal = Portal(); self.github = GitHub(); self.gate_calls = []
    def gates(self, irreversible): self.gate_calls.append(irreversible)
    def run_step(self, recovered=None, gates=None):
        return advance(self.root, self.manifest, self.digest, [self.bundle], self.journal, self.portal,
                       self.github, gates or self.gates, recovered)

    def test_one_complete_deployment_then_idempotent_completion(self):
        self.assertEqual('PENDING', self.run_step()['status'])
        self.assertEqual(7, len(self.portal.files))
        self.assertEqual('COMPLETE', self.run_step()['status'])
        self.assertEqual('COMPLETE', self.run_step()['status'])
        self.assertEqual((1, 1), (self.portal.uploads, self.portal.promotions))
        self.assertEqual(['DRAFT_VERIFIED', 'UPLOAD_INTENT', 'UPLOADED', 'PROMOTION_INTENT', 'CENTRAL_PUBLISHED', 'COMPLETE'],
                         [s['phase'] for s in self.journal.history])

    def test_every_failed_gate_prevents_any_upload(self):
        for failure in ('credentials', 'tag', 'quality', 'soak', 'performance', 'CVE', 'PGP', 'Sigstore', 'SBOM'):
            with self.subTest(failure=failure):
                def reject(_): raise ValueError(failure)
                with self.assertRaises(ValueError): self.run_step(gates=reject)
                self.assertEqual(0, self.portal.uploads)

    def test_asset_collision_and_changed_bundle_prevent_upload(self):
        self.github.files['maven.zip'] = b'old candidate'
        with self.assertRaisesRegex(ValueError, 'collision'): self.run_step()
        self.assertEqual(0, self.portal.uploads)
        self.github.files.clear(); self.bundle.write_bytes(b'tampered')
        with self.assertRaisesRegex(ValueError, 'digest'): self.run_step()
        self.assertEqual(0, self.portal.uploads)

    def test_lost_upload_response_never_causes_a_duplicate(self):
        self.portal.lose_upload = True
        with self.assertRaises(OSError): self.run_step()
        for _ in range(3):
            with self.assertRaises(RecoveryRequired): self.run_step()
        self.assertEqual(1, self.portal.uploads)
        self.run_step(recovered='deployment-1'); self.run_step()
        self.assertEqual((1, 1), (self.portal.uploads, self.portal.promotions))

    def test_runner_loss_before_persisting_deployment_is_also_uncertain(self):
        self.journal.fail_phase = 'UPLOADED'
        with self.assertRaises(OSError): self.run_step()
        self.journal.fail_phase = None
        with self.assertRaises(RecoveryRequired): self.run_step()
        self.assertEqual(1, self.portal.uploads)

    def test_missing_durable_intent_prevents_network_mutation(self):
        self.journal.fail_phase = 'UPLOAD_INTENT'
        with self.assertRaises(OSError): self.run_step()
        self.assertEqual(0, self.portal.uploads)

    def test_wrong_remote_bytes_refuse_recovery_or_promotion(self):
        self.portal.lose_upload = True
        with self.assertRaises(OSError): self.run_step()
        self.portal.corrupt = True
        with self.assertRaisesRegex(ValueError, 'bytes differ'): self.run_step(recovered='deployment-1')
        self.assertEqual(0, self.portal.promotions)

    def test_lost_promotion_response_reconciles_without_second_promotion(self):
        self.portal.lose_promote = True
        with self.assertRaises(OSError): self.run_step()
        self.assertEqual('COMPLETE', self.run_step()['status'])
        self.assertEqual((1, 1), (self.portal.uploads, self.portal.promotions))

    def test_promotion_not_received_retries_only_the_reconciled_deployment(self):
        from unittest.mock import patch
        with patch.object(self.portal, 'promote', side_effect=OSError('request not received')):
            with self.assertRaises(OSError): self.run_step()
        self.assertEqual('PROMOTION_INTENT', self.journal.state['phase'])
        self.run_step(); self.run_step()
        self.assertEqual((1, 1), (self.portal.uploads, self.portal.promotions))

    def test_unjournaled_central_success_allows_completion_after_scan_expiry(self):
        self.portal.lose_promote = True
        with self.assertRaises(OSError): self.run_step()
        def expired(irreversible):
            if not irreversible: raise ValueError('security evidence expired')
        self.assertEqual('COMPLETE', self.run_step(gates=expired)['status'])

    def test_github_failure_after_central_resumes_only_github(self):
        self.run_step(); self.github.fail_publish = True
        with self.assertRaises(OSError): self.run_step()
        self.assertEqual('CENTRAL_PUBLISHED', self.journal.state['phase'])
        self.github.fail_publish = False
        self.assertEqual('COMPLETE', self.run_step()['status'])
        self.assertTrue(self.gate_calls[-1])
        self.assertEqual((1, 1), (self.portal.uploads, self.portal.promotions))

    def test_another_candidate_cannot_resume_the_same_version(self):
        self.run_step(); self.manifest['candidate']['commit'] = 'b'*40
        self.digest = hashlib.sha256(encoded(self.manifest)).hexdigest()
        with self.assertRaisesRegex(ValueError, 'different candidate'): self.run_step()


if __name__ == '__main__': unittest.main()
