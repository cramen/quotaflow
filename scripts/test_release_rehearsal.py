"""Local rehearsal must stay local even when publishing credentials exist."""
import json
import unittest
from unittest.mock import patch

import test_release_candidate as assembly_tests
from release_common import sha256
from release_promotion import encoded
from release_rehearsal import rehearse


class RehearsalTest(unittest.TestCase):
    def setUp(self):
        self.fixture=assembly_tests.AssemblyTest(); self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        self.sealed=self.fixture.seal(); self.root=self.fixture.output
        self.digest=sha256(self.root/'release-manifest.json'); self.output=self.fixture.root/'rehearsal'

    def test_real_assembly_and_journal_run_without_any_network_or_credentials(self):
        with patch.dict('os.environ',{'CENTRAL_PORTAL_USERNAME':'ambient-user','CENTRAL_PORTAL_TOKEN':'ambient-token',
                                     'GH_TOKEN':'ambient-github','SIGNING_KEY':'ambient-secret'}), \
             patch('release_transport.request',side_effect=AssertionError('External transport forbidden')), \
             patch('socket.create_connection',side_effect=AssertionError('Network forbidden')):
            report=rehearse(self.root,self.digest,self.output)
        self.assertEqual('SIMULATED_COMPLETE',report['status'])
        self.assertEqual('UNVERIFIED',report['productionReadiness'])
        self.assertEqual(0,report['externalPublishingOperations'])
        self.assertEqual((1,1),(report['localUploads'],report['localPromotions']))
        self.assertEqual('UNVERIFIED',report['gates']['quality'])
        self.assertEqual('UNVERIFIED',report['gates']['productionPgp'])
        journals=list((self.output/'github/assets').glob('quotaflow-release-journal-*.json'))
        self.assertEqual(6,len(journals))
        self.assertFalse(any('ambient-' in p.read_text() for p in journals))
        self.assertEqual(self.digest,sha256(self.root/'release-manifest.json'))

    def test_incomplete_evidence_pins_stop_before_simulated_upload(self):
        with self.assertRaises(ValueError): rehearse(self.root,self.digest,self.output,{'archiveSha256':'a'*64})
        report=json.loads((self.output/'rehearsal.json').read_text())
        self.assertEqual('FAILED',report['status']); self.assertFalse((self.output/'portal/deployment.zip').exists())

    def test_modified_candidate_or_production_mode_is_refused(self):
        path=self.root/'release-manifest.json'
        self.sealed['mode']='release'; path.write_bytes(encoded(self.sealed))
        with self.assertRaisesRegex(ValueError,'relabeled as production'):
            rehearse(self.root,sha256(path),self.output)
        with self.assertRaisesRegex(ValueError,'digest'): rehearse(self.root,self.digest,self.output)
        self.assertFalse(self.output.exists())


if __name__=='__main__': unittest.main()
