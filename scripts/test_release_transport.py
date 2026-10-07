"""Portal wire contract and durable journal recovery, using in-memory transports."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from release_promotion import encoded
from release_transport import GitHubJournal, Portal
from test_release_promotion import GitHub


class DraftGitHub(GitHub):
    def find(self, tag): return {'id': 123, 'tag_name': tag, 'draft': not self.complete}


class TransportTest(unittest.TestCase):
    def test_portal_wire_contract_uses_user_managed_bundle_and_one_id(self):
        with tempfile.TemporaryDirectory() as directory:
            bundle = Path(directory)/'candidate.zip'; bundle.write_bytes(b'immutable ZIP bytes')
            portal = Portal({'CENTRAL_PORTAL_USERNAME': 'dummy-user', 'CENTRAL_PORTAL_TOKEN': 'dummy-token'})
            deployment = '28570f16-da32-4c14-bd2e-c1acc0782365'
            with patch('release_transport.request', return_value=deployment.encode()) as wire:
                self.assertEqual(deployment, portal.upload(bundle, 'quotaflow-1.0.0', hashlib.sha256(bundle.read_bytes()).hexdigest()))
                args = wire.call_args.args
                self.assertEqual('POST', args[0]); self.assertIn('publishingType=USER_MANAGED', args[1])
                self.assertNotIn('AUTOMATIC', args[1]); self.assertIn(b'name="bundle"', args[3])
                self.assertIn(bundle.read_bytes(), args[3]); self.assertIn('multipart/form-data;', args[4])
            with patch('release_transport.request', return_value=b'{"deploymentState":"VALIDATED"}') as wire:
                portal.status(deployment)
                self.assertIn('/status?id=' + deployment, wire.call_args.args[1])
                portal.promote(deployment)
                self.assertTrue(wire.call_args.args[1].endswith('/deployment/' + deployment))
            with patch('release_transport.request', return_value=b'bytes') as wire:
                portal.download(deployment, 'io/github/cramen/core.jar')
                self.assertIn('/deployment/' + deployment + '/download/', wire.call_args.args[1])
                portal.download(deployment, 'io/github/cramen/core.jar', published=True)
                self.assertEqual(('GET', 'https://repo.maven.apache.org/maven2/io/github/cramen/core.jar'), wire.call_args.args)

    def test_no_credentials_or_unsafe_paths_never_reach_transport(self):
        with patch('release_transport.request') as wire:
            with self.assertRaises(ValueError): Portal({})
            portal = Portal({'CENTRAL_PORTAL_USERNAME': 'dummy', 'CENTRAL_PORTAL_TOKEN': 'dummy'})
            with self.assertRaises(ValueError): portal.download('id', '../secret')
            with self.assertRaises(ValueError): portal.status('../other')
            wire.assert_not_called()

    def test_changed_upload_payload_never_reaches_http(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'candidate.zip'; path.write_bytes(b'changed')
            portal=Portal({'CENTRAL_PORTAL_USERNAME':'dummy','CENTRAL_PORTAL_TOKEN':'dummy'})
            with patch('release_transport.request') as wire:
                with self.assertRaisesRegex(ValueError,'changed before upload'): portal.upload(path,'candidate','0'*64)
                wire.assert_not_called()

    def test_new_process_loads_the_same_durable_deployment_identity(self):
        github = DraftGitHub(); journal = GitHubJournal(github, 'v1.0.0')
        state = {'identity': {'version': '1.0.0'}, 'release': 123, 'phase': 'UPLOAD_INTENT', 'deployment': None}
        journal.save(state)
        state = {**state, 'phase': 'UPLOADED', 'deployment': 'known-deployment'}
        journal.save(state)
        self.assertEqual(state, GitHubJournal(github, 'v1.0.0').load())
        journal.save(state); self.assertEqual(2, len(github.files))

    def test_lost_journal_response_is_resolved_by_readback(self):
        github = DraftGitHub(); journal = GitHubJournal(github, 'v1.0.0')
        state = {'identity': {}, 'release': 123, 'phase': 'UPLOAD_INTENT'}
        original = github.upload_asset
        def lost(*args): original(*args); raise OSError('lost response')
        with patch.object(github, 'upload_asset', side_effect=lost):
            with self.assertRaises(OSError): journal.save(state)
        self.assertEqual(state, GitHubJournal(github, 'v1.0.0').load())

    def test_corrupted_missing_or_forked_journal_cannot_resume(self):
        for corruption in ('bytes', 'gap', 'fork'):
            with self.subTest(corruption=corruption):
                github = DraftGitHub(); journal = GitHubJournal(github, 'v1.0.0')
                state = {'identity': {}, 'release': 123, 'phase': 'UPLOAD_INTENT'}
                journal.save(state); journal.save({**state, 'phase': 'UPLOADED'})
                first = sorted(github.files)[0]
                if corruption == 'bytes': github.files[first] = b'changed'
                elif corruption == 'gap': del github.files[first]
                else:
                    record = json.loads(github.files[first]); record['state']['phase'] = 'other'
                    data = encoded(record)
                    github.files[journal.prefix + '000000-' + hashlib.sha256(data).hexdigest() + '.json'] = data
                with self.assertRaises(ValueError): journal.load()


if __name__ == '__main__': unittest.main()
