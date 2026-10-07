"""Production gate integration controls; no production credentials or publication."""
import contextlib
import copy
import json
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import test_release_candidate as assembly_tests
import test_release_signatures as signature_tests
from publish_release import publish_once
from release_acceptance import expected_cases, production_identity, staged_report, verify
from release_candidate import reference
from release_common import sha256
from release_promotion import encoded
from release_rehearsal import LocalGitHub, LocalPortal


class SignatureIdentityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls): signature_tests.PgpTest.setUpClass()
    @classmethod
    def tearDownClass(cls): signature_tests.PgpTest.tearDownClass()
    def setUp(self):
        self.fixture=signature_tests.PgpTest(); self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        self.report=self.fixture.sign()
        # This test installs an ephemeral identity only in the in-memory policy.
        # The repository policy remains unset and can never trust this fixture.
        self.policy={'pgp':{'fingerprint':self.fixture.identity,'publicKey':str(self.fixture.public)}}
        self.preparation=self.fixture.root/'candidate-manifest.json'
        self.manifest={'mode':'release','candidate':self.fixture.manifest['candidate'],
                       'preparation':reference(self.fixture.root,self.preparation),
                       'pgp':reference(self.fixture.root,self.fixture.root/'signatures/pgp/pgp.json'),
                       'mavenAssets':self.fixture.manifest['assets']+[s['signature'] for s in self.report['signedAssets']]}
    def persist(self):
        path=self.fixture.root/'signatures/pgp/pgp.json'; path.write_text(json.dumps(self.report))
        self.manifest['pgp']=reference(self.fixture.root,path)
    def check(self): return production_identity(self.fixture.root,self.manifest,self.policy)
    def test_test_identity_and_missing_configured_key_are_rejected(self):
        with self.assertRaisesRegex(ValueError,'Test signatures'): self.check()
        self.policy['pgp']['fingerprint']=None
        with self.assertRaisesRegex(ValueError,'trusted PGP'): self.check()
    def test_trusted_fixture_verifies_real_crypto_and_bundle_binding(self):
        self.report['production']=True; self.persist()
        self.assertEqual('PASS',self.check()['status'])
        self.manifest['mavenAssets'][-1]['sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'bundle inventory'): self.check()
    def test_wrong_key_missing_or_modified_signature_is_refused(self):
        self.report['production']=True; self.persist()
        self.policy['pgp']['fingerprint']='0'*40
        with self.assertRaises(ValueError): self.check()
        self.policy['pgp']['fingerprint']=self.fixture.identity
        signature=self.fixture.root/self.report['signedAssets'][0]['signature']['path']
        signature.write_text('tampered')
        with self.assertRaises(ValueError): self.check()
        signature.unlink()
        with self.assertRaises(ValueError): self.check()


class StagedReportTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup); self.root=Path(self.temp.name)
        runtime=patch('release_acceptance.verify_runtime_report',return_value={'status':'PASS'})
        runtime.start(); self.addCleanup(runtime.stop)
        names=('core','kotlin','store-redis','micrometer','spring-boot-starter')
        self.manifest={'candidate':{'version':'1.0.0','sourceSha256':'c'*64,'artifacts':{'quotaflow-'+name+'-1.0.0.jar':'a'*64 for name in names}},
                       'preparation':{'sha256':'b'*64},'reportRoots':{},'files':[]}
    def build(self,role):
        directory=self.root/'reports'/role; directory.mkdir(parents=True,exist_ok=True)
        runs=[]
        for name,jdk in expected_cases(role).items():
            log=directory/(name+'.log'); log.write_text('PUBLISHED '+role.upper()+' VERIFIED case='+name+'\n'+
                ('STAGED RECOVERY VERIFIED unready=reject duplicate=reject distributed=healthy\n' if name.startswith('starter-') else ''))
            runs.append({'id':name,'jdk':jdk,'status':'PASS','exitCode':0,'origin':'staged-repository',
                         'artifacts':dict(self.manifest['candidate']['artifacts']),'runtime':[{'purl':'pkg:maven/example/library@1','sha256':'a'*64}],
                         'log':reference(directory,log)})
        self.report={'schemaVersion':1,'kind':'staged-'+role,'candidate':self.manifest['candidate'],
                     'preparedManifestSha256':'b'*64,'executionSourceSha256':'c'*64,'simulation':False,'status':'PASS','runs':runs}
        self.manifest['reportRoots'][role]='reports/'+role; self.persist(role)
    def persist(self,role):
        path=self.root/'reports'/role/'results.json'; path.write_text(json.dumps(self.report))
        self.manifest['files']=[reference(self.root,p) for p in self.root.rglob('*') if p.is_file()]
    def test_complete_case_sets_require_real_logs_and_exact_artifacts(self):
        for role,count in (('consumers',60),('examples',5)):
            self.build(role); self.assertEqual(count,staged_report(self.root,self.manifest,role)['cases'])
            self.report['runs'][0]['artifacts']={'other.jar':'0'*64}; self.persist(role)
            with self.assertRaisesRegex(ValueError,'other candidate'): staged_report(self.root,self.manifest,role)
    def test_missing_case_simulation_and_failure_do_not_pass(self):
        for fault in ('case','simulation','exit','jdk','origin','marker'):
            self.build('consumers')
            if fault=='case': self.report['runs'].pop()
            elif fault=='simulation': self.report['simulation']=True
            elif fault=='exit': self.report['runs'][0]['exitCode']=1
            elif fault=='jdk': self.report['runs'][0]['jdk']=8
            elif fault=='origin': self.report['runs'][0]['origin']='source-project'
            else:
                run=self.report['runs'][0]; path=self.root/'reports/consumers'/run['log']['path']
                path.write_text('build succeeded but workload did not run'); run['log']=reference(path.parent,path)
            self.persist('consumers')
            with self.subTest(fault=fault),self.assertRaises(ValueError): staged_report(self.root,self.manifest,'consumers')


class PublicationBoundaryTest(unittest.TestCase):
    """Use local service doubles and mock only trust/evidence producers.

    Real validator-specific cryptographic/quality/security controls live above
    and in their dedicated suites. Here every production gate is exercised at
    the publishing boundary to prove that failure precedes mutation.
    """
    def setUp(self):
        self.fixture=assembly_tests.AssemblyTest(); self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        version=self.fixture.manifest['candidate']['version']
        self.fixture.manifest.update(mode='release',tag='v'+version); self.fixture.write_manifest()
        security_dir=self.fixture.root/'security'; security_dir.mkdir()
        (security_dir/'security.json').write_text('{"createdAt":"2026-01-01T00:00:00+00:00"}')
        manifest=self.fixture.seal(reports={'security':security_dir}); manifest['mode']='release'
        self.root=self.fixture.output; (self.root/'release-manifest.json').write_bytes(encoded(manifest))
        (self.root/'release-manifest.sigstore.json').write_text('{}')
        self.digest=sha256(self.root/'release-manifest.json'); self.manifest=manifest
        self.github=LocalGitHub(self.fixture.root/'github'); self.portal=LocalPortal(self.fixture.root/'portal',version)
        self.stack=contextlib.ExitStack(); self.addCleanup(self.stack.close)
        self.stack.enter_context(patch('local_release.Path.home',return_value=self.fixture.root))
        self.stack.enter_context(patch('local_release.github_token',return_value='test-token'))
        self.stack.enter_context(patch('publish_release.Portal',return_value=self.portal))
        self.stack.enter_context(patch('publish_release.GitHub',return_value=self.github))
        self.stack.enter_context(patch('publish_release.production_identity',return_value={'status':'PASS'}))
        self.stack.enter_context(patch('release_acceptance.tag_identity',return_value={'trustedMainCommit':'c'*40}))
        self.stack.enter_context(patch('release_acceptance.source_identity',return_value=({},manifest['candidate']['sourceSha256'])))
        self.gates={}
        for name in ('production_identity','quality','security','staged_report','verify_sigstore'):
            value={'status':'PASS'}
            if name=='verify_sigstore': value['bundleSha256']=sha256(self.root/'release-manifest.sigstore.json')
            self.gates[name]=self.stack.enter_context(patch('release_acceptance.'+name,return_value=value))
    def run_step(self,recovered=None): return publish_once(self.root,self.digest,{},recovered)
    def test_all_actual_gate_boundaries_precede_mutation(self):
        for name,mock in self.gates.items():
            mock.side_effect=ValueError(name+' failed')
            with self.subTest(gate=name),self.assertRaises(ValueError): self.run_step()
            mock.side_effect=None
            self.assertFalse((self.portal.root/'deployment.zip').exists())
            self.assertFalse((self.github.root/'release.json').exists())
    def test_missing_real_project_key_and_rehearsal_mode_stop_before_clients(self):
        with patch('publish_release.project_policy',return_value={'repository':'cramen/quotaflow','pgp':{'fingerprint':None}}), \
             patch('publish_release.production_identity',side_effect=production_identity), \
             patch('publish_release.Portal') as portal,patch('publish_release.GitHub') as github:
            with self.assertRaisesRegex(ValueError,'trusted PGP'): self.run_step()
            self.manifest['mode']='rehearsal'
            (self.root/'release-manifest.json').write_bytes(encoded(self.manifest))
            self.digest=sha256(self.root/'release-manifest.json')
            with self.assertRaisesRegex(ValueError,'Rehearsal packages'): self.run_step()
            portal.assert_not_called(); github.assert_not_called()
    def test_missing_credentials_wrong_tag_or_changed_checkout_prevent_writes(self):
        with patch('publish_release.Portal',side_effect=ValueError('Missing credential')):
            with self.assertRaisesRegex(ValueError,'Missing credential'): self.run_step()
        with patch('release_acceptance.tag_identity',side_effect=ValueError('Tag outside main')):
            with self.assertRaisesRegex(ValueError,'Tag outside main'): self.run_step()
        with patch('release_acceptance.source_identity',return_value=({},'0'*64)):
            with self.assertRaisesRegex(ValueError,'sources differ'): self.run_step()
        self.assertFalse((self.portal.root/'deployment.zip').exists())
        self.assertFalse((self.github.root/'release.json').exists())
    def test_consumers_and_examples_are_both_required_before_upload(self):
        for role in ('consumers','examples'):
            def check(root,manifest,actual_role,**kwargs):
                if actual_role==role: raise ValueError('Missing '+role)
                return {'status':'PASS'}
            self.gates['staged_report'].side_effect=check
            with self.subTest(role=role),self.assertRaisesRegex(ValueError,'Missing '+role): self.run_step()
            self.assertFalse((self.portal.root/'deployment.zip').exists())
    def test_security_freshness_relaxes_only_after_confirmed_publication(self):
        self.run_step()
        def historical(root,manifest,now=None):
            if now is None: raise ValueError('Expired scan')
            return {'status':'PASS'}
        self.gates['security'].side_effect=historical
        self.assertEqual('COMPLETE',self.run_step()['status'])
        state=self.portal.status('local-deployment-1'); state['deploymentState']='VALIDATED'
        (self.portal.root/'state.json').write_bytes(encoded(state))
        with self.assertRaisesRegex(ValueError,'not confirmed'): self.run_step()
    def test_one_complete_bundle_can_resume_with_exact_delivery_assets(self):
        first=self.run_step(); self.assertEqual('PENDING',first['status'])
        second=self.run_step(); self.assertEqual('COMPLETE',second['status'])
        third=self.run_step(); self.assertEqual('COMPLETE',third['status'])
        self.assertEqual(first['deliverySha256'],third['deliverySha256'])
        state=self.portal.status('local-deployment-1'); self.assertEqual((1,1),(state['uploads'],state['promotions']))
        self.assertEqual('completion-only',third['acceptance']['scope'])
    def test_forged_completed_journal_does_not_relax_freshness(self):
        self.run_step(); self.run_step()
        state=self.portal.status('local-deployment-1'); state['deploymentState']='VALIDATED'
        (self.portal.root/'state.json').write_bytes(encoded(state))
        with self.assertRaisesRegex(ValueError,'not confirmed'): self.run_step()
    def test_delivery_corruption_is_detected_before_draft(self):
        from publish_release import delivery_archive
        def corrupt(root,path):
            delivery_archive(root,path)
            import zipfile
            with zipfile.ZipFile(path,'a') as archive: archive.writestr('extra.txt','tampered')
        with patch('publish_release.delivery_archive',side_effect=corrupt):
            with self.assertRaisesRegex(ValueError,'Delivery inventory'): self.run_step()
        self.assertFalse((self.github.root/'release.json').exists())
    def test_ambiguous_upload_requires_verified_existing_id_and_never_reuploads(self):
        original=self.portal.upload
        def lost(*args): original(*args); raise OSError('lost upload response')
        with patch.object(self.portal,'upload',side_effect=lost) as upload:
            with self.assertRaises(OSError): self.run_step()
            for _ in range(2):
                with self.assertRaisesRegex(ValueError,'Upload outcome uncertain'): self.run_step()
            self.assertEqual(1,upload.call_count)
        with patch.object(self.portal,'download',return_value=b'wrong bytes'):
            with self.assertRaisesRegex(ValueError,'bytes differ'): self.run_step('local-deployment-1')
        self.assertEqual(0,self.portal.status('local-deployment-1')['promotions'])
        self.run_step('local-deployment-1'); self.assertEqual('COMPLETE',self.run_step()['status'])
        self.assertEqual(1,self.portal.status('local-deployment-1')['uploads'])

    def test_lost_promotion_response_uses_known_deployment(self):
        original=self.portal.promote
        def lost(*args): original(*args); raise OSError('lost promotion response')
        with patch.object(self.portal,'promote',side_effect=lost):
            with self.assertRaises(OSError): self.run_step()
        with patch.object(self.portal,'upload',side_effect=AssertionError('duplicate upload')):
            self.assertEqual('COMPLETE',self.run_step()['status'])
    def test_github_failure_after_central_resumes_only_completion(self):
        self.run_step()
        with patch.object(self.github,'publish',side_effect=OSError('GitHub outage')):
            with self.assertRaises(OSError): self.run_step()
        with patch.object(self.portal,'upload',side_effect=AssertionError('duplicate upload')), \
             patch.object(self.portal,'promote',side_effect=AssertionError('duplicate promotion')):
            self.assertEqual('COMPLETE',self.run_step()['status'])
    def test_existing_different_asset_is_not_replaced_and_prevents_central_upload(self):
        name='release-manifest.json'; original=b'other candidate'
        self.github.upload_asset(1,name,original)
        with self.assertRaisesRegex(ValueError,'collision'): self.run_step()
        self.assertEqual(original,self.github.download_asset(1,name))
        self.assertFalse((self.portal.root/'deployment.zip').exists())
    def test_journal_runner_loss_before_recording_id_is_uncertain(self):
        from release_transport import GitHubJournal
        original=GitHubJournal.save
        def fail(journal,state):
            if state['phase']=='UPLOADED': raise OSError('runner lost before durable ID')
            original(journal,state)
        with patch.object(GitHubJournal,'save',new=fail):
            with self.assertRaises(OSError): self.run_step()
        with self.assertRaisesRegex(ValueError,'Upload outcome uncertain'): self.run_step()
        self.run_step('local-deployment-1'); self.run_step()
        self.assertEqual(1,self.portal.status('local-deployment-1')['uploads'])


class PublicationWaitTest(unittest.TestCase):
    def invoke(self,extra=()):
        from publish_release import main
        arguments=['publish_release.py','--candidate','unused','--sha256','a'*64,
                   '--quality-archive-sha256','b'*64,'--quality-manifest-sha256','c'*64,'--baseline-review-sha256','d'*64,*extra]
        with patch('sys.argv',arguments),contextlib.redirect_stdout(io.StringIO()):return main()
    def test_pending_is_not_success_without_a_wait(self):
        with patch('publish_release.publish_once',return_value={'status':'PENDING'}) as publish:
            self.assertEqual(2,self.invoke());self.assertEqual(1,publish.call_count)
    def test_bounded_polling_reuses_the_same_candidate(self):
        with patch('publish_release.publish_once',side_effect=[{'status':'PENDING'},{'status':'COMPLETE'}]) as publish, \
             patch('publish_release.time.monotonic',side_effect=[0,1,1]),patch('publish_release.time.sleep') as sleep:
            self.assertEqual(0,self.invoke(['--wait-seconds','10','--poll-seconds','1']))
            self.assertEqual(publish.call_args_list[0],publish.call_args_list[1]);sleep.assert_called_once_with(1)
    def test_uncertain_mutation_is_never_polled_as_a_new_attempt(self):
        with patch('publish_release.publish_once',side_effect=OSError('response lost')) as publish,patch('publish_release.time.sleep') as sleep:
            self.assertEqual(1,self.invoke(['--wait-seconds','10']))
            self.assertEqual(1,publish.call_count);sleep.assert_not_called()


if __name__=='__main__': unittest.main()
