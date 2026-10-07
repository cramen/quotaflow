"""Synthetic gate controls; fixture results are never production certification."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

import test_release_evidence as quality_tests
import test_release_security as security_tests
from release_candidate import reference
from release_evidence_gate import quality, security
from release_tools import ROOT


class QualityGateTest(unittest.TestCase):
    def setUp(self):
        self.fixture = quality_tests.ReleaseEvidenceTest(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups); self.fixture.check()
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); (self.root/'reports/quality').mkdir(parents=True)
        self.manifest = {'candidate':copy.deepcopy(self.fixture.candidate), 'reportRoots':{'quality':'reports/quality'}}
        self.pack()

    def pack(self):
        ref = self.fixture.write('manifest.json', self.fixture.manifest)
        path = self.root/'reports/quality/verification.zip'
        with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as archive:
            for item in self.fixture.root.rglob('*'):
                if item.is_file(): archive.write(item,item.relative_to(self.fixture.root).as_posix())
        entry = reference(self.root,path); self.manifest['files'] = [entry]
        self.pins = {'archiveSha256':entry['sha256'],'manifestSha256':ref['sha256'],
                     'baselineReviewSha256':self.fixture.performance.review_digest}

    def test_real_quality_validator_is_run_on_all_sixteen_synthetic_stages(self):
        result = quality(self.root,self.manifest,self.pins)
        self.assertEqual('PASS',result['status']); self.assertEqual(16,len(result['stages']))

    def test_each_independent_pin_is_required_and_enforced(self):
        for name in self.pins:
            with self.subTest(pin=name):
                pins = dict(self.pins); pins[name] = '0'*64
                with self.assertRaises(ValueError): quality(self.root,self.manifest,pins)
                del pins[name]
                with self.assertRaises(ValueError): quality(self.root,self.manifest,pins)

    def test_changed_commit_version_source_or_binary_cannot_reuse_green_evidence(self):
        for field in ('commit','version','sourceSha256','artifacts'):
            manifest = copy.deepcopy(self.manifest)
            manifest['candidate'][field] = {'different.jar':'0'*64} if field=='artifacts' else 'different'
            with self.subTest(field=field), self.assertRaisesRegex(ValueError,'Wrong release candidate'):
                quality(self.root,manifest,self.pins)

    def test_omitted_soak_is_refused_even_with_new_outer_pins(self):
        del self.fixture.manifest['stages']['soak']; self.pack()
        with self.assertRaisesRegex(ValueError,'Incomplete release evidence'): quality(self.root,self.manifest,self.pins)

    def test_missing_or_tampered_archive_and_missing_performance_fail(self):
        path=self.root/'reports/quality/verification.zip'; original=path.read_bytes()
        path.write_bytes(original+b'changed')
        with self.assertRaisesRegex(ValueError,'digest'): quality(self.root,self.manifest,self.pins)
        path.unlink()
        with self.assertRaises(ValueError): quality(self.root,self.manifest,self.pins)
        del self.fixture.manifest['stages']['performance']; self.pack()
        with self.assertRaisesRegex(ValueError,'Incomplete release evidence'): quality(self.root,self.manifest,self.pins)


class SecurityGateTest(unittest.TestCase):
    def setUp(self):
        self.fixture = security_tests.SecurityGateTest(); self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root; self.directory = self.root/'reports/security'; self.directory.mkdir(parents=True)
        self.manifest = {'candidate':self.fixture.candidate,'preparation':{'sha256':'d'*64},
                         'sbom':{'sha256':'e'*64},'reportRoots':{'security':'reports/security'}}
        self.write()

    def write(self):
        raw = self.directory/'raw-scan.json'; raw.write_text(json.dumps(self.fixture.raw))
        database=self.directory/'vulnerability.db'; database.write_text('synthetic database identity fixture')
        triage=self.directory/'reachability.json'; triage.write_text('{"reviews":[]}')
        tools=json.loads((ROOT/'verification/release-tooling.json').read_text())['tools']['grype']
        platform=next(iter(tools['platforms']))
        report={'schemaVersion':1,'candidate':self.fixture.candidate,'candidateManifestSha256':'d'*64,
                'sbomSha256':'e'*64,'status':'PASS','createdAt':self.fixture.now.isoformat(),
                'scanner':{'version':tools['version'],'platform':platform,'downloadSha256':tools['platforms'][platform]['sha256']},
                'database':reference(self.directory,database),'rawReport':reference(self.directory,raw),'triage':reference(self.directory,triage)}
        (self.directory/'security.json').write_text(json.dumps(report))
        self.manifest['files']=[reference(self.root,p) for p in self.directory.iterdir()]

    def check(self): return security(self.root,self.manifest,now=self.fixture.now)

    def test_security_evidence_is_revalidated_and_not_trusted_by_pass_label(self):
        self.assertEqual('PASS',self.check()['status'])
        self.fixture.raw['matches']=[self.fixture.finding]; self.write()
        with self.assertRaisesRegex(ValueError,'vulnerability blocks'): self.check()

    def test_expired_scan_and_unavailable_database_block_the_gate(self):
        self.fixture.raw['descriptor']['timestamp']='2020-01-01T00:00:00Z'; self.write()
        with self.assertRaisesRegex(ValueError,'Stale'): self.check()
        self.fixture.raw['descriptor']['timestamp']=self.fixture.now.isoformat()
        self.fixture.raw['descriptor']['db']['status']['valid']=False; self.write()
        with self.assertRaisesRegex(ValueError,'unavailable'): self.check()

    def test_prepared_identity_missing_evidence_and_unbound_subreport_fail(self):
        self.manifest['preparation']['sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'prepared manifest'): self.check()
        self.manifest['preparation']['sha256']='d'*64
        self.manifest['files']=[p for p in self.manifest['files'] if not p['path'].endswith('vulnerability.db')]
        with self.assertRaisesRegex(ValueError,'bound evidence'): self.check()
        self.manifest['reportRoots']={}
        with self.assertRaisesRegex(ValueError,'Missing security'): self.check()


if __name__ == '__main__': unittest.main()
