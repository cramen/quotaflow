"""Assembly faults are rejected before any build or publication can occur."""
import copy
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import test_release_preparation as preparation_tests
from prepare_release import inspect_repository, production_sbom
from release_candidate import assemble, reference, verify_sealed
from release_common import MODULES, sha256
from release_promotion import encoded
from release_signatures import gpg, gpg_home, sign_assets, verify_pgp


class AssemblyTest(unittest.TestCase):
    def setUp(self):
        self.fixture = preparation_tests.PublicationTest(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.prepared = self.root / 'prepared'; self.prepared.mkdir()
        shutil.copytree(self.fixture.root, self.prepared / 'repository')
        binaries, assets = inspect_repository(self.prepared/'repository', self.fixture.version, self.fixture.graph)
        graph = self.prepared/'graph.json'; graph.write_text(json.dumps(self.fixture.graph))
        sbom = self.prepared/'sbom.json'; sbom.write_text(json.dumps(production_sbom(self.fixture.graph, self.fixture.version, binaries)))
        validation = self.prepared/'validation.json'
        validation.write_text(json.dumps({'status':'PASS', 'schema':'CycloneDX 1.6', 'sbomSha256':sha256(sbom),
                                          'validator':'org.cyclonedx:cyclonedx-core-java:13.1.0'}))
        self.manifest = {'schemaVersion':1, 'state':'PREPARED_UNSIGNED', 'mode':'dry-run', 'tag':None,
                         'modules':list(MODULES), 'candidate':{'version':self.fixture.version, 'commit':'a'*40,
                         'sourceSha256':'b'*64, 'artifacts':binaries},
                         'assets':[{**a, 'path':'repository/'+a['path']} for a in assets],
                         'productionGraph':reference(self.prepared,graph), 'sbom':reference(self.prepared,sbom),
                         'sbomValidation':reference(self.prepared,validation)}
        self.write_manifest(); self.output = self.root/'sealed'

    def write_manifest(self): (self.prepared/'candidate-manifest.json').write_text(json.dumps(self.manifest))
    def seal(self, **kwargs): return assemble(self.prepared, self.output, rehearsal=True, **kwargs)

    def test_complete_deterministic_bundle_and_canonical_manifest_without_build(self):
        with patch('subprocess.run', side_effect=AssertionError('Assembly must not run tools')):
            result = self.seal()
            self.assertFalse(result['eligibleForPromotion']); self.assertEqual('rehearsal', result['mode'])
            self.assertEqual(7, len(result['candidate']['artifacts']))
            first = sha256(self.output/'release-manifest.json')
            verify_sealed(self.output, first)
            other = self.root/'second'; assemble(self.prepared, other, rehearsal=True)
            self.assertEqual(first, sha256(other/'release-manifest.json'))
            self.assertEqual((self.output/'release-manifest.json').read_bytes(), encoded(result))

    def test_missing_changed_duplicate_and_wrong_version_inputs_fail(self):
        original = copy.deepcopy(self.manifest)
        for kind in ('duplicate', 'missing', 'hash', 'version'):
            with self.subTest(kind=kind):
                self.manifest = copy.deepcopy(original)
                if kind == 'duplicate': self.manifest['assets'].append(self.manifest['assets'][0])
                elif kind == 'missing': self.manifest['assets'].pop()
                elif kind == 'hash': self.manifest['assets'][0]['sha256'] = '0'*64
                else: self.manifest['candidate']['version'] = '2.0.0'
                self.write_manifest()
                with self.assertRaises(ValueError): self.seal()
                self.assertFalse(self.output.exists())

    def test_sbom_content_is_checked_even_with_updated_digest(self):
        path = self.prepared/'sbom.json'; sbom = json.loads(path.read_text()); sbom['components'].pop()
        path.write_text(json.dumps(sbom)); self.manifest['sbom'] = reference(self.prepared,path); self.write_manifest()
        with self.assertRaisesRegex(ValueError,'SBOM differs'): self.seal()

    def test_no_overwrite_of_an_existing_sealed_candidate(self):
        self.seal(); digest = sha256(self.output/'release-manifest.json')
        with self.assertRaisesRegex(ValueError,'replace'): self.seal()
        self.assertEqual(digest,sha256(self.output/'release-manifest.json'))

    def test_changed_missing_or_extra_sealed_files_fail(self):
        sealed = self.seal(); digest = sha256(self.output/'release-manifest.json')
        extra = self.output/'unlisted.txt'; extra.write_text('extra')
        with self.assertRaisesRegex(ValueError,'inventory'): verify_sealed(self.output,digest)
        extra.unlink()
        asset = self.output/sealed['mavenAssets'][0]['path']; asset.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'digest'): verify_sealed(self.output,digest)
        asset.unlink()
        with self.assertRaises(ValueError): verify_sealed(self.output,digest)

    def test_mismatched_bundle_fails_even_if_outer_hash_is_updated(self):
        sealed = self.seal(); bundle = self.output/sealed['mavenBundle']['path']
        with zipfile.ZipFile(bundle,'a') as archive: archive.writestr('unexpected.jar', b'not part of the candidate')
        ref = reference(self.output,bundle); sealed['mavenBundle'] = ref
        sealed['files'] = [ref if x['path']==ref['path'] else x for x in sealed['files']]
        manifest = self.output/'release-manifest.json'; manifest.write_bytes(encoded(sealed))
        with self.assertRaisesRegex(ValueError,'bundle file inventory'): verify_sealed(self.output,sha256(manifest))

    def test_report_bytes_are_bound_but_not_claimed_as_passing(self):
        reports = self.root/'reports'; reports.mkdir(); (reports/'result.json').write_text('{"status":"FAILED"}')
        sealed = self.seal(reports={'security':reports})
        self.assertFalse(sealed['eligibleForPromotion'])
        self.assertEqual('reports/security',sealed['reportRoots']['security'])
        digest = sha256(self.output/'release-manifest.json')
        (self.output/'reports/security/result.json').write_text('{"status":"PASS"}')
        with self.assertRaisesRegex(ValueError,'digest'): verify_sealed(self.output,digest)

    def test_symlink_or_unknown_report_role_is_refused(self):
        reports = self.root/'reports'; reports.mkdir(); (reports/'secret').symlink_to(self.prepared/'graph.json')
        with self.assertRaisesRegex(ValueError,'Nonregular'): self.seal(reports={'security':reports})
        with self.assertRaisesRegex(ValueError,'Unknown'): self.seal(reports={'unknown':reports})
        self.assertFalse(self.output.exists())

    def test_production_cannot_accept_unsigned_dry_run(self):
        with self.assertRaisesRegex(ValueError,'Dry-run'): assemble(self.prepared,self.output)
        self.assertFalse(self.output.exists())

    def test_real_ephemeral_signatures_survive_sealing_and_cover_every_asset(self):
        with gpg_home() as home:
            gpg(home,['--pinentry-mode','loopback','--passphrase','','--quick-generate-key',
                      'Assembly Test <assembly@example.invalid>','ed25519','sign','1d'])
            identity = next(line.split(':')[9] for line in gpg(home,['--with-colons','--list-keys']).splitlines() if line.startswith('fpr:'))
            public = home/'public.asc'; public.write_text(gpg(home,['--armor','--export',identity]))
            private = gpg(home,['--pinentry-mode','loopback','--passphrase','','--armor','--export-secret-keys',identity])
            report = sign_assets(self.prepared,private,'',public,identity,test_only=True)
            sealed = self.seal()
            self.assertEqual(35,len(report['signedAssets']))
            copied = json.loads((self.output/sealed['pgp']['path']).read_text())
            verify_pgp(self.output,copied,self.manifest,identity,production=False)
            with self.assertRaisesRegex(ValueError,'Test signatures'): verify_pgp(self.output,copied,self.manifest,identity)
            with zipfile.ZipFile(self.output/sealed['mavenBundle']['path']) as archive:
                self.assertEqual(35,len([p for p in archive.namelist() if p.endswith('.asc')]))
                self.assertEqual(35,len([p for p in archive.namelist() if p.endswith('.asc.sha256')]))


if __name__ == '__main__': unittest.main()
