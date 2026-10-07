"""Check release job trust boundaries using Ruby's standard safe YAML parser."""
import json
import re
import subprocess
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


class WorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text=(ROOT/'.github/workflows/release.yml').read_text()
        cls.workflow=json.loads(subprocess.check_output(['ruby','-ryaml','-rjson','-e',
            'puts JSON.generate(YAML.safe_load(File.read(ARGV[0])))',str(ROOT/'.github/workflows/release.yml')],text=True))
        cls.jobs=cls.workflow['jobs']
    def test_actions_are_commit_pinned_and_uploads_cannot_overwrite(self):
        for job in self.jobs.values():
            for step in job['steps']:
                if 'uses' in step: self.assertRegex(step['uses'],r'^[\w/-]+@[0-9a-f]{40}$')
                if step.get('uses','').startswith('actions/upload-artifact@'):
                    self.assertIsNot(step.get('with',{}).get('overwrite'),True)
                if step.get('uses','').startswith('actions/download-artifact@'):
                    self.assertIn('artifact-ids',step['with']); self.assertTrue(step['with']['merge-multiple'])
    def test_permissions_and_secret_ownership_are_separate(self):
        self.assertEqual({'contents':'read'},self.workflow['permissions'])
        for name,job in self.jobs.items():
            permissions=job.get('permissions',self.workflow['permissions'])
            self.assertEqual(name=='sign',permissions.get('id-token')=='write')
            self.assertEqual(name=='promote',permissions.get('contents')=='write')
            text=json.dumps(job)
            if name!='promote': self.assertNotIn('secrets.CENTRAL_',text)
            if name!='sign': self.assertNotIn('secrets.SIGNING_',text)
        self.assertIn('environment',self.jobs['sign']); self.assertIn('environment',self.jobs['promote'])
    def test_promotion_is_gated_serialized_and_does_not_rebuild(self):
        self.assertEqual('prepare',self.jobs['sign']['needs'])
        self.assertEqual('sign',self.jobs['accept']['needs'])
        self.assertEqual('accept',self.jobs['promote']['needs'])
        self.assertFalse(self.workflow['concurrency']['cancel-in-progress'])
        self.assertIn('github.ref',self.workflow['concurrency']['group'])
        for name in ('sign','accept','promote'):
            for step in self.jobs[name]['steps']:
                run=step.get('run','')
                self.assertNotRegex(run,r'gradlew|\bmvn\b|prepare_release.py prepare')
        self.assertNotIn('publishAndReleaseToMavenCentral',self.text)
        self.assertIn('publish_release.py',json.dumps(self.jobs['promote']))
    def test_local_benchmarks_are_imported_not_run(self):
        for job in self.jobs.values():
            for step in job['steps']:
                self.assertNotRegex(step.get('run',''),r'run_performance|run_benchmark|\bjmh\b|performanceGate|benchmark\.sh')
        self.assertIn('import_release_bundle.py',self.text)
        for pin in ('RELEASE_EVIDENCE_SHA256','RELEASE_MANIFEST_SHA256','BASELINE_REVIEW_SHA256'):
            self.assertIn(pin,self.text)
    def test_transfer_and_resume_identities_are_independently_bound(self):
        inputs=self.workflow['on']['workflow_dispatch']['inputs']
        for required in ('candidate_run_id','signed_artifact_id','manifest_sha256','transfer_sha256','recovered_deployment'):
            self.assertIn(required,inputs)
        for job in ('sign','accept','promote'):
            runs='\n'.join(step.get('run','') for step in self.jobs[job]['steps'])
            self.assertIn('release_transfer.py unpack',runs)
            self.assertIn('--sha256 "$TRANSFER_SHA256"',runs)
        self.assertIn('inputs.operation !=',json.dumps(self.jobs['prepare']))
    def test_referenced_scripts_exist_and_inputs_are_not_injected_into_shell(self):
        for script in re.findall(r'scripts/[a-zA-Z0-9_]+\.py',self.text): self.assertTrue((ROOT/script).is_file(),script)
        for job in self.jobs.values():
            for step in job['steps']:
                self.assertNotIn('${{ inputs.',step.get('run',''))


if __name__=='__main__': unittest.main()
