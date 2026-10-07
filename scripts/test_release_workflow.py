"""CI verifies sources but cannot sign or publish releases."""
import json
from pathlib import Path
import subprocess
import unittest
ROOT=Path(__file__).resolve().parents[1]

class WorkflowTest(unittest.TestCase):
    def test_ci_has_no_publishing_route_credentials_or_write_permissions(self):
        self.assertFalse((ROOT/'.github/workflows/release.yml').exists())
        for path in (ROOT/'.github/workflows').glob('*.yml'):
            text=path.read_text()
            workflow=json.loads(subprocess.check_output(['ruby','-ryaml','-rjson','-e',
                'puts JSON.generate(YAML.safe_load(File.read(ARGV[0])))',str(path)],text=True))
            self.assertEqual({'contents':'read'},workflow['permissions'])
            for job in workflow['jobs'].values():
                self.assertNotIn('write',job.get('permissions',{}).values())
            for forbidden in ('publish_release.py','pgp-sign','manifest-sign','CENTRAL_PORTAL_',
                              'secrets.SIGNING_', 'gh release', 'publishAndReleaseToMavenCentral'):
                self.assertNotIn(forbidden,text)
    def test_benchmarks_remain_local(self):
        for path in (ROOT/'.github/workflows').glob('*.yml'):
            self.assertNotRegex(path.read_text(),r'run_performance|run_benchmark|\bjmh\b|performanceGate|benchmark\.sh')

if __name__=='__main__':unittest.main()
