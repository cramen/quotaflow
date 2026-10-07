"""Resolved classpaths, scan inputs and fixture evidence must remain bound."""
import copy
import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from release_acceptance import verify_runtime_report
from release_candidate import reference
from release_tools import ROOT
from run_staged_verification import runtime_sbom, isolated_environment, classpath_evidence


class RuntimeEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.base=self.root/'reports/consumers';self.inputs=self.base/'runtime-input';self.scan=self.base/'runtime-security'
        self.inputs.mkdir(parents=True);self.scan.mkdir()
        self.candidate={'sourceSha256':'a'*64,'version':'1.0.0','artifacts':{'core.jar':'b'*64}}
        self.runs=[{'id':'core-maven-jdk17','runtime':[{'purl':'pkg:maven/example/library@1.0','sha256':'c'*64}]}]
        self.manifest={'candidate':self.candidate,'preparation':{'sha256':'d'*64},'files':[]}
        self.report={'runs':self.runs}
        self.raw={'descriptor':{'name':'grype','version':'0.120.0','timestamp':dt.datetime.now(dt.timezone.utc).isoformat(),
                               'configuration':{},'db':{'status':{'valid':True,'built':(dt.datetime.now(dt.timezone.utc)-dt.timedelta(hours=1)).isoformat(),'schemaVersion':'v6'}}},'matches':[]}
        reviews=patch('consumer_security.scoped_reviews',return_value={'schemaVersion':1,'reviews':[]});reviews.start();self.addCleanup(reviews.stop)
        self.write()
    def write(self):
        sbom=self.inputs/'sbom.json';sbom.write_text(json.dumps(runtime_sbom(self.runs,'1.0.0')))
        input_manifest={'candidate':self.candidate,'preparedManifestSha256':'d'*64,'sbom':reference(self.inputs,sbom)}
        (self.inputs/'candidate-manifest.json').write_text(json.dumps(input_manifest))
        raw=self.scan/'raw.json';raw.write_text(json.dumps(self.raw))
        db=self.scan/'db';db.write_text('synthetic scanner identity fixture')
        triage=self.scan/'triage.json';triage.write_text('{"schemaVersion":1,"reviews":[]}')
        tool=json.loads((ROOT/'verification/release-tooling.json').read_text())['tools']['grype'];platform=next(iter(tool['platforms']))
        scan={'schemaVersion':1,'status':'PASS','candidate':self.candidate,'candidateManifestSha256':reference(self.inputs,self.inputs/'candidate-manifest.json')['sha256'],
              'createdAt':dt.datetime.now(dt.timezone.utc).isoformat(),'sbomSha256':reference(self.inputs,sbom)['sha256'],
              'scanner':{'version':tool['version'],'platform':platform,'downloadSha256':tool['platforms'][platform]['sha256']},
              'database':reference(self.scan,db),'rawReport':reference(self.scan,raw),'triage':reference(self.scan,triage)}
        (self.scan/'security.json').write_text(json.dumps(scan));self.bind()
    def bind(self):
        self.report['runtimeSecurity']={key:reference(self.base,path) for key,path in {'inputManifest':self.inputs/'candidate-manifest.json','sbom':self.inputs/'sbom.json','report':self.scan/'security.json'}.items()}
        self.manifest['files']=[reference(self.root,path) for path in self.root.rglob('*') if path.is_file()]
    def check(self):return verify_runtime_report(self.root,self.manifest,self.report)
    def test_complete_runtime_scan_is_revalidated(self):
        self.assertEqual('PASS',self.check()['status'])
    def test_different_resolved_bytes_or_unbound_input_fail(self):
        self.runs[0]['runtime'][0]['sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'differs from resolved'):self.check()
        self.runs[0]['runtime'][0]['sha256']='c'*64
        self.manifest['files']=[f for f in self.manifest['files'] if not f['path'].endswith('/db')]
        with self.assertRaisesRegex(ValueError,'bound evidence'):self.check()
    def test_pass_label_cannot_hide_a_runtime_vulnerability(self):
        self.raw['matches']=[{'vulnerability':{'id':'GHSA-abcd-1234-abcd','severity':'Critical'},'artifact':{'purl':'pkg:maven/example/library@1.0'}}]
        self.write()
        with self.assertRaisesRegex(ValueError,'vulnerability blocks'):self.check()
    def test_other_prepared_manifest_is_rejected(self):
        path=self.inputs/'candidate-manifest.json';value=json.loads(path.read_text());value['preparedManifestSha256']='0'*64
        path.write_text(json.dumps(value));self.bind()
        with self.assertRaisesRegex(ValueError,'input identity'):self.check()


class ReadinessTest(unittest.TestCase):
    def test_missing_identity_never_becomes_ready(self):
        import test_release_candidate as fixtures
        from release_readiness import audit
        fixture=fixtures.AssemblyTest();fixture.setUp();self.addCleanup(fixture.doCleanups)
        with patch('release_readiness.source_identity',return_value=({},fixture.manifest['candidate']['sourceSha256'])):
            report=audit(fixture.prepared)
        self.assertEqual('UNVERIFIED',report['status']);self.assertIn('productionAcceptance',report['unmet'])
        self.assertIn('operatorPrerequisites',report['unmet'])
        self.assertTrue(all(value['status']=='UNVERIFIED' for value in report['auditCoverage'].values()))


class EnvironmentTest(unittest.TestCase):
    def test_ambient_application_settings_credentials_and_launcher_options_are_removed(self):
        unsafe={'QUOTAFLOW_REDIS_URL':'external-endpoint','SPRING_APPLICATION_JSON':'external-config',
                'quotaflow.namespace':'external-namespace','MAVEN_ARGS':'deploy','MAVEN_OPTS':'-javaagent:untrusted',
                '_JAVA_OPTIONS':'-Dquotaflow.redis.url=external','JAVA_OPTS':'unsafe','SIGNING_KEY':'private',
                'GH_TOKEN':'private','GRADLE_USER_HOME':'untrusted-home'}
        with patch.dict('os.environ',unsafe):
            environment=isolated_environment(Path('/isolated'),Path('/jdk'))
        for key in unsafe:
            if key!='GRADLE_USER_HOME':self.assertNotIn(key,environment)
        self.assertEqual('/isolated/gradle-home',environment['GRADLE_USER_HOME'])
        self.assertEqual('true',environment['MAVEN_SKIP_RC'])
        self.assertEqual('/jdk',environment['JAVA_HOME'])
    def test_old_maven_transitive_netty_is_rejected_before_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'maven-repository/io/netty/netty-handler/4.2.13.Final/netty-handler-4.2.13.Final.jar'
            path.parent.mkdir(parents=True);path.write_bytes(b'classpath fixture')
            with self.assertRaisesRegex(ValueError,'unaligned Netty'):
                classpath_evidence(str(path),{'artifacts':{}},'redis')


if __name__=='__main__':unittest.main()
