"""Local configuration precedence and publication serialization controls."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from local_release import properties,central_environment,pgp_configuration,publication_lock,github_token

class LocalTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.env=patch.dict(os.environ,{'GRADLE_USER_HOME':str(self.root)},clear=True);self.env.start();self.addCleanup(self.env.stop)
    def write(self,text): (self.root/'gradle.properties').write_text(text)
    def test_java_properties_escapes_and_continuations(self):
        self.write('# comment\nkey\\:id : value\\ with\\u0020spaces\nmulti=one\\\n  two\npass=a=b:c\n')
        self.assertEqual({'key:id':'value with spaces','multi':'onetwo','pass':'a=b:c'},properties(self.root/'gradle.properties'))
    def test_central_global_properties_and_environment_precedence(self):
        self.write('mavenCentralUsername=local\nmavenCentralPassword=secret\n')
        self.assertEqual('secret',central_environment()['ORG_GRADLE_PROJECT_mavenCentralPassword'])
        with patch.dict(os.environ,{'CENTRAL_PORTAL_USERNAME':'override'}):
            self.assertNotIn('ORG_GRADLE_PROJECT_mavenCentralUsername',central_environment())
    def test_keyring_is_binary_and_password_never_exposed(self):
        key=self.root/'key.gpg';key.write_bytes(b'\xff\x00key')
        self.write(f'signing.keyId=01234567\nsigning.password=secret\nsigning.secretKeyRingFile={key}\n')
        self.assertEqual((b'\xff\x00key','secret','01234567'),pgp_configuration())
    def test_version_lock_refuses_competing_process_and_releases(self):
        with patch('local_release.Path.home',return_value=self.root):
            with publication_lock('owner/repo','1.0.0'):
                with self.assertRaisesRegex(ValueError,'Another local process'):
                    with publication_lock('owner/repo','1.0.0'):pass
                with publication_lock('owner/repo','2.0.0'):pass
            with publication_lock('owner/repo','1.0.0'):pass
    def test_ci_cannot_publish(self):
        with patch.dict(os.environ,{'GITHUB_ACTIONS':'true'}):
            with self.assertRaisesRegex(ValueError,'local-only'):
                with publication_lock('owner/repo','1.0.0'):pass
    def test_github_environment_token_does_not_invoke_cli(self):
        with patch.dict(os.environ,{'GH_TOKEN':'secret'}),patch('local_release.subprocess.run') as run:
            self.assertEqual('secret',github_token());run.assert_not_called()

if __name__=='__main__':unittest.main()
