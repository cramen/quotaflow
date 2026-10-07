"""Immutable job transfers reject changed, duplicate and unsafe inputs."""
from pathlib import Path
import tempfile
import unittest
import warnings
import zipfile
from unittest.mock import patch
from release_common import sha256
from release_transfer import pack, unpack


class TransferTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup); self.root=Path(self.temp.name)
        self.source=self.root/'source'; self.source.mkdir(); (self.source/'bytes').write_text('immutable candidate')
    def test_deterministic_transfer_and_no_overwrite(self):
        first=self.root/'first.zip'; second=self.root/'second.zip'
        digest=pack(self.source,first); self.assertEqual(digest,pack(self.source,second))
        target=self.root/'target'; unpack(first,digest,target)
        self.assertEqual('immutable candidate',(target/'bytes').read_text())
        with self.assertRaises(FileExistsError): pack(self.source,first)
        with self.assertRaises(FileExistsError): unpack(first,digest,target)
    def test_corruption_and_symlinks_are_refused(self):
        archive=self.root/'candidate.zip'; digest=pack(self.source,archive); archive.write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError,'digest'): unpack(archive,digest,self.root/'target')
        (self.source/'link').symlink_to(self.source/'bytes')
        with self.assertRaisesRegex(ValueError,'Nonregular'): pack(self.source,self.root/'links.zip')
    def test_transfer_does_not_load_whole_files_into_memory(self):
        with patch.object(Path,'read_bytes',side_effect=AssertionError('Whole-file allocation forbidden')):
            archive=self.root/'streamed.zip';digest=pack(self.source,archive)
            unpack(archive,digest,self.root/'restored')
    def test_zip_traversal_and_duplicates_are_refused_before_extraction(self):
        for fault in ('../outside','/absolute','duplicate'):
            archive=self.root/(str(len(fault))+'.zip')
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                with zipfile.ZipFile(archive,'w') as output:
                    output.writestr(fault,b'bytes')
                    if fault=='duplicate': output.writestr(fault,b'other')
            with self.assertRaises(ValueError): unpack(archive,sha256(archive),self.root/'target')
            self.assertFalse((self.root/'target').exists())


if __name__=='__main__': unittest.main()
