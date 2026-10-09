"""Fixture-only reachability cannot become a blanket vulnerability exclusion."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from consumer_security import ADVISORY, PACKAGE, CONTEXT, REVIEW_MARKERS, REVIEW_PROOFS, scoped_reviews
from release_common import sha256
from scan_release import artifact_set


class ConsumerReviewTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        (self.root/'verification').mkdir()
        for name in set().union(*REVIEW_PROOFS.values()):
            proof=self.root/name;proof.parent.mkdir(parents=True,exist_ok=True);proof.write_text('fixed proof source')
        self.candidate={'artifacts':{'core.jar':'a'*64}}
        self.document={'schemaVersion':1,'reviews':[{'advisory':advisory,'purl':package,'context':CONTEXT,'decision':'not_reachable',
                         'candidateArtifactsSha256':artifact_set(self.candidate),'evidence':[{'path':name,'sha256':sha256(self.root/name)} for name in sorted(REVIEW_PROOFS[(advisory,package)])]}
                         for advisory,package in REVIEW_MARKERS]}
        self.runs=[{'id':'starter-3.5.16-servlet-maven-jdk17','runtime':[{'purl':p} for p in {p for _,p in REVIEW_MARKERS}]}]
        self.log=''.join(dict.fromkeys(marker for markers in REVIEW_MARKERS.values() for marker in markers))
        root=patch('consumer_security.ROOT',self.root);root.start();self.addCleanup(root.stop)
    def check(self):
        (self.root/'verification/consumer-reachability.json').write_text(json.dumps(self.document))
        return scoped_reviews(self.candidate,self.runs,lambda run:self.log)
    def test_exact_fixed_fixture_with_both_runtime_controls_qualifies(self):
        self.assertEqual(self.document,self.check())
    def test_advisory_version_context_and_binary_mismatch_fail(self):
        for key in ('advisory','purl','context','candidateArtifactsSha256'):
            original=copy.deepcopy(self.document);self.document['reviews'][0][key]='other'
            with self.subTest(key=key),self.assertRaises(ValueError):self.check()
            self.document=original
    def test_missing_proof_or_changed_guard_fails(self):
        self.log='CONSUMER VIEW SAFETY VERIFIED xsltBeans=0 wildcardViewMappings=0\n'
        with self.assertRaisesRegex(ValueError,'executable'):self.check()
        (self.root/'verification/published/common/ConsumerViewSafety.java').write_text('changed guard')
        with self.assertRaisesRegex(ValueError,'digest'):self.check()
    def test_unreviewed_application_or_package_is_never_waived(self):
        self.runs[0]['id']='application-production'
        with self.assertRaisesRegex(ValueError,'application context'):self.check()
        self.runs[0]['runtime']=[{'purl':'pkg:maven/org.springframework/spring-webmvc@7.0.8'}]
        with self.assertRaisesRegex(ValueError,'does not apply'):self.check()

    def test_every_route_absence_and_negative_control_marker_is_required(self):
        original=self.log
        for marker in {m for markers in REVIEW_MARKERS.values() for m in markers}:
            self.log=original.replace(marker,'')
            with self.subTest(marker=marker),self.assertRaisesRegex(ValueError,'executable'):self.check()
        self.log=original

    def test_missing_duplicate_and_unreviewed_advisories_fail(self):
        original=copy.deepcopy(self.document)
        for reviews in (original['reviews'][:-1],original['reviews']+[original['reviews'][0]]):
            self.document['reviews']=reviews
            with self.assertRaises(ValueError):self.check()
        self.document=original
        self.document['reviews'][-1]['advisory']='unreviewed-advisory'
        with self.assertRaises(ValueError):self.check()

    def test_empty_incomplete_or_duplicate_proof_sources_fail(self):
        original=copy.deepcopy(self.document)
        proof=original['reviews'][0]['evidence']
        for entries in ([],proof[:-1],proof[:-1]+[proof[0]]):
            self.document=copy.deepcopy(original);self.document['reviews'][0]['evidence']=entries
            with self.assertRaisesRegex(ValueError,'proof source set'):self.check()


if __name__=='__main__':unittest.main()
