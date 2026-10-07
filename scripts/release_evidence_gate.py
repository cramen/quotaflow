#!/usr/bin/env python3
"""Revalidate independently pinned quality and current security evidence.

This read-only gate consumes sealed bytes and runs no benchmarks or publishing
operations. Its success covers quality/security only, not production approval.
"""
import argparse
import json
from pathlib import Path
import re
import tempfile
import zipfile
import xml.etree.ElementTree as ET

from import_release_bundle import extract
from release_candidate import verify_sealed
from release_common import require, safe_artifact, sha256
from scan_release import verify_security
from verify_release_evidence import verify as verify_quality_bundle


def file_reference(manifest, name):
    matches = [entry for entry in manifest['files'] if entry['path'] == name]
    require(len(matches) == 1, 'Missing or ambiguous bound evidence: ' + name)
    return matches[0]


def quality(root, manifest, pins):
    require(set(pins) == {'archiveSha256', 'manifestSha256', 'baselineReviewSha256'}
            and all(isinstance(v, str) and re.fullmatch(r'[0-9a-f]{64}', v) for v in pins.values()),
            'Three independently reviewed quality evidence digests are required')
    require(manifest['reportRoots'].get('quality') == 'reports/quality', 'Missing quality evidence')
    reference = file_reference(manifest, 'reports/quality/verification.zip')
    require(reference['sha256'] == pins['archiveSha256'], 'Quality archive differs from independently pinned digest')
    archive = safe_artifact(root, reference)
    with tempfile.TemporaryDirectory(prefix='quotaflow-quality-gate-') as directory:
        output = Path(directory) / 'evidence'
        extract(archive, output)
        report = verify_quality_bundle(output, pins['manifestSha256'], manifest['candidate'], pins['baselineReviewSha256'])
    require(sha256(archive) == pins['archiveSha256'], 'Quality archive changed during verification')
    return report


def security(root, manifest, now=None):
    require(manifest['reportRoots'].get('security') == 'reports/security', 'Missing security evidence')
    security_ref = file_reference(manifest, 'reports/security/security.json')
    report = json.loads(safe_artifact(root, security_ref).read_text())
    require(report['candidateManifestSha256'] == manifest['preparation']['sha256'], 'Security scan belongs to another prepared manifest')
    directory = Path(root) / 'reports/security'
    # Every subordinate report must be included in the signed file inventory.
    for key in ('database', 'rawReport', 'triage'):
        entry = report[key]
        bound = file_reference(manifest, 'reports/security/' + entry['path'])
        require(bound['sha256'] == entry['sha256'], 'Security evidence reference mismatch')
    return verify_security(directory, manifest['candidate'], manifest['sbom']['sha256'], now=now)


def verify(root, manifest_digest, pins, now=None):
    manifest = verify_sealed(root, manifest_digest)
    result = {'quality': quality(root, manifest, pins), 'security': security(root, manifest, now)}
    # Recheck sealed bytes after validators finish, before a caller may act.
    verify_sealed(root, manifest_digest)
    return {'status':'PASS', 'scope':'quality-and-security', 'productionReadiness':'UNVERIFIED',
            'candidate':manifest['candidate'], 'manifestSha256':manifest_digest, 'gates':result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path, required=True); parser.add_argument('--sha256', required=True)
    parser.add_argument('--quality-archive-sha256', required=True); parser.add_argument('--quality-manifest-sha256', required=True)
    parser.add_argument('--baseline-review-sha256', required=True)
    args = parser.parse_args()
    pins = {'archiveSha256':args.quality_archive_sha256, 'manifestSha256':args.quality_manifest_sha256,
            'baselineReviewSha256':args.baseline_review_sha256}
    try:
        report = verify(args.candidate, args.sha256, pins)
        print(json.dumps(report, indent=2)); return 0
    except (ValueError, KeyError, TypeError, OSError, zipfile.BadZipFile, ET.ParseError) as error:
        print(json.dumps({'status':'FAILED','error':str(error)})); return 1


if __name__ == '__main__': raise SystemExit(main())
