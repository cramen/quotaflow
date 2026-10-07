#!/usr/bin/env python3
"""Explicit production entry point for one bounded publication advancement.

No build, signing, scan or benchmark is launched here. All inputs must already be
sealed and independently verified. Local operator authentication and final certification are separate.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import zipfile
import shutil
import xml.etree.ElementTree as ET

from release_acceptance import production_identity, project_policy, verify
from release_candidate import regular_files, verify_sealed
from release_common import require, safe_artifact, sha256
from release_promotion import advance
from release_transport import GitHub, GitHubJournal, Portal


def delivery_archive(root, output):
    """Repackage the verified bytes for recovery without rebuilding any library."""
    with zipfile.ZipFile(output,'x',compression=zipfile.ZIP_DEFLATED) as archive:
        for path in regular_files(root):
            info=zipfile.ZipInfo(path.relative_to(root).as_posix(),(1980,1,1,0,0,0))
            info.create_system=3; info.external_attr=0o100644<<16
            info.compress_type=zipfile.ZIP_STORED if path.suffix in {'.gz','.zip','.jar'} else zipfile.ZIP_DEFLATED
            with path.open('rb') as reader, archive.open(info,'w',force_zip64=True) as writer:
                shutil.copyfileobj(reader,writer,1024*1024)


def verify_delivery(path, manifest, digest, signature_digest):
    expected={entry['path']:entry['sha256'] for entry in manifest['files']}
    expected.update({'release-manifest.json':digest,'release-manifest.sigstore.json':signature_digest})
    with zipfile.ZipFile(path) as archive:
        names=archive.namelist()
        require(len(names)==len(set(names)) and set(names)==set(expected),'Delivery inventory mismatch')
        for name in names:
            with archive.open(name) as reader:
                require(hashlib.file_digest(reader,'sha256').hexdigest()==expected[name],'Delivery bytes differ from accepted candidate')


def publish_once(root, digest, pins, recovered_deployment=None):
    from local_release import publication_lock
    manifest=verify_sealed(Path(root).resolve(),digest)
    with publication_lock(project_policy()['repository'],manifest['candidate']['version']):
        return _publish_once(root,digest,pins,recovered_deployment)


def _publish_once(root, digest, pins, recovered_deployment=None):
    root=Path(root).resolve(); manifest=verify_sealed(root,digest); policy=project_policy()
    # Reject rehearsal or an absent project identity before even constructing
    # authenticated clients. Their constructors perform no external operations.
    production_identity(root,manifest,policy)
    from local_release import central_environment, github_token
    portal=Portal(central_environment()); github=GitHub(policy['repository'],github_token())
    journal=GitHubJournal(github,'v'+manifest['candidate']['version'])
    acceptance={}
    def gates(published):
        nonlocal acceptance
        acceptance=verify(root,digest,pins,published=published)
        verify_delivery(delivery,manifest,digest,acceptance['gates']['sigstore']['bundleSha256'])
    # The state machine verifies all evidence and packaged bytes before any
    # external mutation. Delivery packaging itself performs no network calls.
    with tempfile.TemporaryDirectory(prefix='quotaflow-delivery-') as directory:
        delivery=Path(directory)/('quotaflow-'+manifest['candidate']['version']+'-candidate.zip')
        delivery_archive(root,delivery)
        assets=[root/'release-manifest.json',root/'release-manifest.sigstore.json',
                safe_artifact(root,manifest['mavenBundle']),safe_artifact(root,manifest['sbom']),delivery]
        bundle_digest=sha256(delivery)
        result=advance(root,manifest,digest,assets,journal,portal,github,gates,recovered_deployment)
        require(sha256(delivery)==bundle_digest,'Delivery archive changed during publication')
        return {**result,'acceptance':acceptance,'deliverySha256':bundle_digest}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate',type=Path,required=True); parser.add_argument('--sha256',required=True)
    parser.add_argument('--quality-archive-sha256',required=True); parser.add_argument('--quality-manifest-sha256',required=True)
    parser.add_argument('--baseline-review-sha256',required=True)
    parser.add_argument('--recovered-deployment',help='Existing Portal deployment ID after a lost upload response')
    parser.add_argument('--wait-seconds',type=int,default=0,help='Bounded wait for Central processing; never retry uncertain mutations')
    parser.add_argument('--poll-seconds',type=int,default=15)
    args=parser.parse_args()
    pins={'archiveSha256':args.quality_archive_sha256,'manifestSha256':args.quality_manifest_sha256,
          'baselineReviewSha256':args.baseline_review_sha256}
    try:
        require(0 <= args.wait_seconds <= 3600 and 1 <= args.poll_seconds <= 60,'Invalid bounded publication wait')
        deadline=time.monotonic()+args.wait_seconds
        while True:
            result=publish_once(args.candidate,args.sha256,pins,args.recovered_deployment)
            print(json.dumps(result,indent=2),flush=True)
            if result['status']=='COMPLETE': return 0
            if time.monotonic() >= deadline: return 2
            time.sleep(min(args.poll_seconds,max(0,deadline-time.monotonic())))
    except (ValueError,KeyError,TypeError,OSError,zipfile.BadZipFile,ET.ParseError) as error:
        print(json.dumps({'status':'INCOMPLETE','error':str(error),
                          'recovery':'Preserve the candidate and durable journal; reconcile the recorded deployment before retrying.'})); return 1


if __name__=='__main__': raise SystemExit(main())
