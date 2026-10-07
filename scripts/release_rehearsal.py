#!/usr/bin/env python3
"""Exercise release transitions using local files, without publishing credentials.

No endpoint or adapter can be supplied by the caller. Only local implementations
are instantiated; the production HTTP transport is never used.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import zipfile

from release_candidate import verify_sealed
from release_common import GROUP, MODULES, purl, require, safe_artifact, sha256
from release_evidence_gate import quality, security
from release_promotion import advance, encoded
from release_signatures import verify_pgp
from release_transport import GitHubJournal, component


class LocalGitHub:
    def __init__(self, root):
        self.root = root; (root / 'assets').mkdir(parents=True, exist_ok=True)
    def find(self, tag):
        path = self.root / 'release.json'
        if not path.exists(): return None
        value = json.loads(path.read_text()); require(value['tag_name'] == tag, 'Local release tag mismatch')
        return value
    def ensure_draft(self, tag, commit):
        current=self.find(tag)
        if current is not None:
            require(current['draft'] is True and current['target_commitish']==commit,'Local draft identity mismatch')
            return current['id']
        (self.root / 'release.json').write_bytes(encoded({'id':1,'tag_name':tag,'target_commitish':commit,'draft':True,'simulation':True}))
        return 1
    def assets(self, release): return [{'name':p.name} for p in sorted((self.root/'assets').iterdir())]
    def upload_asset(self, release, name, data):
        component(name)
        with (self.root/'assets'/name).open('xb') as stream: stream.write(data)
    def download_asset(self, release, name): component(name); return (self.root/'assets'/name).read_bytes()
    def publish(self, release, tag, commit):
        value = self.find(tag); require(value['target_commitish']==commit,'Local commit mismatch')
        value['draft']=False; (self.root/'release.json').write_bytes(encoded(value))
    def is_complete(self, release, tag, commit):
        value=self.find(tag); return value['draft'] is False and value['target_commitish']==commit


class LocalPortal:
    def __init__(self, root, version):
        self.root=root; root.mkdir(parents=True,exist_ok=True); self.version=version
    def upload(self, bundle, name, expected_sha256):
        destination=self.root/'deployment.zip'
        require(not destination.exists(),'Duplicate local deployment upload')
        payload=bundle.read_bytes()
        require(hashlib.sha256(payload).hexdigest()==expected_sha256,'Local bundle changed before upload')
        destination.write_bytes(payload)
        (self.root/'state.json').write_bytes(encoded({'deploymentState':'VALIDATED','uploads':1,'promotions':0,'simulation':True}))
        return 'local-deployment-1'
    def status(self, deployment):
        require(deployment=='local-deployment-1','Unknown local deployment')
        state=json.loads((self.root/'state.json').read_text())
        return {**state,'deploymentId':deployment,'purls':[purl(GROUP,m,self.version) for m in MODULES]}
    def download(self, deployment, name, published=False):
        self.status(deployment)
        with zipfile.ZipFile(self.root/'deployment.zip') as archive: return archive.read(name)
    def promote(self, deployment):
        state=self.status(deployment); require(state['deploymentState']=='VALIDATED','Invalid local promotion state')
        state['deploymentState']='PUBLISHED'; state['promotions']+=1
        (self.root/'state.json').write_bytes(encoded(state))


def rehearse(candidate, digest, output, quality_pins=None):
    candidate=Path(candidate).resolve(); output=Path(output).absolute()
    manifest=verify_sealed(candidate,digest)
    require(manifest['mode']=='rehearsal','Local rehearsal requires an explicitly non-production package')
    require(not output.exists() and not output.is_relative_to(candidate),'Rehearsal output must be a fresh separate directory')
    output.mkdir(parents=True)
    gates={'productionPgp':'UNVERIFIED','productionSigstore':'UNVERIFIED',
           'stagedConsumers':'UNVERIFIED','readmeExamples':'UNVERIFIED','quality':'UNVERIFIED','security':'UNVERIFIED'}
    report={'schemaVersion':1,'mode':'rehearsal','productionReadiness':'UNVERIFIED','externalPublishingOperations':0,
            'candidate':manifest['candidate'],'manifestSha256':digest,'gates':gates}
    try:
        if quality_pins is not None:
            quality(candidate,manifest,quality_pins); gates['quality']='PASS'
        if 'security' in manifest['reportRoots']:
            security(candidate,manifest); gates['security']='PASS'
        pgp=None
        if manifest['pgp'] is not None:
            pgp=json.loads(safe_artifact(candidate,manifest['pgp']).read_text())
        preparation=json.loads(safe_artifact(candidate,manifest['preparation']).read_text())
        def check(_):
            verify_sealed(candidate,digest)
            if pgp is not None:
                verify_pgp(candidate,pgp,preparation,pgp['fingerprint'],production=False)
                gates['localPgpCryptography']='PASS'
        assets=[candidate/'release-manifest.json',safe_artifact(candidate,manifest['mavenBundle']),safe_artifact(candidate,manifest['sbom'])]
        github=LocalGitHub(output/'github'); portal=LocalPortal(output/'portal',manifest['candidate']['version'])
        journal=GitHubJournal(github,'v'+manifest['candidate']['version'])
        for _ in range(3):
            result=advance(candidate,manifest,digest,assets,journal,portal,github,check)
            if result['status']=='COMPLETE': break
        require(result['status']=='COMPLETE','Local rehearsal did not finish')
        # Reinstantiate every adapter, simulating a clean runner resuming completed work.
        github=LocalGitHub(output/'github'); portal=LocalPortal(output/'portal',manifest['candidate']['version'])
        resumed=advance(candidate,manifest,digest,assets,GitHubJournal(github,'v'+manifest['candidate']['version']),portal,github,check)
        require(resumed['status']=='COMPLETE','Local completion did not resume')
        state=portal.status('local-deployment-1')
        require(state['uploads']==state['promotions']==1,'Rehearsal duplicated a deployment')
        report.update(status='SIMULATED_COMPLETE',localUploads=state['uploads'],localPromotions=state['promotions'])
    except (ValueError,KeyError,TypeError,OSError,zipfile.BadZipFile) as error:
        report.update(status='FAILED',error=str(error))
        raise
    finally:
        (output/'rehearsal.json').write_bytes(encoded(report))
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate',type=Path,required=True); parser.add_argument('--sha256',required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--quality-archive-sha256'); parser.add_argument('--quality-manifest-sha256'); parser.add_argument('--baseline-review-sha256')
    args=parser.parse_args()
    pins=None
    if any((args.quality_archive_sha256,args.quality_manifest_sha256,args.baseline_review_sha256)):
        pins={'archiveSha256':args.quality_archive_sha256,'manifestSha256':args.quality_manifest_sha256,'baselineReviewSha256':args.baseline_review_sha256}
    try:
        print(json.dumps(rehearse(args.candidate,args.sha256,args.output,pins),indent=2)); return 0
    except (ValueError,KeyError,TypeError,OSError,zipfile.BadZipFile) as error:
        print(json.dumps({'status':'FAILED','error':str(error)})); return 1


if __name__=='__main__': raise SystemExit(main())
