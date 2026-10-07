#!/usr/bin/env python3
"""Verify a release candidate before any publishing mutation.

Trust anchors come from the checked-out project policy and independently supplied
digests, never from a candidate's own claim that its evidence has passed.
"""
import argparse
import datetime as dt
import json
from pathlib import Path
import zipfile
import xml.etree.ElementTree as ET

from candidate_identity import source_identity
from release_candidate import verify_sealed
from release_common import require, safe_artifact, sha256, tag_identity
from release_evidence_gate import file_reference, quality, security
from release_signatures import fingerprint, verify_pgp, verify_sigstore
from release_tools import ROOT
from scan_release import instant, verify_security


def project_policy():
    return json.loads((ROOT / 'verification/release-policy.json').read_text())


def production_identity(root, manifest, policy):
    require(manifest['mode'] == 'release', 'Rehearsal packages cannot authorize production publication')
    expected = fingerprint(policy['pgp']['fingerprint'])
    public = ROOT / policy['pgp']['publicKey']
    require(public.is_file() and not public.is_symlink(), 'Approved project public PGP key is missing')
    require(manifest['pgp'] is not None, 'Missing production PGP evidence')
    pgp = json.loads(safe_artifact(root, manifest['pgp']).read_text())
    require(pgp.get('production') is True, 'Test signatures cannot authorize publication')
    require(sha256(public) == pgp['publicKey']['sha256'], 'Candidate public key differs from approved project key')
    preparation = json.loads(safe_artifact(root, manifest['preparation']).read_text())
    verify_pgp(root, pgp, preparation, expected)
    # Bind every verified sidecar to both the signed manifest and Maven ZIP inventory.
    maven = {entry['path']:entry for entry in manifest['mavenAssets']}
    for signed in pgp['signedAssets']:
        for entry in (signed['artifact'], signed['signature']):
            require(entry['path'] in maven and maven[entry['path']]['sha256'] == entry['sha256'],
                    'PGP-verified bytes differ from the Maven bundle inventory')
    return {'status':'PASS', 'fingerprint':expected, 'assets':len(pgp['signedAssets'])}


def expected_cases(role):
    compatibility = json.loads((ROOT / 'verification/compatibility.json').read_text())
    if role == 'examples': return {name:compatibility['bytecodeJdk'] for name in ('reject','throttle','keys','http-429','outage-recovery')}
    require(role == 'consumers', 'Unknown staged evidence role')
    cases = {f'{module}-{tool}-jdk{jdk}':jdk for module in ('core','kotlin','redis','micrometer')
             for tool in ('maven','gradle') for jdk in compatibility['testJdks']}
    cases.update({f"starter-{boot['version']}-{stack}-{tool}-jdk{jdk}":jdk
                  for boot in compatibility['springBoot'] for stack in ('servlet','reactive')
                  for tool in ('maven','gradle') for jdk in compatibility['testJdks']})
    return cases


def verify_runtime_report(root, manifest, report, *, published=False):
    root=Path(root).resolve()
    from run_staged_verification import runtime_sbom
    references=report['runtimeSecurity']
    resolved={}
    for name,entry in references.items():
        bound=file_reference(manifest,'reports/consumers/'+entry['path'])
        require(bound['sha256']==entry['sha256'],'Unbound consumer runtime evidence')
        resolved[name]=safe_artifact(root,bound)
    require(set(resolved)=={'inputManifest','sbom','report'},'Incomplete runtime security references')
    require(json.loads(resolved['sbom'].read_text())==runtime_sbom(report['runs'],manifest['candidate']['version']),
            'Scanned consumer runtime differs from resolved classpaths')
    inputs=json.loads(resolved['inputManifest'].read_text()); scan=json.loads(resolved['report'].read_text())
    require(inputs['candidate']==manifest['candidate'] and inputs['preparedManifestSha256']==manifest['preparation']['sha256']
            and scan['candidateManifestSha256']==sha256(resolved['inputManifest'])
            and inputs['sbom']['sha256']==sha256(resolved['sbom']), 'Consumer scan input identity mismatch')
    scan_root=resolved['report'].parent
    for key in ('database','rawReport','triage'):
        entry=scan[key]; path=(scan_root/entry['path']).relative_to(root).as_posix()
        bound=file_reference(manifest,path)
        require(bound['sha256']==entry['sha256'],'Unbound consumer scan data')
    now=instant(scan['createdAt']) if published else None
    require(now is None or now<=dt.datetime.now(dt.timezone.utc),'Future runtime scan')
    result=verify_security(scan_root,manifest['candidate'],sha256(resolved['sbom']),now=now)
    from consumer_security import scoped_reviews
    reviews=scoped_reviews(manifest['candidate'],report['runs'],lambda run:safe_artifact(root,
        file_reference(manifest,'reports/consumers/'+run['log']['path'])).read_text())
    require(json.loads(safe_artifact(scan_root,result['triage']).read_text())==reviews,
            'Consumer reachability evidence differs from the context-bound policy')
    return {'status':'PASS','runtimeComponents':len(json.loads(resolved['sbom'].read_text())['components'])}


def staged_report(root, manifest, role, *, published=False):
    require(manifest['reportRoots'].get(role) == 'reports/' + role, 'Missing staged ' + role + ' evidence')
    directory = 'reports/' + role
    report = json.loads(safe_artifact(root, file_reference(manifest, directory + '/results.json')).read_text())
    require(report['schemaVersion'] == 1 and report['kind'] == 'staged-' + role
            and report['candidate'] == manifest['candidate']
            and report['executionSourceSha256'] == manifest['candidate']['sourceSha256']
            and report['preparedManifestSha256'] == manifest['preparation']['sha256'], 'Staged evidence belongs to another candidate')
    require(report.get('simulation') is False and report['status'] == 'PASS', 'Simulated or failed staged evidence')
    expected = expected_cases(role); runs = report['runs']
    require(len(runs) == len(expected) and {run['id'] for run in runs} == set(expected), 'Incomplete staged ' + role + ' matrix')
    for run in runs:
        require(run['status'] == 'PASS' and run['exitCode'] == 0 and run['jdk'] == expected[run['id']]
                and run['origin'] == 'staged-repository', 'Invalid staged execution')
        artifacts = run['artifacts']
        require(artifacts and all(manifest['candidate']['artifacts'].get(name) == digest for name,digest in artifacts.items()),
                'Staged consumer resolved other candidate bytes')
        required_module = {'core':'core','kotlin':'kotlin','redis':'store-redis','micrometer':'micrometer','starter':'spring-boot-starter'}
        if role == 'consumers':
            target = required_module[run['id'].split('-')[0]]
            require('quotaflow-' + target + '-' + manifest['candidate']['version'] + '.jar' in artifacts, 'Staged target library is missing')
        log = run['log']; bound = file_reference(manifest, directory + '/' + log['path'])
        require(bound['sha256'] == log['sha256'], 'Unbound staged execution log')
        text = safe_artifact(root, bound).read_text()
        require('PUBLISHED ' + role.upper() + ' VERIFIED case=' + run['id'] + '\n' in text, 'Missing staged execution marker')
        if role=='consumers' and run['id'].startswith('starter-'):
            require('STAGED RECOVERY VERIFIED unready=reject duplicate=reject distributed=healthy\n' in text,
                    'Default distributed starter recovery was not verified')
    if role=='consumers':
        runtime=verify_runtime_report(Path(root),manifest,report,published=published)
    else:
        consumers=json.loads(safe_artifact(root,file_reference(manifest,'reports/consumers/results.json')).read_text())
        allowed={(a['purl'],a['sha256']) for run in consumers['runs'] for a in run['runtime']}
        require(all((a['purl'],a['sha256']) in allowed for run in runs for a in run['runtime']),
                'README examples resolved an unscanned dependency')
        runtime={'status':'PASS','scope':'verified-consumer-runtime'}
    return {'status':'PASS','cases':len(runs),'runtimeSecurity':runtime}


def verify(root, digest, pins, tools_directory=None, *, published=False):
    """Read-only acceptance; all gates must pass before a caller may publish."""
    root = Path(root); manifest = verify_sealed(root, digest); policy = project_policy()
    pgp = production_identity(root, manifest, policy)
    candidate = manifest['candidate']; tag = 'v' + candidate['version']
    identity = tag_identity(ROOT, tag, candidate['commit'])
    require(source_identity(ROOT)[1] == candidate['sourceSha256'], 'Checked-out sources differ from the candidate')
    bundle = root / 'release-manifest.sigstore.json'
    sigstore = verify_sigstore(root / 'release-manifest.json', bundle, policy, tag, candidate['commit'],
                              tools_directory or ROOT / 'build/release-tools')
    security_time = None
    if published:
        # Only the orchestrator supplies this after live reconciliation confirms
        # the same immutable deployment has already been published. There is no
        # CLI flag that can enable historical freshness for a new publication.
        report = json.loads(safe_artifact(root, file_reference(manifest,'reports/security/security.json')).read_text())
        security_time = instant(report['createdAt'])
        require(security_time <= dt.datetime.now(dt.timezone.utc), 'Future security evidence')
    result = {'pgp':pgp,'sigstore':sigstore,'quality':quality(root,manifest,pins),
              'security':security(root,manifest,now=security_time),'consumers':staged_report(root,manifest,'consumers',published=published),
              'examples':staged_report(root,manifest,'examples',published=published)}
    verify_sealed(root,digest)
    require(sha256(bundle) == sigstore['bundleSha256'], 'Sigstore bundle changed during acceptance')
    return {'schemaVersion':1,'status':'PASS','scope':'completion-only' if published else 'pre-publication','candidate':candidate,
            'manifestSha256':digest,'verifiedAt':dt.datetime.now(dt.timezone.utc).isoformat(),
            'trustedMainCommit':identity['trustedMainCommit'],'gates':result}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate',type=Path,required=True); parser.add_argument('--sha256',required=True)
    parser.add_argument('--quality-archive-sha256',required=True); parser.add_argument('--quality-manifest-sha256',required=True)
    parser.add_argument('--baseline-review-sha256',required=True)
    args=parser.parse_args()
    pins={'archiveSha256':args.quality_archive_sha256,'manifestSha256':args.quality_manifest_sha256,
          'baselineReviewSha256':args.baseline_review_sha256}
    try:
        print(json.dumps(verify(args.candidate,args.sha256,pins),indent=2)); return 0
    except (ValueError,KeyError,TypeError,OSError,zipfile.BadZipFile,ET.ParseError) as error:
        print(json.dumps({'status':'FAILED','error':str(error)})); return 1


if __name__=='__main__': raise SystemExit(main())
