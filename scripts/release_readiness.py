#!/usr/bin/env python3
"""Audit exact candidate evidence without equating implementation checks to readiness."""
import argparse
import json
from pathlib import Path
import re
import tempfile
import zipfile
import xml.etree.ElementTree as ET

from candidate_identity import source_identity
from release_acceptance import staged_report, verify as accept
from release_candidate import assemble, verify_prepared, verify_sealed
from release_common import require, safe_artifact, sha256
from release_tools import ROOT
from run_release_controls import SUITES
from scan_release import instant
import datetime as dt

COVERAGE={
    'Canonical hierarchical quota identity':['runtime-17','runtime-21','runtime-25','topology-redis','topology-valkey'],
    'Algorithm state and numerical bounds':['runtime-17','core-coverage','core-mutation'],
    'Conservative degradation and coordinated recovery':['runtime-17','fallback-mutation','soak'],
    'Acquisition deadlines and reload lifecycle':['runtime-17','runtime-21','runtime-25','soak'],
    'Spring and observability contracts':['consumers','native-servlet','native-reactive'],
    'Release quality gates':['native-store','performance','reproducibility','negative-controls']}


def controls(path,candidate):
    path=Path(path);report=json.loads(path.read_text())
    require(report['status']=='PASS' and report['sourceSha256']==candidate['sourceSha256'],'Controls belong to different or failed sources')
    require(len(report['suites'])==len(SUITES) and {s['name'] for s in report['suites']}==set(SUITES),'Incomplete control suites')
    for suite in report['suites']:
        text=safe_artifact(path.parent,suite['log']).read_text()
        count=re.search(r'Ran (\d+) tests? in ',text)
        require(suite['exitCode']==0 and count is not None and int(count[1])==suite['tests']>0 and '\nOK\n' in text,'Failed or empty control evidence')
    return {'status':'PASS','tests':sum(s['tests'] for s in report['suites'])}


def audit(prepared,staged=None,control_report=None,sealed=None,manifest_digest=None,pins=None,operator=None):
    prepared=Path(prepared).resolve();manifest=json.loads((prepared/'candidate-manifest.json').read_text());candidate=manifest['candidate']
    checks={};acceptance=None
    def check(name,fn):
        try:checks[name]=fn()
        except (ValueError,KeyError,TypeError,OSError,zipfile.BadZipFile,ET.ParseError) as error:checks[name]={'status':'UNVERIFIED','reason':str(error)}
    def preparation():
        verify_prepared(prepared,manifest)
        require(source_identity(ROOT)[1]==candidate['sourceSha256'],'Current sources differ from candidate')
        return {'status':'PASS','modules':len(candidate['artifacts'])}
    check('preparedCandidate',preparation)
    check('releaseControls',lambda:controls(control_report,candidate) if control_report else {'status':'UNVERIFIED','reason':'No controls report supplied'})
    def staged_checks():
        require(staged is not None,'No staged evidence supplied')
        with tempfile.TemporaryDirectory(prefix='quotaflow-readiness-') as directory:
            root=Path(directory)/'sealed'
            package=assemble(prepared,root,{role:Path(staged)/role for role in ('consumers','examples')},rehearsal=True)
            results={role:staged_report(root,package,role) for role in ('consumers','examples')}
        return {'status':'PASS','results':results}
    check('stagedExecution',staged_checks)
    def production():
        nonlocal acceptance
        require(sealed is not None and manifest_digest is not None and pins is not None,'No complete signed production candidate supplied')
        package=verify_sealed(sealed,manifest_digest);require(package['candidate']==candidate,'Production candidate differs')
        acceptance=accept(sealed,manifest_digest,pins)
        return {'status':'PASS','manifestSha256':manifest_digest}
    check('productionAcceptance',production)
    def operator_checks():
        require(operator is not None,'Namespace ownership, protected environments and production identities remain operator prerequisites')
        value=json.loads(Path(operator).read_text())
        require(value['candidate']==candidate and value['manifestSha256']==manifest_digest and value['reviewer'].strip(),'Operator review identity mismatch')
        age=dt.datetime.now(dt.timezone.utc)-instant(value['reviewedAt'])
        require(dt.timedelta(0)<=age<=dt.timedelta(days=1),'Operator review is stale or future')
        required={'namespaceOwnership','protectedTags','signingEnvironment','promotionEnvironment','publishedPgpIdentity','evidenceTransfer'}
        require(set(value['checks'])==required and all(value['checks'][k] is True for k in required),'Operator prerequisites are incomplete')
        return {'status':'PASS','reviewer':value['reviewer']}
    check('operatorPrerequisites',operator_checks)
    coverage={name:{'status':'PASS' if acceptance and all(stage in acceptance['gates']['quality']['stages'] for stage in stages) else 'UNVERIFIED',
                    'requiredQualityStages':stages} for name,stages in COVERAGE.items()}
    coverage['Publishing integrity and recovery']={'status':checks['releaseControls']['status'],'evidence':'releaseControls'}
    ready=all(value['status']=='PASS' for value in checks.values()) and all(value['status']=='PASS' for value in coverage.values())
    return {'schemaVersion':1,'status':'READY' if ready else 'UNVERIFIED','candidate':candidate,'checks':checks,'auditCoverage':coverage,
            'unmet':[name for name,value in checks.items() if value['status']!='PASS']}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prepared',type=Path,required=True);parser.add_argument('--staged',type=Path);parser.add_argument('--controls',type=Path)
    parser.add_argument('--sealed',type=Path);parser.add_argument('--sha256');parser.add_argument('--quality-archive-sha256');parser.add_argument('--quality-manifest-sha256');parser.add_argument('--baseline-review-sha256');parser.add_argument('--operator-checks',type=Path)
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    require(not args.output.resolve().is_relative_to(args.prepared.resolve()),'Readiness output must be outside the immutable candidate')
    require(args.sealed is None or not args.output.resolve().is_relative_to(args.sealed.resolve()),'Readiness output must be outside the sealed candidate')
    pins={'archiveSha256':args.quality_archive_sha256,'manifestSha256':args.quality_manifest_sha256,'baselineReviewSha256':args.baseline_review_sha256} if args.quality_archive_sha256 else None
    result=audit(args.prepared,args.staged,args.controls,args.sealed,args.sha256,pins,args.operator_checks)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('x') as output:output.write(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'status':result['status'],'unmet':result['unmet']}))
