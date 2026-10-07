#!/usr/bin/env python3
"""Retain candidate-source-bound release control results without external publication."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
from candidate_identity import source_identity
from release_candidate import reference
from release_common import require
from release_tools import ROOT

SUITES=('test_release_preparation','test_release_candidate','test_release_signatures','test_release_security',
        'test_release_evidence_gate','test_release_acceptance','test_release_promotion','test_release_transport',
        'test_release_rehearsal','test_release_workflow','test_release_transfer','test_consumer_security','test_staged_runtime')


def run(output):
    output=Path(output).resolve();output.mkdir(parents=True,exist_ok=False)
    report={'schemaVersion':1,'status':'RUNNING','sourceSha256':source_identity(ROOT)[1],'suites':[]}
    try:
        for suite in SUITES:
            log=output/(suite+'.log')
            with log.open('w') as stream:
                result=subprocess.run([sys.executable,str(ROOT/'scripts'/(suite+'.py'))],cwd=ROOT,stdout=stream,stderr=subprocess.STDOUT,timeout=300)
            text=log.read_text();count=re.search(r'Ran (\d+) tests? in ',text)
            require(result.returncode==0 and count is not None and int(count[1])>0 and '\nOK\n' in text,'Release controls failed: '+suite)
            report['suites'].append({'name':suite,'exitCode':0,'tests':int(count[1]),'log':reference(output,log)})
            print(suite+': PASS',flush=True)
        require(source_identity(ROOT)[1]==report['sourceSha256'],'Sources changed during control execution')
        report['status']='PASS';report['tests']=sum(s['tests'] for s in report['suites'])
    except BaseException:
        report['status']='FAILED';raise
    finally:(output/'controls.json').write_text(json.dumps(report,indent=2)+'\n')
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    print(json.dumps(run(parser.parse_args().output),indent=2))
