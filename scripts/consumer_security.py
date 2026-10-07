"""Context-bound reachability proof for the fixed legacy REST compatibility fixture."""
import json
from release_common import require, safe_artifact
from release_tools import ROOT
from scan_release import artifact_set

ADVISORY='GHSA-pc63-qcmh-9cmg'
PACKAGE='pkg:maven/org.springframework/spring-webmvc@6.2.19'
CONTEXT='quotaflow-fixed-rest-consumer-fixtures'


def scoped_reviews(candidate,runs,read_log):
    document=json.loads((ROOT/'verification/consumer-reachability.json').read_text())
    require(document['schemaVersion']==1 and len(document['reviews'])==1,'Unexpected consumer review policy')
    review=document['reviews'][0]
    require(review['advisory']==ADVISORY and review['purl']==PACKAGE and review['context']==CONTEXT
            and review['decision']=='not_reachable','Consumer review is outside the fixed context')
    require(review['candidateArtifactsSha256']==artifact_set(candidate),'Consumer review belongs to different library binaries')
    for entry in review['evidence']: safe_artifact(ROOT,entry)
    affected=[run for run in runs if any(a['purl']==PACKAGE for a in run['runtime'])]
    require(affected,'Fixed review does not apply to this runtime matrix')
    for run in affected:
        require(run['id'].startswith('starter-3.5.16-'),'Unreviewed application context')
        text=read_log(run)
        require('CONSUMER VIEW SAFETY VERIFIED xsltBeans=0 wildcardViewMappings=0\n' in text
                and 'CONSUMER VIEW SAFETY NEGATIVE CONTROL PASSED\n' in text,'Missing executable XSLT reachability proof')
    return document
