"""Context-bound reachability proof for the fixed legacy REST compatibility fixture."""
import json
from release_common import require, safe_artifact
from release_tools import ROOT
from scan_release import artifact_set

ADVISORY='GHSA-pc63-qcmh-9cmg'
PACKAGE='pkg:maven/org.springframework/spring-webmvc@6.2.19'
WEBFLUX='pkg:maven/org.springframework/spring-webflux@6.2.19'
CONTEXT='quotaflow-fixed-rest-consumer-fixtures'
VIEW_MARKERS=('CONSUMER VIEW SAFETY VERIFIED xsltBeans=0 wildcardViewMappings=0\n',
              'CONSUMER VIEW SAFETY NEGATIVE CONTROL PASSED\n')
ROUTE_MARKERS=('CONSUMER ROUTE SAFETY VERIFIED sseMappings=0 fragmentHandlers=0 functionalRoutes=0\n',
               'CONSUMER ROUTE SAFETY NEGATIVE CONTROLS PASSED sse=blocked functionalMvc=blocked functionalReactive=blocked\n')
REVIEW_MARKERS={(ADVISORY,PACKAGE):VIEW_MARKERS,
               ('GHSA-j9f9-w8pj-32f8',PACKAGE):ROUTE_MARKERS,
               ('GHSA-j9f9-w8pj-32f8',WEBFLUX):ROUTE_MARKERS,
               ('GHSA-9qf2-26p9-2q2q',WEBFLUX):ROUTE_MARKERS}


def scoped_reviews(candidate,runs,read_log):
    document=json.loads((ROOT/'verification/consumer-reachability.json').read_text())
    identities=[(r['advisory'],r['purl']) for r in document['reviews']]
    require(document['schemaVersion']==1 and len(identities)==len(REVIEW_MARKERS)
            and set(identities)==set(REVIEW_MARKERS),'Unexpected consumer review policy')
    for review in document['reviews']:
        identity=(review['advisory'],review['purl'])
        require(review['context']==CONTEXT and review['decision']=='not_reachable',
                'Consumer review is outside the fixed context')
        require(review['candidateArtifactsSha256']==artifact_set(candidate),'Consumer review belongs to different library binaries')
        for entry in review['evidence']: safe_artifact(ROOT,entry)
        affected=[run for run in runs if any(a['purl']==review['purl'] for a in run['runtime'])]
        require(affected,'Fixed review does not apply to this runtime matrix')
        for run in affected:
            require(run['id'].startswith('starter-3.5.16-'),'Unreviewed application context')
            text=read_log(run)
            require(all(marker in text for marker in REVIEW_MARKERS[identity]),'Missing executable consumer reachability proof')
    return document
