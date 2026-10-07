"""Execute annotations and properties extracted from README against staged JARs."""
import json
import os
from pathlib import Path
import queue
import re
import subprocess
import threading
import time
import uuid

from release_candidate import reference
from release_common import require, sha256
from release_tools import ROOT
from run_staged_verification import classpath_evidence, command, fixture, isolated_environment
from candidate_identity import source_identity


def controlled_outage(args,log,environment,container):
    process=subprocess.Popen(args,env=environment,cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1)
    lines=queue.Queue()
    def reader():
        for line in process.stdout: lines.put(line)
        lines.put(None)
    thread=threading.Thread(target=reader,daemon=True); thread.start()
    paused=False; seen=[]; deadline=time.monotonic()+120
    try:
        with log.open('w') as output:
            while True:
                timeout=deadline-time.monotonic(); require(timeout>0,'README outage example timed out')
                line=lines.get(timeout=timeout)
                if line is None: break
                output.write(line); output.flush()
                marker=line.strip()
                if marker=='OUTAGE_READY':
                    subprocess.run(['docker','pause',container],check=True,stdout=subprocess.DEVNULL)
                    paused=True; seen.append(marker); process.stdin.write('continue\n'); process.stdin.flush()
                elif marker=='RECOVERY_READY':
                    subprocess.run(['docker','unpause',container],check=True,stdout=subprocess.DEVNULL)
                    paused=False; seen.append(marker); process.stdin.write('continue\n'); process.stdin.flush()
        require(process.wait(timeout=10)==0 and seen==['OUTAGE_READY','RECOVERY_READY'],'README outage/recovery failed: '+str(log))
    finally:
        if paused: subprocess.run(['docker','unpause',container],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        if process.poll() is None:
            process.terminate()
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
        process.stdin.close(); process.stdout.close()


def run_examples(prepared,output,work,homes,url,container):
    prepared=Path(prepared); output=Path(output); work=Path(work)
    manifest=json.loads((prepared/'candidate-manifest.json').read_text()); candidate=manifest['candidate']
    readme=(ROOT/'README.md').read_text()
    require('io.quotaflow:quotaflow-spring-boot-starter:'+candidate['version']+'"' in readme,'README coordinates do not match candidate version')
    blocks=re.findall(r'```properties\n(.*?)```',readme,re.S)
    require(len(blocks)>=2 and 'quotaflow.defaults.expected-instances=1' in blocks[0], 'README lacks explicit instance count')
    annotations=re.findall(r'@RateLimited\(policy = "tenant-gold"[^\n]*\)',readme)
    require(len(annotations)==2 and '#tenantId' in annotations[0] and 'waitTimeout = "PT2S"' in annotations[1], 'README annotation contract changed')
    reaction=re.search(r'quotaflow.policies.tenant-gold.reaction=(\w+)',blocks[1])
    require(reaction is not None and reaction[1]=='throttle','README must select throttle mode explicitly')
    directory=output/'examples'; directory.mkdir(parents=True,exist_ok=False)
    (directory/'readme-source.md').write_text(readme)
    (directory/'readme.properties').write_text(blocks[0])
    compatibility=json.loads((ROOT/'verification/compatibility.json').read_text()); feature=compatibility['bytecodeJdk']; home=homes[str(feature)]
    project=work/'projects'/('readme-'+output.name); fixture(project,'starter','maven',candidate['version'],(prepared/'repository').as_uri(),compatibility['springBoot'][0]['version'])
    (project/'src/main/java/CompatibilityApplication.java').unlink()
    source=(ROOT/'verification/published/examples/ReadmeApplication.java').read_text()
    source=source.replace('__REJECT_ANNOTATION__',annotations[0]).replace('__THROTTLE_ANNOTATION__',annotations[1]).replace('__THROTTLE_REACTION__',reaction[1])
    (project/'src/main/java/ReadmeApplication.java').write_text(source)
    environment=isolated_environment(work,home)
    command(['mvn','-B','-U','-s',str(work/'settings.xml'),'-gs',str(work/'settings.xml'),'-Dmaven.repo.local='+str(work/'maven-repository'),
             '-f',str(project/'pom.xml'),'compile','dependency:build-classpath','-Dmdep.outputFile='+str(project/'runtime-classpath.txt')],directory/'build.log',environment)
    classpath=str(project/'target/classes')+os.pathsep+(project/'runtime-classpath.txt').read_text().strip()
    artifacts,runtime=classpath_evidence(classpath,candidate,'starter')
    report={'schemaVersion':1,'kind':'staged-examples','candidate':candidate,'preparedManifestSha256':sha256(prepared/'candidate-manifest.json'),
            'simulation':False,'status':'RUNNING','executionSourceSha256':source_identity(ROOT)[1],
            'runs':[],'readme':reference(directory,directory/'readme-source.md')}
    try:
        for case in ('reject','keys','throttle','http-429','outage-recovery'):
            properties={'verification.case':case,'verification.jdk':str(feature),'verification.properties':str(directory/'readme.properties'),
                        'verification.namespace':'readme-'+uuid.uuid4().hex,'verification.redisUrl':url}
            args=[str(Path(home)/'bin/java'),*[f'-D{k}={v}' for k,v in properties.items()],'-cp',classpath,'io.quotaflow.verification.published.ReadmeApplication']
            log=directory/(case+'.log')
            if case=='outage-recovery': controlled_outage(args,log,environment,container)
            else: command(args,log,environment,timeout=60)
            require('PUBLISHED EXAMPLES VERIFIED case='+case+'\n' in log.read_text(),'README example did not complete: '+case)
            report['runs'].append({'id':case,'jdk':feature,'status':'PASS','exitCode':0,'origin':'staged-repository',
                                   'artifacts':artifacts,'runtime':runtime,'log':reference(directory,log)})
            print('README '+case+': PASS',flush=True)
        report['status']='PASS'
    except BaseException:
        report['status']='FAILED'; raise
    finally: (directory/'results.json').write_text(json.dumps(report,indent=2)+'\n')
    return report
