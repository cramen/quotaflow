#!/usr/bin/env python3
"""Compile and execute clean external consumers against one prepared publication."""
import argparse
import contextlib
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import time
import tomllib
import uuid

from release_candidate import reference, verify_prepared
from release_common import require, sha256, purl, safe_artifact
from release_tools import ROOT
from candidate_identity import source_identity

MAINS={'core':'io.quotaflow.verification.published.CoreConsumer',
       'kotlin':'io.quotaflow.verification.published.KotlinConsumerKt',
       'micrometer':'io.quotaflow.verification.published.MetricsConsumer',
       'redis':'io.quotaflow.verification.published.RedisConsumer',
       'starter':'io.quotaflow.verification.consumer.CompatibilityApplication'}


def isolated_environment(work,java_home):
    prefixes=('ORG_GRADLE_PROJECT_','MAVEN_','GRADLE_','SPRING_','QUOTAFLOW_','SERVER_','MANAGEMENT_','LOGGING_',
              'GRYPE_')
    denied={'GH_TOKEN','GITHUB_TOKEN','SIGNING_KEY','SIGNING_PASSWORD','CENTRAL_PORTAL_USERNAME','CENTRAL_PORTAL_TOKEN',
            'JAVA_TOOL_OPTIONS','JDK_JAVA_OPTIONS','_JAVA_OPTIONS','JAVA_OPTS','M2_HOME'}
    result={key:value for key,value in os.environ.items()
            if not key.upper().replace('.','_').startswith(prefixes) and key.upper() not in denied}
    result.update(JAVA_HOME=str(java_home),PATH=str(Path(java_home)/'bin')+os.pathsep+os.environ['PATH'],
                  GRADLE_USER_HOME=str(Path(work)/'gradle-home'),MAVEN_SKIP_RC='true')
    return result


def command(args, log, env, cwd=ROOT, timeout=600):
    log.parent.mkdir(parents=True,exist_ok=True)
    with log.open('w') as stream:
        result=subprocess.run(args,cwd=cwd,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=timeout)
    require(result.returncode==0,'Staged execution failed: '+str(log))


@contextlib.contextmanager
def redis_server():
    identity=subprocess.check_output(['docker','run','--rm','-d','-p','127.0.0.1::6379','redis:6.2.24-alpine'],text=True).strip()
    try:
        port=subprocess.check_output(['docker','port',identity,'6379/tcp'],text=True).strip().rsplit(':',1)[1]
        for _ in range(60):
            result=subprocess.run(['docker','exec',identity,'redis-cli','ping'],capture_output=True,text=True)
            if result.returncode==0 and result.stdout.strip()=='PONG': break
            time.sleep(.25)
        else: raise ValueError('Disposable Redis did not become ready')
        yield 'redis://127.0.0.1:'+port,identity
    finally: subprocess.run(['docker','rm','-f',identity],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)


def fixture(directory, flavor, tool, version, repository, boot=None):
    directory.mkdir(parents=True)
    source=directory/'src/main'/('kotlin' if flavor=='kotlin' else 'java'); source.mkdir(parents=True)
    if flavor=='starter':
        original=ROOT/'verification/consumer/src/main/java/io/quotaflow/verification/consumer/CompatibilityApplication.java'
        text=original.read_text().replace('key="#p0"','key="#identity"')
        text=text.replace('    @Bean RateLimitStore localStore() { return new LocalRateLimitStore(); }','')
        replacement='''        var settings=io.quotaflow.verification.published.ConsumerRecovery.properties(redisUrl,System.getProperty("verification.namespace"),stack);
        io.quotaflow.verification.published.ConsumerRecovery.refused(()->io.quotaflow.verification.published.ConsumerRecovery.startProbe(CompatibilityApplication.class,settings));
        io.quotaflow.verification.published.ConsumerRecovery.provision(settings);
        try (var context=io.quotaflow.verification.published.ConsumerRecovery.start(CompatibilityApplication.class,settings)) {
            io.quotaflow.verification.published.ConsumerRecovery.healthy(context);
            io.quotaflow.verification.published.ConsumerRecovery.refused(()->io.quotaflow.verification.published.ConsumerRecovery.startProbe(CompatibilityApplication.class,settings));
            io.quotaflow.verification.published.ConsumerRecovery.healthy(context);
            System.out.println("STAGED RECOVERY VERIFIED unready=reject duplicate=reject distributed=healthy");'''
        text,count=re.subn(r'        var application = new SpringApplication\(CompatibilityApplication.class\);.*?        try \(var context = application.run\(args\)\) \{',lambda _:replacement,text,flags=re.S)
        require(count==1,'Starter fixture structure changed; review distributed wiring')
        text=text.replace('"http://localhost:"','"http://127.0.0.1:"')
        # The fixture uses named expressions and must therefore retain parameters.
        text=text.replace('            System.out.println("CONSUMER VERIFIED',
                          '            io.quotaflow.verification.published.ConsumerViewSafety.negativeControl();\n            io.quotaflow.verification.published.ConsumerViewSafety.verify(context);\n            System.out.println("PUBLISHED CONSUMERS VERIFIED case="+System.getProperty("verification.case"));\n            System.out.println("CONSUMER VERIFIED')
        (source/original.name).write_text(text)
        shutil.copyfile(ROOT/'verification/published/common/ConsumerViewSafety.java',source/'ConsumerViewSafety.java')
        shutil.copyfile(ROOT/'verification/published/common/ConsumerRecovery.java',source/'ConsumerRecovery.java')
    else:
        for path in (ROOT/'verification/published'/flavor).iterdir(): shutil.copyfile(path,source/path.name)
    versions=tomllib.loads((ROOT/'gradle/libs.versions.toml').read_text())['versions']
    alignment=json.loads((ROOT/'verification/consumer-runtime.json').read_text())
    modules={'core':['core'],'kotlin':['kotlin'],'micrometer':['micrometer'],'redis':['store-redis','fallback'],'starter':['spring-boot-starter']}[flavor]
    deps=[('io.github.cramen','quotaflow-'+m,version) for m in modules]
    if flavor=='starter': deps += [('org.springframework.boot','spring-boot-starter-web',None),('org.springframework.boot','spring-boot-starter-webflux',None)]
    if tool=='gradle':
        plugin="plugins { id 'java'"+("; id 'org.jetbrains.kotlin.jvm' version '"+versions['kotlin']+"'" if flavor=='kotlin' else '')+" }\n"
        build=plugin+f"repositories {{ maven {{ url=uri('{repository}'); content {{ includeGroup 'io.github.cramen' }} }}; mavenCentral {{ content {{ excludeGroup 'io.github.cramen' }} }} }}\n"
        build+="java { sourceCompatibility=JavaVersion.VERSION_17; targetCompatibility=JavaVersion.VERSION_17 }\ntasks.withType(JavaCompile).configureEach { options.compilerArgs.add('-parameters') }\n"
        if flavor=='kotlin': build+="kotlin { compilerOptions { jvmTarget=org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17 } }\n"
        build+='dependencies {\n'
        if boot:
            build+=f'implementation platform("org.springframework.boot:spring-boot-dependencies:{boot}")\n'
            for bom in alignment['boms']: build+=f'implementation platform("{bom["group"]}:{bom["artifact"]}:{bom["version"]}")\n'
            build+='constraints {\n'
            for name in alignment['tomcatArtifacts']: build+=f'implementation "org.apache.tomcat.embed:{name}:{alignment["tomcat"][boot]}"\n'
            build+='}\n'
        for group,name,selected in deps: build+=f'implementation "{group}:{name}'+(':'+selected if selected else '')+'"\n'
        build+='}\n'
        build+="tasks.register('writeRuntimeClasspath') { dependsOn 'classes'; doLast { file('runtime-classpath.txt').text = sourceSets.main.runtimeClasspath.asPath } }\n"
        (directory/'build.gradle').write_text(build)
        (directory/'settings.gradle').write_text("rootProject.name='published-consumer'\n")
    else:
        dependency=lambda g,a,v: f'<dependency><groupId>{g}</groupId><artifactId>{a}</artifactId>'+ (f'<version>{v}</version>' if v else '')+'</dependency>'
        management=''
        if boot:
            imports=''.join(f'<dependency><groupId>{bom["group"]}</groupId><artifactId>{bom["artifact"]}</artifactId><version>{bom["version"]}</version><type>pom</type><scope>import</scope></dependency>' for bom in alignment['boms'])
            overrides=''.join(dependency('org.apache.tomcat.embed',name,alignment['tomcat'][boot]) for name in alignment['tomcatArtifacts'])
            management=f'<dependencyManagement><dependencies>{overrides}{imports}<dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-dependencies</artifactId><version>{boot}</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement>'
        kotlin=''
        if flavor=='kotlin':
            kotlin=f'<plugin><groupId>org.jetbrains.kotlin</groupId><artifactId>kotlin-maven-plugin</artifactId><version>{versions["kotlin"]}</version><configuration><jvmTarget>17</jvmTarget></configuration><executions><execution><id>compile</id><phase>compile</phase><goals><goal>compile</goal></goals></execution></executions></plugin>'
        pom=f'''<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>io.quotaflow.verification</groupId><artifactId>published-consumer</artifactId><version>1</version>
<properties><maven.compiler.release>17</maven.compiler.release><maven.compiler.parameters>true</maven.compiler.parameters><project.build.sourceEncoding>UTF-8</project.build.sourceEncoding></properties>
<repositories><repository><id>staged</id><url>{repository}</url></repository></repositories>{management}
<dependencies>{''.join(dependency(*d) for d in deps)}</dependencies>
<build>{'<sourceDirectory>src/main/kotlin</sourceDirectory>' if flavor=='kotlin' else ''}<plugins>{kotlin}<plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>3.14.0</version></plugin><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-dependency-plugin</artifactId><version>3.8.1</version></plugin></plugins></build></project>'''
        (directory/'pom.xml').write_text(pom)


def classpath_evidence(classpath, candidate, flavor):
    artifacts={}; runtime=[]
    alignment=json.loads((ROOT/'verification/consumer-runtime.json').read_text())
    netty=next(bom['version'] for bom in alignment['boms'] if bom['group']=='io.netty' and bom['artifact']=='netty-bom')
    for value in classpath.split(os.pathsep):
        path=Path(value)
        if not path.is_file() or path.suffix!='.jar': continue
        digest=sha256(path)
        if path.name.startswith('quotaflow-'):
            require(candidate['artifacts'].get(path.name)==digest,'Consumer resolved different published bytes: '+path.name)
            require(path.name not in artifacts,'Duplicate public artifact on consumer classpath')
            artifacts[path.name]=digest
        parts=path.parts
        if 'files-2.1' in parts:
            index=parts.index('files-2.1'); group,name,version=parts[index+1:index+4]
        else:
            marker='maven-repository' if 'maven-repository' in parts else 'repository'
            require(marker in parts,'Unknown dependency origin: '+str(path))
            index=len(parts)-1-list(reversed(parts)).index(marker)
            group='.'.join(parts[index+1:-3]); name,version=parts[-3:-1]
        require(path.name.startswith(name+'-'+version),'Runtime coordinate/file mismatch')
        if group=='io.netty' and not name.startswith('netty-tcnative'):
            require(version==netty,'Consumer resolved an unaligned Netty module: '+name+'@'+version)
        runtime.append({'filename':path.name,'sha256':digest,'purl':purl(group,name,version)})
    require(artifacts,'Consumer resolved no staged modules')
    if flavor=='core':
        require(all(p['filename'].startswith(('quotaflow-core-','slf4j-api-')) for p in runtime),'Core gained a runtime dependency beyond SLF4J')
    return artifacts,runtime


def runtime_sbom(runs, version):
    packages={}
    for run in runs:
        for artifact in run['runtime']:
            packages.setdefault(artifact['purl'],set()).add(artifact['sha256'])
    return {'bomFormat':'CycloneDX','specVersion':'1.6','version':1,
            'metadata':{'component':{'type':'application','name':'Quotaflow staged consumers','version':version}},
            'components':[{'type':'library','bom-ref':url,'purl':url,'name':url.split('/')[-1].split('@')[0],
                           'version':url.rsplit('@',1)[1],'hashes':[{'alg':'SHA-256','content':h} for h in sorted(hashes)]}
                          for url,hashes in sorted(packages.items())]}


def scan_runtimes(directory, manifest, report):
    from scan_release import scan
    from release_tools import tool
    tool('grype',ROOT/'build/release-tools',install=True)
    inputs=directory/'runtime-input'; inputs.mkdir()
    sbom=inputs/'sbom.json'; sbom.write_text(json.dumps(runtime_sbom(report['runs'],manifest['candidate']['version']),indent=2)+'\n')
    input_manifest={'candidate':manifest['candidate'],'sbom':reference(inputs,sbom),
                    'preparedManifestSha256':report['preparedManifestSha256']}
    (inputs/'candidate-manifest.json').write_text(json.dumps(input_manifest,indent=2)+'\n')
    from consumer_security import scoped_reviews
    reviews=scoped_reviews(manifest['candidate'],report['runs'],lambda run:safe_artifact(directory,run['log']).read_text())
    result=scan(inputs,directory/'runtime-security',ROOT/'build/release-security-db',ROOT/'build/release-tools',refresh=True,review_document=reviews)
    require(result['status']=='PASS','Resolved consumer runtime has unresolved vulnerabilities')
    report['runtimeSecurity']={'inputManifest':reference(directory,inputs/'candidate-manifest.json'),
                              'sbom':reference(directory,sbom),'report':reference(directory,directory/'runtime-security/security.json')}


def run_matrix(prepared, output, work, homes, selected=None):
    prepared=Path(prepared).resolve(); output=Path(output).resolve(); work=Path(work).resolve()
    require(not work.exists(),'Staged work directory must be fresh'); work.mkdir(parents=True)
    manifest=json.loads((prepared/'candidate-manifest.json').read_text()); verify_prepared(prepared,manifest)
    candidate=manifest['candidate']; version=candidate['version']; repository=(prepared/'repository').as_uri()
    execution_source=source_identity(ROOT)[1]
    if not selected: require(execution_source==candidate['sourceSha256'],'Full staged verification requires the exact candidate sources')
    compatibility=json.loads((ROOT/'verification/compatibility.json').read_text())
    require(set(homes)=={str(jdk) for jdk in compatibility['testJdks']},'Supply every required JDK')
    directory=output/'consumers'; directory.mkdir(parents=True,exist_ok=False)
    report={'schemaVersion':1,'kind':'staged-consumers','candidate':candidate,'preparedManifestSha256':sha256(prepared/'candidate-manifest.json'),
            'simulation':False,'status':'RUNNING','executionSourceSha256':execution_source,'runs':[]}
    (work/'settings.xml').write_text('<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"/>')
    # Share only the verified wrapper distribution; never user properties/init scripts.
    (work/'gradle-home').mkdir(); (work/'gradle-home/wrapper').symlink_to(Path.home()/'.gradle/wrapper',target_is_directory=True)
    try:
        with redis_server() as (url,container):
            for flavor in ('core','kotlin','redis','micrometer','starter'):
                if selected and flavor not in selected: continue
                boots=[entry['version'] for entry in compatibility['springBoot']] if flavor=='starter' else [None]
                for boot in boots:
                    for jdk,home in homes.items():
                        environment=isolated_environment(work,home)
                        for tool in ('maven','gradle'):
                            prefix=flavor+('-'+boot if boot else '')+'-'+tool+'-jdk'+jdk
                            project=work/'projects'/prefix; fixture(project,flavor,tool,version,repository,boot)
                            buildlog=directory/(prefix+'-build.log')
                            if tool=='maven':
                                command(['mvn','-B','-U','-s',str(work/'settings.xml'),'-gs',str(work/'settings.xml'),'-Dmaven.repo.local='+str(work/'maven-repository'),'-f',str(project/'pom.xml'),'compile','dependency:build-classpath','-Dmdep.outputFile='+str(project/'runtime-classpath.txt')],buildlog,environment)
                                classpath=str(project/'target/classes')+os.pathsep+(project/'runtime-classpath.txt').read_text().strip()
                            else:
                                command([str(ROOT/'gradlew'),'-p',str(project),'--no-daemon','--refresh-dependencies','writeRuntimeClasspath'],buildlog,environment)
                                classpath=(project/'runtime-classpath.txt').read_text().strip()
                            artifacts,runtime=classpath_evidence(classpath,candidate,flavor)
                            stacks=('servlet','reactive') if flavor=='starter' else (None,)
                            for stack in stacks:
                                case=(f'starter-{boot}-{stack}-{tool}-jdk{jdk}' if stack else prefix)
                                log=directory/(case+'.log')
                                properties={'verification.case':case,'verification.jdk':jdk,'verification.redisUrl':url,
                                            'verification.namespace':'staged-'+uuid.uuid4().hex}
                                if stack: properties.update({'verification.boot':boot,'verification.stack':stack})
                                args=[str(Path(home)/'bin/java'),*[f'-D{k}={v}' for k,v in properties.items()],'-cp',classpath,MAINS[flavor]]
                                command(args,log,environment,timeout=90)
                                require('PUBLISHED CONSUMERS VERIFIED case='+case+'\n' in log.read_text(),'Consumer did not finish assertions: '+case)
                                if flavor=='starter': require('STAGED RECOVERY VERIFIED unready=reject duplicate=reject distributed=healthy\n' in log.read_text(),'Default starter recovery was not verified')
                                report['runs'].append({'id':case,'jdk':int(jdk),'status':'PASS','exitCode':0,'origin':'staged-repository',
                                                       'artifacts':artifacts,'runtime':runtime,'log':reference(directory,log)})
                                (directory/'results.json').write_text(json.dumps(report,indent=2)+'\n')
                                print(case+': PASS',flush=True)
            if not selected:
                scan_runtimes(directory,manifest,report)
                from run_readme_examples import run_examples
                run_examples(prepared,output,work,homes,url,container)
        report['status']='PASS' if not selected else 'DIAGNOSTIC'
        require(json.loads((prepared/'candidate-manifest.json').read_text())==manifest,'Prepared candidate changed')
        if not selected: require(source_identity(ROOT)[1]==execution_source,'Fixture sources changed during staged verification')
        verify_prepared(prepared,manifest)
    except BaseException:
        report['status']='FAILED'; raise
    finally: (directory/'results.json').write_text(json.dumps(report,indent=2)+'\n')
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prepared',type=Path,required=True); parser.add_argument('--output',type=Path,required=True); parser.add_argument('--work',type=Path,required=True)
    parser.add_argument('--java-home',action='append',required=True); parser.add_argument('--only',action='append',choices=list(MAINS))
    args=parser.parse_args(); run_matrix(args.prepared,args.output,args.work,dict(v.split('=',1) for v in args.java_home),args.only)
