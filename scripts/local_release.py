"""Local operator configuration and process serialization; never prints secrets."""
import contextlib
import fcntl
import os
from pathlib import Path
import re
import subprocess
from release_common import require


def properties(path):
    """Read Java properties, including escapes and continued logical lines."""
    if not Path(path).exists(): return {}
    text=Path(path).read_bytes().decode('iso-8859-1')
    lines=[]; pending=''
    for line in text.splitlines():
        line=line.lstrip(' \t\f')
        if not pending and (not line or line.startswith(('#','!'))): continue
        pending+=line
        if (len(pending)-len(pending.rstrip('\\')))%2:
            pending=pending[:-1]; continue
        lines.append(pending); pending=''
    if pending: lines.append(pending)
    def unescape(value):
        def replace(match):
            token=match[1]
            if token.startswith('u'):
                require(re.fullmatch(r'u[0-9a-fA-F]{4}',token), 'Malformed Java property escape')
                return chr(int(token[1:],16))
            return {'t':'\t','r':'\r','n':'\n','f':'\f'}.get(token,token)
        return re.sub(r'\\(u.{0,4}|.)',replace,value)
    result={}
    for line in lines:
        match=re.match(r'((?:\\.|[^\\:=\s])*)(.*)',line)
        key,tail=match.groups(); tail=tail.lstrip(' \t\f')
        if tail.startswith(('=',':')): tail=tail[1:]
        result[unescape(key)]=unescape(tail.lstrip(' \t\f'))
    return result


def local_properties(environment=None):
    env=os.environ if environment is None else environment
    return properties(Path(env.get('GRADLE_USER_HOME',str(Path.home()/'.gradle')))/'gradle.properties')


def central_environment():
    env=dict(os.environ); values=local_properties()
    for name,alias in [('mavenCentralUsername','CENTRAL_PORTAL_USERNAME'),('mavenCentralPassword','CENTRAL_PORTAL_TOKEN')]:
        canonical='ORG_GRADLE_PROJECT_'+name
        if canonical not in env and alias not in env and name in values: env[canonical]=values[name]
    return env


def github_token():
    token=os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
    if token: return token
    result=subprocess.run(['gh','auth','token','--hostname','github.com'],capture_output=True,text=True)
    require(result.returncode==0 and result.stdout.strip(), 'Local GitHub authentication is unavailable; configure gh auth login')
    return result.stdout.strip()


def pgp_configuration():
    values=local_properties(); env=os.environ
    def value(name):return env.get('ORG_GRADLE_PROJECT_'+name,values.get(name))
    key=value('signingInMemoryKey') or env.get('SIGNING_KEY')
    if key:
        return key.encode(),value('signingInMemoryKeyPassword') or env.get('SIGNING_PASSWORD',''),None
    path=value('signing.secretKeyRingFile'); key_id=value('signing.keyId')
    require(path and key_id, 'Configure signing.secretKeyRingFile and signing.keyId in local Gradle properties')
    require(re.fullmatch(r'(?:0x)?[0-9a-fA-F]{8,64}',key_id) is not None, 'Invalid local PGP key ID')
    path=Path(path).expanduser()
    require(path.is_absolute() and path.is_file(), 'Local PGP keyring file is unavailable; use an absolute path')
    return path.read_bytes(),value('signing.password') or '',key_id.removeprefix('0x').upper()


@contextlib.contextmanager
def publication_lock(repository,version):
    require(os.environ.get('GITHUB_ACTIONS')!='true','Publication is local-only')
    directory=Path.home()/'.quotaflow/release-locks'; directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    import hashlib
    name=hashlib.sha256((repository+'\n'+version).encode()).hexdigest()
    descriptor=os.open(directory/(name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    try:
        try: fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError: raise ValueError('Another local process is publishing this version') from None
        yield
    finally: os.close(descriptor)
