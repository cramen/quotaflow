#!/usr/bin/env python3
"""PGP signatures for Maven assets and the canonical release manifest."""
import argparse
import contextlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from release_common import require, safe_artifact, sha256
from release_tools import ROOT

CHECKSUMS = {".md5", ".sha1", ".sha256", ".sha512"}


def fingerprint(value):
    require(isinstance(value, str), "Configure the project's trusted PGP fingerprint and public key")
    normalized = value.replace(" ", "").upper()
    require(re.fullmatch(r"(?:[0-9A-F]{40}|[0-9A-F]{64})", normalized), "Invalid full PGP fingerprint")
    return normalized


@contextlib.contextmanager
def gpg_home():
    with tempfile.TemporaryDirectory(prefix="quotaflow-pgp-", dir="/private/tmp" if Path("/private/tmp").exists() else None) as name:
        home = Path(name); home.chmod(0o700)
        try:
            yield home
        finally:
            subprocess.run(["gpgconf", "--homedir", str(home), "--kill", "gpg-agent"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def gpg(home, arguments, input_data=None):
    result = subprocess.run(["gpg", "--no-options", "--homedir", str(home), "--batch", "--no-tty", *arguments],
                            input=input_data, capture_output=True)
    require(result.returncode == 0, "PGP operation failed; credential material is not logged")
    return result.stdout.decode("utf-8", errors="replace")


def import_public(home, public_key, expected):
    expected = fingerprint(expected)
    gpg(home, ["--import", str(Path(public_key).resolve())])
    listing = gpg(home, ["--with-colons", "--list-keys"])
    primary, waiting = [], False
    for line in listing.splitlines():
        fields = line.split(":")
        if fields[0] == "pub": waiting = True
        elif fields[0] == "sub": waiting = False
        elif fields[0] == "fpr" and waiting:
            primary.append(fields[9]); waiting = False
    require(primary == [expected], "Public key does not match the single trusted primary identity")


def verify_in_home(home, artifact, signature, expected):
    require(Path(signature).is_file(), "Missing PGP signature")
    status = gpg(home, ["--status-fd", "1", "--verify", str(Path(signature).resolve()), str(Path(artifact).resolve())])
    records = [line.split() for line in status.splitlines() if line.startswith("[GNUPG:] ")]
    forbidden = {"BADSIG", "ERRSIG", "EXPSIG", "EXPKEYSIG", "REVKEYSIG", "KEYEXPIRED", "SIGEXPIRED", "KEYREVOKED", "NO_PUBKEY"}
    require(not any(row[1] in forbidden for row in records), "PGP signature or signing key is invalid/expired/revoked")
    valid = [row for row in records if row[1] == "VALIDSIG"]
    require(len(valid) == 1 and len(valid[0]) >= 11, "Missing unambiguous PGP validity evidence")
    row = valid[0]
    require(fingerprint(expected) in (row[2], row[11] if len(row) > 11 else row[2]), "PGP signer identity mismatch")
    require(row[9] in ("8", "9", "10") and row[10] == "00", "PGP requires a strong binary-document signature")


def verify_detached(artifact, signature, public_key, expected):
    with gpg_home() as home:
        import_public(home, public_key, expected)
        verify_in_home(home, artifact, signature, expected)


def sign_assets(candidate_root, private_key, password, public_key, expected, test_only=False):
    root = Path(candidate_root).resolve(); expected = fingerprint(expected)
    manifest = json.loads((root / "candidate-manifest.json").read_text())
    require(test_only or manifest["mode"] == "release", "Dry-run candidates cannot receive production signing evidence")
    required = [a for a in manifest["assets"] if Path(a["path"]).suffix not in CHECKSUMS | {".asc"}]
    require(required, "No Maven assets to sign")
    output = root / "signatures/pgp"; output.mkdir(parents=True, exist_ok=True)
    public_output = output / "public-key.asc"
    if public_output.exists(): require(public_output.read_bytes() == Path(public_key).read_bytes(), "Refusing to replace a different public key")
    else: shutil.copyfile(public_key, public_output)
    signed = []
    with gpg_home() as home:
        import_public(home, public_output, expected)
        secret = home / "signing-key.asc"
        descriptor = os.open(secret, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream: stream.write(private_key.encode() if isinstance(private_key,str) else private_key)
        gpg(home, ["--import", str(secret)]); secret.unlink()
        for reference in required:
            artifact = safe_artifact(root, reference); signature = Path(str(artifact) + ".asc")
            if not signature.exists():
                gpg(home, ["--pinentry-mode", "loopback", "--passphrase-fd", "0", "--local-user", expected,
                           "--digest-algo", "SHA256", "--armor", "--detach-sign", "--output", str(signature), str(artifact)],
                    (password + "\n").encode())
            verify_in_home(home, artifact, signature, expected)
            require(sha256(artifact) == reference["sha256"], "Artifact changed while signing")
            signed.append({"artifact": reference, "signature": {"path": signature.relative_to(root).as_posix(), "sha256": sha256(signature)}})
    report = {"schemaVersion": 1, "status": "PASS", "production": not test_only, "candidate": manifest["candidate"],
              "fingerprint": expected, "publicKey": {"path": public_output.relative_to(root).as_posix(), "sha256": sha256(public_output)},
              "signedAssets": signed}
    (output / "pgp.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def verify_pgp(candidate_root, report, manifest, expected, production=True):
    root = Path(candidate_root); expected = fingerprint(expected)
    require(report["status"] == "PASS" and report["candidate"] == manifest["candidate"] and report["fingerprint"] == expected,
            "PGP evidence belongs to another candidate or identity")
    require(not production or report["production"] is True, "Test signatures cannot authorize publication")
    expected_assets = {a["path"]: a for a in manifest["assets"] if Path(a["path"]).suffix not in CHECKSUMS | {".asc"}}
    require(len(report["signedAssets"]) == len(expected_assets) and
            {a["artifact"]["path"] for a in report["signedAssets"]} == set(expected_assets), "Incomplete PGP signature set")
    public_key = safe_artifact(root, report["publicKey"])
    with gpg_home() as home:
        import_public(home, public_key, expected)
        for entry in report["signedAssets"]:
            require(entry["artifact"] == expected_assets[entry["artifact"]["path"]], "Changed signed Maven asset")
            require(entry["signature"]["path"] == entry["artifact"]["path"] + ".asc",
                    "PGP signature is not the required Maven sidecar")
            verify_in_home(home, safe_artifact(root, entry["artifact"]), safe_artifact(root, entry["signature"]), expected)


def manifest_identity(blob, policy, tag, commit):
    manifest=json.loads(Path(blob).read_text())
    require(manifest.get('repository')==policy['repository'] and manifest['candidate']['commit']==commit
            and 'v'+manifest['candidate']['version']==tag and manifest['mode']=='release',
            'Signed manifest repository, tag, commit or mode mismatch')


def verify_manifest(blob, signature, policy, tag, commit):
    manifest_identity(blob,policy,tag,commit)
    expected=fingerprint(policy['pgp']['fingerprint'])
    verify_detached(blob,signature,ROOT/policy['pgp']['publicKey'],expected)
    return {'status':'PASS','fingerprint':expected,'manifestSha256':sha256(blob),
            'signatureSha256':sha256(signature),'commit':commit,'tag':tag}


def sign_manifest(blob, private_key, password, policy):
    require(os.environ.get('GITHUB_ACTIONS')!='true','Signing is local-only')
    blob=Path(blob); manifest=json.loads(blob.read_text());candidate=manifest['candidate']
    manifest_identity(blob,policy,'v'+candidate['version'],candidate['commit'])
    expected=fingerprint(policy['pgp']['fingerprint']);before=sha256(blob)
    signature=Path(str(blob)+'.asc')
    with gpg_home() as home:
        import_public(home,ROOT/policy['pgp']['publicKey'],expected)
        secret=home/'key.gpg'
        descriptor=os.open(secret,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(descriptor,'wb') as stream:
            stream.write(private_key.encode() if isinstance(private_key,str) else private_key)
        gpg(home,['--import',str(secret)]);secret.unlink()
        if not signature.exists():
            gpg(home,['--pinentry-mode','loopback','--passphrase-fd','0','--local-user',expected,
                      '--digest-algo','SHA256','--armor','--detach-sign','--output',str(signature.resolve()),str(blob.resolve())],
                (password+'\n').encode())
        verify_in_home(home,blob,signature,expected)
    require(sha256(blob)==before,'Manifest changed while signing')
    return verify_manifest(blob,signature,policy,'v'+candidate['version'],candidate['commit'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("pgp-sign", "manifest-sign")); parser.add_argument("--candidate", type=Path)
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args(); policy = json.loads((ROOT / "verification/release-policy.json").read_text())
    try:
        from local_release import pgp_configuration
        expected = fingerprint(policy['pgp']['fingerprint'])
        require(os.environ.get('GITHUB_ACTIONS')!='true','Signing is local-only')
        private,password,key_id=pgp_configuration()
        require(key_id is None or expected.endswith(key_id),'Configured local key ID differs from approved fingerprint')
        if args.operation == 'pgp-sign':
            require(args.candidate is not None, 'Candidate directory is required')
            report = sign_assets(args.candidate, private, password, ROOT / policy['pgp']['publicKey'], expected)
        else:
            require(args.manifest is not None, 'Manifest path is required')
            report = sign_manifest(args.manifest,private,password,policy)
        print(json.dumps(report, indent=2)); return 0
    except (ValueError, OSError, KeyError, TypeError) as failure:
        print(json.dumps({"status": "FAILED", "error": str(failure)})); return 1


if __name__ == "__main__":
    raise SystemExit(main())
