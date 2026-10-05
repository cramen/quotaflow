#!/usr/bin/env python3
"""Opt-in real Cosign verification against a digest-pinned public vendor vector.

This test downloads public bytes and trust metadata; it never signs or publishes.
The vendor's main-branch identity is a test vector only. Quotaflow production
verification continues to require its own exact tag workflow identity.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import urllib.request

from release_common import require, sha256
from release_signatures import sigstore_environment
from release_tools import ROOT, tool

FILES = {
    "grype_0.120.0_checksums.txt": "d7c05f43143d2bb77a49cf91ad81514e9774433c35a1a901850612eb9edc8fb6",
    "grype_0.120.0_checksums.txt.sigstore.json": "d0af7551389e2740b58d8b6737597d0a2706308f95448e8e63f0ab64d34361b8",
}


def run(directory, tools):
    directory.mkdir(parents=True, exist_ok=True)
    for name, expected in FILES.items():
        path = directory / name
        if not path.exists():
            url = "https://github.com/anchore/grype/releases/download/v0.120.0/" + name
            with urllib.request.urlopen(url, timeout=60) as response:
                require(response.url.startswith("https://"), "Insecure vector redirect")
                data = response.read(2 * 1024 * 1024 + 1)
            require(len(data) <= 2 * 1024 * 1024 and hashlib.sha256(data).hexdigest() == expected,
                    "Sigstore test vector download changed")
            path.write_bytes(data)
        require(sha256(path) == expected, "Sigstore test vector digest mismatch")
    binary, identity = tool("cosign", tools)
    blob = directory / "grype_0.120.0_checksums.txt"
    command = [str(binary), "verify-blob", "--bundle", str(directory / "grype_0.120.0_checksums.txt.sigstore.json"),
               "--certificate-identity", "https://github.com/anchore/grype/.github/workflows/release.yaml@refs/heads/main",
               "--certificate-oidc-issuer", "https://token.actions.githubusercontent.com",
               "--certificate-github-workflow-repository", "anchore/grype",
               "--certificate-github-workflow-ref", "refs/heads/main",
               "--certificate-github-workflow-sha", "10fbc043ecbaf8436daa9efac9f1632557caf277", str(blob)]
    results = []
    def check(name, args, expected_success):
        result = subprocess.run(args, capture_output=True, text=True, env=sigstore_environment(tools), timeout=120)
        (directory / (name + ".log")).write_text(result.stdout + result.stderr)
        require((result.returncode == 0) == expected_success, "Unexpected real Sigstore verification result: " + name)
        results.append({"control": name, "exitCode": result.returncode, "expectedSuccess": expected_success})
    check("valid", command, True)
    for name, flag, value in (
        ("wrong-workflow", "--certificate-identity", "https://github.com/anchore/grype/.github/workflows/other.yaml@refs/heads/main"),
        ("wrong-repository", "--certificate-github-workflow-repository", "cramen/quotaflow"),
        ("wrong-ref", "--certificate-github-workflow-ref", "refs/tags/v0.120.0"),
        ("wrong-commit", "--certificate-github-workflow-sha", "0" * 40),
        ("wrong-issuer", "--certificate-oidc-issuer", "https://accounts.google.com"),
    ):
        changed = command.copy(); changed[changed.index(flag) + 1] = value
        check(name, changed, False)
    changed_blob = directory / "tampered-checksums.txt"
    changed_blob.write_bytes(blob.read_bytes() + b"modified\n")
    check("wrong-digest", command[:-1] + [str(changed_blob)], False)
    missing = command.copy(); missing[missing.index("--bundle") + 1] = str(directory / "absent.sigstore.json")
    check("missing-bundle", missing, False)
    report = {"status": "PASS", "productionSigning": False, "tool": identity, "vectors": FILES, "controls": results}
    (directory / "controls.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--tools", type=Path, default=ROOT / "build/release-tools")
    args = parser.parse_args()
    print(json.dumps(run(args.output.resolve(), args.tools), indent=2))
