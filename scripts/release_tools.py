#!/usr/bin/env python3
"""Install and verify digest-pinned release tools in a local build directory."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import platform
import tarfile
import urllib.request

from release_common import require, sha256

ROOT = Path(__file__).resolve().parents[1]


def host_platform():
    architecture = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}.get(platform.machine())
    require(architecture is not None, "Unsupported release-tool architecture")
    return platform.system().lower() + "-" + architecture


def tool(name, directory, install=False, platform_name=None):
    policy = json.loads((ROOT / "verification/release-tooling.json").read_text())["tools"][name]
    selected = policy["platforms"][platform_name or host_platform()]
    destination = Path(directory) / name / policy["version"] / (platform_name or host_platform())
    archive = destination / "download"
    binary = destination / selected["binary"]
    if not archive.exists():
        require(install, "Pinned release tool is not installed: " + name)
        require(selected["url"].startswith("https://github.com/" + policy["repository"] + "/releases/download/"),
                "Unexpected release-tool download origin")
        destination.mkdir(parents=True, exist_ok=True)
        request = urllib.request.Request(selected["url"], headers={"User-Agent": "quotaflow-release-preparation"})
        with urllib.request.urlopen(request, timeout=120) as response:
            require(response.url.startswith("https://"), "Insecure tool download redirect")
            data = response.read(512 * 1024 * 1024 + 1)
        require(len(data) <= 512 * 1024 * 1024 and hashlib.sha256(data).hexdigest() == selected["sha256"],
                "Release-tool download digest mismatch")
        archive.write_bytes(data)
    require(not archive.is_symlink() and sha256(archive) == selected["sha256"], "Cached release-tool input changed")
    data = archive.read_bytes()
    if selected["format"] == "tar.gz":
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as package:
            entries = [m for m in package.getmembers() if m.name == selected["binary"]]
            require(len(entries) == 1 and entries[0].isfile() and entries[0].size <= 512 * 1024 * 1024,
                    "Invalid release-tool archive")
            data = package.extractfile(entries[0]).read()
    if not binary.exists():
        require(install, "Pinned release-tool executable is missing")
        binary.write_bytes(data); binary.chmod(0o755)
    require(not binary.is_symlink() and sha256(binary) == hashlib.sha256(data).hexdigest(), "Release-tool executable changed")
    return binary.resolve(), {"name": name, "version": policy["version"], "platform": platform_name or host_platform(),
                              "downloadSha256": selected["sha256"], "binarySha256": sha256(binary)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", choices=("grype", "cosign")); parser.add_argument("--directory", type=Path, default=ROOT / "build/release-tools")
    parser.add_argument("--install", action="store_true")
    args = parser.parse_args()
    binary, identity = tool(args.name, args.directory, args.install)
    print(json.dumps({**identity, "path": str(binary)}, indent=2))


if __name__ == "__main__":
    main()
