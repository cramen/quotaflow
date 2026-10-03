#!/usr/bin/env python3
"""Download a pinned verification archive and extract regular files safely."""
import argparse
import hashlib
from pathlib import Path
import shutil
import stat
import tempfile
import urllib.request
import zipfile
from verify_performance_bundle import require

MAX_BYTES = 2 * 1024 * 1024 * 1024


def extract(archive, output):
    with zipfile.ZipFile(archive) as source:
        entries = source.infolist()
        require(sum(e.file_size for e in entries) <= MAX_BYTES, "Evidence archive exceeds size limit")
        names = set()
        for entry in entries:
            target = (output / entry.filename).resolve()
            require(target.is_relative_to(output.resolve()) and ".." not in Path(entry.filename).parts and entry.filename not in names, "Unsafe or duplicate archive path")
            names.add(entry.filename)
            mode = entry.external_attr >> 16
            require(not stat.S_ISLNK(mode) and (stat.S_IFMT(mode) in (0, stat.S_IFREG, stat.S_IFDIR)), "Only regular evidence files are accepted")
        output.mkdir(parents=True, exist_ok=False)
        for entry in entries:
            target = output / entry.filename
            if entry.is_dir(): target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with source.open(entry) as reader, target.open("wb") as writer: shutil.copyfileobj(reader, writer)
    require((output / "manifest.json").is_file(), "Evidence manifest must be at archive root")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(args.url.startswith("https://"), "An HTTPS evidence URL is required")
    require(len(args.sha256) == 64 and all(c in "0123456789abcdef" for c in args.sha256), "An explicit archive SHA-256 is required")
    with tempfile.TemporaryFile() as archive:
        sha, total = hashlib.sha256(), 0
        with urllib.request.urlopen(args.url, timeout=60) as response:
            require(response.url.startswith("https://"), "Evidence redirect must remain HTTPS")
            while chunk := response.read(1024 * 1024):
                total += len(chunk); require(total <= MAX_BYTES, "Evidence download exceeds size limit")
                archive.write(chunk); sha.update(chunk)
        require(sha.hexdigest() == args.sha256, "Evidence archive digest mismatch")
        archive.seek(0); extract(archive, args.output)


if __name__ == "__main__":
    main()
