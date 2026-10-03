#!/usr/bin/env python3
"""Capture exact source and artifact identities, including uncommitted candidate edits."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def source_identity(root):
    root = Path(root)
    names = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root).decode().split("\0")
    names = sorted({name for name in names if name and not name.startswith((".agents/", ".codex/", "research/", "openspec/")) and (root / name).is_file()})
    files = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names}
    encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return files, hashlib.sha256(encoded).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--version", required=True)
    parser.add_argument("--artifacts", nargs="+", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    files, source_hash = source_identity(args.root)
    identity = {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.root).decode().strip(),
                "sourceSha256": source_hash, "version": args.version,
                "artifacts": {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in args.artifacts}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(identity, indent=2) + "\n")
    args.output.with_name(args.output.stem + "-sources.json").write_text(json.dumps(files, indent=2) + "\n")


if __name__ == "__main__":
    main()
