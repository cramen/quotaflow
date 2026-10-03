#!/usr/bin/env python3
"""Copy explicitly captured stage evidence into an immutable release directory."""
import argparse
import json
from pathlib import Path
import shutil
from verify_performance_bundle import digest, require
from verify_release_evidence import REQUIRED


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--stages", required=True, type=Path, help="JSON mapping gate names to captured stage directories")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    candidate = json.loads(args.candidate.read_text())
    stages = json.loads(args.stages.read_text())
    require(set(stages) == REQUIRED, "Every required stage must be supplied")
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {"schemaVersion": 1, "candidate": candidate, "stages": {}}
    for kind, location in stages.items():
        source = Path(location).resolve()
        stage = json.loads((source / "stage.json").read_text())
        require(stage["candidate"] == candidate, "Stale stage: " + kind)
        require(not args.output.resolve().is_relative_to(source), "Output cannot be nested inside an input stage")
        destination = args.output / kind
        require(not any(p.is_symlink() for p in source.rglob("*")), "Stage directory contains symbolic links")
        shutil.copytree(source, destination)
        # Stage paths are relative to their original directory. Prefix envelope
        # references; nested raw reports retain paths relative to their own directory.
        def relocate(value):
            if isinstance(value, dict):
                if set(value) == {"path", "sha256"}:
                    value["path"] = kind + "/" + value["path"]
                else:
                    for child in value.values(): relocate(child)
            elif isinstance(value, list):
                for child in value: relocate(child)
        relocate(stage)
        (destination / "stage.json").write_text(json.dumps(stage, indent=2) + "\n")
        relative = kind + "/stage.json"
        manifest["stages"][kind] = {"path": relative, "sha256": digest(args.output / relative)}
    path = args.output / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2) + "\n")
    print("Release manifest SHA-256: " + digest(path))


if __name__ == "__main__":
    main()
