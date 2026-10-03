#!/usr/bin/env python3
"""Build the core twice in isolated source/output trees and compare release inputs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--version", default="0.1.0-SNAPSHOT")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    ignored = shutil.ignore_patterns(".git", ".gradle", "build", ".agents", ".codex", "openspec", "research", "__pycache__")
    results = []
    for number in (1, 2):
        with tempfile.TemporaryDirectory(prefix=f"quotaflow-repro-{number}-") as temporary:
            checkout = Path(temporary) / "source"
            shutil.copytree(root, checkout, ignore=ignored)
            command = [str(checkout / "gradlew"), ":quotaflow-core:jar", ":quotaflow-core:sourcesJar",
                       ":quotaflow-core:generatePomFileForMavenPublication", "--no-build-cache",
                       "--no-configuration-cache", "--rerun-tasks", "--console=plain", "-PtestJdk=17",
                       "-Pversion=" + args.version]
            with (output / f"build-{number}.log").open("w") as log:
                subprocess.run(command, cwd=checkout, stdout=log, stderr=subprocess.STDOUT, check=True)
            candidates = [checkout / f"quotaflow-core/build/libs/quotaflow-core-{args.version}.jar",
                          checkout / f"quotaflow-core/build/libs/quotaflow-core-{args.version}-sources.jar",
                          checkout / "quotaflow-core/build/publications/maven/pom-default.xml"]
            result = {}
            destination = output / str(number)
            destination.mkdir()
            for artifact in candidates:
                result[artifact.name] = hashlib.sha256(artifact.read_bytes()).hexdigest()
                shutil.copy2(artifact, destination / artifact.name)
            results.append(result)
    report = {"schemaVersion": 1, "version": args.version, "builds": results,
              "status": "PASS" if results[0] == results[1] else "FAILED"}
    (output / "reproducibility.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
