#!/usr/bin/env python3
"""Build and execute native store and Spring fixtures, preserving separate evidence."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
from verify_performance_bundle import digest, require

ROOT = Path(__file__).resolve().parents[1]


def run(command, log, env):
    with log.open("w") as output:
        subprocess.run([str(c) for c in command], cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--graal-home", type=Path, required=True)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--version", default="0.1.0-SNAPSHOT")
    parser.add_argument("--redis-url", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fixtures", nargs="+", choices=["store", "servlet", "reactive"], default=["store", "servlet", "reactive"])
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, JAVA_HOME=str(args.graal_home.resolve()), REDIS_URL=args.redis_url)
    for kind in args.fixtures:
        output = args.output / kind; output.mkdir()
        if kind == "store":
            command = [ROOT / "gradlew", ":quotaflow-native-smoke:nativeCompile", "--console=plain"]
            executable = ROOT / "quotaflow-native-smoke/build/native/nativeCompile/quotaflow-native-smoke"
            invocation = [executable]; marker = "native-smoke OK"
        else:
            command = [ROOT / "gradlew", "-p", "verification/native", "nativeCompile", "--refresh-dependencies",
                       "-PverificationRepository=" + str(args.repository.resolve()), "-PcandidateVersion=" + args.version,
                       "-PwebStack=" + kind, "--console=plain"]
            executable = ROOT / f"verification/native/build/native/nativeCompile/quotaflow-{kind}"
            invocation = [executable, "-Dverification.stack=" + kind]; marker = "NATIVE VERIFIED"
        run(command, output / "build.log", env)
        run(invocation, output / "run.log", env)
        require(marker in (output / "run.log").read_text(), "Native fixture did not complete its assertions")
        report = {"native": True, "exitCode": 0, "fixture": kind, "executableSha256": digest(executable),
                  "expectedMarker": marker, "runLog": {"path": "run.log", "sha256": digest(output / "run.log")}}
        (output / "native.json").write_text(json.dumps(report, indent=2) + "\n")
        print("Native verified: " + kind, flush=True)


if __name__ == "__main__":
    main()
