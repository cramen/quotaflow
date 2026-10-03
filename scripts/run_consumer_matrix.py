"""Verify external Gradle and Maven resolution against a staged local candidate."""
import argparse
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "verification/consumer"
MAIN = "io.quotaflow.verification.consumer.CompatibilityApplication"


def run(command, log, environment=None):
    with log.open("w") as output:
        result = subprocess.run(command, cwd=ROOT, env=environment, stdout=output, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(f"Consumer command failed: {log}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--candidate-version", default="0.1.0-SNAPSHOT")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--java-home", action="append", required=True, help="JDK feature=installation directory")
    parser.add_argument("--redis-url", required=True, help="Disposable Redis endpoint used for a real transport probe")
    parser.add_argument("--boot", action="append", help="Limit diagnostic execution to these Boot versions")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    homes = dict(item.split("=", 1) for item in args.java_home)
    manifest = json.loads((ROOT / "verification/compatibility.json").read_text())
    versions = args.boot or [entry["version"] for entry in manifest["springBoot"]]
    repository = args.repository.resolve().as_uri()
    results = []
    report = output / "consumer-results.json"
    for boot in versions:
        for feature, home in homes.items():
            java_home = Path(home)
            environment = {**os.environ, "JAVA_HOME": str(java_home),
                           "PATH": str(java_home / "bin") + os.pathsep + os.environ["PATH"]}
            classpath = output / f"maven-{boot}-jdk{feature}-classpath.txt"
            run(["mvn", "-B", "-U", "-f", str(FIXTURE / "pom.xml"), "clean", "compile", "dependency:build-classpath",
                 f"-DverificationRepository={repository}", f"-DbootVersion={boot}",
                 f"-DcandidateVersion={args.candidate_version}", f"-Dmdep.outputFile={classpath}"],
                output / f"maven-{boot}-jdk{feature}-build.log", environment)
            for stack in ["servlet", "reactive"]:
                for build_tool in ["gradle", "maven"]:
                    label = f"{build_tool}-{boot}-jdk{feature}-{stack}"
                    log = output / f"{label}.log"
                    if build_tool == "gradle":
                        command = [str(ROOT / "gradlew"), "-p", str(FIXTURE), "run", "--refresh-dependencies",
                                   f"-PverificationRepository={repository}", f"-PbootVersion={boot}",
                                   f"-PcandidateVersion={args.candidate_version}", f"-PredisUrl={args.redis_url}", f"-PtestJdk={feature}", f"-PwebStack={stack}"]
                    else:
                        command = [str(java_home / "bin/java"), f"-Dverification.boot={boot}",
                                   f"-Dverification.jdk={feature}", f"-Dverification.stack={stack}", f"-Dverification.redisUrl={args.redis_url}",
                                   "-cp", str(FIXTURE / "target/classes") + os.pathsep + classpath.read_text().strip(), MAIN]
                    run(command, log, environment)
                    if f"CONSUMER VERIFIED boot={boot} stack={stack} jdk={feature}" not in log.read_text():
                        raise RuntimeError(f"Consumer produced no matching execution evidence: {log}")
                    results.append(dict(tool=build_tool, boot=boot, jdk=int(feature), stack=stack, status="PASS", log=log.name))
                    report.write_text(json.dumps(results, indent=2) + "\n")
                    print(label + ": PASS", flush=True)


if __name__ == "__main__":
    main()
