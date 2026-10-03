#!/usr/bin/env python3
"""Collect local allocation/JFR/wire diagnostics; these are never comparison evidence."""
import argparse
import json
import os
from pathlib import Path
import subprocess
from run_local_benchmarks import command, snapshot
from verify_performance_bundle import digest, require


def wire_stats(container):
    raw = command(["docker", "exec", container, "redis-cli", "--raw", "INFO", "stats"])
    values = dict(line.split(":", 1) for line in raw.splitlines() if ":" in line)
    return {key: int(values[key]) for key in ("total_net_input_bytes", "total_net_output_bytes", "total_commands_processed")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--java", type=Path, required=True)
    parser.add_argument("--host-probe", type=Path, required=True)
    parser.add_argument("--redis-url", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") != "true", "Profiling workloads are local-only")
    profile = json.loads(args.profile.read_text())
    require(digest(args.host_probe) == profile["hostProbeSha256"], "Host probe mismatch")
    output = args.output.resolve(); output.mkdir(parents=True, exist_ok=False)
    before = snapshot(profile, args.host_probe)
    (output / "before.json").write_text(json.dumps(before, indent=2))
    require(before["stable"], "Host conditions are not controlled")
    wire_before = wire_stats(profile["container"])
    invocation = [str(args.java), *profile["jvmArgs"], "-jar", str(args.jar.resolve()), "CertificationBenchmark.latency",
                  "-p", "path=distributed-hierarchy,fallback-facade", "-f", "1", "-t", "1",
                  "-wi", "2", "-w", "2s", "-i", "3", "-r", "3s", "-foe", "true",
                  "-prof", "gc", "-prof", f"jfr:dir={output / 'jfr'};configName=profile;stackDepth=128",
                  "-rf", "json", "-rff", str(output / "jmh.json")]
    with (output / "run.log").open("w") as log:
        result = subprocess.run(invocation, stdout=log, stderr=subprocess.STDOUT,
                                env=dict(os.environ, QUOTAFLOW_BENCHMARK_REDIS_URL=args.redis_url))
    after = snapshot(profile, args.host_probe)
    (output / "after.json").write_text(json.dumps(after, indent=2))
    wire_after = wire_stats(profile["container"])
    report = {"status": "PROFILING_ONLY", "comparison": "NOT ASSESSED", "exitCode": result.returncode,
              "benchmarkJarSha256": digest(args.jar), "environmentStable": before["stable"] and after["stable"],
              "wireDelta": {key: wire_after[key] - wire_before[key] for key in wire_before},
              "wireScope": "Entire profiling series, including setup and control-plane traffic; not bytes per decision.",
              "recordings": [str(p.relative_to(output)) for p in output.rglob("*.jfr")],
              "sampling": "JDK profile settings; event absence is not proof of zero allocation or contention."}
    (output / "profiling.json").write_text(json.dumps(report, indent=2) + "\n")
    require(result.returncode == 0 and report["environmentStable"] and len(report["recordings"]) >= 4,
            "Incomplete or uncontrolled profiling run")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
