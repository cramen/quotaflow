#!/usr/bin/env python3
"""Collect every local baseline/candidate JMH run; no GitHub execution is permitted."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time
from verify_performance_bundle import digest, require


def command(args):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT).strip()


SERVER_SETTINGS = ("maxmemory", "maxmemory-policy", "appendonly", "save", "hz", "dynamic-hz",
                   "io-threads", "io-threads-do-reads", "activedefrag", "maxclients", "timeout",
                   "tcp-keepalive", "lazyfree-lazy-eviction", "lazyfree-lazy-expire", "maxmemory-samples")


def server_settings(container):
    script = 'for setting in ' + ' '.join(SERVER_SETTINGS) + '; do redis-cli --raw CONFIG GET "$setting"; done'
    raw = command(["docker", "exec", container, "sh", "-c", script]).splitlines()
    require(len(raw) == 2 * len(SERVER_SETTINGS), "Incomplete server configuration probe")
    values = dict(zip(raw[::2], raw[1::2]))
    # Never persist authentication/TLS secrets or unrelated configuration.
    return {key: values[key] for key in SERVER_SETTINGS}


def snapshot(profile, probe):
    require(platform.system() == "Darwin", "This reviewed execution profile requires the authorized macOS host")
    power = command(["pmset", "-g", "batt"])
    thermal = command(["pmset", "-g", "therm"])
    host_state = json.loads(command([str(probe)]))
    server = server_settings(profile["container"])
    allocation = {"logicalProcessors": int(command(["sysctl", "-n", "hw.logicalcpu"])),
                  "memoryBytes": int(command(["sysctl", "-n", "hw.memsize"])),
                  "architecture": platform.machine(),
                  "docker": command(["docker", "info", "--format", "{{.ServerVersion}} {{.NCPU}} {{.MemTotal}} {{.KernelVersion}}"])}
    load = os.getloadavg()
    # The controlled profile defines admissible host conditions; the raw values are retained.
    stable = (profile["powerSource"] in power and host_state["thermalState"] == profile["thermalState"]
              and host_state["lowPowerMode"] == profile["lowPowerMode"] and load[0] <= profile["maximumLoadAverage"]
              and server == profile["serverConfiguration"]
              and all(profile[key] == value for key, value in allocation.items()))
    return {"profile": hashlib.sha256(json.dumps(profile, sort_keys=True).encode()).hexdigest(),
            "stable": stable, "allocation": allocation, "serverConfiguration": server, "power": power, "thermal": thermal, "hostState": host_state, "loadAverage": load,
            "capturedAt": datetime.datetime.now(datetime.timezone.utc).isoformat()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, type=Path)
    parser.add_argument("--baseline-review", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--baseline-jar", required=True, type=Path)
    parser.add_argument("--candidate-jar", required=True, type=Path)
    parser.add_argument("--java", required=True, type=Path)
    parser.add_argument("--host-probe", required=True, type=Path)
    parser.add_argument("--redis-url", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") != "true", "Benchmark execution in GitHub is prohibited")
    require(args.runs >= 3, "At least three complete runs per version are required")
    output = args.output.resolve(); output.mkdir(parents=True, exist_ok=False)
    profile = json.loads(args.profile.read_text()); review = json.loads(args.baseline_review.read_text())
    profile_hash = hashlib.sha256(json.dumps(profile, sort_keys=True).encode()).hexdigest()
    require(digest(args.host_probe) == profile["hostProbeSha256"], "Host condition probe changed")
    require(profile["thermalState"] in (-1, 0), "A throttled thermal state cannot define the baseline")
    require(profile_hash == review["profile"], "Reviewed profile mismatch")
    require(command([str(args.java), "-version"]) == profile["javaVersion"], "Runtime mismatch")
    require(command(["sysctl", "-n", "machdep.cpu.brand_string"]) == profile["cpu"], "Machine mismatch")
    require(command(["sw_vers"]) == profile["os"], "Operating system mismatch")
    require(command(["docker", "inspect", "--format", "{{.Image}}", profile["container"]]) == profile["serverImageId"], "Server image mismatch")
    require(command(["docker", "inspect", "--format", "{{json .Config.Cmd}}", profile["container"]]) == profile["serverCommand"], "Server configuration mismatch")
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    require(review["reviewedAt"] < started, "Baseline review must precede runs")
    (output / "baseline-review.json").write_bytes(args.baseline_review.read_bytes())
    (output / "profile.json").write_bytes(args.profile.read_bytes())
    identity = json.loads(args.candidate.read_text())
    def ref(name): return {"path": name, "sha256": digest(output / name)}
    bundle = {"schemaVersion": 1, "execution": "local-only", "candidate": identity,
              "baseline": review["baseline"], "profile": profile_hash, "workloadSha256": review["workloadSha256"],
              "baselineReview": ref("baseline-review.json"), "profileData": ref("profile.json"), "startedAt": started, "runs": {"baseline": [], "candidate": []}}
    # Alternate versions; retain all runs even if conditions later invalidate the series.
    for number in range(args.runs):
        for version, jar in (("baseline", args.baseline_jar), ("candidate", args.candidate_jar)):
            stem = f"{number + 1}-{version}"
            before = snapshot(profile, args.host_probe)
            (output / f"{stem}-before.json").write_text(json.dumps(before, indent=2))
            require(before["stable"], "Host conditions are not controlled; no run started")
            require(digest(jar) == review["benchmarkJars"][version], "Benchmark executable changed after review")
            flags = [str(args.java), *profile["jvmArgs"], "-jar", str(jar.resolve()), "CertificationBenchmark",
                     "-f", "1", "-wi", "3", "-i", "5", "-w", "3s", "-r", "3s", "-t", "1",
                     "-foe", "true", "-rf", "json", "-rff", str(output / f"{stem}.json")]
            env = dict(os.environ, QUOTAFLOW_BENCHMARK_REDIS_URL=args.redis_url)
            begin = datetime.datetime.now(datetime.timezone.utc).isoformat()
            with (output / f"{stem}.log").open("w") as log:
                process = subprocess.Popen(flags, stdout=log, stderr=subprocess.STDOUT, env=env)
                code = process.wait()
            end = datetime.datetime.now(datetime.timezone.utc).isoformat()
            after = snapshot(profile, args.host_probe)
            (output / f"{stem}-after.json").write_text(json.dumps(after, indent=2))
            require(code == 0, "JMH failed; partial run retained and cannot certify")
            bundle["runs"][version].append({"identity": bundle[version], "profile": profile_hash,
                "workloadSha256": review["workloadSha256"], "pid": process.pid, "startedAt": begin, "endedAt": end,
                "jmh": ref(f"{stem}.json"), "log": ref(f"{stem}.log"),
                "before": ref(f"{stem}-before.json"), "after": ref(f"{stem}-after.json")})
            (output / "manifest.json").write_text(json.dumps(bundle, indent=2) + "\n")
            require(after["stable"], "Run conditions changed; retained evidence is inconclusive")
    print("Performance manifest SHA-256: " + digest(output / "manifest.json"))


if __name__ == "__main__":
    main()
