#!/usr/bin/env python3
"""Verify immutable local comparison evidence; never execute benchmark workloads."""
import argparse
import datetime
import hashlib
import json
import math
import random
from pathlib import Path
import statistics

PATHS = ("local", "distributed-hierarchy", "fallback-facade")
ALGORITHMS = ("TOKEN_BUCKET", "GCRA")
METRICS = {"throughput": "ops/s", "p99": "ms/op"}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def instant(value):
    parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "Evidence timestamps require an explicit timezone")
    return parsed.astimezone(datetime.timezone.utc)


def artifact(root, item):
    require(isinstance(item, dict), "Missing artifact reference")
    name = item.get("path")
    require(isinstance(name, str) and name, "Missing artifact path")
    path = (root / name).resolve()
    require(path.is_relative_to(root.resolve()) and path.is_file(), "Artifact escapes bundle or is missing")
    require(digest(path) == item.get("sha256"), "Artifact digest mismatch: " + name)
    return path


def measurement(root, run):
    artifact(root, run["log"])
    raw = json.loads(artifact(root, run["jmh"]).read_text())
    before = json.loads(artifact(root, run["before"]).read_text())
    after = json.loads(artifact(root, run["after"]).read_text())
    require(before["profile"] == after["profile"] == run["profile"], "Run conditions changed")
    require(before["stable"] is True and after["stable"] is True, "Uncontrolled run conditions")
    require(isinstance(run["pid"], int) and run["pid"] > 0, "Missing independent process identity")
    require(instant(run["endedAt"]) > instant(run["startedAt"]), "Invalid run interval")
    results, configurations = {}, {}
    for entry in raw:
        path, algorithm = entry["params"]["path"], entry["params"]["algorithm"]
        require(path in PATHS and algorithm in ALGORITHMS, "Unexpected benchmark workload")
        mode = entry["mode"]
        require(mode in ("thrpt", "sample"), "Unexpected benchmark mode")
        method = "throughput" if mode == "thrpt" else "latency"
        require(entry["benchmark"] == "io.quotaflow.verification.CertificationBenchmark." + method, "Unexpected benchmark entry point")
        metric = "throughput" if mode == "thrpt" else "p99"
        key = f"{path}/{algorithm}/{metric}"
        require(key not in results, "Duplicate benchmark metric")
        primary = entry["primaryMetric"]
        require(primary["scoreUnit"] == METRICS[metric], "Wrong measurement unit")
        value = primary["score"] if metric == "throughput" else primary["scorePercentiles"]["99.0"]
        require(type(value) in (int, float) and math.isfinite(value) and value > 0, "Invalid measurement")
        require(entry["forks"] >= 1 and entry["measurementIterations"] >= 3 and entry["warmupIterations"] >= 2,
                "Incomplete JMH run")
        fields = ("jmhVersion", "jdkVersion", "vmName", "vmVersion", "jvmArgs", "threads", "forks",
                  "warmupIterations", "warmupTime", "measurementIterations", "measurementTime", "params", "mode")
        configurations[key] = {field: entry[field] for field in fields}
        results[key] = float(value)
    expected = {f"{path}/{algorithm}/{metric}" for path in PATHS for algorithm in ALGORITHMS for metric in METRICS}
    require(results.keys() == expected, "Run does not cover every path, algorithm and metric")
    return results, configurations


def verify(root, expected_digest, candidate, baseline_digest, diagnostic=False):
    root = Path(root)
    manifest = root / "manifest.json"
    require(digest(manifest) == expected_digest, "Bundle manifest digest mismatch")
    bundle = json.loads(manifest.read_text())
    require(bundle["schemaVersion"] == 1 and bundle["execution"] == "local-only", "Unsupported evidence")
    require(bundle["candidate"] == candidate, "Stale or mismatched candidate identity")
    for identity in (candidate, bundle["baseline"]):
        require(set(identity) == {"commit", "sourceSha256", "version", "artifacts"}, "Incomplete candidate identity")
        require(len(identity["commit"]) == 40 and len(identity["sourceSha256"]) == 64, "Invalid source identity")
        require(identity["artifacts"] and all(len(v) == 64 for v in identity["artifacts"].values()), "Missing binary identities")
    profile = json.loads(artifact(root, bundle["profileData"]).read_text())
    require(hashlib.sha256(json.dumps(profile, sort_keys=True).encode()).hexdigest() == bundle["profile"], "Profile contents mismatch")
    review_path = artifact(root, bundle["baselineReview"])
    require(digest(review_path) == baseline_digest, "Unreviewed or self-updated baseline")
    review = json.loads(review_path.read_text())
    require(review["baseline"] == bundle["baseline"] and review["profile"] == bundle["profile"]
            and review["workloadSha256"] == bundle["workloadSha256"], "Baseline/profile/workload mismatch")
    require(review["reviewer"] and review["reason"] and instant(review["reviewedAt"]) < instant(bundle["startedAt"]), "Baseline review must precede measurement")
    groups, configuration, processes, intervals = {}, None, set(), []
    for version in ("baseline", "candidate"):
        runs = bundle["runs"][version]
        require(len(runs) >= 3, "At least three complete runs per version are required")
        groups[version] = []
        for run in runs:
            require(run["profile"] == bundle["profile"] and run["workloadSha256"] == bundle["workloadSha256"], "Run profile/workload mismatch")
            require(run["identity"] == bundle[version], "Mixed binary/source identities")
            process = (run["pid"], run["startedAt"])
            require(process not in processes, "Reused run")
            processes.add(process)
            require(instant(run["startedAt"]) >= instant(bundle["startedAt"]), "Run predates the reviewed collection")
            intervals.append((instant(run["startedAt"]), instant(run["endedAt"])))
            values, settings = measurement(root, run)
            if configuration is None:
                configuration = settings
            require(settings == configuration, "JMH configuration mismatch")
            groups[version].append(values)
    intervals.sort()
    require(all(a[1] <= b[0] for a, b in zip(intervals, intervals[1:])), "Comparative runs overlap")
    findings, verdict = {}, "PASS"
    for key in groups["baseline"][0]:
        old = [run[key] for run in groups["baseline"]]
        new = [run[key] for run in groups["candidate"]]
        # Resample whole independent runs, never individual correlated JMH samples.
        # With three runs this remains close to the full observed range; additional
        # retained runs can resolve uncertainty without discarding unfavorable data.
        randomizer = random.Random(731902)
        ratios = []
        for _ in range(10_000):
            baseline_median = statistics.median(randomizer.choices(old, k=len(old)))
            candidate_median = statistics.median(randomizer.choices(new, k=len(new)))
            ratios.append(1 - candidate_median / baseline_median if key.endswith("/throughput")
                          else candidate_median / baseline_median - 1)
        ratios.sort()
        best, worst = ratios[249], ratios[9749]
        status = "REGRESSION" if best > .10 + 1e-12 else "PASS" if worst <= .10 + 1e-12 else "INCONCLUSIVE"
        if status == "REGRESSION" or (status == "INCONCLUSIVE" and verdict == "PASS"):
            verdict = status
        findings[key] = {"baseline": old, "candidate": new, "bestRegression": best,
                         "worstRegression": worst, "status": status,
                         "method": "95% percentile bootstrap of independent-run medians; 10000 resamples",
                         "baselineSpread": max(old) - min(old), "candidateSpread": max(new) - min(new)}
    if diagnostic:
        verdict = "NOT ASSESSED"
    return {"schemaVersion": 1, "candidate": candidate, "manifestSha256": expected_digest,
            "comparison": verdict, "metrics": findings}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--baseline-review-sha256", required=True)
    parser.add_argument("--diagnostic", action="store_true")
    args = parser.parse_args()
    try:
        report = verify(args.bundle, args.sha256, json.loads(args.candidate.read_text()),
                        args.baseline_review_sha256, args.diagnostic)
        print(json.dumps(report, indent=2))
        return 0 if report["comparison"] == "PASS" or args.diagnostic else 1
    except (ValueError, KeyError, TypeError, OSError) as error:
        print(json.dumps({"comparison": "FAILED", "error": str(error)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
