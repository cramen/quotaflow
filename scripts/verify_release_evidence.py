#!/usr/bin/env python3
"""Validate complete, digest-pinned evidence for one exact release candidate."""
import argparse
import json
from pathlib import Path
import xml.etree.ElementTree as ET
from verify_performance_bundle import artifact, digest, instant, require, verify as verify_performance

REQUIRED = {"runtime-17", "runtime-21", "runtime-25", "core-coverage", "core-mutation", "fallback-mutation",
            "topology-redis", "topology-valkey", "consumers", "native-store", "native-servlet", "native-reactive",
            "soak", "reproducibility", "performance", "negative-controls"}


RELEASE_POLICY = Path(__file__).resolve().parents[1] / 'verification/release-policy.json'


def performance_exception(candidate):
    """Resolve a version-and-binary-bound operator decision from trusted project policy."""
    policy = json.loads(RELEASE_POLICY.read_text())
    exception = policy.get('performanceException')
    require(isinstance(exception, dict), 'No approved performance exception')
    require(candidate['version'] == exception['version']
            and len(candidate['artifacts']) == 7
            and candidate['artifacts'] == exception['artifacts'],
            'Performance exception does not cover these release binaries')
    require(isinstance(exception['reason'], str) and exception['reason'].strip(),
            'Performance exception lacks a reason')
    return {'status': 'WAIVED', 'performanceCertified': False, 'candidate': candidate,
            'policySha256': digest(RELEASE_POLICY), 'reason': exception['reason']}


def junit(paths):
    count, names = 0, set()
    for path in paths:
        root = ET.parse(path).getroot()
        require(root.tag == "testsuite", "Expected JUnit test suite")
        require(all(int(root.get(field, "0")) == 0 for field in ("failures", "errors", "skipped")), "Failed or skipped required scenario")
        for case in root.findall("testcase"):
            require(not any(case.find(tag) is not None for tag in ("failure", "error", "skipped")), "Incomplete scenario")
            count += 1; names.add(case.get("classname", ""))
    require(count > 0, "Empty required scenario set")
    return count, names


def validate_stage(root, kind, stage, candidate, baseline_digest):
    require(stage["kind"] == kind, "Wrong evidence stage kind")
    require(stage["candidate"] == candidate, "Stale stage identity: " + kind)
    require(stage["exitCode"] == 0 and instant(stage["endedAt"]) > instant(stage["startedAt"]), "Failed or unfinished stage: " + kind)
    artifact(root, stage["log"])
    paths = [artifact(root, item) for item in stage["reports"]]
    require(paths, "Missing reports: " + kind)
    if kind.startswith("runtime-") or kind.startswith("topology-"):
        count, names = junit([p for p in paths if p.name.startswith("TEST-") and p.suffix == ".xml"])
        if kind.startswith("runtime-"):
            feature = int(kind.split("-")[1])
            workers = [json.loads(p.read_text()) for p in paths if p.name.startswith("worker-") and p.suffix == ".json"]
            require(workers and all(w["expectedJdk"] == feature and w["actualJdk"] == feature and w["status"] == "PASS" for w in workers), "Missing or mismatched worker runtime")
            for required in ("GeneratedNumericModelTest", "GeneratedRaceModelTckTest", "RecoveryProcessTckTest",
                             "StaggeredRecoveryEnvelopeTckTest", "ObservationIsolationTest"):
                require(any(name.endswith(required) for name in names), "Missing conformance: " + required)
            if feature >= 21:
                phases = {p.stem: json.loads(p.read_text()) for p in paths if p.name.endswith(("-cold.json", "-warm.json"))}
                require(len(phases) == 12, "Missing cold/warm virtual-thread scenarios")
                cold_pids = set()
                for algorithm in ("TOKEN_BUCKET", "GCRA"):
                    for scenario in ("healthy", "degraded", "throttle"):
                        stem = algorithm + "-" + scenario
                        cold, warm = phases[stem + "-cold"], phases[stem + "-warm"]
                        require(cold["pid"] == warm["pid"], "Warmup belongs to another worker")
                        cold_pids.add(cold["pid"])
                        for phase, callers in ((cold, 512), (warm, 100_000)):
                            require(phase["jdk"] == feature and phase["callers"] == callers
                                    and phase["allowed"] + phase["rejected"] == callers, "Incomplete virtual callers")
                            require(phase["libraryPins"] == 0 and phase["unattributedPins"] == 0, "Unacceptable pinning")
                        require(warm["totalPins"] == 0, "Warm pinning")
                require(len(cold_pids) == 6, "Cold scenarios must use fresh workers")
                require(len([p for p in paths if p.suffix == ".jfr"]) >= 12, "Missing cold/warm recordings")
        else:
            require(any(n.endswith("ClusterTopologyTckTest") for n in names) and any(n.endswith("SentinelTopologyTckTest") for n in names), "Missing required topology")
        return {"scenarios": count}
    if kind == "core-coverage":
        report = ET.parse(paths[0]).getroot()
        branches = report.find("counter[@type='BRANCH']")
        require(branches is not None, "Missing branch coverage")
        covered, missed = int(branches.get("covered")), int(branches.get("missed"))
        require(covered + missed > 0 and covered / (covered + missed) >= .90, "Core branch coverage below 90%")
        return {"covered": covered, "missed": missed}
    if kind.endswith("-mutation"):
        mutations = ET.parse(paths[0]).getroot().findall("mutation")
        require(mutations, "Empty mutation report")
        statuses = [m.get("status") for m in mutations]
        require(all(s in {"KILLED", "SURVIVED", "NO_COVERAGE", "TIMED_OUT", "NON_VIABLE"} for s in statuses), "Incomplete mutation analysis")
        detected = sum(s in {"KILLED", "TIMED_OUT", "NON_VIABLE"} for s in statuses)
        require(detected / len(mutations) >= .80, "Independent mutation score below 80%")
        return {"detected": detected, "total": len(mutations)}
    if kind.startswith("native-"):
        report = json.loads(paths[0].read_text())
        require(report["native"] is True and report["exitCode"] == 0 and len(report["executableSha256"]) == 64,
                "Native compilation alone does not certify execution")
        fixture = kind.removeprefix("native-")
        require(report["fixture"] == fixture, "Wrong native workload")
        marker = "native-smoke OK" if fixture == "store" else "NATIVE VERIFIED"
        log = artifact(paths[0].parent, report["runLog"]).read_text()
        require(marker in log and (fixture == "store" or "stack=" + fixture in log), "Native executable did not complete the required stack assertions")
        return report
    if kind == "soak":
        report = json.loads(paths[0].read_text())
        require(report["status"] == "PASS" and report["durationSeconds"] >= 3600 and report["elapsedNanos"] >= 3_600_000_000_000,
                "Short or incomplete soak")
        require(report["owners"] >= 2 and report["unexpectedExceptions"] == 0, "Invalid fleet soak")
        for field in ("TOKEN_BUCKETAllowed", "GCRAAllowed", "rejected", "cancelled", "reloads", "fallbackDecisions", "transitions"):
            require(report[field] > 0, "Missing soak traffic: " + field)
        require(report["outages"] >= 2 and report["outages"] == report["recoveries"] and report["maximumRecoveryMillis"] <= 15_000,
                "Missing or unbounded soak recovery")
        return report
    if kind == "reproducibility":
        report = json.loads(paths[0].read_text())
        require(report["status"] == "PASS" and len(report["builds"]) == 2 and report["builds"][0] == report["builds"][1], "Non-reproducible core")
        version = candidate["version"]
        binary = f"quotaflow-core-{version}.jar"
        require(report["version"] == version and set(report["builds"][0]) == {binary, f"quotaflow-core-{version}-sources.jar", "pom-default.xml"},
                "Missing candidate binary/source/POM comparison")
        require(report["builds"][0][binary] == candidate["artifacts"][binary], "Reproducibility evidence belongs to another binary")
        return report
    if kind == "performance":
        manifest = paths[0]
        value = json.loads(manifest.read_text())
        if value.get('status') == 'WAIVED':
            approved = performance_exception(candidate)
            require(value == approved, 'Performance exception report differs from approved policy or candidate')
            return approved
        report = verify_performance(manifest.parent, digest(manifest), candidate, baseline_digest)
        require(report["comparison"] == "PASS", "Performance is not certified: " + report["comparison"])
        return report
    if kind == "consumers":
        report = json.loads(paths[0].read_text())
        expected = {(boot, jdk, tool, stack) for boot in ("4.1.1", "4.0.8", "3.5.16") for jdk in (17, 21, 25)
                    for tool in ("maven", "gradle") for stack in ("servlet", "reactive")}
        actual = set()
        for run in report:
            require(run["status"] == "PASS", "Failed external consumer")
            key = (run["boot"], run["jdk"], run["tool"], run["stack"])
            require(key not in actual, "Duplicate consumer result"); actual.add(key)
            log = (paths[0].parent / run["log"]).resolve()
            require(log in paths, "Unverified consumer log")
            require(f"CONSUMER VERIFIED boot={run['boot']} stack={run['stack']} jdk={run['jdk']}" in log.read_text()
                    and "redisVerified=true" in log.read_text(), "Incomplete consumer transport assertions")
        require(actual == expected, "Incomplete consumer matrix")
        return {"combinations": len(actual)}
    if kind == "negative-controls":
        report = json.loads(paths[0].read_text())
        require(report["status"] == "PASS" and set(report["suites"]) == {"build", "thresholds", "performance", "release"}, "Missing gate negative controls")
        return report
    raise ValueError("Unknown required stage: " + kind)


def verify(root, pinned_digest, candidate, baseline_digest):
    root = Path(root)
    require(digest(root / "manifest.json") == pinned_digest, "Release manifest digest mismatch")
    manifest = json.loads((root / "manifest.json").read_text())
    require(manifest["schemaVersion"] == 1 and manifest["candidate"] == candidate, "Wrong release candidate")
    require(set(manifest["stages"]) == REQUIRED, "Incomplete release evidence")
    result = {}
    for kind, reference in manifest["stages"].items():
        stage = json.loads(artifact(root, reference).read_text())
        result[kind] = validate_stage(root, kind, stage, candidate, baseline_digest)
    return {"status": "PASS", "candidate": candidate, "manifestSha256": pinned_digest, "stages": result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline-review-sha256", required=True)
    args = parser.parse_args()
    try:
        result = verify(args.bundle, args.sha256, json.loads(args.candidate.read_text()), args.baseline_review_sha256)
        print(json.dumps(result, indent=2)); return 0
    except (ValueError, KeyError, TypeError, OSError, ET.ParseError) as failure:
        print(json.dumps({"status": "FAILED", "error": str(failure)})); return 1


if __name__ == "__main__":
    raise SystemExit(main())
