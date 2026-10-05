#!/usr/bin/env python3
"""Fail-blocking production SBOM scanning and exact, expiring reachability review."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

from release_common import require, safe_artifact, sha256
from release_tools import ROOT, tool


def instant(value):
    require(isinstance(value, str), "Missing security evidence timestamp")
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "Security evidence timestamps require a timezone")
    return parsed.astimezone(dt.timezone.utc)


def artifact_set(candidate):
    return hashlib.sha256(json.dumps(candidate["artifacts"], sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def evaluate(raw, database, candidate, reviews, policy, now=None, evidence_root=ROOT):
    now = now or dt.datetime.now(dt.timezone.utc)
    require(database.get("valid") is True, "Vulnerability database is unavailable or invalid")
    age = now - instant(database["built"])
    require(-dt.timedelta(minutes=5) <= age <= dt.timedelta(hours=policy["maximumDatabaseAgeHours"]), "Stale or future vulnerability database")
    descriptor = raw["descriptor"]
    require(descriptor["name"] == "grype", "Unexpected vulnerability scanner")
    report_age = now - instant(descriptor["timestamp"])
    require(-dt.timedelta(minutes=5) <= report_age <= dt.timedelta(hours=policy["maximumReportAgeHours"]), "Stale or future vulnerability report")
    require(descriptor["db"]["status"]["built"] == database["built"] and
            descriptor["db"]["status"]["schemaVersion"] == database["schemaVersion"], "Scanner database identity mismatch")
    require(isinstance(raw.get("matches"), list), "Missing vulnerability findings")
    require(not raw.get("ignoredMatches"), "Suppressed scanner findings are not accepted")
    config = descriptor["configuration"]
    require(not config.get("only-fixed") and not config.get("only-notfixed") and not config.get("ignore-wontfix"),
            "Scanner filtering cannot suppress unresolved vulnerabilities")
    indexed = {}
    for review in reviews:
        require(re.fullmatch(r"(?:GHSA-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}|CVE-[0-9]{4}-[0-9]{4,})", review["advisory"]), "Invalid advisory review identity")
        require(re.fullmatch(r"pkg:maven/[^/@*]+/[^/@*]+@[^/@*]+", review["purl"]), "Reachability review requires an exact package and version")
        key = review["advisory"], review["purl"]
        require(key not in indexed, "Duplicate reachability review")
        indexed[key] = review
    findings = []
    for match in raw["matches"]:
        vulnerability, package = match["vulnerability"], match["artifact"]
        key = vulnerability["id"], package.get("purl")
        require(key[1], "Vulnerability finding lacks an exact package identity")
        review = indexed.get(key)
        accepted = False
        if review is not None:
            require(review["candidateArtifactsSha256"] == artifact_set(candidate), "Reachability review belongs to different library binaries")
            reviewed, expires = instant(review["reviewedAt"]), instant(review["expiresAt"])
            require(reviewed <= now < expires and expires - reviewed <= dt.timedelta(days=policy["maximumTriageDays"]), "Expired or excessively long reachability review")
            require(review.get("owner", "").strip() and len(review.get("rationale", "").strip()) >= 30 and review.get("evidence"), "Incomplete reachability rationale or evidence")
            for reference in review["evidence"]:
                safe_artifact(evidence_root, reference)
            accepted = review["decision"] == "not_reachable"
        findings.append({"advisory": key[0], "purl": key[1], "severity": vulnerability.get("severity", "Unknown"),
                         "disposition": "reviewed_not_reachable" if accepted else "BLOCKED"})
    return {"status": "BLOCKED" if any(f["disposition"] == "BLOCKED" for f in findings) else "PASS", "findings": findings}


def scan(candidate_root, output, cache, tools_directory, refresh=False):
    candidate_root = Path(candidate_root).resolve(); output = Path(output).resolve(); cache = Path(cache).resolve()
    manifest_path = candidate_root / "candidate-manifest.json"
    manifest = json.loads(manifest_path.read_text()); sbom = safe_artifact(candidate_root, manifest["sbom"])
    candidate = manifest["candidate"]
    output.mkdir(parents=True, exist_ok=False); cache.mkdir(parents=True, exist_ok=True)
    scanner, scanner_identity = tool("grype", tools_directory)
    policy = json.loads((ROOT / "verification/release-policy.json").read_text())["vulnerabilities"]
    config = output / "grype-config.json"
    config.write_text(json.dumps({"check-for-app-update": False, "only-fixed": False, "only-notfixed": False,
        "db": {"cache-dir": str(cache), "auto-update": False, "validate-age": True,
               "max-allowed-built-age": str(policy["maximumDatabaseAgeHours"]) + "h"}, "ignore": []}, indent=2))
    environment = {k:v for k,v in os.environ.items() if not k.startswith("GRYPE_")}
    if refresh:
        with (output / "database-update.log").open("w") as log:
            result = subprocess.run([str(scanner), "-c", str(config), "db", "update"], stdout=log, stderr=subprocess.STDOUT, env=environment)
        require(result.returncode == 0, "Vulnerability database refresh failed")
    status = subprocess.run([str(scanner), "-c", str(config), "db", "status", "-o", "json"], capture_output=True, text=True, env=environment)
    require(status.returncode == 0, "Vulnerability database is unavailable")
    database = json.loads(status.stdout); (output / "database-status.json").write_text(json.dumps(database, indent=2) + "\n")
    database_path = Path(database["path"]).resolve()
    require(database_path.is_relative_to(cache) and database_path.is_file(), "Unexpected vulnerability database path")
    before = sha256(database_path)
    started = dt.datetime.now(dt.timezone.utc)
    with (output / "scan.log").open("w") as log:
        result = subprocess.run([str(scanner), "-c", str(config), "sbom:" + str(sbom), "-o", "json", "--file", str(output / "raw-scan.json")],
                                stdout=log, stderr=subprocess.STDOUT, env=environment)
    require(result.returncode == 0, "Vulnerability scan failed; absence is not an empty finding set")
    require(sha256(database_path) == before, "Vulnerability database changed during scanning")
    require(sha256(sbom) == manifest["sbom"]["sha256"], "SBOM changed during scanning")
    raw = json.loads((output / "raw-scan.json").read_text())
    require(raw["descriptor"]["version"] == scanner_identity["version"], "Scanner version mismatch")
    require(raw["source"]["type"] == "sbom-file" and Path(raw["source"]["target"]).resolve() == sbom, "Scanner analyzed another input")
    require(instant(raw["descriptor"]["timestamp"]) >= started - dt.timedelta(minutes=5), "Reused scanner report")
    reviews_path = ROOT / "verification/reachability.json"
    reviews = json.loads(reviews_path.read_text())
    require(reviews["schemaVersion"] == 1, "Unsupported reachability policy")
    verdict = evaluate(raw, database, candidate, reviews["reviews"], policy)
    shutil.copyfile(database_path, output / "vulnerability.db")
    shutil.copyfile(reviews_path, output / "reachability.json")
    report = {"schemaVersion": 1, "candidate": candidate, "candidateManifestSha256": sha256(manifest_path),
              "sbomSha256": manifest["sbom"]["sha256"], "scanner": scanner_identity,
              "createdAt": dt.datetime.now(dt.timezone.utc).isoformat(), "databaseBuiltAt": database["built"],
              "database": {"path": "vulnerability.db", "sha256": before},
              "rawReport": {"path": "raw-scan.json", "sha256": sha256(output / "raw-scan.json")},
              "triage": {"path": "reachability.json", "sha256": sha256(output / "reachability.json")}, **verdict}
    (output / "security.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def verify_security(directory, candidate, sbom_sha256, now=None, evidence_root=ROOT):
    directory = Path(directory)
    report = json.loads((directory / "security.json").read_text())
    require(report["schemaVersion"] == 1 and report["candidate"] == candidate and report["sbomSha256"] == sbom_sha256,
            "Security evidence belongs to another candidate")
    configured = json.loads((ROOT / "verification/release-tooling.json").read_text())["tools"]["grype"]
    scanner = report["scanner"]
    require(scanner["version"] == configured["version"] and
            scanner["downloadSha256"] == configured["platforms"][scanner["platform"]]["sha256"], "Unpinned security scanner")
    safe_artifact(directory, report["database"])
    raw = json.loads(safe_artifact(directory, report["rawReport"]).read_text())
    reviews = json.loads(safe_artifact(directory, report["triage"]).read_text())["reviews"]
    policy = json.loads((ROOT / "verification/release-policy.json").read_text())["vulnerabilities"]
    now = now or dt.datetime.now(dt.timezone.utc)
    require(-dt.timedelta(minutes=5) <= now - instant(report["createdAt"]) <= dt.timedelta(hours=policy["maximumReportAgeHours"]), "Expired security evidence")
    require(raw["descriptor"]["version"] == scanner["version"], "Scanner report version mismatch")
    verdict = evaluate(raw, raw["descriptor"]["db"]["status"], candidate, reviews, policy, now, evidence_root)
    require(verdict["status"] == report["status"] == "PASS", "Reachable or unreviewed vulnerability blocks promotion")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True); parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--database-cache", type=Path, default=ROOT / "build/release-security-db")
    parser.add_argument("--tools", type=Path, default=ROOT / "build/release-tools"); parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    try:
        report = scan(args.candidate, args.output, args.database_cache, args.tools, args.refresh)
        print(json.dumps({"status": report["status"], "findings": report["findings"]}, indent=2))
        return 0 if report["status"] == "PASS" else 1
    except (ValueError, KeyError, OSError, TypeError) as error:
        print(json.dumps({"status": "FAILED", "error": str(error)})); return 1


if __name__ == "__main__":
    raise SystemExit(main())
