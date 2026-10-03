#!/usr/bin/env python3
"""Execute every release-gate negative control and retain individual command logs."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    suites = {"build": ["test_build_verification.py"], "thresholds": ["test_quality_thresholds.py"],
              "performance": ["test_performance_bundle.py"],
              "release": ["test_release_evidence.py", "test_evidence_collection.py"]}
    results = []
    for suite, scripts in suites.items():
        for script in scripts:
            with (args.output / (script + ".log")).open("w") as log:
                result = subprocess.run([sys.executable, str(ROOT / "scripts" / script)], cwd=ROOT,
                                        stdout=log, stderr=subprocess.STDOUT)
            results.append({"suite": suite, "script": script, "exitCode": result.returncode})
    report = {"status": "PASS" if all(item["exitCode"] == 0 for item in results) else "FAILED",
              "suites": list(suites), "invocations": results}
    (args.output / "controls.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
