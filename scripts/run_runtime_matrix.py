"""Run JVM verification sequentially: topology fixtures use shared host ports."""
import argparse
from pathlib import Path
import shutil
import subprocess
from candidate_identity import source_identity

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdks", nargs="+", type=int, choices=[17, 21, 25], default=[17, 21, 25])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    expected_source = source_identity(ROOT)[1]
    for jdk in args.jdks:
        # A JDK 17 build has no virtualTest task; old reports from a different
        # runtime must not be copied into its evidence directory.
        for module in ROOT.glob("quotaflow-*"):
            for relative in ("build/test-results", "build/reports/runtime", "build/reports/generated"):
                shutil.rmtree(module / relative, ignore_errors=True)
        with (output / f"jdk{jdk}.log").open("w") as log:
            result = subprocess.run([str(ROOT / "gradlew"), "build", "dependencyAudit",
                                     ":quotaflow-core:jacocoTestReport", "--rerun-tasks", f"-PtestJdk={jdk}"],
                                    cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        destination = output / f"jdk{jdk}"
        for module in ROOT.glob("quotaflow-*"):
            for relative in ["build/test-results", f"build/reports/runtime/test-jdk{jdk}",
                             f"build/reports/runtime/virtualTest-jdk{jdk}", f"build/reports/runtime/valkeyTopologyTest-jdk{jdk}", "build/reports/jacoco",
                             f"build/reports/virtual-threads/jdk{jdk}", "build/reports/generated"]:
                source = module / relative
                if source.exists():
                    shutil.copytree(source, destination / module.name / relative, dirs_exist_ok=True)
        if source_identity(ROOT)[1] != expected_source:
            raise RuntimeError("Source changed during runtime matrix verification; retained reports are diagnostic only")
        print(f"JDK {jdk}: exit {result.returncode}", flush=True)
        if result.returncode:
            raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
