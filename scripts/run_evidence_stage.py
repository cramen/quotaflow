#!/usr/bin/env python3
"""Capture a gate invocation and fresh reports against an immutable candidate identity."""
import argparse
import datetime
import glob
import json
import os
from pathlib import Path
import shutil
import subprocess
from candidate_identity import source_identity
from verify_performance_bundle import digest, require
from verify_release_evidence import REQUIRED

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--kind", choices=sorted(REQUIRED), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", action="append", required=True, help="Repository-relative glob for generated report files")
    parser.add_argument("--reset", action="append", default=[], help="Generated build report directory to clear before execution")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    candidate = json.loads(args.candidate.read_text())
    require(source_identity(ROOT)[1] == candidate["sourceSha256"], "Source changed after candidate capture")
    require(subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip() == candidate["commit"], "Candidate commit mismatch")
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    require(command, "Missing gate command")
    output = args.output.resolve(); output.mkdir(parents=True, exist_ok=False)
    for name in args.reset:
        path = (ROOT / name).resolve()
        require(path.is_relative_to(ROOT) and "build" in path.relative_to(ROOT).parts and not path.is_symlink(), "Only generated build directories may be reset")
        if path.is_dir(): shutil.rmtree(path)
        elif path.exists(): path.unlink()
    started = datetime.datetime.now(datetime.timezone.utc)
    with (output / "run.log").open("w") as log:
        visible, hide_next = [], False
        for argument in command:
            sensitive = any(word in argument.lower() for word in ("password", "secret", "token", "redis-url"))
            if hide_next or "://" in argument:
                visible.append("<redacted>")
            elif sensitive and "=" in argument:
                visible.append(argument.split("=", 1)[0] + "=<redacted>")
            else:
                visible.append(argument)
            hide_next = sensitive and "=" not in argument
        log.write("Command: " + json.dumps(visible) + "\n"); log.flush()
        result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    ended = datetime.datetime.now(datetime.timezone.utc)
    references = []
    for pattern in args.report:
        for name in sorted(glob.glob(str(ROOT / pattern), recursive=True)):
            path = Path(name)
            if not path.is_file(): continue
            require(not path.is_symlink() and path.resolve().is_relative_to(ROOT), "Report escaped repository")
            require(path.stat().st_mtime >= started.timestamp(), "Stale report: " + str(path))
            relative = Path("reports") / path.relative_to(ROOT)
            destination = output / relative; destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
            references.append({"path": relative.as_posix(), "sha256": digest(destination)})
    require(references, "Gate produced no reports")
    require(source_identity(ROOT)[1] == candidate["sourceSha256"], "Source changed during verification")
    stage = {"candidate": candidate, "kind": args.kind, "exitCode": result.returncode,
             "startedAt": started.isoformat(), "endedAt": ended.isoformat(),
             "log": {"path": "run.log", "sha256": digest(output / "run.log")}, "reports": references}
    (output / "stage.json").write_text(json.dumps(stage, indent=2) + "\n")
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
