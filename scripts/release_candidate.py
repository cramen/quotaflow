#!/usr/bin/env python3
"""Seal prepared release bytes into a canonical, independently verifiable package.

Assembly never builds or publishes. A sealed package is not promotion approval;
production must independently verify its signatures and all referenced evidence.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import tempfile
import zipfile

from prepare_release import inspect_repository, production_sbom
from release_common import MODULES, require, release_version, safe_artifact, sha256
from release_promotion import encoded
from release_signatures import CHECKSUMS, fingerprint, verify_pgp
from release_tools import ROOT


def reference(root, path):
    path = Path(path)
    return {"path": path.relative_to(root).as_posix(), "sha256": sha256(path), "size": path.stat().st_size}


def indexed(root, entries):
    require(isinstance(entries, list) and entries, "Missing file inventory")
    result = {}
    for entry in entries:
        require(entry["path"] not in result, "Duplicate manifest path")
        path = safe_artifact(root, entry)
        require(entry.get("size", path.stat().st_size) == path.stat().st_size, "File size mismatch")
        result[entry["path"]] = entry
    return result


def regular_files(root):
    require(root.is_dir() and not root.is_symlink(), "Expected a regular directory")
    entries = sorted(root.rglob("*"))
    require(all(not p.is_symlink() and (p.is_file() or p.is_dir()) for p in entries), "Nonregular package input")
    return [p for p in entries if p.is_file()]


def verify_prepared(root, manifest, sealed=False):
    root = Path(root)
    require(manifest["schemaVersion"] == 1 and manifest["state"] == "PREPARED_UNSIGNED"
            and manifest["mode"] in {"dry-run", "release"}, "Unsupported prepared candidate")
    version = release_version(manifest["candidate"]["version"])
    require(re.fullmatch(r"[0-9a-f]{40}", manifest["candidate"]["commit"])
            and re.fullmatch(r"[0-9a-f]{64}", manifest["candidate"]["sourceSha256"]), "Invalid source identity")
    require(len(manifest["modules"]) == len(MODULES) and set(manifest["modules"]) == set(MODULES), "Wrong module inventory")
    require(manifest["mode"] != "release" or manifest["tag"] == "v" + version, "Release tag/version mismatch")
    assets = indexed(root, manifest["assets"])
    require(all(name.startswith("repository/io/github/cramen/") and ".asc" not in Path(name).suffixes for name in assets),
            "Prepared inventory must contain unsigned Maven assets only")
    graph = json.loads(safe_artifact(root, manifest["productionGraph"]).read_text())
    binaries, actual = inspect_repository(root / "repository", version, graph)
    require(binaries == manifest["candidate"]["artifacts"], "Prepared binaries differ from candidate identity")
    actual = {"repository/" + item["path"]: item for item in actual}
    # Signing adds sidecars after preparation; no other bytes may appear silently.
    sidecars = {name + ".asc" for name in assets if Path(name).suffix not in CHECKSUMS}
    if sealed:
        sidecars |= {name + suffix for name in set(assets) | sidecars
                     if Path(name).suffix not in CHECKSUMS for suffix in (".sha256", ".sha512")}
    require(set(actual) - set(assets) <= sidecars and set(assets) <= set(actual), "Unexpected staged artifact inventory")
    for name, entry in assets.items():
        require(actual[name]["sha256"] == entry["sha256"], "Staged artifact changed")
    sbom_path = safe_artifact(root, manifest["sbom"])
    sbom = json.loads(sbom_path.read_text()); expected = production_sbom(graph, version, binaries)
    expected["metadata"]["timestamp"] = sbom["metadata"]["timestamp"]
    require(sbom == expected, "SBOM differs from resolved production graph or binary hashes")
    validation = json.loads(safe_artifact(root, manifest["sbomValidation"]).read_text())
    require(validation["status"] == "PASS" and validation["schema"] == "CycloneDX 1.6"
            and validation["sbomSha256"] == sha256(sbom_path)
            and validation["validator"] == "org.cyclonedx:cyclonedx-core-java:13.1.0", "Missing exact SBOM schema validation")
    return assets


def write_archive(path, root, entries):
    with zipfile.ZipFile(path, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in sorted(entries):
            source = safe_artifact(root, entries[name])
            info = zipfile.ZipInfo(name.removeprefix("repository/"), (1980, 1, 1, 0, 0, 0))
            info.create_system = 3; info.external_attr = 0o100644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, source.read_bytes())


def verify_archive(path, entries):
    expected = {name.removeprefix("repository/"): item for name, item in entries.items()}
    require(len(expected) == len(entries), "Maven archive path collision")
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        require(len(names) == len(set(names)) and set(names) == set(expected), "Maven bundle file inventory mismatch")
        for name in names:
            require(hashlib.sha256(archive.read(name)).hexdigest() == expected[name]["sha256"], "Maven bundle bytes differ")


def assemble(prepared, output, reports=None, rehearsal=False):
    prepared, output = Path(prepared).resolve(), Path(output).absolute()
    require(not output.exists() and not output.is_symlink(), "Refusing to replace an existing sealed candidate")
    require(not output.is_relative_to(prepared), "Output cannot be inside the prepared candidate")
    manifest_path = prepared / "candidate-manifest.json"
    require(not manifest_path.is_symlink(), "Symlink preparation manifest refused")
    original = manifest_path.read_bytes(); manifest = json.loads(original)
    base_assets = verify_prepared(prepared, manifest)
    policy = json.loads((ROOT / "verification/release-policy.json").read_text())
    pgp_path = prepared / "signatures/pgp/pgp.json"
    pgp = json.loads(pgp_path.read_text()) if pgp_path.exists() else None
    if not rehearsal:
        require(manifest["mode"] == "release", "Dry-run preparation cannot become a production package")
        expected = fingerprint(policy["pgp"]["fingerprint"])
        require(pgp is not None, "Missing production PGP signatures")
        verify_pgp(prepared, pgp, manifest, expected)
    elif pgp is not None:
        verify_pgp(prepared, pgp, manifest, pgp["fingerprint"], production=False)
    reports = reports or {}
    require(set(reports) <= {"quality", "security", "consumers", "examples"}, "Unknown release report role")
    if not rehearsal: require(set(reports) == {"quality", "security", "consumers", "examples"}, "Incomplete production report set")
    sources = dict(base_assets)
    for key in ("sbom", "sbomValidation", "productionGraph"):
        entry = manifest[key]; safe_artifact(prepared, entry)
        require(entry["path"] not in sources, "Preparation metadata collision")
        sources[entry["path"]] = entry
    if pgp is not None:
        for entry in [pgp["publicKey"], *[s["signature"] for s in pgp["signedAssets"]]]:
            require(entry["path"] not in sources, "Signature metadata collision")
            safe_artifact(prepared, entry); sources[entry["path"]] = entry
        sources["signatures/pgp/pgp.json"] = reference(prepared, pgp_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    # A failed assembly leaves no apparently complete output directory.
    with tempfile.TemporaryDirectory(prefix=".quotaflow-seal-", dir=output.parent) as temporary:
        work = Path(temporary) / "candidate"; work.mkdir()
        for name, entry in sources.items():
            destination = work / name; destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(safe_artifact(prepared, entry), destination)
            safe_artifact(work, entry)
        (work / "candidate-manifest.json").write_bytes(original)
        report_roots = {}
        for role, directory in sorted(reports.items()):
            directory = Path(directory).resolve(); files = regular_files(directory)
            require(files, "Empty report directory: " + role)
            for source in files:
                entry = reference(directory, source)
                target = work / "reports" / role / entry["path"]; target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(safe_artifact(directory, entry), target)
                require(sha256(target) == entry["sha256"], "Evidence changed while copying")
            report_roots[role] = "reports/" + role
        maven = {p.relative_to(work).as_posix(): reference(work, p) for p in regular_files(work / "repository")}
        # Central checksum sidecars cover signatures as well as the original assets.
        for name in list(maven):
            if Path(name).suffix in CHECKSUMS: continue
            for algorithm in ("sha256", "sha512"):
                checksum = work / (name + "." + algorithm)
                value = hashlib.new(algorithm, (work / name).read_bytes()).hexdigest()
                if checksum.exists(): require(checksum.read_text().strip() == value, "Conflicting checksum")
                else: checksum.write_text(value)
                maven[checksum.relative_to(work).as_posix()] = reference(work, checksum)
        version = manifest["candidate"]["version"]
        bundle = work / ("quotaflow-" + version + "-maven.zip")
        write_archive(bundle, work, maven); verify_archive(bundle, maven)
        sealed = {"schemaVersion": 1, "state": "SEALED_UNVERIFIED", "mode": "rehearsal" if rehearsal else "release",
                  "repository": "cramen/quotaflow", "eligibleForPromotion": False, "candidate": manifest["candidate"], "modules": list(MODULES),
                  "preparation": reference(work, work / "candidate-manifest.json"), "mavenBundle": reference(work, bundle),
                  "sbom": manifest["sbom"], "reportRoots": report_roots,
                  "pgp": reference(work, work / "signatures/pgp/pgp.json") if pgp else None,
                  "mavenAssets": [maven[name] for name in sorted(maven)],
                  "files": [reference(work, p) for p in regular_files(work)]}
        (work / "release-manifest.json").write_bytes(encoded(sealed))
        verify_sealed(work, sha256(work / "release-manifest.json"))
        require(manifest_path.read_bytes() == original, "Preparation changed during sealing")
        for entry in sources.values(): safe_artifact(prepared, entry)
        require(not output.exists(), "Output appeared during assembly")
        work.rename(output)
    return sealed


def verify_sealed(root, expected_digest):
    root = Path(root); path = root / "release-manifest.json"
    require(not path.is_symlink() and sha256(path) == expected_digest, "Release manifest digest mismatch")
    manifest = json.loads(path.read_bytes())
    require(path.read_bytes() == encoded(manifest), "Noncanonical release manifest")
    require(manifest["schemaVersion"] == 1 and manifest["state"] == "SEALED_UNVERIFIED"
            and manifest["mode"] in {"release", "rehearsal"} and manifest["eligibleForPromotion"] is False,
            "Invalid sealed candidate state")
    files = indexed(root, manifest["files"])
    actual = {p.relative_to(root).as_posix() for p in regular_files(root)}
    # A detached Sigstore bundle is added later and cannot be in its own signed manifest.
    require(actual - set(files) - {"release-manifest.json", "release-manifest.sigstore.json"} == set()
            and set(files) <= actual, "Sealed file inventory mismatch")
    preparation = json.loads(safe_artifact(root, manifest["preparation"]).read_text())
    verify_prepared(root, preparation, sealed=True)
    require(manifest["mode"] != "release" or preparation["mode"] == "release",
            "Rehearsal preparation cannot be relabeled as production")
    require(manifest["candidate"] == preparation["candidate"] and manifest["modules"] == preparation["modules"]
            and manifest["sbom"] == preparation["sbom"], "Sealed candidate identity mismatch")
    maven = indexed(root, manifest["mavenAssets"])
    require({p.relative_to(root).as_posix() for p in regular_files(root / "repository")} == set(maven), "Unlisted Maven asset")
    require(all(name in files and files[name]["sha256"] == ref["sha256"] for name, ref in maven.items()), "Maven inventory mismatch")
    verify_archive(safe_artifact(root, manifest["mavenBundle"]), maven)
    for key in ("preparation", "sbom", "mavenBundle"):
        ref = manifest[key]; require(ref["path"] in files and files[ref["path"]]["sha256"] == ref["sha256"], "Unbound metadata")
    if manifest["pgp"] is not None:
        ref = manifest["pgp"]
        require(ref["path"] == "signatures/pgp/pgp.json" and ref["path"] in files
                and files[ref["path"]]["sha256"] == ref["sha256"], "Unbound PGP report")
    for role, directory in manifest["reportRoots"].items():
        require(role in {"quality", "security", "consumers", "examples"} and directory == "reports/" + role,
                "Invalid evidence path")
        require(any(name.startswith(directory + "/") for name in files), "Empty evidence inventory")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="operation", required=True)
    make = sub.add_parser("assemble"); make.add_argument("--prepared", type=Path, required=True)
    make.add_argument("--output", type=Path, required=True); make.add_argument("--rehearsal", action="store_true")
    make.add_argument("--report", action="append", default=[], metavar="ROLE=DIRECTORY")
    check = sub.add_parser("verify"); check.add_argument("--candidate", type=Path, required=True); check.add_argument("--sha256", required=True)
    args = parser.parse_args()
    try:
        if args.operation == "assemble":
            pairs = [value.split("=", 1) for value in args.report]
            require(all(len(pair) == 2 for pair in pairs) and len({p[0] for p in pairs}) == len(pairs), "Invalid or duplicate report role")
            manifest = assemble(args.prepared, args.output, dict(pairs), args.rehearsal)
            digest = sha256(args.output / "release-manifest.json")
        else:
            manifest = verify_sealed(args.candidate, args.sha256); digest = args.sha256
        print(json.dumps({"status": "PASS", "mode": manifest["mode"], "productionReadiness": "UNVERIFIED",
                          "manifestSha256": digest, "mavenAssets": len(manifest["mavenAssets"])})); return 0
    except (ValueError, KeyError, TypeError, OSError, zipfile.BadZipFile) as error:
        print(json.dumps({"status": "FAILED", "error": str(error)})); return 1


if __name__ == "__main__": raise SystemExit(main())
