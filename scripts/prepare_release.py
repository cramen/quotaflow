#!/usr/bin/env python3
"""Prepare an inspectable unsigned release candidate without external publication."""
import argparse
import datetime
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import xml.etree.ElementTree as ET
import zipfile

from candidate_identity import source_identity
from release_common import GROUP, MODULES, credential_environment, git, purl, release_version, require, sha256, tag_identity

ROOT = Path(__file__).resolve().parents[1]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
CHECKSUMS = {"md5", "sha1", "sha256", "sha512"}


def inspect_pom(path, module, version, graph=None):
    data = Path(path).read_bytes()
    require(b"<!DOCTYPE" not in data.upper() and b"<!ENTITY" not in data.upper(), "Unsafe POM XML")
    pom = ET.fromstring(data)
    def value(name):
        return pom.findtext("/".join("m:" + part for part in name.split("/")), namespaces=NS)
    require((value("groupId"), value("artifactId"), value("version")) == (GROUP, module, version), "Wrong POM coordinates")
    for name in ("name", "description", "url", "licenses/license/name", "licenses/license/url",
                 "developers/developer/id", "developers/developer/name", "scm/url", "scm/connection"):
        require(bool((value(name) or "").strip()), "Missing POM metadata: " + name)
    imports = pom.findall("m:dependencyManagement/m:dependencies/m:dependency[m:scope='import']", NS)
    for dep in pom.findall(".//m:dependency", NS):
        group = dep.findtext("m:groupId", namespaces=NS)
        name = dep.findtext("m:artifactId", namespaces=NS)
        dep_version = dep.findtext("m:version", namespaces=NS)
        if dep_version is None:
            require(group != GROUP and imports and graph is not None and any(
                node["group"] == group and node["name"] == name for node in graph["components"].values()),
                "Versionless dependency lacks a resolved production BOM binding")
        else:
            require("snapshot" not in dep_version.lower() and "${" not in dep_version
                    and not dep_version.endswith("+") and not any(c in dep_version for c in "[]()")
                    and dep_version.upper() not in ("LATEST", "RELEASE"),
                    "Unresolved, dynamic or snapshot publication dependency")
        if group == GROUP:
            require(name in MODULES and dep_version == version, "Wrong inter-module publication dependency")


def inspect_jar(path, kind):
    with zipfile.ZipFile(path) as jar:
        names = [n for n in jar.namelist() if not n.endswith("/")]
        require(len(names) == len(set(names)), "Duplicate JAR entry")
        require(all(not n.startswith("/") and ".." not in PurePosixPath(n).parts and "\\" not in n for n in names),
                "Unsafe JAR entry")
        if kind == "binary":
            classes = [n for n in names if n.endswith(".class")]
            require(classes and all(jar.read(n).startswith(b"\xca\xfe\xba\xbe") for n in classes), "Empty or invalid binary JAR")
        elif kind == "sources":
            require(any(n.endswith((".java", ".kt")) and jar.getinfo(n).file_size > 0 for n in names), "Empty source JAR")
        else:
            pages = [n for n in names if n.endswith(".html") and jar.getinfo(n).file_size > 100]
            require(len(pages) >= 2 and "index.html" in pages, "Empty or unusable API documentation JAR")


def inspect_repository(repository, version, graph=None):
    repository = Path(repository)
    namespace = repository / "io/quotaflow"
    require(namespace.is_dir(), "Missing publication namespace")
    require({p.name for p in namespace.iterdir() if p.is_dir()} == set(MODULES), "Expected exactly seven public modules")
    binaries, assets = {}, []
    for module in MODULES:
        parent = namespace / module
        require({p.name for p in parent.iterdir() if p.is_dir()} == {version}, "Mixed candidate versions")
        directory = parent / version
        stem = module + "-" + version
        required = {stem + suffix for suffix in (".pom", ".jar", "-sources.jar", "-javadoc.jar", ".module")}
        require(required <= {p.name for p in directory.iterdir()}, "Incomplete publication: " + module)
        inspect_pom(directory / (stem + ".pom"), module, version, graph)
        for suffix, kind in ((".jar", "binary"), ("-sources.jar", "sources"), ("-javadoc.jar", "docs")):
            inspect_jar(directory / (stem + suffix), kind)
        metadata = json.loads((directory / (stem + ".module")).read_text())
        require(tuple(metadata["component"].get(k) for k in ("group", "module", "version")) == (GROUP, module, version),
                "Wrong Gradle publication coordinates")
        for variant in metadata["variants"]:
            for file in variant.get("files", []):
                require(file["url"] == Path(file["url"]).name and (directory / file["url"]).is_file(), "Missing Gradle variant artifact")
                if "sha256" in file:
                    require(sha256(directory / file["url"]) == file["sha256"], "Gradle metadata artifact digest mismatch")
            for dep in variant.get("dependencies", []) + variant.get("dependencyConstraints", []):
                selected = dep.get("version", {}).get("requires", dep.get("version", {}).get("strictly", ""))
                require("snapshot" not in selected.lower(), "Snapshot Gradle dependency")
                if dep.get("group") == GROUP:
                    require(dep["module"] in MODULES and selected == version, "Wrong Gradle inter-module version")
        for path in sorted(directory.iterdir()):
            require(path.is_file() and not path.is_symlink() and re.fullmatch(
                re.escape(stem) + r"(?:-[A-Za-z0-9._-]+)?\.(?:jar|pom|module)(?:\.asc)?(?:\.(?:md5|sha1|sha256|sha512))?",
                path.name), "Unexpected Maven asset")
            require(path.stat().st_size > 0, "Empty Maven asset")
            if path.suffix.removeprefix(".") in CHECKSUMS:
                import hashlib
                original = path.with_suffix("")
                require(original.is_file(), "Orphan Maven checksum")
                expected = hashlib.new(path.suffix[1:], original.read_bytes(), usedforsecurity=False).hexdigest()
                require(path.read_text().strip() == expected, "Incorrect Maven checksum")
            assets.append({"path": path.relative_to(repository).as_posix(), "sha256": sha256(path), "size": path.stat().st_size})
        binaries[stem + ".jar"] = sha256(directory / (stem + ".jar"))
    require(len({Path(a["path"]).name for a in assets}) == len(assets), "Release asset filenames collide")
    return binaries, assets


def production_sbom(graph, version, binaries):
    require(graph["version"] == version and set(graph["modules"]) == set(MODULES), "Production graph identity mismatch")
    components, references = [], {}
    for ref, node in sorted(graph["components"].items()):
        require("snapshot" not in node["version"].lower(), "Snapshot production dependency")
        require(not node["group"].startswith(("org.junit", "org.testcontainers", "org.openjdk.jmh", "org.pitest",
                                              "org.jetbrains.dokka", "org.gradle")),
                "Verification or build tooling cannot be labeled as a shipped runtime dependency")
        if node["group"] == GROUP:
            require(node["name"] in MODULES and node["version"] == version, "Internal or mismatched production module")
            hashes = [{"alg": "SHA-256", "content": binaries[node['name'] + '-' + version + '.jar']}]
        else:
            require(node["artifacts"], "Production component has no resolved artifact")
            hashes = [{"alg": "SHA-256", "content": item["sha256"]} for item in node["artifacts"]]
        url = purl(node["group"], node["name"], node["version"])
        references[ref] = url
        components.append({"type": "library", "bom-ref": url, "group": node["group"], "name": node["name"],
                           "version": node["version"], "purl": url, "scope": "required", "hashes": hashes})
    require({c["name"] for c in components if c["group"] == GROUP} == set(MODULES), "Incomplete production SBOM")
    dependencies = [{"ref": references[key], "dependsOn": sorted(references[dep] for dep in graph["dependencies"].get(key, []))}
                    for key in sorted(references)]
    aggregate = "quotaflow-release:" + version
    dependencies.append({"ref": aggregate, "dependsOn": [purl(GROUP, m, version) for m in MODULES]})
    core = next(d for d in dependencies if d["ref"] == purl(GROUP, "quotaflow-core", version))
    require(core["dependsOn"] and all(r.startswith("pkg:maven/org.slf4j/slf4j-api@") for r in core["dependsOn"]),
            "Core publication is not SLF4J-only")
    return {"bomFormat": "CycloneDX", "specVersion": "1.6", "version": 1,
            "metadata": {"timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                         "component": {"type": "framework", "bom-ref": aggregate, "name": "Quotaflow", "version": version}},
            "components": components, "dependencies": dependencies}


def prepare(output, version, dry_run=False, tag=None):
    version = release_version(version)
    identity = tag_identity(ROOT, tag) if not dry_run else {"commit": git(ROOT, "rev-parse", "HEAD"), "version": version}
    require(identity["version"] == version, "Tag/version conflict")
    files, source_hash = source_identity(ROOT)
    output = Path(output).resolve()
    require(not output.exists(), "Candidate output already exists; refusing to replace immutable bytes")
    output.mkdir(parents=True)
    repo = output / "repository"
    graph = output / "production-graph.json"
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("ORG_GRADLE_PROJECT_mavenCentral", "ORG_GRADLE_PROJECT_signing"))
           and key not in {"CENTRAL_PORTAL_USERNAME", "CENTRAL_PORTAL_TOKEN", "SIGNING_KEY", "SIGNING_PASSWORD", "GH_TOKEN", "GITHUB_TOKEN"}}
    command = [str(ROOT / "gradlew"), "publishAllPublicationsToVerificationRepository", "writeProductionDependencyGraph",
               "-PreleasePreparation=true", "-Pversion=" + version, "-PverificationRepository=" + str(repo),
               "-PproductionGraphOutput=" + str(graph), "--console=plain"]
    with (output / "preparation.log").open("w") as log:
        result = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    require(result.returncode == 0, "Candidate build failed; see preparation.log")
    require(source_identity(ROOT)[1] == source_hash, "Source changed during candidate preparation")
    production_graph = json.loads(graph.read_text())
    binaries, assets = inspect_repository(repo, version, production_graph)
    sbom = production_sbom(production_graph, version, binaries)
    sbom_path = output / ("quotaflow-" + version + "-sbom.cdx.json")
    sbom_path.write_text(json.dumps(sbom, indent=2) + "\n")
    validation = output / "sbom-validation.json"
    with (output / "sbom-validation.log").open("w") as log:
        result = subprocess.run([str(ROOT / "gradlew"), "validateReleaseSbom", "-PreleasePreparation=true",
                                 "-Pversion=" + version, "-PreleaseSbomInput=" + str(sbom_path),
                                 "-PreleaseSbomValidationOutput=" + str(validation), "--console=plain"],
                                cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    require(result.returncode == 0, "Release SBOM schema validation failed; see sbom-validation.log")
    require(source_identity(ROOT)[1] == source_hash, "Source changed during candidate validation")
    # Portal owns repository-level Maven metadata. Only immutable version paths enter the bundle.
    package = output / ("quotaflow-" + version + "-unsigned-maven.zip")
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as archive:
        for asset in assets:
            archive.write(repo / asset["path"], asset["path"])
    candidate = {"commit": identity["commit"], "sourceSha256": source_hash, "version": version, "artifacts": binaries}
    manifest = {"schemaVersion": 1, "state": "PREPARED_UNSIGNED", "mode": "dry-run" if dry_run else "release",
                "eligibleForPromotion": False, "candidate": candidate, "tag": tag, "modules": list(MODULES),
                "assets": [{**a, "path": "repository/" + a["path"]} for a in assets],
                "sbom": {"path": sbom_path.name, "sha256": sha256(sbom_path)},
                "sbomValidation": {"path": validation.name, "sha256": sha256(validation)},
                "unsignedBundle": {"path": package.name, "sha256": sha256(package)},
                "productionGraph": {"path": graph.name, "sha256": sha256(graph)}}
    (output / "candidate.json").write_text(json.dumps(candidate, indent=2) + "\n")
    (output / "source-files.json").write_text(json.dumps(files, indent=2) + "\n")
    (output / "candidate-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="operation", required=True)
    tag = commands.add_parser("validate-tag"); tag.add_argument("--tag", required=True); tag.add_argument("--expected-commit")
    credentials = commands.add_parser("check-credentials"); credentials.add_argument("--scope", choices=("central", "pgp", "all"), required=True)
    candidate = commands.add_parser("prepare"); candidate.add_argument("--version", required=True); candidate.add_argument("--tag"); candidate.add_argument("--dry-run", action="store_true"); candidate.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.operation == "validate-tag":
            report = tag_identity(ROOT, args.tag, args.expected_commit)
        elif args.operation == "check-credentials":
            report = {"configuredProperties": sorted(credential_environment(args.scope)), "status": "PASS"}
        else:
            require(args.dry_run or args.tag, "Real candidate preparation requires a release tag")
            report = prepare(args.output, args.version, args.dry_run, args.tag)
        print(json.dumps(report, indent=2)); return 0
    except (ValueError, OSError, KeyError, ET.ParseError, zipfile.BadZipFile) as failure:
        print(json.dumps({"status": "FAILED", "error": str(failure)})); return 1


if __name__ == "__main__":
    raise SystemExit(main())
