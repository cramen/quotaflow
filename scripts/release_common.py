"""Shared release invariants. None of these helpers publishes artifacts."""
import hashlib
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import quote

MODULES = tuple("quotaflow-" + name for name in (
    "core", "store-redis", "fallback", "config", "spring-boot-starter", "kotlin", "micrometer"))
GROUP = "io.quotaflow"
SEMVER = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def release_version(value):
    match = SEMVER.fullmatch(value)
    require(match is not None and "snapshot" not in value.lower(), "Expected a non-snapshot semantic version")
    if match.group(4):
        require(all(not item.isdigit() or item == "0" or not item.startswith("0")
                    for item in match.group(4).split(".")), "Numeric prerelease identifiers cannot have leading zeroes")
    return value


def git(root, *args):
    result = subprocess.run(["git", *args], cwd=root, text=True, capture_output=True)
    require(result.returncode == 0, "Git identity check failed: " + args[0])
    return result.stdout.strip()


def clean_tree(root):
    require(not git(root, "diff", "HEAD", "--name-only"), "Release preparation requires a clean tracked tree")
    extra = git(root, "ls-files", "--others", "--exclude-standard", "-z").split("\0")
    require(not [p for p in extra if p and not p.startswith((".agents/", ".codex/"))],
            "Release preparation requires no untracked source files")


def tag_identity(root, tag, expected_commit=None):
    require(tag.startswith("v"), "Release tag must start with v")
    version = release_version(tag[1:])
    require(git(root, "rev-parse", "--is-shallow-repository") == "false",
            "Complete Git history is required; fetch full trusted main history")
    commit = git(root, "rev-parse", "--verify", "--end-of-options", f"refs/tags/{tag}^{{commit}}")
    main = git(root, "rev-parse", "--verify", "refs/remotes/origin/main^{commit}")
    require(git(root, "rev-parse", "HEAD") == commit, "Checkout must equal the release tag commit")
    if expected_commit is not None:
        require(commit == expected_commit, "Release commit differs from the authorized commit")
    result = subprocess.run(["git", "merge-base", "--is-ancestor", commit, main], cwd=root)
    require(result.returncode == 0, "Release tag is outside trusted main history")
    clean_tree(root)
    return {"tag": tag, "version": version, "commit": commit, "trustedMainCommit": main}


def credential_environment(scope, environment=None):
    source = os.environ if environment is None else environment
    result = {}
    def configured(alias, target, required=True):
        canonical = "ORG_GRADLE_PROJECT_" + target
        if alias in source and canonical in source:
            require(source[alias] == source[canonical], "Conflicting credential configuration: " + canonical)
        value = source.get(canonical, source.get(alias, ""))
        require(not required or bool(value.strip()), "Missing required credential: " + alias)
        result[canonical] = value
    if scope in ("central", "all"):
        for origin, target in (("CENTRAL_PORTAL_USERNAME", "mavenCentralUsername"),
                               ("CENTRAL_PORTAL_TOKEN", "mavenCentralPassword")):
            configured(origin, target)
    if scope in ("pgp", "all"):
        configured("SIGNING_KEY", "signingInMemoryKey")
        # An explicitly unprotected key has no passphrase; an empty value is valid.
        configured("SIGNING_PASSWORD", "signingInMemoryKeyPassword", required=False)
    require(scope in ("central", "pgp", "all"), "Unknown credential scope")
    return result


def purl(group, name, version):
    return "pkg:maven/" + quote(group, safe=".") + "/" + quote(name, safe=".-_") + "@" + quote(version, safe=".-_")


def safe_artifact(root, reference):
    root = Path(root).resolve()
    name = Path(reference["path"])
    require(not name.is_absolute() and ".." not in name.parts, "Unsafe artifact path")
    target = root / name
    require(not any(p.is_symlink() for p in [target, *target.parents] if p != root.parent), "Symlink artifact refused")
    require(target.resolve().is_relative_to(root) and target.is_file(), "Missing candidate artifact")
    require(sha256(target) == reference["sha256"], "Candidate artifact digest mismatch: " + name.as_posix())
    return target
