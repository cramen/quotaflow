"""Resumable promotion of immutable bytes; adapters own durable state and transport.

The journal must survive runner loss. A caller must serialize operations for a
version (the release workflow's concurrency group), and persist each transition
before the following network mutation. An uncertain upload is never retried.
"""
import hashlib
import json
import uuid
import zipfile

from release_common import GROUP, MODULES, purl, require, safe_artifact, sha256


class RecoveryRequired(ValueError):
    """Remote state needs reconciliation before another mutation is safe."""


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def reconcile_assets(github, release, assets):
    """Existing assets are verified by downloading bytes, never silently replaced."""
    require(len(assets) == len({a.name for a in assets}), "Duplicate release asset filename")
    existing = github.assets(release)
    require(len(existing) == len({a["name"] for a in existing}), "Duplicate remote asset filename")
    by_name = {a["name"]: a for a in existing}
    for path in assets:
        if path.name not in by_name:
            github.upload_asset(release, path.name, path.read_bytes())
        # Read back even after a successful upload response.
        data = github.download_asset(release, path.name)
        require(hashlib.sha256(data).hexdigest() == sha256(path), "Release asset collision: " + path.name)


def verify_deployment(portal, deployment, bundle, expected_purls, published=False):
    status = portal.status(deployment)
    require(status["deploymentId"] == deployment, "Portal returned a different deployment")
    require(status["deploymentState"] in {"PENDING", "VALIDATING", "VALIDATED", "PUBLISHING", "PUBLISHED", "FAILED"},
            "Unknown Portal deployment state")
    if status["deploymentState"] in {"VALIDATED", "PUBLISHING", "PUBLISHED"}:
        require(set(status["purls"]) == set(expected_purls) and len(status["purls"]) == len(expected_purls),
                "Central deployment coordinates differ from the candidate")
        with zipfile.ZipFile(bundle) as archive:
            names = archive.namelist()
            require(len(names) == len(set(names)), "Duplicate bundle entry")
            for name in names:
                data = portal.download(deployment, name, published=published or status["deploymentState"] == "PUBLISHED")
                require(hashlib.sha256(data).digest() == hashlib.sha256(archive.read(name)).digest(),
                        "Central deployment bytes differ: " + name)
    return status["deploymentState"]


def advance(root, manifest, manifest_sha256, assets, journal, portal, github, verify_gates, recovered_deployment=None):
    """Perform one bounded advancement; pending remote work is returned to the caller.

    verify_gates must independently validate all evidence and exact candidate
    bytes. It receives True when publication is already irreversible, so expiry
    can be reported without preventing completion of the matching GitHub release.
    The journal implementation must read back persisted transitions before return.
    """
    require(hashlib.sha256(encoded(manifest)).hexdigest() == manifest_sha256, "Promotion manifest digest mismatch")
    candidate = manifest["candidate"]
    identity = {"manifestSha256": manifest_sha256, "commit": candidate["commit"], "version": candidate["version"]}
    state = journal.load()
    if state is not None:
        require(state["identity"] == identity, "Version already belongs to a different candidate")
    irreversible = state is not None and state["phase"] in {"CENTRAL_PUBLISHED", "COMPLETE"}
    if state is not None and state["phase"] in {"UPLOADED", "PROMOTION_INTENT"}:
        # A runner can die after Central commits but before journaling success.
        # Query the known ID before applying expiring pre-publication evidence.
        observed = portal.status(state["deployment"])
        require(observed["deploymentId"] == state["deployment"], "Portal returned a different deployment")
        irreversible = observed["deploymentState"] == "PUBLISHED"
    verify_gates(irreversible)
    bundle = safe_artifact(root, manifest["mavenBundle"])
    expected = [purl(GROUP, module, candidate["version"]) for module in MODULES]
    if state is None:
        release = github.ensure_draft("v" + candidate["version"], candidate["commit"])
        reconcile_assets(github, release, assets)
        state = {"identity": identity, "phase": "DRAFT_VERIFIED", "release": release, "deployment": None}
        journal.save(state)
    release = state["release"]
    reconcile_assets(github, release, assets)

    def transition(phase, **values):
        nonlocal state
        state = {**state, **values, "phase": phase}
        journal.save(state)

    if state["phase"] == "DRAFT_VERIFIED":
        transition("UPLOAD_INTENT", attempt=str(uuid.uuid4()))
        # Any exception, process death or failed journal write leaves UPLOAD_INTENT.
        # The next invocation refuses another upload, even if the first sent no bytes.
        deployment = portal.upload(bundle, "quotaflow-" + candidate["version"] + "-" + manifest_sha256[:16])
        transition("UPLOADED", deployment=deployment)
    if state["phase"] == "UPLOAD_INTENT":
        if recovered_deployment is None:
            raise RecoveryRequired("Upload outcome uncertain; recover the existing Portal deployment ID, never re-upload")
        remote = verify_deployment(portal, recovered_deployment, bundle, expected)
        require(remote in {"VALIDATED", "PUBLISHING", "PUBLISHED"}, "Recovered deployment must expose verifiable coordinates and bytes")
        transition("UPLOADED", deployment=recovered_deployment)
    elif recovered_deployment is not None:
        require(recovered_deployment == state["deployment"], "Cannot replace a recorded deployment ID")
    if state["phase"] in {"UPLOADED", "PROMOTION_INTENT"}:
        remote = verify_deployment(portal, state["deployment"], bundle, expected)
        require(remote != "FAILED", "Portal validation failed; keep the deployment for diagnosis")
        if remote == "VALIDATED":
            # The same known deployment was queried and its bytes rechecked.
            # Retrying its promotion cannot create another deployment/upload.
            verify_gates(False)
            transition("PROMOTION_INTENT")
            portal.promote(state["deployment"])
            return {**state, "status": "PENDING"}
        if remote == "PUBLISHED":
            transition("CENTRAL_PUBLISHED")
        else:
            return {**state, "status": "PENDING"}
    if state["phase"] == "CENTRAL_PUBLISHED":
        # Completion never rebuilds, reuploads, or promotes a second deployment.
        github.publish(release, "v" + candidate["version"], candidate["commit"])
        require(github.is_complete(release, "v" + candidate["version"], candidate["commit"]), "GitHub completion is unconfirmed")
        transition("COMPLETE")
    require(state["phase"] == "COMPLETE", "Unknown release journal phase")
    require(github.is_complete(release, "v" + candidate["version"], candidate["commit"]), "Completed release metadata changed")
    return {**state, "status": "COMPLETE"}
