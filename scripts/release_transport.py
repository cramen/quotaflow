"""Release-only HTTP adapters. Constructing an adapter performs no requests."""
import base64
import hashlib
import json
from pathlib import PurePosixPath
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid

from release_common import credential_environment, require
from release_promotion import encoded


class HttpFailure(OSError):
    def __init__(self, status):
        self.status = status
        super().__init__("Release service request failed (HTTP " + str(status) + "); reconcile before retry")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl): return None


def request(method, url, token=None, data=None, content_type="application/json", download=False):
    require(url.startswith("https://"), "Release endpoints require HTTPS")
    headers = {"User-Agent": "quotaflow-release", "Accept": "application/octet-stream" if download else "application/json"}
    if token: headers["Authorization"] = "Bearer " + token
    if data is not None: headers["Content-Type"] = content_type
    if urllib.parse.urlsplit(url).hostname == "api.github.com": headers["X-GitHub-Api-Version"] = "2022-11-28"
    opener = urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(urllib.request.Request(url, data=data, headers=headers, method=method), timeout=120) as response:
            return response.read()
    except urllib.error.HTTPError as failure:
        # GitHub private assets redirect to a temporary CDN URL. Never forward
        # API credentials to that origin and never follow mutation redirects.
        if download and method == "GET" and failure.code in (301, 302, 303, 307, 308):
            target = failure.headers.get("Location", "")
            host = urllib.parse.urlsplit(target).hostname or ""
            require(host in {"release-assets.githubusercontent.com", "objects.githubusercontent.com"}, "Unexpected asset redirect")
            return request("GET", target, download=False)
        raise HttpFailure(failure.code) from None
    except (urllib.error.URLError, TimeoutError):
        raise OSError("Release service response unavailable; reconcile before retry") from None


def component(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.+-]+", value), "Unsafe release identifier")
    return urllib.parse.quote(value, safe="")


class Portal:
    base = "https://central.sonatype.com/api/v1/publisher"

    def __init__(self, environment=None):
        credentials = credential_environment("central", environment)
        user = credentials["ORG_GRADLE_PROJECT_mavenCentralUsername"]
        password = credentials["ORG_GRADLE_PROJECT_mavenCentralPassword"]
        self.token = base64.b64encode((user + ":" + password).encode()).decode()

    def upload(self, bundle, name):
        boundary = "quotaflow-" + uuid.uuid4().hex
        body = (("--" + boundary + '\r\nContent-Disposition: form-data; name="bundle"; filename="candidate.zip"'
                 + "\r\nContent-Type: application/octet-stream\r\n\r\n").encode()
                + bundle.read_bytes() + ("\r\n--" + boundary + "--\r\n").encode())
        query = urllib.parse.urlencode({"publishingType": "USER_MANAGED", "name": name})
        value = request("POST", self.base + "/upload?" + query, self.token, body,
                        "multipart/form-data; boundary=" + boundary).decode().strip()
        require(re.fullmatch(r"[0-9a-fA-F-]{36}", value), "Upload response lacks a valid deployment ID; reconcile before retry")
        require(str(uuid.UUID(value)) == value.lower(), "Invalid deployment UUID")
        return value

    def status(self, deployment):
        return json.loads(request("POST", self.base + "/status?id=" + component(deployment), self.token, b""))

    def promote(self, deployment):
        request("POST", self.base + "/deployment/" + component(deployment), self.token, b"")

    def download(self, deployment, name, published=False):
        path = PurePosixPath(name)
        require(not path.is_absolute() and ".." not in path.parts and not name.endswith("/"), "Unsafe Maven path")
        encoded_path = urllib.parse.quote(name, safe="/")
        if published:
            return request("GET", "https://repo.maven.apache.org/maven2/" + encoded_path)
        return request("GET", self.base + "/deployment/" + component(deployment) + "/download/" + encoded_path, self.token)


class GitHub:
    def __init__(self, repository, token):
        require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository), "Invalid GitHub repository")
        require(bool(token.strip()), "Missing GitHub release credential")
        self.repository = repository; self.token = token
        self.base = "https://api.github.com/repos/" + repository

    def api(self, method, path, data=None):
        response = request(method, self.base + path, self.token, encoded(data) if data is not None else None)
        return json.loads(response) if response else None

    def find(self, tag):
        # Tag lookup can omit drafts, so enumerate accessible releases including drafts.
        matches = []; page = 1
        while True:
            entries = self.api("GET", "/releases?per_page=100&page=" + str(page))
            matches.extend(r for r in entries if r["tag_name"] == tag)
            if len(entries) < 100: break
            page += 1
        require(len(matches) <= 1, "Multiple GitHub releases for one tag")
        return matches[0] if matches else None

    def ensure_draft(self, tag, commit):
        release = self.find(tag)
        if release is None:
            release = self.api("POST", "/releases", {"tag_name": tag, "target_commitish": commit,
                               "name": tag, "draft": True, "prerelease": "-" in tag,
                               "body": "Verified immutable candidate; release reconciliation is in progress."})
        require(release["draft"] is True and release["target_commitish"] == commit,
                "Existing release is not the expected candidate draft")
        return release["id"]

    def assets(self, release):
        result = []; page = 1
        while True:
            entries = self.api("GET", "/releases/" + str(int(release)) + "/assets?per_page=100&page=" + str(page))
            result.extend(entries)
            if len(entries) < 100: return result
            page += 1

    def upload_asset(self, release, name, data):
        component(name)
        url = "https://uploads.github.com/repos/" + self.repository + "/releases/" + str(int(release)) + "/assets?name=" + urllib.parse.quote(name, safe="")
        request("POST", url, self.token, data, "application/octet-stream")

    def download_asset(self, release, name):
        matches = [a for a in self.assets(release) if a["name"] == name]
        require(len(matches) == 1, "Missing or duplicate release asset: " + name)
        return request("GET", self.base + "/releases/assets/" + str(int(matches[0]["id"])), self.token, download=True)

    def publish(self, release, tag, commit):
        current = self.api("GET", "/releases/" + str(int(release)))
        require(current["tag_name"] == tag and current["target_commitish"] == commit, "Release metadata identity changed")
        self.api("PATCH", "/releases/" + str(int(release)), {"draft": False,
                 "body": "Published from the verified immutable candidate. See the attached signed manifest, SBOM and verification evidence."})

    def is_complete(self, release, tag, commit):
        current = self.api("GET", "/releases/" + str(int(release)))
        return current["draft"] is False and current["tag_name"] == tag and current["target_commitish"] == commit


class GitHubJournal:
    """Append-only, hash-chained release assets survive loss of a CI runner.

    One workflow concurrency group per version is mandatory. This is not a
    multi-writer database; unexpected concurrent records fail verification.
    """
    prefix = "quotaflow-release-journal-"

    def __init__(self, github, tag):
        self.github = github; self.tag = tag

    def records(self):
        release = self.github.find(self.tag)
        if release is None: return None, []
        names = sorted(a["name"] for a in self.github.assets(release["id"]) if a["name"].startswith(self.prefix))
        records = []; previous = None
        for number, name in enumerate(names):
            data = self.github.download_asset(release["id"], name)
            digest = hashlib.sha256(data).hexdigest()
            require(name == self.prefix + f"{number:06d}-" + digest + ".json", "Release journal gap, fork or digest mismatch")
            record = json.loads(data)
            require(encoded(record) == data, "Noncanonical release journal record")
            require(record["previousSha256"] == previous and record["sequence"] == number, "Release journal chain mismatch")
            require(record["state"]["release"] == release["id"], "Release journal belongs to another draft")
            if records: require(record["state"]["identity"] == records[0]["state"]["identity"], "Release journal candidate changed")
            records.append(record); previous = digest
        return release, records

    def load(self):
        _, records = self.records()
        return records[-1]["state"] if records else None

    def save(self, state):
        release, records = self.records()
        require(release is not None and release["id"] == state["release"], "Durable draft is missing")
        if records and records[-1]["state"] == state: return
        record = {"sequence": len(records), "previousSha256": hashlib.sha256(encoded(records[-1])).hexdigest() if records else None,
                  "state": state}
        data = encoded(record); digest = hashlib.sha256(data).hexdigest()
        name = self.prefix + f"{len(records):06d}-" + digest + ".json"
        self.github.upload_asset(release["id"], name, data)
        require(self.github.download_asset(release["id"], name) == data and self.load() == state, "Durable journal write is unconfirmed")
