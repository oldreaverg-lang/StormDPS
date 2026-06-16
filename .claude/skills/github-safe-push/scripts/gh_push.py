#!/usr/bin/env python3
"""Commit one or more files to GitHub `main` via the Git Data API.

Avoids local `git` (the NTFS mount blocks `.git` writes) and pushes a single
atomic commit built from the given files' current contents.

Usage:
    python .claude/skills/github-safe-push/scripts/gh_push.py PATH [PATH ...] -m "message"

Reads GITHUB_TOKEN from .env (the fine-grained github_pat_ token) or the
environment. Paths are repo-relative. Guards: every .py is byte-compiled and
every file is checked for NUL bytes before anything is pushed, so truncated or
corrupted content never reaches production.
"""
import argparse
import base64
import json
import os
import py_compile
import sys
import urllib.error
import urllib.request

REPO = "oldreaverg-lang/StormDPS"
API = "https://api.github.com/repos/" + REPO


def load_token():
    """Last GITHUB_TOKEN= line in .env wins (the valid github_pat_ one)."""
    token = None
    try:
        with open(".env", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if line.startswith("GITHUB_TOKEN=") and len(line) > len("GITHUB_TOKEN="):
                    token = line.split("=", 1)[1].strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    token = token or os.environ.get("GITHUB_TOKEN")
    if not token:
        sys.exit("No GITHUB_TOKEN found in .env or environment.")
    return token


def api(method, path, token, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        API + path,
        data=data,
        method=method,
        headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as exc:
        sys.exit("GitHub API {} {} -> {}: {}".format(method, path, exc.code, exc.read().decode()[:300]))


def main():
    parser = argparse.ArgumentParser(description="Safely commit files to main via the GitHub API.")
    parser.add_argument("paths", nargs="+", help="repo-relative file paths to commit")
    parser.add_argument("-m", "--message", required=True, help="commit message")
    parser.add_argument("--branch", default="main")
    args = parser.parse_args()

    # Integrity guards BEFORE any network call.
    for path in args.paths:
        if not os.path.isfile(path):
            sys.exit("Not a file: " + path)
        with open(path, "rb") as fh:
            blob = fh.read()
        if b"\x00" in blob:
            sys.exit("Refusing to push: {} contains NUL bytes (mount truncation/padding).".format(path))
        if path.endswith(".py"):
            try:
                py_compile.compile(path, doraise=True)
            except py_compile.PyCompileError as exc:
                sys.exit("Refusing to push: {} does not compile:\n{}".format(path, exc))

    token = load_token()
    head = api("GET", "/git/ref/heads/" + args.branch, token)["object"]["sha"]
    base_tree = api("GET", "/git/commits/" + head, token)["tree"]["sha"]

    tree = []
    for path in args.paths:
        with open(path, "rb") as fh:
            content = fh.read()
        blob_sha = api(
            "POST", "/git/blobs", token,
            {"content": base64.b64encode(content).decode(), "encoding": "base64"},
        )["sha"]
        tree.append({"path": path.replace("\\", "/"), "mode": "100644", "type": "blob", "sha": blob_sha})

    new_tree = api("POST", "/git/trees", token, {"base_tree": base_tree, "tree": tree})["sha"]
    commit = api("POST", "/git/commits", token, {"message": args.message, "tree": new_tree, "parents": [head]})
    api("PATCH", "/git/refs/heads/" + args.branch, token, {"sha": commit["sha"], "force": False})

    print("Pushed {} file(s) to {}: {}".format(len(args.paths), args.branch, commit["sha"][:9]))
    print("https://github.com/{}/commit/{}".format(REPO, commit["sha"]))


if __name__ == "__main__":
    main()
