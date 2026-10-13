"""Fail if a tracked file carries something that must not be published.

The repository is written to be public from its first commit. This looks at every file git tracks
(and every commit message with --history) for the kinds of thing that leak: e-mail addresses, local
paths, credentials. It prints only the file and line of each finding, never the match itself.

    python scripts/audit_publishable.py [--history]
"""

from __future__ import annotations

import re
import subprocess
import sys

PATTERNS = {
    "email address": re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    "windows user path": re.compile(r"[A-Za-z]:[\\/]+Users[\\/]", re.I),
    "drive path": re.compile(r"\b[A-Z]:[\\/](?!/)", re.I),
    "home path": re.compile(r"/home/[a-z]|/Users/[A-Za-z]"),
    "credential assignment": re.compile(r"(SECRET|PASSWORD|TOKEN)\s*=\s*['\"]?[A-Za-z0-9&%$#!_\-]{6,}", re.I),
    "bearer token": re.compile(r"Bearer\s+[A-Za-z0-9._\-]{20,}"),
}

# Allowed on purpose: a placeholder that says what goes there, and URLs and examples that
# contain an @ without being an address.
ALLOW = [
    re.compile(r"BW_CLIENT_SECRET=$"),
    re.compile(r"git@github\.com"),
]


def tracked() -> list[str]:
    return subprocess.run(["git", "ls-files"], capture_output=True, text=True, check=True).stdout.split()


def scan(name: str, text: str) -> list[str]:
    findings = []
    for n, line in enumerate(text.splitlines(), 1):
        if any(a.search(line) for a in ALLOW):
            continue
        for label, pattern in PATTERNS.items():
            if pattern.search(line):
                findings.append(f"{name}:{n}: {label}")
    return findings


def main() -> int:
    findings: list[str] = []
    for path in tracked():
        if path.endswith((".parquet", ".jar", ".png")) or path == "scripts/audit_publishable.py":
            continue
        try:
            with open(path, encoding="utf-8") as f:
                findings += scan(path, f.read())
        except UnicodeDecodeError:
            continue
    if "--history" in sys.argv:
        log = subprocess.run(["git", "log", "--format=%H%n%B"], capture_output=True, text=True, check=True).stdout
        findings += scan("commit messages", log)
        authors = subprocess.run(["git", "log", "--format=%an <%ae>"], capture_output=True, text=True, check=True)
        identities = sorted(set(authors.stdout.splitlines()))
        print("commit identities:", ", ".join(identities))
    for f in findings:
        print(f)
    print(f"{len(findings)} finding(s)")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
