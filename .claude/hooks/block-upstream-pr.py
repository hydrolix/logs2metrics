#!/usr/bin/env python3
"""PreToolUse guard: agents may open PRs only against hydrolix/metrics-exporter.

This repo is a GitHub fork of mercereau/hydrolix-metrics-go, so both the
GitHub web UI and a bare `gh pr create` default the PR base to that parent.
Contributing upstream is intended, but only on purpose, by a human. This
hook keeps an agent from doing it by accident.

Covers two surfaces:

1. Bash: `gh pr create` must name its target with `--repo`/`-R` (or
   `GH_REPO=`), and that target must be hydrolix/metrics-exporter.
2. MCP: any tool whose name ends in `create_pull_request` (the GitHub MCP
   servers take `owner`/`repo` parameters) must target
   hydrolix/metrics-exporter.

Exit codes: 0 allows the call; 2 blocks it and shows stderr to the agent.
Any other failure (bad payload, missing python3) fails open.
"""

from __future__ import annotations

import json
import re
import sys

ALLOWED_OWNER = "hydrolix"
ALLOWED_REPO = "metrics-exporter"
ALLOWED_SLUG = f"{ALLOWED_OWNER}/{ALLOWED_REPO}"
FORK_PARENT = "mercereau/hydrolix-metrics-go"


def is_pr_create(command: str) -> bool:
    return bool(re.search(r"(?:^|[;&|`$(\s])gh\s+pr\s+create\b", command))


def extract_target(command: str) -> str | None:
    m = re.search(r"(?:--repo|-R)(?:[=\s]+)(['\"]?)([^'\"\s;|&]+)\1", command)
    if not m:
        m = re.search(r"\bGH_REPO=(['\"]?)([^'\"\s;|&]+)\1", command)
    if not m:
        return None
    # gh accepts HOST/OWNER/REPO; compare on OWNER/REPO.
    return "/".join(m.group(2).lower().rstrip("/").split("/")[-2:])


def block(message: str) -> int:
    sys.stderr.write(
        f"BLOCKED: {message}\n"
        f"Agent-opened PRs in this repo must target {ALLOWED_SLUG}. This repo\n"
        f"is a fork of {FORK_PARENT}, which GitHub picks as the default base.\n"
        "Upstream contributions are made deliberately by a human, not by an\n"
        "agent. See CLAUDE.md and .claude/hooks/block-upstream-pr.py.\n"
    )
    return 2


def handle_bash(tool_input: dict) -> int:
    command = tool_input.get("command", "")
    if not isinstance(command, str) or not is_pr_create(command):
        return 0

    target = extract_target(command)
    if target is None:
        return block(
            "`gh pr create` without `--repo` could default to the fork parent.\n"
            f"Re-run with `--repo {ALLOWED_SLUG}`."
        )
    if target != ALLOWED_SLUG:
        return block(f"refusing to open a PR against {target}.")
    return 0


def handle_mcp_pr_create(tool_name: str, tool_input: dict) -> int:
    owner = str(tool_input.get("owner", "")).lower()
    repo = str(tool_input.get("repo", "")).lower()
    if (owner, repo) != (ALLOWED_OWNER, ALLOWED_REPO):
        return block(f"refusing to open a PR against {owner}/{repo} via {tool_name}.")
    return 0


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    tool_name = payload.get("tool_name", "")
    tool_input = payload.get("tool_input") or {}
    if not isinstance(tool_input, dict):
        return 0

    if tool_name == "Bash":
        return handle_bash(tool_input)
    if tool_name.startswith("mcp__") and tool_name.endswith("create_pull_request"):
        return handle_mcp_pr_create(tool_name, tool_input)
    return 0


if __name__ == "__main__":
    sys.exit(main())
