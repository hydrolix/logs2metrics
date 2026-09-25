# Claude project rules — logs2metrics

## PRs MUST target hydrolix/logs2metrics

This repository is a GitHub fork of `mercereau/hydrolix-metrics-go`. Because
of that, both the GitHub web UI and `gh pr create` without an explicit
`--repo` flag default to opening a PR against the **upstream parent**, not
this repo.

We do intend to contribute changes upstream, but only on purpose: a human
decides and opens those PRs. An agent never does, and never by accident.

**Rule:** every `gh pr create` invocation in this repo MUST include
`--repo hydrolix/logs2metrics` explicitly, and GitHub MCP
`create_pull_request` calls MUST use `owner: hydrolix`,
`repo: logs2metrics`. Never open a PR against any other repository,
including `mercereau/hydrolix-metrics-go` and contributors' forks.

This rule applies in every permission mode. A `PreToolUse` hook at
`.claude/hooks/block-upstream-pr.py` enforces it (exit code 2 = hard block),
and `.claude/settings.json` carries deny rules as a second layer. Do not
disable, weaken, or work around these guards. If an upstream PR is wanted,
tell the user and let them open it.
