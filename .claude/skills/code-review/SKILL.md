---
name: code-review
description: Run a fresh-eyes sub-agent review of pending changes before pushing to main. Use before any non-trivial commit/deploy - especially scoring-formula, api/routes, or frontend changes - to catch regressions, scope creep, and unsupported claims.
---

# Code review (fresh-eyes sub-agent)

`git push` to `main` auto-deploys to Railway, so changes ship straight to production. Before pushing anything non-trivial, get an independent review from a sub-agent that did NOT write the change - the Feedback Loop / Review Agent pattern catches what the author's own context hides.

## When to use

- Before pushing changes to `core/` scoring, `api/routes.py`, `compile_cache.py`, or `frontend/index.html`.
- Before landing a basin-formula change (pair with `basin-dps-audit`).
- Any commit touching more than a couple of files.

## Steps

1. Summarize the diff: which files changed and why (one line each).
2. Spawn a review sub-agent (Task tool) with an isolated, self-contained prompt. Give it the changed paths, the intent, and explicit READ-ONLY instructions. Do NOT include your own justification - let it judge independently.
3. Ask the reviewer to check, concretely:
   - Correctness / regressions - does it do what's claimed? any broken call sites?
   - Python: does every changed file `py_compile`? Scoring: do affected storms stay within the expected ~1-4 pt tolerance vs `frontend/compiled_bundle.json`?
   - Scope creep - anything changed that wasn't intended?
   - Secret / large-file hazards - no tokens; no file >50 KB read through the NTFS mount (see `github-safe-push`).
   - Unsupported claims in the commit message or docs.
4. Require a tight reply: per concern, "OK" or a specific fix.
5. Incorporate the fixes (or reject with a reason), then push via `github-safe-push` and verify with `deploy-verify`.

## Verify against the source of truth

Check findings against **origin/main via the GitHub API**, not just local `git` - the local working copy can be behind, which makes "nothing changed" reviews misleading. Example:

```
curl -s -H "Authorization: Bearer $GITHUB_TOKEN" \
  https://api.github.com/repos/oldreaverg-lang/StormDPS/git/ref/heads/main
```
