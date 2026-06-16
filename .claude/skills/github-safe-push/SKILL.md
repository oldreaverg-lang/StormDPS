---
name: github-safe-push
description: Commit and push edited source files to GitHub main safely, avoiding the NTFS-mount truncation bug. Use whenever pushing changed files to main (which auto-deploys via Railway), especially mid-size source files like api/routes.py or services/noaa_client.py.
---

# Safe push to main (NTFS-truncation-proof)

This repo lives on an NTFS mount that silently truncates large files on read and can leave trailing NUL bytes after in-place edits. Both have shipped broken deploys (syntax errors, missing tags) in routes.py, noaa_client.py, atcf_bdeck_client.py, main.py, frontend/index.html. `git push` here also auto-deploys to Railway, so corruption reaches production.

## Rules

- NEVER read a large file (> ~50 KB) through the mount and push its bytes - the read may be truncated mid-file (a clean cut, no error).
- Local `git` cannot write `.git/` on this mount; commit via the GitHub Git Data API instead.
- Verify before pushing: every Python file must `py_compile`, and no file may contain NUL bytes.

## Steps

1. Make your edits using the Read/Edit tools (Windows `C:\Users\...` paths read the real filesystem and are always complete) or by patching a known-good remote blob in memory.
2. Push with the helper (it reads each file fully, refuses NUL-corrupted or non-compiling files, then makes ONE atomic commit on `main`):
   ```
   python .claude/skills/github-safe-push/scripts/gh_push.py <path> [<path> ...] -m "message"
   ```
   It reads the `github_pat_...` token from `.env` (`GITHUB_TOKEN`). Railway auto-deploys after the ref updates.
3. Run the `deploy-verify` skill.

## Large generated files

`frontend/compiled_bundle.json` / `preload_bundle.json` exceed the safe-read size. Commit those from **native Windows git** (`git add ... && git commit && git push`), not through the mount. See `compile-cache-bake`.

## Recovery (file already truncated on the mount)

Fetch the last known-good version from git history, patch in memory, and push - never touch the mount copy:
```
info  = GET /repos/oldreaverg-lang/StormDPS/contents/<path>?ref=<good_sha>
fixed = base64decode(info.content).replace(OLD, NEW)   # in memory only
# py_compile fixed, then commit via gh_push.py / the API
```

## Tool

- `scripts/gh_push.py` - commit one or more files to `main` via the Git Data API, with NUL + compile guards.
