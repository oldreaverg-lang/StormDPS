# StormDPS project skills

Project-specific Claude Code skills, committed to the repo (the monorepo
pattern). The `.gitignore` is scoped so `.claude/skills/` is tracked while
`.claude/worktrees/` (session transcripts) and any `settings.local.json`
stay ignored.

| Skill | Use it when |
|-------|-------------|
| `nri-zone-rebuild` | Regenerate `frontend/nri_zones.json` from FEMA NRI, then audit it |
| `compile-cache-bake` | Rebake `frontend/compiled_bundle.json` after a scoring-formula change |
| `basin-dps-audit` | Validate a basin's DPS formula against ground truth before changing coefficients |
| `github-safe-push` | Commit/push to `main` without the NTFS-mount truncation bug |
| `deploy-verify` | Purge/warm Cloudflare and verify SSR + PageSpeed after a deploy |

Each skill is a folder with a `SKILL.md` (lean, prescriptive). Reference
material and tools live alongside (`references/`, `scripts/`).
