---
name: create-pr
description: >-
  Create pull request: git diff summary, test plan, optional issue link. Universal, user-level.
  Use when create PR, pull request, describe the PR, test plan, gh pr create.
---

# Create Pull Request

Universal workflow for describing and creating a PR. Fully architecture-agnostic.

## When to apply

- The user asks to create a PR or describe a pull request.
- After `code-review` with no blockers.
- The branch is ready for review.

## Workflow

```
- [ ] 1. git status — no uncommitted junk
- [ ] 2. git log + git diff <base>...HEAD — full picture of the branch
- [ ] 3. Check against the task / AC
- [ ] 4. Draft Summary + Test plan (template below)
- [ ] 5. Show the user the title + body
- [ ] 6. Push + gh pr create — only on an explicit request
```

## Gathering context

In parallel (if available):

```bash
git status
git diff
git log --oneline -10
git diff main...HEAD   # base branch — confirm with the user or from remote
```

Account for **all** commits on the branch, not only the latest.

## PR body template

```markdown
## Summary
- …
- …

## Test plan
- [ ] …
- [ ] …
```

### Summary

- 1–3 bullets: **what** and **why** (not a line-by-line changelog).
- If there is a breaking change — say so explicitly.

### Test plan

Checklist for QA and the reviewer:

- Happy path from the AC.
- Negative / edge cases.
- Regression of adjacent areas (if they were touched).
- Commands for a local check (`npm run test`, … — from the project).

## Title

- Conventional Commits style, if the repo uses it — see `commit-messages` and `git log`.
- Or a short description of the feature/fix in the team's language.
- ≤ ~100 characters.

## Push and create

**Only on an explicit user request:**

```bash
git push -u origin HEAD
gh pr create --title "…" --body "$(cat <<'EOF'
## Summary
…

## Test plan
- [ ] …
EOF
)"
```

Do not push without a request. Do not force-push. Do not `--no-verify`.

## Anti-patterns

- PR description = a file list with no meaning.
- Test plan "checked locally" with no concrete steps.
- One summary bullet for 15 commits of different meaning — suggest splitting the PR.
- Secrets in the description.

## Related skills

| Skill | When |
| --- | --- |
| `commit-messages` | Title style, if conventional |
| `code-review` | Before creating the PR |
| `fix-bug` / `write-tests` | Fix context and test plan |
