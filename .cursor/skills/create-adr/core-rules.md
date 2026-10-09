# ADR — short rules (skill)

Full policy: project rule **`adr.mdc`**.
Template: [template-repo.md](./template-repo.md).

## Gate (short)

| ADR needed | ADR not needed |
| --- | --- |
| 2+ options, trade-offs | project convention |
| auth, deploy, migrations, infra | how-to in guides |
| cross-module choice | flow description in architecture.md |

## Where

`docs/04-architecture/adr/<nnn>-<slug>.md` (legacy: `docs/adr/` — not for new projects).

## Self-check

```
- [ ] Repo-level ADR (not a local note in a PR)
- [ ] One file = one decision
- [ ] ≥1 alternative rejected
- [ ] Trade-offs present if there is a compromise
- [ ] Row in adr/README.md
- [ ] Supersede, not delete
```
