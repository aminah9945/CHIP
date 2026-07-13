# ADR-004: Self-Hosted, Cloud-Agnostic Infrastructure

- **Status:** Accepted (2026-07-10 — constraint set by institutional decision-makers)
- **Context:** HEC-funded project; infrastructure decisions rest with institutional decision-makers who strongly prefer self-hosting and full control, with cloud only if absolutely necessary. University lab servers at NUCES Islamabad are the target environment.

## Decision

- Everything runs **self-hosted on university hardware**, fully containerized.
- **Docker Compose** first; migration to **k3s** only when explicit triggers are met (defined in the infrastructure subsystem doc).
- **No managed cloud services** anywhere in the critical path. Cloud-portable by construction: S3 API via MinIO, standard Postgres, containers — so a future lift-and-shift is possible without redesign.
- External stakeholder access (NIH/NDMA/MoCC) via reverse proxy + TLS exposing only the dashboard/API; data infrastructure stays internal.

## Consequences

- The team owns backups, monitoring, TLS, and hardware capacity planning (covered in `subsystems/07-infrastructure-operations.md`).
- Hardware procurement becomes a project dependency — sizing tiers documented so the grant budget conversation can happen early.
- If decision-makers later approve cloud, migration is lift-and-shift, not redesign.
