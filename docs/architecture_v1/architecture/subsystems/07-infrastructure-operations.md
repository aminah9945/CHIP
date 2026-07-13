# 07 — Infrastructure & Operations

**Subsystem:** Infrastructure, deployment, data protection, observability, CI/CD, operations
**Status:** Draft v1 for review
**Date:** 2026-07-10
**Owner:** Infrastructure/platform architecture (PCN research group, NUCES/FAST Islamabad)
**Related docs:** `01-data-model-and-schemas` (schema registry choice), `02-ingestion-connectors`, `03-nlp-pipeline`, `04-knowledge-graph-rag`, `05-analytics-forecasting-alerting`, `06-serving-dashboard`. Reconciliation ADRs binding here: **ADR-009** (schema registry = Apicurio, supersedes the Karapace assumption throughout this doc) and ADR-007 (Kafka wire contract).

---

## 0. Purpose and design principles

This document is the micro-level design for everything that keeps CHIP alive: hardware, environments,
container platform, networking, backups, monitoring, CI/CD, and the human processes around them.

The scale reality drives every decision here: **~0.5–1 TB over the project's life (worst case a few TB), low
request volume, and a team of rotating MS/PhD students with no prior ops experience.** The enemy is not load.
The enemy is entropy: a student graduates, a disk fills silently, a cert expires, nobody remembers how the
Kafka topics were created. Every choice below is optimized for *survivability over 3+ years under turnover*,
not throughput.

> **Storage figure corrected 2026-07-13.** This document previously said *"tens of GB/year"* while 00's overview
> said *"under 100 GB lifetime"* — the two never agreed, and neither was right. The real lifetime figure is
> **~0.5–1 TB** (00 §1.1 has the breakdown), which still fits comfortably inside the 2×4 TB NVMe + 2×8 TB HDD
> below. The **conclusion is unchanged** — a few TB on one box is not a big-data problem — **but the number had to
> be right**, because a false premise is load-bearing rhetoric, and the moment someone catches it they will
> challenge the conclusion resting on it.
>
> **The single largest lever on that figure is ADR-013's relevance gate.** Because news arrives keyword-filtered
> from NAaaS, CHIP never stores the 95–98% of the firehose that is irrelevant. Without that gate, news bronze
> alone would have been ~100 GB/**year** of raw HTML, and the pgvector/Neo4j sizings in this document would be
> off by 20–50×.

Non-negotiable principles (treat violations as design bugs):

1. **Boring technology only.** Every component must have 5+ years of production history, huge community
   documentation, and be debuggable by a student with Google and one afternoon.
2. **If it is not in git, it does not exist.** No hand-edited config on servers, no snowflake state,
   no "Ali knows how that works." Ansible + compose files + docs in the monorepo are the single source of truth.
3. **One way to do each thing.** One deploy command, one backup mechanism per store, one secrets tool,
   one alerting channel. Options are entropy.
4. **Fewest stateful services possible.** Every stateful service is a backup obligation, a restore drill,
   and a 3 a.m. incident. We consolidate state into Postgres + MinIO wherever a tool allows it.
5. **Everything replayable that can be replayable.** Raw documents land in MinIO; canonical facts land in
   Postgres. Kafka topics and the Neo4j graph are *derived* and rebuildable. This shrinks the backup
   surface to two systems that matter.
6. **A first-semester student must be able to operate prod from the runbooks alone.** If a runbook step
   requires judgment, the runbook is incomplete.

LOCKED upstream decisions honored here: self-hosted on-prem on university hardware; Docker Compose first
with explicit k3s migration triggers (§3.3); no managed services; cloud-agnostic design; external
government stakeholders (NIH/NDMA/MoCC) reach the dashboard securely over the internet.

---

## 1. Hardware sizing — three tiers

### 1.1 Sizing basis (per service group, pilot steady state)

| Service group | Containers | vCPU (steady) | RAM | Disk (3-yr) | Notes |
|---|---|---|---|---|---|
| PostgreSQL (PostGIS + TimescaleDB + pgvector) | 1 | 4 | 16 GB | 500 GB NVMe | The analytical core; give it the best disk. `shared_buffers` 4 GB, `work_mem` modest. |
| MinIO | 1 | 2 | 2 GB | 2–4 TB HDD OK | Raw documents, MLflow artifacts, Dagster compute logs, pgBackRest repo replica. |
| Neo4j Community | 1 | 2 | 8 GB | 100 GB NVMe | 4 GB heap + 2–3 GB page cache. Community = single instance, no clustering — accepted. |
| Kafka (KRaft, single broker) + **Apicurio** (ADR-009) | 2 | 2 | 4 GB | 200 GB | 2 GB broker heap. Short retention (§5.1) keeps disk small. **Apicurio persists schemas in the main Postgres**, so it rides pgBackRest and needs no separate export job — unlike Karapace, which stores them in a compacted Kafka topic. (This table said "Karapace" throughout; **ADR-009 supersedes it.**) |
| Dagster (webserver + daemon + 3 code locations) | 5 | 2 | 6 GB | 20 GB | Run/event storage in Postgres; compute logs to MinIO. Also runs the **batch drains** for the low-frequency Kafka consumers (ADR-002 §A3) — no long-running consumer daemons. |
| Spark (local-mode job container, on demand) | 0–1 | 0–8 burst | 0–16 GB burst | scratch 100 GB | Only runs during NLP enrichment/backfills. Not resident. **Local mode — a library with a JVM, not a cluster.** |
| NLP inference workers (CPU path) | 1–2 | 2–8 burst | 8 GB | — | Fine-tuned XLM-R NER/RE. CPU fallback must remain supported. |
| **Triton (GPU 0 — resident serving)** | 1 | 2 | 8 GB + **~7 GB VRAM** | 50 GB (models) | XLM-R NER/RE/causal + **BGE-M3 embedder, 1024-dim** (ADR-011). Dynamic batching. |
| **vLLM — graph-RAG (GPU 0 — resident serving)** | 1 | 4 | 16 GB + **~8–12 GB VRAM** | 100 GB (models) | **7–14B AWQ.** ADR-014. Shares GPU 0 with Triton (~15–18 GB total). **Never preempted.** |
| **vLLM — teacher / batch (GPU 1)** | 0–1 | 4 | 16 GB + **~17–20 GB VRAM** | 100 GB (models) | Qwen3-30B-A3B Q4 class, **offline teacher only** (distillation, causal fallback, pre-annotation). Shares GPU 1 with fine-tuning + GNN training. Preemptible; owns the card. |
| FastAPI + SPA + Caddy | 3 | 1 | 2 GB | 10 GB | Low request volume. |
| Superset | 2 | 1 | 4 GB | 10 GB | Metadata DB inside main Postgres (separate database). |
| MLflow | 1 | 0.5 | 1 GB | — | Backend store = Postgres DB; artifacts = MinIO. Zero extra state. |
| Label Studio | 1 | 1 | 2 GB | — | DB = Postgres; file storage = MinIO. Zero extra state. |
| Observability (Prometheus, Grafana, Loki, Alertmanager, exporters) | 6 | 2 | 6 GB | 200 GB | 30-day metrics, 30-day logs. |
| **Total steady state** | ~25 | **~20 vCPU** | **~75 GB** | **~3.5 TB** | Bursts to ~28 vCPU / ~100 GB during Spark backfills. |

Rule of thumb applied: buy 2× the steady-state RAM and 3× the projected 3-year disk. RAM and disks are
the cheap insurance; student time debugging OOM kills is the expensive part.

### 1.2 Tier A — minimum dev environment (project month 0–6, before pilot)

**Option A1 (preferred): one beefy server + one GPU workstation.**

```
chip-dev-01 (rack or tower server)
  CPU:  16C/32T (AMD Ryzen 9 7950X, or used EPYC 7302/7402, or Xeon Silver 4314)
  RAM:  128 GB (ECC strongly preferred; mandatory if used EPYC/Xeon route)
  Disk: 2 × 4 TB NVMe (mdraid RAID1 or ZFS mirror) — OS + all container volumes
        2 × 8 TB SATA HDD (RAID1) — MinIO bulk + local backup repo
  NIC:  2 × 1 GbE (10 GbE unnecessary at this scale)
  Runs: the ENTIRE stack (one compose environment) + staging copies

chip-gpu-01 (workstation, doubles as ML dev box)
  CPU:  Ryzen 9 7900X / 7950X
  RAM:  64–128 GB
  GPU:  *** 2 × 24 GB (ADR-014) ***  — 2 × RTX 3090 (used, budget path) or 2 × RTX 4090
        GPU 0 = RESIDENT SERVING   (Triton: XLM-R NER/RE/causal + BGE-M3 · vLLM: 7–14B RAG LLM)
                                    ~15–18 GB. Never preempted. Keeps the dashboard live.
        GPU 1 = BATCH / TRAINING   (LLM teacher 30B-Q4 · XLM-R fine-tuning · historical
                                    enrichment backfill · GNN training). Owns the card. May OOM freely.
  PSU:  1200 W+ (two cards)
  Disk: 2 TB NVMe + 4 TB HDD
  Runs: the two GPU roles above, nothing else
```

**Option A2 (if only one machine is fundable now):** put the cards inside chip-dev-01 (tower chassis,
1200 W+ PSU). Acceptable for ~6 months; split the GPU box out at pilot.

Indicative cost (mid-2026, ex-import duties, volatile — treat as ±30%): chip-dev-01 ≈ USD 4,000–5,500;
chip-gpu-01 ≈ USD 4,000–5,500 (2 × used 3090) to USD 6,000–7,500 (2 × 4090). Both fit typical NRPU
equipment lines.

**GPU sizing (per ADR-014 — this supersedes the single-card assumption in earlier drafts):**

| Workload | VRAM | Device |
|---|---|---|
| XLM-R NER + RE + causal (Triton, batched) | ~4 GB | **GPU 0** — resident |
| BGE-M3 embedder, 1024-dim (ADR-011) | ~3 GB | **GPU 0** — resident |
| Graph-RAG LLM, interactive, 7–14B AWQ | ~8–12 GB | **GPU 0** — resident |
| **GPU 0 total** | **~15–18 GB** | fits one 24 GB card with headroom |
| LLM teacher (30–32B class, Q4/AWQ) — distillation, causal fallback, pre-annotation | 17–20 GB | **GPU 1** |
| XLM-R fine-tuning | 12–24 GB | **GPU 1** |
| Historical enrichment backfill / GNN training | varies, saturates | **GPU 1** |

- **Why 2 × 24 GB and not 1 × 48 GB** (they look equivalent — 48 GB either way — and are not): with two
  physical devices, **a student's fine-tune OOMing on GPU 1 cannot touch the serving stack on GPU 0.** On a
  single 48 GB card it can, and you would need MIG/MPS or a real scheduler to prevent it — which a rotating
  student team will not maintain. **Failure isolation, not VRAM, is the argument.** "A training job crashed the
  stakeholder dashboard" is exactly the failure this project cannot afford, and it is the failure ADR-004's whole
  premise exists to prevent.
- Isolation is enforced by `CUDA_VISIBLE_DEVICES`. **No GPU scheduler, no preemption policy, no runbook** — the
  contention was the problem, and the split removes it.
- **24 GB is the number that matters — buy card *count* over per-card capability.** Used 3090s are plentiful.
- Buying the hosted agentic parser (**ADR-012**) *removes* a GPU workload — no local VLM is needed for document
  parsing. That is part of why two cards suffice rather than three.
- **If only one card is fundable at month 0:** time-slice as an interim (encoders + BGE-M3 resident ≈ 7 GB; a
  7–8B RAG model by day; LLM batch nightly). This breaks the moment the historical enrichment backfill runs
  *while* the dashboard must be live — which is precisely when you demo. **Buy the second card before Phase 2.**
- Do **NOT** buy A100/H100-class hardware. Nothing in this project needs it; it destroys the budget and the power
  envelope.

### 1.3 Tier B — pilot/production (stakeholder demos, months 6–24)

Three machines, three roles. Separation rationale: (1) the GPU box has different power/thermal/driver
churn and must be rebootable without touching prod data; (2) the ops box watches and backs up the core
box — the monitor must not die with the monitored.

```
+---------------------------------------------------------------------------------+
|  NUCES server room (UPS-backed, see §1.5)                                       |
|                                                                                 |
|  chip-core-01  (prod core)              chip-gpu-01  (GPU node)                 |
|  24–32C / 128–192 GB ECC                16C / 64–128 GB                         |
|  2×4TB NVMe RAID1 (hot data)            2 × 24 GB GPU (4090/3090) — ADR-014     |
|  2×8TB HDD RAID1 (MinIO + scratch)      2 TB NVMe                               |
|  Runs: Postgres, MinIO, Neo4j,          GPU0: Triton (XLM-R + BGE-M3) + vLLM    |
|  Kafka+Apicurio, Dagster, Spark jobs,         (7-14B RAG) — RESIDENT, never     |
|  FastAPI, SPA, Superset, MLflow,              preempted                         |
|  Label Studio, Caddy                    GPU1: LLM teacher (30B-Q4), fine-tuning,|
|                                               backfill, GNN — BATCH, owns card  |
|                                                                                 |
|  chip-ops-01  (ops + staging; can be a repurposed older box)                    |
|  8C / 32–64 GB / 2×4TB HDD RAID1 + 500 GB NVMe                                  |
|  Runs: Prometheus/Grafana/Loki/Alertmanager, pgBackRest repo (primary),         |
|  WireGuard, staging compose environment (scaled-down full stack)                |
+---------------------------------------------------------------------------------+
```

Plus: 1 managed 1 GbE switch, 1 online double-conversion UPS sized for ~1.5 kW / 15+ min runtime
(mandatory — see §1.5), a lockable rack or cabinet, and 2 × 8 TB external USB HDDs for off-site
rotation (§6.5). Indicative additional cost: core-01 ≈ USD 5,000–7,000 (or promote chip-dev-01 into
this role and buy dev replacement later); ops-01 ≈ USD 800–1,500 or repurposed; UPS + rack + drives
≈ USD 1,500–2,500.

### 1.4 Tier C — growth path (months 24+, only if triggers fire)

Do not pre-buy. Grow along these axes, in this order, each gated on an observed constraint:

1. **RAM/disk in place** (core-01 to 256 GB / add NVMe) — first response to pressure; cheapest.
2. **Second core node (chip-core-02)** — same spec as core-01. Trigger: k3s migration decided (§3.3)
   or a hard requirement for < 5 min recovery of the dashboard during monsoon season. With core-01 +
   core-02 + ops-01 you have the 3 nodes for a k3s cluster with etcd quorum.
3. **Second GPU box or second card** — trigger: fine-tuning queue regularly blocks the resident
   LLM/inference serving, measured over a month.
4. **Second-site replica** (another NUCES campus/building): a single mid-range box receiving pgBackRest
   repo + MinIO mirror + weekly Neo4j dumps. Trigger: platform declared "operational" to government
   stakeholders. This is a backup target first, a DR site only if ever needed.

Never on the growth path: SAN/NAS appliances, VMware/Proxmox clusters (containers on bare Ubuntu are
enough), Kubernetes distributions other than k3s, GPUs above 24 GB class.

### 1.5 Power, environment, and physical realities (Pakistan-specific, do not skip)

- **UPS is not optional.** Load-shedding and voltage sag will otherwise corrupt Postgres and mdraid.
  Online double-conversion UPS, NUT (`nut` daemon) on all hosts for monitored clean shutdown at
  ~40% battery. Test the shutdown path quarterly (drill D-3, §9.3).
- Generator changeover at NUCES: confirm the server room circuit is on it (Open question OQ-2).
- Dust and heat: positive-pressure filtered rack fans if the room is not properly conditioned; GPU box
  thermals logged in Prometheus (nvidia exporter) with alerts at sustained > 83 °C.
- Physical security: lockable rack; the off-site USB drives are encrypted (§6.5) precisely because
  physical custody by students is assumed imperfect.

---

## 2. Environment strategy

### 2.1 The scheme: prod + staging + laptop dev + ephemeral CI

Four environments, no more. On limited hardware, "dev/staging/prod parity" means *same compose files,
different overrides* — not three copies of the hardware.

| Env | Where | Data | Purpose | Compose project name |
|---|---|---|---|---|
| **prod** | chip-core-01 (+ gpu-01) | Real | Stakeholder-facing. Deploys only via release procedure (§8.5). | `chip-prod` |
| **staging** | chip-ops-01 | Weekly anonymized/sampled restore from prod backups (doubles as the restore drill) | Pre-release validation, migration rehearsal, demo rehearsal | `chip-staging` |
| **dev (per student)** | Student laptop / chip-dev-01 | Seeded sample data (§2.3) | Daily development | `chip-dev-<username>` |
| **CI** | GitHub Actions runner | Seeded sample data | Automated tests | ephemeral |

Rules:
- Staging is a *scaled-down full stack* (all services, small resource limits), not a partial stack.
  If it doesn't run in staging, it doesn't deploy.
- No shared long-lived "dev server" environment that everyone mutates — that pattern rots in one
  semester. Students who need more power than their laptop get an *ephemeral personal* compose project
  on chip-dev-01 (`chip-dev-ahmed`), namespaced by compose project name and port offset, torn down by a
  weekly cleanup script that deletes projects idle > 14 days (with Discord warning 48 h before).
- Prod data never leaves prod except via the backup/anonymization path. Health surveillance data,
  even district-aggregated, follows the data-sharing agreements — dev/staging use the seeded corpus
  and public historical data only, unless the data-governance doc explicitly clears a dataset.

### 2.2 Student laptop dev experience (onboarding target: < 1 day to running stack, < 3 days to first PR)

Assume a 16 GB RAM laptop, Windows (WSL2) or Linux, possibly poor bandwidth.

- **Devcontainer** in the monorepo root (`.devcontainer/devcontainer.json`): Python toolchain (uv),
  pre-commit hooks, docker-outside-of-docker. VS Code "Reopen in container" is the only setup step
  besides installing Docker Desktop + cloning.
- **`compose.dev.yaml` subset** budgeted to fit in ~6–8 GB RAM: Postgres, MinIO, Kafka (single broker,
  512 MB heap), Karapace, Dagster, FastAPI, dashboard dev server. Neo4j, Superset, Label Studio, MLflow,
  observability are opt-in compose profiles (`--profile graph`, `--profile analytics`, `--profile ml`).
  LLM work on laptops uses Ollama with a small quantized model, or points at the shared vLLM endpoint
  on chip-gpu-01 over WireGuard.
- **Seeded sample data**: `make seed` loads a versioned sample pack from a public MinIO bucket / GitHub
  release: ~2k news articles (Urdu + English, licensing-cleared subset), 12 months of public PMD weather
  for 10 districts, synthetic IDSR-shaped health series, district boundary shapefiles, a pre-built mini
  knowledge graph dump. The pack is regenerated by a Dagster job monthly so it never drifts from schema.
- **Makefile facade** (the entire daily interface): `make up`, `make up-min`, `make down`, `make seed`,
  `make test`, `make lint`, `make logs s=<service>`. Nobody types raw `docker compose` in week one.
- **Day-one checklist** lives at `docs/onboarding/day-one.md` (§9.1) and ends with the student running
  one end-to-end pipeline (news article → NLP → Postgres → dashboard) locally. That is the definition
  of "onboarded to the stack."

---

## 3. Container platform

### 3.1 Docker Compose organization

One logical stack per environment, **split into per-service-group files joined with Compose `include:`**
(Compose v2.20+). This gives students small readable files, while `docker compose ps` still shows one
coherent stack. Layout in the monorepo:

```
infra/
├── compose/
│   ├── compose.yaml                 # top-level: `include:` of groups below + shared networks
│   ├── groups/
│   │   ├── data.yaml                # postgres, minio, neo4j
│   │   ├── streaming.yaml           # kafka, karapace
│   │   ├── orchestration.yaml       # dagster-webserver, dagster-daemon, code locations
│   │   ├── ml.yaml                  # mlflow, label-studio, nlp-workers
│   │   ├── llm.yaml                 # vllm/ollama  (deployed only on chip-gpu-01)
│   │   ├── apps.yaml                # fastapi, dashboard, superset
│   │   ├── observability.yaml       # prometheus, grafana, loki, alertmanager, exporters
│   │   └── edge.yaml                # caddy, wireguard (wg-easy)
│   ├── overrides/
│   │   ├── compose.prod.yaml        # resource limits, restart: always, prod volumes/ports
│   │   ├── compose.staging.yaml     # small limits, staging ports, staging hostnames
│   │   └── compose.dev.yaml         # dev subset, profiles, bind mounts for hot reload
│   ├── env/
│   │   ├── prod.env.sops            # SOPS-encrypted (§6.6)
│   │   ├── staging.env.sops
│   │   └── dev.env.example          # committed plaintext defaults, no secrets
│   └── Makefile
├── ansible/                          # §3.4
├── scripts/                          # deploy.sh, backup wrappers, seed, cleanup
└── registry/                         # image build metadata, Renovate config
```

Conventions (enforced by CI lint on the compose files):
- Every service: pinned image tag **by digest** in prod overrides, healthcheck, explicit
  `mem_limit`/`cpus` in prod/staging, log driver limits (`max-size: 10m`, `max-file: 5`), named
  volumes only (no anonymous volumes), `restart: unless-stopped` (prod: `always`).
- Two docker networks: `chip-internal` (everything) and `chip-edge` (Caddy + the two services it
  fronts). Only Caddy and WireGuard publish host ports. No other `ports:` in prod overrides —
  admin UIs are reached via WireGuard + internal DNS names (§4.3).
- GPU host runs its own small stack (`llm.yaml` + node exporters); it joins the core box via the
  normal LAN, not an overlay — plain TCP with TLS where the service supports it. No Swarm, no
  overlay networking; two hosts do not need it.

### 3.2 Why Compose (restating the locked decision with teeth)

Compose is the platform until a trigger in §3.3 fires. A student can read the entire prod topology in
eight YAML files. `docker compose up -d` is the whole scheduler. Debugging is `docker logs`. That is
worth more than every Kubernetes feature at this scale.

### 3.3 Explicit k3s migration triggers

Migrate to k3s (specifically k3s — single-binary, SQLite-or-etcd, boring) when **any two** of the
following are true, or when trigger T1 alone is true. Review the triggers at the quarterly ops review;
record the assessment in an ADR each time. Do not migrate before project month 12 regardless.

- **T1.** A government stakeholder SLA requires zero-downtime deploys or automatic failover of the
  dashboard/API (Compose cannot do rolling restarts across hosts).
- **T2.** We operate ≥ 4 hosts running application containers (excluding the ops box) — Compose
  placement-by-SSH stops being tractable.
- **T3.** GPU jobs need real scheduling/queueing across ≥ 2 GPU hosts (bin-packing fine-tunes against
  the resident vLLM server by hand has failed for a full quarter).
- **T4.** The team has run 2 consecutive quarters with clean ops reviews (drills done, alerts green,
  restores rehearsed) — i.e., we have earned the complexity budget.
- **T5.** A concrete need for per-namespace multi-tenancy (e.g., a partner institution deploys their
  own connector workloads on our hardware).

Pre-commitments that keep the migration cheap when it happens: images are already OCI-standard and
registry-hosted; config is env-var based (12-factor); no compose-only features (no `depends_on`
ordering logic in app code — services retry connections themselves); Caddy config translates to an
Ingress; volumes are named and documented. Target shape at migration: k3s on core-01 + core-02 +
ops-01, workloads as plain Deployments + a couple of StatefulSets, manifests rendered with Kustomize
(not Helm charts of our own — consume upstream Helm only where unavoidable).

### 3.4 Host provisioning: Ansible

All three hosts are Ubuntu LTS (24.04 now; upgrade only to even-numbered LTS, one release behind
latest, during summer break). One Ansible repo directory (`infra/ansible/`) with roles:

- `base`: users + SSH keys (from git-managed list), UFW, fail2ban, unattended-upgrades (security only),
  chrony, NUT (UPS), node_exporter, sysctl, journald limits.
- `docker`: Docker Engine pinned to a tested minor version, daemon.json (log limits, live-restore).
- `storage`: mdraid/ZFS layout, mount points, SMART monitoring (smartd → Prometheus).
- `backup`: systemd timers for pgBackRest, Neo4j dump, MinIO mirror, off-site sync (§6).
- `deploy`: lays down `/opt/chip/`, the age host key (manually placed once, never via Ansible),
  the `chipctl` deploy script.

Run from any student laptop over WireGuard: `ansible-playbook site.yaml --limit chip-core-01`.
Rule: **nobody apt-installs or edits config on a host by hand.** Hotfix in an incident if you must,
then port it into Ansible within 48 hours (the ops review checks `ansible-playbook --check` is clean).

### 3.5 Image registry: GHCR primary, no Harbor

**Decision: GitHub Container Registry (ghcr.io), private, under the PCN GitHub org.**

- Harbor is a multi-container stateful service with its own Postgres, Redis, and upgrade treadmill —
  exactly the kind of ops burden principle 4 forbids for a student team. Rejected.
- GHCR needs only outbound HTTPS (works through university NAT; no firewall change), integrates with
  GitHub Actions natively, is free at our volume, and images are pulled with a fine-scoped PAT / GitHub
  App token stored via SOPS.
- **Offline resilience** (GHCR outage or internet outage must not block an emergency redeploy): the
  release workflow also produces `docker save` bundles of every release's images, stored in MinIO
  (`chip-artifacts/releases/vX.Y.Z/images.tar.zst`) and on the off-site drives. `chipctl deploy --offline vX.Y.Z`
  loads from the bundle. Additionally run `registry:2` as a pull-through cache on chip-ops-01 (one
  container, cache-only, zero backup obligation — it is disposable).

---

## 4. Networking & exposure

### 4.1 University network reality and the topology

Assume: hosts sit behind university NAT; inbound requires a firewall change request to NUCES IT
(slow, weeks); outbound HTTPS generally open; we may get one public IP or DNAT rule if we ask early.

**File the firewall request in project month 1** (it is the longest lead-time item): one DNAT —
public IP/hostname, TCP 443 → chip-core-01 (Caddy), plus UDP 51820 → chip-ops-01 (WireGuard).
Nothing else is ever exposed. If NUCES IT refuses UDP 51820, WireGuard can run on UDP 443 from the
second public mapping, or fall back to SSH-tunnel-only admin access (documented in RB-05).

```
                        Internet
                            │
              ┌─────────────┴──────────────┐
              │  NUCES firewall / NAT      │
              │  DNAT: 443→core-01:443     │
              │  DNAT: 51820/udp→ops-01    │
              └─────────────┬──────────────┘
       ┌────────────────────┼─────────────────────────┐
       │            campus LAN / server VLAN          │
       │                                              │
  chip-core-01          chip-gpu-01              chip-ops-01
  ┌──────────┐          ┌──────────┐             ┌───────────┐
  │ Caddy:443│◄─ public │ vLLM     │             │ WireGuard │◄─ admins & partners
  │  ├ dashboard        │ NLP-GPU  │             │ Prom/Graf │
  │  └ /api  │          └──────────┘             │ Loki/AM   │
  │ chip-edge net                                │ pgBackRest│
  │ ── chip-internal net ──────────────────────► │ staging   │
  │ postgres, minio, neo4j, kafka, dagster, ...  └───────────┘
  └──────────┘
  Admin UIs (Dagster, Grafana, MinIO console, MLflow,
  Label Studio, Superset*) bind to chip-internal only;
  reached via WireGuard + internal Caddy vhosts.
  (*Superset may later be exposed publicly if stakeholders need it — separate decision + review.)
```

### 4.2 Reverse proxy: Caddy (decision)

**Caddy**, not Traefik, not nginx.

- Automatic TLS issuance/renewal with zero cron jobs and zero certbot glue — the classic student-team
  failure ("cert expired during the demo") is designed out.
- A Caddyfile is readable by a first-semester student; Traefik's label-based indirection and nginx's
  config sprawl are not.
- Handles both public vhosts and internal (WireGuard-only) vhosts with internal CA (`tls internal`)
  for admin UIs.
- Basic rate limiting, security headers, and access logs (to Loki) configured once in the Caddyfile,
  committed to git.

### 4.3 DNS and TLS

- **Public domain**: acquire a project domain the team controls (e.g., `chip-pk.org`) rather than
  waiting on `*.nuces.edu.pk` delegation; also request a university CNAME later for legitimacy
  (`chip.nuces.edu.pk → dashboard.chip-pk.org`). DNS hosted at a registrar/provider with an API
  supported by Caddy for **DNS-01 ACME challenges** (e.g., Cloudflare in DNS-only mode — DNS hosting
  is not a "managed service" running our workload, and DNS-01 means cert issuance never depends on
  inbound port 80/443 reachability, which the university firewall may complicate).
- **TLS: Let's Encrypt via DNS-01** (decision). University-issued certs are the fallback only if
  policy forces it (they bring manual renewal — the thing we are eliminating). Internal admin vhosts
  use Caddy's internal CA; the WireGuard client bundle includes the root cert.
- **Internal DNS**: dnsmasq on chip-ops-01 serving `*.chip.internal` (e.g., `dagster.chip.internal`),
  pushed as the DNS server in WireGuard client configs; hosts get static entries via Ansible. No mDNS,
  no editing hosts files by hand.

### 4.4 Partner/stakeholder access (NIH / NDMA / MoCC)

Tiered, simplest-that-works:

1. **Dashboard + API over public HTTPS with authentication** (OIDC/session auth per serving-subsystem
   doc) — this is the default for government users; do not make ministries install VPN clients to view
   a dashboard. Add per-institution IP allowlisting at Caddy *if and when* a partner's security office
   requests it (government offices usually have static egress IPs).
2. **WireGuard** for: platform admins (students/faculty), and any partner integration that needs to
   reach non-public services (e.g., bulk data pulls). Managed with `wg-easy` (web UI on internal net)
   — one config file per person, named `firstname-device`, revoked on offboarding (RB-10).
   Plain self-hosted WireGuard, not Tailscale — Tailscale's control plane is a third-party dependency
   that conflicts with the "full control" constraint; revisit headscale only if peer count exceeds ~30.
3. **No SSH exposed to the internet.** SSH only over WireGuard or from campus LAN.

### 4.5 Hardening checklist (applied by Ansible `base` role; audited quarterly, drill D-4)

- [ ] SSH: key-only, no root login, no password auth; keys listed in git (`infra/ansible/files/ssh-keys/`)
- [ ] UFW default-deny inbound; allow 443 (core-01), 51820/udp (ops-01), SSH from LAN/WG subnets only
- [ ] fail2ban on sshd; Caddy rate limits on `/api` and auth endpoints
- [ ] unattended-upgrades: security patches auto; reboot-required flag alerts to Discord, reboot done manually in maintenance window
- [ ] Docker: no container runs `privileged`; no host network mode except node_exporter; internal services never publish host ports; `no-new-privileges` default
- [ ] All admin UIs behind WireGuard AND their own auth (defense in depth — Dagster/Grafana/MinIO console all have auth enabled, no anonymous access)
- [ ] Postgres/Neo4j/Kafka/MinIO listen on the docker internal network only; strong generated passwords via SOPS; Postgres `pg_hba` scoped to container subnets
- [ ] Secrets never in images, compose files, or logs (CI secret-scan via gitleaks on every PR)
- [ ] NTP (chrony) on all hosts — TLS, Kafka, and forensic timelines all need sane clocks
- [ ] Off-site/backup media encrypted (age / LUKS); laptops with prod WireGuard access require full-disk encryption
- [ ] Quarterly: review WireGuard peer list, SSH key list, GitHub org members against the current team roster
- [ ] Annual: dependency of last resort — verify the sealed envelope (§6.6) contents are current

---

## 5. Kafka / Spark / Dagster deployment specifics

### 5.1 Kafka: single-broker KRaft at every tier until Tier C

**Decision: one broker, KRaft combined mode (broker+controller in one process), at dev and pilot.
3-broker KRaft only at Tier C and only if trigger T1 (§3.3) or a durability requirement forces it.**

Rationale: with tens of GB/year, Kafka here is a decoupling bus and replay buffer, not the system of
record. Durability comes from the architecture (raw docs in MinIO, canonical facts in Postgres —
topics are re-derivable, §6.4), so replication factor 1 is acceptable; a broker loss costs at most the
retention window of in-flight data, which connectors re-fetch.

Concrete settings (pilot):
- `KAFKA_HEAP_OPTS=-Xmx2g`, single volume on NVMe, `num.partitions` default 3 (allows consumer
  parallelism later without repartitioning pain), RF=1, `min.insync.replicas=1`.
- Retention: raw-source topics 14 days; enriched/derived topics 7 days; small reference topics
  (district codes, vocabularies) log-compacted. Disk stays under ~50 GB.
- Topic creation is **declarative**: a versioned `topics.yaml` in the monorepo applied by an idempotent
  Dagster asset/job (also runnable as `chipctl kafka-apply`) — never `kafka-topics.sh` by hand.
  This file is also the rebuild script after a broker loss (RB-07).
- Schema registry (**Apicurio, per ADR-009** — supersedes the Karapace assumption; single container,
  schemas persisted in the **main Postgres** so the registry is covered by the normal pgBackRest backup and
  needs **no** separate compacted-topic export job). Karapace-specific rows elsewhere in this doc (hardware
  table, §6.2 "Karapace schemas → nightly JSON export") are superseded accordingly.

### 5.2 Spark: local-mode job containers, no standing cluster

**Decision: no resident Spark cluster at any tier below Tier C.** Spark is used ONLY by the news NLP
enrichment pipeline and backfills. Each run is a container from one pinned `apache/spark`-based image
(project image adds Python deps), launched by Dagster with `spark-submit --master local[*]`, capped at
8 CPUs / 16 GB. On our data volumes, a single node in local mode outperforms the operational cost of a
standing master/worker cluster by an enormous margin.

- Escalation step 1 (only if a backfill demonstrably exceeds one container): Spark standalone —
  one master + one worker container on core-01, plus optionally a worker on gpu-01. Still Compose.
- Escalation step 2: Spark-on-k8s — only after the k3s migration, never before, and only if step 1
  has actually been outgrown. Do not skip steps.
- Keep the enrichment logic in plain PySpark jobs that also run under `pytest` with a local
  SparkSession — the deployment mode must never leak into pipeline code.

### 5.3 Dagster deployment layout

Dagster is the single pane of glass for everything scheduled — connectors, pipelines, model runs,
and also *operational* jobs (sample-pack regeneration, schema-registry export, freshness checks).
Backups are the deliberate exception: they run from host systemd timers because the backup system
must not depend on the platform it protects (§6).

Containers (all in `orchestration.yaml`):
- `dagster-webserver` — UI, internal-only, `dagster.chip.internal`.
- `dagster-daemon` — schedules, sensors, run queue (`run_coordinator: QueuedRunCoordinator`,
  max 4 concurrent runs; tag-based limits: `gpu: 1`, `spark: 1`).
- Three code-location gRPC servers matching monorepo layout: `chip-connectors`, `chip-pipelines`,
  `chip-ml`. Each is its own image, so teams deploy independently and one bad import cannot take down
  another team's schedules.
- **Run launcher: `DockerRunLauncher`** — every run executes in a fresh container of the code
  location's image. Crashes are isolated, resource-capped, and logs are per-run. (On k3s this maps
  1:1 to `K8sRunLauncher` — another cheap migration.)
- Storage: run/event/schedule storage in the main Postgres (`dagster` database); compute logs to
  MinIO. Dagster itself therefore carries zero backup obligation beyond Postgres.
- GPU jobs (fine-tunes, batch LLM inference) run on chip-gpu-01 via a small Docker socket-over-TLS
  connection from the run launcher (or, simpler and preferred: GPU jobs are `dagster-pipes`-invoked
  scripts triggered over SSH to gpu-01 — decide during implementation, record as ADR).

---

## 6. Data protection

### 6.1 What is canonical vs. derived (the backup surface)

| Store | Role | Rebuildable from | Backup obligation |
|---|---|---|---|
| PostgreSQL | Canonical: harmonized facts, features, app state, Dagster/MLflow/Label Studio/Superset metadata | Nothing — this is the crown jewel | **Highest** (PITR) |
| MinIO | Canonical: raw fetched documents, **cached agentic-parse outputs (ADR-012)**, model artifacts, exports, image bundles | **NOT re-derivable.** Source sites rot; and re-parsing would re-bill the agentic parser *and break replay determinism*. Treat as canonical. | **High** (versioning + object-lock + mirror) |
| Neo4j (CHKG) | **Derived**: built from Postgres entities/relations by the KG pipeline | Postgres + pipeline code | Medium (weekly dump as a *convenience* to avoid multi-hour rebuilds) |
| Kafka topics | Derived/transit | MinIO raw docs + connectors re-fetch | **None** (RB-07 rebuild runbook instead) |
| **Apicurio schemas (ADR-009)** | **Stored in the main Postgres** | — | **Covered by pgBackRest automatically. No separate export job.** (Karapace would have needed one — it keeps schemas in a compacted Kafka topic. This is precisely why ADR-009 chose Apicurio: principle 4, *fewest stateful services*.) |
| Prometheus/Loki | Observability history | Nothing, and we accept losing it | None (30-day retention, best-effort) |
| Grafana dashboards, Caddy config, topics.yaml, all infra | Config | **Git** | Git is the backup |

Design rule enforced in review: **no pipeline may treat a Kafka topic or Neo4j as its only source of
truth.** If a new feature needs durable state, it goes in Postgres or MinIO.

### 6.2 Backup matrix

All backup jobs run from **host systemd timers** (Ansible-managed), write success/failure + timestamp
to a Prometheus textfile collector (node_exporter), and ping a dead-man's-switch check (§7.3).

| What | Tool | Schedule | Retention | RPO / RTO target | Destination(s) |
|---|---|---|---|---|---|
| Postgres | **pgBackRest**: full weekly (Sun 01:00), diff daily (01:00), WAL archiving continuous | continuous | 4 fulls, 14 diffs, WAL to cover | RPO ≤ 5 min / RTO ≤ 1 h | repo1: chip-ops-01 (primary repo); repo2: MinIO S3 bucket on core-01 HDDs; weekly copy to off-site (§6.5) |
| MinIO buckets | Bucket **versioning ON** + `mc mirror --remove` nightly 02:00 | nightly | Mirror + 30-day noncurrent-version lifecycle | RPO ≤ 24 h / RTO ≤ 2 h | chip-ops-01 HDD; off-site weekly |
| Neo4j | `neo4j-admin database dump` (offline — see §6.3) | weekly Sun 02:30 | 4 dumps | RPO ≤ 7 d (acceptable: derived) / RTO ≤ 1 h from dump, ≤ 1 day full rebuild | MinIO + off-site |
| ~~Karapace schemas~~ **Apicurio schemas** | ~~Dagster job → JSON export~~ **None needed — they live in Postgres (ADR-009), so pgBackRest already covers them.** | — | — | Same as Postgres (RPO ≤ 5 min) | pgBackRest repos |
| Host configs (`/etc`, docker volumes list, package state) | etckeeper + Ansible is source of truth | on change | git history | — | git (GitHub + local mirror on ops-01) |
| Git monorepo itself | GitHub + `git clone --mirror` cron on ops-01 | nightly | forever | RPO ≤ 24 h if GitHub lost | ops-01 + off-site |
| Release image bundles | CI `docker save` per release | per release | last 6 releases | — | MinIO + off-site |
| GPU box model cache | none — re-download / re-train | — | — | — | (weights are in MLflow/MinIO already) |

### 6.3 Neo4j Community: the offline-dump reality, stated plainly

Neo4j Community has **no online backup** — `neo4j-admin database dump` requires the database stopped.
Consequence and mitigation, in order:

1. Weekly dump window **Sunday 02:30, ~10–20 min downtime** for the graph. The API degrades
   gracefully: dashboard endpoints backed by Postgres stay up; graph-RAG/graph queries return a
   "maintenance" response. This window is documented in the stakeholder-facing SLO ("graph features
   may be briefly unavailable Sunday nights").
2. Because the CHKG is derived, the true disaster recovery is `dagster job launch --job rebuild_chkg`
   from Postgres — rehearsed in the quarterly drill (D-2). The weekly dump exists to make RTO 1 hour
   instead of up-to-a-day.
3. Revisit if the downtime becomes unacceptable: options are (a) dump from a throwaway copy of the
   volume (stop, snapshot/cp, start, dump from copy — reduces downtime to ~1 min at the cost of disk),
   which we adopt as the default refinement once volumes are on ZFS/LVM; (b) Memgraph or Neo4j
   Enterprise academic licensing — separate ADR, not assumed.

### 6.4 Kafka: explicitly NOT backed up

In-flight topic data is a transit buffer. Loss scenario and recovery: recreate topics from
`topics.yaml` (idempotent), restart connectors — they re-fetch from sources and/or replay raw
documents from MinIO through the enrichment pipeline. RB-07 walks this end-to-end and drill D-2
includes it annually. The 14-day retention means the worst case is re-processing two weeks of
already-persisted raw data.

### 6.5 Off-site strategy under the "full control" constraint (3-2-1)

3 copies, 2 media, 1 off-site — without clouds:

- **Copy 1 (live)**: the services themselves on core-01.
- **Copy 2 (on-site, different machine/media)**: pgBackRest repo + MinIO mirror + dumps on chip-ops-01 HDDs.
- **Copy 3 (off-site)**, two mechanisms, adopt A immediately and B when Tier C's second site exists:
  - **A. Rotating encrypted external drives**: two 8 TB USB HDDs, `restic` repository (encrypted,
    deduplicated, integrity-checkable — one tool for the whole off-site copy) refreshed weekly
    (Mon 03:00 auto-run to whichever drive is docked), drives swapped weekly and the off-duty drive
    stored **off-campus with the PI or in a second building's locked cabinet** — not in the server
    room. Restic passphrase in the sealed envelope (§6.6). Drive-swap is a named rota duty (§9.2).
  - **B. Second-campus replica box** (Tier C): receives pgBackRest repo2, MinIO mirror, dumps over
    WireGuard site-to-site. Supersedes drive rotation for RPO but keep one drive rotation monthly as
    the air-gapped ransomware hedge.
- **Ransomware posture**: MinIO versioning + restic append-mostly + the weekly air-gapped drive mean
  an attacker with root on core-01 cannot silently destroy all copies. The backup user on ops-01 has
  no SSH access *to* core-01 (backups are pulled/pushed via scoped credentials, not root trust both ways).

### 6.6 Restore drills, retention, verification

- **Quarterly restore drill (D-2)** rotating through: (Q1) Postgres PITR to staging from repo2;
  (Q2) MinIO bucket restore + Neo4j dump load; (Q3) full CHKG rebuild from Postgres + Kafka topic
  recreation; (Q4) **bare-metal fire drill**: bring up the core stack on chip-dev-01 from off-site
  drive + git alone, following runbooks only, executed by the *newest* student on the team with the
  ops lead observing silently. Every drill produces a timed report in `docs/drills/`; runbook gaps
  found are fixed within the week.
- pgBackRest `--repo verify` weekly (cron); restic `check --read-data-subset=5%` weekly.
- Staging's weekly refresh from prod backups (§2.1) is itself a continuous restore test.

### 6.7 Secrets management: SOPS + age (decision)

**SOPS + age. Not Vault.** Vault is a highly-available stateful service with unsealing ceremonies and
an operational learning curve — precisely wrong for this team. SOPS gives encrypted-at-rest secrets
*inside the git repo*, diffable, with zero servers.

- One age keypair per environment (`prod`, `staging`) + one per admin human. `.sops.yaml` maps
  `infra/compose/env/*.env.sops` and any `secrets/*.yaml` to the right recipients.
- Host private keys live only at `/etc/chip/age.key` on the respective host (mode 0400, placed
  manually at provisioning, never in Ansible vars or git).
- `chipctl deploy` decrypts env files to a tmpfs path consumed by `docker compose --env-file`,
  and shreds them after `up` completes.
- Human credential store (things that aren't machine env vars: registrar login, GHCR org admin,
  Grafana admin, university IT ticket portal): a **KeePassXC** database in a private git repo,
  master passphrase known to PI + 2 leads.
- **Sealed-envelope escrow** (bus factor zero-point): printed copy of — prod/staging age private keys,
  restic passphrase, KeePassXC master passphrase, domain registrar recovery codes — in a sealed,
  dated envelope in the PI's office safe / departmental safe. Verified and re-sealed annually
  (hardening checklist).
- Rotation: on every offboarding of a key-holding member (RB-10) and annually otherwise; SOPS makes
  re-encryption to a new recipient set a one-command operation (drill D-4 rehearses it).

---

## 7. Observability

### 7.1 Stack: Prometheus + Alertmanager + Grafana + Loki (decision)

Justification against "something simpler" (e.g., Uptime Kuma alone, or Netdata): this stack is the
industry lingua franca — every exporter we need exists off-the-shelf, every question a student has is
answered on the first page of search results, and dashboards/alert rules are provisioned as code in
git (Grafana provisioning + Prometheus rule files), which no simpler tool does as cleanly. It runs on
chip-ops-01 (the watcher lives apart from the watched) at ~6 GB RAM. We deliberately do **not** add
tracing (Tempo/Jaeger) — request volumes don't justify it; structured logs with request IDs suffice.

Components: prometheus (30 d retention), alertmanager, grafana, loki + promtail/alloy (Docker log
scraping on all three hosts, 30 d), node_exporter + cAdvisor + smartd + NUT exporter on all hosts,
nvidia-dcgm-exporter on gpu-01, blackbox_exporter (probes the public dashboard URL from ops-01,
i.e., through the real edge path), postgres_exporter, kafka JMX exporter, minio's native metrics.

### 7.2 Golden signals for THIS platform (freshness beats latency here)

The platform's job is "continuously updated." The signals that matter are staleness signals:

| Signal | Definition | Warning / Critical |
|---|---|---|
| **Source freshness** | `max(now - latest_record_ts)` per source (PMD, NIH/IDSR, NDMA, each news outlet), exported by a Dagster freshness job into Prometheus | per-source SLO, e.g., news > 6 h / > 24 h; weekly IDSR > 9 d / > 16 d |
| **Last successful run per Dagster job** | age of last SUCCESS per job (Dagster → Prometheus) | > 2× schedule interval / > 4× |
| **Pipeline lag** | Kafka consumer group lag per enrichment consumer | growing 1 h / growing 6 h |
| **Disk headroom** | per filesystem, plus *days-until-full* linear projection | < 25% or < 30 days / < 10% or < 7 days |
| **Backup recency** | age of last successful pgBackRest diff, MinIO mirror, Neo4j dump, restic off-site (textfile metrics) | > 2× schedule / > 4× |
| **Cert expiry** | blackbox probe cert age (belt) though Caddy auto-renews (suspenders) | < 21 d / < 7 d |
| **Edge availability** | blackbox HTTPS probe of dashboard + `/api/health` | 2 failed probes / 5 min down |
| **Postgres health** | connections vs max, replication of WAL archiving (archive failures), autovacuum stuck, TimescaleDB job failures | standard postgres_exporter rules |
| **Host health** | load, RAM, mdraid/ZFS degraded, SMART pre-fail, UPS on-battery, temperature (incl. GPU) | SMART/RAID/UPS are always critical |
| **Dead man's switch** | Alertmanager Watchdog → healthchecks.io (§7.3) | external notification if the monitoring itself dies |

### 7.3 Alerting channel and fatigue control

- **Channel: Discord server** (`#chip-alerts` for critical, `#chip-warnings` for warnings), via
  Alertmanager Discord webhook. Students live in Discord; history is searchable; zero cost. Weekly
  digest email (Grafana report or simple cron summary) to PI + leads for visibility without noise.
- **Dead man's switch**: Alertmanager's always-firing Watchdog alert pings a free healthchecks.io
  check every 5 min; if pings stop, healthchecks.io emails/Discords the team. This is the **single
  sanctioned external dependency** in the whole design (it monitors the case where everything of ours,
  including the monitor, is down — that cannot be self-hosted by definition). Documented opt-out:
  a cron on a lab desktop that curls Alertmanager's health endpoint.
- **Fatigue rules** (enforced at ops review): every alert must be *actionable* and must link a runbook
  in its annotation (`runbook_url`); anything a human wouldn't act on within a day is a dashboard
  panel, not an alert; target < 5 critical alerts/month in steady state — if an alert fires 3×
  without action, it gets fixed, re-thresholded, or deleted at the next review; there is exactly one
  "on-duty" student per week (§9.2) so alerts have an owner, not a bystander crowd.

---

## 8. CI/CD & code health

### 8.1 GitHub Actions: hosted runners for CI; no self-hosted runner initially

- **Hosted runners** for lint/type/test/build/push: zero maintenance, and GHCR pushes stay inside
  GitHub's network. Our builds are small; free/edu minutes suffice (monitor usage; if exceeded, a
  self-hosted runner on chip-dev-01 is the pressure valve).
- Self-hosted runners need only **outbound** HTTPS (long-poll), so the university firewall is not a
  blocker if we later add one for GPU tests or heavy integration jobs — but each self-hosted runner is
  an attack surface and a pet to maintain, so we start without.
- **Deployment is pull-based and human-initiated** (§8.5) — CI never holds SSH keys to prod. This
  both survives the firewall (no inbound to campus needed) and keeps the blast radius of a
  compromised CI token to "bad image published," which digest pinning and review catch.

### 8.2 Monorepo pipeline design

Monorepo layout (given): `libs/ connectors/ pipelines/ services/ dashboard/ ml/ infra/ docs/`.
Python managed as a **uv workspace**; internal libs are consumed path-locally, never published to an
index — **everything builds from the same commit; there is no internal semver.** The git SHA is the
version of everything (release tags `vYYYY.MM.PATCH` are human-friendly pointers to SHAs, CalVer
because features-per-release is meaningless in a research platform).

Workflows:

1. **`ci.yaml`** (every PR): change detection by path filters → run only affected package jobs, but
   *always* run `libs/` consumers when `libs/` changes (dependency graph in a small script, not
   hand-maintained lists). Jobs: ruff (lint+format check), mypy, pytest (unit; integration tests spin
   ephemeral Postgres/MinIO/Kafka via compose in the runner), compose-file lint, hadolint, gitleaks,
   Alembic single-head check + upgrade/downgrade smoke test against ephemeral Postgres, docs build.
   PR merge requires green CI + 1 review. Target wall time < 10 min or students will stop waiting for it.
2. **`build.yaml`** (merge to `main`): build+push changed images to GHCR tagged `sha-<shortsha>` and `edge`.
3. **`release.yaml`** (tag `v*`): builds ALL images, writes a **release manifest** —
   `releases/vYYYY.MM.P.yaml` committed to the repo: every image by digest, the compose bundle hash,
   migration head expected — plus the offline `docker save` bundle to MinIO (§3.5). The manifest is
   the unit of deployment and of rollback.
4. **Renovate** (or Dependabot): weekly grouped dependency PRs; base images and pinned tool versions
   included. Someone owns merging these (ownership matrix) or the project is unpatchable in year 3.

### 8.3 Code health standards (pre-commit, enforced in CI too)

`pre-commit` hooks (installed by the devcontainer automatically): ruff (lint + format), mypy
(strict on `libs/`, standard elsewhere), end-of-file/trailing-whitespace, gitleaks, hadolint,
`compose config` validation, markdownlint for `docs/`. Conventions doc (`docs/eng/conventions.md`)
is one page, and CI is the enforcement — no convention exists that a tool doesn't enforce.

### 8.4 Migration discipline (Alembic)

- One Alembic environment for the analytical core schema (owned per data-model doc), living in
  `libs/chip-db`. Other tools (Dagster, MLflow, Superset, Label Studio) own their own schemas in
  their own databases within the same Postgres instance — we never hand-migrate those; their images'
  built-in migrations run on upgrade, and *their* upgrades are release-noted.
- Rules enforced by CI: linear history, single head (`alembic heads` == 1), every migration has a
  working `downgrade`, upgrade+downgrade+upgrade smoke test passes on ephemeral Postgres.
- **Expand–contract requirement**: a migration must keep the *previous* release's code working
  (add columns nullable, backfill, then contract in a later release). This is what makes rollback
  (§8.5) a one-command operation instead of a restore.
- Migrations run as an **explicit deploy step** (`chipctl migrate`), never on container startup —
  startup migrations + multiple replicas + impatient students = corrupted deploys.

### 8.5 Deployment procedure (pull-based, student-runnable) and rollback

Deploying is running `chipctl deploy vYYYY.MM.P` over SSH (via WireGuard) on the target host.
`chipctl` is a bash/Python script in `infra/scripts/`, ~200 lines, readable in one sitting. It:

1. Fetches the release manifest from git (`git -C /opt/chip/repo pull --tags`), verifies digests.
2. Preflight: disk headroom > 15%, backups fresher than 26 h (refuses otherwise — deploy never
   outruns the safety net), staging has run this release ≥ 1 day (flag `--force` for hotfixes,
   which posts a notice to Discord automatically).
3. `docker compose pull` (or `--offline` load from bundle), decrypt env via SOPS to tmpfs.
4. `chipctl migrate` (Alembic upgrade, with pre-migration `pg_dump --schema-only` snapshot for the audit trail).
5. `docker compose up -d --remove-orphans`, then health-gate: waits for all healthchecks + blackbox
   probe green; posts result to Discord.
6. Records the deploy (version, who, when) to a local ledger file and a Prometheus metric.

**Rollback story**: `chipctl deploy <previous-version>` — manifests are immutable and images pinned
by digest, so rolling back is deploying an older manifest. Expand–contract (§8.4) guarantees the
previous code runs against the current schema; schema *downgrades* are exceptional, human-decided,
and use the tested `downgrade` scripts. If data corruption is involved, that is not a rollback but a
restore — RB-02, and the decision belongs to the ops lead + PI, not the on-duty student alone.

Cadence: staging deploys freely; prod deploys in a weekly window (e.g., Tue 10:00, humans awake and
caffeinated — never Friday), or on-demand for hotfixes with the `--force` notice.

---

## 9. Turnover-proofing

### 9.1 Documentation system (in-repo, versioned, CI-built)

`docs/` in the monorepo, built by MkDocs (material theme) in CI, served internally at
`docs.chip.internal` (and the public subset on the project webpage per the dissemination plan):

- **ADRs** — `docs/adr/NNNN-title.md`, MADR format, immutable once accepted (supersede, don't edit).
  Every decision in this document that says "(decision)" gets an ADR at implementation time. ADRs are
  the answer to "why is it like this?", which is the question every new cohort asks.
- **Runbooks** — `docs/runbooks/RB-NN-title.md`. One page each. Format: symptoms → preconditions →
  numbered commands (copy-pasteable) → verification → escalation contact. A runbook that has never
  been executed by someone other than its author is marked `UNVERIFIED` in its header; drills exist
  to burn that flag down.
- **Onboarding** — `docs/onboarding/day-one.md` (env running + one pipeline end-to-end, < 1 day),
  `week-one.md` (first PR merged, deploy to staging shadowed), `ops-cert.md` (checklist a student
  completes — including performing one staging deploy and one drill — before joining the on-duty rota).
- **The map** — `docs/architecture/` holds these subsystem docs plus one C4-style context diagram
  kept current (reviewed quarterly; a stale diagram is worse than none).

**Runbook index (initial set — write these before prod go-live, not after):**

| ID | Runbook |
|---|---|
| RB-01 | Full cold start / restart of all hosts (post power outage — includes NUT/UPS notes and startup order) |
| RB-02 | Restore Postgres (PITR from pgBackRest; to staging and to prod variants) |
| RB-03 | Rebuild the knowledge graph (from weekly dump; from Postgres full rebuild) |
| RB-04 | Rotate secrets (SOPS re-encrypt, service credential rotation, envelope update) |
| RB-05 | TLS/DNS/edge issues (Caddy, DNS-01, firewall escalation path to NUCES IT, WG fallback) |
| RB-06 | Disk pressure (identify, prune, expand; what is safe to delete — spoiler: never volumes) |
| RB-07 | Kafka broker loss / topic recreation and connector replay |
| RB-08 | Deploy and rollback with chipctl (incl. --offline and --force paths) |
| RB-09 | Onboard a student (accounts, keys, WG peer, ownership matrix update) |
| RB-10 | Offboard a student (revoke WG/SSH/GitHub/Grafana, rotate held secrets, handover checklist) |
| RB-11 | Restore MinIO objects (versioned undelete; mirror restore; restic off-site restore) |
| RB-12 | GPU node issues (driver/container-toolkit mismatch after upgrades, vLLM restart, thermal) |
| RB-13 | Monitoring is down / dead-man's-switch fired (bootstrap observability from scratch) |
| RB-14 | Backup failure triage (pgBackRest, mirror, restic, dump — per-signal decision tree) |
| RB-15 | Incident template + comms (what to tell the PI, what to tell stakeholders, postmortem format) |

### 9.2 Ownership matrix and the rota

`docs/ops/ownership.md` — reviewed and re-signed at the start of every semester:

| Area | Primary (student) | Secondary (student) | Faculty sponsor |
|---|---|---|---|
| Hosts/Ansible/network/edge | — | — | — |
| Postgres + backups/restores | — | — | — |
| Kafka/Karapace + connectors ops | — | — | — |
| Dagster + pipelines ops | — | — | — |
| GPU node + LLM serving | — | — | — |
| Observability + alert hygiene | — | — | — |
| CI/CD + registry + releases | — | — | — |
| Docs/onboarding quality | — | — | — |

Rules: **minimum-two rule** — every area has a primary and a secondary at all times; a graduation
that would leave an area single-owned triggers recruitment/handover *before* the person leaves, not
after. **Weekly on-duty rota** (one student, ops-certified per §9.1): triages alerts, does the drive
swap, runs the prod deploy window, hands over in a 15-min Monday sync with a written baton note in
`docs/ops/journal/` (the journal is also where "weird thing I noticed" goes — institutional memory).

### 9.3 Quarterly ops drills (calendar-blocked; skipping one is a red flag at the review)

- **D-1 Game day**: kill a service (or the primary host, once a year) in staging unannounced;
  on-duty student recovers from runbooks; timed.
- **D-2 Restore drill**: the rotating restore exercise from §6.6.
- **D-3 Power drill**: pull utility power (UPS/NUT clean-shutdown path), then execute RB-01 cold start.
- **D-4 Security/secrets drill**: hardening checklist audit, peer/key/roster reconciliation,
  one secret rotation via RB-04, envelope verification (annually).

Each drill ends with fixing the gaps it found (docs PRs merged within a week) — the drill's output
is better runbooks, not a report.

### 9.4 Bus-factor mitigations (summary of mechanisms defined above)

1. Everything in git (Ansible, compose, dashboards, alert rules, topics.yaml, docs, chipctl).
2. SOPS + sealed-envelope escrow: no credential exists in exactly one head or one laptop.
3. Minimum-two ownership + ops-certification before rota duty.
4. Drills executed by the *newest* qualified person, not the most expert one.
5. Offboarding runbook (RB-10) with secret rotation, run every single time — including for the
   most senior, most trusted departing student, precisely because they hold the most.
6. Faculty sponsors per area: continuity across cohorts sits with people who don't graduate.
7. Boring stack: every tool chosen is one a new student can learn from public docs in days.

---

## 10. Open questions

- **OQ-1 — Actual hardware inventory**: what servers/GPUs does PCN already own from NAaaS, and what
  is their condition/warranty? Tier A may be partially covered already. (Owner: PI; blocks the
  procurement list.)
- **OQ-2 — University server room**: rack space allocation, generator-backed circuit availability,
  cooling capacity, physical access policy for students. (Owner: PI + NUCES IT.)
- **OQ-3 — University firewall policy**: will NUCES IT grant the 443 DNAT and UDP 51820, and what is
  the change-request lead time? Determines whether the WG fallback (UDP 443 / SSH-only) is needed.
  File the request in month 1 regardless. (Owner: infra lead.)
- **OQ-4 — Domain and DNS**: `chip.nuces.edu.pk` delegation feasibility vs. independent domain;
  who pays/holds the registrar account (must be a project account, not a personal one). (Owner: PI.)
- **OQ-5 — Data-sharing agreement constraints on environments**: do NIH/PMD/NDMA agreements permit
  restored prod data in staging even anonymized/aggregated, and any data-residency clauses affecting
  the off-site copy location? (Owner: data-governance doc; affects §2.1 and §6.5.)
  **⚠️ Extended 2026-07-13 (ADR-012):** this now also governs **which documents may be sent to a
  third-party cloud parser.** `dim_source.access_tier` is the control: `public`/`historical` may use
  the hosted agentic parser; `mou-pending`/`restricted` **must** be parsed on-prem, enforced in the
  SDK. Any future NIH-under-MOU feed — and any hospital-level data (00 §6.1) — falls on the wrong
  side of that line. **Confirm with the DSAs before the first restricted feed lands.**
- ~~**OQ-6 — Schema registry final pick**~~ — **CLOSED by ADR-009: Apicurio, Postgres-backed.** It
  rides pgBackRest and needs no separate export job; Karapace would have added a compacted Kafka topic
  as new durable state. The Karapace rows in the hardware/backup tables above are superseded.
- **OQ-7 — GPU procurement reality**: import duties/availability in Pakistan at purchase time; used-3090
  market as the budget path. **ADR-014 fixes the requirement at 2 × 24 GB** (GPU0 serving / GPU1 batch),
  so procurement must plan for **two cards and a 1200 W+ PSU**, not one. (Owner: ML lead.)
- **OQ-8 — Stakeholder auth expectations**: will NIH/NDMA/MoCC accept email-based OIDC accounts, or
  require IP allowlists/mutual-TLS/officially hosted domains? Affects §4.4 tiering. (Owner: serving doc.)
- **OQ-9 — Second-site option**: does NUCES have a second campus/building where the Tier C replica
  box (§6.5-B) could live with network reachability? (Owner: PI + NUCES IT.)
- **OQ-10 — GH Actions minutes/education plan** for the org: confirm quota covers CI at our cadence
  before committing to hosted-only runners. (Owner: CI/CD area owner.)
- **OQ-11 — Superset public exposure**: stakeholders may eventually want self-service analytics;
  exposing Superset publicly is a materially larger attack surface than the dashboard. Deferred
  decision; requires its own ADR + security review. (Owner: serving doc + infra lead.)

---

*Every "(decision)" in this document becomes an ADR (`docs/adr/`) at implementation time. First
review of this document: at the month-3 ops review, then quarterly with the drills.*
