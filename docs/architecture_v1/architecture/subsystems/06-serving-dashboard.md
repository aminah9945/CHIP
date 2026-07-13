# 06 — Serving Layer & Dashboard: Micro-Architecture

**Subsystem:** CHIP serving application + stakeholder web dashboard
**Status:** Draft for review · **Owner:** Serving-layer lead · **Last updated:** 2026-07-10
**Governing ADRs (locked, do not relitigate here):**
- ADR-001 — one modular-monolith serving app (FastAPI); SPA frontend allowed
- ADR-003 — PostgreSQL (PostGIS/Timescale/pgvector) + Neo4j + MinIO only
- ADR-004 — self-hosted lab servers, containerized, exposed to government stakeholders via reverse proxy + TLS

**Audience:** grad students who will build this. When this doc says "Decision:", that is the plan. If you want to change one, write a one-page counter-proposal — do not silently deviate.

---

## 0. Scope and upstream contracts

This subsystem is everything between the gold data layer and a human's eyeball:

```
  PostgreSQL gold marts ─┐
  Neo4j CHKG ────────────┼──▶  chip-serving (FastAPI monolith)  ──▶  SPA dashboard (React)
  MinIO documents ───────┘         │  auth · API · alerts · RAG      Superset (researchers)
                                   │  briefs · audit · delivery      Email digests (SMTP)
                                   ▼
                             serving schema (Postgres):
                             users, sessions, audit, deliveries, briefs
```

What we **consume** (read-only, produced by subsystems 02–05):

| Source | Contract |
|---|---|
| `gold.epi_weekly` | district_code × epi_week × disease → cases, incidence, completeness flag |
| `gold.climate_weekly` | district_code × epi_week → tmean/tmax, rainfall, humidity, anomaly z-scores |
| `gold.hazard_events` | NDMA/PDMA events: type, district(s), start/end, severity |
| `gold.media_signals` | district × week × topic signal scores, linked doc IDs |
| `gold.forecasts` | model, district, disease, issue_week, horizon, point + PI bounds |
| `gold.forecast_skill` | model × district × disease rolling skill (CRPS/MAE, coverage) |
| `gold.alerts` | alert rows with severity, trigger rationale, evidence link IDs |
| `gold.rag_summaries` | generated summaries + citation manifest (doc IDs, KG node IDs) |
| Neo4j CHKG | nodes/edges: Disease, District, ClimateIndicator, HazardEvent, Document, with provenance properties |
| MinIO `documents/` | source news articles / reports referenced by citations |

**Conventions fixed here, upstream must match:** `district_code` = OCHA admin-2 P-code from the catalog; epi weeks as ISO-8601 week strings (`2026-W27`); all timestamps UTC in storage, rendered as PKT in the UI.

What we **own** (read-write): the `serving` Postgres schema — users, sessions, roles, institution grants, audit log, alert subscriptions and deliveries, brief records, saved views. We never write to gold, Neo4j, or pipeline-owned MinIO buckets. We write only to MinIO `briefs/` and `exports/`.

---

## 1. Modular monolith design

### 1.1 Project layout

One repo `chip-serving`, one deployable image, two entrypoints (API and worker).

```
chip-serving/
├── pyproject.toml                  # deps, import-linter contracts, ruff, pytest
├── Dockerfile                      # single image; CMD switches api|worker
├── compose.serving.yml
├── alembic/                        # migrations for the serving schema ONLY
├── src/chip_serving/
│   ├── main.py                     # app factory: mounts all module routers, middleware
│   ├── worker.py                   # scheduler entrypoint (see 1.4)
│   ├── export_openapi.py           # dumps openapi.json for client generation (see 2.5)
│   ├── shared/                     # the ONLY code modules may import from each other's world
│   │   ├── config.py               # pydantic-settings; env-driven
│   │   ├── db.py                   # two SQLAlchemy engines: gold_ro, serving_rw; neo4j driver; minio client
│   │   ├── security.py             # get_current_user / require_role deps, AccessPolicy (see §5)
│   │   ├── audit.py                # audit middleware + record_access() helper
│   │   ├── pagination.py           # Page[T], CursorPage[T], limit/offset params
│   │   ├── problems.py             # RFC 7807 error responses
│   │   ├── epiweek.py              # parse/format/arithmetic on ISO epi-weeks
│   │   └── dto.py                  # tiny cross-module value objects only (DistrictRef, Severity)
│   └── modules/
│       ├── auth/                   # login, logout, sessions, password reset, TOTP for admins
│       ├── catalog/                # districts, diseases, indicators metadata; boundary files
│       ├── indicators/             # epi + climate + hazard + media panels, choropleth values
│       ├── forecasts/              # forecast series, uncertainty, skill history
│       ├── alerts/                 # alert feed, evidence assembly, ack, subscriptions
│       ├── kg/                     # read-only CHKG traversal (nodes, neighbors, paths)
│       ├── summaries/              # RAG summary serving, citations, review workflow
│       ├── exports/                # policy briefs, CSV extracts, brief scheduling logic
│       └── admin/                  # user/institution mgmt, audit search, delivery logs
└── tests/
    ├── unit/<module>/
    └── api/                        # httpx TestClient tests per router
```

Every module has the same internal shape — keep it mechanical so rotating students always know where things go:

```
modules/alerts/
├── router.py      # FastAPI APIRouter; thin: parse → call service → shape response
├── service.py     # use-case logic; the only file allowed to talk to other modules' public.py
├── repo.py        # ALL SQL/Cypher/MinIO access for this module; takes AccessPolicy
├── models.py      # SQLAlchemy models for serving-schema tables this module OWNS
├── schemas.py     # pydantic request/response models
└── public.py      # the module's facade: small typed functions other modules may call
```

### 1.2 Boundary enforcement

**Decision: `import-linter` in CI, failing the build.** Boundaries that are only in a README die within one student rotation.

Contracts (in `pyproject.toml`):

```toml
[tool.importlinter]
root_package = "chip_serving"

# Layer 0: shared          — imports nothing from modules/
# Layer 1: catalog, indicators, forecasts, kg, auth      — import shared only
# Layer 2: alerts, summaries                             — may import L1 public.py
# Layer 3: exports, admin                                — may import L1/L2 public.py
# Layer 4: main, worker                                  — may import everything

[[tool.importlinter.contracts]]
name = "layers"
type = "layers"
layers = [
  "chip_serving.main | chip_serving.worker",
  "chip_serving.modules.exports | chip_serving.modules.admin",
  "chip_serving.modules.alerts | chip_serving.modules.summaries",
  "chip_serving.modules.catalog | chip_serving.modules.indicators | chip_serving.modules.forecasts | chip_serving.modules.kg | chip_serving.modules.auth",
  "chip_serving.shared",
]

[[tool.importlinter.contracts]]
name = "cross-module imports only via public facades"
type = "forbidden"
# generated: for each module M, forbid importing M.service / M.repo / M.models from outside M
```

Rules of thumb for students:
- `router.py` never imports another module. Ever.
- If you need another module's data, call a function on its `public.py`. If the function doesn't exist, add it there (small, typed, documented) — don't reach into its repo.
- If two modules keep needing the same helper, it moves to `shared/` via PR review, not copy-paste.

### 1.3 Data access: shared vs per-module

- **Gold layer:** read-only Postgres role `chip_serving_ro`. Connection factory lives in `shared/db.py`; **queries live in each module's `repo.py`** (indicators owns epi/climate SQL, forecasts owns forecast SQL, etc.). No shared ORM models for gold — modules use SQLAlchemy Core / textual SQL against gold views. Upstream publishes versioned views (`gold.v1_epi_weekly`); we query views, never their base tables, so pipeline refactors don't break us.
- **Serving schema:** each module owns its tables (declared in its `models.py`, migrated via one shared Alembic history). Ownership map: auth → users/sessions/roles/institutions/grants; alerts → subscriptions/deliveries/acks; summaries → review states/feedback; exports → briefs/extract jobs; shared → audit_log.
- **Neo4j:** only `kg/repo.py` and (for evidence assembly) `alerts/service.py` via `kg.public`. One place to enforce Cypher query timeouts (5 s) and result caps.
- **MinIO:** presigned-URL generation only in `catalog` (boundaries), `summaries`/`alerts` (cited documents), `exports` (briefs). The SPA never gets raw MinIO credentials; it gets short-lived presigned GET URLs minted by the API **after** the access-policy check and audit write.

### 1.4 Background work

Needs: nightly + immediate alert email delivery, daily/weekly digest assembly, scheduled monsoon-brief generation, audit-log export to MinIO, session cleanup, CSV extract jobs.

Options considered:
1. In-process APScheduler inside the API — free, but uvicorn runs multiple workers → duplicate firing, and a long brief render blocks nothing but does share the container's fate with API deploys.
2. Dagster-triggered (pipeline orchestrator calls us) — couples stakeholder-facing delivery to the data-pipeline subsystem's deploy cadence and its failure domain. Alert *generation* is theirs; alert *delivery* is ours.
3. Celery/Redis — a broker we don't otherwise need. Violates the boring bias and ADR-003's spirit.

**Decision: option "small worker" — a second container from the same image running `python -m chip_serving.worker`: APScheduler + a Postgres job table (`serving.jobs`), single replica, holding a Postgres advisory lock at startup so an accidental second replica exits.** Enqueue = insert a job row (API does this for on-demand briefs/extracts); the worker polls every 10 s and also runs cron entries. No broker, no new infra, same codebase and module boundaries (`worker.py` sits in Layer 4).

One narrow Dagster touchpoint, kept as a webhook not a dependency: when the pipeline finishes a gold refresh it POSTs `/internal/hooks/data-refreshed` (network-restricted, shared-secret header). The worker then re-evaluates pending immediate-severity deliveries early instead of waiting for the next cron tick. If the hook never fires, cron still catches everything — degradation, not breakage.

---

## 2. API design

### 2.1 Principles

- REST-ish resources, JSON, `GET` is always safe and cacheable, all reads filtered by `AccessPolicy`, all responses in English keys (i18n is a frontend concern; bilingual *content* fields come as `name_en`/`name_ur` pairs).
- **Versioning: URL prefix `/api/v1`.** Additive changes (new fields, new endpoints) never bump. Breaking changes require `/api/v2` plus a 2-release overlap. Realistically we stay on v1 for the project's life; the prefix exists so we *can* move.
- Errors: RFC 7807 `application/problem+json` (`{type, title, status, detail, instance}`) from `shared/problems.py`. No ad-hoc `{"error": ...}` shapes.
- IDs: ULIDs for serving-owned rows; upstream natural keys (district_code, epi_week, disease_code) for gold data.

### 2.2 Pagination & filtering conventions

- Bounded collections (districts, briefs, users): `?limit=50&offset=0` → `{"items": [...], "total": 312, "limit": 50, "offset": 0}`. Max limit 200.
- Feeds (alerts, audit log, deliveries): opaque cursor — `?limit=50&cursor=…` → `{"items": [...], "next_cursor": "…" | null}`. Cursor encodes `(created_at, id)`; stable under inserts.
- Filters are flat query params, repeatable where multi-valued: `?district=PK308&district=PK309&disease=dengue&severity=warning,emergency`.
- Time ranges: `?from=2026-W01&to=2026-W28` (epi weeks) or `?since=2026-07-01T00:00:00Z` (feeds).
- Sparse responses: `?fields=cases,rainfall` supported only where payloads are genuinely heavy (indicator panels).

### 2.3 Endpoint catalog (v1)

```
AUTH        POST /api/v1/auth/login                 POST /api/v1/auth/logout
            GET  /api/v1/auth/session               POST /api/v1/auth/password-reset[/confirm]

CATALOG     GET  /api/v1/catalog/districts          GET  /api/v1/catalog/diseases
            GET  /api/v1/catalog/indicators         GET  /api/v1/catalog/datasets
            GET  /api/v1/geo/districts-<hash>.topojson        (immutable, long-cache)

INDICATORS  GET  /api/v1/indicators/panel           # district drill-down payload
            GET  /api/v1/indicators/choropleth      # one metric, one week, all districts
            GET  /api/v1/indicators/national-kpis   # headline numbers for overview

FORECASTS   GET  /api/v1/forecasts                  # series + prediction intervals
            GET  /api/v1/forecasts/skill            # historical skill for honesty panel

ALERTS      GET  /api/v1/alerts                     # cursor feed
            GET  /api/v1/alerts/{id}
            GET  /api/v1/alerts/{id}/evidence       # KG trace + documents + indicators
            POST /api/v1/alerts/{id}/ack
            GET/PUT /api/v1/alerts/subscriptions    # my channels, thresholds, districts

KG          GET  /api/v1/kg/nodes/{id}
            GET  /api/v1/kg/nodes/{id}/neighbors?rel=ASSOCIATED_WITH&limit=25
            GET  /api/v1/kg/paths?from={id}&to={id}&max_hops=3
            GET  /api/v1/kg/search?q=dengue+lahore&type=Disease,District

SUMMARIES   GET  /api/v1/summaries?district=&period=&language=
            GET  /api/v1/summaries/{id}             # text + citation manifest
            POST /api/v1/summaries/{id}/feedback    # thumbs + free text (researcher review input)
            POST /api/v1/summaries/{id}/review      # researcher/admin approve|reject

EXPORTS     GET  /api/v1/briefs?season=&district=   POST /api/v1/briefs        (request generation)
            GET  /api/v1/briefs/{id}                GET  /api/v1/briefs/{id}/download
            POST /api/v1/exports/csv                GET  /api/v1/exports/{job_id}

DOCUMENTS   GET  /api/v1/documents/{doc_id}         # metadata + presigned URL (audited)

ADMIN       CRUD /api/v1/admin/users                CRUD /api/v1/admin/institutions
            PUT  /api/v1/admin/institutions/{id}/grants
            GET  /api/v1/admin/audit?user=&resource=&from=&to=    (cursor feed)
            GET  /api/v1/admin/deliveries?status=failed

INTERNAL    POST /internal/hooks/data-refreshed     GET /healthz   GET /readyz   GET /metrics
```

### 2.4 Request/response sketches (the five that matter)

> **Reconciled (ADR-010):** the alert-evidence sketch below shows a direct `INCREASES_RISK_OF` KG edge with
> a `confidence`. That edge shape does **not** exist in the CHKG — subsystem 04 reifies every causal/statistical
> link as an `:Assertion` node with `assertion_type`, epistemic `status` (observed/statistical/hypothesized),
> `confidence`, `model_version`, and `SUPPORTED_BY → :Evidence`. The evidence endpoint returns that
> assertion shape (per 04 §4.1), and each evidence link **carries a `status` field the dashboard must render**
> (a hypothesized link is worded as a hypothesis, never as fact). Only definitional edges
> (`IN_LOCATION`, `DURING`, `OCCURRED_IN`, `OF_DISEASE`, `PRECEDES`, `PARENT_OF`) are traversed as raw edges.

**District indicator panel** — one request paints the drill-down screen:

```
GET /api/v1/indicators/panel?district=PK308&disease=dengue&from=2026-W14&to=2026-W28

200 {
  "district": {"code": "PK308", "name_en": "Lahore", "name_ur": "لاہور", "province": "Punjab",
                "population": 13004135},
  "range": {"from": "2026-W14", "to": "2026-W28"},
  "epi": [ {"week": "2026-W14", "disease": "dengue", "cases": 42, "incidence_100k": 0.32,
            "completeness": "complete"}, … ],
  "climate": [ {"week": "2026-W14", "tmean_c": 31.2, "tmax_c": 39.0, "rain_mm": 4.1,
                "humidity_pct": 41, "rain_anom_z": -0.7, "temp_anom_z": 1.4}, … ],
  "hazards": [ {"id": "hz_01J9…", "type": "urban_flood", "start": "2026-W26",
                "end": "2026-W27", "severity": "moderate", "source": "PDMA Punjab"} ],
  "media_signal": [ {"week": "2026-W26", "topic": "dengue", "signal": 0.81, "doc_count": 14} ],
  "lag_profile": {"disease": "dengue", "driver": "rain_mm",
                  "correlations": [{"lag_weeks": 0, "r": 0.11}, {"lag_weeks": 4, "r": 0.58},
                                   {"lag_weeks": 6, "r": 0.44}],
                  "method": "spearman, 5y window", "computed_at": "2026-07-06T02:00:00Z"}
}
```
(`lag_profile` is precomputed upstream in `gold.lag_profiles`; we serve, we don't compute.)

**Forecast + uncertainty:**

```
GET /api/v1/forecasts?district=PK308&disease=dengue&issue_week=latest&horizon=4

200 {
  "model": "lstm_v3", "issue_week": "2026-W28",
  "series": [
    {"target_week": "2026-W29", "point": 210, "pi80": [150, 285], "pi95": [110, 360]},
    {"target_week": "2026-W30", "point": 265, "pi80": [175, 380], "pi95": [120, 495]}, …
  ],
  "skill_summary": {"window": "52w", "mae": 31.5, "pi80_coverage": 0.79,
                    "verdict": "usable", "vs_seasonal_baseline": "+18%"}
}
```

**Alert feed + evidence:**

```
GET /api/v1/alerts?status=active&severity=warning,emergency&limit=20

200 { "items": [ {
    "id": "al_01J9X…", "created_at": "2026-07-09T04:10:00Z",
    "district": {"code": "PK308", "name_en": "Lahore", "name_ur": "لاہور"},
    "disease": "dengue", "severity": "warning", "status": "active",
    "headline_en": "Dengue risk rising in Lahore: post-rainfall surge expected within 2–4 weeks",
    "headline_ur": "لاہور میں ڈینگی کا خطرہ بڑھ رہا ہے …",
    "trigger": {"rule": "forecast_exceeds_p90_seasonal", "value": 265, "threshold": 190},
    "evidence_counts": {"kg_paths": 3, "documents": 5, "indicators": 4},
    "ack": null
  }, … ], "next_cursor": "eyJj…" }

GET /api/v1/alerts/al_01J9X…/evidence

200 {
  "alert_id": "al_01J9X…",
  "kg_paths": [ {
     "label_en": "Heavy rainfall → standing water hazard → dengue vector amplification (Lahore)",
     "nodes": [ {"id": "n:Hazard:hz_01J9", "type": "HazardEvent", "name": "Urban flooding W26"},
                {"id": "n:District:PK308", "type": "District", "name": "Lahore"},
                {"id": "n:Disease:dengue", "type": "Disease", "name": "Dengue"} ],
     "edges": [ {"type": "OCCURRED_IN", "confidence": 1.0, "provenance": "PDMA sitrep 2026-06-29"},
                {"type": "INCREASES_RISK_OF", "confidence": 0.82,
                 "provenance": "learned + literature-supported"} ] } ],
  "documents": [ {"doc_id": "doc_9f2…", "title": "Stagnant water persists in Lahore suburbs…",
                  "source": "Dawn", "published": "2026-07-02", "language": "en",
                  "snippet": "…standing water reported in 14 union councils…",
                  "url": "/api/v1/documents/doc_9f2…"} ],
  "indicators": [ {"kind": "climate", "week": "2026-W26", "metric": "rain_mm",
                   "value": 188.0, "anomaly_z": 2.9} ],
  "forecast_ref": {"model": "lstm_v3", "issue_week": "2026-W28", "link":
                   "/api/v1/forecasts?district=PK308&disease=dengue&issue_week=2026-W28"}
}
```

**KG traversal (evidence explorer):**

```
GET /api/v1/kg/nodes/n:Disease:dengue/neighbors?rel=ASSOCIATED_WITH,INCREASES_RISK_OF&limit=25
200 { "center": {…}, "neighbors": [ {"node": {…}, "edge": {"type": "INCREASES_RISK_OF",
      "confidence": 0.82, "sources": ["doc_9f2…"], "first_seen": "2025-09-01"}} ],
      "truncated": false }
```

**RAG summary with citations:**

```
GET /api/v1/summaries/sm_01J9…?language=en

200 {
  "id": "sm_01J9…", "scope": {"district": "PK308", "period": "2026-W25/2026-W28"},
  "language": "en", "generated_at": "2026-07-09T05:00:00Z",
  "model_version": "chip-graphrag-0.4",
  "review": {"status": "approved", "by": "u_researcher_7", "at": "2026-07-09T09:12:00Z"},
  "text": "Dengue cases in Lahore rose 34% over the last three weeks [1], following heavy
           rainfall in W26 [2]. Historical lag analysis indicates peak transmission risk in
           W30–W32 [3]. PDMA Punjab has reported standing water in 14 union councils [4]. …",
  "citations": [
    {"marker": "[1]", "kind": "indicator", "ref": "gold.epi_weekly:PK308:dengue:2026-W25..W28"},
    {"marker": "[2]", "kind": "indicator", "ref": "gold.climate_weekly:PK308:2026-W26"},
    {"marker": "[3]", "kind": "kg_edge",  "ref": "n:edge:e_884…", "confidence": 0.58},
    {"marker": "[4]", "kind": "document", "ref": "doc_9f2…", "source": "Dawn", "published": "2026-07-02"}
  ],
  "disclaimer_key": "ai_generated_evidence_linked"
}
```

Every citation resolves to something a user can click and inspect. A summary whose citation manifest fails resolution is served with `review.status="quarantined"` and hidden from institutional roles — auditability is the product.

### 2.5 OpenAPI → frontend client

**Decision: generate, don't hand-write, the API client.** CI job on the serving repo: `python -m chip_serving.export_openapi > openapi.json`; frontend CI runs `openapi-typescript openapi.json -o src/api/schema.d.ts` and uses `openapi-fetch` (a 6 kB typed fetch wrapper — no heavy generated SDK classes). The `openapi.json` artifact is published on every tagged release; the frontend pins a version and a CI drift check fails when the pinned spec and `main` diverge on endpoints the frontend uses. This is the cheapest possible contract test and it survives student turnover.

---

## 3. Dashboard (stakeholder SPA)

### 3.1 Stack decision

**Decision: React 18 + Vite + TypeScript. Not Next.js.** Reasons: no SEO or SSR requirement (auth-gated app), static build served by the reverse proxy means no Node process in production on lab servers, and Vite is the smallest mental model for rotating students. Supporting cast — all mainstream and boring:

| Concern | Pick | Why |
|---|---|---|
| Data fetching | TanStack Query + `openapi-fetch` | caching, retries, stale-while-revalidate for free |
| Routing | react-router v6 | default choice |
| UI kit | Ant Design 5 | data-dense tables/forms out of the box; first-class RTL via `ConfigProvider direction` |
| Map | **MapLibre GL JS** (confirmed) | vector rendering of one TopoJSON, no external tile dependency, canvas perf on weak laptops |
| Charts | Apache ECharts (`echarts-for-react`) | dual-axis epi/climate overlays, canvas perf for long series, built-in data zoom; lazy-loaded |
| i18n | react-i18next | standard; namespace per screen |
| State | server state in Query; UI state in React; add Zustand only if a real need appears | avoid Redux ceremony |
| Testing | Vitest + Playwright (3–5 smoke flows) | keep it honest, keep it small |

Map basemap: **none for v1.** We render province outlines + district choropleth on a neutral background from our own TopoJSON. No OSM tile server dependency — government networks with restrictive egress will still render the map, and we ship zero external requests (also simplifies our CSP). Self-hosted Pakistan vector tiles are a later nicety (open question §10).

### 3.2 Screens and ASCII wireframes

Persistent chrome on all screens:

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ CHIP ▌Climate–Health Intelligence   [National] [Alerts(3)] [Forecasts]       │
│                                     [Briefs] [Explorer*]      🔔  اردو|EN  ⚙ │
└──────────────────────────────────────────────────────────────────────────────┘
  * Explorer (KG) visible to researcher/admin roles only
```

**S1 — National overview (landing):**

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  Week 2026-W28   Disease: [Dengue ▾]   Metric: [Incidence /100k ▾]           │
│  ┌────────────────────┬────────────────────┬────────────────────┐            │
│  │ ACTIVE ALERTS   12 │ DISTRICTS WATCH 31 │ DATA THROUGH  W28  │  (KPI row) │
│  │ ▲3 vs last week    │ monsoon phase      │ NIH feed: current  │            │
│  └────────────────────┴────────────────────┴────────────────────┘            │
│  ┌───────────────────────────────────────────┐  ┌──────────────────────────┐ │
│  │        PAKISTAN CHOROPLETH (MapLibre)     │  │ TOP DISTRICTS THIS WEEK  │ │
│  │                                           │  │ 1. Lahore     ⚠ 265 ▲34% │ │
│  │        ██▓▓░░  districts shaded by        │  │ 2. Karachi C. ⚠ 198 ▲21% │ │
│  │        ▓▓██▓░  selected metric;           │  │ 3. Rawalpindi ● 121 ▲12% │ │
│  │        ░▓▓░░░  ⚠ badges on alerted        │  │ 4. Multan     ● 96  ▲9%  │ │
│  │        districts; click → S2              │  │ …            [full table]│ │
│  │  [◀ W20 ────────●────── W28 ▶] week slider│  └──────────────────────────┘ │
│  │  Legend: 0 ░ 1 ▒ 5 ▓ 20+ █  (per 100k)   │  ┌──────────────────────────┐ │
│  └───────────────────────────────────────────┘  │ LATEST ALERTS            │ │
│  [Switch to table view]  ← low-bandwidth & a11y │ ⚠ Dengue – Lahore   2h   │ │
│                             fallback            │ ⚠ AWD – Sukkur      1d   │ │
│                                                 │ ● Heat – Jacobabad  2d   │ │
└──────────────────────────────────────────────────────────────────────────────┘
```

**S2 — District drill-down (epi curves vs climate, lag view):**

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  ◀ National   LAHORE (لاہور) · Punjab · pop 13.0M     Disease: [Dengue ▾]     │
│               Range: [last 26 weeks ▾]        [Download CSV] [Generate brief]│
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │  EPI CURVE + CLIMATE OVERLAY (dual axis, ECharts)                       ││
│  │  cases                                                        rain (mm) ││
│  │  300┤                                    ╭─╮ forecast → ┈┈┈╮  ┃200      ││
│  │  200┤                          ╭────────╯  ╰──╮      ░░░░░░░  ┃  ← PI80 ││
│  │  100┤ ▂▂▃▃▂▃▄▅▆▆▅▄▃▂ bars=rain ╱              ╰───   ░░░░░░░  ┃100      ││
│  │    0┤▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁┃0        ││
│  │      W14      W18      W22   ▲hazard: urban flood W26    W32            ││
│  │  ☑ cases ☑ rainfall ☐ tmax ☐ humidity ☐ media signal   [shift rain +4w] ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│  ┌──────────────────────────────┐  ┌──────────────────────────────────────┐ │
│  │ LAG PROFILE rain → dengue    │  │ THIS DISTRICT NOW                    │ │
│  │  r 0.6┤      ●               │  │ • 1 active alert  [open in Alerts →] │ │
│  │    0.4┤    ●   ●             │  │ • Forecast W29–W32: rising [→ S4]    │ │
│  │    0.2┤  ●       ●           │  │ • Latest summary (AI, reviewed):     │ │
│  │      0└─┬──┬──┬──┬──┬        │  │   "Dengue cases rose 34%… [1][2]"    │ │
│  │        0  2  4  6  8 wk lag  │  │   [read with citations → S5]         │ │
│  │  peak at 4 weeks (r=0.58)    │  │                                      │ │
│  └──────────────────────────────┘  └──────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
```
The `[shift rain +4w]` control re-plots the climate series shifted by the peak lag so a non-technical user can *see* the alignment — this is the single most persuasive visual for the lag story.

**S3 — Alert center with evidence panel ("why this alert"):**

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  ALERTS   [Active ▾] [Severity: all ▾] [Province: all ▾] [Disease: all ▾]    │
│ ┌───────────────────────────────┬──────────────────────────────────────────┐│
│ │ FEED (cursor-paged)           │ WHY THIS ALERT — Dengue · Lahore · W28   ││
│ │ ⚠ EMERG AWD – Sukkur     2h ▸ │ Trigger: forecast 265 > seasonal p90 190 ││
│ │ ⚠ WARN Dengue – Lahore ● 2h ▸ │──────────────────────────────────────────││
│ │ ⚠ WARN Malaria – D.I.K.  9h ▸ │ EVIDENCE CHAIN (from knowledge graph)    ││
│ │ ● WATCH Heat – Jacobabad 1d ▸ │  [Rain W26 z=2.9]─OCCURRED_IN→[Lahore]   ││
│ │ ● WATCH Dengue – Multan  2d ▸ │  [Urban flood W26]─INCREASES_RISK_OF     ││
│ │ ✓ resolved…                   │        (conf 0.82)→[Dengue]              ││
│ │                               │  ▸ open full graph view (researcher)     ││
│ │ [Load more]                   │──────────────────────────────────────────││
│ │                               │ SUPPORTING SIGNALS                       ││
│ │                               │ • rain W26: 188mm (2.9σ above normal)    ││
│ │                               │ • media signal 0.81 (14 articles) ▸      ││
│ │                               │──────────────────────────────────────────││
│ │                               │ SOURCE DOCUMENTS (5)                     ││
│ │                               │ 📄 Dawn 02-Jul "Stagnant water…" [open]  ││
│ │                               │ 📄 PDMA sitrep 29-Jun [open]             ││
│ │                               │──────────────────────────────────────────││
│ │                               │ [Acknowledge]  [Subscribe district]      ││
│ │                               │ Acked by: NIH-FELTP (Dr. …) 09-Jul 11:02 ││
│ └───────────────────────────────┴──────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────────────┘
```

**S4 — Forecast view with historical skill:**

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  FORECASTS   District:[Lahore ▾]  Disease:[Dengue ▾]  Model:[lstm_v3 ▾]     │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │ observed ───   forecast ┄┄┄   PI80 ░░   PI95 ▒▒                         ││
│  │ 400┤                                       ▒▒▒▒▒▒▒▒                     ││
│  │ 300┤                              ╭──╮   ░░░░░░░░░░░                    ││
│  │ 200┤                    ╭────────╯   ╰┄┄░░░░┄┄┄┄░░░░                    ││
│  │ 100┤ ─────╮╭───────────╯                                                ││
│  │    └──────┴┴──────────────────────────┬──────────────                   ││
│  │     W16        W20        W24        W28(now)   W32                     ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│  ┌───────────────────────────────┐ ┌───────────────────────────────────────┐│
│  │ SHOULD YOU TRUST THIS?        │ │ PAST FORECASTS vs REALITY (backtest)  ││
│  │ 52-week skill, this district: │ │  ┊ issued W20 ┄╮__ actual ──          ││
│  │ • MAE: 31.5 cases             │ │  ┊ issued W24 ┄┄╮_╭──                 ││
│  │ • 80% interval covered 79%    │ │  small-multiples of recent issues     ││
│  │   of actuals ✓ well-calibrated│ │  so users see typical miss size       ││
│  │ • Beats seasonal-normal +18%  │ │                                       ││
│  │ Verdict: USABLE ●●●○          │ │                                       ││
│  └───────────────────────────────┘ └───────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────────────┘
```
The skill panel is non-negotiable: showing historical misses is what earns institutional trust, per the proposal's explainability commitment.

**S5 — Summary / brief view (RAG output with citations):**

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  SITUATION SUMMARY · Lahore · W25–W28        [اردو] [EN]   [Download PDF]    │
│  ⓘ AI-generated, evidence-linked. Reviewed & approved by PCN (09 Jul 2026).  │
│ ┌───────────────────────────────────────────────┬──────────────────────────┐│
│ │ Dengue cases in Lahore rose 34% over the      │ CITATIONS                ││
│ │ last three weeks [1], following heavy         │ [1] NIH IDSR W25–W28,    ││
│ │ rainfall in W26 [2]. Historical lag analysis  │     Lahore dengue ▸chart ││
│ │ indicates peak transmission risk in W30–W32   │ [2] PMD rainfall W26,    ││
│ │ [3]. PDMA Punjab reported standing water in   │     188mm (z=2.9) ▸chart ││
│ │ 14 union councils [4].                        │ [3] CHKG edge e_884      ││
│ │                                               │     (conf 0.58) ▸graph   ││
│ │ RECOMMENDED ATTENTION                         │ [4] Dawn, 02-Jul-2026    ││
│ │ • Vector control in UCs listed in [4]         │     ▸ source article     ││
│ │ • Pre-position test kits (forecast [→S4])     │                          ││
│ │                                               │ Hover a [n] in the text  ││
│ │ Past summaries: [W21–W24] [W17–W20] …         │ → citation card popover  ││
│ └───────────────────────────────────────────────┴──────────────────────────┘│
└──────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Urdu/English i18n and RTL

- All UI strings in `locales/en/*.json` and `locales/ur/*.json` (react-i18next, namespace per screen). **Content** bilingual fields (district names, alert headlines, summaries) come from the API — never from locale files.
- Language toggle swaps `<html lang="ur" dir="rtl">`, AntD `ConfigProvider direction="rtl"`, and the i18next language, persisted in `localStorage` and mirrored to the user profile.
- CSS: logical properties only (`margin-inline-start`, `padding-inline-end`, `inset-inline`) — lint-enforced via stylelint. No `left`/`right` in layout code.
- Numbers, dates, epi-week labels stay LTR Western digits inside RTL text (standard Pakistani government practice); wrap mixed runs in `<bdi>`/`unicode-bidi: isolate`. Charts remain LTR (time axes flow left→right in both locales) — flipping chart direction confuses more than it helps; only titles/legends translate.
- Fonts, self-hosted woff2 (CSP forbids external fonts anyway): **Noto Naskh Arabic for Urdu UI chrome** (legible at 12–14 px, sane line height) and **Noto Nastaliq Urdu for long-form summary/brief text** (the register readers expect). Subset both; Nastaliq is heavy (~500 kB unsubset) — load it only on summary/brief routes.
- Locale QA gate: an untranslated key renders as `⟦key.name⟧` in dev builds so gaps are visible, not silently English.

### 3.4 Low-bandwidth / progressive loading

Assume a 2–5 Mbps government office connection and 8-year-old laptops.

- Route-level code splitting; the landing route budget is **≤ 300 kB gz** before the map chunk. MapLibre (~220 kB gz) and ECharts (~110 kB gz, tree-shaken) are lazy chunks loaded after first paint; KPI cards and the alert list render from the first bundle.
- District boundaries: one quantized TopoJSON (mapshaper-simplified, ~160 districts, target ≤ 250 kB gz), served as an immutable hashed file with `Cache-Control: max-age=31536000` — downloaded once per browser, ever. Choropleth values arrive as a separate small JSON (`{"PK308": 2.04, …}`, ~5 kB) and are joined client-side; the week slider fetches only new value maps.
- All static assets brotli-precompressed at build; API responses gzip. Poll cadence for alerts: 5 minutes, with `If-None-Match` so unchanged feeds cost a 304.
- Skeleton loaders everywhere; TanStack Query serves cached data instantly and revalidates in background (stale-while-revalidate), so navigation feels local even on slow links.
- **Table view is a first-class citizen:** every map/chart screen has a "switch to table" toggle (sortable AntD table of the same data). It's the low-bandwidth mode, the screen-reader mode, and the copy-paste-into-a-report mode in one feature.

### 3.5 Accessibility basics (target: WCAG 2.1 AA pragmatically)

- Severity never encoded by color alone: shape + label (⚠ WARN) accompany the palette; choropleth ramps are colorblind-safe (viridis-family) with a discrete legend.
- Full keyboard operability (AntD gives most of it free); visible focus rings; skip-to-content link.
- `aria-live="polite"` region announces new alerts; charts carry a generated text summary in `aria-label` ("Dengue cases in Lahore rising for 3 consecutive weeks, current 265") plus the table fallback.
- Minimum 14 px body text, 44 px touch targets, `prefers-reduced-motion` respected (no animated map transitions).
- Both locales screen-reader tested at least once per release with NVDA (free, and what's actually used locally).

---

## 4. Internal analytics for researchers

**Decision: deploy Apache Superset for researcher self-service. Do not build exploratory screens into the SPA.**

Rationale: researchers' questions are open-ended SQL over the gold layer ("dengue vs 3-week-lagged rainfall for districts with >2 flood events since 2023"); every bespoke internal screen we build is a screen a grad student maintains instead of doing research. Superset is Python (the team can patch it), self-hosted, and free.

Guardrails:
- Separate container, **internal-only vhost** (university network / VPN; never on the stakeholder-facing hostname). Superset is a large attack surface — it does not meet government-facing hardening standards and doesn't need to.
- Connects with Postgres role `superset_ro`: SELECT on gold views only; the `serving` schema and any NIH-restricted marts are excluded at the role level (a second `superset_restricted_ro` role can be granted to named researchers if/when NIH data terms allow — see §5.4).
- Superset's own local auth for the ~10–15 researcher accounts; admin creates accounts manually. Not wired to CHIP auth — not worth the integration for this population.
- Neo4j exploration stays in **Neo4j Browser/Bloom** on the same internal vhost with a read-only Neo4j user. Superset doesn't speak Cypher; don't force it.
- Rule of promotion: when a Superset chart proves institutionally valuable, it gets *rebuilt properly* as an SPA feature with access control and audit — Superset dashboards are never shown to external stakeholders.

---

## 5. AuthN/Z, sessions, audit

### 5.1 Identity provider decision

**Decision: lightweight in-app auth — users, roles, and sessions in Postgres. Not Keycloak.** Keycloak is another always-on JVM service with its own upgrade treadmill, realm-config expertise, and backup story — that is real ops load for a rotating-student team, purchased to serve maybe 100–200 total accounts across four institutions with no SSO federation requirement in sight. If a ministry later mandates SSO/SAML, we put Keycloak (or oauth2-proxy) *in front* and map identities into the same `users` table — the door stays open.

Within "lightweight", one refinement: **opaque session tokens, not stateless JWTs.** At this scale the one indexed session lookup per request is free, and it buys instant revocation ("this official left NDMA — kill access *now*") and a true record of live sessions — both of which matter more than statelessness ever will here. Password hashing: argon2id. Admin accounts require TOTP (pyotp, ~50 lines); optional for others.

### 5.2 Session security

- Session token: 256-bit random, stored hashed (SHA-256) in `serving.sessions`; delivered as `__Host-chip_session` cookie: `HttpOnly; Secure; SameSite=Lax; Path=/`.
- CSRF: state-changing requests require `X-CSRF-Token` header matching a per-session token (double-submit); SameSite=Lax already blocks the bulk.
- Idle timeout 8 h, absolute lifetime 30 days, sliding renewal; login rate-limited (slowapi + fail2ban at the proxy); lockout after 10 failures/15 min.
- Standard headers at Caddy: HSTS, `X-Content-Type-Options`, `frame-ancestors 'none'`, and a strict CSP (`default-src 'self'` — trivially achievable since we self-host everything, including fonts and map data).
- Account lifecycle: admin-created accounts with institutional email, invite links (24 h expiry), self-service password reset via SMTP, and a **quarterly access review** job that emails admins a list of accounts unused for 90 days (contractual hygiene for government partners).

### 5.3 Role model

```
role                  scope
─────────────────────────────────────────────────────────────────────────────
admin                 everything incl. user mgmt, audit search, delivery logs
researcher            all data incl. unreviewed summaries, KG explorer, Superset-tier detail
institutional-partner all *approved* content; restricted datasets per institution grants;
                      can ack alerts, manage own subscriptions, download briefs
viewer                approved, non-restricted content, read-only; no downloads of
                      restricted docs (future public-tier prototype = viewer minus login)
```

Enforced by `require_role(...)` dependencies on routers. Roles are additive-flat, not hierarchical — four rows in a table, no permission DSL.

### 5.4 Per-institution data visibility (NIH-restricted data)

Design now, so restricted NIH feeds slot in without refactoring:

- Every gold mart row family carries a `dataset_id`; `serving.datasets(dataset_id, sensitivity ∈ {public, internal, restricted}, owner_institution)`.
- `serving.institution_grants(institution_id, dataset_id)` — which institution may see which restricted dataset. NIH restricting district-level line data to NIH + relevant PDMA = two grant rows.
- `shared/security.py` builds one **`AccessPolicy`** per request: `{role, institution_id, allowed_dataset_ids}`. Every `repo.py` read function takes the policy and appends `WHERE dataset_id = ANY(:allowed)` — a repo function without a policy parameter fails a custom lint check in CI. Single enforcement point, default-deny.
- We deliberately use app-level filtering, not Postgres RLS: one code path, testable with plain pytest, and the gold schema (owned by another subsystem) stays untouched. If a second enforcement layer is ever demanded contractually, RLS can be added beneath without changing the app.
- Aggregation-tier rule per the proposal's ethics section: restricted data is only ever served at approved admin resolutions; the serving layer never holds sub-district or line-level data at all.

### 5.5 Audit logging (contractual)

- `serving.audit_log` — append-only, monthly partitions: `(id, ts, user_id, institution_id, role, method, path, query_params, resource_type, resource_ids[], dataset_ids[], status, latency_ms, ip, session_id)`.
- Written by middleware for every authenticated request; for sensitive reads (indicator panels touching restricted datasets, document opens, brief/CSV downloads, evidence views) the module calls `audit.record_access()` with the **actual resource IDs returned**, so the log answers "who saw *what*", not just "who called which URL".
- No REST DELETE/UPDATE surface exists for audit rows; the app's DB role has INSERT+SELECT only on this table. Nightly job exports closed partitions to MinIO `audit-archive/` as Parquet (checksummed). Retention: life of project + 5 years (mirror NRPU record-keeping).
- Admin UI: searchable audit browser (`/admin/audit`) filterable by user, institution, dataset, resource, date — because the first NIH question in any incident will be "show me everyone who opened that file."

---

## 6. Alert delivery

### 6.1 Channels

**In-app (primary):** alert feed + bell counter, 5-minute polling with ETag (no WebSockets — polling is boring, proxy-friendly, and indistinguishable from push at a 5-minute operational tempo). Per-user read/unread in `serving.alert_reads`.

**Email (secondary, digest-first):**
- `watch` → daily digest 08:00 PKT; `warning` → daily digest + same-day batch (sent within the hour); `emergency` → immediate individual email to subscribed users of granted institutions.
- Digests are compiled per user from `serving.subscriptions` (districts × diseases × min-severity). Content: plain-text-first with a simple HTML part, bilingual per user preference, deep links back to the dashboard. **No restricted data in email bodies** — headline + link only, so a forwarded email leaks nothing gated.

**Self-hosted SMTP realities (do not learn these the hard way):** a lab server's IP sending direct-to-MX email to `nih.org.pk` / `ndma.gov.pk` will be greylisted or silently spam-foldered — university IP ranges have no sending reputation, and gov mail gateways are unforgiving. **Decision: run a local Postfix container as a queue/retry buffer only, relaying through the NUCES mail infrastructure** (authenticated smarthost), with SPF/DKIM/DMARC set up for a delegated subdomain (`chip.nu.edu.pk`) — action item with campus IT in §10. Volumes are tiny (tens of recipients), so no commercial relay needed. Bounce handling: a `chip-bounces@` mailbox polled hourly by the worker (IMAP), mapping bounces to delivery rows; 3 hard bounces auto-pauses the address and flags an admin.

### 6.2 WhatsApp / SMS for Pakistani institutional users — discussion and recommendation

Reality check for Pakistan: WhatsApp is the de-facto working channel of every district health officer and PDMA duty room — reach is genuinely better than email. Feasibility:

- **WhatsApp Business Cloud API:** requires Meta Business verification of a legal entity (NUCES or a partner — a real administrative hurdle for a university), pre-approved message templates, and per-conversation utility pricing (order of USD 0.02–0.05 in Pakistan; verify current rates at implementation time). Technically simple (HTTPS API), administratively slow.
- **SMS:** local aggregators / telco corporate SMS (Jazz, Telenor, or resellers) at roughly PKR 1–3 per message, plus PTA-registered masked sender ID paperwork. Ubiquitous reach, 160 chars, no links culture-fit for feature phones; delivery reporting is mediocre.

**Decision: defer both to Phase 2, after the first monsoon pilot — but abstract now.** The pilot will tell us whether institutional users actually live in the dashboard/email or ignore it; buying Meta verification and telco contracts before that evidence is premature. Concretely today: `alerts/service.py` dispatches through a `DeliveryChannel` protocol (`send(recipient, rendered_alert) -> DeliveryResult`) with `InAppChannel`, `EmailChannel`, and a stub `WhatsAppChannel` that logs; `serving.subscriptions.channel` is already an enum including `whatsapp|sms`. Adding a real gateway later is one class + credentials, zero schema change. Budget line for Phase 2: assume ~USD 30–60/month WhatsApp at pilot scale (500 conversations) — trivial; the cost is the paperwork, not the messages.

### 6.3 Delivery tracking

`serving.alert_deliveries(id, alert_id, user_id, channel, status ∈ {queued, sent, failed, bounced, suppressed}, attempts, last_error, queued_at, sent_at)` — one row per (alert|digest, recipient, channel). The worker owns state transitions with capped exponential retry (3 attempts). In-app "seen" comes from `alert_reads`; email opens are **not** pixel-tracked (unreliable, and creepy in a government context) — "delivered + acknowledged-in-app" is our accountability chain, and `POST /alerts/{id}/ack` records the institutional acknowledgement that actually matters for the workflow-integration commitment.

---

## 7. Policy-brief generation

### 7.1 Pipeline

**Decision: Jinja2 HTML template → WeasyPrint PDF, charts rendered server-side with matplotlib. Pure-Python, no headless browser.**

```
worker job: generate_brief(scope, period, language)
  1. exports/service.py assembles BriefContext via public facades:
     indicators.public.panel(), forecasts.public.series_and_skill(),
     alerts.public.active_for(), summaries.public.approved_for()
  2. charts: matplotlib (Agg) → PNG @ 200dpi: epi+climate overlay, forecast fan,
     choropleth (geopandas + district shapes), lag bar chart. Same viridis-family
     palette + labeling rules as the SPA so briefs and dashboard look like one product.
  3. render templates/brief_{national|district}.html.j2 (A4, @page CSS, header/footer,
     page numbers, "AI-assisted, evidence-linked" disclaimer block)
  4. WeasyPrint → PDF; store PDF + source HTML in MinIO briefs/YYYY/; insert
     serving.briefs row (scope, period, language, minio_key, sha256, generated_by,
     review_status)
  5. notify subscribed institutional users via EmailChannel (link, not attachment)
```

Brief anatomy (proposal-committed "policy-brief style outputs"): title block → 5-bullet key findings (from the approved RAG summary) → national/district choropleth → epi-vs-climate chart with lag annotation → forecast fan + skill statement in plain words ("in the last year this forecast was off by ~30 cases on average") → active alerts table → **numbered citation appendix resolving every claim** to IDSR week, PMD observation, KG edge, or news article (source, date, outlet). The citation appendix is the explainability commitment made portable — the brief must stand alone when printed and photocopied.

**Urdu briefs:** WeasyPrint shapes RTL text via Pango/HarfBuzz; embed Noto Nastaliq Urdu and validate rendering in **month 1 of frontend work** with a real Urdu template (Nastaliq line-height and justification are the known risk). Contained fallback if quality disappoints: swap step 4 for headless-Chromium print-to-PDF (Playwright) — the HTML input is unchanged, so this is a renderer swap, not a redesign.

### 7.2 Scheduling & review

- **Monsoon season (Jun 15 – Sep 30): weekly national brief** (generated Mon 06:00 PKT after the W-close data refresh) **+ district briefs for any district with an active warning/emergency alert.** Off-season: monthly national brief. On-demand: `[Generate brief]` on S2/S5 enqueues a job (researcher/partner roles), result appears under `/briefs` in ~1–2 min.
- **Review gate:** scheduled briefs are generated in `draft` status; a researcher/admin approves in the UI before partners see them and emails go out. A standing worker reminder escalates drafts older than 24 h. Fully-automatic distribution of generative text to ministries is how a project loses institutional trust in one bad paragraph; the human gate stays until at least a full season of clean output, then we revisit (open question §10).

---

## 8. Performance & caching

### 8.1 Honest sizing

~160 districts × ~15 diseases × 52 weeks × 10 years ≈ **1.2 M rows** in the largest mart — small. Concurrency: tens of users, peaking during a monsoon emergency at maybe 100. Every panel query is an indexed range scan returning < 30 kB.

**Decision: no Redis, no CDN, no query-cache layer. Postgres with correct indexes + HTTP caching is the entire performance architecture.** Adding a cache tier here would be resume-driven engineering, and stale-cache bugs during an actual emergency are the one failure mode we cannot afford. Targets: p95 < 300 ms for panel endpoints, < 800 ms for KG traversals (Neo4j, 5 s hard timeout + result caps) — assert in CI with a small load test (locust, 50 virtual users) so regressions are caught, not felt.

What we *do* cache, because it's free:

| Thing | Strategy |
|---|---|
| District TopoJSON, SPA assets | content-hashed filenames, `max-age=31536000, immutable`, brotli-precompressed, served by Caddy directly (not FastAPI) |
| Choropleth values, indicator panels, national KPIs | `Cache-Control: private, max-age=3600` + `ETag` derived from the mart's `refreshed_at`; gold updates weekly-to-daily, so 1 h staleness is safely inside the data cadence. Alert endpoints: `no-store`. |
| Generated briefs/exports | MinIO objects; API redirects to presigned URLs post-audit |
| Fonts (esp. Nastaliq) | same immutable-hash treatment; loaded per-route |

### 8.2 Choropleth serving strategy

No tile server. The entire geometry problem at admin-2 for one country fits in **one static simplified TopoJSON ≤ 250 kB gz** (mapshaper: visvalingam simplify to ~1:500k, quantize 1e5, verify shared borders stay topologically clean and Karachi's districts stay distinguishable). MapLibre renders it as a GeoJSON source client-side; weekly metric JSONs (~5 kB) join by `district_code` via feature-state. The week-slider animation is therefore ~5 kB per step. If a later feature needs tehsil (admin-3, ~600 units) resolution, *that's* when tippecanoe + self-hosted PMTiles enters — explicitly out of scope for v1. Boundary updates (districts do get re-drawn in Pakistan) are a new hashed file + catalog version bump, keeping old briefs reproducible against the geometry they were rendered with.

---

## 9. Deployment shape (within ADR-004)

```
                         Internet (gov stakeholders)          University network
                                    │                                │
                     ┌──────────────▼───────────────┐   ┌────────────▼───────────┐
                     │ Caddy (TLS, HSTS, rate-limit)│   │ internal vhost         │
                     │  chip.<domain>               │   │  superset.chip.internal│
                     └──────┬──────────────┬────────┘   │  neo4j-browser (ro)    │
                            │ /            │ /api,      └────────────┬───────────┘
                            ▼              ▼ /internal(IP-allowlist) ▼
                     ┌────────────┐  ┌──────────────┐        ┌──────────────┐
                     │ SPA static │  │ serving-api  │        │ superset     │
                     │ (Caddy fs) │  │ uvicorn ×4   │        └──────────────┘
                     └────────────┘  └──────┬───────┘
                                            │            ┌──────────────────┐
                                     ┌──────▼───────┐    │ postgres · neo4j │
                                     │serving-worker│───▶│ · minio (existing│
                                     │  (1 replica) │    │  data-layer hosts│
                                     └──────────────┘    └──────────────────┘
                                     + postfix relay container (queue → NUCES smarthost)
```

Compose-managed containers on the lab servers; serving-api and serving-worker are the same image. TLS via Let's Encrypt on the public hostname (or a university-issued cert if DNS/CAA policy requires — §10). `/internal/*` and Superset are never routable from the public vhost. Backups: nightly `pg_dump` of the `serving` schema to MinIO (gold/Neo4j backup is the data subsystem's job). Monitoring: `/metrics` (Prometheus format) scraped by the platform's existing stack; uptime check on `/readyz` which verifies Postgres-gold, Postgres-serving, Neo4j, and MinIO connectivity.

---

## 10. Open questions

1. **NIH data-sharing terms** — the actual sensitivity tiers, permitted resolutions, and whether PDMAs may see NIH district data are unknown until the MoU lands. §5.4 is designed to absorb any answer, but grant rows can't be written until the agreement text exists. *Owner: PI; blocks nothing technically.*
2. **Public hostname, DNS, and TLS** — do we get `chip.nu.edu.pk` (needs campus IT delegation, SPF/DKIM for the mail subdomain, CAA compatibility with Let's Encrypt) or must we buy a neutral domain? *Blocks external stakeholder onboarding.*
3. **University SMTP relay policy** — will NUCES IT allow an authenticated smarthost + DKIM for `chip.nu.edu.pk`? If refused, fallback is a low-cost commercial relay, which needs a procurement conversation.
4. **Province-scoped tenancy depth** — do PDMAs require that Punjab PDMA sees *only* Punjab districts (visibility scoping beyond restricted datasets)? Current model can express it via dataset partitioning by province, but confirm before the marts are cut.
5. **Urdu terminology source of truth** — who signs off disease/indicator Urdu terms (NIH glossary? PMD?), and Nastaliq vs Naskh preference of actual stakeholder readers. Needs one workshop decision; affects locale files and brief templates.
6. **Brief sign-off protocol** — is PCN researcher approval sufficient for a brief addressed to a ministry, or does the PI (or a partner focal person) need to be in the approval chain? Governance, not code.
7. **WhatsApp business verification entity** — if Phase 2 proceeds, which legal entity gets Meta-verified (NUCES? a partner institution?)? Long administrative lead time; start the question early even though the build is deferred.
8. **Alert acknowledgement semantics** — is one ack per institution enough, or do NDMA workflows expect per-role acks / escalation timers? Shapes `alert_acks` uniqueness and the S3 UI.
9. **Neo4j query load isolation** — if researcher Bloom/Browser sessions ever degrade stakeholder-facing KG endpoints, do we need a read replica or query-queueing? Watch p95s during the first monsoon before spending anything.
10. **When does the human review gate come off?** Criteria proposal: one full monsoon season with < 2% summaries rejected on factual grounds → auto-publish `watch`-tier summaries, keep the gate for briefs and emergencies. Revisit with evidence, spring 2027.
11. **Public tier** — viewer-minus-login is architecturally cheap (role already exists), but publishing risk-signal data publicly is a policy decision with partner-relations consequences. Park until partners are comfortable.
