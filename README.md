# Climate–Health Intelligence Platform (CHIP)

**Layer 2 — Data Collection Connectors (Thin Connector Model)**

CHIP's Layer 2 is a thin archival gateway that discovers, fetches, and archives raw disease surveillance bulletins to MinIO (Bronze storage) and registers document metadata and extractor status in PostgreSQL.

---

## Prerequisites

- **Python 3.12+**
- **[uv](https://github.com/astral-sh/uv)** (Python package & project manager)
- **Docker & Docker Compose** (for running PostgreSQL and MinIO)

---

## Quickstart Guide

### 1. Set Up Python Environment

Install dependencies and set up workspace packages:

```bash
uv sync
```

### 2. Run All Tests

Run the full test suite (SDK unit tests, connector tests, integration, concurrency, and idempotency tests):

```bash
uv run pytest
```

---

### 3. Start Local Infrastructure (PostgreSQL + MinIO)

Start PostgreSQL (with automatic schema migration) and MinIO (with `chip-bronze` bucket initialization):

```bash
docker compose -f infra/docker/docker-compose.layer2.yaml up -d
```

Verify services are healthy:
- **PostgreSQL**: `localhost:5432` (User: `chip`, Password: `chip_password`, DB: `chip`)
- **MinIO API**: `localhost:9000` (User: `minioadmin`, Password: `minioadminpassword`)
- **MinIO Console**: [http://localhost:9001](http://localhost:9001)

---

### 4. Run Data Collection Pipelines (Backfill)

You can run the connectors either via the **Dagster CLI** or the **Dagster Web UI**.

#### Method A: Dagster CLI (Command Line)

Materialize all 4 connector assets to ingest all historical bulletins:

```bash
PYTHONPATH=libs/chip_connectors:connectors/ajk_idsrs:connectors/pitb_dss:connectors/dhis_punjab_weekly:connectors/nih_idsr:. \
uv run dagster asset materialize --select "*" -f pipelines/ingestion/definitions.py
```

Or materialize a specific connector asset:

```bash
# AJK IDSRS (3 bulletins)
PYTHONPATH=libs/chip_connectors:connectors/ajk_idsrs:connectors/pitb_dss:connectors/dhis_punjab_weekly:connectors/nih_idsr:. \
uv run dagster asset materialize --select raw_ajk_idsrs -f pipelines/ingestion/definitions.py

# NIH IDSR (174 bulletins)
PYTHONPATH=libs/chip_connectors:connectors/ajk_idsrs:connectors/pitb_dss:connectors/dhis_punjab_weekly:connectors/nih_idsr:. \
uv run dagster asset materialize --select raw_nih_idsr -f pipelines/ingestion/definitions.py
```

#### Method B: Dagster Web UI

Launch the interactive web UI:

```bash
PYTHONPATH=libs/chip_connectors:connectors/ajk_idsrs:connectors/pitb_dss:connectors/dhis_punjab_weekly:connectors/nih_idsr:. \
uv run dagster dev -f pipelines/ingestion/definitions.py
```

Open [http://localhost:3000](http://localhost:3000) in your browser, navigate to **Assets**, select all ingestion assets, and click **Materialize selected**.

---

### 5. Verify Archived Data & Database Receipts

#### Inspect PostgreSQL Database

Connect via `docker exec` using the PostgreSQL container:

```bash
docker exec -it chip-postgres psql -U chip -d chip
```

Run verification queries:

```sql
-- Check total archived raw documents per source
SELECT source, count(*) FROM ingestion.raw_documents GROUP BY source;

-- Check pending work items seeded for Layer 3 extractors
SELECT extractor_name, status, count(*) FROM ingestion.extractor_status GROUP BY extractor_name, status;

-- View operational run audit logs
SELECT run_id, source, discovered, archived, skipped_identity, errors, duration_ms FROM ingestion.connector_runs;
```

#### Inspect MinIO Bronze Objects

Log in to the MinIO Console at [http://localhost:9001](http://localhost:9001) (`minioadmin` / `minioadminpassword`) and browse the `chip-bronze` bucket.

Or use `docker exec` with the `mc` CLI:

```bash
docker exec -it chip-minio-init mc ls myminio/chip-bronze/
```

---

## Monorepo Directory Structure

```
.
├── Data_sources_1/                  # Static raw data directory (NIH, PITB-DSS, AJK, DHIS)
├── connectors/                      # Source-specific thin connector packages
│   ├── ajk_idsrs/                   # AJK IDSRS connector
│   ├── dhis_punjab_weekly/          # DHIS Punjab weekly connector
│   ├── nih_idsr/                    # NIH IDSR connector
│   ├── pitb_dss/                    # PITB-DSS connector
│   └── README.md                    # Guide on adding new connectors
├── docs/                            # Architectural specifications & implementation plans
├── infra/
│   └── docker/
│       └── docker-compose.layer2.yaml # Local Postgres + MinIO stack
├── libs/
│   └── chip_connectors/             # Shared Connector SDK (base, bronze, dedup, handoff, runner)
├── migrations/
│   └── V001__layer2_ingestion_schema.sql # Database DDL & extractor seed data
├── pipelines/
│   └── ingestion/                   # Dagster asset definitions, resources & checks
├── pyproject.toml                   # Root monorepo configuration (uv workspace)
└── tests/                           # SDK unit tests & integration test suite
```

---

## Development Reference Commands (Makefile Shortcuts)

You can use `make` shortcuts for all common development tasks:

| Command | Action |
|---|---|
| `make help` | Show list of available commands |
| `make setup` | Install dependencies (`uv sync`) |
| `make test` | Run full test suite (`uv run pytest`) |
| `make up` | Start local Postgres + MinIO stack (`docker compose up -d`) |
| `make down` | Stop local Postgres + MinIO stack |
| `make backfill` | Run historical data ingestion via Dagster CLI |
| `make ui` | Launch Dagster web UI on http://localhost:3000 |
| `make db-status` | Query Postgres for document, extractor status, and run counts |
| `make clean` | Clean python cache files |

