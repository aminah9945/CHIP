# ADR-001: Service Topology — Event-Driven Pipeline + Modular Serving Core

- **Status:** Accepted (2026-07-10, confirmed by project lead)
- **Context:** CHIP is a data-integration platform with heterogeneous workloads: scheduled scrapers, GPU NLP enrichment, graph builds, model training, and an always-on stakeholder dashboard. The dev team is rotating MS/PhD students. Data volume is modest (~160 districts, weekly bulletins, low-thousands of news articles/day) — the operational risk is entropy and turnover, not load.

## Decision

Adopt an **event-driven pipeline architecture** with 5–7 coarse-grained deployables around a Kafka backbone, plus **one modular-monolith serving application**:

```
connectors (per source) → Kafka → normalizers (geo/temporal) → enrichers (NLP/NER)
                                        ↓                            ↓
                              storage zones (bronze/silver/gold)  KG builder → Neo4j
                                        ↓                            ↓
                          district × epi-week panel (gold)      graph-RAG
                                        ↓                            ↓
                     serving core (modular monolith): FastAPI API + alerts + dashboard
```

- Pipeline stages are independently deployable, restartable, and replayable from Kafka.
- The serving layer (API, auth, alerting, RAG serving, dashboard backend) is ONE codebase with enforced internal module boundaries.
- Unit of decomposition is **data domain + pipeline stage**, not business capability.

## Alternatives rejected

- **Full microservices:** solves problems CHIP doesn't have (many teams, independent scaling domains); operational tax (tracing, discovery, network failure modes) is unaffordable for a rotating student team.
- **Single monolith:** cannot give failure isolation or independent scheduling to workloads as different as scrapers, GPU inference, and a public dashboard.

## Consequences

- New data source = new connector plugin; nothing else changes (expandability axis #1).
- Students can own one pipeline stage end-to-end without understanding the whole system.
- Kafka becomes a hard dependency for inter-stage communication (accepted; it is proposal-committed).
