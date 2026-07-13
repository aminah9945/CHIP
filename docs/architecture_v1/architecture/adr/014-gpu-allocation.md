# ADR-014: GPU Allocation — Two 24 GB Cards, Split Serving / Batch

- **Status:** **Accepted** (2026-07-13)
- **Closes:** 03 OQ-9 ("GPU contention policy… who preempts whom").
- **Supersedes:** the "7–8B model on one 24 GB card" sizing in 07 §1.1/§1.2, and reconciles it with 03 §1.3 (Qwen3-30B-A3B Q4) and 04 §6.2 (Qwen3-14B/32B) — three documents that assumed three different machines.

## Context

Three subsystem docs independently sized the GPU and disagreed:

| Doc | Assumed |
|---|---|
| 07 §1.1 | LLM serving = **7–8B**, 4-bit → "~10–14 GB, fits alongside an embedding model on one 24 GB card" |
| 03 §1.3 | **Qwen3-30B-A3B Q4** (~17 GB) as the annotation teacher / causal fallback |
| 04 §6.2 | **Qwen3-14B or 32B** (~17–20 GB at Q4) as the graph-RAG workhorse |

Meanwhile the workloads that must **coexist** are:

| Workload | VRAM | Duty cycle |
|---|---|---|
| XLM-R NER + RE + causal classifier (Triton) | ~4 GB | Resident, online |
| BGE-M3 embedder (ADR-011) | ~3 GB | Resident, online |
| Graph-RAG LLM, interactive | 8 GB (7–8B) … 20 GB (32B Q4) | Resident, working hours |
| LLM teacher batch (distillation, causal fallback, pre-annotation) | 17–20 GB | Off-peak batch |
| XLM-R fine-tuning | 12–24 GB | Periodic, hours |
| Historical enrichment backfill | saturates a card | One-off, days |
| GNN training (research track) | a few GB | Monthly |

Resident serving alone is **~15–18 GB**. Every batch workload wants **17–24 GB**. They do not fit on one 24 GB card together, and 03 correctly flagged this as unresolved.

## Decision

**Buy two 24 GB GPUs and split them by role.**

```
GPU 0 — "serving"                       GPU 1 — "batch / training"
  ├─ Triton: xlmr-ner, xlmr-re,           ├─ XLM-R fine-tuning
  │          xlmr-causal                  ├─ LLM teacher batch (distillation,
  ├─ Triton: bge-m3 embedder              │   causal fallback, pre-annotation)
  └─ vLLM:   graph-RAG LLM (7–14B AWQ)    ├─ Historical enrichment backfill
                                          └─ GNN training (PyG)
  ALWAYS RESIDENT. Never preempted.       SCHEDULED. Preemptible. May OOM freely.
  ~15–18 GB.                              Owns the whole card.
```

- **GPU 0 is never touched by a batch job.** This is what makes a live dashboard demo possible *while* the pipeline is running — which is precisely when stakeholders will be watching.
- **GPU 1 absorbs every heavy, bursty, crash-prone workload.** A fine-tune that OOMs takes down nothing a stakeholder can see.
- **Interactive RAG runs a 7–14B model, not a 32B.** The 30–32B class stays on GPU 1 as the **offline teacher** (03's distillation loop), where its quality-per-token is worth the latency. This is the correct split anyway: the teacher should be big and slow; the online path should be cheap and deterministic.

**Hardware:** 2 × RTX 3090 24 GB (used, budget path — already listed as an option in 07 §1.2) or 2 × RTX 4090 24 GB (~1.5–2× faster on inference-bound work). **24 GB is the number that matters; buy count over per-card capability.**

## Why 2 × 24 GB beats 1 × 48 GB

They look equivalent on paper (48 GB either way) and are not:

| | 2 × 24 GB (chosen) | 1 × 48 GB (e.g. A6000) |
|---|---|---|
| Failure isolation | **A fine-tune OOM on GPU 1 cannot touch serving on GPU 0** — separate devices, separate memory | An OOM in a training job can take down the resident serving process on the same device |
| Scheduling | Trivial: pin by `CUDA_VISIBLE_DEVICES`. No queueing system needed. | Requires MPS/MIG or a real scheduler to isolate, which a rotating student team will not maintain |
| Cost | 2 used 3090s are substantially cheaper | Higher, and the budget is NRPU-capped |
| Parallel throughput | Two independent streams (serve + train simultaneously) | One device, contended |

Failure isolation is the deciding argument. ADR-004's whole premise is survivability under a rotating student team; "a student's fine-tune crashed the stakeholder dashboard" is exactly the failure this project cannot afford.

## Alternatives rejected

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **2 × 24 GB, serving/batch split (chosen)** | Resident serving never preempted; batch isolated; cheap (used 3090s); no scheduler needed | Two cards to buy and cool | **Chosen** |
| 1 × 24 GB, time-sliced | Cheapest; survivable in Phase 0–2 (encoders resident, LLM batch nightly, small RAG model by day) | Breaks the moment the historical enrichment backfill runs *and* the dashboard must be live — which is when you demo. Also forces the RAG model down to 7–8B permanently. | **Rejected as the target**; acceptable as an interim if only one card is fundable at month 0 |
| 1 × 48 GB (A6000-class) | Single card; can host a 32B interactively | No failure isolation; needs MIG/MPS to be safe; more expensive; still one device | Rejected |
| 3+ GPUs / A100-class | Headroom | Nothing in CHIP needs it (07 §1.2 already says so); destroys the budget and power envelope | Rejected |

## Consequences

- 07 §1.1 sizing table and §1.2 hardware spec are updated: **`chip-gpu-01` ships with 2 × 24 GB, not 1.**
- 03's model roles are pinned to a device: the online encoder path (NER/RE/causal/embed) is **GPU 0**; the LLM teacher, fallback batches, and fine-tuning are **GPU 1**.
- 04 §6.2's "on 48 GB, host Qwen3-32B for EN" is revised: the interactive RAG model is **7–14B on GPU 0**; the 32B class is the **offline** teacher on GPU 1.
- Buying the hosted agentic parser (ADR-012) **saves a GPU workload** — no local VLM is needed for document parsing, which is what makes two cards sufficient rather than three.
- **A GPU-contention runbook is no longer needed.** The contention was the problem; the split removes it. That is the point.
