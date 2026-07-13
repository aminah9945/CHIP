# ADR-008: Epidemiological-Week Convention — WHO/ISO (Monday-start), Validation-Gated

- **Status:** Accepted (2026-07-10 — reconciliation decision; locks the *default* and the process, resolves a cross-subsystem conflict). The empirical confirmation (OQ-1) remains open and can flip the default cheaply.
- **Context:** ADR-005 and subsystem 01 §2 default to **WHO/ISO (Monday-start)**. But analytics (05 §2.1) states "MMWR is standard for US-style IDSR" and models on **MMWR (Sunday-start)**, and the CHKG (04) leaves it open. NIH IDSR bulletins print "Week N" **without** start/end dates, so the true convention is unverified. Two subsystems assuming different week starts silently misalign every disease/climate/media join at year boundaries.

## Decision

1. **Platform-wide default: WHO/ISO, Monday-start, week 1 contains the year's first Thursday.** Rationale: Pakistan's IDSR runs under WHO/EMRO guidance, and WHO's standard epidemiological week is Monday–Sunday.
2. **`libs/epiweek` is convention-parameterized** (`"iso"` = WHO/Monday default, `"cdc"` = MMWR/Sunday switchable via env), wrapping the `epiweeks` PyPI package. It is the **only** place week↔date math happens.
3. **Canonical key everywhere: `epiweek_id = year*100 + week`** (e.g. `202526`), used as the FK across all facts, the CloudEvents `chip_epiweek` attribute, KG `EpiWeek.epi_week_id`, and API `2026-Www` strings (rendered from the same id).
4. **Validation gate (OQ-1):** before `dim_epiweek` is frozen, reconcile several known `(week, year)` labels against dated WHO-EMRO/NIH material (or ask NIH directly). If NIH turns out to follow MMWR, flip the default to `"cdc"` — a one-line config change, because both systems are built in.
5. **Analytics (05) conforms to WHO/ISO**, not MMWR; sub-weekly data (weather, news) keeps native timestamps and maps to an `epiweek_id` via `from_date()`.

## Alternatives rejected

- **MMWR/CDC (Sunday-start) as default:** the US IDSR tradition and supported by `epiweeks`, but Pakistan's surveillance sits under WHO/EMRO, so MMWR would most likely misalign to the actual NIH bulletins — the opposite of the goal. Kept as the switchable fallback in case validation says otherwise.
- **Pure ISO-8601 with no epi-year semantics:** effectively identical week boundaries to WHO here, but loses the epi-year handling at boundaries that the `epiweeks` package gives for free.
- **Freezing a convention now without the empirical check:** faster, but risks silently encoding the wrong week start into ten years of backfilled panel data — the one error that is expensive to unwind later.

## Consequences

- Supersedes 05 §2.1's MMWR assumption; 04's open item is resolved to WHO/ISO (validation-gated).
- `dim_epiweek` cannot be frozen until OQ-1 is closed; the golden-table unit test (01 §2.1) guards against silent drift after.
- Because the library is parameterized and the key is `year*100+week` regardless of system, a late flip re-materializes `dim_epiweek` and re-buckets sub-weekly data without schema change.
