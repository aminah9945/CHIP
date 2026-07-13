# Subsystem 05 — Analytics, Forecasting & Alerting

**Project:** CHIP (Climate–Health Intelligence Platform), NRPU / PCN research group, NUCES/FAST Islamabad
**Grain:** district × ISO epidemiological week (~160 districts × weekly)
**Diseases (v1):** dengue, malaria, cholera, other diarrhoeal disease, acute respiratory infection (ARI)
**Owner:** analytics/ML architecture
**Status:** design (v1), binding for the production pipeline; research tracks may extend but not bypass it

---

## 0. Purpose, scope, and design philosophy

This subsystem turns the harmonised gold panel (district × epi-week) into three products, in strict order of scientific defensibility:

1. **Descriptive intelligence** — spatio-temporal patterns, climate–health associations, situational monitoring.
2. **Forecasts** — probabilistic disease-count forecasts 1–8 weeks ahead per district.
3. **Alerts** — human-reviewed, evidence-linked early warnings issued to NIH / NDMA / Ministry of Climate Change.

### Non-negotiable design principles

- **Credibility ladder, not model zoo.** We ship the simplest defensible model that works and only climb to more complex models when they beat the rung below on a pre-registered metric. The default deliverable to stakeholders is a **negative-binomial GLM with distributed-lag climate terms**, not an LSTM. Deep models are a research bet, not the product spine.
- **Baselines are first-class citizens.** A seasonal-naive forecaster and the GLM baseline are computed for *every* district-week and stored forever. No model is reported as "good" in the absence of these two numbers next to it.
- **Vintage-aware everything.** IDSR counts are revised for weeks after first report. Every training row, every backtest, every alert-time evaluation uses **only data that was actually available as of the decision time**. This is the single most common way climate-health forecasting papers fool themselves; we design against it from row zero.
- **Probabilistic, count-native.** Disease counts are non-negative, over-dispersed, and often small. We predict *distributions* (negative binomial / quantiles), score them with proper scoring rules (CRPS / log score), and never report only a point MAE.
- **Weekly batch, not real-time.** The grain is weekly. There is no operational need for streaming inference in this subsystem. We say so explicitly (Section 6) and spend the saved complexity budget on evaluation rigor and explainability.
- **Explainability is a data contract, not a slide.** Every alert carries machine-readable evidence (which features fired, which climate anomaly, which knowledge-graph path). If we cannot explain it, we do not issue it.

### Position in the platform

```
gold panel (TimescaleDB)  ──►  [05 analytics/forecasting/alerting]  ──►  forecast + alert tables
   (subsystem 03/04)              descriptive │ models │ anomaly │ alert SM        │
                                        │            │                            ▼
   Climate–Health KG  ◄──── evidence links ─┘            dashboard + policy briefs (subsystem 06)
   (subsystem 04)
Orchestration: Dagster (weekly) · Tracking: MLflow · Compute: self-hosted lab + 1–2 workstation GPUs
```

---

## 1. Modeling roadmap (ordered by scientific credibility)

The roadmap is a **ladder**. Each rung must be built, evaluated, and beaten before the next rung is promoted to production. All rungs remain in the codebase as baselines forever.

### Model specification table

| # | Layer | Model | Target / output | Primary role | Data sufficiency | Production status (v1) |
|---|-------|-------|-----------------|--------------|------------------|------------------------|
| L0 | Descriptive | STL / seasonal decomposition, cross-correlation maps, association plots | none (EDA) | Understand structure, generate hypotheses, QA the panel | Always | **Production** |
| L1 | Baseline | **Seasonal-naive** (count in same epi-week last year, dispersion from history) | NB predictive dist. | Floor every other model must beat | Always | **Production (mandatory baseline)** |
| L2 | Baseline | **Negative-binomial GLM + DLNM** climate cross-basis, harmonic seasonality, population offset, district effects | NB predictive dist., relative-risk curves | **Primary epidemiological product** + inference (RR of temp/precip anomalies) | Good (pooled) | **Production (primary model)** |
| L3 | Baseline | **Prophet** (per district, with regressors) | point + interval | Interpretable per-district trend/seasonality baseline; committed method | Moderate | **Production (secondary baseline)** |
| L4 | ML | Global gradient-boosted quantile model (LightGBM via `mlforecast`) | quantiles | Strong non-linear pooled baseline; often the one to beat | Good (global) | Candidate |
| L5 | Deep | **LSTM / deep temporal** (global, probabilistic — DeepAR/LSTM, optionally NBEATSx/TFT) | NB / quantile dist. | Committed method; only if it beats L2–L4 | Marginal — see §1.4 | **Research track** |
| L6 | Fusion | Media-signal-augmented variants of L2/L4/L5 | as parent | Test lead-time value of news signals | Moderate | **Research track → promote if lead-time proven** |

Committed proposal methods (descriptive spatio-temporal, LSTM, Prophet, GLMs for relative risk, lag/DLNM exposure models, anomaly/surge detection, KG-grounded explainable alerts) are all present: L0, L5, L3, L2, L2, Section 4, Section 5 respectively.

### 1.1 Descriptive / exploratory layer (L0)

Ships first and underpins everything. Concrete outputs, all reproducible from config:

- **Spatio-temporal incidence maps** (district choropleths per epi-week; animated over season), incidence = cases / population × 100k.
- **Seasonal decomposition** (STL) per district-disease to separate trend, annual/monsoon seasonality, remainder — the remainder is the raw material for anomaly detection.
- **Climate–health lagged cross-correlation** panels: correlation of disease residual against temperature/precip/humidity at lags 0–12 weeks, per district, to empirically confirm the lag windows we later hard-code into DLNM (sanity, not model fitting).
- **Structural-break markers**: 2022 monsoon floods, COVID-era reporting collapse, heatwave weeks — flagged as covariates and as evaluation-fold boundaries.
- **Data-quality panels**: reporting completeness, revision magnitude (vintage lag), zero-inflation and dispersion diagnostics per district-disease. These decide which districts/diseases even qualify for forecasting in v1.

### 1.2 Classical epi baseline FIRST — Negative-binomial GLM with DLNM (L2, primary product)

This is the scientific spine and what we defend to epidemiologists. **Build and beat this before touching deep learning.**

**Structure (per disease; pooled across districts with district effects):**

```
cases_{d,t} ~ NegativeBinomial(mu_{d,t}, alpha)
log(mu_{d,t}) = log(pop_{d,t})                         # population offset → models incidence
              + cb_temp(temp_{d, t-0..L})              # DLNM cross-basis, non-linear × lag
              + cb_precip(precip_{d, t-0..L})          # DLNM cross-basis
              + cb_humidity(...)                        # optional per disease
              + s_seasonal(week_of_year)               # cyclic harmonics / natural cubic spline
              + f(year)                                 # slow trend
              + hazard_flags_{d,t}                      # flood/heatwave/drought active or lagged
              + district_effect_d                       # fixed effect or random intercept (mixed GLM)
              + AR term (log cases_{d,t-1..k}, optional, endemic-style)
```

- **Cross-basis (DLNM)** simultaneously models the *non-linear* exposure–response and the *delayed* effect. Lag window `L` is disease-specific and biology-informed (Section 2.2): dengue/malaria up to **12 weeks** (vector breeding + extrinsic + intrinsic incubation), cholera/diarrhoeal **0–4 weeks** (short incubation, flood-driven contamination), ARI **0–3 weeks** (temperature-driven).
- **Over-dispersion** handled natively by the NB `alpha`; if zero-inflation is severe for a district-disease (many structural zeros in low-transmission districts) fall back to a hurdle/ZINB variant for that stratum only.
- **Inference product**: DLNM yields **relative-risk curves** (RR vs lag, RR vs exposure value with confidence bands) — e.g. "RR of dengue at +3 °C above district norm, peaking at 6–8 week lag." This is a *committed deliverable* and directly feeds the KG and policy briefs, independent of forecast accuracy.

**Tooling (verified).** Distributed-lag non-linear models originate in R's `dlnm` (Gasparrini). In Python, use the cross-basis port **`pydlnm`** or the PyPI **`crossbasis`** package to generate the cross-basis matrix, then fit with **statsmodels** `GLM(family=NegativeBinomial)` / discrete `NegativeBinomial`, or `glum` for penalized/regularized GLMs; mixed-effects via `statsmodels` or `pymer4`. Splines via `patsy` / natural cubic bases. **Decision:** primary implementation in Python (`crossbasis`/`pydlnm` + statsmodels) so it lives in the standard pipeline; keep an **rpy2 bridge to R `dlnm`** as the *reference implementation* to validate the Python cross-basis numerically on a fixed dataset (one-time + CI regression test). This is the safe path — the R package is the field standard and reviewers will trust a cross-check against it.

### 1.3 Prophet as interpretable per-district baseline (L3)

Prophet is a committed method and a good *operator-facing* baseline: decomposable (trend + yearly/monsoon seasonality + holiday/hazard regressors), robust to gaps, easy for grad students to run per district. **Role and honest limits:**

- One model **per district-disease** (Prophet is univariate; no cross-district pooling → weak for short-history/low-count districts).
- Add climate as **extra regressors** with pre-computed lags (Prophet has no native distributed-lag; we feed engineered lagged/anomaly features from Section 2).
- Model in a variance-stabilised space (e.g. fit on `log1p(cases)` or use Prophet's multiplicative seasonality) since native Prophet assumes roughly Gaussian noise — a poor fit for small counts. Report intervals but treat them as approximate; **the GLM/quantile models own the probabilistic scoring**, Prophet owns interpretable decomposition.
- Prophet is a **baseline and a communication tool**, not the primary forecaster. If it beats L2 on CRPS for some districts, that is a finding, not the default.

### 1.4 LSTM / deep temporal models — only after baselines are beaten (L5)

**Honest data-sufficiency assessment (read before building).** We have ~160 districts × a few hundred weeks. Per-district that is **~200–400 points** — far too little to fit a per-district LSTM without overfitting. Deep models are therefore only viable as **global (pooled) models** that learn one set of weights across all districts, treating district as an embedding/covariate and exploiting the panel's cross-sectional breadth. Even then, the effective signal is modest and dominated by strong annual seasonality that L2–L4 already capture cheaply. **We expect deep models to struggle to beat a well-specified GLM/GBM and we design the evaluation to make that comparison honest, not flattering.**

Rules for the deep track:

- **Global only.** No per-district deep nets in v1. District enters as a learned embedding; static covariates (population, ecological zone, urban/rural) as features.
- **Probabilistic output required.** Use a count/quantile head (DeepAR-style NB likelihood, or quantile loss), not plain MSE on counts. Candidate stack: **`neuralforecast`** (LSTM, DeepAR, NBEATSx, TFT) which gives probabilistic heads and global training out of the box; GPU jobs fit comfortably on 1–2 workstation GPUs at this data scale.
- **Promotion gate.** A deep model is promoted to production only if it **beats L2 and L4 on mean CRPS with a paired test across rolling-origin folds AND does not lose to seasonal-naive in any disease-region stratum** (Section 3). Otherwise it stays a thesis result with an honest "did not beat baseline" write-up — which is itself a publishable, credible finding.
- **Structural breaks.** LSTMs extrapolate poorly across regime shifts (2022 floods). We explicitly test out-of-regime skill and never claim performance that leaks post-break information.

### 1.5 Media-signal fusion — how news enters and how lead-time is measured (L6)

The proposal's headline hypothesis is that **news signals lead official surveillance**. We treat this as a *measurable claim*, not an assumption.

**How media signals enter models:**

- Media features (district-week mention counts, disease-topic intensity, hazard-narrative counts, media-surge z-scores — Section 2.4) enter L2/L4/L5 as **additional lagged covariates**, at lags 0–4 weeks (media is expected to *lead*, so *contemporaneous and short* lags matter most).
- Two model variants are always run head-to-head: **(A) climate+surveillance only** and **(B) climate+surveillance+media**. The value of media is `skill(B) − skill(A)`.

**How lead-time value is measured (the actual experiment):**

1. **Predictive-lead test.** In the GLM, include media at lags 0–4 and report which lag's coefficient is significant and its sign. A significant *positive* coefficient at lag ≥1 with media leading cases is direct evidence of lead-time.
2. **Granger-style / transfer-entropy check** on media→cases residuals per district, controlling for climate and seasonality (descriptive, hypothesis-level).
3. **Forecast-skill delta at the horizon that matters.** Compare CRPS of A vs B specifically for the **1–2 week-ahead nowcast** during outbreak onsets, since that is where a leading indicator pays off. Report the *timeliness gain* (Section 4) — median days/weeks earlier that variant B crosses the alert threshold vs variant A, on the outbreak gold standard.
4. **Guard against reverse causation / media echo.** News often *reacts* to official outbreaks (a lagging echo, not a leading signal). We test whether media adds skill *before* case counts rise, not after, and down-weight media features whose predictive mass sits at lag 0 co-incident with case surges.

Media fusion stays **research track** until (3) shows a positive, stable timeliness gain; then the media-augmented variant is promoted for the diseases where it proved out.

---

## 2. Feature engineering — the gold panel spec

### 2.1 Panel spine

One row per **(district_id, disease, epi_week)**, where `district_id` is the **COD-AB P-code** (ADR-006) and `epi_week` uses the **WHO/ISO Monday-start** convention (ADR-008: parameterized in `libs/epiweek`, validation-gated by OQ-1; *not* MMWR — the earlier MMWR note here is superseded), keyed by `epiweek_id = year*100 + week`. Keys join to the gold layer in TimescaleDB (hypertable partitioned on time, district as space dimension).

### 2.2 Lag structure (incubation- and biology-informed)

Climate acts on disease with delays from vector ecology + incubation. We hard-code **disease-specific maximum lag windows** and let DLNM place the smooth basis inside them:

| Disease | Dominant pathway | Climate lag window (weeks) | Rationale |
|---------|------------------|----------------------------|-----------|
| Dengue | Rain→breeding sites; temp→vector & viral extrinsic incubation | **0–12** | mosquito development (weeks) + extrinsic incubation (8–12 d) + intrinsic (4–7 d) |
| Malaria | Rain→breeding; temp→parasite sporogony | **0–12** | similar vector-borne cascade |
| Cholera | Flood/heavy rain→water contamination | **0–4** | very short incubation (hrs–5 d); flood-driven |
| Diarrhoeal (other) | Temp→pathogen growth; water quality | **0–4** | short incubation |
| ARI | Cold/temperature swings, smog | **0–3** | short latency, direct exposure |

**Engineered lags** for non-DLNM models (Prophet, GBM, LSTM inputs): for each climate var create lags `t-0 … t-L` and rolling means over `[t-3,t-0]`, `[t-8,t-4]` windows (short-recent vs delayed-exposure buckets). For counts, autoregressive features `cases_{t-1..t-4}` and same-week-last-year — **all computed vintage-safe (Section 2.5).**

### 2.3 Climate anomaly features (not just raw values)

Disease responds to **departures from local normal**, not absolute values. Compute per district against a climatological baseline (rolling multi-year, epi-week-of-year specific):

- **Temperature anomaly** `T_anom = T - clim_T(week, district)`; heat metrics: consecutive days > local 95th pctile, weekly max, diurnal range.
- **Heatwave indicator**: binary + intensity (degree-weeks above threshold), aligned to NDMA/PMD heatwave definitions where available.
- **Precipitation anomaly / SPI-like index**: standardised precipitation index over 4/8/12-week accumulation windows (z-score of accumulated precip vs climatology). Captures both drought (negative) and flood-precursor (positive) regimes.
- **Humidity** anomaly and dew-point where available.
- **Hazard-event features** (from NDMA/PDMA): flood active flag + weeks-since-flood-onset; drought index; each with the disease-appropriate lag.
- **Reanalysis vs station provenance flag** per feature (reanalysis-derived vs station-observed) so models and reviewers know data quality per row.

### 2.4 Media-surge features (from the NLP pipeline)

Per district-week, from subsystem NLP outputs:

- `media_mentions[disease]` — raw count of disease-topic mentions.
- `media_surge_z[disease]` — z-score of mentions vs district's own rolling media baseline (controls for districts/outlets that are just chattier).
- `media_hazard_mentions` — flood/heatwave/water-shortage narrative counts.
- `media_topic_intensity` — normalized topic weight for disease/hazard clusters.
- Short lags 0–4 weeks of the above (media expected to lead).
- **Exposure normalization**: divide by district total news volume to avoid conflating "more news overall" with "more disease news."

### 2.5 Leakage prevention — vintage / as-of joins (the make-or-break rule)

> ### ⚠️ Revised 2026-07-13 (ADR-015): **we do not yet know whether IDSR revises at all — so we measure it before building for it.**
>
> This section's premise — *"IDSR counts are **revised** for several weeks after first report"* — is an **assumption**, and OQ-3 below admits it. The project lead's current belief is that **NIH bulletins do not reprint prior weeks' numbers.** Both cannot drive the design.
>
> **ADR-015 settles the method: instrument the UPSERT, don't assume.** The normalizer writes an `ingestion_revision` row whenever an incoming value differs from a stored one, capturing both values and both `bronze_uri`s. Cost: ~10 lines. It commits us to nothing, and the historical backfill answers it for free.
>
> **This cannot be discovered later in the KG.** The CHKG is built *from* the gold panel (04 §2.1). If the normalizer overwrote week 39's count when a later bulletin restated it, **the revision was destroyed before the graph ever saw it.** The UPSERT is the only place a revision is observable.
>
> | If `ingestion_revision` is… | Then | Consequence for this section |
> |---|---|---|
> | **Empty** | NIH publishes a **single vintage** — the first-reported value, never restated | **Most of the bitemporal machinery below is unnecessary.** There is no revision, therefore no leakage: training data, decision-time data, and target are all the same provisional vintage. **This is a simplification, not a problem.** |
> | **Non-empty** | Each bulletin is a dated snapshot of what was known then | The archive of weekly PDFs **is** a complete vintage record (`knowledge_time` = the publishing bulletin's epi-week). Everything below applies in full. |
>
> **Either way, the parser must extract each bulletin's FULL retrospective table** (02 §2.1), not just the current-week row — cheap insurance, and the only way the vintage record survives if it turns out to exist.
>
> #### The consequence nobody had stated: what CHIP is actually forecasting
>
> **If revisions do not exist, CHIP forecasts *first-reported* counts, not truth.** That must be said out loud in three places:
> - **§3.4's outbreak gold standard** is currently defined on *"retrospective, revision-mature counts."* **If no mature vintage exists, that definition is unbuildable** and must be redefined on provisional counts (or on NIH's own declared outbreaks). **This changes every operational metric in §3.2 — settle it before the evaluation harness is frozen.**
> - **Every model card** states the vintage of its target.
> - **Every stakeholder-facing forecast** is labelled a forecast of *reported* cases, not *true incidence*. Under-reporting is a property of the surveillance system, not a bug in the model — and conflating the two is how a platform loses institutional trust.

IDSR counts **may be revised** for several weeks after first report; climate reanalysis certainly is restated. If training uses final revised values but the model would only have had provisional values at decision time, every reported metric is optimistic and the system will underperform in production. The rules below are the defence — **and remain correct whenever revisions do occur (reanalysis restatement is confirmed, regardless of what IDSR does).**

**Rules, enforced in code (not by convention):**

1. **Bitemporal storage.** The gold panel stores `(value, valid_week, knowledge_time)` — i.e. every observation is versioned by *when it became known*. TimescaleDB table keeps all vintages, not just latest.
2. **As-of join contract.** Every feature build takes a `decision_time` argument and joins **only rows with `knowledge_time <= decision_time`**. There is a single `build_features(decision_time)` function; nothing bypasses it.
3. **Target definition is explicit about vintage.** We predict the count *as it will eventually be revised* (final) but we may only *train/score at a horizon where that vintage is stable*, or we predict the provisional-and-model-the-revision. Decision (v1): **target = value at a fixed maturity (e.g. count as known 8 weeks after the week), and we forecast that**; nowcasting the current provisional count is a separate, labelled task.
4. **No same-week leakage.** Features for predicting week `t` at decision time `t` may not use any `knowledge_time > t`. Rolling stats and climatology baselines exclude the target week's future.
5. **Backtest = replay of vintages.** The rolling-origin backtest reconstructs, at each origin, exactly the data snapshot that existed then (Section 3). A backtest that reads today's revised table is rejected in review.

### 2.6 Example feature list (per district-week row, dengue illustration)

| Feature | Type | Lag/window | Source | Leakage-sensitive |
|---------|------|-----------|--------|-------------------|
| `cases_lag1..4` | count | t-1..t-4 | IDSR (as-of) | **yes (vintage)** |
| `cases_sameweek_lastyear` | count | t-52 | IDSR (as-of) | yes |
| `temp_anom_cb` | DLNM cross-basis | t-0..t-12 | PMD/reanalysis | mild (reanalysis restatement) |
| `precip_spi8` | float | 8-wk accum | PMD/reanalysis | mild |
| `precip_cb` | DLNM cross-basis | t-0..t-12 | PMD/reanalysis | mild |
| `heatwave_intensity` | float | t-0..t-3 | PMD/NDMA | mild |
| `flood_active`, `weeks_since_flood` | bin/int | t-0..t-8 | NDMA/PDMA | no |
| `humidity_anom` | float | t-0..t-6 | PMD | mild |
| `media_surge_z_dengue` | float | t-0..t-4 | NLP pipeline | **yes (media vintage)** |
| `media_hazard_mentions` | count | t-0..t-4 | NLP pipeline | yes |
| `pop` (offset), `urban_frac`, `eco_zone` | static | — | census/geo | no |
| `week_of_year_sin/cos` | cyclic | — | calendar | no |
| `report_completeness`, `provenance_flag` | quality | t | pipeline meta | no |

---

## 3. Evaluation protocol

The evaluation protocol is **pre-registered and identical for every model** so comparisons are honest. Implemented once as a shared harness; models plug in.

### 3.1 Rolling-origin cross-validation at district-week grain

- **Expanding-window, rolling-origin** ("backtesting"): pick origins every 4 weeks across the usable history; at each origin train on all vintage-correct data up to the origin, forecast horizons `h = 1..8` weeks, record the predictive distribution per district.
- **Vintage replay** (Section 2.5): each origin uses the data snapshot as it existed then. This is the whole point — no peeking at revisions.
- **Blocked by season**: ensure folds cover monsoon onsets, inter-epidemic troughs, and at least one structural-break period (2022 floods) evaluated *out-of-regime* (train before, test across).

### 3.2 Metrics — proper scoring for counts, plus operational metrics

**Statistical (distributional) accuracy — primary:**

- **CRPS** (Continuous Ranked Probability Score) — main headline metric; generalizes MAE to distributions. Python: **`properscoring`** or **`scoringrules`** (`crps_ensemble` for sample-based deep models; closed forms for NB where available).
- **Log score** (negative log predictive density under NB) — sharper penalty, sensitive to tail misses.
- **Count-appropriate alternatives** for cross-checking: Dawid–Sebastiani score and the **ranked probability score for counts** (as in the R `surveillance::scores` / `scoringRules`) — reproduce via `scoringrules`/rpy2 where needed.
- **Calibration**: PIT histograms / reliability of prediction intervals (are 80% intervals right 80% of the time?). A model that is accurate but miscalibrated is not shippable for alerting.
- **Point-metric companions** (report but never decide on these alone): MAE, and scale-free MASE vs seasonal-naive.

**Operational (decision) accuracy — co-primary, because the product is alerts:**

Against a **defined outbreak gold standard** (Section 3.4):

- **Outbreak-detection sensitivity** (recall of true outbreak weeks) and **specificity** / **false-alarm rate** (1 − specificity, and false alarms per district-year).
- **Timeliness**: median lead time (weeks) between the alert firing and the outbreak's confirmed onset; earlier is better, but only counts if the alarm is true.
- **PPV** at the operating threshold — because stakeholder trust dies on false alarms.
- Summarize with a **sensitivity-vs-false-alarm trade-off curve** (like an ROC but per-district-year alarm rate on the x-axis) to choose operating points per disease.

### 3.3 Spatial holdouts

Beyond temporal CV, do **leave-districts-out** CV: hold out whole districts (and whole ecological regions) to test whether a **global model generalizes to districts it never trained on** — critical because some districts have almost no history and will rely on borrowed strength. Report skill separately for high-history vs low-history districts.

### 3.4 Outbreak gold standard (defined once, up front)

Operational metrics need a label for "was there really an outbreak in district d, week t?" We define it before modeling to avoid circularity:

- **Primary**: retrospective, revision-mature counts exceeding a disease-specific epidemic threshold (e.g. classic **endemic-channel / mean+2SD** of the seasonal baseline, or a moving-percentile threshold), confirmed to persist ≥2 weeks.
- **Cross-reference** with any officially declared outbreaks / NIH bulletin events where available (preferred when it exists).
- Labels are frozen and versioned; the gold standard is *not* derived from any model being evaluated.

### 3.5 Comparison discipline

- **Every model reports against two fixed baselines in the same table: seasonal-naive (L1) and GLM (L2).** A model that does not beat both on CRPS is not promoted, full stop.
- **Paired tests across folds** (e.g. paired comparison of per-origin CRPS) to check the improvement is not noise; report skill scores (CRPS relative to seasonal-naive).
- **Stratified reporting**: by disease, by ecological region, by high/low history, by in-regime vs out-of-regime. A model that wins on average but loses catastrophically in flood weeks is flagged.

### 3.6 Reproducibility

- **MLflow** tracks every run: params, the exact feature-config hash, the data-vintage snapshot id, per-fold CRPS/log-score/operational metrics, artifacts (calibration plots, RR curves).
- **Config-driven experiments** (one YAML per experiment; no notebook-only results in the record). The config hash + vintage id makes any number re-derivable.
- **Model cards** (one per promoted model): intended use, training data window, features, metrics vs baselines, known failure modes (e.g. "underestimates during flood-regime shifts"), calibration, and the promotion decision + reviewer. Required before any model touches the alerting path.
- **Seeded, deterministic pipelines** where feasible; deep-model seeds logged.

---

## 4. Anomaly & early-warning detection

Two complementary detector families run every week; their outputs feed the alert state machine (Section 5). Statistical surveillance detectors are **robust, well-understood, and trusted by epidemiologists** — they are the backbone; model-based detection adds forward-looking lead time.

### 4.1 Surveillance-statistics detectors (backbone)

Established count-surveillance aberration methods, run per district-disease on the count series:

- **EARS C1 / C2 / C3** — CDC Early Aberration Reporting System control-chart methods; C2/C3 use a moving baseline with a guard window, well-suited to short-history series (they need only a few weeks of baseline) — ideal for our data-poor districts and newly reporting areas.
- **Farrington / Farrington-Flexible** — quasi-Poisson regression on historical comparable weeks with trend + seasonality + over-dispersion; the reference method at PHE/UKHSA and ECDC for weekly infectious-disease aberration detection. Use where ≥3 years of history exist.
- **(Improved) negative-binomial GLR-CUSUM** for cumulative surge detection.

**Tooling (verified).** These live in R's **`surveillance`** package. In Python, use **`epysurv`** (Robert Koch Institute) which wraps `surveillance` and exposes Farrington/FarringtonFlexible, EARS C1–C3, and GLR-CUSUM with a scikit-learn-style API. **Decision:** adopt `epysurv` as the surveillance-detector library (it is purpose-built, RKI-maintained, and gives us the trusted algorithms without reimplementation); keep the R `surveillance` package reachable via the same rpy2 bridge for methods `epysurv` doesn't surface and for validation. This is the safe, defensible choice — reviewers recognize `surveillance`/Farrington immediately.

### 4.2 Model-based detection

- **Forecast-exceedance**: the L2 (and any promoted) model's predictive distribution defines an expected corridor; observed counts above a high quantile (e.g. > 95th percentile of the 1-week-ahead predictive NB) are anomalies. This is *forward-looking* — it can flag "counts are higher than the climate/season would predict."
- **Residual surge**: standardized residuals from the GLM (observed − expected, over dispersion) exceeding a threshold for consecutive weeks.
- These catch **climate-driven aberrations** the pure count-surveillance methods miss (they know the weather is anomalous, not just the count).

### 4.3 Fusion logic when media surges before case counts

The distinctive CHIP capability. Weekly, per district-disease, combine three streams:

1. `S_surv` — surveillance detector fired? (EARS/Farrington)
2. `S_fore` — forecast/residual exceedance fired?
3. `S_media` — media-surge z-score above threshold (from L6 features)?

**Fusion rules (produce an alert *candidate* type, not a final alert):**

- `S_surv` OR `S_fore` true → **surveillance-anomaly / forecast-exceedance candidate** (counts already moving).
- `S_media` true AND counts *not yet* elevated → **media early-signal candidate** (the lead-time play). Escalated only if media surge is corroborated by a plausible mechanism (e.g. concurrent flood/heatwave hazard flag or a climate anomaly with the right lag) — reduces reacting to isolated news noise.
- `S_media` true AND (`S_surv` OR `S_fore`) true → **compound / high-confidence candidate** (independent streams agree — strongest signal).
- **Media echo guard**: if media surge is *contemporaneous with* an already-detected case surge, it is treated as confirmation, not as lead-time (no extra "early-signal" credit).

Fusion weights and thresholds are tuned against the outbreak gold standard on the timeliness metric, per disease, and reviewed before going live.

---

## 5. Alerting subsystem

Alerts are the only thing external stakeholders see, so **institutional trust is the design objective**. No alert reaches NIH/NDMA without human review, and every alert carries evidence.

### 5.1 Alert taxonomy

| Type | Trigger | Meaning | Typical lead |
|------|---------|---------|--------------|
| **Surveillance anomaly** | EARS/Farrington fired on counts | Counts already aberrant vs baseline | 0 (concurrent) |
| **Forecast exceedance** | Predictive distribution predicts threshold breach ahead | Model expects a surge in `h` weeks | +1..8 wk |
| **Media early-signal** | Media surge before counts, corroborated | Community/news signal leads surveillance | leading |
| **Compound climate–health risk** | Climate hazard + disease-conducive conditions + ≥1 detector | e.g. flood + cholera-conducive lag window + rising signals | leading/concurrent |

### 5.2 Severity levels

| Severity | Definition (illustrative; calibrated per disease) |
|----------|---------------------------------------------------|
| **Watch** | One detector fired, modest exceedance (e.g. > mean+2SD, or forecast > 80th pct), low corroboration |
| **Warning** | Strong single or two corroborating detectors; forecast > 95th pct; or hazard-compound |
| **Alert** | Multiple independent detectors agree; large exceedance; sustained ≥2 weeks; compound climate-health risk in vulnerable district |

Severity is a function of **exceedance magnitude × number of corroborating streams × district vulnerability × persistence**, computed by a transparent rubric (not a black box) so reviewers can see why.

### 5.3 Thresholds and hysteresis (anti-flapping)

Raw threshold crossings flap week to week and destroy trust. We use **hysteresis**:

- **Raise** to a severity when the trigger metric crosses the *upper* threshold for that level.
- **Hold** the severity as long as the metric stays above a *lower* (release) threshold.
- **De-escalate/clear** only after the metric stays below the release threshold for **N consecutive weeks** (e.g. N=2).
- **Minimum dwell time** so an alert cannot be issued and resolved in the same review cycle.
- Thresholds are per-disease, per-region, calibrated to hit a target false-alarm budget (Section 5.6).

### 5.4 Human-in-the-loop review workflow

**No candidate becomes an issued alert automatically.** Weekly cycle:

1. Pipeline generates **alert candidates** with severity, evidence bundle, and recommended action.
2. **TRIAGE (see §5.4.1) reduces ~800 candidate slots to a reviewable queue.**
3. A **reviewer** (project epidemiologist / trained analyst, with domain partner consultation for high severity) sees each surviving candidate in the dashboard: the counts, the forecast corridor, the climate anomaly, the media signal, the KG evidence path, and the model card.
4. Reviewer **confirms, downgrades, upgrades, merges, or rejects** (false-alarm) — every action logged with reviewer id, timestamp, and rationale.
5. Only **confirmed** alerts are **issued** to external stakeholders (via briefs/dashboard). High-severity alerts may require a second reviewer or partner sign-off.

#### 5.4.1 Triage is mandatory, not optional — and it must not become a silent filter

**~160 districts × 5 diseases = ~800 candidate slots per week.** No human reviews 800 candidates weekly. OQ-8 raised this and left it open; it cannot stay open, because **the review step is the only thing standing between a model output and a government stakeholder.** An unreviewable queue is an unreviewed queue.

**Design (v1):**

1. **Most slots are silent.** The vast majority of district-disease-weeks fire *no* detector. Candidates are only generated where `S_surv ∨ S_fore ∨ S_media` (§4.3) — realistically tens, not hundreds, in a normal week.
2. **Rank, don't threshold.** Surface candidates ordered by severity × corroboration × district vulnerability. The reviewer works down the list until it stops being worth their time — rather than a hard cutoff deciding *for* them.
3. **Auto-issue nothing. Auto-*suppress* nothing.** A candidate below the review line is **deferred, not discarded**: it stays in `CANDIDATE`, remains visible in a "not reviewed this cycle" bucket, and is re-scored next week. **A suppressed candidate that later became an outbreak must be recoverable and countable.**
4. **Measure what triage costs.** Track the **missed-outbreak rate among *unreviewed* candidates** against the §3.4 gold standard, separately from the missed-outbreak rate among rejected ones. If triage is hiding true signals, that number reveals it. **This is the number that tells you whether the triage design is safe** — without it, triage is indistinguishable from suppression.
5. **Escalate on compound signals.** §4.3's *compound / high-confidence* class (independent streams agree) always surfaces, regardless of rank.

> **The `media early-signal` class is the one most at risk from triage** — it is by construction *weak, early, and uncorroborated*, which is exactly what a ranking function demotes. Yet it is the project's headline capability. **Give it a reserved quota in the review queue** rather than letting it lose on score to loud, already-obvious surveillance anomalies.

### 5.5 Alert lifecycle state machine

```
                 detector/fusion
                   fires
                     │
                     ▼
              ┌─────────────┐   reviewer rejects
   (new)      │  CANDIDATE  │───────────────────────►  FALSE_ALARM ──┐
              └─────────────┘                                        │
                     │ reviewer confirms/adjusts                     │
                     ▼                                               │
              ┌─────────────┐                                        │
              │  REVIEWED   │──── reviewer holds (needs more data) ──┤
              └─────────────┘        (stays REVIEWED next cycle)     │
                     │ approved (+ sign-off if high severity)        │
                     ▼                                               │
              ┌─────────────┐   severity change → re-review          │
              │   ISSUED    │◄──────────────┐                        │
              └─────────────┘               │ escalate/de-escalate   │
                     │                       (with hysteresis)       │
        ┌────────────┼───────────────┐                               │
        │ metric clears N weeks       │ post-hoc: outbreak did       │
        ▼ (hysteresis)                ▼ not materialise              ▼
   ┌──────────┐                 ┌──────────────┐            all terminal states
   │ RESOLVED │                 │ FALSE_ALARM  │◄───────────  feed false-alarm
   └──────────┘                 └──────────────┘              tracking (5.6)
   (true positive,              (issued but no
    outbreak confirmed)          real outbreak)
```

States: `CANDIDATE → REVIEWED → ISSUED → {RESOLVED | FALSE_ALARM}`, with `CANDIDATE → FALSE_ALARM` (rejected pre-issue) and self-loop `REVIEWED` (held). Every transition is timestamped and attributed. `RESOLVED` vs `FALSE_ALARM` is adjudicated later against the outbreak gold standard, closing the learning loop.

### 5.6 False-alarm tracking as a first-class metric

**Institutional trust depends on this more than on raw accuracy.** We track, per disease/district/severity, continuously:

- **False-alarm rate** (false alarms per district-year) and **PPV of issued alerts**.
- **Missed-outbreak rate** (issued nothing where gold standard says outbreak) — the counterweight so we don't just suppress alerts.
- **Time-to-resolution** and **reviewer override rate** (how often humans reject candidates — high rate means the detectors are miscalibrated).
- A **false-alarm budget** per severity is a design constraint: thresholds are tuned so `Alert`-severity false alarms stay under an agreed cap (set with NIH/NDMA). This budget, not CRPS, is the governing constraint on the operating point.
- Published on the dashboard as a **historical-skill / trust panel** so stakeholders see our track record honestly.

### 5.7 Knowledge-graph evidence linkage (explainability)

Every issued alert carries a machine-readable **evidence bundle** linking to the Climate–Health KG:

- The **feature attributions** that drove the trigger (which climate anomaly, which lag, media surge, hazard flag) — from GLM coefficients / DLNM RR curves for L2, or SHAP-style attributions for L4/L5.
- **KG paths**: e.g. `HeavyRainfall(district, wk t-3) → StandingWater → AedesBreeding → Dengue(district)`, with the supporting nodes/edges (climate obs, hazard event, prior association, news evidence).
- **Provenance**: exact data vintage, model id + version, run timestamp.
- This bundle powers the RAG/graph-retrieval explainable summary in the brief and is auditable — "why did CHIP warn us?" always has a grounded answer. **An alert with no evidence bundle cannot leave `CANDIDATE`.**

---

## 6. Serving & scheduling

### 6.1 Weekly batch scoring via Dagster — and explicitly no real-time serving

**The grain is weekly; there is no operational need for real-time or streaming inference in this subsystem, and we deliberately do not build it.** IDSR reporting, climate aggregation, and the review cadence are all weekly. Real-time model serving would add latency-SLA, online-feature, and infra complexity for zero decision value at this grain. Streaming (Kafka/Spark) belongs to *ingestion* (upstream subsystems), not to forecasting/alerting.

**Dagster weekly job (a single asset graph), run after the gold panel refreshes each epi-week:**

```
build_features(decision_time)        # vintage-safe, one function
   → run_baselines (seasonal-naive, GLM+DLNM)   # always
   → run_promoted_models (Prophet, GBM, [deep if promoted])
   → run_surveillance_detectors (epysurv)
   → fuse_signals → generate_alert_candidates (+ KG evidence bundles)
   → persist forecasts + candidates + metrics (MLflow + tables)
   → notify reviewers (dashboard queue)
```

- **Assets, not tasks**: each output (features, forecasts, candidates) is a Dagster asset with lineage, so a re-run/backfill is one command and lineage is auditable.
- **Backfills** replay historical vintages (Section 3.1) to (re)populate the backtest store.
- **Retraining cadence**: baselines/GLM re-fit on a schedule (e.g. every 4–8 weeks or on drift trigger); deep models retrained less often and always re-gated against baselines before their outputs are trusted.
- **Sensors/SLAs**: Dagster sensor triggers the run when the gold panel's weekly partition lands; freshness checks alert the team (not stakeholders) if a run is late or data is missing.

### 6.2 Forecast storage schema (with model version + run timestamp)

Probabilistic forecasts stored as quantiles (+ distribution params) so any downstream metric or interval is reconstructable. TimescaleDB.

```sql
CREATE TABLE forecast (
  forecast_id      BIGSERIAL PRIMARY KEY,
  district_id      INT       NOT NULL,
  disease          TEXT      NOT NULL,
  target_week      DATE      NOT NULL,      -- epi-week being predicted (MMWR week start)
  horizon_weeks    SMALLINT  NOT NULL,      -- 1..8
  origin_week      DATE      NOT NULL,      -- decision week (as-of)
  model_name       TEXT      NOT NULL,      -- 'glm_dlnm','prophet','lgbm_global','deepar',...
  model_version    TEXT      NOT NULL,      -- semver / git sha
  mlflow_run_id    TEXT      NOT NULL,      -- links to full experiment record
  data_vintage_id  TEXT      NOT NULL,      -- snapshot of inputs used (leakage audit)
  dist_family      TEXT,                    -- 'negbin','quantile','sample'
  mean             DOUBLE PRECISION,
  q05 DOUBLE PRECISION, q25 DOUBLE PRECISION, q50 DOUBLE PRECISION,
  q75 DOUBLE PRECISION, q95 DOUBLE PRECISION,
  dist_params      JSONB,                   -- e.g. {mu, alpha} for NB
  run_timestamp    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (district_id, disease, target_week, horizon_weeks, origin_week, model_name, model_version)
);
SELECT create_hypertable('forecast','target_week');
```

Every forecast is traceable to model version, run time, and exact input vintage — required for the leakage audit and for the dashboard to show *which* model said what, *when*.

### 6.3 Backtest storage (historical skill for the dashboard)

```sql
CREATE TABLE backtest_score (
  backtest_id     BIGSERIAL PRIMARY KEY,
  model_name      TEXT, model_version TEXT, mlflow_run_id TEXT,
  disease         TEXT, district_id INT, region TEXT,
  origin_week     DATE, horizon_weeks SMALLINT,
  crps            DOUBLE PRECISION,
  log_score       DOUBLE PRECISION,
  mae             DOUBLE PRECISION,
  in_regime       BOOLEAN,                 -- out-of-regime (flood) flag
  vs_seasonal_naive_skill DOUBLE PRECISION,-- CRPS skill score vs L1
  vs_glm_skill    DOUBLE PRECISION,        -- vs L2
  outbreak_true   BOOLEAN,                 -- gold-standard label
  alert_fired     BOOLEAN, lead_time_weeks SMALLINT
);
```

Also an **alert-history** table (one row per alert with full state-machine transitions, reviewer, evidence-bundle id, and eventual RESOLVED/FALSE_ALARM label) powering the dashboard's **historical-skill & trust panel** — stakeholders see real forecast skill and the honest false-alarm record, per district, over time.

---

## 7. Research-track vs production-track split

Two tracks share the same feature store, evaluation harness, and MLflow — but have different stability guarantees and a **one-way promotion gate** between them.

| | **Production track (stable pipeline)** | **Research track (student theses)** |
|---|---|---|
| Contents | L0 descriptive, L1 seasonal-naive, **L2 GLM+DLNM (primary)**, L3 Prophet, surveillance detectors (epysurv), fusion, alert SM, storage, Dagster job | L4 GBM, L5 deep/LSTM/DeepAR/TFT, L6 media fusion experiments, GNN-on-KG, novel anomaly methods, alternative outbreak labels |
| Stability | Versioned, reviewed, runs every week, feeds stakeholders | Free to break; runs offline / on GPUs; no stakeholder exposure |
| Data access | Vintage-safe feature store only | Same feature store (must use `build_features`; may add candidate features) |
| Evaluation | Full protocol, model card required | Same harness (mandatory — thesis results must be comparable) |
| Change control | PR + review + model card + re-gate | Student-owned, PI-supervised |

**Promotion path (research → production):**

1. Candidate model runs the **identical** rolling-origin + spatial-holdout protocol on frozen folds.
2. Must **beat L1 and L2 on CRPS** (paired across folds) **and** not lose to seasonal-naive in any disease-region stratum, **and** be calibrated (PIT), **and** meet the false-alarm budget when wired to alerting.
3. Author writes a **model card** (incl. failure modes, out-of-regime behavior).
4. Two-person review (a maintainer + PI/epi advisor) signs off.
5. Model is version-pinned, added to the Dagster asset graph as a *shadow* run first (scored weekly, not shown to stakeholders) for ≥1 season, then promoted to visible.
6. **Demotion** is symmetric: if a promoted model degrades (drift, worse than baseline for K weeks), it auto-falls back to L2 and returns to research.

Thesis alignment: each committed method maps to a student track (DLNM inference; Prophet baselines; global deep forecasting; media lead-time; KG/GNN explainability), and a "did not beat baseline" outcome is an explicitly acceptable, publishable thesis result — which protects scientific honesty against pressure to over-claim.

---

## 8. Open questions

### Changed by the 2026-07-13 review

| Was | Now |
|---|---|
| ~~OQ-3 Vintage data availability~~ | **Downgraded from a design blocker to a measurement (ADR-015).** `ingestion_revision` instruments the normalizer UPSERT and answers it empirically from the historical backfill. **If it comes back empty, most of §2.5's bitemporal apparatus is unnecessary — a simplification.** But see the knock-on to OQ-1 below, which is *not* a simplification. |
| ~~OQ-8 Alert grain vs review capacity~~ | **Now designed: §5.4.1.** Rank-don't-threshold, defer-don't-discard, a reserved quota for `media early-signal`, and — critically — **measure the missed-outbreak rate among *unreviewed* candidates**, which is the only number that distinguishes triage from suppression. |

### Still open

1. **⚠️ Outbreak gold-standard definition — now coupled to ADR-015.** Endemic-channel / mean+2SD over *revision-mature* counts is the current default. **If IDSR turns out never to revise, "revision-mature" does not exist and this definition is unbuildable** — it must be redefined on provisional counts, or on NIH's declared outbreaks where those exist. **This choice drives every operational metric in §3.2, so it must be settled before the evaluation harness is frozen.** Which diseases have usable declared-outbreak records?
2. **Usable history per disease-district.** How many series have enough weeks to forecast at all in v1? A data-audit gate (§1.1) decides the v1 scope — likely dengue/malaria in high-history districts first, everything else descriptive-only. **This is the binding constraint on the whole project: it is disease-data-limited, not compute-limited.** (Which is why the 02 §4a backfill is the highest-value task there is — each extra year of bulletins is ~20% more training data.)
3. **Media signal reliability & reverse causation.** How much of a media surge genuinely *leads* vs *echoes* official reports? Needs the §1.5 lead-time experiment on real data before media is trusted in production alerting. **Two new sources of bias, both from ADR-013:** (a) NAaaS's district coverage may be metro-biased, skewing the signal geographically (03 OQ-8); (b) **unresolved wire copy** — the same PPI/APP story in several outlets — inflates `media_mentions` directly (02 OQ-5). Neither is a modelling problem; both are ingestion contracts.
4. **⚠️ The media-surge denominator depends on an external contract.** `media_surge_z` normalises by *district total news volume* (§2.4). Under ADR-013 the keyword-filtered NAaaS feed supplies **only the numerator**. **ADR-013 contract C1** (an unfiltered count endpoint) is what makes this feature computable at all. **If C1 is not obtained, `media_surge_z` is uninterpretable and the project's headline hypothesis cannot be tested honestly** — a district whose papers simply publish more would look exactly like a district with an outbreak. **Chase C1 now, while the NAaaS API is still being designed.**
5. **Climate baseline for anomalies.** What climatological reference period and product define "normal" per district, and how do we handle non-stationarity (warming trend) so anomalies aren't systematically biased?
6. **DLNM cross-basis validation in Python.** `pydlnm`/`crossbasis` must be numerically validated against R `dlnm` on a fixed dataset before we publish RR curves. Who owns that regression test, and is rpy2/R in the production image acceptable to ops?
7. **False-alarm budget.** The acceptable false-alarm rate per severity must be **agreed with NIH/NDMA**, not chosen by us. Until set, alerting stays in shadow mode.
8. **Small-count / zero-inflation strata.** For low-transmission districts: forecast, aggregate to region, or run surveillance detectors only (EARS C2/C3 handle short baselines well)? Pooling helps but can smear genuine local spikes.
9. **Structural breaks (2022-flood-scale).** Model regime shifts explicitly (change-point/covariate), or accept degraded skill and widen intervals during declared disaster periods? Over-fitting to rare mega-events is its own risk.
10. **⭐ NEW — the mechanistic priors this subsystem's alerts depend on (04 §0.2).** §5.7 promises every alert carries a KG path (`HeavyRainfall → StandingWater → AedesBreeding → Dengue`). **Those intermediate nodes are disease biology and exist in no data source** — they must be hand-curated and cited. **An alert with no evidence bundle cannot leave `CANDIDATE` (§5.7), so this subsystem's headline deliverable is blocked on a workstream that currently has no owner.** The upside: the ontology is also the *citable justification* for the lag windows §2.2 already hard-codes.

---

*This document is binding for the production track. Research extensions are welcome but must enter through the promotion gate in Section 7 and be scored by the protocol in Section 3.*
