# CHIP — Layer 1: Data Sources Complete Schema Catalog

**Zoomed-in layer:** Layer 1 — Data Sources
**Parent document:** `ARCHITECTURE-DIAGRAM-GUIDE.md` Block 1
**Status:** Design (prototype phase — static, pre-parsed data only)
**Audience:** MS/PhD implementers building ingestion connectors
**Last updated:** 2026-07-15

> This document is the authoritative catalog of every data source currently available for the CHIP prototype. It documents what each source is, what it contains, its complete schema, its quality issues, and how it maps to the canonical district × epi-week grain. It is the input contract for the connector implementers in Layer 2.

---

## 0. Document Scope & Relationship to Upstream Architecture

### 0.1 What this layer covers

Layer 1 of the CHIP architecture is the set of external data sources the platform draws on. In the architecture diagram (Block 1), there are four families:

| Block in diagram | This document |
|---|---|
| Disease Surveillance Bulletins | NIH, AJK, WHO, PITB-DSS |
| Climate & Weather Records | Not yet available (Layer 1 Block 2 — future) |
| Disaster Situation Reports | Not yet available (Layer 1 Block 3 — future) |
| Relevance-Filtered News Feed | Not yet available (Layer 1 Block 4 — future) |

**Current scope:** This document covers only Block 1 of Layer 1 — **disease surveillance bulletins and related health data**. All data is pre-parsed from original PDFs into markdown (`.md`) and plain text (`.txt`), stored in `Data_sources_1/`.

### 0.2 What this document is NOT

- It is NOT a connector design document (that's Layer 2, see `02-ingestion-connectors.md`)
- It is NOT a parsing strategy document (that's ADR-012)
- It is NOT a data normalization plan (that's Layer 4, see `01-data-model-and-schemas.md`)
- It specifies nothing about _how_ we ingest — only _what_ there is to ingest

### 0.3 Conventions

- Source directories are referenced by their path relative to `Data_sources_1/`
- All `.md` files contain the same content as their `.txt` counterparts; `.md` is treated as authoritative (it preserves table structure through HTML markup)
- "District-level" means data broken out by individual administrative districts (admin2)
- "Epi-week" refers to the epidemiological week number as stated in the source document
- File counts: `N + N` means N unique documents × 2 formats (MD + TXT)

---

## 1. Source Classification & Overview

The 7 available sources are classified by **temporal granularity** — the natural reporting rhythm that determines how each source's data aligns with the canonical grain.

### 1.1 Weekly Surveillance (the core disease data)

| Source | Dir | Unique docs | Time span | Geographic scope | District-level? | Key diseases tracked |
|---|---|---|---|---|---|---|
| **NIH IDSR** | `NIH/` | 174 | 2021–2026 | National (all provinces) | YES (Sindh, Balochistan, KP) | ~40 priority diseases |
| **WHO DEWS** | `WHO/` | 94 | 2013–2014 | National (74 districts) | YES (province tables) | 17 priority diseases |
| **PITB-DSS** | `PITB-DSS/` | 169 | 2015–2018 | Punjab province only | YES (all 36 districts) | ~15 communicable diseases |
| **AJK IDSRS** | `AJK/` | 3 | 2026 (3 weeks) | AJ&K (10 districts) | YES (all 10 districts) | ~18 priority diseases |

### 1.2 Monthly & Annual Reports (health system data)

| Source | Dir | Unique docs | Time span | Geographic scope | District-level? | Content type |
|---|---|---|---|---|---|---|
| **DHIS Punjab** | `DHIS/` | 111 | 2013–2025 | Punjab province | Limited (era-dependent) | OPD, MNCH, FP, disease stats, outbreak reports |
| **KPK Report 2017** | `KPK Report 2017/` | 1 | 2017 | Khyber Pakhtunkhwa (25 districts) | YES | Annual HMIS/DHIS compilation |

### 1.3 Clinical Screening Campaigns

| Source | Dir | Unique docs | Time span | Geographic scope | District-level? | Content type |
|---|---|---|---|---|---|---|
| **Punjab Health Week** | `Punjab Health Week 2/` | 2 | 2017–2018 | Punjab (36 districts) | YES | Mass screening: NCDs, blood-borne diseases |

### 1.4 Cross-source coverage heatmap

```
              2013  2014  2015  2016  2017  2018  2019  2020  2021  2022  2023  2024  2025  2026
NIH IDSR                                                      ████  ████  ████  ████  ████  ██
WHO DEWS      ████  ████
PITB-DSS                   ████  ████  ████  ████
AJK IDSRS                                                                                     ██
DHIS Punjab   ████  ████  ████  ████  ████  ████  ████  ████  ████  ████  ████  ████  ████
KPK 2017                                    █
PHW 2017-18                                 ██
```

**Key observation:** NIH IDSR and DHIS Punjab overlap in 2021–2025 and cover Punjab. This is the only temporal overlap with geographic overlap — a validation opportunity.

---

## 2. Weekly Surveillance Sources — Deep Schema Analysis

### 2.1 NIH IDSR — National Weekly Epidemiological Bulletins

#### 2.1.1 Source Identity

| Attribute | Value |
|---|---|
| **Full name** | Integrated Disease Surveillance & Response (IDSR) Weekly Public Health Bulletin |
| **Custodian** | Center of Disease Control, National Institute of Health (NIH), Islamabad |
| **Publishing cadence** | Weekly (one bulletin per epidemiological week) |
| **Publishing mechanism** | PDF posted to `nih.org.pk/wp-content/uploads/YYYY/MM/` with inconsistent filenames |
| **Partner logos** | UK Health Security Agency (UKHSA), WHO, USAID, CDC |
| **Access tier** | Public (Phase 1 — static files from PDF parsing) |
| **Data lineage in this repo** | Original PDFs → parsed to markdown → stored in `NIH/MD/` |
| **File naming conventions observed** | `Week-{01}-{2025}.md`, `Weekly Report-{01}-{2024}.md`, `IDSR-Weekly-Report-{16}-{2022}.md`, `IDSRS Weekly Report-{13}-{2025}.md`, `Weekly_Report_{52}_2023.md`, `IDSR Week 13 Bulletin (2025).md` (6+ distinct patterns across 2021–2026) |

#### 2.1.2 Document Structure (per weekly bulletin)

Every bulletin follows a stable section structure that evolved in two phases:

**Phase 1 (2021–2023):** Compact IDSR reports
- Highlights / Overview (narrative)
- Figure 1: Province-level trend bars (multi-week)
- Table 1: Province/Area summary (1 row per disease, 1 column per province + Total)
- Per-province sections: Sindh, Balochistan, KP (each with narrative + district tables + trend charts)
- ICT, AJK, GB sections: narrative + trend charts only (no district tables)
- IDSR Participating Districts compliance table

**Phase 2 (2024–2026):** "Public Health Bulletin-Pakistan" expanded format
- Editorial content (message from Chief Editor, subscribe CTA)
- Overview (narrative summary of the week)
- IDSR Reports (same disease tables as Phase 1)
- Ongoing Events (outbreak investigations, field reports)
- Field Reports (multi-page narrative epidemiological investigations)
- Public Health Laboratories (confirmed test results table)
- Public Health Actions (disease-specific recommendations)
- Knowledge Hub (disease prevention educational content)

#### 2.1.3 Complete Table Inventory

##### Table 1 — Province/Area Summary (every week, 2021–2026)

The highest-value table. One row per disease, one column per province/area, plus a Total column.

**Columns by era:**

| Era | Columns |
|---|---|
| 2021–2022 | `Diseases`, `AJK`, `Balochistan`, `GB`, `ICT`, `KP`, `Sindh`, `Total` |
| 2023 | Added `Punjab` column (all values `NR`) — 8 data columns |
| 2024–2026 | `Diseases`, `AJK`, `Balochistan`, `GB`, `ICT`, `KP`, `Punjab`, `Sindh`, `Total` — Punjab now has real data |

**Disease rows (varies by week; ~25–30 diseases).** Full disease inventory:

| Canonical disease | IDSR label(s) seen | ICD-10 |
|---|---|---|
| Acute diarrhoea (non-cholera) | `AD (Non-Cholera)`, `Acute Diarrhea (Non-Cholera)` | A09 |
| Acute watery diarrhoea (suspected cholera) | `AWD (S. Cholera)`, `S. Cholera` | A00 |
| Bloody diarrhoea | `B. Diarrhea`, `Bloody Diarrhea` | A09 |
| Typhoid fever | `Typhoid`, `Suspected Enteric/Typhoid` | A01.0 |
| Malaria | `Malaria` | B54 |
| Influenza-like illness | `ILI` | J11 |
| Acute lower respiratory infection (<5yr) | `ALRI < 5 years`, `ALRI <5 years` | J22 |
| Severe acute respiratory infection | `SARI` | J22 |
| Tuberculosis | `TB` | A15–A19 |
| Dengue fever | `Dengue` | A90–A91 |
| Viral hepatitis (B, C, D) | `VH (B, C & D)`, `VH (B,C&D)` | B16–B18 |
| Acute viral hepatitis (A, E) | `AVH (A & E)`, `AVH(A&E)` | B15, B17.2 |
| Measles | `Measles` | B05 |
| Meningitis | `Meningitis` | G00–G03 |
| Diphtheria | `Diphtheria` | A36 |
| Pertussis | `Pertussis` | A37 |
| Neonatal tetanus | `NT`, `Neonatal Tetanus` | A33 |
| Acute flaccid paralysis | `AFP` | G82.0 |
| Chickenpox / varicella | `Chickenpox`, `Chickenpox / Varicella`, `Chickenpox/ Varicella` | B01 |
| Mumps | `Mumps` | B26 |
| Cutaneous leishmaniasis | `CL`, `Cutaneous Leishmaniasis` | B55.1 |
| Visceral leishmaniasis | `VL`, `Visceral Leishmaniasis` | B55.0 |
| HIV/AIDS | `HIV / AIDS`, `HIV/AIDS` | B20–B24 |
| Gonorrhoea | `Gonorrhea` | A54 |
| Syphilis | `Syphilis` | A51–A53 |
| Brucellosis | `Brucellosis` | A23 |
| Anthrax | `Anthrax` | A22 |
| Chikungunya | `Chikungunya` | A92.0 |
| Dog bite (rabies risk) | `Dog Bite`, `Animal / Dog Bite`, `Rabies/Dog Bite` | T14.1 |
| Scabies | `Scabies` | B86 |
| Rubella / CRS | `Rubella (CRS)`, `Rubella` | B06, P35.0 |
| Leprosy | `Leprosy` | A30 |

**Data type:** Integer case counts. `NR` = not reported. Blank = zero or not reported (ambiguous).

##### Tables 2–4 — District-Level Disease Tables (every week, 2021–2026)

One table per province with district-level granularity. **Orientation varies by era:**

**2021–2022: Transposed** — Diseases as rows, districts as columns.
```
Diseases            | Ghotki | Hyderabad | Karachi East | ... | Total
ILI                 | 424    | 3,130     | 1            | ... | 4,271
AD (Non-Cholera)    | 777    | 1,018     | 132          | ... | 3,069
```

**2023–2026: Pivoted** — Districts as rows, diseases as columns.
```
DISTRICTS  | Malaria | AD (Non-Cholera) | ILI | ... | Total
Badin      | 1,668   | 1,902            | 544 | ... | 4,508
Dadu       | 543     | 1,188            | 79  | ... | 2,475
```

**Provinces with district tables:**
- **Table 2:** Sindh — 29–30 districts (varies by era; Shaheed Benazirabad added in 2023)
- **Table 3:** Balochistan — 33–36 districts (varies by era)
- **Table 4:** KP — 27–39 districts (varies as FATA merged districts added)

**Provinces WITHOUT district tables:**
- AJK (10 districts)
- GB (10 districts)
- ICT / Islamabad (1 district)
- Punjab (36 districts — joined IDSR in 2024 but provides only category-level table, not district)

**District name quality (Sindh example):**

| Standard name | Observed variants |
|---|---|
| Naushahro Feroze | `Naushero Feroze`, `N. Feroze`, `NosheroFeroz` |
| Kamber Shahdadkot | `Kamber`, `Kam-ber`, `Kamber Shadadkot` |
| Karachi Malir | `Karachi Malir`, `Karachi-Malir`, `Kar-Malir` |
| Karachi East | `Karachi East`, `Karachi-East`, `Kar-East` |
| Tharparkar | `Tharparkar`, `Thar-parkar` |
| Lasbela | `Lasbella`, `Lasbela` |
| Kech | `Kech (Turbat)`, `Kech` |
| Naseerabad | `Naseerabad`, `Naserabad` |
| Jaffarabad | `Jaffarabad`, `Jaffrabad` |

**Implication:** District name normalization is required before any join. Every alias variant needs to be in `location_alias`.

##### Table 5/6 — IDSR Participating Districts Compliance

| Column | Description |
|---|---|
| `Province` | Province/area name |
| `District` | District name (not always properly nested under province — structural errors observed) |
| `Reporting Sites` | Number of reporting health facilities |
| `Compliance %` | Percentage of expected sites that reported |

**Quality note:** In some 2023 files, Balochistan district names appear nested under the AJK row — a structural error in the original PDF parsing.

##### Table 7 — Public Health Laboratories Confirmed Cases (2024+ only)

| Column | Description |
|---|---|
| `Disease` | Disease name |
| `Province` | Province name |
| `Total Test` | Number of lab tests performed |
| `Total Positive` | Number of positive results |

##### Additional content types (2024+ only)

- **Outbreak Investigation Field Reports:** 2–3 page narrative sections describing active outbreak responses (scabies, typhoid, leishmaniasis, measles)
- **Public Health Actions:** Disease-specific prevention and control recommendations (e.g., meningitis vaccination campaigns, neonatal tetanus protocol)
- **Knowledge Hub:** Community-facing disease prevention guides (urinary tract infections, rabies awareness, nutrition)

#### 2.1.4 Quality Issues Catalog

| ID | Issue | Severity | Affected files | Mitigation |
|---|---|---|---|---|
| NIH-01 | District name inconsistency (9+ variants for same district) | HIGH | All district tables | `location_alias` normalization; trigram fuzzy fallback |
| NIH-02 | Punjab data entirely `NR` in 2023 (full year) | HIGH | All 2023 files | Document as known gap; Punjab joins in 2024 |
| NIH-03 | Punjab absent from 2021–2022 entirely (no column) | HIGH | All 2021–2022 files | Design schema flexibly for column addition |
| NIH-04 | Table orientation changed (transposed → pivoted at 2023) | HIGH | Boundary: 2022→2023 | Parser must detect orientation before extraction |
| NIH-05 | `NR` vs blank vs `0` semantics ambiguous | MEDIUM | All district tables | Treat `NR` as NULL; blank as unknown (quarantine); `0` as zero |
| NIH-06 | Structural errors in compliance tables (mis-nested rows) | MEDIUM | 2023 files | Validate parent province matches expected parent |
| NIH-07 | Trend graph placeholder values (flat `100` across weeks) | MEDIUM | 2021–2022 trend figures | Do not ingest trend graph data as factual |
| NIH-08 | Week identifier format drift (`W1`, `WK1`, `WK 1`, `WK 14`) | LOW | Cross-file | Normalize to format `YYYY-W{NN}` at parse time |
| NIH-09 | File naming: 6+ distinct patterns across years | LOW | All files | Use content-based week/year extraction, not filename parsing |
| NIH-10 | CDA/ICT split in compliance tables (sometimes one row, sometimes two) | LOW | Compliance tables | Treat ICT and CDA as the same district |
| NIH-11 | Disease label evolution (e.g., `S. Cholera` → `AWD (S. Cholera)`) | LOW | Cross-year | Map all historical labels to canonical disease_code |

#### 2.1.5 Canonical Grain Mapping Assessment

| Canonical dimension | NIH IDSR support | Gap |
|---|---|---|
| **District (admin2)** | YES — for Sindh, Balochistan, KP. NO — for AJK, GB, ICT. Partial — for Punjab (2024+, province-level only) | Missing district data for 4 of 7 provinces |
| **Epi-week** | YES — explicit week number in every bulletin | Week convention unconfirmed (WHO/ISO vs CDC/MMWR) |
| **Disease** | YES — ~30 distinct disease labels | Labels evolve over time; map to canonical disease_code |
| **Case count** | YES — integer suspected case counts | No confirmed cases in most tables; no population denominator |
| **Death count** | Not tabulated | Deaths mentioned narratively only |

**Assessment:** NIH IDSR is the richest and most structurally complete source, but has a major gap — no district-level data for Punjab (largest province), AJK, GB, or ICT. These are filled by PITB-DSS (Punjab), AJK IDSRS (AJK), and partially by WHO DEWS (historical national coverage).

---

### 2.2 WHO DEWS — National Disease Early Warning System

#### 2.2.1 Source Identity

| Attribute | Value |
|---|---|
| **Full name** | Weekly Epidemiological Bulletin — Disease Early Warning System and Response in Pakistan |
| **Custodian** | WHO Country Office, Pakistan + NIH Islamabad |
| **Publishing cadence** | Weekly (one bulletin per epidemiological week) |
| **Coverage** | 74 districts, 2,000+ health facilities nationwide |
| **Access tier** | Public |
| **Data lineage** | Original PDFs → parsed to markdown → stored in `WHO/MD/` |
| **File naming convention** | `{NN}-{DDMM}-{YYYY}.md` where NN = week number, DDMM = publishing date (e.g., `01-0801-2014.md` = Week 1, Jan 8, 2014) |

#### 2.2.2 Document Structure

Structure is highly consistent across all 94 bulletins:

1. Highlights (narrative)
2. Cumulative case count table (year-to-date running totals)
3. Current-week disease case-count table
4. Alert table (disease alerts identified and responded to)
5. Outbreak response narratives
6. District reporting compliance summary

#### 2.2.3 Complete Table Inventory

##### Table A — Cumulative Annual Case Counts

Year-to-date running totals for the full surveillance year.
```
Disease                | # of Cases | Percentage
ARI                    | 8,359,370  | 20.05%
Bloody diarrhoea       | 99,182     | <0.5%
Acute diarrhoea        | 3,040,364  | 7.29%
S. Malaria             | 1,761,436  | 4.23%
Skin Diseases          | 1,564,821  | 3.75%
Unexplained fever      | 1,273,308  | 3.05%
Total (All consultations)| 41,690,074 | —
```

**Fields:** `Disease`, `# of Cases`, `Percentage`
**Note:** Contains a Total row. These are **cumulative** (running from Week 1) — useful for cross-validating the sum of weekly tables but not directly ingestible as-is.

##### Table B — Current Week Disease Case Counts

Current-week case counts for the same disease set.
```
Disease                | # of Cases | Percentage
ARI                    | 120,456    | 20.1%
Bloody diarrhoea       | 1,940      | 0.3%
...
Total consultations    | 721,698    | —
```

##### Table C — Disease Alerts

```
Alert Disease | Number of Alerts
Measles       | 21
Leishmaniasis | 10
NNT           | 8
Typhoid       | 6
Diphtheria    | 3
Scabies       | 3
Dengue fever  | 1
Total         | 52
```

**Fields:** `Alert Disease`, `Number of Alerts`

##### Priority Diseases Under Surveillance (not a data table — a reference list)

17 diseases: Pneumonia, Acute Watery Diarrhoea, Bloody diarrhoea, Acute Diarrhoea, Suspected Enteric/Typhoid Fever, Suspected Malaria, Suspected Meningitis, Suspected Dengue fever, Suspected Viral Hemorrhagic Fever, Suspected Measles, Suspected Diphtheria, Suspected Pertussis, Suspected Acute Viral Hepatitis, Neonatal Tetanus, Acute Flaccid Paralysis, Scabies, Cutaneous Leishmaniasis

#### 2.2.4 Quality Issues

| ID | Issue | Severity | Detail |
|---|---|---|---|
| WHO-01 | NO district-level disease breakdown | HIGH | Only national + province references in narrative. Disease tables are national aggregate. |
| WHO-02 | District count mentioned narratively only ("74 districts reported") | HIGH | Can't extract per-district case counts |
| WHO-03 | Cumulative tables require subtraction to derive weekly counts | MEDIUM | Ingest the current-week table directly; use cumulative for reconciliation |
| WHO-04 | Disease labels are WHO-standard (different from NIH IDSR labels) | MEDIUM | e.g., WHO's "Suspected Malaria" vs NIH's "Malaria" — need label crosswalk |

#### 2.2.5 Canonical Grain Mapping

| Dimension | Support | Detail |
|---|---|---|
| District | ❌ NO | National aggregate only; "74 districts reported" is metadata, not a data dimension |
| Epi-week | ✅ YES | Explicit in every bulletin |
| Disease | ✅ YES | 17 priority diseases |
| Case count | ✅ YES | Current week and cumulative |

**Assessment:** WHO DEWS is a national-level aggregator. It provides a valuable **cross-validation signal** for NIH IDSR totals but cannot contribute district-level data to the canonical panel. Its primary value is historical coverage (2013–2014) and the alert system data.

---

### 2.3 PITB-DSS — Punjab Disease Surveillance System (later HRS)

#### 2.3.1 Source Identity

| Attribute | Value |
|---|---|
| **Full name** | Disease Surveillance System (2015) → Health Reporting System (2016–2018) |
| **Custodian** | Punjab Information Technology Board (PITB) + Institute of Public Health + WHO + Health Department Punjab + King Edward Medical University + University of Agriculture Faisalabad |
| **Publishing cadence** | Weekly |
| **Coverage** | All 36 districts of Punjab |
| **Access tier** | Public |
| **Data lineage** | Original PDFs (sitreps) → parsed to markdown → stored in `PITB-DSS/{year}/MD/` |
| **Organization** | Yearly subdirectories: `2015/`, `2016/`, `2017/`, `2018/` |
| **Files per year** | 2015: 38 weeks, 2016: 52 weeks, 2017: 53 weeks, 2018: 26 weeks (partial year) |

#### 2.3.2 Naming Evolution

| Year | System name | File naming pattern | Key format shift |
|---|---|---|---|
| 2015 | DSS (Disease Surveillance System) | `DSS-Bulletin-Week-{N}.md` | Original format |
| 2016 | DSS → HRS transition | `DSS Bulletin Week {N}-2016.md` and `HRS Bulletin Week {N}-2016.md` mixed | Two naming styles coexist |
| 2017 | HRS (Health Reporting System) | `HRS Bulletin Week {N},2017.md` | Comma separator |
| 2018 | HRS | `HRS Bulletin Week {N},2018.md` | Consistent HRS naming |

The 2016 transition from DSS to HRS naming is a cosmetic change — the data structure remained stable.

#### 2.3.3 Document Structure & Tables

Every bulletin follows a consistent structure:

1. **Editorial masthead** (Patrons in Chief, Patrons, Editors, Sub-Editors, Designer — names listed)
2. **Summary boxes** — key disease stats with district counts inline
3. **Communicable Disease Situation table** — diseases with >1000 cases and 100–1000 cases
4. **Full disease table** — all tracked diseases by district
5. **Focus disease section** — deep-dive on one disease per week (e.g., "Focusing Viral Hemorrhagic Fever (Suspected)")

##### Table A — Communicable Disease Situation (over/under 1000 cases)

```
Disease (Non-Zero Reported Cases)      | Number of Cases
Diarrhoea (Acute)                      | 8,076
Acute (upper) Respiratory Infections   | 30,649
Dog Bite (Suspected)                   | 1,290
Pyrexia of Unknown Origin              | 7,316
Scabies (Suspected)                    | 2,911
Tuberculosis (Suspected)               | 1,425
```

**Note:** This table summarizes diseases above/below 1000 cases. It's an extract of the full table, not a separate dataset.

##### Table B — Full Disease × District Matrix

This is the primary value table. It is transposed: **diseases as rows, districts as columns**. ALL 36 Punjab districts are represented across columns.

**Diseases tracked (~15):**

| Disease label in DSS/HRS | Canonical |
|---|---|
| `Diarrhoea (Acute)` | Acute diarrhoea |
| `Acute (upper) Respiratory Infections` | ARI |
| `Dog Bite (Suspected)` | Dog bite |
| `Pyrexia of Unknown Origin` | PUO / unexplained fever |
| `Scabies (Suspected)` | Scabies |
| `Tuberculosis (Suspected)` | TB |
| `Malaria (Suspected)` | Malaria |
| `Bloody Diarrhoea (Suspected)` | Bloody diarrhoea |
| `Acute Flaccid Paralysis (Suspected)` | AFP |
| `HIV/AIDS (Suspected)` | HIV/AIDS |
| `Viral Hemorrhagic Fever (Suspected)` | VHF |
| `Neonatal Tetanus (Suspected)` | NNT |
| `Diphtheria (Suspected)` | Diphtheria |
| `Measles (Suspected)` | Measles |
| `Enteric/Typhoid Fever (Suspected)` | Typhoid |
| `Meningitis (Suspected)` | Meningitis |

**District names:** All 36 Punjab districts in a consistent ordering. Some abbreviations: `D.G KHAN`, `R.Y. KHAN`, `T.T SINGH`, `M.B DIN`.

#### 2.3.4 Quality Issues

| ID | Issue | Severity | Detail |
|---|---|---|---|
| DSS-01 | 2018 partial year (Weeks 1–26 only) | MEDIUM | No data after June 2018 |
| DSS-02 | District abbreviations (`D.G KHAN`, `R.Y. KHAN`) need expansion | LOW | Create aliases: `D.G KHAN` → `Dera Ghazi Khan` |
| DSS-03 | Disease names carry `(Suspected)` qualifier consistently | LOW | Strip qualifier for canonical mapping; keep provenance |
| DSS-04 | Some weeks have duplicate/missing files | LOW | 2015: weeks 1, 2 missing; jump from week 11 |

#### 2.3.5 Canonical Grain Mapping

| Dimension | Support | Detail |
|---|---|---|
| District | ✅ YES | All 36 Punjab districts, every week |
| Epi-week | ✅ YES | Explicit week number with date range |
| Disease | ✅ YES | ~15 communicable diseases with consistent labeling |
| Case count | ✅ YES | Integer case counts, `(Suspected)` qualified |

**Assessment:** PITB-DSS is the single best source for Punjab district-level disease data for 2015–2018. It fills the Punjab gap in NIH IDSR (which doesn't report Punjab at district level even in 2024+).

---

### 2.4 AJK IDSRS — AJ&K Provincial Weekly Bulletins

#### 2.4.1 Source Identity

| Attribute | Value |
|---|---|
| **Full name** | Integrated Disease Surveillance & Response System (IDSRS) Weekly Bulletin |
| **Custodian** | Provincial Disease Surveillance & Response Unit (PDSRU), Directorate General Health, AJ&K |
| **Publishing cadence** | Weekly |
| **Coverage** | 465 reporting sites across 10 districts of Azad Jammu & Kashmir |
| **Access tier** | Public |
| **Data lineage** | Original PDFs → parsed to markdown → stored in `AJK/out/MD/` |
| **Available files** | 3 weeks: Epi Weeks 18, 19, 20 (2026) |

#### 2.4.2 Document Structure

Highly structured, consistent across all 3 weeks:

1. Header with 4 institutional logos (AJ&K Government, UKHSA, NIH, AJ&K Health Department)
2. Geographical map of AJ&K showing all 10 districts
3. "Message of the Week" (health awareness infographic in Urdu)
4. Highlights (narrative + key stats)
5. Compliance rate table
6. Overall suspected cases & deaths table
7. District-wise disease detail table (the primary data table)
8. Trend charts: ILI and Acute Diarrhea (multi-week)
9. Weekly comparison tables (current week vs last 2 weeks, grouped by disease category)
10. Hotspot maps (AFP, TB)
11. Training & field activity reports (photographs + narrative)

#### 2.4.3 Complete Table Inventory

##### Table A — Compliance Rate of Reporting Sites

```
District       | Expected Reporting Sites | Actual Reported Sites | IDSR Compliance %
Muzaffarabad   | 45                       | 45                    | 100.0%
Jhelum Valley  | 29                       | 29                    | 100.0%
Neelum         | 39                       | 38                    | 97.4%
Poonch         | 46                       | 45                    | 97.8%
Bagh           | 54                       | 54                    | 100.0%
Haveli         | 39                       | 39                    | 100.0%
Sudhnoti       | 27                       | 27                    | 100.0%
Mirpur         | 41                       | 41                    | 100.0%
Bhimber        | —                        | —                     | — (not in all reports)
Kotli          | —                        | —                     | — (not in all reports)
```

**Note:** Bhimber and Kotli appear in some weeks but not all. Reporting compliance averages ~99.6%.

##### Table B — Overall Suspected Cases & Deaths (Provincial Aggregate)

```
Disease                                   | Suspected Cases | Deaths
Acute Diarrhea (Non-Cholera)              | 2,146           | 0
Influenza-Like Illness (ILI)              | 1,795           | 0
Pneumonia/ALRI (<5 years)                 | 1,279           | 0
SARI                                      | 56              | 0
Animal / Dog Bite                         | 112             | 0
Tuberculosis                              | 112             | 0
Bloody Diarrhea                           | 22              | 0
Viral Hepatitis (B, C & D)               | 19              | 0
Typhoid Fever                             | 17              | 0
AVH (A & E)                               | 0               | 0
Acute Flaccid Paralysis (AFP)             | 4               | 0
AWD (Suspected Cholera)                   | 0               | 0
Mumps                                     | 0               | 0
Chickenpox / Varicella                    | 5               | 0
Measles                                   | 0               | 0
Meningitis                                | 0               | 0
Dengue Fever                              | 0               | 0
HIV / AIDS                                | 0               | 0
Total                                     | 5,567            | 0
```

**Fields:** `Disease`, `Suspected Cases`, `Deaths`
**Note:** Deaths are tracked explicitly — consistently zero across all 3 weeks.

##### Table C — District Wise Detail of Reported Cases (THE PRIMARY DATA TABLE)

Full disease × district matrix. 10 districts × 18 diseases.

```
Disease           | MZD | JV | Neelum | Poonch | Bagh | Haveli | Sudhnoti | Mirpur | Bhimber | Kotli | Total
AD (Non-Cholera)  | 155 | 77 | 153    | 412    | 377  | 116    | 269      | 218    | 155     | 214   | 2,146
ILI               | 78  | 94 | 84     | 155    | 232  | 142    | 142      | 243    | 385     | 240   | 1,795
Pneumonia/ALRI    | 50  | 54 | 128    | 240    | 168  | 116    | 61       | 116    | 243     | 103   | 1,279
...
```

**District abbreviations:** `MZD` (Muzaffarabad), `JV` (Jhelum Valley). All 10 districts have columns.

##### Table D — Weekly Comparisons (Last 3 Weeks, by Disease Category)

Multiple small tables, one per disease category:

```
Vaccine Preventable Diseases | Week 16 | Week 17 | Week 18
Measles                      | 0       | 0       | 0
Chickenpox / Varicella       | 6       | 1       | 5
Meningitis                   | 0       | 0       | 0
Mumps                        | 0       | 1       | 0
AFP                          | 3       | 2       | 4
Diphtheria                   | 0       | 0       | 0
Neonatal Tetanus             | 0       | 0       | 0
Pertussis                    | 0       | 0       | 0
Rubella (CRS)                | 0       | 0       | 0
```

**Categories present:** Vaccine Preventable Diseases, Respiratory Diseases, Water/Food Borne Diseases, STIs, Zoonotic/Other, Vector Borne Diseases

##### Disease Categories (from the weekly comparison section):

| Category | Diseases |
|---|---|
| **Vaccine Preventable** | Measles, Chickenpox/Varicella, Meningitis, Mumps, AFP, Diphtheria, Neonatal Tetanus, Pertussis, Rubella (CRS) |
| **Respiratory** | ILI, Pneumonia/ALRI <5yr, SARI, Tuberculosis |
| **Water/Food Borne** | Acute Diarrhea (Non-Cholera), Bloody Diarrhea, AWD (Suspected Cholera), AVH (A & E), Typhoid Fever |
| **STIs** | HIV/AIDS, Syphilis, Gonorrhea |
| **Zoonotic/Other** | Animal/Dog Bite, VH (B, C & D), Brucellosis, Rabies |
| **Vector Borne** | Malaria, Cutaneous Leishmaniasis, Dengue Fever, Visceral Leishmaniasis |

#### 2.4.4 Quality Issues

| ID | Issue | Severity | Detail |
|---|---|---|---|
| AJK-01 | Only 3 weeks available | HIGH | Cannot establish trend baselines or validate consistency |
| AJK-02 | District name abbreviations (`MZD`, `JV`) | LOW | Create aliases: `MZD` → `Muzaffarabad`, `JV` → `Jhelum Valley` |
| AJK-03 | District spelling variations (`Sudhnoti` vs `Sudhnooti` vs `Sudhnuti`) | LOW | Normalize to one canonical form |
| AJK-04 | Bhimber and Kotli sometimes absent from compliance table | LOW | Check if they report in all weeks |

#### 2.4.5 Canonical Grain Mapping

| Dimension | Support | Detail |
|---|---|---|
| District | ✅ YES | All 10 AJ&K districts |
| Epi-week | ✅ YES | Explicit in every bulletin (weeks 18–20, 2026) |
| Disease | ✅ YES | ~18 diseases with categorized grouping |
| Case count | ✅ YES | Integer case counts; deaths tracked separately |
| Death count | ✅ YES | Explicit death column (zero for all 3 weeks) |

**Assessment:** AJK IDSRS is the most well-structured source overall — explicit death tracking, categorized disease groups, and full district matrices. Only limitation is the tiny sample (3 weeks).

---

## 3. Monthly & Annual Reports — Deep Schema Analysis

### 3.1 DHIS Punjab — District Health Information System

#### 3.1.1 Source Identity

| Attribute | Value |
|---|---|
| **Full name** | District Health Information System (DHIS) — Directorate General Health Services, Punjab |
| **Publishing cadence** | Mixed: weekly bulletins (2022, 2024–2025), monthly feedback reports (2020–2021), annual reports (2013–2024) |
| **Coverage** | All public health facilities in Punjab (BHU, RHC, THQ, DHQ, Teaching Hospitals) |
| **Access tier** | Public |
| **Data lineage** | Various source documents → parsed to markdown → stored in `DHIS/MD/` |
| **File count** | 111 unique documents across all report types |

#### 3.1.2 Two Format Eras

**Era 1: DHIS-II (2022 and earlier)**
- Weekly Feedback Reports
- Rich **district-level** data tables
- 31 districts (pre-2023 district creation)
- TABLES INCLUDE: district × disease matrix (12 epidemic diseases), district × facility-type matrices (OPD, deliveries, ANC, FP), compliance rates
- **Representative file:** `week_18.md` (Week 18, 2022 — May 2–8)

**Era 2: DHIS2 (2024–2025)**
- Weekly Bulletins
- **Province-level aggregate** data ONLY
- Standardized template with ~24 recurring table types
- Disease data mentioned narratively by district but NOT tabulated
- Richer in MNCH, FP, surgeries, and non-communicable disease indicators
- **Representative files:** `Week_1.md`, `Week_14.md`, `Week_18(1).md`

#### 3.1.3 DHIS-II Era (2022) — Complete Table Inventory

This era has the most ingestible district-level disease data.

##### Table A — DHIS-II Weekly Reporting Compliance
```
District    | Daily IPD (%) | Daily OPD (%) | Daily RMNCH (%)
ATTOCK      | 94%           | 98%           | 94%
BAHAWALNAGAR| 88%           | 95%           | 89%
...
```

**Fields:** `District`, `Daily IPD (%)`, `Daily OPD (%)`, `Daily RMNCH (%)`
**Grain:** District × week

##### Table B — OPD Health Facility Wise
```
DISTRICT   | BHU   | RHC  | THQ  | DHQ  | THOS | Total OPD | Per Day OPD
ATTOCK     | 3,764 | 6,487| 6,196| 9,063| —    | 25,510    | 3,644
...
```

**Fields:** `DISTRICT`, `BHU`, `RHC`, `THQ`, `DHQ`, `THOS`, `Total OPD`, `Per Day OPD`
**Grain:** District × week

##### Table C — Prone Epidemic Diseases (THE PRIMARY DISEASE TABLE)

```
DISTRICT    | ILI | AFP | Typhoid | HIV/AIDS | Measles | Meningitis | NNT | CCHF | Dengue | Diphtheria | Pertussis | Chicken Pox
ATTOCK      | —   | —   | 14      | —        | —       | —          | —   | —    | —      | —          | —         | —
BAHAWALNAGAR| —   | —   | 10      | —        | —       | —          | —   | —    | —      | —          | —         | —
...
```

**Fields:** `DISTRICT` + 12 disease columns
**Grain:** District × week × disease (matches canonical grain)
**District count:** 31 (ALL CAPS formatting)
**Coverage:** All Punjab districts

##### Table D–K — Additional District-Level Tables

| Table | Content | Columns |
|---|---|---|
| D — ANC-1 Visits | Antenatal care first visits by facility type | DISTRICT, BHU, RHC, THQ, DHQ, THOS |
| E — Deliveries | C-Section vs Normal by district | District, C-Sections (%), Normal+VF (%) |
| F — Total Deliveries/ANC/PNC | Delivery and care visit counts | District, TOTAL DELIVERIES, ANC-1 VISITS, PNC-1 VISITS |
| G — Indoor Admissions | Total admissions by district | District, Indoor Total Admissions |
| H — Indoor Deaths | Total deaths by district | District, Indoor Total Deaths |
| I — FP Visits | Family planning visits by facility type | District, BHU, RHC, THQ, DHQ, THOS |
| J — FP Methods | Method-wise family planning clients | DISTRICT, COC cycles, Condom, DMPA, IUCD, Implants, POP, Tubal Ligation, Vasectomy |
| K — FP Visits Status | PAFP, PPFP, <25yr breakdowns | DISTRICT, Total FP, PAFP, PPFP, FP <25yr, PPFP <25yr, PAFP <25yr |

#### 3.1.4 DHIS2 Era (2024–2025) — Complete Table Inventory

~24 standardized tables, ALL at provincial aggregate level. District data only in maps and narrative text.

##### Core OPD Tables

| Table | Content | Key fields |
|---|---|---|
| Age & Gender OPD Attendance | OPD visits by age group, split by male/female | Age Group (<1yr through 50+), Female (%), Male (%) |
| Total OPD Visits Weekly Comparison | Week-over-week OPD trend | Week, Total OPD Visits |
| Average OPD per Day by Facility Type | Facility workload | Facility Type (BHU/RHC/THQ/DHQ/THOS), Average Visits |
| Specialty-wise OPD New Cases | Cases by medical specialty | Specialty (20+ specialties), Cases (thousands) |

##### Disease Tables

| Table | Content | Key fields |
|---|---|---|
| Suspected OPD Disease-wise New Cases | ~55-68 diseases across 15 disease categories | sr., Disease Category, Disease Name, Count |
| Top 10 Communicable Diseases | Ranked by volume | Disease, Percentage |
| Top 10 Non-Communicable Diseases | Ranked by volume | Disease, Percentage |
| Disease-wise Indoor Admission | ~65-67 diseases admitted | sr., Disease Category, Disease Name, Count |

**Disease categories in the OPD disease table** (15 categories, ~68 diseases):

| Category | Example diseases |
|---|---|
| Respiratory | ARI, Pneumonia, Asthma, COPD, TB |
| Communicable | Diarrhea, Typhoid, Malaria, Dengue, Measles, Meningitis |
| Skin | Scabies, Dermatitis, Fungal Infections |
| Cardiovascular | Hypertension, IHD, CVA/Stroke |
| Psychiatric | Depression, Anxiety, Drug Dependence |
| Eye/ENT | Otitis Media, Cataract, Trachoma, Glaucoma |
| Gastrointestinal | Peptic Ulcer, Hepatitis, Cirrhosis |
| Vaccine Preventable | Measles, Diphtheria, Pertussis, NNT, AFP |
| Cancer | Oral Cancer, Breast Cancer (narrative mentions) |
| Oral | Dental Caries |
| Neurological | Epilepsy |
| Injuries | RTA, Burns, Fractures |
| Endocrine | Diabetes Mellitus |
| STI | Gonorrhea, Syphilis, HIV/AIDS |

##### MNCH Tables

| Table | Content |
|---|---|
| Age & Gender Indoor Admission | Admissions by age group and gender |
| Surgeries by Anesthesia Type | GA vs Spinal vs Local percentages |
| MNCH Services Summary | ANC, deliveries, PNC, C-section counts |
| Weekly Comparative ANC Anemia | ANC-1 visits and Hb<10 prevalence |
| Deliveries by Health Facility Type | Normal/C-section by BHU/RHC/THQ/DHQ/THOS |
| Types of Deliveries | C-Section vs Normal/Forceps |
| Maternal Complications (Inpatient) | Complication type and admission counts |
| Neonatal Deaths with Complications | Complication type and death counts |

##### Family Planning Tables

| Table | Content |
|---|---|
| Family Planning by Method | COC, Condoms, DMPA, IUCD, Implants, POP, TL, Vasectomy |
| FP Client Visits by Service Type | PAFP, PPFP, <25yr breakdowns |
| FP Visits by Health Facility | BHU/RHC/THQ/DHQ/THOS distribution |
| Weekly FP Visits Comparison | Week-over-week trend |

##### Trend Analysis & Outbreak Tables

| Table | Content |
|---|---|
| Trend Analysis (daily OPD) | Daily OPD disease counts for outbreak-prone diseases |
| Trend Analysis (daily IPD) | Daily IPD disease counts |
| Disease Alerts | Disease names flagged as alerts |

##### Additional Tables

| Table | Content |
|---|---|
| KMC Indicators | Kangaroo Mother Care initiation, outcome, follow-up |
| Laboratory Investigations | Test categories and counts |

#### 3.1.5 Annual Reports (2013–2024)

Annual reports contain the same indicators as weekly reports but at annual granularity, with year-over-year trend comparisons. The `Analysis_2021.md` (Monthly Feedback Report for Jan–Oct 2021) includes:

- OPD attendance by age and gender
- Per-capita OPD trends (2011–2021)
- Priority disease 10-year comparisons
- Drug stock-out status by district (36 districts, percentage stock-out)
- Immunization coverage comparisons
- Delivery trends and maternal/neonatal death analysis
- Disease forecasting for epidemic-prone diseases (2021–2023)
- Facility-type comparisons

#### 3.1.6 Special Outbreak Reports

**`lahore_measles_outbreak.md`** (Lahore Measles Outbreak Investigation, June 2023):
- Sub-district granularity: 10 towns / union councils within Lahore
- Suspected vs confirmed case breakdowns
- Vaccination campaign coverage by district
- Daily vaccination trend

**`jhelum_leishmania.md`** (Leishmaniasis Upsurge Visit Report, June 2023):
- Sub-district granularity: 16 areas/neighborhoods within Tehsil Pind Dadan Khan, Jhelum
- Month-wise trend: 2021, 2022, 2023
- Age and gender distribution of cases

#### 3.1.7 Quality Issues

| ID | Issue | Severity | Detail |
|---|---|---|---|
| DHIS-01 | Two incompatible formats (DHIS-II vs DHIS2) | HIGH | Require separate parsers or a format-detector |
| DHIS-02 | DHIS2 era has NO district-level disease tables | HIGH | 2024–2025 data is province aggregate only — cannot contribute to canonical grain |
| DHIS-03 | Filename does not encode year | HIGH | `Week_18.md` could be 2022, 2024, or 2025 — must open file to determine |
| DHIS-04 | Multiple files per week number (up to 4) | HIGH | `week_41.md`, `week_41(1).md`, `week_41(2).md`, `week_41(3).md` — unclear which is which year |
| DHIS-05 | Case inconsistency (`Week_18.md` vs `week_18.md`) | MEDIUM | File system treats these as different files |
| DHIS-06 | District name formatting varies (ALL CAPS in 2022, Title Case in 2024–2025) | MEDIUM | Normalize before joining |
| DHIS-07 | Misspelled filename (`janauary_2021.md`) | LOW | Use content-based date extraction |
| DHIS-08 | Coverage gaps — no year has all 52 weeks | MEDIUM | Document which weeks are available per year |
| DHIS-09 | 2022 format: 31 districts; 2024–2025: 36 districts (new districts created in 2023) | MEDIUM | Boundary versioning needed |

#### 3.1.8 Canonical Grain Mapping

| Dimension | DHIS-II (2022) | DHIS2 (2024–2025) |
|---|---|---|
| District | ✅ YES (31 districts) | ❌ Province only |
| Epi-week | ✅ YES | ✅ YES |
| Disease | ✅ YES (12 epidemic diseases) | ✅ YES (68 diseases, but aggregate) |
| Case count | ✅ YES | ✅ YES (province-level) |

**Assessment:** DHIS-II era (2022) is valuable for its district × disease matrix of 12 epidemic-prone diseases covering Punjab. DHIS2 era is mostly useful for MNCH, FP, NCD burden indicators, and disease trend analysis — but cannot feed the canonical district × epi-week disease panel.

---

### 3.2 KPK Report 2017 — Khyber Pakhtunkhwa DHIS Annual Report

#### 3.2.1 Source Identity

| Attribute | Value |
|---|---|
| **Full name** | DHIS Annual Report 2017 — District Health Information System, Khyber Pakhtunkhwa |
| **Custodian** | Directorate General Health Services, Khyber Pakhtunkhwa |
| **Publishing cadence** | Annual (single report for calendar year 2017) |
| **Coverage** | 25 districts of KPK, all public primary and secondary care facilities |
| **Access tier** | Public |
| **Data lineage** | Original PDF → parsed to markdown → stored in `KPK Report 2017/out/MD/` |
| **File size** | 285 KB (largest single file in the dataset) |

#### 3.2.2 Document Structure

A comprehensive annual health system performance report with 20+ sections:

1. Provincial profile (capital, population, districts, health facility counts)
2. Reporting compliance by district (target: 95%)
3. General OPD Attendance — 25.1M total visits, by gender and age group
4. Specialty-wise patient breakup (Emergency, General, Pediatric, OB/GYN, Medicine, etc.)
5. Average daily new cases per facility — district ranking (Swat highest: 93K/day)
6. Disease Pattern — 43 priority diseases ranked by case count
7. Top 10 Diseases — with % of total OPD
8. Communicable vs Non-Communicable split
9. Lab services utilization
10. ANC services (average ANC visits per facility)
11. District-wise deliveries (institutional)
12. Anemia among women at ANC-1
13. Family Planning visits & methods
14. Immunization coverage
15. Malaria cases — district-wise
16. Hepatitis B and C patients — district-wise
17. TB-DOTS patients — district-wise (including treatment adherence)
18. Mortality rates — multiple indicators
19. District comparison: Live Births vs Low Birth Weight & Still Births
20. Trend analysis (5-year: 2012-13 to 2016-17)
21. Key Performance Indicators — district and DHQ hospital rankings
22. Medical Teaching Hospitals (LRH, KTH, DHQ Bannu) sections
23. Independent Monitoring Unit data

#### 3.2.3 Disease Classification

The report tracks **43 priority diseases** in two categories:

**Communicable (20 diseases):**
ARI (3.35M — #1 overall), Diarrhea/Dysentery <5yr (1.1M), Diarrhea/Dysentery >5yr (900K), Suspected Malaria (494K), Scabies (365K), Worm Infestation (326K), Enteric/Typhoid (225K), Pneumonia <5yr (189K), Pneumonia >5yr (135K), TB Suspects (116K), Viral Hepatitis (78K), Dog Bite (67K), Measles (33K), STIs (13K), Cutaneous Leishmaniasis (6K), Meningitis (6K), Neonatal Tetanus (2.6K), Snake Bites (1.4K), AFP (349), HIV/AIDS (42)

**Non-Communicable (23 diseases):**
Fever due to other causes (1.0M), UTI (816K), Hypertension (615K), Dental Caries (588K), Peptic Ulcer (501K), Diabetes Mellitus (368K), Otitis Media (241K), Dermatitis (235K), Asthma (226K), RTA (225K), Depression (204K), Fractures (77K), Cataract (77K), IHD (65K), COPD (52K), Trachoma (30K), Glaucoma (27K), Burns (22K), Epilepsy (21K), Drug Dependence (19K), BEP (19K), Nephritis/Nephrosis (14K), Cirrhosis of Liver (12K)

**District names:** 25 KPK districts with standard proper names (Chitral, Upper Dir, Lower Dir, Swat, Kohistan, Shangla, Bajaur, Battagram, Malakand, Mansehra, Mohmand, Mardan, Buner, Charsadda, Swabi, Haripur, Abbottabad, Peshawar, Nowshera, Khyber, Kurram, Orakzai, Hangu, Kohat, Karak, Bannu, North Waziristan, Lakki Marwat, South Waziristan, Tank, D.I. Khan). Mostly consistent naming.

#### 3.2.4 Quality Issues

| ID | Issue | Severity | Detail |
|---|---|---|---|
| KPK-01 | Annual granularity only | HIGH | Cannot contribute to weekly canonical grain |
| KPK-02 | Single year snapshot | MEDIUM | No trend comparison possible from this source alone |
| KPK-03 | OPD visits, not epi-surveillance | MEDIUM | Data represents healthcare utilization, not active disease surveillance — different interpretation |
| KPK-04 | District-to-facility mapping not documented | LOW | Which facilities report to which districts is implicit |

#### 3.2.5 Canonical Grain Mapping

| Dimension | Support | Detail |
|---|---|---|
| District | ✅ YES | All 25 KPK districts |
| Epi-week | ❌ NO | Annual granularity — data is for the full calendar year 2017 |
| Disease | ✅ YES | 43 priority diseases (CD + NCD) |
| Case count | ✅ YES | OPD visit counts for each disease |

**Assessment:** KPK 2017 is a rich annual reference dataset. It can validate annual aggregates from other sources and provide baseline disease burden numbers for KPK. It cannot feed the weekly panel but is valuable for annual comparisons and validating the coverage/completeness of other weekly sources.

---

## 4. Clinical Screening Campaigns — Deep Schema Analysis

### 4.1 Punjab Health Week — Mass Clinical Screening Campaign

#### 4.1.1 Source Identity

| Attribute | Value |
|---|---|
| **Full name** | Punjab Health Week — Mass Community Health Screening Campaign |
| **Custodian** | Health Department, Government of Punjab |
| **Publishing cadence** | Point-in-time (2 campaign waves) |
| **Campaign dates** | Health Week 2017 + Health Week 2 (February 2018) |
| **Coverage** | All 36 districts of Punjab |
| **Access tier** | Public |
| **Data lineage** | Campaign report PDFs → parsed to markdown → stored in `Punjab Health Week 2/MD/` |
| **Sample sizes** | Hb: 233,864 | PEFR: 208,033 | TB: 326,781 |

#### 4.1.2 Nature of Data — Important Classification

This is **clinical screening data from a mass community health campaign**, not routine disease surveillance. Key distinctions:

| Attribute | Disease surveillance | Punjab Health Week |
|---|---|---|
| Data origin | Healthcare facility reports | Community screening camps |
| What's measured | Suspected/confirmed disease cases | Clinical measurements (Hb, BP, BSR, PEFR, BMI) + rapid tests |
| Temporal nature | Ongoing, weekly | Point-in-time, 2 discrete waves |
| Denominators | Usually none (raw counts) | Screened population (n) = explicit denominator |
| Output | Case counts | Prevalence rates (% positive among screened) |
| Diseases | Infectious/communicable | NCDs + blood-borne infections |

#### 4.1.3 Document Structure

**`PunjabHealthReport.md`** (229 KB — Health Week 2017):
1. Hemoglobin Level & Anemia — n=233,864; 56.6% anemic (Hb<12). District-wise tables + age/gender
2. Peak Expiratory Flow Rate (PEFR) — n=208,033; 41.3% deranged. District tables
3. Random Blood Sugar — diabetes screening. District tables
4. Blood Pressure — hypertension screening. District tables
5. Blood-Borne Disease Screening — Hepatitis B, Hepatitis C, HIV district-wise reactive rates
6. Malaria — district-wise screening
7. Pediatrics (<14 Years) — child health screening
8. Pulmonary TB Suspects — n=326,781; 2% suspects, 0.15% AFB positive
9. Antenatal Check-up — ANC screening
10. Community-Based Activities — immunization, deworming, iron tablets, nutrition, health education
11. Third Party Validation — citizen feedback

**`Week2VsWeek1.md`** (22.5 KB — February 2018 comparison):
District-wise comparison of all indicators between HW1 and HW2:
- High BP, BSR, BMI>25, Anemia, Deranged PEFR, Malaria, Hepatitis B, Hepatitis C

#### 4.1.4 Complete Table Inventory

##### Hemoglobin/Anemia Tables

```
Category        | Count    | Percentage
Normal (>12)    | 101,461  | 43.4%
Mild Anemia     | 107,019  | 45.8%
Moderate Anemia | 19,850   | 8.5%
Severe Anemia   | 5,534    | 2.4%
```

```
Age Group | % Screened | % with Hb <12
14-25     | 21%        | 58%
25-40     | 39%        | 57%
40-55     | 26%        | 55%
55+       | 14%        | 52%
```

**Fields:** `Category` (anemia severity or age group), `Count`, `Percentage`
**District-level:** Yes — separate tables for district-wise anemia prevalence

##### Blood-Borne Disease Screening Tables

Hepatitis B, Hepatitis C, and HIV rapid test results by district. Output format: "% reactive among screened."

##### TB Screening Tables

n=326,781 screened. Output: % TB suspects, % AFB positive. District-wise breakdown available.

##### Health Week Comparison Tables (Week2VsWeek1.md)

```
Indicator     | Health Week 1 | Health Week 2 | Difference
High BP       | 23.1%         | 28.4%         | +5.3%
BSR > Random  | 6.8%          | 8.2%          | +1.4%
BMI > 25      | 34.2%         | 37.1%         | +2.9%
Anemia        | 56.6%         | 52.4%         | -4.2%
```

#### 4.1.5 Quality Issues

| ID | Issue | Severity | Detail |
|---|---|---|---|
| PHW-01 | Point-in-time data — cannot be trended with weekly surveillance | HIGH | Use as a reference/validation dataset, not as a time series |
| PHW-02 | Self-selection bias (voluntary screening) | MEDIUM | Screened population ≠ general population; prevalence rates may be biased |
| PHW-03 | No epi-week mapping | HIGH | Data is for campaign dates, not epi-weeks |
| PHW-04 | District name abbreviations (`M.B Din`, `D.G KHAN`, `R.Y. KHAN`, `T.T SINGH`) | LOW | Normalize via aliases |

#### 4.1.6 Mapping to CHIP Data Model

| Canonical dimension | Support | Detail |
|---|---|---|
| District | ✅ YES | All 36 Punjab districts |
| Epi-week | ❌ NO | Campaign dates only |
| Disease (communicable) | ✅ Partial | TB, Malaria, Hepatitis B/C, HIV |
| Disease (NCD) | ✅ YES | Anemia, Hypertension, Diabetes, Respiratory (PEFR), Obesity (BMI) |
| Case count | ✅ Modified | Prevalence rates with explicit denominator (screened population) |

**Assessment:** Punjab Health Week data does not fit the canonical district × epi-week × disease surveillance grain. It represents a different measurement paradigm (population screening prevalence). It is valuable as a cross-sectional validation dataset for NCD burden and blood-borne disease prevalence, and might inform baseline risk models, but it should not be forced into the weekly disease case-count schema. Treat as a **supplementary dataset** with its own schema.

---

## 5. Cross-Source Comparison Matrix

### 5.1 All Sources Mapped to Canonical Dimensions

| Source | District-level | Epi-week | Disease cases | Deaths | Time span | Unique weeks available |
|---|---|---|---|---|---|---|
| NIH IDSR | Yes (3 provinces) | Yes | ~30 diseases | Narratively | 2021–2026 | ~174 |
| WHO DEWS | No (national only) | Yes | 17 diseases | Not tabulated | 2013–2014 | 94 |
| PITB-DSS | Yes (36 Punjab districts) | Yes | ~15 diseases | Not tabulated | 2015–2018 | ~169 |
| AJK IDSRS | Yes (10 AJ&K districts) | Yes | ~18 diseases | Yes (explicit) | 2026 | 3 |
| DHIS Punjab (2022) | Yes (31 Punjab districts) | Yes | 12 epidemic diseases | Yes (indoor deaths) | 2022 | ~52 |
| DHIS Punjab (2024–25) | No (province only) | Yes | 68 diseases | Not tabulated | 2024–2025 | ~52 |
| KPK Report 2017 | Yes (25 KPK districts) | No (annual) | 43 diseases | Yes (mortality section) | 2017 | 1 (annual) |
| Punjab Health Week | Yes (36 Punjab districts) | No (campaign dates) | 7 conditions | Not applicable | 2017–2018 | 2 campaigns |

### 5.2 Geographic Coverage by Province

| Province | NIH IDSR | WHO DEWS | PITB-DSS | AJK IDSRS | DHIS Punjab | KPK 2017 |
|---|---|---|---|---|---|---|
| Punjab | Province-only (2024+) | Province-only | ✅ All 36 districts | — | ✅ 31–36 districts | — |
| Sindh | ✅ 29–30 districts | Province-only | — | — | — | — |
| KP | ✅ 27–39 districts | Province-only | — | — | — | ✅ 25 districts |
| Balochistan | ✅ 33–36 districts | Province-only | — | — | — | — |
| AJ&K | Province-only | Not covered | — | ✅ 10 districts | — | — |
| GB | Province-only | Not covered | — | — | — | — |
| ICT | Province-only | Not covered | — | — | — | — |

**Key gaps:**
- **AJ&K and GB** have no district-level data in NIH (only AJK IDSRS has 3 weeks for AJ&K)
- **ICT/Islamabad** has no district-level data anywhere
- **Sindh, Balochistan, KP** have no district-level data outside of NIH IDSR
- **Punjab** has district-level data from PITB-DSS (2015–2018) and DHIS-II (2022) but NOT from NIH IDSR or DHIS2 (2024–2025)

### 5.3 Disease Coverage Overlap (where cross-validation is possible)

| Disease | NIH IDSR | WHO DEWS | PITB-DSS | AJK IDSRS | DHIS-II (2022) |
|---|---|---|---|---|---|
| Acute diarrhoea | ✅ | ✅ | ✅ | ✅ | — |
| Bloody diarrhoea | ✅ | ✅ | ✅ | ✅ | — |
| Typhoid | ✅ | ✅ | ✅ | ✅ | ✅ |
| Malaria | ✅ | ✅ | ✅ | ✅ | — |
| Measles | ✅ | ✅ | ✅ | ✅ | ✅ |
| Meningitis | ✅ | ✅ | — | ✅ | ✅ |
| AFP | ✅ | ✅ | ✅ | ✅ | — |
| NNT | ✅ | ✅ | ✅ | ✅ | ✅ |
| Diphtheria | ✅ | ✅ | ✅ | ✅ | ✅ |
| Pertussis | ✅ | ✅ | — | ✅ | ✅ |
| Dengue | ✅ | ✅ | — | ✅ | ✅ |
| ARI/ILI | ✅ | ✅ | ✅ | ✅ | ✅ |
| Pneumonia/ALRI | ✅ | ✅ | — | ✅ | — |
| Dog bite | ✅ | — | ✅ | ✅ | — |
| Scabies | ✅ | ✅ | ✅ | — | — |
| Leishmaniasis | ✅ | ✅ | — | ✅ | — |
| HIV/AIDS | ✅ | — | ✅ | ✅ | ✅ |
| Viral Hepatitis | ✅ | ✅ | — | ✅ | — |
| TB | ✅ | — | ✅ | — | — |
| Chickenpox | ✅ | — | — | ✅ | ✅ |
| CCHF | — | — | — | — | ✅ |

**Best cross-validation opportunities (same disease, same province, overlapping time):**
- NIH IDSR (2021–2026) × PITB-DSS (2015–2018): No temporal overlap. PITB ends 2018, NIH starts 2021.
- NIH IDSR (2021–2026) × DHIS-II (2022): **YES — Punjab, 2022, for ILI, Typhoid, Measles, Meningitis, NNT, Diphtheria, Pertussis, Dengue, Chickenpox**
- NIH IDSR (2021–2026) × WHO DEWS (2013–2014): No temporal overlap
- NIH IDSR (2024+) × DHIS2 (2024–2025): Temporal overlap but DHIS2 is province-level only — can cross-validate national totals, not district data

---

## 6. Reference Tables

### 6.1 All District Names (as Observed in Sources)

This table is a work-in-progress reference for the `location_alias` seeding. Bold indicates the recommended canonical name.

#### Punjab (36 districts)

| Canonical | Observed variants |
|---|---|
| Attock | `ATTOCK`, `Attock` |
| Bahawalnagar | `BAHAWALNAGAR`, `BAHAWALNAG...`, `Bahawalnagar` |
| Bahawalpur | `BAHAWALPUR`, `Bahawalpur` |
| Bhakkar | `BHAKKAR`, `Bhakkar` |
| Chakwal | `CHAKWAL`, `Chakwal` |
| Chiniot | `CHINIOT`, `Chiniot` |
| Dera Ghazi Khan | `D.G KHAN`, `D.G. KHAN`, `Dera Ghazi Khan` |
| Faisalabad | `FAISALABAD`, `Faisalabad` |
| Gujranwala | `GUJRANWALA`, `Gujranwala` |
| Gujrat | `GUJRAT`, `Gujrat` |
| Hafizabad | `HAFIZABAD`, `Hafizabad` |
| Jhang | `JHANG`, `Jhang` |
| Jhelum | `JHELUM`, `Jehlum`, `Jhelum` |
| Kasur | `KASUR`, `Kasur` |
| Khanewal | `KHANEWAL`, `Khanewal` |
| Khushab | `KHUSHAB`, `Khushab` |
| Lahore | `LAHORE`, `Lahore` |
| Layyah | `LAYYAH`, `Layyah` |
| Lodhran | `LODHRAN`, `Lodhran` |
| Mandi Bahauddin | `MANDI BAHAUDDIN`, `M.B DIN`, `Mandi Bahauddin`, `MANDI...` |
| Mianwali | `MIANWALI`, `Mianwali` |
| Multan | `MULTAN`, `Multan` |
| Muzaffargarh | `MUZAFFARGARH`, `MUZAFFARGA...`, `Muzaffargarh` |
| Nankana Sahib | `NANKANA SAHIB`, `NANKANA...`, `Nankana Sahib` |
| Narowal | `NAROWAL`, `Narowal`, `Narrowal` |
| Okara | `OKARA`, `Okara` |
| Pakpattan | `PAKPATTAN`, `PAK PATTAN`, `Pakpattan` |
| Rahim Yar Khan | `RAHIM YAR KHAN`, `R.Y. KHAN`, `RAHIMYAR KHAN`, `RAHIMYAR...`, `Rahim Yar Khan` |
| Rajanpur | `RAJANPUR`, `Rajanpur` |
| Rawalpindi | `RAWALPINDI`, `Rawalpindi` |
| Sahiwal | `SAHIWAL`, `Sahiwal` |
| Sargodha | `SARGODHA`, `Sargodha` |
| Sheikhupura | `SHEIKHUPURA`, `Sheikhupura` |
| Sialkot | `SIALKOT`, `Sialkot` |
| Toba Tek Singh | `TOBA TEK SINGH`, `T.T SINGH`, `T.T. SINGH`, `TOBA TEK...`, `Toba Tek Singh` |
| Vehari | `VEHARI`, `Vehari` |

#### Sindh (29 districts — NIH IDSR)

| Canonical | Observed variants |
|---|---|
| Badin | `Badin` |
| Dadu | `Dadu` |
| Ghotki | `Ghotki` |
| Hyderabad | `Hyderabad` |
| Jacobabad | `Jacobabad` |
| Jamshoro | `Jamshoro` |
| Kamber Shahdadkot | `Kamber`, `Kam-ber`, `Kamber Shadadkot` |
| Karachi Central | `Karachi Central`, `Kar-Central` |
| Karachi East | `Karachi East`, `Karachi-East`, `Kar-East` |
| Karachi Korangi | `Karachi Korangi`, `Kar-Korangi` |
| Karachi Malir | `Karachi Malir`, `Karachi-Malir`, `Kar-Malir` |
| Karachi South | `Karachi South`, `Kar-South` |
| Karachi West | `Karachi West`, `Kar-West` |
| Kashmore | `Kashmore` |
| Khairpur | `Khairpur` |
| Larkana | `Larkana`, `Lar-kana` |
| Matiari | `Matiari` |
| Mirpur Khas | `Mirpur Khas`, `Mir-purkhas` |
| Naushahro Feroze | `Naushero Feroze`, `Naushahro Feroze`, `N. Feroze`, `NosheroFeroz` |
| Sanghar | `Sanghar` |
| Shaheed Benazirabad | `Shaheed Benazirabad` (2023+) |
| Shikarpur | `Shikarpur` |
| Sujawal | `Sujawal` |
| Sukkur | `Sukkur` |
| Tando Allahyar | `Tando Allahyar` |
| Tando Muhammad Khan | `Tando Muhammad Khan` |
| Tharparkar | `Tharparkar`, `Thar-parkar` |
| Thatta | `Thatta` |
| Umerkot | `Umerkot` |

#### Balochistan (33+ districts — NIH IDSR)

District names observed: Awaran, Barkhan, Chagai, Chaman, Dera Bugti, Duki, Gwadar, Harnai, Jaffarabad/Jaffrabad, Jhal Magsi, Kachhi/Bolan, Kalat, Kech/Turbat, Kharan, Khuzdar, Killa Abdullah, Killa Saifullah, Kohlu, Lasbela/Lasbella, Loralai, Mastung, Musakhel, Naseerabad/Naserabad, Nushki, Panjgur, Pishin, Quetta, Sherani, Sibi, Sohbatpur, Surab, Washuk, Ziarat, Zhob

#### KP (27+ districts — NIH IDSR and KPK Report 2017)

District names observed: Abbottabad, Bajaur, Bannu, Battagram, Buner/Bunner, Charsadda, Chitral (Lower/Upper in later years), D.I. Khan, Dir Lower, Dir Upper, Hangu, Haripur, Karak, Khyber, Kohat, Kohistan (Lower/Upper in later years), Kolai-Palas, Kurram, Lakki Marwat, Malakand, Mansehra, Mardan, Mohmand, Nowshera/Nowshehra, Orakzai, Peshawar, Shangla, Swabi, Swat, Tank, Toor Ghar, Torghar, North Waziristan, South Waziristan

#### AJ&K (10 districts — AJK IDSRS)

| Canonical | Observed variants |
|---|---|
| Muzaffarabad | `MZD`, `Muzaffarabad` |
| Jhelum Valley | `JV`, `Jhelum Valley` |
| Neelum | `Neelum` |
| Poonch | `Poonch` |
| Bagh | `Bagh` |
| Haveli | `Haveli` |
| Sudhnoti | `Sudhnoti`, `Sudhnooti`, `Sudhnuti` |
| Mirpur | `Mirpur` |
| Bhimber | `Bhimber` |
| Kotli | `Kotli` |

#### Gilgit-Baltistan (10 districts — NIH IDSR only, no district tables)

Not enumerated in data tables. Districts include: Gilgit, Skardu, Diamer, Ghizer, Ghanche, Astore, Hunza, Nagar, Shigar, Kharmang.

### 6.2 Disease Label Crosswalk (All Sources → Canonical)

See Appendix A for the full crosswalk table (available as a separate CSV file at `data/vocab/disease_crosswalk.csv`).

---

## 7. Resolved Design Decisions

### 7.1 Source classification & logical source boundaries

| ID | Decision | Rationale |
|---|---|---|
| **L1-02** | DHIS-II (2022) and DHIS2 (2024–2025) are **separate logical sources** with independent connectors: `dhis_punjab_weekly_v1` and `dhis_punjab_bulletin_v1`. | They have incompatible table structures, different granularity (district vs province), and different disease sets. A single connector would need to fork its entire parse, validate, and produce path — effectively two connectors wearing one name. They share a custodian but not a data model. |
| **L1-01** | Punjab Health Week data will be stored in a **separate `fact_screening_campaign` table** (not forced into `fact_disease_cases`). | It uses a different measurement paradigm: prevalence rates with explicit denominators from mass screening, not weekly suspected-case counts from surveillance. Hep B/C/HIV prevalence rates from 36 districts are the only population-level blood-borne disease numbers available and are useful for validating surveillance sensitivity. |

### 7.2 Connector behavior

| ID | Decision | Rationale |
|---|---|---|
| **L1-03** | NIH IDSR connectors **will produce province-level records** for AJK, GB, ICT, and Punjab (provinces that appear in Table 1 but have no district tables). These records carry a special `pcode` (e.g., `PK-AJK-ALL`) and a provenance flag `district_breakdown: false`. | It's better to emit the province-level number with a clear tag than to silently drop data. The normalizer (Layer 4) can decide whether to use or exclude these records. They also provide a reconciliation total against which district-level sums from other sources (AJK IDSRS, PITB-DSS) can be validated. |
| **L1-04** | DHIS filename disambiguation happens **inside the connector's `fetch` stage**: open each file, extract the year from content (every report has a date range in the opening paragraphs), and derive the stable `identity` key (`dhis_punjab:2022:W41`). The connector **normalizes the filename before archiving to bronze** — the clean identity becomes the bronze object key, and the original messy filename is preserved in the `.meta.json` sidecar. | Content-based extraction is reliable (seconds for 111 files). The pre-archive rename keeps bronze clean while preserving provenance. When live PDF ingestion begins later, HTTP downloads already have messy filenames (`Weekly_Report-39-2025.pdf?v=NN`), so the connector-owned normalization pattern scales naturally. |
| **L1-05** | Connectors **emit the ORIGINAL disease label** as written in the source document. The normalizer (Layer 4) maps to the canonical `disease_code`. | If the crosswalk is wrong, we fix it in the normalizer and replay from the existing Kafka topic without re-parsing. If the connector had emitted the canonical label, a crosswalk error would require re-parsing from bronze. |
| **L1-06** | Trend graphs and figure data are **not ingested**. Only explicit tabular data sections are parsed. | NIH trend graphs contain demonstrably synthetic placeholder values (flat "100" across 50 weeks). Ingesting figure-derived data would corrupt the panel. |

### 7.3 Prototype prioritization

| ID | Decision | Rationale |
|---|---|---|
| **L1-07** | **Phase 1 prototype ingestion targets 4 sources:** NIH IDSR, PITB-DSS, AJK IDSRS, and DHIS-II (2022). DHIS2 (2024–2025), KPK 2017, and Punjab Health Week are deferred to a second pass. | These 4 are the only sources that directly map to the canonical district × epi-week × disease grain. They provide ~400 weekly bulletins with district-level disease data covering 4 provinces and 6+ years. |
| **L1-08** | **Build the `location_alias` registry before writing any connector.** Seed it from the observed district name variants documented in §6.1 of this catalog. | District name inconsistency (30+ variants for the same districts) is the #1 integration risk. Every connector will produce records keyed by district, and every one will encounter misspellings, abbreviations, and historical names. A shared alias registry seeded upfront prevents every connector from implementing its own ad-hoc normalization. |

---

### 7.4 Deferred to downstream layers

| ID | Decision | Rationale |
|---|---|---|
| **OQ-1** | Epi-week convention is **not resolved at Layer 1**. Each source's stated week number is ingested as-is. The target granularity is the temporal range (week), not day-level precision, so the convention mismatch between WHO/ISO and CDC/MMWR is immaterial at this grain. | The platform's canonical grain is district × epi-week. Whether a Tuesday-to-Sunday "Week 1" differs from a Monday-to-Sunday "Week 1" by one day does not affect weekly aggregation at this scale. |
| **OQ-5** | Institutional grain for future feeds is **removed as an open question**. When NIH MOUs land, CHIP will specify the preferred data format — we are building the platform that ingests their data, so the grain is ours to define. | Not a question to resolve; it's a requirement to communicate upstream. |
| **L1-09** | DHIS special outbreak reports (sub-district granularity) require **no special handling at Layer 1**. They are ingested like any other document. The sub-district place names will be resolved to districts via NER/geocoding in Layer 4 (the normalizer). | District normalization is a general capability the normalizer needs for all text-heavy sources (news, sitreps). Outbreak reports are not unique in having sub-district place mentions. |
| **L1-10** | Cross-source disagreements between NIH and DHIS are **not resolved at Layer 1**. Both sources are ingested independently with full provenance. Disagreement detection is an anomaly detection concern in the knowledge graph / analytics layer (Layer 6). | Ingesting both sources faithfully and tagging disagreements when they surface is more valuable than pre-judging which source is authoritative. |

---

## 8. Open Questions

*None remaining at Layer 1. All identified questions have been resolved or explicitly deferred to downstream layers with documented rationale.*


---

## Appendix A: Document History

| Date | Author | Change |
|---|---|---|
| 2026-07-15 | Architecture team | Initial complete catalog from `Data_sources_1/` |
| 2026-07-15 | Architecture team | Resolved L1-01 through L1-08; restructured into Resolved Decisions + Open Questions sections |
| 2026-07-15 | Architecture team | Resolved OQ-1, OQ-5, L1-09, L1-10. All open questions closed or deferred to downstream layers. |
