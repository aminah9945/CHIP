# ADR-006: One Canonical Spatial Key — COD-AB District P-codes

- **Status:** Accepted (2026-07-10 — reconciliation decision; resolves a cross-subsystem contract conflict found in architecture review)
- **Context:** ADR-005 makes the district the canonical grain and calls for a versioned gazetteer with P-codes. In practice the subsystem docs drifted onto **three different code spaces**: COD-AB P-codes (`PK101`) in subsystems 01/06, **GADM 4.1** ids (`PAK.8.14_1`) in the NLP geo-linker (03), and a `pcode` described as **"PBS/HDX admin code"** in the CHKG (04); analytics (05) used an opaque integer `district_id`. Because the NLP geo-linker's output feeds the CHKG and the gold panel, three code spaces on one join is a genuine integration break, not cosmetics.

## Decision

1. **The single canonical spatial key is the OCHA/HDX COD-AB Pakistan `admin2` P-code** (e.g. `PK101`), sourced from the versioned gazetteer defined in subsystem 01 §1. This is the one key every fact row, every KG `Location` node, every forecast row, and every API response uses.
2. **All other place-code systems are alias/enrichment sources only**, mapped *into* COD-AB via the alias table (01 §1.2–1.3), never used as a primary key:
   - **GADM 4.1** and **GeoNames** — alternate-name harvesting and geometry cross-check.
   - **PBS census codes**, **PMD station registry**, **NIH reporting-unit lists** — crosswalked into P-codes (01 §5.5, OQ-5).
3. **The NLP geo-linker (03) emits `pcode`, not `gadm_id`.** Its internal candidate generation may still use GADM/GeoNames alt-name tables, but the resolved output field is the COD-AB P-code. GADM ids appearing in subsystem-03 examples are illustrative and resolve to a P-code.
4. **The CHKG `Location.pcode` (04) is the COD-AB P-code** (the "PBS/HDX" wording in 04 is corrected to COD-AB).
5. **Analytics `district_id` (05) is the P-code** (or carries a 1:1 mapping column to it); the serving layer (06) already uses OCHA admin-2 P-codes — no change.

Boundary-change/versioning (splits, merges, renames) is handled by the SCD-2 + `location_lineage` design already in 01 §1.4; that machinery is unaffected by this choice.

## Alternatives rejected

- **GADM 4.1 as canonical:** rich alternate names and global coverage, but GADM ids are opaque and **not stable across GADM versions**, it is not the humanitarian/government standard, and NDMA/NIH/PDMA reporting aligns to OCHA P-codes — we would be translating away from our own stakeholders' code space.
- **PBS census codes as canonical:** the official Pakistani administrative coding, but not published as a clean open gazetteer with geometries and alias tables, and it re-versions on census cycles — more operational friction than COD-AB for the same districts.
- **geoBoundaries:** good open geometries, but COD-AB is the de-facto standard for exactly this humanitarian/health context and ships the tabular alias source we need.

## Consequences

- Subsystem 01 is confirmed as the single owner of the gazetteer and alias tables; 03/04/05 conform to its P-code output.
- The GADM/GeoNames alt-name lists become *inputs to the alias seed* (01 §1.3), preserving the NLP team's disambiguation signal without making GADM a key.
- Supersedes any conflicting statement in subsystems 03 (§5.1 "canonical to GADM 4.1"), 04 (§1.1/§2.4 "PBS/HDX"), and 05 (opaque `district_id`).
