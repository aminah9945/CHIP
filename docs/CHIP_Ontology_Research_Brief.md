# Ontology Selection for CHIP: Climate–Health Knowledge Graph
### Evidence-based reference brief — for presentation to project supervisors

This document is structured so every ontology is evaluated against the same six questions, each backed by a citable source. Use Section 8 (References) to pull up any claim on demand during the meeting.

---

## How to read this document

For each ontology:
1. **What is it** — scope and technical nature
2. **Who created it, and what is their credibility** — named individuals, their institution, and independently verifiable markers of authority (grants, standards bodies, prior track record)
3. **Where is it already applied** — named organizations/projects, not "widely used" hand-waving
4. **Pros and cons**
5. **Relevance to CHIP** — the specific architectural role it would play in your Kafka/Spark/knowledge-graph pipeline
6. Citation is inline as `[n]`, resolved in Section 8

---

# PART A — WEATHER / CLIMATE / ENVIRONMENT ONTOLOGIES

## A1. SOSA/SSN (Sensor, Observation, Sample, Actuator / Semantic Sensor Network)

**1. What it is**
A pair of ontologies — SOSA (lightweight core) and SSN (expressive extension) — that formally model the *act of observation*: a sensor observes a property of a feature-of-interest, producing a result, at a place and time. Published simultaneously as a **W3C Recommendation** and an **OGC (Open Geospatial Consortium) Implementation Standard** [1][2]. This is not a research prototype — it is a ratified international specification with the same standards-track status as HTML or GeoJSON.

**2. Who created it, and credibility**
Developed by the joint **W3C/OGC Spatial Data on the Web (SDW) Working Group**, chartered jointly by the two standards bodies. Named editors on the standard and its companion peer-reviewed paper include **Armin Haller** (CSIRO Data61 / later Australian National University), **Krzysztof Janowicz** (UC Santa Barbara, later University of Vienna), **Simon Cox** (CSIRO — notably also a co-editor of **ISO 19156 Observations & Measurements**, the ISO standard that national meteorological and hydrological agencies already use for reporting), **Danh Le Phuoc**, and **Maxime Lefrançois** [1][3]. Credibility markers: (a) dual ratification by W3C *and* OGC — two independent standards bodies had to independently approve it; (b) the lead editor of the geospatial-observation ISO standard is a co-author, meaning SOSA/SSN was deliberately designed to be compatible with what meteorological agencies already do, not invented in a vacuum; (c) the design process itself was evidence-based — the working group collected **51 real-world use cases and derived 62 formal requirements** before writing the ontology [3].

**3. Where it is already applied**
Per the ontology authors' own published usage survey [3]: **Geoscience Australia** (Australian federal government geoscience agency) uses SOSA to model environmental samples; the **Center for Marine Environmental Sciences, University of Bremen** uses it for oceanographic time-series data; **Irstea** (French National Research Institute for Agriculture, Food and Environment) publishes **meteorological weather-station measurement datasets** directly modeled in SOSA — this is the closest documented precedent to what CHIP itself needs to do. The same survey documents at least 23 independent ontologies and 23 independent datasets reusing SOSA classes.

**4. Pros and cons**

| Pros | Cons |
|---|---|
| Ratified dual international standard — very defensible to reviewers | Only models the *act of sensing*, not weather semantics (doesn't define what a "heatwave" *is*) |
| Directly compatible with ISO 19156, which met agencies already use | Needs pairing with a domain ontology (ENVO/SWEET) for actual concept hierarchy |
| Lightweight core (SOSA) keeps implementation overhead low | SSN's fuller axiomatization (DUL alignment) adds complexity if you need it |
| Actively maintained — 2023 edition released, backward-compatible [2] | No native disease/health linkage (expected — out of scope by design) |
| Documented precedent for exactly your use case (Irstea weather stations) | |

**5. Relevance to CHIP**
This is the structural template for your Apache Kafka ingestion layer from PMD and the Climatological Data Processing Centre. Every incoming weather-station reading (temperature, precipitation, humidity) becomes a `sosa:Observation` with `sosa:hasFeatureOfInterest`, `sosa:observedProperty`, and `sosa:hasResult` — giving you a standards-compliant, queryable structure before any domain-specific weather typing is even applied.

---

## A2. ENVO (Environment Ontology)

**1. What it is**
An OBO Foundry ontology providing a controlled, formally logical vocabulary for environmental systems, processes, and features — habitats, ecosystems, environmental processes (including floods and droughts as process types), and anthropogenic environments [4][5].

**2. Who created it, and credibility**
Founded by **Pier Luigi Buttigieg** and **Chris Mungall** [4][5]. Mungall leads the **Berkeley Bioinformatics Open-source Projects (BBOP) group at Lawrence Berkeley National Laboratory (a US Department of Energy national lab)**. Credibility marker that matters most here: BBOP is the same group that develops and stewards the **Gene Ontology (GO)** — widely regarded as the most successful and highly-cited ontology in the life sciences, in continuous operation and use since 1998. A team with a 25+ year track record of building an ontology that became global infrastructure is a materially different credibility class than a one-off academic ontology. ENVO is formally aligned to **BFO 2.0**, the same upper ontology underlying IDO (Part B), by explicit design decision "in aid of semantic homogeneity" with biomedical ontologies [4].

**3. Where it is already applied**
ENVO's own governance documentation [5][6] records formal collaborations with the **Genomic Standards Consortium (GSC)** — ENVO originated as GSC's metadata checklist vocabulary — the **ESIP Federation**, **UN Environment**, and the **IOC-UNESCO** (Intergovernmental Oceanographic Commission). It is used to annotate sample metadata for large international projects including the **Tara Oceans expedition** and **Ocean Sampling Day**, and is served through the **GFBio (German Federation for Biological Data) biodiversity portal** [6].

**4. Pros and cons**

| Pros | Cons |
|---|---|
| Same BFO foundation as IDO → causal edges between weather and disease are logically native, not bolted on | Historically stronger on habitats/ecosystems than fine-grained meteorological variables |
| Built by the same team behind the Gene Ontology — proven long-term maintenance capability | You'll likely need a small local extension for Pakistan-specific administrative/geographic terms |
| OBO Foundry governance = peer-reviewed ontology engineering, not ad hoc | |
| Free, OWL-native, actively versioned on GitHub | |

**5. Relevance to CHIP**
ENVO is the single most important architectural choice in this whole brief, because of the shared BFO foundation with IDO. It's what lets "flooding" (an ENVO environmental process) and "cholera outbreak" (an IDO disease process) sit in the same causal graph structure without you inventing custom relationship semantics — the two ontologies were designed by their maintainers to interoperate this way.

---

## A3. SWEET (Semantic Web for Earth and Environmental Terminology)

**1. What it is**
A large (~6,000+ concept) Earth-science ontology covering atmosphere, ocean, solid earth, physical processes, human activity, and data representation — the deepest, most granular vocabulary of the three weather ontologies here for actual meteorological variables (temperature, precipitation, humidity, wind) [7][8].

**2. Who created it, and credibility**
Created at **NASA's Jet Propulsion Laboratory** (operated by Caltech under contract to NASA) by **Rob Raskin and Michael Pan**, funded through **NASA's Earth Science Technology Office / AIST Program** [7][8]. Explicit original mission: improve semantic discoverability of NASA's own satellite Earth-science data holdings via the **Global Change Master Directory (GCMD)** catalog. Since 2017, formally transitioned to community governance under the **ESIP (Earth Science Information Partners) Federation** — a nonprofit consortium substantially funded by **NASA, NOAA, and USGS** [9]. Credibility marker: this is a rare case of an ontology surviving the end of its original grant funding cycle by successfully transitioning to independent multi-agency-backed governance, which is itself evidence of sustained institutional value rather than an abandoned research artifact.

**3. Where it is already applied**
Originally deployed in production to power semantic search over NASA's GCMD Earth-science data catalog [7]. ESIP's own 2018 symposium documentation records ongoing, published alignment work between SWEET and the **OBO Foundry ontology collection**, **W3C SOSA/SSN**, and **W3C PROV-O** [9] — meaning the mapping work needed to pair SWEET with the SOSA ontology above has already been done and published by SWEET's own maintainers. It is also cited as a reference ontology in current (2024) GeoAI literature presented at the American Geophysical Union [10], and has been independently extended by academic groups for domains such as hydrogeology [11].

**4. Pros and cons**

| Pros | Cons |
|---|---|
| Deepest, most granular meteorological/atmospheric vocabulary of the three | Not BFO/OBO-Foundry-aligned — bridging to IDO takes more manual mapping than ENVO |
| NASA-originated engineering rigor, now multi-agency (NASA/NOAA/USGS-adjacent) governed | Very large (6,000+ concepts) — import only relevant modules, not the whole thing |
| Already has published alignment to SOSA/SSN and PROV-O | |
| Apache 2.0 licensed — zero licensing friction for an academic NRPU project | |

**5. Relevance to CHIP**
Use SWEET selectively (atmosphere/meteorological-phenomena modules only) where ENVO's environmental-process vocabulary isn't granular enough for the specific weather variables NIH/PMD report — e.g., distinguishing precipitation types or wind categories at the resolution your LSTM/Prophet models will need.

---

# PART B — INFECTIOUS DISEASE ONTOLOGIES

## B1. IDO (Infectious Disease Ontology) Core + Extensions (IDOMAL, IDODEN)

**1. What it is**
The reference ontology for the infectious disease domain within the OBO Foundry, built on **BFO**. IDO Core defines general infectious-disease terms (infectious agent, disease course, transmission); a family of extension ontologies cover specific pathogens [12][13].

**2. Who created it, and credibility — the strongest single credential in this whole brief**
Co-founded by **Lindsay G. Cowell** (then Duke University Medical Center, now Cowell Lab, UT Southwestern Medical Center) and **Barry Smith** (University at Buffalo, Department of Philosophy) [12][14]. Barry Smith is not an ordinary contributor: he is the **co-creator of BFO (Basic Formal Ontology)** — the upper ontology that IDO, ENVO, DOID, and OGMS all comply with — and a **co-founder of the OBO Foundry** itself, the governance framework that defines what counts as a well-formed, interoperable biomedical ontology. Citing IDO is effectively citing the ontology built by the person who wrote the rulebook every other ontology in this brief follows. The project launched with a **$1.25 million grant from NIAID (National Institute of Allergy and Infectious Diseases, part of NIH)**, with pilot funding from the **Burroughs Wellcome Fund**, and has drawn further support from the **Canadian Institutes of Health Research**, the **Public Health Agency of Canada**, and **European Union** funding streams [14][15].

**3. Where it is already applied**
IDO was stress-tested in real time during COVID-19: the same core team built **VIDO (IDO Virus)**, **CIDO (Coronavirus Infectious Disease Ontology)**, and an **IDO-COVID-19 extension** within the outbreak itself, and published the methodology as a peer-reviewed case study on rapid ontology extension for emerging pathogens [13]. Beyond COVID, independent research groups — not the original founders — have built disease-specific extensions on the IDO Core skeleton, which is itself evidence the architecture generalizes across teams:
- **IDOMAL** (malaria) — built by Topalis, Mitraka, Dritsou, Dialynas, and Louis at the **Institute of Molecular Biology and Biotechnology, FORTH (Foundation for Research and Technology-Hellas), Greece**, with additional input from the University of Perugia Medical School [16]. Directly relevant: matches one of CHIP's four target diseases and already models vector-borne transmission structure (mosquito vector + pathogen + human host).
- **IDODEN** (dengue) — directly matches CHIP's second target disease [13].
- IDOFLU (influenza), IDOHIV, IDOBRU (brucellosis), IDOTB (tuberculosis), IDOMEN (meningitis) — further independent extensions on the same core [12][13].

**4. Pros and cons**

| Pros | Cons |
|---|---|
| Strongest institutional credibility of any ontology in this brief (BFO/OBO Foundry co-founder) | No existing extension for cholera or generic acute respiratory infection (2 of your 4 diseases) — original extension work required |
| NIAID/NIH-funded, internationally co-funded (Canada, EU) | Steeper learning curve — BFO's formal categories (disposition, realizable entity, etc.) require some ontology-engineering literacy |
| IDOMAL and IDODEN already cover malaria and dengue out of the box | |
| Proven, published, real-time extension methodology (COVID-19 case study) | |
| Shares BFO with ENVO — direct interoperability with your weather layer | |

**5. Relevance to CHIP**
This is your primary disease-side backbone. IDOMAL and IDODEN give you two of four target diseases immediately usable. For cholera and respiratory infection, you would follow the same reengineering methodology the IDO team published for COVID-19 [13] — this is legitimate, citable original contribution for your NRPU project, not a gap in the plan.

---

## B2. DOID (Human Disease Ontology)

**1. What it is**
A large-scale, OWL-based, hierarchical classification of human disease covering infectious, genetic, cancer, cardiovascular, and mental-health categories, cross-mapped to major clinical vocabularies [17][18].

**2. Who created it, and credibility**
Maintained by **Lynn M. Schriml** and team at the **Institute for Genome Sciences, University of Maryland School of Medicine**, with a formal Scientific Advisory Board including **Ada Hamosh** (co-creator of OMIM, the Online Mendelian Inheritance in Man database) and **Judith Blake** (long-time Mouse Genome Informatics lead) [18]. The Disease Ontology Knowledgebase (DO-KB) built on DOID is formally designated a **Global Core Biodata Resource** [19] — an internationally recognized sustainability/quality certification awarded to a select set of data resources judged critical global scientific infrastructure. This is an *independently verified* credibility marker, not a self-claim by the ontology's own team.

**3. Where it is already applied**
DOID is the disease backbone for the **Alliance of Genome Resources**, a consortium of NIH-funded model-organism genome databases (mouse, zebrafish, fly, worm, yeast) [18]. It is directly cross-referenced into **MeSH, ICD, NCI Thesaurus, SNOMED CT, and OMIM** [17] — meaning if Pakistan's NIH IDSR surveillance reports use any standard clinical coding (common in government epidemiological reporting), DOID gives an existing mapping path. It also underlies the **Mondo Disease Ontology**, the field's emerging unification target, funded by **NIH-NHGRI** [20].

**4. Pros and cons**

| Pros | Cons |
|---|---|
| Independently certified Global Core Biodata Resource | Less mechanistic/causal depth than IDO for infection-specific processes (transmission, host-pathogen interaction) |
| Extensive cross-references to clinical coding systems your NIH data likely already uses | Some conceptual overlap with IDO — needs a clear internal rule for which ontology "owns" a term |
| Very active maintenance, large community, strong tooling (BioPortal, OLS) | |

**5. Relevance to CHIP**
Use DOID as your coding/classification bridge — the layer that ingests and standardizes whatever ICD/MeSH/SNOMED-style codes appear in NIH's raw IDSR reports, and cross-walks them into your knowledge graph, including for cholera and respiratory infection where IDO doesn't yet have a dedicated extension.

---

## B3. OGMS (Ontology for General Medical Science)

**1. What it is**
An upper-level medical ontology defining foundational concepts — disease, disorder, diagnosis, patient — that IDO itself is built upon [21].

**2. Who created it, and credibility**
Also a **Barry Smith / National Center for Ontological Research, University at Buffalo** project, with **Werner Ceusters** and **Richard H. Scheuermann** as co-authors on its foundational publication [21] — same institutional lineage as BFO and IDO above.

**3. Where it is already applied**
Documented directly in DOID's own maintainer FAQ [17]: when OGMS was created, its `Disease` term was explicitly cross-defined against DOID's root `disease` term (DOID:4 ↔ OGMS:0000031). This means the bridge between your disease-classification layer (DOID) and your mechanistic layer (IDO/OGMS) is not something you would need to invent — it is already documented and maintained by the ontologies' own authors.

**4. Pros and cons**

| Pros | Cons |
|---|---|
| Explains the logical rigor underneath IDO's disease/diagnosis distinctions | Not something you populate directly with pathogen data — it's scaffolding, not a working component |
| Already formally cross-linked to DOID by both maintaining teams | If your panel wants three *actively-queried* ontologies rather than one foundational one, swap this slot for practical SNOMED CT usage (see note below) |

**5. Relevance to CHIP**
Mostly useful as the answer to "what is IDO built on, and is that solid?" — shows your team understands the full ontology stack rather than just top-level names. If your supervisors specifically want three components you will *query and populate*, use **SNOMED CT** in this slot instead: it is not OBO/BFO-native and carries real engineering overhead to map into an OWL-based graph, but it is very likely the actual coding scheme already present in Pakistani clinical/EHR-adjacent data, which is a different, more pragmatic kind of feasibility.

---

# PART C — HOW THE TWO SIDES CONNECT

```
ENVO (weather/environment process) ──┐
                                      ├── shared BFO upper ontology ──> causal edges are logically native
IDO (infectious disease mechanism) ──┘

SOSA/SWEET (weather DATA structure) ──> feeds ENVO-typed nodes in your CHKG
DOID/SNOMED (disease DATA coding)   ──> feeds IDO-typed nodes in your CHKG
```

**The one sentence to say out loud in the meeting:** *"We selected ENVO and IDO specifically because both were designed, by their own maintainers, to share the BFO upper ontology — so a causal relationship like 'flooding causally influences a cholera outbreak' is logically native to the knowledge graph's structure, not a custom edge type we invented. This is documented directly in ENVO's own published rationale for BFO alignment."* [4]

---

# PART D — SUMMARY TABLE

| Domain | Ontology | Created by | Institutional backing | Independently verified use |
|---|---|---|---|---|
| Weather | SOSA/SSN | W3C+OGC joint working group (Haller, Janowicz, Cox, Le Phuoc, Lefrançois) | W3C + OGC (dual ratified standard) | Geoscience Australia, U. Bremen, Irstea (weather stations) |
| Weather | ENVO | Buttigieg & Mungall, LBNL BBOP (Gene Ontology's creators) | OBO Foundry, US Dept. of Energy national lab | GSC, ESIP, UN Environment, IOC-UNESCO, Tara Oceans |
| Weather | SWEET | Raskin & Pan, NASA JPL | NASA Earth Science Tech Office → ESIP (NASA/NOAA/USGS) | NASA GCMD data catalog |
| Disease | IDO (+IDOMAL, IDODEN) | Cowell (Duke/UT Southwestern) & Smith (Buffalo, BFO/OBO Foundry co-founder) | NIAID/NIH ($1.25M), Burroughs Wellcome, CIHR, PHAC, EU | COVID-19 real-time extension (VIDO/CIDO); IDOMAL by FORTH Greece |
| Disease | DOID | Schriml et al., U. Maryland Institute for Genome Sciences | Global Core Biodata Resource (independently certified) | Alliance of Genome Resources (NIH); underlies Mondo (NIH-NHGRI) |
| Disease | OGMS | Smith, Ceusters, Scheuermann, U. Buffalo | Same lineage as BFO/IDO | Formally cross-linked to DOID's own `disease` root term |

---

# PART E — REFERENCES

[1] Haller, A., Janowicz, K., Cox, S.J.D., Lefrançois, M., Taylor, K., Le Phuoc, D., et al. (2018). "The modular SSN ontology: A joint W3C and OGC standard specifying the semantics of sensors, observations, sampling, and actuation." *Semantic Web*, 10(1), 9–32. https://doi.org/10.3233/SW-180320

[2] W3C/OGC. "Semantic Sensor Network Ontology" — 2023 Edition. https://www.w3.org/TR/vocab-ssn-2023/ (original 2017 edition: https://www.w3.org/TR/vocab-ssn/)

[3] Janowicz, K., Haller, A., Cox, S.J.D., Le Phuoc, D., Lefrançois, M. (2019). "SOSA: A lightweight ontology for sensors, observations, samples, and actuators." *Journal of Web Semantics*, 56, 1–10. Preprint: https://arxiv.org/pdf/1805.09979

[4] Buttigieg, P.L., Morrison, N., Smith, B., Mungall, C.J., Lewis, S.E. (2013). "The environment ontology: contextualising biological and biomedical entities." *Journal of Biomedical Semantics*, 4:43. https://doi.org/10.1186/2041-1480-4-43

[5] Buttigieg, P.L., Pafilis, E., Lewis, S.E., Schildhauer, M.P., Walls, R.L., Mungall, C.J. (2016). "The environment ontology in 2016: bridging domains with increased scope, semantic density, and interoperation." *Journal of Biomedical Semantics*, 7:57. https://doi.org/10.1186/s13326-016-0097-6

[6] OBO Foundry. "Environment Ontology (ENVO)." http://obofoundry.org/ontology/envo.html

[7] Raskin, R.G., Pan, M.J. (2005). "Knowledge representation in the semantic web for Earth and environmental terminology (SWEET)." *Computers & Geosciences*, 31(9), 1119–1125. https://doi.org/10.1016/j.cageo.2004.12.004

[8] Raskin, R., Pan, M. "Semantic Web for Earth and Environmental Terminology (SWEET)." NASA/JPL, ESTO Conference paper. https://esto.nasa.gov/conferences/estc2003/papers/A7P2(Raskin).pdf

[9] McGibbney, L.J. et al. (2018). "Semantic Web for Earth and Environmental Terminology (SWEET)." ESIP GeoSemantics Symposium. https://esipfed.github.io/stc/symposium/2018/talks/sweet_2018_mcgibbney.pdf

[10] "Artificial Intelligence in Earth Science: A GeoAI Perspective." Greg Leptoukh Lecture, AGU 2024 (commentary citing SWEET as reference ontology).

[11] "Developing a modular hydrogeology ontology by extending the SWEET upper-level ontologies." *ScienceDirect*. https://www.sciencedirect.com/science/article/abs/pii/S009830040800085X

[12] OBO Foundry. "Infectious Disease Ontology (IDO)." http://obofoundry.org/ontology/ido.html

[13] Babcock, S., Beverley, J., Cowell, L.G., Smith, B. (2021). "The Infectious Disease Ontology in the age of COVID-19." *Journal of Biomedical Semantics*, 12:13. https://doi.org/10.1186/s13326-021-00245-1 (PMC: https://www.ncbi.nlm.nih.gov/pmc/articles/PMC8286442/)

[14] University at Buffalo News. (2009). "To Fight Infectious Disease, Medical Research Turns To Philosophy — and Buffalo." https://www.buffalo.edu/news/releases/2009/01/9857.html

[15] "The future of infectious disease research and the problem of data sharing." University at Buffalo, Ontology consortium page. https://ontology.buffalo.edu/medo/IDO_Invitation.htm

[16] Topalis, P., Mitraka, E., Dritsou, V., Dialynas, E., Louis, C. (2013). "IDOMAL: the malaria ontology revisited." *Journal of Biomedical Semantics*, 4:16. https://doi.org/10.1186/2041-1480-4-16

[17] Institute for Genome Sciences. "Disease Ontology — Frequently Asked Questions." https://disease-ontology.org/about/faq/

[18] Schriml, L.M. et al. (2022). "The Human Disease Ontology 2022 update." *Nucleic Acids Research*, 50(D1), D1255–D1261. https://pmc.ncbi.nlm.nih.gov/articles/PMC8728220/

[19] Institute for Genome Sciences. "Disease Ontology — About." https://disease-ontology.org/about/ (Global Core Biodata Resource designation)

[20] Monarch Initiative. "Mondo Disease Ontology." https://mondo.monarchinitiative.org/ (NIH-NHGRI grant # 1 RM1 HG010860-01)

[21] Scheuermann, R.H., Ceusters, W., Smith, B. "Toward an Ontological Treatment of Disease and Diagnosis." *AMIA Summit on Translational Bioinformatics*, 2009, 116–120. Cited via Disease Ontology FAQ [17].

