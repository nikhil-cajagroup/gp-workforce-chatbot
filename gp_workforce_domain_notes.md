# GP Workforce (NHS England Digital) — Domain Notes (for Chatbot)

## What this publication is
The **General Practice Workforce** is an Official Statistics series published by NHS England Digital. It provides a monthly **snapshot** of staff working in general practices in England. The publication URL is: https://digital.nhs.uk/data-and-information/publications/statistical/general-and-personal-medical-services

A "snapshot statistic" reflects the workforce situation at a specific date — the **last calendar day of each month** (including weekends and public holidays).

Key reported measures:
- **FTE (Full-Time Equivalent)**
- **Headcount (distinct individuals)**

The publication is released approximately 3-4 weeks after the snapshot date.

---

## Publication frequency history
- **Pre-September 2015**: Annual publication (30 September census date), using older data sources
- **September 2015 - June 2017**: Six-monthly releases for full workforce, with quarterly GP-only releases
- **September 2017 - June 2021**: Quarterly releases for entire workforce
- **July 2021 onwards**: Monthly releases (current frequency)

---

## FTE definition (important)
For NHS workforce stats, **full-time = 37.5 hours/week**.

FTE is a standardised measure of workload:
- 37.5 hours/week = **1.0 FTE**
- 30 hours/week = **0.8 FTE**
- 18.75 hours/week = **0.5 FTE**

FTE allows comparisons across part-time and full-time working patterns.

---

## Headcount definition (important)
Headcount = number of **distinct individuals**.

A person may appear in multiple places if:
- they work in multiple practices (counted once at practice level in each practice, but deduplicated at England level)
- they hold multiple roles (may appear in role-specific headcount for each role, but counted once in overall GP headcount)
- the dataset cannot perfectly deduplicate due to missing identifiers

**Critical**: Headcount at higher levels (ICB, region, national) is **NOT** equal to the sum of practice-level headcounts. Summing practice headcounts would double-count staff who work across multiple practices. Use the **individual** table for above-practice headcount calculations.

---

## Staff groups (Core structure)
This publication groups staff into four high-level staff groups:

### 1) GP staff
General Practitioners: includes GP Providers/Partners, Salaried/Other GPs, GPs in training (registrars), retainers, and locums.

### 2) Nurses
General practice nurses, nurse practitioners, enhanced practice nurses, community mental health nurses, and related nursing roles.

### 3) Direct Patient Care (DPC)
DPC includes staff directly delivering patient care but who are not Nurses or GPs.
Examples: dispensers, healthcare assistants, phlebotomists, pharmacists, pharmacy technicians, physiotherapists, podiatrists, social prescribing link workers, therapist-counsellors, physician associates, paramedics, occupational therapists, dietitians, social workers, and other DPC roles.

### 4) Admin / Non-clinical
Administrative and non-clinical support roles (managers, receptionists, secretaries, etc.).

For chatbot logic:
**Non-GP staff = Nurses + DPC + Admin/Non-clinical**
(or simply staff_group <> 'GP')

---

## Scope — what IS included
This publication covers staff in **traditional general practices** in England that are registered with the Organisation Data Service and provide NHS primary medical services. A general practice is defined as an organisation which offers primary care medical services by a qualified General Practitioner where patients can be registered and held on a list.

## Scope — what is NOT included (exclusions)
The following are **explicitly excluded** from this publication:
- **Prisons** and custodial settings
- **Army bases** and military medical facilities
- **Walk-in centres** and urgent treatment centres
- **Minor injury units**
- **Educational establishments**
- **Specialist care centres**
- **Drug rehabilitation units**
- **Hospital and community health services** (GPs working in hospitals are not covered)
- **Dental practices**
- **GP appointments data** — this is a completely separate publication ("Appointments in General Practice")
- **Prescribing data** — covered by separate NHSBSA publications
- **Patient satisfaction scores** — covered by the GP Patient Survey (separate)
- **Real-time workforce data** — this is a retrospective snapshot, not live data
- **Patient-level clinical records** — no diagnosis, treatment, or patient-identifiable information

---

## Primary Care Networks (PCNs) vs General Practices
The **PCN-employed workforce** is separate and distinct from the General Practice workforce. They are published as different statistical series:
- General Practice Workforce → this publication
- Primary Care Network Workforce → separate publication (staff employed directly by PCNs)

Although PCNs and GP practices share several common roles (primarily in the DPC staff group), and some individuals work in both types of organisation, the totals are **not combined**. PCN workforce data has been collected separately since March 2020.

**IMPORTANT**: The practice_detailed table includes a `pcn_name` column, because each GP practice belongs to a PCN. This means you CAN aggregate GP practice workforce data BY PCN (e.g. "total GPs per PCN" or "PCNs with most practices"). This is NOT the same as the PCN-employed workforce — it is the GP practice workforce grouped by the PCN their practice belongs to.

---

## Data sources

### NWRS (National Workforce Reporting Service)
NWRS is an **online data collection tool** used by general practices and PCNs in England to report individual record-level workforce data. Practices access it via the Strategic Data Collection Service (SDCS). It collects data on each individual staff member including: demographics, joining/leaving dates, role titles, staff group, working hours, reason for leaving, destination on leaving, and recruitment source. NWRS does NOT collect data on GPs in training.

### wMDS (workforce Minimum Data Set)
The **workforce Minimum Data Set for Primary Care** is the formal data collection specification introduced in **September 2015**. It defines what data items must be submitted via NWRS. The wMDS replaced the older, less detailed data collection methods. All GP practices providing NHS services must submit wMDS returns.

### TIS (Trainee Information System)
GP trainee (registrar) data comes from Health Education England's **Trainee Information System (TIS)**, which has been the primary source for training data since **June 2018**.

### Other reference data sources
- **General Medical Council (GMC) Register** — used for demographic data on GPs
- **Organisation Data Service (ODS)** — practice reference data (codes, names, addresses, organisational relationships)

### Pre-2015 data sources (historical, NOT comparable)
- **NHAIS (Exeter) system / General Practice Payments System** — used for GP data before September 2015
- **CCG aggregate submissions** — used for non-GP staff data (2010-2015)
- **Primary Care Trust (PCT) aggregations** — used before 2010

**Important**: Figures from before September 2015 are derived from completely different data sources and are **NOT comparable** with any figures in the current series.

---

## Locums (important nuance)

### Regular locums
Collected in main workforce returns via NWRS and included as structured workforce records in the main snapshot. They appear in the main FTE and headcount totals.

### Ad-hoc locums
"Ad-hoc locum" GPs are those working in a short-term and/or short-notice capacity. They are collected **separately** (reported in Annexes B and C of the Excel bulletin). They cover the **period since the previous snapshot**, not a point-in-time snapshot.

**Key caveat**: Ad-hoc locum FTE and headcount figures are **NOT directly comparable** with the main workforce snapshot because:
1. They measure a different thing (period activity vs point-in-time snapshot)
2. Headcount estimation for ad-hoc locums is unreliable
3. When the publication moved from quarterly to monthly collection in July 2021, ad-hoc locum figures were removed from main tables because monthly headcount figures would not be comparable with previous quarterly collections

### Locum comparability around 2017
The methodology for collecting and presenting locum data changed during 2017. Users should exercise caution when comparing locum figures around this period.

---

## Estimations & partial estimates

### What are "partial estimates"?
When a practice provides records for identifiable staff members but **does not provide hours information**, NHS England calculates estimated FTE values. These are called "partial estimates" and are included in the published totals. The percentage of estimated working hours for each staff group is reported in Annex A of the Excel tables.

### Full estimates (missing practice data)
When a practice provides **no valid data at all** for an entire staff group (GPs, Nurses, DPC, or Admin), Sub-ICB Location-level estimates are calculated based on perceived need according to the number of patients registered at the practice.

### Practice-level CSVs
- Aggregated at practice level
- Do **NOT** include fully estimated records
- Because they are aggregated and exclude estimates, they **cannot** reliably calculate higher-level headcount
- Suitable for practice-level analysis and rankings only

### Individual-level CSV
- **Includes** both collected and estimated data (data_source = 'Collected' or 'Estimated')
- Can be used to calculate headcount + FTE above practice-level (Sub-ICB, ICB, region, national)
- Suitable for demographic cuts (age, gender, country of qualification)

---

## Zero-hours contracts
Staff with **zero contracted hours but recorded working hours** during the reporting period are counted in both FTE and headcount. Staff with **neither contracted nor recorded hours** are excluded from both FTE and headcount measures.

---

## Joiners and leavers data
The publication provides supplementary **GP joiners and leavers data**, including:
- Number of joiners and leavers by time period
- Breakdowns by NHS England region and ICB
- **Reason for leaving** (collected via NWRS leaving reason field)
- **Destination on leaving** (where the GP went after leaving)
- Mean age of leavers by staff group
- Percentage distribution of leaving reasons

This data is available from **July 2021 onwards** (when monthly collection began). It is published as supplementary files alongside the main release.

NWRS collects the following leaving-related fields: leaving date, reason for leaving, destination on leaving, and recruitment source.

---

## Seasonality guidance
Month-to-month comparisons can be **misleading** due to seasonal patterns in the workforce (e.g., GP trainee rotations in August/September, seasonal staffing patterns). NHS England recommends **year-over-year comparisons** (e.g., December 2024 vs December 2023) for meaningful trend analysis.

GP trainee numbers in particular show strong seasonal variation due to training rotation cycles (intake periods). Comparing trainee numbers month-to-month can be very misleading.

---

## Time series comparability (critical)

### Comparable time series begins: September 2015
The current comparable time series starts from **September 2015** when the wMDS was introduced.

### Cannot compare pre-2015 with current figures
Figures before September 2015 used entirely different data sources (NHAIS/Exeter for GPs, CCG aggregates for other staff) and are **NOT comparable** with the current wMDS-based series. Any such comparison would be methodologically invalid.

### September 2015 caveat
The very first September 2015 collection covered only three of four HEE regions and is therefore **less complete** than subsequent periods. Until January 2019, four HEE regions collected data directly.

### Major revisions in 2021
- **August 2021**: Entire time series was revised — fully-estimated records were removed
- **December 2021**: Time series revised again — estimates were reinstated using improved methodology. Figures in the December 2021 release **supersede** all previously published figures

### Quarterly to monthly transition (July 2021)
The move from quarterly to monthly publication in July 2021 is a potential comparability point. Ad-hoc locum figures were removed from main tables at this transition.

---

## Files available in each monthly release
Each monthly publication typically includes:
1. **Excel Bulletin** (main tables) — FTE and headcount by gender, role, age band, work commitment at England level, plus ICB and Sub-ICB breakdowns by role and ethnicity
2. **Individual-level CSV** — record-level data at Sub-ICB Location level with demographics
3. **Practice-level CSVs** — aggregate figures per practice (two formats: high-level tidy and detailed wide)
4. **GP Joiners and Leavers** supplementary files
5. **Interactive Power BI dashboard** (refreshed with most publications)
6. **Background and data quality statement**
7. **Partner/salaried GP role tracking** supplementary data
8. **Ethnicity by job role** data

---

## Geographic hierarchy used
- Practice (prac_code / prac_name)
- Primary Care Network (pcn_code / pcn_name) — only in practice_detailed
- Sub-ICB Location (sub_icb_code / sub_icb_name)
- ICB (icb_code / icb_name)
- Region (region_code / region_name OR comm_region_code / comm_region_name)
- England (national)

A **Sub-ICB Location** (formerly CCG/Clinical Commissioning Group) is a geographic subdivision within an Integrated Care Board responsible for commissioning primary care services in a specific area.

---

## Table selection guide

### individual table
- **22 columns**, record-level rollups
- Columns: comm_region_code, comm_region_name, icb_code, icb_name, sub_icb_code, sub_icb_name, data_source, unique_identifier, staff_group, detailed_staff_role, staff_role, country_qualification_area, country_qualification_group, age_band, age_years, gender, fte, snapshot_year, snapshot_month, snapshot_date, year, month
- USE FOR: national/regional/ICB/Sub-ICB aggregations, demographics (age, gender, country of qualification), staff role breakdowns, FTE totals using SUM(fte)
- staff_group values: 'GP', 'Nurses', 'Direct Patient Care', 'Admin/Non-Clinical'
- NOTE: for headcount, COUNT(DISTINCT unique_identifier). For FTE, SUM(fte).
- NOTE: data_source can be 'Collected' or 'Estimated'. Include both for full picture.

### practice_high table
- **8 columns**, tidy format (one row per practice+role+measure)
- Columns: prac_code, prac_name, staff_group, detailed_staff_role, measure, value, year, month
- USE FOR: practice-level rankings, comparing practices, searching by practice name
- measure column values: 'FTE' or 'Headcount'
- To get FTE: WHERE measure = 'FTE'
- To get headcount: WHERE measure = 'Headcount'
- Value is in the 'value' column (numeric)

### practice_detailed table
- **830+ columns**, wide format (one row per practice per month)
- Contains geography hierarchy (prac -> PCN -> Sub-ICB -> ICB -> Region)
- Contains patient demographics (registered patients by age/gender)
- Contains detailed GP/Nurse/DPC/Admin breakdowns by gender, age band, country of qualification
- USE FOR: practice-level detail, patient list sizes, practice-to-ICB lookups, GP sub-type breakdowns (partners, salaried, locums, trainees)
- Key summary columns: total_gp_hc, total_gp_fte, total_nurses_hc, total_nurses_fte, total_dpc_hc, total_dpc_fte, total_admin_hc, total_admin_fte, total_patients
- Abbreviation guide:
  - hc = headcount, fte = full-time equivalent
  - extg = excluding trainees, exl = excluding locums, extgl = excluding trainees and locums
  - sen_ptnr = senior partner, ptnr_prov = partner/provider
  - sal_by_prac = salaried by practice, sal_by_oth = salaried by other
  - trn_gr = trainee/registrar, st1-st4 = specialty training year 1-4
  - ret = retainer, locum_vac = locum vacancy, locum_abs = locum absence
  - n_ = nurse prefix, dpc_ = direct patient care prefix, admin_ = admin prefix
  - hca = healthcare assistant, phleb = phlebotomist, pharma = pharmacist
  - pharmt = pharmacy technician, physio = physiotherapist, podia = podiatrist
  - splw = social prescribing link worker, thera_cou = therapist-counsellor
  - coq = country of qualification (uk, eea, africa, asia_south, etc.)

---

## Common KPI interpretations for chatbot

### "National totals"
Use **individual** table: SELECT staff_group, SUM(fte) FROM individual WHERE year=... AND month=... GROUP BY staff_group

### "Top N ICBs / Sub-ICBs"
Use **individual** table: GROUP BY icb_name ORDER BY SUM(fte) DESC LIMIT N

### "Top practices by GP FTE"
Use **practice_high**: WHERE staff_group = 'GP' AND measure = 'FTE' ORDER BY CAST(value AS DOUBLE) DESC LIMIT N

### "Practice lookup" (e.g. Keele Practice)
Use **practice_detailed**: WHERE LOWER(prac_name) LIKE '%keele%'

### "% split GP vs Non-GP"
Compute using **individual** table:
- GP % = SUM(CASE WHEN staff_group='GP' THEN fte ELSE 0 END) / SUM(fte) * 100
- Non-GP is everything else

### "Trend over time"
Use any table with multiple year/month values. GROUP BY year, month ORDER BY year, month.
For individual: SUM(fte) grouped by year, month.
For practice_high: SUM(CAST(value AS DOUBLE)) grouped by year, month.

### "Demographics breakdown"
Use **individual** table: has gender, age_band, country_qualification_group columns.

### "Patients per GP ratio"
Use **practice_detailed**: total_patients / NULLIF(total_gp_fte, 0)

### "How many staff at a practice"
Use **practice_detailed** for totals or **practice_high** for role breakdowns.

---

## Time period handling
- year and month columns are strings (e.g. year='2024', month='08')
- month is zero-padded (e.g. '01', '02', ..., '12')
- "latest month" = MAX year+month in the table
- "last 12 months" = filter by year/month >= (latest minus 12 months)
- "year over year" = compare same month in consecutive years

---

## What this chatbot CAN and CANNOT do
### CAN do (using database queries):
- Show FTE / headcount figures for any staff group, geography, time period in the database
- Compare practices, ICBs, regions by workforce metrics
- Show trends over time, demographic breakdowns, top-N rankings
- Look up specific practices or geographic areas
- Calculate ratios (e.g. patients per GP)

### CANNOT do (out of scope for this dataset):
- GP appointment volumes or patient wait times (separate "Appointments in General Practice" publication at digital.nhs.uk)
- Prescribing data (separate NHSBSA publication)
- Patient satisfaction scores (separate GP Patient Survey)
- Hospital or community health service workforce (separate NHS Workforce Statistics)
- Real-time or live workforce numbers
- Patient-level clinical data (diagnoses, treatments)
- Individual staff member identification
- PCN-employed workforce figures (staff employed directly by PCNs — this is a separate "Primary Care Network Workforce" publication). However, **GP practice staff CAN be grouped by PCN** using the pcn_name column in practice_detailed, since each practice belongs to a PCN. This shows the GP practice workforce within each PCN's footprint, not the PCN's own directly-employed staff.
- Financial data (practice income, GP salaries, NHS spending)
- Individual joiners/leavers tracking — cannot track whether a specific trainee converted to a qualified GP role. The dataset provides snapshot counts, not individual-level career transitions.
- Trainee "turnover" or "conversion rates" — while we can show trainee numbers over time and qualified GP numbers over time, the dataset does not link individual trainees to their eventual qualified GP roles.

## Common analytical concepts

### Retirement eligibility
The NHS pension scheme has different retirement ages depending on the scheme:
- 1995 Section: Normal pension age = 60
- 2008 Section: Normal pension age = 65
- 2015 Scheme: Normal pension age = State Pension Age (currently 66, rising to 67 by 2028)
For analytical purposes, GPs aged 55+ are commonly considered in the "retirement risk" window.
To calculate retirement-eligible proportions, use qualified GPs only (exclude trainees and locums)
and filter by age_band IN ('55-59', '60-64', '65-69', '70+').

### FTE to headcount ratio
- FTE / Headcount ratio indicates what proportion of staff work full-time equivalent hours
- A ratio of 1.0 means every staff member works full-time
- A ratio of 0.6 means on average staff work 60% of full-time hours
- Lower ratios may indicate more part-time working, which can affect service continuity

### Practice sustainability indicators
While "sustainability" is not a formal metric in the dataset, useful proxies include:
- GP FTE / GP Headcount ratio (higher = more full-time commitment)
- Patients per GP FTE (lower = less pressure)
- Proportion of locums (higher = more reliance on temporary staff)
- Proportion of GPs aged 55+ (higher = more retirement risk)
- Trainee pipeline size relative to qualified GP count

### Trainee pipeline
GP training typically follows: F1 → F2 → ST1 → ST2 → ST3 (→ ST4 for some), total ~8 years post-medical school.
- ST3 trainees are ~1 year from completion
- ST2 trainees are ~2 years from completion
- ST1 trainees are ~3 years from completion
- F1/F2 are foundation doctors, 4-5 years from GP qualification
All current trainees represent the future qualified GP pipeline, but the dataset cannot track whether
specific individuals complete training or leave the profession.

### Relationship between GPs and patient wait times
The GP Workforce dataset does NOT include appointment or wait time data. However, the "Appointments
in General Practice" dataset (also published by NHS England) contains appointment volumes, wait times,
and booking methods. The patients-per-GP ratio from the workforce data can serve as a proxy
for workload pressure but does not directly measure appointment availability or wait times.
