# GP Workforce (NHS England Digital) — Domain Notes (for Chatbot)

## What this dataset is
The **General Practice Workforce** Official Statistics provide a monthly **snapshot** of staff working in traditional GP practices in England.
A "snapshot statistic" reflects the workforce situation at a specific date (the **last calendar day of each month**).

Key reported measures:
- **FTE (Full-Time Equivalent)**
- **Headcount (distinct individuals)**

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
- they work in multiple practices
- they hold multiple roles
- the dataset cannot perfectly deduplicate due to missing identifiers

Headcount at higher levels (ICB, region, national) is **not always** equal to "sum of practices".

---

## Staff groups (Core structure)
This publication groups staff into four high-level staff groups:

### 1) GP staff
General Practitioners: includes GP Providers/Partners, Salaried/Other GPs, GPs in training, retainers, and locums.

### 2) Nurses
General practice nurses, nurse practitioners, and related nursing roles.

### 3) Direct Patient Care (DPC)
DPC includes staff directly delivering patient care but who are not Nurses or GPs.
Examples: dispensers, healthcare assistants, phlebotomists, pharmacists, physiotherapists, podiatrists, therapists, and other DPC roles.

### 4) Admin / Non-clinical
Administrative and non-clinical support roles.

For chatbot logic:
**Non-GP staff = Nurses + DPC + Admin/Non-clinical**
(or simply staff_group <> 'GP')

---

## Locums (important nuance)
There are different locum concepts:

### Regular locums
Collected in main workforce returns and included as structured workforce records.

### Ad-hoc locums
"Ad-hoc locum" is a data-collection construct for sessional/one-off coverage.
It is complex to estimate headcount reliably for ad-hoc locums.

---

## Estimations & limitations
### Practice-level CSVs
- Aggregated at practice level
- Do NOT include fully estimated records
- Because it is aggregated, it cannot reliably calculate higher-level headcount

### Individual-level CSV
- Includes estimates
- Can be used to calculate headcount + FTE above practice-level
  (Sub-ICB, ICB, region, national)
- Suitable for demographic cuts (age, gender etc.)

---

## Seasonality guidance
Month-to-month comparisons can be misleading due to seasonal patterns.
Better comparisons: **year-over-year** (e.g., Sep vs Sep).

---

## Geographic hierarchy used
- Practice (prac_code / prac_name)
- Primary Care Network (pcn_code / pcn_name) — only in practice_detailed
- Sub-ICB Location (sub_icb_code / sub_icb_name)
- ICB (icb_code / icb_name)
- Region (region_code / region_name OR comm_region_code / comm_region_name)
- England (national)

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
