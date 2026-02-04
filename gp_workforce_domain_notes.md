# GP Workforce (NHS England Digital) — Domain Notes (for Chatbot)

## What this dataset is
The **General Practice Workforce** Official Statistics provide a monthly **snapshot** of staff working in traditional GP practices in England.
A “snapshot statistic” reflects the workforce situation at a specific date (the **last calendar day of each month**).  

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

⚠️ A person may appear in multiple places if:
- they work in multiple practices
- they hold multiple roles
- the dataset cannot perfectly deduplicate due to missing identifiers

Headcount at higher levels (ICB, region, national) is **not always** equal to “sum of practices”.

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

✅ For chatbot logic:
**Non-GP staff = Nurses + DPC + Admin/Non-clinical**  
(or simply staff_group <> 'GP')

---

## Locums (important nuance)
There are different locum concepts:

### Regular locums
Collected in main workforce returns and included as structured workforce records.

### Ad-hoc locums
“Ad-hoc locum” is a data-collection construct for sessional/one-off coverage.
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
- Practice
- Sub-ICB Location (legacy CCG areas retained)
- ICB
- Region
- England

---

## Common KPI interpretations for chatbot
### “National totals”
Use **individual** table (it supports national aggregation better).

### “Top N ICBs / Sub-ICBs”
Use **individual** table.

### “Top practices”
Use **practice_high** or practice-level view.

### “% split GP vs Non-GP”
Compute using **individual** table:
- GP % = GP FTE / Total FTE
- Non-GP % = (Total - GP) / Total

Non-GP includes Nurses + DPC + Admin/Non-clinical.

---

## Reminder: your 3 tables in Athena
Your Athena tables mirror the 3 files you get inside the downloaded zip:
1) individual (record-level rollups for demographics + geography)
2) practice-level (aggregated / practice-high)
3) practice-level detailed (wide format / many columns)

The chatbot should prefer:
✅ individual for national/regional/ICB breakdowns  
✅ practice_high for practice-level ranking  
⚠️ practice_detailed only when needed
