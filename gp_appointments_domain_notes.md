## GP Appointments Dataset — Domain Notes (for Chatbot)

This dataset covers **general practice appointments activity in England**. It is designed for management information about scheduled appointment activity and usage in general practice.

**Official publication name**: Appointments in General Practice
**Published by**: NHS England
**Publication URL**: https://digital.nhs.uk/data-and-information/publications/statistical/appointments-in-general-practice

The dataset records: total appointments, appointment status, appointment mode, Health Care Professional (HCP) type, time between booking and appointment, and national category (consultation type).

Use this dataset for **appointments activity**, not workforce headcount/FTE.

---

## What counts as an appointment in this dataset

### Included:
- Appointments where a **slot was created in the clinical system** (SystmOne, EMIS, Vision etc.)
- Appointments attended in person, by telephone, by video, or at home
- DNA (Did Not Attend) appointments — where a slot was booked but the patient did not attend
- Appointments with any health care professional at the practice (GP, nurse, pharmacist, healthcare assistant, etc.)
- Appointments for which the booked date fell within the reporting month

### NOT included (exclusions):
- **Online consultations that do not generate a slot** (e.g. askmyGP, AccuRx message-only contacts) — these are increasingly common but are NOT counted in this dataset
- **Walk-in contacts** that bypass the appointment booking system
- **Admin-only calls** (repeat prescription requests, results queries with no clinical interaction booked as a slot)
- **Emergency or out-of-hours appointments** (111, OOH services, A&E — separate datasets)
- **Hospital appointments** — this covers primary care GP practices only
- **Community services appointments** — separate dataset
- **Dental appointments** — separate system entirely
- **PCN direct booking appointments** that are not recorded at practice level
- **Appointments at practices not submitting data** — ~1-2% of practices do not submit in any given month

**Key implication**: The dataset undercounts total primary care contacts because it excludes online consultations and other non-slot-generating contacts. Total primary care activity is higher than what this dataset shows.

---

## Data coverage

- **Practice coverage**: Approximately **98.9% of GP practices** submit data in any given month (over 6,400 out of ~6,500 practices)
- **Patient coverage**: Because large practices tend to submit, approximately **96.4% of registered patients** are covered
- **Submission**: Practices submit data through their clinical system. NHS England receives this automatically.
- **Non-submitting practices**: Small number (~1%) do not submit each month. Their appointments are not estimated — they are simply absent from the data.

---

## Authoritative Tables In This Chatbot

Only treat these as source-of-truth tables:

### `practice`
- Practice-level appointment activity.
- Main fields include:
  - `gp_code`
  - `gp_name`
  - `sub_icb_location_name`
  - `hcp_type`
  - `appt_mode`
  - `appt_status`
  - `national_category`
  - `time_between_book_and_appt`
  - `count_of_appointments`
  - `year`
  - `month`
- Use for:
  - national totals
  - practice totals
  - top practices
  - national trends
  - national appointment mode breakdown
  - national DNA rate
  - HCP-type analysis
  - **national_category (consultation type) breakdowns** — this column only exists in `practice`, NOT in `pcn_subicb`

### `pcn_subicb`
- Geography-aware appointments activity at PCN / sub-ICB / ICB / region hierarchy.
- Main fields include:
  - `pcn_code`
  - `pcn_name`
  - `sub_icb_location_code`
  - `sub_icb_location_name`
  - `icb_name`
  - `region_name`
  - `appointment_month`
  - `appt_status`
  - `hcp_type`
  - `appt_mode`
  - `time_between_book_and_appt`
  - `count_of_appointments`
  - `year`
  - `month`
- Use for:
  - region totals
  - ICB totals
  - sub-ICB totals
  - PCN totals
  - geography comparisons
  - geography-specific DNA rate
  - geography-specific appointment mode breakdown
  - geography-specific trends
- **DOES NOT contain `national_category`** — use `practice` table for consultation category analysis

---

## Table routing rules

| Query type | Use table |
|---|---|
| National totals | `practice` |
| Region / ICB / sub-ICB / PCN breakdown | `pcn_subicb` |
| Practice-level detail | `practice` |
| PCN-level DNA / mode / HCP breakdown | `pcn_subicb` |
| Sub-ICB breakdown | `pcn_subicb` |
| Trend over time (national) | `practice` |
| Trend over time (regional) | `pcn_subicb` |
| national_category (consultation type) breakdowns | `practice` ALWAYS |
| national_category broken down by ICB/PCN | `practice` grouped by both `sub_icb_location_name` and `national_category` |

---

## Important Metric Guidance

- `count_of_appointments` is the core measure. It is a count, not a rate.
- A **DNA rate** means:
  - numerator = appointments with `appt_status = 'DNA'`
  - denominator = total appointments in the same scope and time period
  - Formula: `100.0 * SUM(CASE WHEN appt_status='DNA' THEN count_of_appointments ELSE 0 END) / NULLIF(SUM(count_of_appointments), 0)`
- For **mode breakdowns**, group by `appt_mode`.
- For **HCP analysis**, filter or group by `hcp_type`.
- For **trends**, group by `year` and `month`.
- For **consultation category analysis**, group by `national_category`.

---

## Appointment status (appt_status)

### appt_status values

| Value | Meaning | Approximate national % |
|---|---|---|
| `Attended` | Patient attended the appointment | ~88–89% |
| `DNA` | Did Not Attend — patient booked but did not attend | ~6–7% |
| `Unknown` | Status not recorded in clinical system | ~4–5% |

**DNA = Did Not Attend**: The patient made a booking but failed to attend without cancelling. This is a significant metric tracked by NHS England and individual ICBs as it represents wasted clinical capacity.

**DNA rate calculation:**
```sql
SELECT
  ROUND(100.0 * SUM(CASE WHEN appt_status = 'DNA' THEN count_of_appointments ELSE 0 END)
        / NULLIF(SUM(count_of_appointments), 0), 1) AS dna_rate_pct
FROM practice
WHERE year = '2026' AND month = '02'
```

**DNA rate benchmarks:**
- **National average**: approximately 6–7%
- **London tends higher**: London sub-ICBs consistently show higher DNA rates (~8–10%) compared to the national average, likely related to demographic factors, urban mobility, and appointment booking patterns
- **Variation by HCP type**: DNA rates differ between GP appointments (~5–6%) and Other Practice Staff (~7–8%)
- When quoting DNA rates, always specify the geography and time period

---

## HCP type (hcp_type)

### hcp_type values (exact strings in data)
- `GP` — General Practitioner
- `Other Practice staff` — nurses, pharmacists, healthcare assistants, and all other non-GP clinical staff
- `Unknown` — HCP type not recorded in the clinical system

When filtering by GP: `hcp_type = 'GP'`
When filtering by non-GP staff: `hcp_type = 'Other Practice staff'`

### Important note on HCP type classification
The HCP type categories are intentionally broad. "Other Practice staff" is a single category covering all non-GP clinicians. This means the data cannot distinguish between, for example, a nurse appointment and a pharmacist appointment at the national level through this column alone.

### SDS Role Groups (future change to watch)
NHS England is transitioning towards using **SDS (Spine Directory Services) Role Groups** for HCP type classification. This would provide more granular role categorisation (e.g. distinguishing pharmacists from nurses within "Other Practice staff"). At the time of writing, the existing 3-category classification (GP / Other Practice staff / Unknown) is still the standard in this dataset. Any future data comparing SDS Role Group data with legacy hcp_type data may not be directly comparable.

### Approximate share of appointments by HCP type (recent months):
- **GP**: approximately 50–55% of appointments
- **Other Practice staff**: approximately 40–45% of appointments
- **Unknown**: approximately 3–5%

---

## Appointment mode (appt_mode)

### appt_mode values (exact strings in data)

| Mode | Definition | Approx % (Feb 2026) |
|---|---|---|
| `Face-to-Face` | In-person consultation at the practice | ~67% |
| `Telephone` | Telephone consultation | ~25% |
| `Home Visit` | Clinician visits patient at home | ~1% |
| `Video Conference/Online` | Remote consultation via video | ~1% |
| `Unknown` | Mode not recorded | ~6% |

**Important trends:**
- **COVID-19 impact**: Before March 2020, Face-to-Face was ~80%+ of appointments. During COVID-19 (2020-2021), Telephone became dominant (>50%). Since 2022, Face-to-Face has recovered to current levels (~67%) but has NOT returned to pre-pandemic proportions — telephone remains structurally higher than pre-pandemic.
- **Video/Online**: Despite investment in video consultation infrastructure, Video remains a very small proportion (~1%). The category "Video Conference/Online" includes both dedicated video calls and some online booking systems that categorise contacts as "online" — so the true video consultation figure may be slightly different.
- **Home Visit**: Rare and declining as a proportion, reserved for housebound patients.

### Key calculation for mode analysis:
```sql
SELECT
  appt_mode,
  SUM(count_of_appointments) AS appointments,
  ROUND(100.0 * SUM(count_of_appointments) / NULLIF(SUM(SUM(count_of_appointments)) OVER (), 0), 1) AS pct
FROM practice
WHERE year = '2026' AND month = '02'
GROUP BY appt_mode
ORDER BY appointments DESC
```

---

## National category (consultation type)

### What is national_category?
`national_category` is a **standardised classification of consultation type** assigned to each appointment slot by the clinical system. It provides context about what kind of clinical encounter the appointment represents.

**Critical technical note**: `national_category` exists ONLY in the `practice` table, NOT in `pcn_subicb`. Any query involving national_category MUST use the `practice` table and GROUP BY `national_category`. The `practice` table also has `sub_icb_location_name` so you can combine national_category breakdowns with sub-ICB geography.

### All 19 national_category values (exact strings in data)

| national_category value | Description |
|---|---|
| `General Consultation Routine` | Standard, non-urgent consultation scheduled in advance. The largest single category — typically 40–45% of all appointments. |
| `General Consultation Acute` | Urgent or same-day consultation for an acute (sudden/unplanned) clinical need. |
| `Planned Clinical Procedure` | Pre-scheduled clinical procedure (e.g. cervical screening, immunisation, wound care, health check). |
| `Care Related Letter` | Administrative appointment associated with a clinical letter (e.g. discussing a referral letter, reviewing correspondence). |
| `Home Visit` | Clinician visits a patient at their home address. Correlates with `appt_mode = 'Home Visit'` but the category is separately coded. |
| `Care Home Visit` | Visit to a patient residing in a care home or nursing home. |
| `Urgent` | Appointment flagged as clinically urgent (may overlap with acute). |
| `Mental Health` | Mental health consultation or review (depression, anxiety, ADHD, etc.). |
| `Group Consultation and Group Education` | Group appointment where clinician sees multiple patients simultaneously (e.g. diabetes group education, postnatal group). |
| `Structured Medication Review` | Formal review of a patient's complete medication list, typically by a clinical pharmacist. Part of the PCSE Structured Medication Review programme. |
| `Flu Vaccination` | Flu vaccination appointment only. |
| `COVID Vaccination` | COVID-19 vaccination appointment (became relevant from late 2020). |
| `Contraception` | Contraception-related consultation (prescription, review, coil fitting etc.). |
| `Ante Natal` | Antenatal care appointments (pregnancy monitoring). |
| `Post Natal` | Post-natal appointments (newborn checks, 6-week maternal review). |
| `Walking Aid/Dressing/Other Admin` | Administrative appointments related to physical aids, dressings, or non-clinical admin tasks. |
| `Inconsistent Mapping` | The clinical system submitted a mapping that does not correspond to a valid national_category. See detailed explanation below. |
| `Unmapped` | The clinical system did not map the appointment to any national category — the local slot/session type was not mapped to a national category in the national mapping table. |
| `Unknown` | Category not recorded or not determinable. |

**Volume guidance:**
- `General Consultation Routine` is by far the largest category
- `General Consultation Acute` is typically second
- `Unmapped` and `Inconsistent Mapping` can represent a significant proportion (~5–15%) in some periods/practices

### Inconsistent Mapping — explanation
`Inconsistent Mapping` does NOT mean the appointment itself was problematic. It means the **mapping** from the practice's local slot/session type label to the national category was internally inconsistent. For example:
- The practice maps "Dr Smith Routine" → "General Consultation Routine", but the same local label has been mapped to "General Consultation Acute" in a different record
- NHS England's mapping table detected the inconsistency and flagged the record rather than assigning it to an incorrect category

When a user asks about `Inconsistent Mapping`, explain: "This represents appointments where the clinical system's local appointment type label was mapped to the national category in an inconsistent way. These are real appointments, but the consultation type could not be definitively classified. They are included in total appointment counts."

### Unmapped — explanation
`Unmapped` means the practice has a local appointment type label (e.g. "Dr Jones Morning Surgery") that **has not been mapped** to any national_category at all. The appointment is real and counted in totals, but has no consultation type classification. Some practices have higher unmapped rates than others depending on how well their clinical system has been configured.

---

## Time between booking and appointment (time_between_book_and_appt)

### What it measures
This is the **booking lead time** — the gap between the date the appointment was booked and the date the appointment was actually due to take place. It is NOT a "waiting time" in the traditional sense — it does not measure time from when the patient wanted an appointment to when they got one.

### Exact bucket values (strings in data)

| Value | Meaning | Approx % nationally |
|---|---|---|
| `Same Day` | Booked and appointment on same calendar day | ~44% |
| `1 Day` | 1 day between booking and appointment | ~11% |
| `2 to 7 Days` | 2–7 days lead time | ~19% |
| `8 to 14 Days` | 8–14 days lead time | ~9% |
| `15 to 21 Days` | 15–21 days lead time | ~5% |
| `22 to 28 Days` | 22–28 days lead time | ~3% |
| `More than 28 Days` | Over 4 weeks lead time | ~4% |
| `Unknown / Data Quality` | Lead time not calculable or data quality issue | ~5% |

**Key insight**: The most common lead time is **Same Day** (~44%), meaning nearly half of all appointments are booked on the same day they take place. This reflects the acute/urgent nature of much GP demand.

**For "within 2 weeks" queries** use:
```sql
time_between_book_and_appt IN ('Same Day', '1 Day', '2 to 7 Days', '8 to 14 Days')
```

**For "booked more than 28 days ahead"** use:
```sql
time_between_book_and_appt = 'More than 28 Days'
```

**Important caveat**: "Same Day" does not necessarily mean the patient was seen urgently or without planning. Many practices pre-book their own "on-the-day" appointment slots which are only made available at 8am — these appear as "Same Day" bookings but the patient may have rung at 8am for a 9am appointment. It does NOT exclusively measure walk-in or truly urgent access.

---

## Geography Guidance

Answer appointments questions at these scopes:
- national (use `practice` table)
- region (use `pcn_subicb`)
- ICB (use `pcn_subicb`)
- sub-ICB (use `pcn_subicb`)
- PCN (use `pcn_subicb`)
- practice (use `practice`)

Main geography fields:
- `region_name` — in `pcn_subicb` only
- `icb_name` — in `pcn_subicb` only
- `sub_icb_location_name` — in BOTH `practice` and `pcn_subicb`
- `pcn_name` — in `pcn_subicb` only
- `gp_name`, `gp_code` — in `practice` only

If a user asks for a city such as Leeds, you may need to map it to the relevant ICB while also checking sub-ICB names.

### Key geography mapping examples
- **London** → NHS North East London ICB, NHS South East London ICB, NHS South West London ICB, NHS North West London ICB, NHS North Central London ICB (5 London ICBs)
- **Greater Manchester** → NHS Greater Manchester ICB
- **Birmingham** → NHS Birmingham and Solihull ICB
- For city-level queries, prefer sub_icb_location_name search or icb_name search

---

## Seasonal patterns in appointments data

Monthly appointment volumes vary seasonally:
- **Winter peak (December–February)**: Higher volumes due to respiratory illness, flu season
- **Summer trough (July–August)**: Lower volumes; practices may have reduced capacity, some patients on holiday
- **August dip for GPs**: New GP trainee rotations begin in August — some practices temporarily have reduced substantive GP capacity
- **Bank holidays**: Months containing bank holidays (April, May, August, December) typically show slightly lower volumes

NHS England advises **year-on-year comparisons** (e.g., January 2026 vs January 2025) rather than month-on-month for trend analysis.

---

## COVID-19 impact on the dataset

The COVID-19 pandemic (March 2020 onwards) caused significant structural changes:
1. **Appointment mode shift**: Face-to-Face dropped sharply as telephone and video consultations were mandated. The mode distribution has partially but not fully recovered.
2. **Volume changes**: April–May 2020 saw significant drops in appointment volumes; subsequent months showed recovery and catch-up activity
3. **Category changes**: COVID Vaccination category was added to national_category
4. **Comparability caveat**: Comparisons across the 2020 pandemic boundary require caution — the nature of appointments changed fundamentally.

---

## Official Terminology Guidance

### DNA
`DNA` means **Did Not Attend**. The patient booked a slot but did not turn up and did not cancel in advance. This wastes clinical capacity and is tracked as a quality/efficiency metric.

### Appointment mode
See full table in the appt_mode section above.

### HCP type
`hcp_type` identifies the type of healthcare professional associated with the appointment.

### National category / Consultation category
A standardised 19-value classification of consultation type. Not all practices map to all categories. See national_category section above.

### Time from booking to appointment
`time_between_book_and_appt` is a booking lead-time band (7 buckets from Same Day to 28+ Days), not an exact wait-time in days. Does not capture demand-side waiting — only the actual lead time between booking date and appointment date.

---

## Appointment Column Reference (Quick Lookup)

### hcp_type values (exact strings in data)
- `GP` — General Practitioner
- `Other Practice staff` — nurses, pharmacists, and other non-GP clinical staff
- `Unknown` — HCP type not recorded

### appt_mode values (exact strings in data)
- `Face-to-Face` — in-person consultation
- `Telephone` — telephone consultation
- `Home Visit` — appointment at patient home
- `Video Conference/Online` — remote video or online consultation
- `Unknown` — mode not recorded

### time_between_book_and_appt values (exact strings in data)
- `Same Day` — booked and seen on same day (~44%)
- `1 Day` — next day appointment (~11%)
- `2 to 7 Days` — within one week (~19%)
- `8 to 14 Days` — 8 to 14 days ahead (~9%)
- `15 to 21 Days` — 15 to 21 days (~5%)
- `22 to 28 Days` — 22 to 28 days (~3%)
- `More than 28 Days` — over 4 weeks (~4%)
- `Unknown / Data Quality` — not recorded or data quality issue (~5%)

For "within 2 weeks" calculations use: `IN ('Same Day', '1 Day', '2 to 7 Days', '8 to 14 Days')`

### appt_status values (exact strings in data)
- `Attended` — patient attended (~88–89%)
- `DNA` — did not attend (~6–7%)
- `Unknown` — status not recorded (~4–5%)

### national_category values (all 19 — exact strings in data)
1. `General Consultation Routine`
2. `General Consultation Acute`
3. `Planned Clinical Procedure`
4. `Care Related Letter`
5. `Home Visit`
6. `Care Home Visit`
7. `Urgent`
8. `Mental Health`
9. `Group Consultation and Group Education`
10. `Structured Medication Review`
11. `Flu Vaccination`
12. `COVID Vaccination`
13. `Contraception`
14. `Ante Natal`
15. `Post Natal`
16. `Walking Aid/Dressing/Other Admin`
17. `Inconsistent Mapping`
18. `Unmapped`
19. `Unknown`

---

## Cross-dimension Queries

When a question asks for mode × HCP type (e.g. "face-to-face appointments by GPs"):
```sql
WHERE appt_mode = 'Face-to-Face' AND hcp_type = 'GP'
```

When a question asks for DNA rate by HCP type:
```sql
SELECT hcp_type,
  ROUND(100.0 * SUM(CASE WHEN appt_status = 'DNA' THEN count_of_appointments ELSE 0 END)
        / NULLIF(SUM(count_of_appointments), 0), 1) AS dna_rate_pct
FROM pcn_subicb
WHERE year = '2026' AND month = '02'
GROUP BY hcp_type
```

When a question asks for national_category breakdown by ICB (note: use practice table, group by both):
```sql
SELECT sub_icb_location_name, national_category, SUM(count_of_appointments) AS appointments
FROM practice
WHERE year = '2026' AND month = '02'
GROUP BY sub_icb_location_name, national_category
ORDER BY sub_icb_location_name, appointments DESC
```

---

## What This Dataset Is Good For

- "How many appointments were there nationally in the latest month?"
- "What is the DNA rate in NHS Greater Manchester ICB?"
- "Show appointment mode breakdown nationally."
- "Which practices had the most appointments?"
- "Show GP appointments trend over the past year."
- "What percentage of appointments were face-to-face?"
- "How many appointments were booked same day?"
- "Show breakdown of consultation categories nationally."
- "What is the DNA rate for GP appointments vs nurse appointments?"

## What This Dataset Is Not For

- GP workforce headcount or FTE
- Salary / earnings
- Clinical outcomes
- Prescribing
- Patient satisfaction
- Real-time operational status
- Online consultation contacts that don't generate appointment slots
- Out-of-hours or 111 activity
- Hospital appointment waiting times

---

## Answering Rules

- Never answer appointments questions from workforce tables.
- Never answer workforce questions from appointments tables.
- Use `practice` for national and practice-level appointment activity unless the user clearly wants region / ICB / sub-ICB geography.
- Use `pcn_subicb` for region / ICB / sub-ICB / PCN geography.
- **Always use `practice` for national_category (consultation type) queries** — this column does not exist in pcn_subicb.
- Keep geography consistent through follow-up questions.
- Do not call a total an average unless the SQL explicitly calculates an average.
- If a geography name is ambiguous, prefer clarification over guessing.
- When quoting DNA rates or mode percentages, always state the time period.
- When explaining "Unmapped" or "Inconsistent Mapping" categories, clarify these are real appointments with classification issues, not missing or erroneous records.

---

## Data latency
The Appointments publication is released approximately **4–5 weeks after the reporting month ends**. For example:
- January 2026 data → published approximately late February / early March 2026

The data in this chatbot is updated after each NHS England publication release. "Current" data means the latest available published month, not real-time live appointment counts.
