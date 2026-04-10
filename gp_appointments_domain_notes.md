## GP Appointments Dataset

This dataset covers **general practice appointments activity in England**. It is designed for management information about scheduled appointment activity and usage in general practice.

The official NHS England / NHS Digital GP Appointments Data service describes the dashboard and collection as covering:
- total appointments
- appointment status
- appointment mode
- Health Care Professional (HCP) type
- time between booking and appointment
- national category

Use this dataset for **appointments activity**, not workforce headcount/FTE.

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

### `pcn_subicb`
- Geography-aware appointments activity at PCN / sub-ICB / ICB / region hierarchy.
- Main fields include:
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
  - geography comparisons
  - geography-specific DNA rate
  - geography-specific appointment mode breakdown
  - geography-specific trends

## Important Metric Guidance

- `count_of_appointments` is the core measure. It is a count, not a rate.
- A **DNA rate** means:
  - numerator = appointments with `appt_status = 'DNA'`
  - denominator = total appointments in the same scope and time period
- For **mode breakdowns**, group by `appt_mode`.
- For **HCP analysis**, filter or group by `hcp_type`.
- For **trends**, group by `year` and `month`.

## Official Terminology Guidance

### DNA
`DNA` means **Did Not Attend**.

### Appointment mode
Common modes include:
- `Face-to-Face`
- `Telephone`
- `Video Conference/Online`
- `Home Visit`
- `Unknown`

### HCP type
`hcp_type` identifies the type of healthcare professional associated with the appointment activity, for example GP, nurse, or other practice staff grouping.

### Time from booking to appointment
`time_between_book_and_appt` is a booking lead-time band, not an exact wait-time in days.

## Geography Guidance

Answer appointments questions at these scopes:
- national
- region
- ICB
- sub-ICB
- practice

Main geography fields:
- `region_name`
- `icb_name`
- `sub_icb_location_name`
- `gp_name`
- `gp_code`

If a user asks for a city such as Leeds, you may need to map it to the relevant ICB while also checking sub-ICB names.

## What This Dataset Is Good For

- "How many appointments were there nationally in the latest month?"
- "What is the DNA rate in NHS Greater Manchester ICB?"
- "Show appointment mode breakdown in North East and Yorkshire."
- "Which practices had the most appointments?"
- "Show GP appointments trend over the past year."

## What This Dataset Is Not For

- GP workforce headcount or FTE
- salary / earnings
- clinical outcomes
- prescribing
- patient satisfaction
- real-time operational status

## Answering Rules

- Never answer appointments questions from workforce tables.
- Never answer workforce questions from appointments tables.
- Use `practice` for national and practice-level appointment activity unless the user clearly wants region / ICB / sub-ICB geography.
- Use `pcn_subicb` for region / ICB / sub-ICB geography.
- Keep geography consistent through follow-up questions.
- Do not call a total an average unless the SQL explicitly calculates an average.
- If a geography name is ambiguous, prefer clarification over guessing.
