# Data Cleaning & Clinical Rules Report (Member B)

## 1. Executive Summary
- **Raw Ingested Records**: 101,766 encounters
- **Clean Records Kept**: 101,763 encounters
- **Records Excluded**: 3 encounters
- **Reconciliation Audit**: `Raw (101,766) = Clean (101,763) + Rejected (3)` (0 row loss, balanced)

---

## 2. Cleaning Rules Summary Table (`dq_summary`)

| Rule ID | Rule Name | Rows Affected | Action | Description & Decision Rationale |
| :--- | :--- | :--- | :--- | :--- |
| **R1** | Nulls & Types | 101,766 | Fixed | Converted hidden `?` values to `NULL`; cast hospital metrics to integers; mapped `None` in A1Cresult and max_glu_serum to `"Not tested"` so untested patients remain in HbA1c quality metrics. |
| **R2** | Deduplication | 0 | Excluded | Checked `encounter_id` uniqueness; 0 duplicates found. |
| **R3** | Invalid Gender | 3 | Excluded | Excluded 3 encounters where `gender == 'Unknown/Invalid'`. Saved to `rejected` dataset for compliance auditability. |
| **R4** | Labels & Lookups | 65 | Relabelled | Parsed stacked mapping tables; consolidated missing categories (`NULL`, `Not Available`, `Not Mapped`, `Unknown/Invalid`) into a single `"Unknown"` category. |
| **R5** | Death / Hospice | 2,423 | Flagged | Set `is_readmission_eligible = False` for discharge disposition codes 11, 13, 14, 19, 20, 21. Kept records for Length of Stay (ALOS) metrics while excluding them from readmission penalty metrics. |
| **R6** | Sparse Fields | 101,766 | Relabelled | Dropped `weight` (~97% missing); imputed nulls in `race`, `payer`, and `medical_specialty` as `"Unknown"`. |
| **R7** | Diagnosis Unpivot | 303,487 | Fixed | Normalized `diag_1`, `diag_2`, and `diag_3` into 1 row per encounter-position; mapped ICD-9 codes to clinical study groups. |
| **R8** | Age Mapping | 101,763 | Fixed | Parsed bracketed age ranges (e.g. `[70-80)`) into standardized label `"70-79"` and numeric midpoint `75.0`. |
| **R9** | Medications Unpivot | 2,340,549 | Fixed | Unpivoted 23 medication columns into long format (1 row per encounter × medication). |
| **R10** | Operational Flags | 101,763 | Flagged | Generated `readmitted_30d` boolean, visit sequence numbers (`encounter_seq`), and `is_first_encounter` flags. |

---

## 3. Benchmark Verification Check
- **Target Definition**: First encounter per patient (`is_first_encounter = True`) and readmission-eligible discharges (`is_readmission_eligible = True`).
- **Published Study Benchmark**: ~69,973 patients with a 30-day readmission rate of ~9.0%.
- **Pipeline Observed Values**: **69,933 patients** with **6,298 readmissions** (**9.01%**).
- **Status**: Passed benchmark validation.