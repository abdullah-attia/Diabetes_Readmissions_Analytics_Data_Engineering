# Data Issues Register

**Owner:** Member A · **Source:** `stg.encounters_raw`, `stg.id_mapping` · **Profiled on:** 2026-10-21 · **Query file:** `ingestion/profiling.sql`

## Overview

| Measure | Value |
|---|---|
| Rows in file | 101,766 |
| Rows in staging | 101,766 |
| Audit: rows_read = rows_written? | Yes (load_id 3: 101,766 = 101,766) |
| Distinct encounter_id | 101,766 (equals rows – no duplicate encounters) |
| Distinct patient_nbr | 71,518 |
| Mapping file | 68 lines → 63 IDs (8 admission_type, 30 discharge_disposition, 25 admission_source); 3 header lines + 2 blank lines removed |

## Issues

### 1. Missing values hidden as "?"
| Column | "?" count | % |
|---|---|---|
| weight | 98,569 | 96.86% |
| medical_specialty | 49,949 | 49.08% |
| payer_code | 40,256 | 39.56% |
| race | 2,273 | 2.23% |
| diag_3 | 1,423 | 1.40% |
| diag_2 | 358 | 0.35% |
| diag_1 | 21 | 0.02% |

7 columns contain "?". No column has true blanks/NULLs – every missing value is hidden as "?".

**Business impact:** tools treat "?" as a real category, so missing data looks complete.
**For Member B:** Rule R1 – replace "?" with null.

### 2. Very sparse fields
| Column | % missing |
|---|---|
| weight | 96.86% |
| medical_specialty | 49.08% |
| payer_code | 39.56% |

**Business impact:** department and payer analysis looks precise but isn't.
**For Member B:** Rule R6 – drop weight; "Unknown" for the rest.

### 3. Codes without labels
| Mapping type | IDs in mapping file | IDs used in encounters with no label |
|---|---|---|
| admission_type | 8 | 0 |
| discharge_disposition | 30 | 0 |
| admission_source | 25 | 0 |

Every ID used in the encounters has a label – but only in a separate file that holds 3 tables stacked in one CSV, separated by blank lines and repeated header rows. Some admission_source labels also start with a leading space (e.g. " Not Mapped").

**Business impact:** nobody can read the reports.
**For Member B:** Rule R4 – parse the mapping, trim labels, join to encounters.

### 4. Many ways to say "unknown"
| Mapping type | ID | Label | Encounters |
|---|---|---|---|
| admission_type | 6 | NULL | 5,291 |
| admission_type | 5 | Not Available | 4,785 |
| admission_type | 8 | Not Mapped | 320 |
| discharge_disposition | 18 | NULL | 3,691 |
| discharge_disposition | 25 | Not Mapped | 989 |
| discharge_disposition | 26 | Unknown/Invalid | 0 |
| admission_source | 17 | NULL | 6,781 |
| admission_source | 20 | Not Mapped | 161 |
| admission_source | 9 | Not Available | 125 |
| admission_source | 15 | Not Available | 0 |
| admission_source | 21 | Unknown/Invalid | 0 |

| Mapping type | Encounters with an "unknown" label | % of encounters |
|---|---|---|
| admission_type | 10,396 | 10.2% |
| admission_source | 7,067 | 6.9% |
| discharge_disposition | 4,680 | 4.6% |

4 different labels (NULL, Not Available, Not Mapped, Unknown/Invalid) mean the same thing. admission_source even has two different IDs (9 and 15) both labelled "Not Available".

**Business impact:** the same missing value is split across several categories.
**For Member B:** Rule R4 – merge these 11 IDs into "Unknown".

### 5. Patients who died or went to hospice
| ID | Label | Encounters |
|---|---|---|
| 11 | Expired | 1,642 |
| 13 | Hospice / home | 399 |
| 14 | Hospice / medical facility | 372 |
| 19 | Expired at home. Medicaid only, hospice. | 8 |
| 20 | Expired in a medical facility. Medicaid only, hospice. | 2 |
| 21 | Expired, place unknown. Medicaid only, hospice. | 0 (not used) |
| **Total** | | **2,423 (2.38%)** |

1,652 deaths (IDs 11, 19, 20) and 771 hospice discharges (IDs 13, 14).

**Business impact:** 2,423 encounters cannot be readmitted, so including them makes the readmission rate look better than it is.
**For Member B:** Rule R5 – is_readmission_eligible = false for IDs 11, 13, 14, 19, 20, 21.

### 6. Repeat patients
| Encounters per patient | Patients | Encounters |
|---|---|---|
| 1 | 54,745 | 54,745 |
| 2 | 10,434 | 20,868 |
| 3–5 | 5,466 | 19,253 |
| 6+ | 873 | 6,900 |
| **Total** | **71,518** | **101,766** |

16,773 patients (23.5%) came more than once and account for 47,021 encounters (46.2%). The most frequent patient (88785891) has 40 encounters.

**Business impact:** counting rows as patients overstates patient numbers by 42% (101,766 rows vs 71,518 patients).
**For Member B:** Rule R10 – encounter_seq and is_first_encounter.

### 7. Raw diagnosis codes
| Measure | Value |
|---|---|
| Distinct codes in diag_1–3 | 916 (including "?") |
| Distinct V codes / E codes | 49 / 85 |
| V-code values / E-code values | 7,263 / 1,976 |
| "?" values | 1,802 (= 21 + 358 + 1,423 from issue 1) |
| Diabetes (250.xx) values | 38,708 |

**Business impact:** 916 raw codes give no meaningful breakdown by condition.
**For Member B:** Rule R7 – map ICD-9 codes to 9 groups; V and E codes → Other, "?" → Unknown.

### 8. Invalid / awkward values
| Field | Value | Encounters |
|---|---|---|
| gender | Female | 54,708 |
| gender | Male | 47,055 |
| gender | Unknown/Invalid | **3** |

| age | Encounters | age | Encounters |
|---|---|---|---|
| [0-10) | 161 | [50-60) | 17,256 |
| [10-20) | 691 | [60-70) | 22,483 |
| [20-30) | 1,657 | [70-80) | 26,068 |
| [30-40) | 3,775 | [80-90) | 17,197 |
| [40-50) | 9,685 | [90-100) | 2,793 |

Age is stored as 10 text brackets, not a number. 65,541 encounters (64.4%) are patients aged 60+.

**Business impact:** errors in demographics; age can't be sorted or averaged.
**For Member B:** Rules R3 (reject the 3 invalid-gender rows) and R8 (age_group + age_mid).

### 9. "None" means "not tested"
| Test | None (not tested) | Norm | >7 / >200 | >8 / >300 | Tested |
|---|---|---|---|---|---|
| A1Cresult | 84,748 | 4,990 | 3,812 | 8,216 | 17,018 (16.7%) |
| max_glu_serum | 96,420 | 2,597 | 1,485 | 1,264 | 5,346 (5.3%) |

"None" is stored as the word None (not blank), so it survives the load – but many readers (e.g. pandas) turn it into null.

**Business impact:** if "None" becomes null, 83% of patients silently disappear from the HbA1c analysis.
**For Member B:** Rule R1 – keep "None" and relabel it "Not tested".

### 10. Wide medication columns
23 medication columns, each with values No / Steady / Up / Down.

| Medication | Distinct values | Times used (not "No") | Flag |
|---|---|---|---|
| examide | 1 | 0 | **CONSTANT** |
| citoglipton | 1 | 0 | **CONSTANT** |
| acetohexamide | 2 | 1 | NEARLY CONSTANT |
| glimepiride-pioglitazone | 2 | 1 | NEARLY CONSTANT |
| metformin-pioglitazone / metformin-rosiglitazone | 2 | 1 / 2 | NEARLY CONSTANT |
| troglitazone | 2 | 3 | NEARLY CONSTANT |
| glipizide-metformin | 2 | 13 | NEARLY CONSTANT |
| tolbutamide | 2 | 23 | NEARLY CONSTANT |
| miglitol | 4 | 38 | NEARLY CONSTANT |
| tolazamide | 3 | 39 | NEARLY CONSTANT |
| chlorpropamide | 4 | 86 | NEARLY CONSTANT |
| acarbose | 4 | 308 | |
| nateglinide | 4 | 703 | |
| glyburide-metformin | 4 | 706 | |
| repaglinide | 4 | 1,539 | |
| glimepiride | 4 | 5,191 | |
| rosiglitazone | 4 | 6,365 | |
| pioglitazone | 4 | 7,328 | |
| glyburide | 4 | 10,650 | |
| glipizide | 4 | 12,686 | |
| metformin | 4 | 19,988 | |
| insulin | 4 | 54,383 | |

2 columns are constant (never prescribed: examide, citoglipton) and 10 more were used fewer than 100 times – so 12 of 23 medication columns carry almost no information. Insulin is the most used (54,383 encounters, 53%).

**Business impact:** hard to analyse; constant columns add noise.
**For Member B:** Rule R9 – unpivot into medications; note constant columns in dq_summary.

## Target column (for the benchmark)
| readmitted | Encounters | % |
|---|---|---|
| <30 | 11,357 | 11.16% |
| >30 | 35,545 | 34.93% |
| NO | 54,864 | 53.91% |

This is the raw encounter-level rate (11.16%). The benchmark (9.0%) uses first encounter per patient and excludes death/hospice, so our clean figure should drop towards 9%.

## Governance note
Data is fully de-identified public data (UCI dataset 296, CC BY 4.0, Strack et al. 2014). It is not data from our training hospital.
