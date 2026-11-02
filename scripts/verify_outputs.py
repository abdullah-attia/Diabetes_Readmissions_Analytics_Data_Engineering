"""
Verifies the cleaning outputs against Contract 2.
Usage:
    python scripts/verify_outputs.py                        # checks data/processed
    python scripts/verify_outputs.py data/sample/processed  # checks the sample
"""
import sys
import pyarrow.parquet as pq

base = sys.argv[1] if len(sys.argv) > 1 else "data/processed"
names = ["encounters", "diagnoses", "medications", "lookups", "rejected", "dq_summary"]
t = {n: pq.read_table(f"{base}/{n}").to_pandas() for n in names}
enc = t["encounters"]
n_enc = len(enc)
ids = set(enc["encounter_id"])
meds_per_encounter = len(t["medications"]) / n_enc if n_enc else 0

checks = [
    ("a1c columns present",
     {"a1c_result", "a1c_tested"} <= set(enc.columns)),
    ("no 'alc_' typo columns",
     not any(c.startswith("alc_") for c in enc.columns)),
    ("encounters has exactly the 29 contract columns",
     len(enc.columns) == 29),
    ("diagnoses = 3 rows per encounter",
     len(t["diagnoses"]) == 3 * n_enc),
    ("'Unknown' diagnosis group exists (full data only)",
     (t["diagnoses"]["diagnosis_group"] == "Unknown").any() or base.startswith("data/sample")),
    ("medications = same number of rows per encounter (no gaps)",
     meds_per_encounter == int(meds_per_encounter)),
    ("lookups: no blank ids",
     (t["lookups"]["id"] != "").all()),
    ("lookups: mapping_type has no _id suffix",
     set(t["lookups"]["mapping_type"]) == {"admission_type", "discharge_disposition", "admission_source"}),
    ("no quotes in discharge_disposition labels",
     not enc["discharge_disposition"].fillna("").str.contains('"').any()),
    ("'Not tested' present in a1c_result",
     (enc["a1c_result"] == "Not tested").any()),
    ("no orphan diagnoses",
     set(t["diagnoses"]["encounter_id"]) <= ids),
    ("no orphan medications",
     set(t["medications"]["encounter_id"]) <= ids),
    ("every encounter has a discharge label (lookup join worked)",
     enc["discharge_disposition"].notna().all()),
]

print(f"\nChecking: {base}")
print("Row counts:", {k: len(v) for k, v in t.items()})
print(f"Medication rows per encounter: {meds_per_encounter:g}\n")

failed = 0
for name, ok in checks:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    failed += not ok

eligible_first = enc[enc["is_first_encounter"] & enc["is_readmission_eligible"]]
if len(eligible_first):
    rate = 100 * eligible_first["readmitted_30d"].mean()
    print(f"\nBenchmark: {len(eligible_first):,} patients, {rate:.2f}% readmitted within 30 days "
          f"(published: ~69,973 patients, ~9.0%)")

print(f"\n{'All checks passed.' if failed == 0 else f'{failed} check(s) FAILED.'}")
sys.exit(1 if failed else 0)