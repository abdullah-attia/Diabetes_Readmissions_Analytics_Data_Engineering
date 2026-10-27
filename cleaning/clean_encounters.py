import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
import rules

hadoop_dir = os.path.expanduser("~/hadoop")
os.environ["HADOOP_HOME"] = hadoop_dir
os.environ["hadoop.home.dir"] = hadoop_dir
os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")

def main():
    spark = SparkSession.builder \
        .appName("DiabetesPipeline-MemberB-Cleaning") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # 1. READ RAW DATA
    raw_encounters_path = "data/raw/diabetic_data.csv"
    mapping_csv_path = "data/raw/IDs_mapping.csv"

    raw_encounters = spark.read.option("header", "true").csv(raw_encounters_path)
    total_raw_count = raw_encounters.count()
    dq_records = []

    # 2. RUN CLEANING RULES
    # R1: Nulls & Types
    r1_df = rules.rule_r1_nulls_and_types(raw_encounters)
    dq_records.append(("R1", "Nulls & Types", total_raw_count, "fixed", "Replaced ? with null, cast metrics to int"))

    # R2: Duplicates
    r2_kept, r2_rej = rules.rule_r2_deduplicate(r1_df)
    r2_rej_count = r2_rej.count()
    dq_records.append(("R2", "Deduplication", r2_rej_count, "excluded", "Removed duplicate encounter_ids"))

    # R3: Invalid Gender
    r3_kept, r3_rej = rules.rule_r3_invalid_gender(r2_kept)
    r3_rej_count = r3_rej.count()
    dq_records.append(("R3", "Invalid Gender", r3_rej_count, "excluded", "Removed Unknown/Invalid gender rows"))

    # Combine rejected rows
    all_rejected = r2_rej.unionByName(r3_rej, allowMissingColumns=True)

    # R4: Process Lookups from stacked CSV
    lookups_df = rules.rule_r4_process_lookups(mapping_csv_path, spark_session=spark)
    dq_records.append(("R4", "Labels & Lookups", lookups_df.count(), "relabelled", "Merged ambiguous unknowns to Unknown"))

    # R5: Death / Hospice Eligibility
    r5_df = rules.rule_r5_death_hospice(r3_kept)
    death_hospice_count = r5_df.filter(~F.col("is_readmission_eligible")).count()
    dq_records.append(("R5", "Death/Hospice", death_hospice_count, "flagged", "is_readmission_eligible=False"))

    # R6: Sparse Fields
    r6_df = rules.rule_r6_sparse_fields(r5_df)
    dq_records.append(("R6", "Sparse Fields", total_raw_count, "relabelled", "Dropped weight; nulls converted to Unknown"))

    # R7: Unpivot Diagnoses
    diagnoses_df = rules.rule_r7_unpivot_diagnoses(r6_df)
    dq_records.append(("R7", "Diagnosis Unpivot", diagnoses_df.count(), "fixed", "Unpivoted diag 1-3 to relational long format"))

    # R8: Age Parsing
    r8_df = rules.rule_r8_age_parsing(r6_df)
    dq_records.append(("R8", "Age Mapping", r8_df.count(), "fixed", "Extracted age_group text and age_mid integer"))

    # R9: Unpivot Medications
    medications_df = rules.rule_r9_unpivot_medications(r8_df)
    dq_records.append(("R9", "Medications Unpivot", medications_df.count(), "fixed", "Unpivoted 23 med columns to long format"))

    # R10: Metric Flags & Sequences
    final_encounters = rules.rule_r10_flags_and_windows(r8_df)
    dq_records.append(("R10", "Operational Flags", final_encounters.count(), "flagged", "Created readmitted_30d, seq, and first_encounter"))

    # 3. CONTRACT 2 SCHEMA ALIGNMENT
    # Rename 'id' on each lookup to avoid ambiguous column lineage conflicts
    adm_types = lookups_df.filter(F.col("mapping_type") == "admission_type_id") \
                          .select(F.col("id").alias("adm_type_id_join"), F.col("label").alias("admission_type"))
                          
    adm_src = lookups_df.filter(F.col("mapping_type") == "admission_source_id") \
                        .select(F.col("id").alias("adm_src_id_join"), F.col("label").alias("admission_source"))
                        
    disch_disp = lookups_df.filter(F.col("mapping_type") == "discharge_disposition_id") \
                           .select(F.col("id").alias("disch_disp_id_join"), F.col("label").alias("discharge_disposition"))

    contract_encounters = final_encounters \
        .join(adm_types, final_encounters.admission_type_id.cast(StringType()) == adm_types.adm_type_id_join, "left") \
        .join(adm_src, final_encounters.admission_source_id.cast(StringType()) == adm_src.adm_src_id_join, "left") \
        .join(disch_disp, final_encounters.discharge_disposition_id.cast(StringType()) == disch_disp.disch_disp_id_join, "left") \
        .drop("adm_type_id_join", "adm_src_id_join", "disch_disp_id_join") \
        .withColumn("payer", F.col("payer_code")) \
        .select(
            F.col("encounter_id").cast(StringType()),
            F.col("patient_nbr").cast(StringType()),
            "encounter_seq", "is_first_encounter", "race", "gender",
            "age_group", "age_mid", "admission_type", "admission_source",
            "discharge_disposition", "is_readmission_eligible", "time_in_hospital",
            "payer", "medical_specialty", "num_lab_procedures", "num_procedures",
            "num_medications", "number_outpatient", "number_emergency",
            "number_inpatient", "number_diagnoses", "alc_result", "alc_tested",
            "glucose_result", "med_changed", "on_diabetes_med", "readmitted",
            "readmitted_30d"
        )

    # 4. RECONCILIATION CHECK
    clean_count = contract_encounters.count()
    rejected_count = all_rejected.count()

    print("----------------------------------------------------------------------")
    print(f"RECONCILIATION AUDIT: Raw({total_raw_count}) vs Clean({clean_count}) + Rejected({rejected_count})")
    print("----------------------------------------------------------------------")
    if total_raw_count != (clean_count + rejected_count):
        print("CRITICAL FAILURE: Pipeline record reconciliation did not balance!", file=sys.stderr)
        sys.exit(1)

    # BUILD DQ_SUMMARY DATAFRAME
    dq_schema = StructType([
        StructField("rule_id", StringType(), False),
        StructField("rule_name", StringType(), False),
        StructField("rows_affected", LongType(), False),
        StructField("action", StringType(), False),
        StructField("notes", StringType(), True)
    ])
    dq_summary_df = spark.createDataFrame(dq_records, schema=dq_schema)

    # 5. WRITE CONTRACT 2 PARQUET ARTIFACTS
    out_dir = "data/processed"
    contract_encounters.write.mode("overwrite").parquet(f"{out_dir}/encounters")
    diagnoses_df.write.mode("overwrite").parquet(f"{out_dir}/diagnoses")
    medications_df.write.mode("overwrite").parquet(f"{out_dir}/medications")
    lookups_df.write.mode("overwrite").parquet(f"{out_dir}/lookups")
    all_rejected.write.mode("overwrite").parquet(f"{out_dir}/rejected")
    dq_summary_df.write.mode("overwrite").parquet(f"{out_dir}/dq_summary")

    print("\nSUCCESS: All 6 Parquet outputs created in data/processed/!")

if __name__ == "__main__":
    main()