from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

MED_COLUMNS = [
    "metformin", "repaglinide", "nateglinide", "chlorpropamide",
    "glimepiride", "acetohexamide", "glipizide", "glyburide", "tolbutamide",
    "pioglitazone", "rosiglitazone", "acarbose", "miglitol", "troglitazone",
    "tolazamide", "examide", "citoglipton", "insulin", "glyburide-metformin",
    "glipizide-metformin", "glimepiride-pioglitazone",
    "metformin-rosiglitazone", "metformin-pioglitazone"
]

def rule_r1_nulls_and_types(df: DataFrame):
    """Replaces '?' with null, trims strings, casts ints, retains 'None' as 'Not tested'."""
    cols = df.columns
    out = df
    for c in cols:
        out = out.withColumn(c, F.when(F.trim(F.col(c)) == "?", None).otherwise(F.trim(F.col(c))))
    
    # Preserve untested status for lab metrics
    out = out.withColumn("A1Cresult", F.when(F.col("A1Cresult").isNull() | (F.col("A1Cresult") == "None"), "Not tested").otherwise(F.col("A1Cresult")))
    out = out.withColumn("max_glu_serum", F.when(F.col("max_glu_serum").isNull() | (F.col("max_glu_serum") == "None"), "Not tested").otherwise(F.col("max_glu_serum")))
    
    int_cols = [
        "time_in_hospital", "num_lab_procedures", "num_procedures",
        "num_medications", "number_outpatient", "number_emergency",
        "number_inpatient", "number_diagnoses"
    ]
    for c in int_cols:
        out = out.withColumn(c, F.col(c).cast(IntegerType()))
        
    return out

def rule_r2_deduplicate(df: DataFrame):
    """Rejects duplicate encounter_id entries."""
    w = Window.partitionBy("encounter_id").orderBy(F.lit(1))
    flagged = df.withColumn("_row_num", F.row_number().over(w))
    kept = flagged.filter(F.col("_row_num") == 1).drop("_row_num")
    rejected = flagged.filter(F.col("_row_num") > 1).drop("_row_num") \
                      .withColumn("reject_reason", F.lit("R2: Duplicate encounter_id"))
    return kept, rejected

def rule_r3_invalid_gender(df: DataFrame):
    """Rejects records where gender is 'Unknown/Invalid'."""
    invalid_mask = (F.col("gender") == "Unknown/Invalid") | (F.col("gender").isNull())
    kept = df.filter(~invalid_mask)
    rejected = df.filter(invalid_mask).withColumn("reject_reason", F.lit("R3: Invalid gender"))
    return kept, rejected

def rule_r4_process_lookups(mapping_path_or_df, spark_session=None):
    """
    Parses IDs_mapping.csv or DataFrame into Contract 2 lookup schema:
    (mapping_type, id, label)
    """
    # If a string path to CSV is passed, parse the stacked text format
    if isinstance(mapping_path_or_df, str):
        with open(mapping_path_or_df, "r", encoding="utf-8") as f:
            lines = f.readlines()

        rows = []
        current_type = None
        unknowns = {"null", "not available", "not mapped", "unknown/invalid", "unknown", ""}

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Detect section header transitions
            lower_line = line.lower()
            if "admission_type_id" in lower_line:
                current_type = "admission_type_id"
                continue
            elif "discharge_disposition_id" in lower_line:
                current_type = "discharge_disposition_id"
                continue
            elif "admission_source_id" in lower_line:
                current_type = "admission_source_id"
                continue

            parts = line.split(",", 1)
            if len(parts) == 2 and current_type:
                code_id, desc = parts[0].strip(), parts[1].strip()
                label = "Unknown" if desc.lower() in unknowns or not desc else desc
                rows.append((current_type, str(code_id), label))

        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("mapping_type", StringType(), False),
            StructField("id", StringType(), False),
            StructField("label", StringType(), False)
        ])
        return spark_session.createDataFrame(rows, schema=schema).dropDuplicates(["mapping_type", "id"])

    # Fallback if Member A's staging table DataFrame is passed via JDBC
    df = mapping_path_or_df
    unknowns = ["null", "not available", "not mapped", "unknown/invalid", "unknown"]
    return df.withColumn(
        "label",
        F.when(F.lower(F.col("description")).isin(unknowns) | F.col("description").isNull(), "Unknown")
         .otherwise(F.col("description"))
    ).select(
        F.col("mapping_type").cast(StringType()),
        F.col("id").cast(StringType()),
        F.col("label").cast(StringType())
    ).dropDuplicates(["mapping_type", "id"])

def rule_r5_death_hospice(df: DataFrame):
    """Flags is_readmission_eligible = False for death/hospice discharge codes."""
    # Discharge IDs: 11 (Expired), 13 (Hospice home), 14 (Hospice medical), 
    # 19 (Expired at home), 20 (Expired in facility), 21 (Expired unknown)
    death_hospice_ids = ["11", "13", "14", "19", "20", "21"]
    out = df.withColumn(
        "is_readmission_eligible",
        ~F.col("discharge_disposition_id").cast(StringType()).isin(death_hospice_ids)
    )
    return out

def rule_r6_sparse_fields(df: DataFrame):
    """Drops uninformative sparse weight; defaults nulls to 'Unknown'."""
    out = df.drop("weight")
    for col_name in ["race", "payer_code", "medical_specialty"]:
        out = out.withColumn(col_name, F.when(F.col(col_name).isNull(), "Unknown").otherwise(F.col(col_name)))
    return out

def rule_r7_unpivot_diagnoses(df: DataFrame):
    """
    Transforms diag_1, diag_2, diag_3 into a normalized relational table
    and classifies codes using native Spark SQL expressions.
    """
    # 1. Unpivot diag_1, diag_2, diag_3
    stack_expr = "stack(3, '1', diag_1, '2', diag_2, '3', diag_3) as (diag_position, icd9_code)"
    dx_df = df.select("encounter_id", F.expr(stack_expr)) \
              .filter(F.col("icd9_code").isNotNull())

    # 2. Clean code text
    dx_df = dx_df.withColumn("code_clean", F.trim(F.col("icd9_code")))

    # 3. Safe numeric extraction (null if V/E code, '?', or invalid)
    dx_df = dx_df.withColumn(
        "code_num",
        F.when(
            ~F.col("code_clean").rlike(r"^[VE\?]"),
            F.regexp_extract(F.col("code_clean"), r"^(\d+(\.\d+)?)", 1).cast("double")
        ).otherwise(None)
    )

    # 4. Native Spark SQL classification logic (B.2 Study Groups)
    dx_group = (
        F.when(F.col("code_clean").isin("?", "", "None") | F.col("code_clean").isNull(), "Unknown")
        .when(F.col("code_clean").startswith("V") | F.col("code_clean").startswith("E"), "Other")
        # Diabetes: 250.xx
        .when((F.col("code_num") >= 250.0) & (F.col("code_num") < 251.0), "Diabetes")
        # Circulatory: 390-459, 785
        .when(((F.col("code_num") >= 390.0) & (F.col("code_num") <= 459.9)) |
              ((F.col("code_num") >= 785.0) & (F.col("code_num") < 786.0)), "Circulatory")
        # Respiratory: 460-519, 786
        .when(((F.col("code_num") >= 460.0) & (F.col("code_num") <= 519.9)) |
              ((F.col("code_num") >= 786.0) & (F.col("code_num") < 787.0)), "Respiratory")
        # Digestive: 520-579, 787
        .when(((F.col("code_num") >= 520.0) & (F.col("code_num") <= 579.9)) |
              ((F.col("code_num") >= 787.0) & (F.col("code_num") < 788.0)), "Digestive")
        # Injury: 800-999
        .when((F.col("code_num") >= 800.0) & (F.col("code_num") <= 999.9), "Injury")
        # Musculoskeletal: 710-739
        .when((F.col("code_num") >= 710.0) & (F.col("code_num") <= 739.9), "Musculoskeletal")
        # Genitourinary: 580-629, 788
        .when(((F.col("code_num") >= 580.0) & (F.col("code_num") <= 629.9)) |
              ((F.col("code_num") >= 788.0) & (F.col("code_num") < 789.0)), "Genitourinary")
        # Neoplasms: 140-239
        .when((F.col("code_num") >= 140.0) & (F.col("code_num") <= 239.9), "Neoplasms")
        .otherwise("Other")
    )

    return dx_df.withColumn("diagnosis_group", dx_group) \
                .select("encounter_id", "diag_position", "icd9_code", "diagnosis_group")

def rule_r8_age_parsing(df: DataFrame):
    """Parses bracketed age groups (e.g., '[70-80)') into display label and midpoint."""
    # Derive '70-79' and 75
    clean_brackets = F.regexp_replace(F.col("age"), r"[\[\)]", "")
    split_age = F.split(clean_brackets, "-")
    low = split_age.getItem(0).cast(IntegerType())
    high = split_age.getItem(1).cast(IntegerType())
    
    out = df.withColumn("age_group", F.concat(low, F.lit("-"), high - 1)) \
            .withColumn("age_mid", (low + high) / 2)
    return out

def rule_r9_unpivot_medications(df: DataFrame):
    """Unpivots 23 medication columns into 1 row per encounter x medication."""
    stack_args = ", ".join([f"'{c}', `{c}`" for c in MED_COLUMNS])
    stack_expr = f"stack({len(MED_COLUMNS)}, {stack_args}) as (medication, status)"
    meds_df = df.select("encounter_id", F.expr(stack_expr)) \
                .filter(F.col("status").isNotNull())
    return meds_df

def rule_r10_flags_and_windows(df: DataFrame):
    """Calculates operational flags, sequential visit order, and first-encounter markers."""
    out = df.withColumn("readmitted_30d", F.col("readmitted") == "<30") \
            .withColumn("alc_tested", F.col("A1Cresult") != "Not tested") \
            .withColumn("glucose_result", F.col("max_glu_serum")) \
            .withColumn("alc_result", F.col("A1Cresult")) \
            .withColumn("med_changed", F.col("change") == "Ch") \
            .withColumn("on_diabetes_med", F.col("diabetesMed") == "Yes")
            
    # Window ordered by encounter_id for patient progression
    w_patient = Window.partitionBy("patient_nbr").orderBy("encounter_id")
    out = out.withColumn("encounter_seq", F.row_number().over(w_patient)) \
            .withColumn("is_first_encounter", F.col("encounter_seq") == 1)
            
    return out