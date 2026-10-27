from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def map_icd9_code(code: str) -> str:
    """Classifies ICD-9 string code into study diagnosis group."""
    if code is None or code in ("?", "", "None"):
        return "Unknown"
    
    clean_c = str(code).strip()
    if clean_c.startswith("V") or clean_c.startswith("E"):
        return "Other"
        
    try:
        val = float(clean_c)
    except ValueError:
        return "Other"
        
    if 250.0 <= val < 251.0 or clean_c.startswith("250"):
        return "Diabetes"
    if (390.0 <= val <= 459.9) or (785.0 <= val < 786.0):
        return "Circulatory"
    if (460.0 <= val <= 519.9) or (786.0 <= val < 787.0):
        return "Respiratory"
    if (520.0 <= val <= 579.9) or (787.0 <= val < 788.0):
        return "Digestive"
    if 800.0 <= val <= 999.9:
        return "Injury"
    if 710.0 <= val <= 739.9:
        return "Musculoskeletal"
    if (580.0 <= val <= 629.9) or (788.0 <= val < 789.0):
        return "Genitourinary"
    if 140.0 <= val <= 239.9:
        return "Neoplasms"
        
    return "Other"

udf_map_icd9 = F.udf(lambda x: map_icd9_code(x), StringType())