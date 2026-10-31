SELECT e.discharge_disposition_id, m.description, COUNT(*) AS encounters
FROM stg.encounters_raw e
LEFT JOIN stg.id_mapping m ON m.mapping_type = 'discharge_disposition' AND m.id = e.discharge_disposition_id
WHERE e.discharge_disposition_id IN ('11','13','14','19','20','21')
GROUP BY e.discharge_disposition_id, m.description;

SELECT 'examide' AS med, COUNT(DISTINCT examide) AS distinct_values, SUM(CASE WHEN examide <> 'No' THEN 1 ELSE 0 END) AS used FROM stg.encounters_raw
UNION ALL SELECT 'citoglipton', COUNT(DISTINCT citoglipton), SUM(CASE WHEN citoglipton <> 'No' THEN 1 ELSE 0 END) FROM stg.encounters_raw
UNION ALL SELECT 'acetohexamide', COUNT(DISTINCT acetohexamide), SUM(CASE WHEN acetohexamide <> 'No' THEN 1 ELSE 0 END) FROM stg.encounters_raw
UNION ALL SELECT 'miglitol', COUNT(DISTINCT miglitol), SUM(CASE WHEN miglitol <> 'No' THEN 1 ELSE 0 END) FROM stg.encounters_raw
UNION ALL SELECT 'tolazamide', COUNT(DISTINCT tolazamide), SUM(CASE WHEN tolazamide <> 'No' THEN 1 ELSE 0 END) FROM stg.encounters_raw
UNION ALL SELECT 'chlorpropamide', COUNT(DISTINCT chlorpropamide), SUM(CASE WHEN chlorpropamide <> 'No' THEN 1 ELSE 0 END) FROM stg.encounters_raw;