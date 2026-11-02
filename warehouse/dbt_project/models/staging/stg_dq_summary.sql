select
    rule_id,
    rule_name,
    rows_affected::int   as rows_affected,
    action,
    notes
from {{ source('raw', 'dq_summary') }}