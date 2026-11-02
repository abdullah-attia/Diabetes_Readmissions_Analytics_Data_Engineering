select benchmark_rate_30d
from {{ ref('mart_readmission_kpis') }}
where benchmark_rate_30d not between 0.085 and 0.095