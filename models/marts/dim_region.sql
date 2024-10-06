select distinct {{ dbt_utils.generate_surrogate_key(['region_description'])}} as _region_sk,
    region_description,
localtimestamp                      as dbt_loaded_at
from {{ ref('stg_fuel_data_wide_table') }}

