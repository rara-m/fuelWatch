select distinct {{ dbt_utils.generate_surrogate_key(['brand_description'])}} as _brand_sk,
    brand_description,
localtimestamp                      as dbt_loaded_at
from {{ ref('stg_fuel_data_wide_table') }}
