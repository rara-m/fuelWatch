select distinct
    {{ dbt_utils.generate_surrogate_key(['area_description'])}} as _area_sk,
    area_description,
localtimestamp                      as dbt_loaded_at
from {{ ref('stg_fuel_data_wide_table') }}