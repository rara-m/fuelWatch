select distinct {{ dbt_utils.generate_surrogate_key(['suburb', 'postcode'])}} as _suburb_sk,
    suburb,
    postcode::int2 as postcode,
localtimestamp                      as dbt_loaded_at
from {{ ref('stg_fuel_data_wide_table') }}
