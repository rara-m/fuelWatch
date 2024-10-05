select distinct {{ dbt_utils.generate_surrogate_key(['trading_name', 'address', 'publish_date'])}} as _station_sk,
    trading_name,
                address,
                suburb,
localtimestamp                      as dbt_loaded_at
from {{ ref('stg_fuel_data_wide_table') }}
