{{
    config(
        materialized='incremental'
    )
}}

select distinct
    {{ dbt_utils.generate_surrogate_key(['trading_name', 'address', 'publish_date'])}} as _station_sk,
    {{ dbt_utils.generate_surrogate_key(['area_description'])}} as _area_sk,
    {{ dbt_utils.generate_surrogate_key(['brand_description'])}} as _brand_sk,
    {{ dbt_utils.generate_surrogate_key(['region_description'])}} as _region_sk,
    {{ dbt_utils.generate_surrogate_key(['suburb', 'postcode'])}} as _suburb_sk,
       date_key,
       publish_date,
       product_price,
       product_description,
localtimestamp                      as dbt_loaded_at
from {{ ref ('stg_fuel_data_wide_table') }}


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
where dbt_loaded_at >= (select coalesce(max(dbt_loaded_at),'1900-01-01') from {{ this }} )

{% endif %}