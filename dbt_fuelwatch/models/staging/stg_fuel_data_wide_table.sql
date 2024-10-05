{{
    config(
        materialized='incremental'
    )
}}

with cte as (select publish_date,
                    cast(trading_name as varchar(60)),
                    brand_description,
                    product_description,
                    product_price::double precision,
                    address,
                    location                            as suburb,
                    postcode::integer,
                    area_description,
                    region_description,
                    localtimestamp                      as dbt_loaded_at
             from {{ source('raw', 'raw_fuel_watch__historic_fuel_price') }}
             where product_price is not null)

select cte.*,
       replace(cast(publish_date as varchar), '-', '')::int4 as date_key
from cte

{% if is_incremental() %}

-- this filter will only be applied on an incremental run
-- (uses >= to include records whose timestamp occurred since the last run of this model)
-- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
where dbt_loaded_at >= (select coalesce (max(dbt_loaded_at)
    , '1900-01-01') from {{ this }} )

    {% endif %}

