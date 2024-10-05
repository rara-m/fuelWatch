{{
    config(
        materialized='incremental',
        on_schema_change='fail'
    )
}}

select *
from {{ ref('int_fct_fuel_price') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
where dbt_loaded_at >= (select coalesce(max(dbt_loaded_at),'1900-01-01') from {{ this }} )

{% endif %}
