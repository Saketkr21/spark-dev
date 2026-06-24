-- DBT-2/3: incremental fact, MERGE strategy on Delta (Hive tables can't MERGE), keyed by order_id.
-- The incremental filter watermarks on EVENT time (ordered_at). With lookback_hours=0 it drops
-- late-arriving rows whose event time predates the high-water mark; a lookback window recaptures
-- them (the merge dedups by unique_key, so re-scanning is safe).
{{ config(
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key='order_id'
) }}

select
    order_id,
    customer_id,
    amount,
    status,
    ordered_at,
    loaded_at
from {{ ref('stg_orders') }}

{% if is_incremental() %}
where ordered_at > (select max(ordered_at) from {{ this }})
                   - interval '{{ var("lookback_hours", 0) }}' hour
{% endif %}
