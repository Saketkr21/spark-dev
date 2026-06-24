-- Typed orders. The `load_through` var simulates "data available as of this load time"
-- (filtering on loaded_at, the LOAD time) so the incremental/late-arrival labs are reproducible.
with source as (

    select * from {{ ref('orders') }}

),

typed as (

    select
        cast(order_id as bigint)        as order_id,
        customer_id,
        cast(amount as double)          as amount,
        status,
        cast(ordered_at as timestamp)   as ordered_at,   -- EVENT time
        cast(loaded_at  as timestamp)   as loaded_at      -- LOAD time (when it landed)
    from source

)

select * from typed
where loaded_at <= cast('{{ var("load_through", "2099-01-01 00:00:00") }}' as timestamp)
