-- gold aggregate must equal the sum of its detail rows
with daily as (select sum(gross_revenue) as total from {{ ref('fct_daily_revenue') }}),
detail as (
    select coalesce(sum(amount), 0) as total
    from {{ ref('fct_orders') }}
    where status <> 'cancelled'
)
select * from daily, detail where daily.total <> detail.total
