{% test no_overlapping_windows(model, key) %}
-- versions of the same key must not overlap in time (windows are half-open)
with ordered as (
    select
        {{ key }},
        valid_from,
        coalesce(valid_to, timestamptz '9999-12-31 00:00:00+00') as valid_to,
        lead(valid_from) over (partition by {{ key }} order by valid_from, valid_to) as next_from
    from {{ model }}
)
select * from ordered
where next_from is not null and next_from < valid_to
{% endtest %}
