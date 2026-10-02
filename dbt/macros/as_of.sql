{% macro as_of(relation, ts) %}
{#- Rows of an SCD2 relation as they were at timestamp `ts` (system time). -#}
select *
from {{ relation }}
where valid_from <= {{ ts }}
  and (valid_to is null or valid_to > {{ ts }})
  and not is_deleted
{% endmacro %}
