{% test one_current_per_key(model, key) %}
-- every key must have exactly one current version
select {{ key }}, count(*) filter (where is_current) as current_versions
from {{ model }}
group by {{ key }}
having count(*) filter (where is_current) <> 1
{% endtest %}
