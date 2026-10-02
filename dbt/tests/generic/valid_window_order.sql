{% test valid_window_order(model) %}
-- a window may be empty (two changes in one commit) but never inverted
select * from {{ model }}
where valid_to is not null and valid_to < valid_from
{% endtest %}
