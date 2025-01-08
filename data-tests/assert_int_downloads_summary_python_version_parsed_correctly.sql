with base as (
    select
        group_name,
        group_value,
        regexp_matches(group_value, '^\d{1}\.\d{1,2}$|^unknown$|^other$') as is_valid
    from
        {{ ref('int_downloads_summary') }}
    where group_name = 'python_version'
)

select
    group_value,
    is_valid,
    count(*) as n
from base
where not is_valid
group by all
order by n desc
