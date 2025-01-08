with expected_group_names as (
    select tmp.*
    from (
        values
        ('overall'),
        ('python_version'),
        ('package_version')
    ) as tmp (group_name)
),

group_counts as (
    select
        group_name,
        count(*) as n
    from {{ ref('mart_downloads_summary') }}
    group by all
)

select
    a.group_name,
    b.n
from expected_group_names as a
left join group_counts as b
    on a.group_name = b.group_name
where b.n is null
