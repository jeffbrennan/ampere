with
    melted as ( {{
    dbt_utils.unpivot(
        ref('stg_downloads'),
        exclude=['project','timestamp', 'download_count'],
        remove=['retrieved_at'],
        field_name='group_name',
        value_name='group_value'
    )
}} )
select
    project             as repo,
    "timestamp"         as download_timestamp,
    group_name,
    group_value,
    sum(download_count) as download_count
from melted
group by all
