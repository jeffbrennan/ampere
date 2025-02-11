with
    base as (
        select "model", unnest("page") as "page", "timestamp", timestamp_col
        from {{ ref("int_status_details") }}
        where "timestamp" is not null
    ),
    base_ranked as (
        select
            "page",
            "model",
            timestamp_col,
            "timestamp",
            row_number() over (partition by "page" order by "timestamp" asc) as rn
        from base
        where 
            "model" not like 'mart_%' 
            and "model" <> 'stg_commits'
    ),
    oldest as (
        select
            "page",
            "model",
            timestamp_col,
            "timestamp" as min_timestamp
        from base_ranked
        where rn=1
    ),
    final as (
        select
            page,
            min_timestamp,
            round(
                (extract(epoch from now()) - extract(epoch from min_timestamp)) / 3600, 2
            ) as hours_stale,
            case
                when timestamp_col = 'retrieved_at' then 24
                when
                    timestamp_col in ('download_timestamp', 'download_date')
                    then 24 * 2 + 10 + 1
                -- metric data that can vary (not every day will have a new commit etc.)
                else 24 * 7 * 4
            end as hours_stale_threshold,
            hours_stale > hours_stale_threshold as stale,
            case when stale then '⚠️'
            else '✅'
            end as stale_emoji
        from oldest
    )
select page, min_timestamp, hours_stale, hours_stale_threshold, stale, stale_emoji
from final
