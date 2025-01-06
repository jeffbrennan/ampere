with
    base as (
        select "model", unnest(page) as page, "timestamp"
        from {{ ref("int_status_details") }}
        where "timestamp" is not null
    ),
    oldest as (
        select page, min("timestamp") as min_timestamp
        from base
        where "model" not like 'mart_%' and "model" <> 'stg_commits'
        group by page
    ),
    final as (
        select
            page,
            min_timestamp,
            round(
                (extract(epoch from now()) - extract(epoch from min_timestamp)) / 3600, 2
            ) as hours_stale,
            case
                when
                    page in (
                        'summary',
                        'feed',
                        'issues',
                        'network_followers',
                        'network_stargazers'
                    )
                then 24
                when page in ('downloads')
                then 168
            end as hours_stale_threshold,
            hours_stale > hours_stale_threshold as stale,
            case when stale then '⚠️'
            else '✅'
            end as stale_emoji
        from oldest
    )
select page, min_timestamp, hours_stale, hours_stale_threshold, stale, stale_emoji
from final
