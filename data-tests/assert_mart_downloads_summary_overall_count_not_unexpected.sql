with
ranked_weeks as (
    select
        repo,
        download_count::bigint as download_count,
        row_number()
            over (
                partition by repo
                order by download_date desc
            )
            as week_rnk
    from {{ ref('mart_downloads_summary') }}
    where group_name = 'overall'
),

current as (
    select
        repo,
        download_count
    from ranked_weeks
    where week_rnk = 1
),

prev_week as (
    select
        repo,
        download_count
    from ranked_weeks
    where week_rnk = 2
),

combined as (
    select
        cur.repo,
        cur.download_count as current_week_downloads,
        prev.download_count as previous_week_downloads
    from current as cur
    left join prev_week as prev
        on cur.repo = prev.repo
),

diff as (
    select
        repo,
        previous_week_downloads,
        current_week_downloads,
        current_week_downloads - previous_week_downloads as week_downloads_diff,
        (current_week_downloads - previous_week_downloads)
        / previous_week_downloads as week_downloads_diff_pct
    from combined
    where
        previous_week_downloads is not null
        and current_week_downloads is not null
)

-- assert that counts less than double previous week
-- assert that counts greater than half previous week
select
    repo,
    previous_week_downloads,
    current_week_downloads,
    week_downloads_diff,
    week_downloads_diff_pct
from diff
where
    (week_downloads_diff_pct >= 1 and previous_week_downloads > 50000)
    or (week_downloads_diff_pct <= -0.4 and previous_week_downloads > 50000)
