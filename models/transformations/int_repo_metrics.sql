with
star_metrics as (
    select
        a.repo_id,
        'stars' as metric_type,
        null as metric_id,
        a.starred_at as metric_timestamp,
        a.user_id,
        count(a.user_id) over (
            partition by a.repo_id
            order by a.starred_at
            rows between unbounded preceding and current row
        ) as metric_count
    from {{ ref('stg_stargazers') }} as a
),

issue_metrics_open as (
    select
        repo_id,
        issue_id as metric_id,
        created_at as metric_timestamp,
        author_id as user_id,
        1 as metric_count
    from {{ ref('stg_issues') }}
    where created_at is not null
),

issue_metrics_closed as (
    select
        repo_id,
        issue_id as metric_id,
        closed_at as metric_timestamp,
        author_id as user_id,
        -1 as metric_count
    from {{ ref('stg_issues') }}
    where closed_at is not null
),

issue_metrics_in_pr as (
    select
    a.repo_id,
    b.pr_id as metric_id,
    b.created_at + interval '1 second' as metric_timestamp,
    b.author_id as user_id,
    -1 as metric_count
    from {{ ref('stg_issues') }} as a
    inner join {{ ref('stg_pull_requests') }} as b
    on a.repo_id = b.repo_id
    and a.issue_number = b.pr_number
    where b.closed_at is null
),

issue_metrics as (
    select
        repo_id,
        'issues' as metric_type,
        metric_id,
        metric_timestamp,
        user_id,
        sum(metric_count) over (
            partition by repo_id
            order by metric_timestamp
            rows between unbounded preceding and current row
        ) as metric_count
    from
        (
            select *
            from issue_metrics_open
            union all
            select *
            from issue_metrics_closed
            union all
            select *
            from issue_metrics_in_pr
        )
),

pr_metrics_open as (
    select
        repo_id,
        pr_id as metric_id,
        created_at as metric_timestamp,
        author_id as user_id,
        1 as metric_count
    from {{ ref('stg_pull_requests') }}
    where created_at is not null
),

pr_metrics_closed as (
    select
        repo_id,
        pr_id as metric_id,
        closed_at as metric_timestamp,
        author_id as user_id,
        -1 as metric_count
    from {{ ref('stg_pull_requests') }}
    where closed_at is not null
),

pr_metrics as (
    select
        repo_id,
        'pull requests' as metric_type,
        metric_id,
        metric_timestamp,
        user_id,
        sum(metric_count) over (
            partition by repo_id
            order by metric_timestamp
            rows between unbounded preceding and current row
        ) as metric_count
    from
        (
            select *
            from pr_metrics_open
            union all
            select *
            from pr_metrics_closed
        )
),

fork_metrics as (
    select
        repo_id,
        'forks' as metric_type,
        fork_id as metric_id,
        created_at as metric_timestamp,
        owner_id as user_id,
        count(user_id) over (
            partition by repo_id
            order by created_at
            rows between unbounded preceding and current row
        ) as metric_count
    from {{ ref('stg_forks') }}
),
commit_metrics_unnested as (
    select
        commit_id,
        unnest(stats) as stats_unnested
    from {{ ref('stg_commits') }}
),
commit_metrics_summed as (
    select
        commit_metrics_unnested.commit_id,
        sum(stats_unnested.additions) as additions_count, --noqa: RF01
        sum(stats_unnested.deletions) as deletions_count --noqa: RF01
    from commit_metrics_unnested
    where
        ends_with(stats_unnested.filename, '.py') --noqa: RF01
        or ends_with(stats_unnested.filename, '.scala') --noqa: RF01
        or ends_with(stats_unnested.filename, '.rs') --noqa: RF01
    group by commit_metrics_unnested.commit_id
),
commit_metrics_added as (
    select
        a.repo_id,
        a.commit_id as metric_id,
        a.committed_at as metric_timestamp,
        a.author_id as user_id,
        b.additions_count as metric_count
    from {{ ref('stg_commits') }} as a
    inner join commit_metrics_summed as b
    on a.commit_id = b.commit_id
),

commit_metrics_deleted as (
    select
        a.repo_id,
        a.commit_id as metric_id,
        a.committed_at as metric_timestamp,
        a.author_id as user_id,
        b.deletions_count * -1 as metric_count
    from {{ ref('stg_commits') }} as a
    inner join commit_metrics_summed as b
    on a.commit_id = b.commit_id
),

commit_metrics_combined as (
    select
        repo_id,
        metric_id,
        metric_timestamp,
        user_id,
        sum(metric_count) as metric_count
    from
        (
            select *
            from commit_metrics_added
            union all
            select *
            from commit_metrics_deleted
        )
    group by all

),

codebase_size_metrics as (
    select
        repo_id,
        'lines of code' as metric_type,
        metric_id,
        metric_timestamp,
        user_id,
        sum(metric_count) over (
            partition by repo_id
            order by metric_timestamp
            rows between unbounded preceding and current row
        ) as metric_count
    from commit_metrics_combined
),

commit_metrics as (
    select
        repo_id,
        'commits' as metric_type,
        commit_id as metric_id,
        committed_at as metric_timestamp,
        author_id as user_id,
        count(commit_id) over (
            partition by repo_id
            order by metric_timestamp
            rows between unbounded preceding and current row
        ) as metric_count
    from  {{ ref('stg_commits') }}
),

combined as (
    select *
    from star_metrics
    union all
    select *
    from issue_metrics
    union all
    select *
    from fork_metrics
    union all
    select *
    from codebase_size_metrics
    union all
    select *
    from commit_metrics
    union all
    select *
    from pr_metrics
)

select -- noqa
    repo_id,
    metric_type,
    metric_timestamp,
    coalesce(metric_id, 'N/A') as metric_id,
    user_id::bigint as user_id,
    metric_count::bigint as metric_count
from combined
