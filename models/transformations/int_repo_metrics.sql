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
                order by starred_at
                rows between unbounded preceding and current row
            ) as metric_count
        from stargazers a
    ),
    issue_metrics_open as (
        select
            repo_id,
            issue_id as metric_id,
            created_at as metric_timestamp,
            author_id as user_id,
            1 as metric_count
        from issues
        where created_at is not null
    ),
    issue_metrics_closed as (
        select
            repo_id,
            issue_id as metric_id,
            closed_at as metric_timestamp,
            author_id as user_id,
            -1 as metric_count
        from issues
        where closed_at is not null
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
            )
    ),
    pr_metrics_open as (
        select
            repo_id,
            pr_id as metric_id,
            created_at as metric_timestamp,
            author_id as user_id,
            1 as metric_count
        from pull_requests
        where created_at is not null
    ),
    pr_metrics_closed as (
        select
            repo_id,
            pr_id as metric_id,
            closed_at as metric_timestamp,
            author_id as user_id,
            -1 as metric_count
        from pull_requests
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
        from forks
    ),
    commit_metrics_added as (
        select
            repo_id,
            commit_id as metric_id,
            committed_at as metric_timestamp,
            author_id as user_id,
            additions_count as metric_count,
        from commits
    ),
    commit_metrics_deleted as (
        select
            repo_id,
            commit_id as metric_id,
            committed_at as metric_timestamp,
            author_id as user_id,
            deletions_count * -1 as metric_count,
        from commits
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
            metric_id,
            metric_timestamp,
            user_id,
            count(metric_id) over (
                partition by repo_id
                order by metric_timestamp
                rows between unbounded preceding and current row
            ) as metric_count
        from commit_metrics_combined
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
select *
from combined
