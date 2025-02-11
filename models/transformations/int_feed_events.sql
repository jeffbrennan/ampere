with star_events as (
    select
        'star' as event_type,
        'created' as event_action,
        starred_at as event_timestamp,
        repo_id,
        user_id,
        user_id as event_id,
        null as event_data
    from {{ ref('stg_stargazers') }}
),

commit_events as (
    select
        'commit' as event_type,
        'created' as event_action,
        committed_at as event_timestamp,
        repo_id,
        author_id as user_id,
        commit_id as event_id,
        message as event_data
    from {{ ref('stg_commits') }}
),

fork_events as (
    select
        'fork' as event_type,
        'created' as event_action,
        created_at as event_timestamp,
        repo_id,
        owner_id as user_id,
        fork_id as event_id,
        null as event_data
    from {{ ref('stg_forks') }}
),

issue_created_events as (
    select
        'issue' as event_type,
        'created' as event_action,
        created_at as event_timestamp,
        repo_id,
        author_id as user_id,
        issue_id as event_id,
        issue_title as event_data
    from {{ ref('stg_issues') }}
    where created_at is not null
),

issue_updated_events as (
    select
        'issue' as event_type,
        'updated' as event_action,
        updated_at as event_timestamp,
        repo_id,
        author_id as user_id,
        issue_id as event_id,
        issue_title as event_data
    from {{ ref('stg_issues') }}
    where updated_at is not null
),

issue_closed_events as (
    select
        'issue' as event_type,
        'closed' as event_action,
        closed_at as event_timestamp,
        repo_id,
        author_id as user_id,
        issue_id as event_id,
        issue_title as event_data
    from {{ ref('stg_issues') }}
    where closed_at is not null
),

pr_created_events as (
    select
        'pull request' as event_type,
        'created' as event_action,
        created_at as event_timestamp,
        repo_id,
        author_id as user_id,
        pr_id as event_id,
        pr_title as event_data
    from {{ ref('stg_pull_requests') }}
    where created_at is not null
),

pr_updated_events as (
    select
        'pull request' as event_type,
        'updated' as event_action,
        updated_at as event_timestamp,
        repo_id,
        author_id as user_id,
        pr_id as event_id,
        pr_title as event_data
    from {{ ref('stg_pull_requests') }}
    where updated_at is not null
),

pr_closed_events as (
    select
        'pull request' as event_type,
        'closed' as event_action,
        closed_at as event_timestamp,
        repo_id,
        author_id as user_id,
        pr_id as event_id,
        pr_title as event_data
    from {{ ref('stg_pull_requests') }}
    where closed_at is not null
),

pr_merged_events as (
    select
        'pull request' as event_type,
        'merged' as event_action,
        merged_at as event_timestamp,
        repo_id,
        author_id as user_id,
        pr_id as event_id,
        pr_title as event_data
    from {{ ref('stg_pull_requests') }}
    where merged_at is not null
),

combined as (
    select * from star_events
    union all
    select * from commit_events
    union all
    select * from fork_events
    union all
    select * from issue_created_events
    union all
    select * from issue_updated_events
    union all
    select * from issue_closed_events
    union all
    select * from pr_created_events
    union all
    select * from pr_updated_events
    union all
    select * from pr_closed_events
    union all
    select * from pr_merged_events
)

select
    event_type,
    event_action,
    event_timestamp,
    repo_id,
    user_id::bigint as user_id,
    event_id,
    event_data
from combined
