with
    combined as (
        select
            ['summary', 'feed'] as page,
            'stg_commits' as "model",
            'retrieved_at' as timestamp_col,
            max(retrieved_at) as "timestamp",
            count(*) as records
        from {{ ref("stg_commits") }}
        union all

        select
            ['network_followers'] as page,
            'stg_followers' as "model",
            'retrieved_at' as timestamp_col,
            max(retrieved_at) as "timestamp",
            count(*) as records
        from {{ ref("stg_followers") }}
        union all

        select
            ['feed'] as page,
            'stg_forks' as "model",
            'retrieved_at' as timestamp_col,
            max(retrieved_at) as "timestamp",
            count(*) as records
        from {{ ref("stg_forks") }}
        union all

        select
            ['summary', 'feed', 'issues'] as page,
            'stg_issues' as "model",
            'retrieved_at' as timestamp_col,
            max(retrieved_at) as "timestamp",
            count(*) as records
        from {{ ref("stg_issues") }}
        union all

        select
            ['feed'] as page,
            'stg_pull_requests' as "model",
            'retrieved_at' as timestamp_col,
            max(retrieved_at) as "timestamp",
            count(*) as records
        from {{ ref("stg_pull_requests") }}
        union all

        select
            ['downloads'] as page,
            'stg_pypi_downloads' as "model",
            'retrieved_at' as timestamp_col,
            max(retrieved_at) as "timestamp",
            count(*) as records
        from {{ ref("stg_pypi_downloads") }}
        union all

        select
            null as page,
            'stg_releases' as "model",
            'retrieved_at' as timestamp_col,
            max(retrieved_at) as "timestamp",
            count(*) as records
        from {{ ref("stg_releases") }}
        union all

        select
            [
                'summary',
                'downloads',
                'feed',
                'issues',
                'network_followers',
                'network_stargazers'
            ] as page,
            'stg_repos' as "model",
            'retrieved_at' as timestamp_col,
            max(retrieved_at) as "timestamp",
            count(*) as records
        from {{ ref("stg_repos") }}
        union all

        select
            ['summary', 'feed', 'network_stargazers'] as page,
            'stg_stargazers' as "model",
            'retrieved_at' as timestamp_col,
            max(retrieved_at) as "timestamp",
            count(*) as records
        from {{ ref("stg_stargazers") }}
        union all

        select
            ['feed', 'issues', 'network_followers', 'network_stargazers'] as page,
            'stg_users' as "model",
            'retrieved_at' as timestamp_col,
            max(retrieved_at) as "timestamp",
            count(*) as records
        from {{ ref("stg_users") }}
        union all

        select
            ['downloads'] as page,
            'int_downloads_melted' as "model",
            'download_timestamp' as timestamp_col,
            max(download_timestamp) as "timestamp",
            count(*) as records
        from {{ ref("int_downloads_melted") }}
        union all

        select
            ['downloads'] as page,
            'int_downloads_melted_weekly' as "model",
            'download_timestamp' as timestamp_col,
            max(download_timestamp)::timestamp as "timestamp",
            count(*) as records
        from {{ ref("int_downloads_melted_weekly") }}
        union all

        select
            ['downloads'] as page,
            'int_downloads_summary' as "model",
            'download_date' as timestamp_col,
            max(download_date)::timestamp as "timestamp",
            count(*) as records
        from {{ ref("int_downloads_summary") }}
        union all

        select
            ['feed'] as page,
            'int_feed_events' as "model",
            'event_timestamp' as timestamp_col,
            max(event_timestamp) as "timestamp",
            count(*) as records
        from {{ ref("int_feed_events") }}
        union all

        select
            ['network_followers'] as page,
            'int_internal_followers' as "model",
            null as timestamp_col,
            null as "timestamp",
            count(*) as records
        from {{ ref("int_internal_followers") }}
        union all

        select
            ['network_followers'] as page,
            'int_network_follower_details' as "model",
            null as timestamp_col,
            null as "timestamp",
            count(*) as records
        from {{ ref("int_network_follower_details") }}
        union all

        select
            ['network_stargazers'] as page,
            'int_network_stargazers' as "model",
            'retrieved_at' as timestamp_col,
            max(retrieved_at) as "timestamp",
            count(*) as records
        from {{ ref("int_network_stargazers") }}
        union all

        select
            ['network_stargazers'] as page,
            'int_network_stargazers_pivoted' as "model",
            null as timestamp_col,
            null as "timestamp",
            count(*) as records
        from {{ ref("int_network_stargazers_pivoted") }}
        union all

        select
            ['summary'] as page,
            'int_repo_metrics' as "model",
            'metric_timestamp' as timestamp_col,
            max(metric_timestamp) as "timestamp",
            count(*) as records
        from {{ ref("int_repo_metrics") }}
        union all

        select
            null as page,
            'int_repo_metrics_changes' as "model",
            'metric_date' as timestamp_col,
            max(metric_date)::timestamp as "timestamp",
            count(*) as records
        from {{ ref("int_repo_metrics_changes") }}
        union all

        select
            ['summary'] as page,
            'int_repo_metrics_filled' as "model",
            'metric_date' as timestamp_col,
            max(metric_date)::timestamp as "timestamp",
            count(*) as records
        from {{ ref("int_repo_metrics_filled") }}
        union all

        select
            null as page,
            'int_repo_metrics_filled_partial' as "model",
            'metric_date' as timestamp_col,
            max(metric_date)::timestamp as "timestamp",
            count(*) as records
        from {{ ref("int_repo_metrics_filled_partial") }}
        union all

        select
            ['downloads'] as page,
            'mart_downloads_summary' as "model",
            'download_date' as timestamp_col,
            max(download_date)::timestamp as "timestamp",
            count(*) as records
        from {{ ref("mart_downloads_summary") }}
        union all

        select
            ['feed'] as page,
            'mart_feed_events' as "model",
            'event_timestamp' as timestamp_col,
            max(event_timestamp) as "timestamp",
            count(*) as records
        from {{ ref("mart_feed_events") }}
        union all

        select
            ['issues'] as page,
            'mart_issues' as "model",
            'date' as timestamp_col,
            max("date")::timestamp as "timestamp",
            count(*) as records
        from {{ ref("mart_issues") }}
        union all

        select
            ['issues'] as page,
            'mart_issues_summary' as "model",
            null as timestamp_col,
            null as "timestamp",
            count(*) as records
        from {{ ref("mart_issues_summary") }}
        union all

        select
            ['summary'] as page,
            'mart_repo_summary' as "model",
            'metric_date' as timestamp_col,
            max(metric_date)::timestamp as "timestamp",
            count(*) as records
        from {{ ref("mart_repo_summary") }}
        union all

        select
            ['network_stargazers'] as page,
            'mart_stargazers_pivoted' as "model",
            null as timestamp_col,
            null as "timestamp",
            count(*) as records
        from {{ ref("mart_stargazers_pivoted") }}
    )

select
    "model",
    page,
    timestamp_col,
    "timestamp",
    records
from combined
