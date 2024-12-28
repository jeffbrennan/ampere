{{
    config(
        materialized='incremental',
        unique_key=[
          'repo_id',
          'commit_id',
          'retrieved_at'
        ]
    )
}}
select
    repo_id,
    commit_id,
    author_id::bigint as author_id,
    comment_count,
    message,
    stats,
    committed_at,
    retrieved_at
from {{ source('main', 'commits') }}
{% if is_incremental() %}
    where
        retrieved_at
        > (select coalesce(max(retrieved_at), '1900-01-01') from {{ this }}) --noqa
{% endif %}
