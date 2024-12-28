{{
    config(
        materialized='incremental',
        unique_key=[
          'repo_id',
          'retrieved_at',
        ]
    )
}}
select
    repo_id,
    repo_name,
    license,
    topics,
    language,
    repo_size,
    forks_count,
    stargazers_count,
    open_issues_count,
    pushed_at,
    created_at,
    updated_at,
    retrieved_at
from {{ source('main', 'repos') }}
{% if is_incremental() %}
    where
       retrieved_at 
        > (select coalesce(max(retrieved_at), '1900-01-01') from {{ this }}) --noqa
{% endif %}
