{{
    config(
        materialized='incremental',
        unique_key=[
          'repo_id',
          'pr_id',
          'retrieved_at',
        ]
    )
}}
select
    repo_id,
    pr_id,
    pr_number,
    pr_title,
    pr_state,
    pr_body,
    author_id,
    created_at,
    updated_at,
    closed_at,
    merged_at,
    retrieved_at
from {{ source('main', 'pull_requests') }}
{% if is_incremental() %}
    where
       retrieved_at 
        > (select coalesce(max(retrieved_at), '1900-01-01') from {{ this }}) --noqa
{% endif %}
