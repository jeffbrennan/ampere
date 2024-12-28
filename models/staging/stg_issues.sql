{{
    config(
        materialized='incremental',
        unique_key=[
          'repo_id',
          'issue_id',
          'retrieved_at',
        ]
    )
}}
select
    repo_id,
    issue_id,
    issue_number,
    issue_title
    issue_body,
    author_id,
    state,
    state_reason,
    comments_count,
    created_at,
    updated_at,
    closed_at,
    retrieved_at
from {{ source('main', 'issues') }}
{% if is_incremental() %}
    where
       retrieved_at 
        > (select coalesce(max(retrieved_at), '1900-01-01') from {{ this }}) --noqa
{% endif %}
