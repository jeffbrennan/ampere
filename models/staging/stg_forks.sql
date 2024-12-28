{{
    config(
        materialized='incremental',
        unique_key=[
          'repo_id',
          'fork_id',
          'owner_id',
          'retrieved_at',
        ]
    )
}}
select
    repo_id,
    fork_id,
    owner_id,
    created_at,
    retrieved_at
from {{ source('main', 'forks') }}
{% if is_incremental() %}
    where
       retrieved_at 
        > (select coalesce(max(retrieved_at), '1900-01-01') from {{ this }}) --noqa
{% endif %}
