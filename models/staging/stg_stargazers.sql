{{
    config(
        materialized='incremental',
        unique_key=[
          'repo_id',
          'user_id',
        ]
    )
}}
select
    repo_id,
    user_id,
    starred_at,
    retrieved_at
from {{ source('main', 'stargazers') }}
{% if is_incremental() %}
    where
       retrieved_at 
        > (select coalesce(max(retrieved_at), '1900-01-01') from {{ this }}) --noqa
{% endif %}
