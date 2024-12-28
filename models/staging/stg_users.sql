{{
    config(
        materialized='incremental',
        unique_key=[
          'user_id',
          'retrieved_at',
        ]
    )
}}
select
    user_id,
    user_name,
    full_name,
    company,
    avatar_url,
    repos_count,
    followers_count,
    following_count,
    created_at,
    updated_at,
    retrieved_at
from {{ source('main', 'users') }}
{% if is_incremental() %}
    where
       retrieved_at 
        > (select coalesce(max(retrieved_at), '1900-01-01') from {{ this }}) --noqa
{% endif %}
