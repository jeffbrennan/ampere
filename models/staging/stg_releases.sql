{{
    config(
        materialized='incremental',
        unique_key=[
          'repo_id',
          'release_id'
          'retrieved_at',
        ]
    )
}}
select
    repo_id,
    release_id,
    release_name,
    tag_name,
    release_body,
    created_at,
    published_at,
    retrieved_at
from {{ source('main', 'releases') }}
{% if is_incremental() %}
    where
       retrieved_at 
        > (select coalesce(max(retrieved_at), '1900-01-01') from {{ this }}) --noqa
{% endif %}
