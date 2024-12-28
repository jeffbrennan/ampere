{{
    config(
        materialized='incremental',
        unique_key=[
          'user_id',
          'follower_id',
          'retrieved_at'
        ]
    )
}}
select
    user_id,
    follower_id,
    retrieved_at
from {{source('main', 'followers')}}
{% if is_incremental() %}
    where
       retrieved_at 
        > (select coalesce(max(retrieved_at), '1900-01-01') from {{ this }}) --noqa
{% endif %}

