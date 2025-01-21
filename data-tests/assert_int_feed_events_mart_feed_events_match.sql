{{
    config(
        meta={
            'dagster': {
                'ref': {
                    'name': 'mart_feed_events',
                    'package_name': 'ampere'
                },
            }
        }
    )
}}
select
    event_type,
    max_int_timestamp,
    max_mart_timestamp
from {{ ref('test_int_feed_events_mart_feed_events_match') }}
