{{
    config(
        meta={
            'dagster': {
                'ref': {
                    'name': 'int_downloads_melted_monthly',
                    'package_name': 'ampere'
                },
            }
        }
    )
}}
select * from 
{{ ref('test_downloads_summary_not_stale') }} 
where monthly_days_diff >= 1
