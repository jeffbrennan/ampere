with
date_spine as (
        {{
            dbt_utils.date_spine(
                datepart="day",
                start_date="(select min(metric_timestamp) from "
                + ref("int_repo_metrics")
                | string + ")",
                end_date="(select max(metric_timestamp) from "
                + ref("int_repo_metrics")
                | string + ")",
            )
        }}
),

date_spine_clean as (select cast(date_day as date) as spine_date from date_spine
)

select *
from date_spine_clean
