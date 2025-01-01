from dagster import AssetSelection, DefaultScheduleStatus, ScheduleDefinition

# split because of 5,000 requests per hour limit
# https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2022-11-28#primary-rate-limit-for-authenticated-users
github_metrics_daily_4 = ScheduleDefinition(
    name="github_metrics_daily_4",
    target=AssetSelection.groups("github_metrics_daily_4"),
    cron_schedule="0 0,6,12,18 * * *",  # every 6 hours, starting at midnight
    default_status=DefaultScheduleStatus.RUNNING,
)

bigquery_daily = ScheduleDefinition(
    name="bigquery_daily",
    target=AssetSelection.groups("bigquery_daily"),
    cron_schedule="0 10 * * *",  # daily 10am utc
    default_status=DefaultScheduleStatus.STOPPED,
)
schedules = [
    github_metrics_daily_4,
    github_metrics_daily_4,
    bigquery_daily,
]
