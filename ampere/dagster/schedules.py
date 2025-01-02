from dagster import DefaultScheduleStatus, ScheduleDefinition

from .jobs import bigquery_daily_job, github_metrics_daily_4_job

github_metrics_daily_4 = ScheduleDefinition(
    name="github_metrics_daily_4",
    job=github_metrics_daily_4_job,
    cron_schedule="0 0,6,12,18 * * *",  # every 6 hours, starting at midnight
    default_status=DefaultScheduleStatus.RUNNING,
)

bigquery_daily = ScheduleDefinition(
    name="bigquery_daily",
    job=bigquery_daily_job,
    cron_schedule="0 10 * * *",  # daily 10am utc
    default_status=DefaultScheduleStatus.RUNNING,
)
schedules = [github_metrics_daily_4, bigquery_daily]
