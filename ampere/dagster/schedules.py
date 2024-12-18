from dagster import AssetSelection, DefaultScheduleStatus, ScheduleDefinition

bigquery_schedule = ScheduleDefinition(
    name="bigquery_schedule",
    target=AssetSelection.groups("bigquery_daily"),
    cron_schedule="0 12 * * *",  # daily 7 am et
    default_status=DefaultScheduleStatus.RUNNING,
)
schedules = [bigquery_schedule]
