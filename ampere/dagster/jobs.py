from dagster import AssetSelection, define_asset_job

github_metrics_daily_4_job = define_asset_job(
    name="github_metrics_daily_4",
    selection=AssetSelection.groups("github_metrics_daily_4"),
)

bigquery_daily_job = define_asset_job(
    name="bigquery_daily", selection=AssetSelection.groups("bigquery_daily")
)
