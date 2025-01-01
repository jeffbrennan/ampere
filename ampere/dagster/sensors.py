from dagster import (
    DefaultSensorStatus,
    make_email_on_run_failure_sensor,
)

from ampere.common import get_secret

email_on_run_failure = make_email_on_run_failure_sensor(
    email_from=get_secret("AMPERE_BACKEND_EMAIL_FROM"),
    email_password=get_secret("AMPERE_BACKEND_EMAIL_PW"),
    email_to=get_secret("AMPERE_BACKEND_EMAIL_LIST").split(","),
    monitor_all_code_locations=True,
    default_status=DefaultSensorStatus.RUNNING,
)
