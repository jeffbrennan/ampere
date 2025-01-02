from dagster import (
    DefaultSensorStatus,
    RunFailureSensorContext,
    make_email_on_run_failure_sensor,
)
from dagster._core.execution.plan.objects import StepFailureData
from dagster._core.execution.stats import StepEventStatus

from ampere.common import get_current_time, get_secret


def generate_run_url(context: "RunFailureSensorContext") -> str:
    run_id = context.dagster_run.run_id
    run_url = f"{get_secret('AMPERE_BACKEND')}/runs/{run_id}"
    run_id_short = run_id.split("-")[0]

    return f'<h2> <a href="{run_url}">{run_id_short}</a> </h2>'


def generate_body_text(context: "RunFailureSensorContext") -> str:
    passes = 0
    pass_times = []

    run_url_str = generate_run_url(context)

    run_id = context.dagster_run.run_id
    run_step_stats = context.instance.get_run_step_stats(run_id)

    for step_stats in run_step_stats:
        if step_stats.status == StepEventStatus.SUCCESS:
            pass_times.extend([step_stats.start_time, step_stats.end_time])
            passes += 1

    pass_elapsed_time = max(pass_times) - min(pass_times)
    pass_line = f"{passes} steps passed in {pass_elapsed_time:.02f}s"

    fail_lines = []
    fails = context.get_step_failure_events()
    for fail in fails:
        event_data = fail.event_specific_data
        if not isinstance(event_data, StepFailureData):
            raise TypeError()

        error_message = str(event_data.error.cause).split("Stack Trace")[0]  # type: ignore
        fail_lines.append(
            "<br>".join([f"<h3> {fail.node_name} </h3>", error_message, "<hr>"])
        )

    output = "<br>".join(
        [
            run_url_str,
            "<hr> <h2> Passes </h2>",
            pass_line,
            "<hr> <h2> Fails </h2>",
            *fail_lines,
        ]
    )
    return output.replace("<br><br>", "<br>")


def create_email_alert_body(context: "RunFailureSensorContext") -> str:
    try:
        body_text = generate_body_text(context)
    except Exception as e:
        body_text = (
            f"SENSOR PARSING OF RUN FAILURE FAILED - {e}<br>{generate_run_url(context)}"
        )

    return body_text


def create_email_alert_subject(context: "RunFailureSensorContext") -> str:
    current_time_str = get_current_time().strftime("%Y-%m-%dT%H:%M:%S")

    schedule_name = context.dagster_run.tags.get(
        "dagster/schedule_name", "UNKNOWN SCHEDULE"
    )
    return f"{current_time_str} Ampere Dagster Run Failed: {schedule_name}"


email_on_run_failure = make_email_on_run_failure_sensor(
    email_from=get_secret("AMPERE_BACKEND_EMAIL_FROM"),
    email_password=get_secret("AMPERE_BACKEND_EMAIL_PW"),
    email_to=get_secret("AMPERE_BACKEND_EMAIL_LIST").split(","),
    monitor_all_code_locations=True,
    default_status=DefaultSensorStatus.RUNNING,
    email_body_fn=create_email_alert_body,
    email_subject_fn=create_email_alert_subject,
)
