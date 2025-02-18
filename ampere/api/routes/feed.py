import datetime
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.exceptions import RequestValidationError
from pydantic import TypeAdapter

from ampere.api.limiter import limiter
from ampere.cli.common import CLIEnvironment
from ampere.common import get_frontend_db_con
from ampere.models import (
    FeedBounds,
    FeedPublic,
    FeedPublicAction,
    FeedPublicEvent,
    FeedPublicRecord,
    create_repo_enum,
)

router = APIRouter(prefix="/feed", tags=["feed"])


@dataclass
class FeedGroups:
    repo: str | None = None
    event: str | None = None
    action: str | None = None
    username: str | None = None


def apply_group_filters(
    base_query: str,
    params: list[Any],
    groups: FeedGroups,
) -> tuple[str, list[Any]]:
    if groups.repo is not None:
        valid_repos = create_repo_enum(CLIEnvironment.dev, False)
        try:
            repo_validated = valid_repos(groups.repo)  # type: ignore
        except ValueError:
            raise ValueError(f"Invalid repo: {groups.repo}")

        base_query += " and repo_name = ?"
        params.append(repo_validated.value)

    if groups.event is not None:
        base_query += " and event_type = ?"
        params.append(groups.event)

    if groups.action is not None:
        base_query += " and event_action = ?"
        params.append(groups.action)

    if groups.username is not None:
        base_query += " and lower(user_name) = ?"
        params.append(groups.username)

    return base_query, params


@router.get("/bounds", response_model=FeedBounds)
@limiter.limit("60/minute")
def get_feed_bounds(
    request: Request,
    repo: str | None = None,
    event: FeedPublicEvent | None = None,
    action: FeedPublicAction | None = None,
    username: str | None = None,
):
    con = get_frontend_db_con()

    repo_name = "'overall'" if repo is None else "repo_name"
    event_type = "'overall'" if event is None else "event_type"
    event_action = "'overall'" if action is None else "event_action"
    user_name = "'overall'" if username is None else "user_name"

    base_query = f"""
    select
        {repo_name} as repo,
        {event_type} as event,
        {event_action} as action,
        {user_name} as username,
        min(event_timestamp) as min_date, 
        max(event_timestamp) as max_date 
    from mart_feed_events
    where 1 = 1
    """
    params = []
    base_query, params = apply_group_filters(
        base_query,
        params,
        FeedGroups(repo, event, action, username),
    )

    base_query += "group by all"
    print(base_query)
    result = con.sql(base_query, params=params).to_df().to_dict(orient="records")
    return FeedBounds.model_validate(result[0])


@router.get("/list", response_model=FeedPublic)
@limiter.limit("60/minute")
def read_feed(
    request: Request,
    repo: str | None = None,
    event: FeedPublicEvent | None = None,
    action: FeedPublicAction | None = None,
    username: str | None = None,
    n_days: int | None = Query(default=60, le=9999),
    min_date: str | None = Query(None, regex=r"\d{4}-\d{2}-\d{2}", format="date"),
    max_date: str | None = Query(None, regex=r"\d{4}-\d{2}-\d{2}", format="date"),
    limit: int = Query(default=50, le=10_000),
    descending: bool = Query(default=True),
) -> FeedPublic:
    con = get_frontend_db_con()
    sort_order = "desc" if descending else "asc"
    params = []

    if any([min_date, max_date]):
        n_days = None

    base_query = """
    select * from mart_feed_events
    where 1 = 1
    """

    bounds = get_feed_bounds(request, repo, event, action)
    if n_days is not None:
        requested_min_date = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(days=n_days)

        if requested_min_date < bounds.min_date:
            maximum_n_days = (
                datetime.datetime.now(datetime.timezone.utc) - bounds.min_date
            ).days
            err = ""
            err += f"requested min date {requested_min_date.strftime('%Y-%m-%d')} [{n_days} days] < "
            err += f"available min date {bounds.min_date.strftime('%Y-%m-%d')} | "
            err += f"please use a value <= {maximum_n_days} for `n_days`"
            raise RequestValidationError(err)
        base_query += f"and event_timestamp >= now() - interval {n_days} days"

    if min_date is not None:
        try:
            min_date_dt = datetime.datetime.strptime(min_date, "%Y-%m-%d").astimezone(
                datetime.timezone.utc
            )
        except ValueError:
            raise RequestValidationError("min_date must be in the format YYYY-MM-DD")

        if min_date_dt < bounds.min_date:
            raise RequestValidationError(f"min_date must be after {bounds.min_date}")
        print(min_date)
        base_query += f" and event_timestamp >= date '{min_date}'"

    if max_date is not None:
        try:
            max_date_dt = datetime.datetime.strptime(max_date, "%Y-%m-%d").astimezone(
                datetime.timezone.utc
            )
        except ValueError:
            raise RequestValidationError("max_date must be in the format YYYY-MM-DD")

        if max_date_dt > bounds.max_date:
            raise RequestValidationError(f"max_date must be before {bounds.max_date}")
        base_query += f" and event_timestamp <= date '{max_date}'"

    base_query, params = apply_group_filters(
        base_query,
        params,
        FeedGroups(repo, event, action, username),
    )

    base_query += f"""
    order by event_timestamp {sort_order}
    limit {limit}
    """

    print(base_query, params)

    result = con.sql(base_query, params=params).to_df().to_dict(orient="records")
    ta = TypeAdapter(list[FeedPublicRecord])
    return FeedPublic(data=ta.validate_python(result), count=len(result))
