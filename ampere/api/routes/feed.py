from fastapi import APIRouter, Query, Request
from pydantic import TypeAdapter

from ampere.api.limiter import limiter
from ampere.cli.common import CLIEnvironment
from ampere.common import get_frontend_db_con
from ampere.models import (
    FeedPublic,
    FeedPublicAction,
    FeedPublicEvent,
    FeedPublicRecord,
    create_repo_enum,
)

router = APIRouter(prefix="/feed", tags=["feed"])


@router.get("/list", response_model=FeedPublic)
@limiter.limit("60/minute")
def read_feed(
    request: Request,
    repo: str | None = None,
    event: FeedPublicEvent | None = None,
    action: FeedPublicAction | None = None,
    username: str | None = None,
    n_days: int = Query(default=60, le=24 * 365 * 5),
    limit: int = Query(default=1_000, le=10_000),
    descending: bool = Query(default=True),
) -> FeedPublic:
    con = get_frontend_db_con()
    sort_order = "desc" if descending else "asc"
    params = []

    base_query = f"""
    select * from mart_feed_events
    where event_timestamp >= now() - interval {n_days} days
    """

    if repo is not None:
        valid_repos = create_repo_enum(CLIEnvironment.dev)
        if repo not in valid_repos:
            raise ValueError(f"Invalid repo: {repo}")
        base_query += " and repo_name = ?"
        params.append(repo)

    if event is not None:
        base_query += " and event_type = ?"
        params.append(event)

    if action is not None:
        base_query += " and event_action = ?"
        params.append(action)

    if username is not None:
        base_query += " and lower(user_name) = ?"
        params.append(username)

    base_query += f"""
    order by event_timestamp {sort_order}
    limit {limit}
    """

    print(base_query, params)

    result = con.sql(base_query, params=params).to_df().to_dict(orient="records")
    ta = TypeAdapter(list[FeedPublicRecord])
    return FeedPublic(data=ta.validate_python(result), count=len(result))
