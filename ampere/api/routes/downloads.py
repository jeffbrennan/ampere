from fastapi import APIRouter, Query, Request
from pydantic import TypeAdapter

from ampere.api.limiter import limiter
from ampere.cli.common import CLIEnvironment
from ampere.common import get_frontend_db_con
from ampere.models import (
    DownloadPublic,
    DownloadsGranularity,
    DownloadsPublic,
    DownloadsPublicGroup,
    GetDownloadsPublicConfig,
    ReposWithDownloads,
    create_repo_enum,
)

router = APIRouter(prefix="/downloads", tags=["downloads"])


def get_downloads_base(
    table_name: str, config: GetDownloadsPublicConfig
) -> DownloadsPublic:
    con = get_frontend_db_con()
    valid_repos = create_repo_enum(CLIEnvironment.dev, True)
    try:
        repo = valid_repos(config.repo)  # type: ignore
    except ValueError:
        raise ValueError(f"Invalid repo: {config.repo}")

    params = [repo.value, config.group]
    sort_order = "desc" if config.descending else "asc"

    query = f"""
    select * 
    from {table_name}
    where repo = ?
    and group_name = ?
    and download_timestamp >= now() - interval {config.n_days} days
    order by download_timestamp {sort_order}, download_count {sort_order}
    limit {config.limit}
    """

    result = con.sql(query, params=params).to_df().to_dict(orient="records")
    ta = TypeAdapter(list[DownloadPublic])
    return DownloadsPublic(data=ta.validate_python(result), count=len(result))


@router.get("/hourly", response_model=DownloadsPublic)
@limiter.limit("60/minute")
def read_downloads_hourly(
    request: Request,
    repo: str,
    group: DownloadsPublicGroup = DownloadsPublicGroup.overall,
    n_days: int = Query(default=7, le=24 * 365 * 5),
    limit: int = Query(default=7 * 24, le=100_000),
    descending: bool = Query(default=True),
) -> DownloadsPublic:
    return get_downloads_base(
        table_name="int_downloads_melted",
        config=GetDownloadsPublicConfig(
            granularity=DownloadsGranularity.hourly,
            repo=repo,
            group=group,
            n_days=n_days,
            limit=limit,
            descending=descending,
        ),
    )


@router.get("/daily", response_model=DownloadsPublic)
@limiter.limit("60/minute")
def read_downloads_daily(
    request: Request,
    repo: str,
    group: DownloadsPublicGroup = DownloadsPublicGroup.overall,
    n_days: int = Query(default=30, le=365 * 5),
    limit: int = Query(default=100, le=100_000),
    descending: bool = Query(default=True),
) -> DownloadsPublic:
    return get_downloads_base(
        table_name="int_downloads_melted_daily",
        config=GetDownloadsPublicConfig(
            granularity=DownloadsGranularity.daily,
            repo=repo,
            group=group,
            n_days=n_days,
            limit=limit,
            descending=descending,
        ),
    )


@router.get("/weekly", response_model=DownloadsPublic)
@limiter.limit("60/minute")
def read_downloads_weekly(
    request: Request,
    repo: str,
    group: DownloadsPublicGroup = DownloadsPublicGroup.overall,
    n_days: int = Query(default=30, le=365 * 5),
    limit: int = Query(default=100, le=10_000),
    descending: bool = Query(default=True),
) -> DownloadsPublic:
    return get_downloads_base(
        table_name="int_downloads_melted_weekly",
        config=GetDownloadsPublicConfig(
            granularity=DownloadsGranularity.weekly,
            repo=repo,
            group=group,
            n_days=n_days,
            limit=limit,
            descending=descending,
        ),
    )


@router.get("/monthly", response_model=DownloadsPublic)
@limiter.limit("60/minute")
def read_downloads_monthly(
    request: Request,
    repo: str,
    group: DownloadsPublicGroup = DownloadsPublicGroup.overall,
    n_days: int = Query(default=60, le=365 * 5),
    limit: int = Query(default=100, le=10_000),
    descending: bool = Query(default=True),
) -> DownloadsPublic:
    return get_downloads_base(
        table_name="int_downloads_melted_monthly",
        config=GetDownloadsPublicConfig(
            granularity=DownloadsGranularity.monthly,
            repo=repo,
            group=group,
            n_days=n_days,
            limit=limit,
            descending=descending,
        ),
    )


@router.get("/repos", response_model=ReposWithDownloads)
@limiter.limit("60/minute")
def read_repos_with_downloads(request: Request) -> ReposWithDownloads:
    con = get_frontend_db_con()
    query = """
        select distinct a.repo 
        from mart_downloads_summary a 
        left join stg_repos b on a.repo = b.repo_name
        order by b.stargazers_count desc, a.repo
    """
    repos = con.sql(query).to_df().squeeze().tolist()
    return ReposWithDownloads(repos=repos, count=len(repos))
