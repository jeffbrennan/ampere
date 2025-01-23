from dataclasses import dataclass
from enum import StrEnum, auto
from functools import cache

from fastapi import APIRouter, Query, Request
from pydantic import TypeAdapter

from ampere.api.limiter import limiter
from ampere.api.models import DownloadPublic, DownloadsPublic
from ampere.common import get_frontend_db_con
from ampere.viz import get_repos_with_downloads

router = APIRouter(prefix="/downloads", tags=["downloads"])


@cache
def create_repo_enum() -> StrEnum:
    repos = get_repos_with_downloads()
    return StrEnum("RepoEnum", {repo: repo for repo in repos})


RepoEnum = create_repo_enum()


class DownloadsPublicGroup(StrEnum):
    overall = auto()
    country_code = auto()
    package_version = auto()
    python_version = auto()
    system_distro_name = auto()
    system_distro_version = auto()
    system_name = auto()
    system_release = auto()


class DownloadsGranularity(StrEnum):
    hourly = auto()
    daily = auto()
    weekly = auto()
    monthly = auto()


class DownloadsSummaryGranularity(StrEnum):
    weekly = auto()
    monthly = auto()


@dataclass
class GetDownloadsPublicConfig:
    granularity: DownloadsGranularity | DownloadsSummaryGranularity
    repo: RepoEnum  # type: ignore
    group: DownloadsPublicGroup
    n_days: int
    limit: int
    descending: bool


def get_downloads_base(
    table_name: str, config: GetDownloadsPublicConfig
) -> DownloadsPublic:
    con = get_frontend_db_con()
    params = [config.repo.value, config.group]
    query = f"""
    select * 
    from {table_name}
    where repo = ?
    and group_name = ?
    and download_timestamp >= now() - interval {config.n_days} days
    order by download_timestamp {'desc' if config.descending else 'asc'}
    limit {config.limit}
    """

    result = con.sql(query, params=params).to_df().to_dict(orient="records")
    ta = TypeAdapter(list[DownloadPublic])
    return DownloadsPublic(data=ta.validate_python(result), count=len(result))


@router.get("/hourly", response_model=DownloadsPublic)
@limiter.limit("6/minute")
def read_downloads_hourly(
    request: Request,
    repo: RepoEnum,  # type: ignore
    group: DownloadsPublicGroup = DownloadsPublicGroup.overall,
    n_days: int = Query(default=7, le=30),
    limit: int = Query(default=7 * 24, le=7 * 30),
    descending: bool = Query(default=True),
):
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
@limiter.limit("6/minute")
def read_downloads_daily(
    request: Request,
    repo: RepoEnum,  # type: ignore
    group: DownloadsPublicGroup = DownloadsPublicGroup.overall,
    n_days: int = Query(default=30, le=365),
    limit: int = Query(default=100, le=365),
    descending: bool = Query(default=True),
):
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
@limiter.limit("6/minute")
def read_downloads_weekly(
    request: Request,
    repo: RepoEnum,  # type: ignore
    group: DownloadsPublicGroup = DownloadsPublicGroup.overall,
    n_days: int = Query(default=30, le=365),
    limit: int = Query(default=100, le=365),
    descending: bool = Query(default=True),
):
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
@limiter.limit("6/minute")
def read_downloads_monthly(
    request: Request,
    repo: RepoEnum,  # type: ignore
    group: DownloadsPublicGroup = DownloadsPublicGroup.overall,
    n_days: int = Query(default=6, le=12 * 10),
    limit: int = Query(default=100, le=12 * 10),
    descending: bool = Query(default=True),
):
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
