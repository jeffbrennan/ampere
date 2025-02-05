import datetime
from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Optional, TypeVar

from sqlmodel import Field, SQLModel

from ampere.cli.common import CLIEnvironment
from ampere.viz import get_repos_with_downloads

SQLModelType = TypeVar("SQLModelType", bound=SQLModel)


class Stargazer(SQLModel):
    __tablename__ = "stargazers"  # pyright: ignore [reportAssignmentType]
    repo_id: int = Field(primary_key=True, foreign_key="repo.repo_id")
    user_id: int = Field(primary_key=True, foreign_key="user.user_id")
    starred_at: datetime.datetime
    retrieved_at: datetime.datetime = Field(primary_key=True)


@dataclass
class Language:
    name: str
    size_bytes: int


class Repo(SQLModel):
    __tablename__ = "repos"  # pyright: ignore [reportAssignmentType]
    repo_id: int = Field(primary_key=True)
    repo_name: str
    license: Optional[str] = None
    topics: list[str]
    language: Optional[list[Language]] = None
    repo_size: int
    forks_count: int
    stargazers_count: int
    open_issues_count: int
    pushed_at: datetime.datetime
    created_at: datetime.datetime
    updated_at: datetime.datetime
    retrieved_at: datetime.datetime = Field(primary_key=True)


class Fork(SQLModel):
    __tablename__ = "forks"  # pyright: ignore [reportAssignmentType]
    repo_id: int = Field(primary_key=True, foreign_key="repo.repo_id")
    fork_id: int = Field(primary_key=True)
    owner_id: int = Field(primary_key=True, foreign_key="user.user_id")
    created_at: datetime.datetime
    retrieved_at: datetime.datetime = Field(primary_key=True)


# https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits
# https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#get-a-commit
@dataclass
class CommitStats:
    filename: str
    additions: int
    deletions: int
    changes: int
    status: str


class Commit(SQLModel):
    __tablename__ = "commits"  # pyright: ignore [reportAssignmentType]
    repo_id: int = Field(primary_key=True, foreign_key="repo.repo_id")
    commit_id: str = Field(primary_key=True)
    author_id: Optional[int] = Field(foreign_key="user.user_id")
    comment_count: int
    message: str
    stats: list[CommitStats]
    committed_at: datetime.datetime
    retrieved_at: datetime.datetime = Field(primary_key=True)


# https://docs.github.com/en/rest/issues/issues?apiVersion=2022-11-28#list-repository-issues
class Issue(SQLModel):
    __tablename__ = "issues"  # pyright: ignore [reportAssignmentType]
    repo_id: int = Field(primary_key=True, foreign_key="repo.repo_id")
    issue_id: int = Field(primary_key=True)
    issue_number: int
    issue_title: str
    issue_body: Optional[str] = None
    author_id: int = Field(foreign_key="user.user_id")
    state: str
    state_reason: Optional[str] = None
    comments_count: int
    created_at: datetime.datetime
    updated_at: Optional[datetime.datetime] = None
    closed_at: Optional[datetime.datetime] = None
    retrieved_at: datetime.datetime = Field(primary_key=True)


# https://docs.github.com/en/rest/pulls/pulls?apiVersion=2022-11-28#list-pull-requests
class PullRequest(SQLModel):
    __tablename__ = "pull_requests"  # pyright: ignore [reportAssignmentType]
    repo_id: int = Field(primary_key=True, foreign_key="repo.repo_id")
    pr_id: int = Field(primary_key=True)
    pr_number: int
    pr_title: str
    pr_state: str
    pr_body: Optional[str] = None
    author_id: int = Field(foreign_key="user.user_id")
    created_at: datetime.datetime
    updated_at: datetime.datetime
    closed_at: Optional[datetime.datetime] = None
    merged_at: Optional[datetime.datetime] = None
    retrieved_at: datetime.datetime = Field(primary_key=True)


# https://docs.github.com/en/rest/users/users?apiVersion=2022-11-28#get-a-user
class User(SQLModel):
    __tablename__ = "users"  # pyright: ignore [reportAssignmentType]
    user_id: int = Field(primary_key=True)
    user_name: str
    full_name: Optional[str] = None
    company: Optional[str] = None
    avatar_url: str
    repos_count: int
    followers_count: int
    following_count: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    retrieved_at: datetime.datetime = Field(primary_key=True)


# https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28#list-releases
class Release(SQLModel):
    __tablename__ = "releases"  # pyright: ignore [reportAssignmentType]
    repo_id: int = Field(primary_key=True, foreign_key="repo.repo_id")
    release_id: int = Field(primary_key=True)
    release_name: str
    tag_name: str
    release_body: Optional[str] = None
    created_at: datetime.datetime
    published_at: datetime.datetime
    retrieved_at: datetime.datetime = Field(primary_key=True)


class Follower(SQLModel):
    __tablename__ = "followers"  # pyright: ignore [reportAssignmentType]
    user_id: int = Field(primary_key=True, foreign_key="users.user_id")
    follower_id: int = Field(primary_key=True, foreign_key="users.user_id")
    retrieved_at: datetime.datetime = Field(primary_key=True)


class PyPIDownload(SQLModel):
    __tablename__ = "pypi_downloads"  # pyright: ignore [reportAssignmentType]
    project: str = Field(primary_key=True, foreign_key="repo.repo_name")
    timestamp: datetime.datetime = Field(primary_key=True)
    country_code: str = Field(primary_key=True)
    package_version: str = Field(primary_key=True)
    python_version: str = Field(primary_key=True)
    system_distro_name: str = Field(primary_key=True)
    system_distro_version: str = Field(primary_key=True)
    system_name: str = Field(primary_key=True)
    system_release: str = Field(primary_key=True)
    download_count: int
    retrieved_at: datetime.datetime


# used to track which repos have been queried to prevent repeated date range queries on repos with no downloads
@dataclass
class PyPIQueryConfig(SQLModel):
    __tablename__ = "pypi_download_queries"  # pyright: ignore [reportAssignmentType]
    repo: str = Field(primary_key=True, foreign_key="repo.repo_name")
    retrieved_at: datetime.datetime = Field(primary_key=True)
    min_date: str
    max_date: Optional[str]


# viz model dataclases
@dataclass(slots=True, frozen=True)
class StargazerNetworkRecord:
    user_name: str
    followers_count: int
    starred_at: datetime.datetime
    retrieved_at: datetime.datetime
    repo_name: str


@dataclass(slots=True, frozen=True)
class Followers:
    user_id: str
    follower_id: int


@dataclass(slots=True, frozen=True)
class FollowerDetails:
    user_id: int
    user_name: str
    followers_count: int
    following_count: int
    followers: Optional[list[str]]
    following: Optional[list[str]]
    internal_followers_count: int
    internal_following_count: int
    internal_followers_pct: float
    internal_following_pct: float


# cli models


def create_repo_enum(env: CLIEnvironment = CLIEnvironment.prod) -> StrEnum:
    repos = get_repos_with_downloads(env)
    return StrEnum("RepoEnum", {repo: repo for repo in repos})




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
    repo: create_repo_enum(CLIEnvironment.dev)  # type: ignore
    group: DownloadsPublicGroup
    n_days: int
    limit: int
    descending: bool


class DownloadPublic(SQLModel):
    repo: str
    download_timestamp: datetime.datetime
    group_name: str
    group_value: str
    download_count: int


class DownloadsPublic(SQLModel):
    data: list[DownloadPublic]
    count: int


class ReposWithDownloads(SQLModel):
    repos: list[str]
    count: int
