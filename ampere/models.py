import datetime
from dataclasses import dataclass
from typing import Optional, TypeVar

from sqlmodel import Field, SQLModel

SQLModelType = TypeVar("SQLModelType", bound=SQLModel)


class Stargazer(SQLModel):
    __tablename__ = "stargazers"  # pyright: ignore [reportAssignmentType]
    repo_id: int = Field(primary_key=True, foreign_key="repo.repo_id")
    user_id: int = Field(primary_key=True, foreign_key="user.user_id")
    starred_at: datetime.datetime
    retrieved_at: datetime.datetime


class Watcher(SQLModel):
    __tablename__ = "watchers"  # pyright: ignore [reportAssignmentType]
    repo_id: int = Field(primary_key=True, foreign_key="repo.repo_id")
    user_id: int = Field(primary_key=True, foreign_key="user.user_id")
    retrieved_at: datetime.datetime


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
    watchers_count: int
    stargazers_count: int
    open_issues_count: int
    pushed_at: datetime.datetime
    created_at: datetime.datetime
    updated_at: datetime.datetime
    retrieved_at: datetime.datetime


class Fork(SQLModel):
    __tablename__ = "forks"  # pyright: ignore [reportAssignmentType]
    repo_id: int = Field(primary_key=True, foreign_key="repo.repo_id")
    fork_id: int = Field(primary_key=True)
    owner_id: int = Field(primary_key=True, foreign_key="user.user_id")
    created_at: datetime.datetime
    retrieved_at: datetime.datetime


# https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits
# https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#get-a-commit
@dataclass
class CommitStats:
    additions: int
    deletions: int


class Commit(SQLModel):
    __tablename__ = "commits"  # pyright: ignore [reportAssignmentType]
    repo_id: int = Field(primary_key=True, foreign_key="repo.repo_id")
    commit_id: str = Field(primary_key=True)
    author_id: Optional[int] = Field(foreign_key="user.user_id")
    comment_count: int
    message: str
    additions_count: int
    deletions_count: int
    committed_at: datetime.datetime
    retrieved_at: datetime.datetime


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
    retrieved_at: datetime.datetime


# https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-page-views
class View(SQLModel):
    __tablename__ = "views"  # pyright: ignore [reportAssignmentType]
    repo_id: int = Field(primary_key=True, foreign_key="repo.repo_id")
    view_count: int
    unique_view_count: int
    view_date: datetime.date
    retrieved_at: datetime.datetime


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
    retrieved_at: datetime.datetime


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
    retrieved_at: datetime.datetime


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
    retrieved_at: datetime.datetime


class Follower(SQLModel):
    __tablename__ = "followers"  # pyright: ignore [reportAssignmentType]
    user_id: int = Field(primary_key=True, foreign_key="users.user_id")
    follower_id: int = Field(primary_key=True, foreign_key="users.user_id")
    retrieved_at: datetime.datetime


# https://pypistats.org/api
# class PyPIDownload(SQLModel):
#     __tablename__ = "pypi_downloads"  # pyright: ignore [reportAssignmentType]
#     package: str = Field(primary_key=True, foreign_key="repo.repo_name")
#     type: str = Field(primary_key=True)
#     category: str = Field(primary_key=True)
#     date: str = Field(primary_key=True)
#     downloads: int
#     retrieved_at: datetime.datetime


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
