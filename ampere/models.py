import datetime
from typing import Optional, TypeVar

from sqlmodel import SQLModel

SQLModelType = TypeVar("SQLModelType", bound=SQLModel)


class StarInfo(SQLModel):
    user_id: int
    user_name: str
    user_avatar_link: str
    starred_at: datetime.datetime
    retrieved_at: datetime.datetime


class Repo(SQLModel):
    repo_id: int
    repo_name: str
    license: Optional[dict[str, str]] = None
    topics: list[str]
    language: Optional[str] = None
    size: int
    forks_count: int
    watchers_count: int
    stargazers_count: int
    open_issues_count: int
    pushed_at: datetime.datetime
    created_at: datetime.datetime
    updated_at: datetime.datetime
    retrieved_at: datetime.datetime


class ForkInfo(SQLModel):
    owner_id: int
    owner_name: str
    fork_id: int
    retrieved_at: datetime.datetime
