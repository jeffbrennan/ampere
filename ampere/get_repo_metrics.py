import datetime
import json
import os
from pathlib import Path
from typing import Optional, TypeVar

import dotenv
import pandas as pd
import requests
from deltalake import DeltaTable, write_deltalake
from sqlmodel import SQLModel

from ampere.common import create_header

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


def get_token(secret_name: str) -> str:
    dotenv.load_dotenv()
    token = os.environ.get(secret_name)
    if token is None:
        raise ValueError()
    return token


def get_current_time() -> datetime.datetime:
    current_time = datetime.datetime.now()
    return datetime.datetime.strptime(current_time.isoformat(timespec='seconds'), "%Y-%m-%dT%H:%M:%S")


def get_repo_stars(owner_name: str, repo_name: str) -> list[StarInfo]:
    # https://docs.github.com/en/rest/activity/starring?apiVersion=2022-11-28
    print("getting stars...")
    url = (
        f"https://api.github.com/repos/{owner_name}/{repo_name}/stargazers?per_page=100"
    )
    headers = {
        "Accept": "application/vnd.github.star+json",
        "Authorization": f'Bearer {get_token("GITHUB_TOKEN")}',
        "X-GitHub-Api-Version": "2022-11-28",
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise ValueError(response.status_code)

    output = [
        StarInfo(
            user_id=result['user']["id"],
            user_name=result['user']["login"],
            user_avatar_link=result['user']["avatar_url"],
            starred_at=datetime.datetime.strptime(result['starred_at'], "%Y-%m-%dT%H:%M:%SZ"),
            retrieved_at=get_current_time(),
        )
        for result in json.loads(response.content)
    ]
    if "next" not in response.links:
        return output

    requests_finished = False
    max_pages = 10
    pages_checked = 1
    while not requests_finished and pages_checked < max_pages:
        print(f"n={len(output)}")
        response = requests.get(response.links["next"]["url"], headers=headers)
        if response.status_code != 200:
            raise ValueError(response.status_code)
        results = json.loads(response.content)
        for result in results:
            output.append(
                StarInfo(
                    user_id=result['user']["id"],
                    user_name=result['user']["login"],
                    user_avatar_link=result['user']["avatar_url"],
                    starred_at=datetime.datetime.strptime(result['starred_at'], "%Y-%m-%dT%H:%M:%SZ"),
                    retrieved_at=get_current_time()
                )
            )
        pages_checked += 1
        requests_finished = "next" not in response.links

    return output


def write_delta_table(records: list[SQLModelType], table_dir: str, table_name: str, pk: str) -> None:
    data_dir = Path(__file__).parents[1] / "data" / table_dir
    table_path = data_dir / table_name

    df = pd.DataFrame.from_records([vars(i) for i in records])
    delta_log_dir = table_path / "_delta_log"
    print(f"writing {len(records)} to {table_path}...")
    if not delta_log_dir.exists():
        table_path.mkdir(exist_ok=True, parents=True)
        write_deltalake(table_path, df, mode="error")
        return

    delta_table = DeltaTable(table_path)
    merge_results = (
        delta_table
        .merge(df,
               predicate=f"s.{pk} = t.{pk}",
               source_alias="s",
               target_alias="t", )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete()
        .execute()
    )
    print(merge_results)


def get_repos(org_name: str) -> list[Repo]:
    """
    get repos in org
    """
    url = (f"https://api.github.com/orgs/{org_name}/repos")

    headers = {
        "Accept": "application/vnd.github.star+json",
        "Authorization": f'Bearer {get_token("GITHUB_TOKEN")}',
        "X-GitHub-Api-Version": "2022-11-28",
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise ValueError(response.status_code)
    output = []
    results = json.loads(response.content)
    for result in results:
        output.append(
            Repo(
                repo_id=result['id'],
                repo_name=result['name'],
                license=result['license'],
                topics=result['topics'],
                language=result['language'],
                size=result['size'],
                forks_count=result['forks_count'],
                watchers_count=result['watchers_count'],
                stargazers_count=result['stargazers_count'],
                open_issues_count=result['open_issues_count'],
                pushed_at=datetime.datetime.strptime(result['pushed_at'], "%Y-%m-%dT%H:%M:%SZ"),
                created_at=datetime.datetime.strptime(result['created_at'], "%Y-%m-%dT%H:%M:%SZ"),
                updated_at=datetime.datetime.strptime(result['updated_at'], "%Y-%m-%dT%H:%M:%SZ"),
                retrieved_at=get_current_time(),
            )
        )
    return output


def main():
    owner_name = "mrpowers-io"
    repos = get_repos(owner_name)
    write_delta_table(repos, "bronze", "github_repos", "repo_id")
    repo_stars = []
    for i, repo in enumerate(repos, 1):
        header_text = f"[{i:02d}/{len(repos):02d}] {repo.repo_name}"
        print(create_header(80, header_text, True, '-'))

        results = get_repo_stars(owner_name, repo.repo_name)
        print(f"obtained {len(results)} stargazers")
        repo_stars.extend(results)

    write_delta_table(repo_stars, "bronze", "github_stars", "user_id")


if __name__ == "__main__":
    main()
