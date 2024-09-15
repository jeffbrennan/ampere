import datetime
import json
import os
from pathlib import Path
from typing import Optional

import dotenv
import pandas as pd
import requests
from deltalake import DeltaTable
from sqlmodel import SQLModel


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
        print(f"obtained {len(output)} stars for {repo_name}")
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


def write_star_info(stars: list[StarInfo]) -> None:
    data_dir = Path(__file__).parents[1] / "data" / "bronze"
    table_path = data_dir / "github_stars"

    df = pd.DataFrame.from_records([vars(i) for i in stars])
    delta_log_dir = table_path / "_delta_log"
    if not delta_log_dir.exists():
        table_path.mkdir(exist_ok=True, parents=True)
        df.write_delta(table_path, mode="error")
        return

    delta_table = DeltaTable(table_path)
    merge_results = (
        delta_table
        .merge(df,
               predicate="s.user_id = t.user_id",
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
    repos = ["quinn"]
    repos = get_repos(owner_name)
    print(repos)
    # repo_stars = []
    # for i, repo in enumerate(repos, 1):
    #     print(f"{i}/{len(repos)} - {repo}")
    #
    #     results = get_repo_stars(owner_name, repo)
    #     print(f"obtained {len(results)} stars for {repo}")
    #     repo_stars.extend(results)

    # write_star_info(repo_stars)


if __name__ == "__main__":
    main()
