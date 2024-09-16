import datetime
import json
from typing import Callable

import requests

from ampere.common import (
    create_header,
    get_token,
    get_current_time,
    write_delta_table,
    DeltaWriteConfig,
)
from ampere.models import Repo, Fork, Stargazer, Release, Language


def get_forks(owner_name: str, repo_name: str) -> list[Fork]:
    print("getting forks...")
    # https://docs.github.com/en/rest/activity/starring?apiVersion=2022-11-28
    url = f"https://api.github.com/repos/{owner_name}/{repo_name}/forks?per_page=100"
    headers = {
        "Accept": "application/vnd.github.star+json",
        "Authorization": f'Bearer {get_token("GITHUB_TOKEN")}',
        "X-GitHub-Api-Version": "2022-11-28",
    }

    output = []
    requests_finished = False
    max_pages = 10
    pages_checked = 0

    response = requests.get(url, headers=headers)
    while not requests_finished and pages_checked < max_pages:
        if response.status_code != 200:
            raise ValueError(response.status_code)
        results = json.loads(response.content)

        for result in results:
            output.append(
                Fork(
                    fork_id=result["id"],
                    owner_id=result["owner"]["id"],
                    retrieved_at=get_current_time(),
                )
            )

        pages_checked += 1
        print(f"n={len(output)}")
        requests_finished = "next" not in response.links
        if requests_finished:
            break

        response = requests.get(response.links["next"]["url"], headers=headers)

    return output


def get_stargazers(owner_name: str, repo_name: str) -> list[Stargazer]:
    # https://docs.github.com/en/rest/activity/starring?apiVersion=2022-11-28
    print("getting stargazers...")
    url = f"https://api.github.com/repos/{owner_name}/{repo_name}/stargazers?per_page=100"
    headers = {
        "Accept": "application/vnd.github.star+json",
        "Authorization": f'Bearer {get_token("GITHUB_TOKEN")}',
        "X-GitHub-Api-Version": "2022-11-28",
    }

    response = requests.get(url, headers=headers)

    output = []
    requests_finished = False
    max_pages = 10
    pages_checked = 0

    while not requests_finished and pages_checked < max_pages:
        if response.status_code != 200:
            raise ValueError(response.status_code)
        results = json.loads(response.content)

        for result in results:
            output.append(
                Stargazer(
                    user_id=result["user"]["id"],
                    starred_at=datetime.datetime.strptime(
                        result["starred_at"], "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    retrieved_at=get_current_time(),
                )
            )

        pages_checked += 1
        print(f"n={len(output)}")
        requests_finished = "next" not in response.links
        if requests_finished:
            break

        response = requests.get(response.links["next"]["url"], headers=headers)

    return output


def get_repo_language(owner_name: str, repo_name: str) -> list[Language]:
    url = f"https://api.github.com/repos/{owner_name}/{repo_name}/languages"

    headers = {
        "Accept": "application/vnd.github.star+json",
        "Authorization": f'Bearer {get_token("GITHUB_TOKEN")}',
        "X-GitHub-Api-Version": "2022-11-28",
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise ValueError(response.status_code)

    result = json.loads(response.content)
    return [Language(name=k, size_bytes=v) for k, v in result.items()]


def get_repos(org_name: str) -> list[Repo]:
    """
    get repos in org
    """
    url = f"https://api.github.com/orgs/{org_name}/repos"

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
        language = get_repo_language(owner_name=org_name, repo_name=result["name"])
        repo_license = None
        if result['license'] is not None:
            repo_license = result["license"]["name"]
        output.append(
            Repo(
                repo_id=result["id"],
                repo_name=result["name"],
                license=repo_license,
                topics=result["topics"],
                language=language,
                repo_size=result["size"],
                forks_count=result["forks_count"],
                watchers_count=result["watchers_count"],
                stargazers_count=result["stargazers_count"],
                open_issues_count=result["open_issues_count"],
                pushed_at=datetime.datetime.strptime(
                    result["pushed_at"], "%Y-%m-%dT%H:%M:%SZ"
                ),
                created_at=datetime.datetime.strptime(
                    result["created_at"], "%Y-%m-%dT%H:%M:%SZ"
                ),
                updated_at=datetime.datetime.strptime(
                    result["updated_at"], "%Y-%m-%dT%H:%M:%SZ"
                ),
                retrieved_at=get_current_time(),
            )
        )

    return output


def get_releases(owner_name: str, repo_name: str) -> list[Release]:
    print("getting releases...")
    url = f"https://api.github.com/repos/{owner_name}/{repo_name}/releases?per_page=100"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f'Bearer {get_token("GITHUB_TOKEN")}',
        "X-GitHub-Api-Version": "2022-11-28",
    }

    output = []
    requests_finished = False
    max_pages = 10
    pages_checked = 0

    response = requests.get(url, headers=headers)
    while not requests_finished and pages_checked < max_pages:
        if response.status_code != 200:
            raise ValueError(response.status_code)
        results = json.loads(response.content)
        for result in results:
            output.append(
                Release(
                    release_id=result["id"],
                    release_name=result["name"],
                    tag_name=result["tag_name"],
                    release_body=result["body"],
                    release_state=result["state"],
                    release_size=result["size"],
                    download_count=result["download_count"],
                    created_at=result["created_at"],
                    published_at=result["published_at"],
                    retrieved_at=get_current_time(),
                )
            )

        pages_checked += 1
        print(f"n={len(output)}")
        requests_finished = "next" not in response.links
        if requests_finished:
            break

        response = requests.get(response.links["next"]["url"], headers=headers)

    return output


def refresh_github_table(
    owner_name: str,
    repos: list[Repo],
    config: DeltaWriteConfig,
    get_func: Callable,
) -> None:
    all_results = []
    for i, repo in enumerate(repos, 1):
        header_text = f"[{i:02d}/{len(repos):02d}] {repo.repo_name}"
        print(create_header(80, header_text, True, "-"))

        results = get_func(owner_name, repo.repo_name)
        print(f"obtained {len(results)} records")
        all_results.extend(results)

    write_delta_table(all_results, config.table_dir, config.table_name, config.pk)


def main():
    owner_name = "mrpowers-io"
    repos = get_repos(owner_name)
    write_delta_table(repos, "bronze", "github_repos", "repo_id")
    refresh_github_table(
        owner_name,
        repos,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=str(Fork.__tablename__),
            pk="fork_id",
        ),
        get_forks,
    )
    refresh_github_table(
        owner_name,
        repos,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=str(Stargazer.__tablename__),
            pk="user_id",
        ),
        get_stargazers,
    )


if __name__ == "__main__":
    main()
