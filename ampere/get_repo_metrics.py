import datetime
import json
import os

import dotenv
import requests
from sqlmodel import SQLModel


class StarInfo(SQLModel):
    user_id: int
    user_name: str
    user_avatar_link: str
    starred_at: datetime.datetime
    retrieved_at: datetime.datetime


class Repo(SQLModel):
    repo_id: str
    repo_name: str


def get_token(secret_name: str) -> str:
    dotenv.load_dotenv()
    token = os.environ.get(secret_name)
    if token is None:
        raise ValueError()
    return token


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
            retrieved_at=datetime.datetime.now()
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
                    retrieved_at=datetime.datetime.now()
                )
            )
        pages_checked += 1
        requests_finished = "next" not in response.links

    return output


def get_repos(owner_name: str) -> list[Repo]:
    """
    get repos in org
    """
    raise NotImplementedError()


def main():
    repo_name = "quinn"
    repo_stars = get_repo_stars("mrpowers-io", repo_name)
    print(f"obtained {len(repo_stars)} stars for {repo_name}")


if __name__ == "__main__":
    main()
