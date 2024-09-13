import requests
import dotenv
from sqlmodel import SQLModel
import os
import json
import datetime


class StarInfo(SQLModel):
    user_id: int
    user_name: str
    user_avatar_link: str
    timestamp: datetime.datetime

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
        "Accept": "application/vnd.github+json",
        "Authorization": f'Bearer {os.environ.get("GITHUB_TOKEN")}',
        "X-GitHub-Api-Version": "2022-11-28",
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise ValueError(response.status_code)

    output = [
        StarInfo(
            user_id=user["id"],
            user_name=user["login"],
            user_avatar_link=user["avatar_url"],
            timestamp=datetime.datetime.now(),
        )
        for user in json.loads(response.content)
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
        users = json.loads(response.content)
        for user in users:
            output.append(
                StarInfo(
                    user_id=user["id"],
                    user_name=user["login"],
                    user_avatar_link=user["avatar_url"],
                    timestamp=datetime.datetime.now(),
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
