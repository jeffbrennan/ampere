import datetime
import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import duckdb
import requests
from deltalake import DeltaTable

from ampere.common import (
    DeltaWriteConfig,
    create_header,
    get_current_time,
    get_db_con,
    get_model_foreign_key,
    get_token,
    write_delta_table,
)
from ampere.models import (
    Commit,
    CommitStats,
    Follower,
    Fork,
    Issue,
    Language,
    PullRequest,
    Release,
    Repo,
    Stargazer,
    User,
)


def get_forks(owner_name: str, repo: Repo) -> list[Fork]:
    print("getting forks...")
    output = []
    results = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo.repo_name}/forks?per_page=100",
            max_requests=50,
            max_errors=1,
            headers=get_github_api_header(),
        )
    )
    for result in results:
        output.append(
            Fork(
                repo_id=repo.repo_id,
                fork_id=result["id"],
                owner_id=result["owner"]["id"],
                created_at=result["created_at"],
                retrieved_at=get_current_time(),
            )
        )
    return output


def get_stargazers(owner_name: str, repo: Repo) -> list[Stargazer]:
    # https://docs.github.com/en/rest/activity/starring?apiVersion=2022-11-28
    print("getting stargazers...")
    output = []
    results = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo.repo_name}/stargazers?per_page=100",
            max_requests=50,
            max_errors=1,
            headers=get_github_api_header(),
        )
    )
    for result in results:
        output.append(
            Stargazer(
                repo_id=repo.repo_id,
                user_id=result["user"]["id"],
                starred_at=result["starred_at"],
                retrieved_at=get_current_time(),
            )
        )
    return output


def get_repo_language(owner_name: str, repo_name: str) -> list[Language]:
    result = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo_name}/languages",
            max_requests=1,
            max_errors=0,
            headers=get_github_api_header(),
        )
    )[0]

    return [Language(name=k, size_bytes=v) for k, v in result.items()]


def get_repos(org_name: str) -> list[Repo]:
    print("getting repos")
    output = []
    results = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/orgs/{org_name}/repos",
            max_requests=1,
            max_errors=0,
            headers=get_github_api_header(),
        )
    )

    for result in results:
        language = get_repo_language(owner_name=org_name, repo_name=result["name"])
        repo_license = None
        if result["license"] is not None:
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
                stargazers_count=result["stargazers_count"],
                open_issues_count=result["open_issues_count"],
                pushed_at=result["pushed_at"],
                created_at=result["updated_at"],
                updated_at=result["updated_at"],
                retrieved_at=get_current_time(),
            )
        )

    return output


def read_repos() -> list[Repo]:
    con = get_db_con()
    repos_dict = con.sql("select * from stg_repos").to_df().to_dict("records")
    repos = [Repo.model_validate(i) for i in repos_dict]
    return repos


def get_releases(owner_name: str, repo: Repo) -> list[Release]:
    print("getting releases...")
    output = []
    results = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo.repo_name}/releases?per_page=100",
            max_requests=50,
            max_errors=1,
            headers=get_github_api_header(),
        )
    )
    for result in results:
        output.append(
            Release(
                repo_id=repo.repo_id,
                release_id=result["id"],
                release_name=result["name"],
                tag_name=result["tag_name"],
                release_body=result["body"],
                created_at=result["created_at"],
                published_at=result["published_at"],
                retrieved_at=get_current_time(),
            )
        )

    return output


def get_commit_stats(
    owner_name: str, repo_name: str, commit_id: str
) -> list[CommitStats]:
    output = []
    result = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo_name}/commits/{commit_id}",
            max_requests=1,
            max_errors=0,
            headers=get_github_api_header(),
        )
    )[0]

    for f in result["files"]:
        output.append(
            CommitStats(
                filename=f["filename"],
                additions=f["additions"],
                deletions=f["deletions"],
                changes=f["changes"],
                status=f["status"],
            )
        )

    return output


def get_commits(owner_name: str, repo: Repo) -> list[Commit]:
    print("getting commits...")
    # by default, sorts by created descending
    output = []
    results = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo.repo_name}/commits?per_page=100",
            max_requests=10,
            max_errors=1,
            headers=get_github_api_header(),
            termination_key="sha",
            termination_value=get_latest_commit(repo),
        )
    )

    for result in results:
        commit_stats = get_commit_stats(owner_name, repo.repo_name, result["sha"])
        if result["author"] is not None:
            author_id = result["author"]["id"]
        elif result["committer"] is not None:
            author_id = result["committer"]["id"]
        else:
            author_id = None

        output.append(
            Commit(
                repo_id=repo.repo_id,
                commit_id=result["sha"],
                author_id=author_id,
                comment_count=result["commit"]["comment_count"],
                message=result["commit"]["message"],
                stats=commit_stats,
                committed_at=result["commit"]["author"]["date"],
                retrieved_at=get_current_time(),
            )
        )

    return output


def get_latest_commit(repo: Repo) -> str | None:
    con = get_db_con()
    query = f"""
        select commit_id
        from stg_commits 
        where committed_at = (
            select max(committed_at) from stg_commits where repo_id = {repo.repo_id}
        )
        """

    try:
        latest_commit = con.sql(query).fetchone()
    except duckdb.CatalogException as e:
        print(e)
        return None

    if latest_commit is None:
        print(f"{repo.repo_name} has no commits")
        return latest_commit

    return latest_commit[0]


def get_pull_requests(owner_name: str, repo: Repo) -> list[PullRequest]:
    print("getting prs...")
    output = []
    results = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo.repo_name}/pulls",
            max_requests=50,
            max_errors=1,
            parameters={"per_page": 100, "state": "all"},
            headers=get_github_api_header(),
        )
    )
    for result in results:
        output.append(
            PullRequest(
                repo_id=repo.repo_id,
                pr_id=result["id"],
                pr_number=result["number"],
                pr_title=result["title"],
                pr_state=result["state"],
                pr_body=result["body"],
                author_id=result["user"]["id"],
                created_at=result["created_at"],
                updated_at=result["updated_at"],
                closed_at=result["closed_at"],
                merged_at=result["merged_at"],
                retrieved_at=get_current_time(),
            )
        )

    return output


def get_issues(owner_name: str, repo: Repo) -> list[Issue]:
    print("getting issues...")
    output = []
    results = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo.repo_name}/issues",
            headers=get_github_api_header(),
            parameters={"per_page": 100, "state": "all"},
            max_requests=20,
            max_errors=1,
        )
    )
    for result in results:
        output.append(
            Issue(
                repo_id=repo.repo_id,
                issue_id=result["id"],
                issue_number=result["number"],
                issue_title=result["title"],
                issue_body=result["body"],
                author_id=result["user"]["id"],
                state=result["state"],
                state_reason=result["state_reason"],
                comments_count=result["comments"],
                created_at=result["created_at"],
                updated_at=result["updated_at"],
                closed_at=result["closed_at"],
                retrieved_at=get_current_time(),
            )
        )
    return output


def handle_follower_following_request(
    user_id: int, url: str, user_type: str
) -> list[Follower]:
    if user_type not in ["follower", "following"]:
        raise ValueError("expecting one of [follower, following]")

    results = handle_api_response(
        APIRequest(
            url=url,
            max_requests=5000,
            max_errors=1,
            headers=get_github_api_header(),
            parameters={"per_page": 100},
        )
    )

    if user_type == "follower":
        output = [
            Follower(
                user_id=user_id, follower_id=i["id"], retrieved_at=i["ampere_timestamp"]
            )
            for i in results
        ]
    else:
        output = [
            Follower(
                user_id=i["id"], follower_id=user_id, retrieved_at=i["ampere_timestamp"]
            )
            for i in results
        ]
    time.sleep(
        get_task_sleep_seconds(
            TaskSleepConfig(
                n_workers=2,
                target_adj_pct=0.9,
                request_cpu_time_seconds=0.25,
                task_real_time_seconds=0.3,
            )
        )
    )
    return output


def get_followers(user_id: int) -> list[Follower]:
    print("getting followers...")
    url = f"https://api.github.com/user/{user_id}/followers"
    return handle_follower_following_request(user_id, url, "follower")


def get_following(user_id: int) -> list[Follower]:
    print("getting following...")
    url = f"https://api.github.com/user/{user_id}/following"
    return handle_follower_following_request(user_id, url, "following")


@dataclass
class APIRequest:
    url: str
    max_requests: int
    max_errors: int
    headers: Optional[dict] = None
    parameters: Optional[dict] = None
    termination_key: Optional[str] = None
    termination_value: Optional[str] = None


def handle_api_response(config: APIRequest) -> list[dict]:
    url = config.url
    n_requests = 0
    errors = 0

    results = []
    while n_requests < config.max_requests:
        response = requests.get(url=url, headers=config.headers, params=config.parameters)
        n_requests += 1

        response_json = response.json()
        response_json["ampere_timestamp"] = get_current_time()
        n_requests += 1
        if response.status_code in [403, 429]:
            errors += 1
            if errors > config.max_errors:
                break
            response_text = str(response_json)

            if "secondary" in response_text:
                time.sleep(60 * config.max_errors)
            else:
                time.sleep(get_rate_limit_reset_sleep_seconds())
            continue

        if response.status_code != 200:
            raise ValueError(response.status_code)

        results.append(response_json)

        # exit if no links section to contain a continuation link
        if not response.links:
            break

        # exit if termination condition met
        if (
            config.termination_key is not None
            and config.termination_value is not None
            and response_json[config.termination_key] == config.termination_value
        ):
            break

        # exit if no more continuation tokens
        requests_finished = "next" not in response.links
        if requests_finished:
            break

        url = response.links["next"]["url"]
    return results


def get_rate_limit_reset_sleep_seconds() -> int:
    result = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/rate_limit",
            max_requests=1,
            max_errors=0,
            headers=get_github_api_header(),
        )
    )[0]

    reset = int(result["resources"]["core"]["reset"])
    seconds_to_sleep = reset - int(time.time()) + 1

    print("sleeping for", seconds_to_sleep, "seconds")
    return seconds_to_sleep


@dataclass
class TaskSleepConfig:
    n_workers: int
    target_adj_pct: float
    request_cpu_time_seconds: float
    task_real_time_seconds: float


def get_task_sleep_seconds(config: TaskSleepConfig) -> float:
    # https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2022-11-28#about-secondary-rate-limits
    rate_limit_seconds_per_60_seconds = 90
    target_cpu_time_per_60_seconds = (
        rate_limit_seconds_per_60_seconds * config.target_adj_pct
    )
    task_sleep_seconds = (
        1
        / (
            (target_cpu_time_per_60_seconds)
            / (config.request_cpu_time_seconds * 60 * config.n_workers)
        )
        - config.task_real_time_seconds
    ) / config.n_workers
    return task_sleep_seconds


def get_github_api_header():
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f'Bearer {get_token("GITHUB_TOKEN")}',
        "X-GitHub-Api-Version": "2022-11-28",
    }


def get_user(user_id: int) -> Optional[User]:
    results = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/user/{user_id}",
            max_requests=3,
            max_errors=1,
            headers=get_github_api_header(),
        )
    )
    if len(results) > 1:
        raise ValueError("expecting one result")

    results = results[0]
    time.sleep(
        get_task_sleep_seconds(
            TaskSleepConfig(
                n_workers=2,
                target_adj_pct=0.9,
                request_cpu_time_seconds=0.25,
                task_real_time_seconds=0.3,
            )
        )
    )
    return User(
        user_id=results["id"],
        user_name=results["login"],
        full_name=results["name"],
        company=results["company"],
        avatar_url=results["avatar_url"],
        repos_count=results["public_repos"],
        followers_count=results["followers"],
        following_count=results["following"],
        created_at=results["created_at"],
        updated_at=results["updated_at"],
        retrieved_at=get_current_time(),
    )


def get_user_ids() -> list[int]:
    user_models = [Stargazer, Fork, Commit, Issue, PullRequest]

    users = []
    print("getting user ids from existing tables")
    for model in user_models:
        print("-", model.__name__)
        table_path = Path(__file__).parents[1] / "data" / "bronze" / model.__tablename__
        delta_dir_path = table_path / "_delta_log"
        if not delta_dir_path.exists():
            print("not a delta table:", table_path)
            continue
        user_col = get_model_foreign_key(model, "user.user_id")
        if user_col is None:
            print("missing user id foreign key")
            continue

        df = DeltaTable(table_path).to_pandas(columns=[user_col])
        users.extend(df.squeeze().tolist())

    unique_users = sorted(set(users))
    print(f"got {len(unique_users)} unique users")
    return unique_users


def get_stale_followers_user_ids() -> list[int]:
    con = get_db_con()
    stale_hours = 20
    query = f"""
        select 
        a.user_id
        from stg_users a
        left join stg_followers b 
            on a.user_id = b.user_id
        where 
            b.user_id is null
            or b.retrieved_at < current_time() - interval {stale_hours} hour
        union
        select 
        a.user_id
        from stg_users a
        left join stg_followers b 
            on a.user_id = b.follower_id
        where 
            b.follower_id is null
            or b.retrieved_at < current_time() - interval {stale_hours} hour
    """

    try:
        user_ids = con.sql(query).to_df().squeeze().tolist()
    except duckdb.CatalogException as e:
        print(e)
        return get_user_ids()

    return user_ids


def refresh_github_table(
    owner_name: str,
    repos: list[Repo],
    config: DeltaWriteConfig,
    get_func: Callable,
) -> int:
    all_results = []
    for i, repo in enumerate(repos, 1):
        header_text = f"[{i:02d}/{len(repos):02d}] {repo.repo_name}"
        print(create_header(80, header_text, True, "-"))

        results = get_func(owner_name, repo)
        print(f"obtained {len(results)} records")
        all_results.extend(results)

    if len(all_results) == 0:
        print("no records to write")
        return 0

    write_delta_table(records=all_results, config=config)
    return len(all_results)


def refresh_users(
    user_ids: list[int],
    config: DeltaWriteConfig,
) -> int:
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=2) as executor:
        raw_results = list(executor.map(get_user, user_ids))

    all_results = [i for i in raw_results if i is not None]
    elapsed_time = time.time() - start_time
    avg_time_per_user = elapsed_time / len(user_ids)

    print(f"elapsed time: {elapsed_time:.2f} seconds")
    print(f"average time per user: {avg_time_per_user:.2f} seconds")

    write_delta_table(records=all_results, config=config)
    return len(all_results)


def refresh_followers(
    user_ids: list[int],
    config: DeltaWriteConfig,
) -> int:
    all_results = []
    start_time = time.time()
    for i, user_id in enumerate(user_ids, 1):
        header_text = f"[{i:04d}/{len(user_ids):04d}] {user_id}"
        print(create_header(80, header_text, True, "-"))
        try:
            follower_result = get_followers(user_id)
            all_results.extend(follower_result)
        except Exception as e:
            print(e)

        try:
            following_result = get_following(user_id)
            all_results.extend(following_result)
        except Exception as e:
            print(e)

    elapsed_time = time.time() - start_time
    if len(all_results) == 0:
        print("no results obtained")
        return 0

    avg_time_per_user = elapsed_time / len(user_ids)

    print(f"elapsed time: {elapsed_time:.2f} seconds")
    print(f"average time per user: {avg_time_per_user:.2f} seconds")
    valid_results = [i for i in all_results if i is not None]

    write_delta_table(records=valid_results, config=config)
    return len(all_results)
