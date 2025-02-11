import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Optional

import duckdb
import requests

from ampere.common import (
    DeltaWriteConfig,
    create_header,
    get_backend_db_con,
    get_current_time,
    get_secret,
    timeit,
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


@dataclass
class APIRequest:
    url: str
    max_requests: int = 50
    max_errors: int = 1
    headers: dict = field(
        default_factory=lambda: {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {get_secret('GITHUB_TOKEN')}",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    parameters: Optional[dict] = None
    wait_for_quota: bool = True


@dataclass
class APIResponse:
    results: list[dict]
    timestamp: datetime.datetime
    status_code: int


@dataclass
class TaskSleepConfig:
    n_workers: int
    target_adj_pct: float
    request_cpu_time_seconds: float
    task_real_time_seconds: float


def get_rate_limit_reset_sleep_seconds() -> int:
    result = handle_api_response(
        APIRequest(
            url="https://api.github.com/rate_limit",
            max_requests=1,
            max_errors=0,
        )
    )[0].results[0]

    reset = int(result["resources"]["core"]["reset"])
    seconds_to_sleep = reset - int(time.time()) + 1

    print("sleeping for", seconds_to_sleep, "seconds")
    print(
        "requests will resume at",
        datetime.datetime.now() + datetime.timedelta(seconds_to_sleep),
    )
    return seconds_to_sleep


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


def read_repos(con: duckdb.DuckDBPyConnection) -> list[Repo]:
    repos_dict = con.sql("select * from stg_repos").to_df().to_dict("records")
    repos = [Repo.model_validate(i) for i in repos_dict]
    return repos


def get_latest_commit_timestamp(repo: Repo) -> datetime.datetime | None:
    con = get_backend_db_con()
    query = f"""
        select max(committed_at)
        from stg_commits 
        where repo_id = {repo.repo_id}
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


@timeit
def get_org_user_ids() -> list[int]:
    """
    gets unique list of user ids from tracked tables to be added to the `users` table
    if `stg_users` is available, only returns user_ids that have not been refreshed in past `stale_hours` hours
    """
    print("getting user ids from org tables...")
    con = get_backend_db_con()
    stale_hours = 24
    user_ids = (
        con.sql(
            """
            with combined as (
                select user_id from stg_stargazers
                union
                select owner_id as user_id from stg_forks
                union
                select author_id as user_id from stg_commits
                union
                select author_id as user_id from stg_issues
                union
                select author_id as user_id from stg_pull_requests
            )
            select distinct user_id::bigint as user_id
            from combined 
            where user_id is not null
        """
        )
        .to_df()
        .squeeze()
        .tolist()
    )

    try:
        fresh_user_ids = (
            con.sql(
                f"""
                select distinct user_id
                from stg_users
                where retrieved_at >= now() - interval {stale_hours} hour
            """
            )
            .to_df()
            .squeeze()
            .tolist()
        )
        stale_user_ids = sorted(set(user_ids) - set(fresh_user_ids))

    except duckdb.CatalogException as e:
        print(e)
        stale_user_ids = user_ids

    print(f"got {len(stale_user_ids)} stale users")
    return stale_user_ids


@timeit
def get_stale_followers_user_ids(endpoint: str) -> list[int]:
    if endpoint not in ["followers", "following"]:
        raise ValueError("expecting one of ['followers', 'following']")

    con = get_backend_db_con()
    stale_hours = 24
    if endpoint == "followers":
        query = f"""
        select distinct
        a.user_id
        from stg_users a
        left join stg_followers b 
            on a.user_id = b.user_id
        where 
            b.user_id is null
            or b.retrieved_at < now() - interval {stale_hours} hour 
    """

    else:
        query = f"""
        select distinct
        a.user_id
        from stg_users a
        left join stg_followers b 
            on a.user_id = b.follower_id
        where 
            b.follower_id is null
            or b.retrieved_at < now() - interval {stale_hours} hour
        """

    try:
        user_ids = con.sql(query).to_df().squeeze().tolist()
        if isinstance(user_ids, int):
            user_ids = [user_ids]
        print("got stale followers from stg_followers table")
    except Exception as e:
        if "does not exist" in str(e):
            return get_org_user_ids()
        raise Exception(e)

    return user_ids


def handle_api_response(config: APIRequest) -> list[APIResponse]:
    url = config.url
    endpoint = config.url.split("api.github.com")[-1]
    n_requests = 0
    errors = 0

    output: list[APIResponse] = []
    while n_requests < config.max_requests:
        response = requests.get(url=url, headers=config.headers, params=config.parameters)
        n_requests += 1
        print(f"[{endpoint}] requests: {n_requests}")
        # print(f"[{endpoint}] requests: {n_requests}", end="\r", flush=True)

        response_json = response.json()
        if response.status_code == 404:
            return [
                APIResponse(
                    [response_json], get_current_time(), status_code=response.status_code
                )
            ]

        if response.status_code in [403, 429]:
            errors += 1
            if errors > config.max_errors:
                break

            if not config.wait_for_quota:
                # return single response with the rate limit status code to exclude incomplete output downstream
                return [
                    APIResponse(
                        [response_json],
                        get_current_time(),
                        status_code=response.status_code,
                    )
                ]

            response_text = str(response_json)
            if "secondary" in response_text:
                print("hit secondary rate limit")
                time.sleep(60 * errors)
            else:
                time.sleep(get_rate_limit_reset_sleep_seconds())
            continue

        if response.status_code != 200:
            raise ValueError(response.status_code)

        if not isinstance(response_json, list):
            response_json = [response_json]

        output.append(
            APIResponse(response_json, get_current_time(), response.status_code)
        )

        if not response.links:
            break

        # exit if no more continuation tokens
        requests_finished = "next" not in response.links
        if requests_finished:
            break

        url = response.links["next"]["url"]
    return output


def get_forks(owner_name: str, repo: Repo) -> list[Fork]:
    print("getting forks...")
    output = []
    responses = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo.repo_name}/forks",
            parameters={"per_page": 100},
        )
    )

    for response in responses:
        for result in response.results:
            output.append(
                Fork(
                    repo_id=repo.repo_id,
                    fork_id=result["id"],
                    owner_id=result["owner"]["id"],
                    created_at=result["created_at"],
                    retrieved_at=response.timestamp,
                )
            )
    return output


def get_stargazers(owner_name: str, repo: Repo) -> list[Stargazer]:
    # https://docs.github.com/en/rest/activity/starring?apiVersion=2022-11-28
    print("getting stargazers...")
    output = []
    config = APIRequest(
        url=f"https://api.github.com/repos/{owner_name}/{repo.repo_name}/stargazers",
        parameters={"per_page": 100},
    )
    config.headers["Accept"] = "application/vnd.github.star+json"

    responses = handle_api_response(config)

    for response in responses:
        for result in response.results:
            output.append(
                Stargazer(
                    repo_id=repo.repo_id,
                    user_id=result["user"]["id"],
                    starred_at=result["starred_at"],
                    retrieved_at=response.timestamp,
                )
            )
    return output


def get_repo_language(owner_name: str, repo_name: str) -> list[Language]:
    result = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo_name}/languages",
            max_requests=1,
            max_errors=0,
        )
    )[0].results[0]

    return [Language(name=k, size_bytes=v) for k, v in result.items()]


def get_repos(org_name: str) -> list[Repo]:
    print("getting repos")
    output = []
    response = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/orgs/{org_name}/repos",
            max_requests=1,
        )
    )[0]

    for result in response.results:
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
                created_at=result["created_at"],
                updated_at=result["updated_at"],
                retrieved_at=response.timestamp,
            )
        )

    return output


def get_releases(owner_name: str, repo: Repo) -> list[Release]:
    print("getting releases...")
    output = []
    responses = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo.repo_name}/releases",
            parameters={"per_page": 100},
        )
    )
    for response in responses:
        for result in response.results:
            output.append(
                Release(
                    repo_id=repo.repo_id,
                    release_id=result["id"],
                    release_name=result["name"],
                    tag_name=result["tag_name"],
                    release_body=result["body"],
                    created_at=result["created_at"],
                    published_at=result["published_at"],
                    retrieved_at=response.timestamp,
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
        )
    )[0].results[0]

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
    latest_commit_timestamp = get_latest_commit_timestamp(repo)

    config = APIRequest(
        url=f"https://api.github.com/repos/{owner_name}/{repo.repo_name}/commits",
        parameters={"per_page": 100},
    )

    if latest_commit_timestamp is not None and config.parameters is not None:
        latest_commit_timestamp = latest_commit_timestamp + datetime.timedelta(seconds=1)
        config.parameters["since"] = latest_commit_timestamp

    responses = handle_api_response(config)
    for response in responses:
        for result in response.results:
            commit_stats = get_commit_stats(
                owner_name,
                repo.repo_name,
                result["sha"],
            )

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
                    retrieved_at=response.timestamp,
                )
            )

    return output


def get_pull_requests(owner_name: str, repo: Repo) -> list[PullRequest]:
    print("getting prs...")
    output = []
    responses = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo.repo_name}/pulls",
            parameters={"per_page": 100, "state": "all"},
        )
    )

    for response in responses:
        for result in response.results:
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
                    retrieved_at=response.timestamp,
                )
            )

    return output


def get_issues(owner_name: str, repo: Repo) -> list[Issue]:
    print("getting issues...")
    output = []
    responses = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/repos/{owner_name}/{repo.repo_name}/issues",
            parameters={"per_page": 100, "state": "all"},
        )
    )
    for response in responses:
        for result in response.results:
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
                    retrieved_at=response.timestamp,
                )
            )
    return output


def get_followers(user_id: int, endpoint: str) -> tuple[list[Follower], bool]:
    # returns tuple of follower list and termination indicator
    if endpoint not in ["followers", "following"]:
        raise ValueError("expecting one of ['followers', 'following']")

    print(f"getting {endpoint} ...")
    output = []
    responses = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/user/{user_id}/{endpoint}",
            max_requests=5000,
            parameters={"per_page": 100},
            wait_for_quota=False,
        )
    )

    skips = 0
    terminate_requests = False
    if endpoint == "followers":
        for response in responses:
            if response.status_code != 200:
                if response.status_code in [403, 429]:
                    terminate_requests = True
                skips += 1
                print(f"skipping {response.status_code} | n skips = {skips}")
                continue
            for result in response.results:
                output.append(
                    Follower(
                        user_id=user_id,
                        follower_id=result["id"],
                        retrieved_at=response.timestamp,
                    )
                )
    else:
        for response in responses:
            if response.status_code != 200:
                if response.status_code in [403, 429]:
                    terminate_requests = True
                skips += 1
                print(f"skipping {response.status_code} | n skips = {skips}")
                continue
            for result in response.results:
                output.append(
                    Follower(
                        user_id=result["id"],
                        follower_id=user_id,
                        retrieved_at=response.timestamp,
                    )
                )

    return output, terminate_requests


def get_user(user_id: int) -> Optional[User]:
    response = handle_api_response(
        APIRequest(
            url=f"https://api.github.com/user/{user_id}",
            max_requests=3,
        )
    )[0]

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
    if response.status_code != 200:
        return None

    results = response.results[0]
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
        retrieved_at=response.timestamp,
    )


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
    if len(user_ids) == 0:
        print("no user ids to process. exiting early")
        return 0

    if any(not isinstance(i, int) for i in user_ids):
        raise TypeError("expecting user id of type `int`")

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=2) as executor:
        raw_results = list(executor.map(get_user, user_ids))

    all_results = [i for i in raw_results if i is not None]
    if len(all_results) == 0:
        print("no results obtained")
        return 0

    elapsed_time = time.time() - start_time
    avg_time_per_user = elapsed_time / len(user_ids)

    print(f"elapsed time: {elapsed_time:.2f} seconds")
    print(f"average time per user: {avg_time_per_user:.2f} seconds")

    write_delta_table(records=all_results, config=config)
    return len(all_results)


def refresh_followers(
    user_ids: list[int], config: DeltaWriteConfig, endpoint: str
) -> int:
    all_results = []
    start_time = time.time()
    if len(user_ids) == 0:
        print("no user ids to process. exiting early")
        return 0

    if any(not isinstance(i, int) for i in user_ids):
        raise TypeError("expecting user id of type `int`")

    for i, user_id in enumerate(user_ids, 1):
        header_text = f"[{i:04d}/{len(user_ids):04d}] {user_id}"
        print(create_header(80, header_text, True, "-"))
        result, terminate_requests = get_followers(user_id, endpoint)

        if terminate_requests:
            break

        all_results.extend(result)

    elapsed_time = time.time() - start_time
    valid_results = [i for i in all_results if i is not None]
    if len(valid_results) == 0:
        print("no results obtained")
        return 0

    avg_time_per_user = elapsed_time / len(user_ids)

    print(f"elapsed time: {elapsed_time:.2f} seconds")
    print(f"average time per user: {avg_time_per_user:.2f} seconds")

    write_delta_table(records=valid_results, config=config)
    return len(valid_results)
