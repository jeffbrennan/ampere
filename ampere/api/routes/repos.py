from fastapi import APIRouter, Request

from ampere.api.limiter import limiter
from ampere.cli.common import CLIEnvironment
from ampere.models import ReposPublic, get_repo_names

router = APIRouter(prefix="/repos", tags=["repo"])


@limiter.limit("60/minute")
@router.get("/list", response_model=ReposPublic)
def read_repos(request: Request) -> ReposPublic:
    repos = get_repo_names(CLIEnvironment.dev)
    print(repos)
    return ReposPublic(repos=repos, count=len(repos))
