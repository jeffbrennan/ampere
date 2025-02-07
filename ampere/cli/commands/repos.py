import requests
import typer
from rich.console import Console

from ampere.cli.common import get_api_url
from ampere.models import ReposPublic

console = Console()
repos_app = typer.Typer()


def get_repo_list_response(ctx: typer.Context) -> ReposPublic:
    env = ctx.obj["env"]
    base_url = get_api_url(env)
    url = f"{base_url}/repos/list"

    print(f"Making request to {url}")
    response = requests.get(url)
    assert response.status_code == 200, response.json()
    model = ReposPublic.model_validate(response.json())
    return model


@repos_app.command("list")
def list_repos(ctx: typer.Context) -> None:
    model = get_repo_list_response(ctx)
    console.print_json(model.model_dump_json())
