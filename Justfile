
set dotenv-load
set shell := ["bash", "-c"]

@up *FLAGS:
    docker compose up --build {{FLAGS}}

@down:
    docker compose down

@quota:
    curl -s -L \
         -H "Accept: application/vnd.github+json" \
         -H "Authorization: Bearer $GITHUB_TOKEN" \
         -H "X-GitHub-Api-Version: 2022-11-28" \
         https://api.github.com/rate_limit | \
    jq -r '"Github REST API Quota\n=====================\nUsed: \(.rate.used)/\(.rate.limit)\nRemaining: \(.rate.remaining)\nResets: \(.rate.reset | tonumber | todate)"'

@user USER_ID:
    curl -s -L \
         -H "Accept: application/vnd.github+json" \
         -H "Authorization: Bearer $GITHUB_TOKEN" \
         -H "X-GitHub-Api-Version: 2022-11-28" \
         https://api.github.com/user/{{USER_ID}}

@repo ORG_NAME REPO_NAME PAGE_NUM:
    curl -s -L \
         -H "Accept: application/vnd.github.star+json" \
         -H "Authorization: Bearer $GITHUB_TOKEN" \
         -H "X-GitHub-Api-Version: 2022-11-28" \
         https://api.github.com/repos/{{ORG_NAME}}/{{REPO_NAME}}/stargazers?page={{PAGE_NUM}}


@mirror:
    uv run python ampere/mirror.py

@front:
    uv run duckdb -readonly data/frontend.duckdb --init data/init.sql

@back:
    uv run duckdb data/backend.duckdb --init data/init.sql

@reload *FLAGS:
    dbt build &&
    uv run python ampere/mirror.py &&
    docker compose up --build {{FLAGS}}

@sync:
    sh utils/sync_local.sh

@apidev:
    uv run uvicorn ampere.api.main:app --reload

@lint:
    source .venv/bin/activate
    echo '=======linting======='
    echo '\nruff ----------------'
    time ruff check ampere/
    echo '\nsqlfluff ------------'
    time sqlfluff lint models/ --disable-progress-bar
    echo '====================='

@tagbump:
    NEW_VERSION=`git describe --tags --abbrev=0 | awk -F. '{OFS="."; $NF+=1; print $0}'`; \
    echo $NEW_VERSION; \
    uvx --from=toml-cli toml set --toml-path=pyproject.toml project.version $NEW_VERSION; \
    git commit -am "Bump version to $NEW_VERSION"; \
    git push; \
    git tag $NEW_VERSION; \
    git push origin --tags;