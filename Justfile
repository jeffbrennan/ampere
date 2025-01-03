
set dotenv-load

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

@mirror:
    poetry run python ampere/maintenance.py

@front:
    poetry run duckdb -readonly data/frontend.duckdb

@back:
    poetry run duckdb data/backend.duckdb