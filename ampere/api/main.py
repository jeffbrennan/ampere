from fastapi import FastAPI
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from ampere.api.limiter import limiter
from ampere.api.routes import downloads, feed, repos

app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(
    RateLimitExceeded,
    _rate_limit_exceeded_handler,
)

app.include_router(downloads.router)
app.include_router(feed.router)
app.include_router(repos.router)


@app.get("/")
def root():
    return {"message": "Hello Ampere!"}
