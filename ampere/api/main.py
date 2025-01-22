import uvicorn
from fastapi import FastAPI
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from ampere.api.limiter import limiter
from ampere.api.routes import downloads

app = FastAPI()
app.include_router(downloads.router)
app.state.limiter = limiter
app.add_exception_handler(
    RateLimitExceeded,
    _rate_limit_exceeded_handler,
)


@app.get("/")
def root():
    return {"message": "Hello Ampere!"}


# if __name__ == "__main__":
#     uvicorn.run(app, host="127.0.0.1", port=5049)
