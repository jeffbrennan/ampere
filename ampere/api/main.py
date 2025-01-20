import uvicorn
from fastapi import FastAPI

from ampere.api.routes import downloads

app = FastAPI()
app.include_router(downloads.router)


@app.get("/")
async def root():
    return {"message": "Hello Ampere!"}


# if __name__ == "__main__":
#     uvicorn.run(app, host="127.0.0.1", port=5049)
