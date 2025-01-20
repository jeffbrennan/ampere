
import datetime

from sqlmodel import SQLModel


class DownloadPublic(SQLModel):
    repo: str
    download_timestamp: datetime.datetime
    group_name: str
    group_value: str
    download_count: int

class DownloadsPublic(SQLModel):
    data: list[DownloadPublic]
    count: int