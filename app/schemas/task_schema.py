from pydantic import BaseModel


class TaskCreate(BaseModel):
    url: str