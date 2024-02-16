import datetime as dt
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, EmailStr


class BaseUser(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    username: str = Field(min_length=4, max_length=10)
    hashed_password: str = Field(min_length=4)
    full_name: str
    email: EmailStr = Field(default="example@email.com")
    phone_number: str


class User(BaseUser):
    id: str
    date_created: dt.datetime


class BaseUserActivityLog(BaseModel):
    def __init__(self, /, **data: Any):
        super().__init__(**data)
    model_config = ConfigDict(from_attributes=True)
    user_id: str
    message: str
    ipv4: str = Field(
        pattern="^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")


class UserActivityLog(BaseUserActivityLog):
    id: str
    date_created: dt.datetime

