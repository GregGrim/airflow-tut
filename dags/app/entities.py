import datetime as dt

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

