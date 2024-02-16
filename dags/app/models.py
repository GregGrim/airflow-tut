import datetime as dt
import sqlalchemy as sa
from sqlalchemy import ForeignKey

from sqlalchemy.orm import declarative_base


Base = declarative_base()


class UserModel(Base):
    __tablename__ = "users"
    id = sa.Column(
        sa.String, primary_key=True, server_default=sa.func.uuid_generate_v4()
    )
    username = sa.Column(sa.String, index=True, unique=True)
    hashed_password = sa.Column(sa.String, index=True)
    full_name = sa.Column(sa.String, index=True)
    email = sa.Column(sa.String, index=True, unique=True)
    phone_number = sa.Column(sa.String, index=True, unique=True)
    date_created = sa.Column(sa.DateTime, default=dt.datetime.utcnow())


class LogModel(Base):
    __tablename__ = "logs"
    id = sa.Column(
        sa.String, primary_key=True, server_default=sa.func.uuid_generate_v4()
    )
    user_id = sa.Column(ForeignKey("users.id"))
    message = sa.Column(sa.String)
    date_created = sa.Column(sa.DateTime, default=dt.datetime.utcnow())
    ipv4 = sa.Column(sa.String)
