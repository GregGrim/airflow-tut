import datetime as dt
import sqlalchemy as sa

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
