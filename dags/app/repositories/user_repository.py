from sqlalchemy.orm import Session

from app.entities import User
from app.models import UserModel


class UserRepository:
    def __init__(self, engine):
        self.engine = engine

    def delete_one(self, user_id: str) -> None:
        with Session(self.engine) as session:
            user = self.get_one(user_id)
            session.delete(user)
            session.commit()
            print(f'{user_id = } deleted from db')

    async def create_one(self, schema):
        raise NotImplementedError

    def get_one(self, user_id: str) -> User:
        with Session(self.engine) as session:
            user = session.query(UserModel).filter(UserModel.id == user_id).first()
            print(f'received {user.id = }')
        return user
