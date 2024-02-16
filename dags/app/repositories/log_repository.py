from typing import List

from sqlalchemy.orm import Session

from app.entities import UserActivityLog
from app.models import LogModel


class LogRepository:
    def __init__(self, engine):
        self.engine = engine

    def get_all_logs(self) -> List[UserActivityLog]:
        with Session(self.engine) as session:
            logs = session.query(LogModel).all()
            logs = [UserActivityLog.model_validate(log) for log in logs]
        return logs
