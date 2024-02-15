import pendulum
import sqlalchemy as sa

from airflow.decorators import dag, task
from airflow.models import Variable
from app.repositories.token_repository import TokenRepository
from app.repositories.user_repository import UserRepository


@dag(
    dag_id="remove_user_dag",
    start_date=pendulum.datetime(2016, 1, 1),
    schedule_interval=None,
    default_args={"retries": 1},
)
def remove_user_dag():
    @task(task_id="remove_user_tokens")
    def remove_user_tokens(**kwargs):
        user_id = kwargs["dag_run"].conf.get("user_id")
        redis_host = Variable.get('redis_host')
        redis_port = Variable.get('redis_port')
        cache_repo = TokenRepository(redis_host, redis_port)
        cache_repo.remove_all_user_tokens(user_id)
        return {"message": f"Removed tokens for user {user_id}"}

    @task(task_id="remove_user_from_db")
    def remove_user_from_db(**kwargs):
        user_id = kwargs["dag_run"].conf.get("user_id")
        db_url = Variable.get('db_url')
        user_repo = UserRepository(engine=sa.create_engine(db_url))
        user_repo.delete_one(user_id)
        return {"message": f"Removed user {user_id} from DB"}

    remove_user_tokens()
    remove_user_from_db()


dag = remove_user_dag()
