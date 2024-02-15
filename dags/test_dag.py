from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable

from sqlalchemy import create_engine


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def test_dag():
    @task()
    def ping_database():
        db_url = Variable.get('db_url')

        engine = create_engine(db_url)

        engine.execute("select 1")

    ping_database()


dag = test_dag()
