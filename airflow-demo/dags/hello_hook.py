"""3주차 4 — 외부 연결 (Connection + Hook) 최소 예제.

지금까지 데이터를 파일에 썼다. 하지만 실무 웨어하우스는 DB 다. 그리고 DB 접속
정보(호스트·비밀번호)를 코드에 박으면 안 된다.

* **Connection** — 접속 정보를 Airflow 가 안전하게 보관한다. 여기선
  airflow_settings.yaml 의 `demo_postgres`(host/계정/비밀번호가 거기 있다).
* **Hook** — 그 Connection 으로 외부 시스템에 붙는 어댑터. `conn_id` 만 넘기면
  된다 — 비밀번호는 코드 어디에도 없다.
"""

from __future__ import annotations

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task


@dag(schedule=None, catchup=False, tags=["hook", "101"])
def hello_hook():
    @task
    def count_orders() -> None:
        # conn_id 만 준다. 호스트·계정·비밀번호는 Connection 이 들고 있다.
        hook = PostgresHook(postgres_conn_id="demo_postgres")
        (count,) = hook.get_first("SELECT count(*) FROM orders")
        print(f"orders 테이블 행 수: {count}")

    count_orders()


hello_hook()
