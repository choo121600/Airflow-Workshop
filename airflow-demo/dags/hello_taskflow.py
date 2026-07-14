"""Dag 작성 101 1 — TaskFlow API.

평범한 파이썬 함수에 ``@task`` 만 붙이면 태스크가 된다. 게다가 태스크 간
값 전달(XCom)이 자동이다 — hello 가 리턴한 값을 airflow 가 그냥 인자로 받아
"Hello Airflow" 를 출력한다:

    airflow(hello())   # 순서(의존성) + 값 전달을 한 줄로

같은 동작을 Classic 오퍼레이터로 짠 ``hello_classic.py`` 와 비교해보면, 거기선
같은 값을 ``ti.xcom_pull(task_ids="hello")`` 로 손수 꺼내고 의존성도 따로
걸어야 한다. 그래서 커스텀 파이썬 로직엔 TaskFlow 를 먼저 쓴다.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["101", "taskflow"]
)
def hello_taskflow():
    @task
    def hello() -> str:
        return "Hello"  # 리턴값이 곧 XCom 이 된다

    @task
    def airflow(greeting: str) -> None:
        print(f"{greeting} Airflow")  # 앞 태스크의 리턴을 그냥 인자로 받는다

    airflow(hello())  # 순서 + 값 전달을 한 줄로. 의존성이 곧 데이터 흐름.


hello_taskflow()
