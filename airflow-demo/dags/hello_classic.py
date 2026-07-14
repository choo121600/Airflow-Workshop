"""Dag 작성 101 2 — Classic Operator.

``hello_taskflow.py`` 와 결과는 똑같다("Hello Airflow"). 차이는 '값을 어떻게
잇느냐'다:

* 함수를 ``PythonOperator`` 로 감싸고 ``task_id`` 를 손으로 붙인다.
* 리턴값이 XCom 에 자동 저장되긴 하지만, 꺼낼 땐 ``ti.xcom_pull`` 로 task_id 를
  **문자열**로 직접 지목해야 한다.
* 결정적으로 ``xcom_pull`` 은 의존성을 만들지 않는다 → ``hello >> airflow`` 를
  손으로 걸어야 한다. 빼먹으면 airflow 가 hello 보다 먼저 돌아 None 을 당긴다.

즉 TaskFlow 가 한 줄로 하던 '순서 + 값 전달'을 여기선 둘로 나눠 관리한다.
그래서 커스텀 파이썬 로직엔 TaskFlow 를 먼저 쓴다.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _hello() -> str:
    return "Hello"  # 리턴값은 XCom(return_value)에 자동 저장된다


def _airflow(ti) -> None:  # ti(task instance)는 context 에서 주입된다
    greeting = ti.xcom_pull(task_ids="hello")  # task_id 를 문자열로 직접 지목해 꺼낸다
    print(f"{greeting} Airflow")


with DAG(
    dag_id="hello_classic",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["101", "classic"],
):
    hello = PythonOperator(task_id="hello", python_callable=_hello)
    airflow = PythonOperator(task_id="airflow", python_callable=_airflow)

    # xcom_pull 은 의존성을 만들지 않는다. 
    # 이 줄을 빼먹으면 순서가 보장되지 않아 airflow 가 None 을 당길 수 있다 
    # → 데이터와 배선을 따로 관리해야 한다.
    hello >> airflow
