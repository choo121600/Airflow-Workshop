"""Dag 작성 101 3 — Classic 이 정답인 경우: Provider 오퍼레이터.

TaskFlow 는 '내가 직접 짠 파이썬 함수'를 태스크로 만들 때 최고다. 하지만
할 일이 파이썬이 아니라면(셸 명령, SQL, S3 복사, K8s Pod 실행 …) 이미
만들어진 Provider 오퍼레이터를 쓰는 게 맞다 — 굳이 파이썬 함수로
감쌀 이유가 없다.

여기선 ``BashOperator``(standard provider)로 셸에서 직접 echo 한다.
"함수 → @task" 로 바꾸는 게 오히려 부자연스러운, Classic 이 어울리는 자리다.

실무 팁: 한 Dag 안에서 둘을 섞어 쓴다. 커스텀 로직은 @task(TaskFlow),
외부 시스템/셸 연동은 이런 provider 오퍼레이터로.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="hello_bash_operator",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["101", "classic", "provider"],
):
    # bash_command 는 셸에서 그대로 실행된다. 감쌀 파이썬 함수가 없다.
    hello = BashOperator(task_id="hello", bash_command="echo Hello")
    airflow = BashOperator(task_id="airflow", bash_command="echo Airflow")

    hello >> airflow
