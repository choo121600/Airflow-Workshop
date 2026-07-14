"""Dag 작성 101 4 — Asset (데이터 인지 스케줄링).

1, 2 의 ``>>`` 의존성은 **한 Dag 안**에서만 통한다. 그렇다면 서로 다른
Dag 는 어떻게 잇지? → Asset(데이터)을 매개로 잇는다.

* 프로듀서(hello_producer)가 태스크의 ``outlets`` 로 Asset 을 갱신한다.
* 컨슈머(airflow_consumer)는 크론 대신 그 Asset 을 ``schedule`` 로 구독한다.
* 프로듀서가 성공하면 → Asset 이 '갱신됨'으로 표시 → 컨슈머가 자동 실행.

즉 사람이 시각을 맞추거나 컨슈머를 직접 트리거할 필요가 없다.
데이터가 준비되는 순간이 곧 스케줄이 된다.
"""

from __future__ import annotations

from airflow.sdk import Asset, dag, task

# 이 이름이 두 Dag 를 잇는 '계약'이다. 양쪽이 같은 Asset 을 가리킨다.
hello_asset = Asset("hello_message")


@dag(schedule=None, catchup=False, tags=["101", "asset"])
def hello_producer():
    @task(outlets=[hello_asset])  # 성공하면 hello_asset 이 갱신된다
    def hello():
        print("Hello")

    hello()


@dag(schedule=[hello_asset], catchup=False, tags=["101", "asset"])  # 크론이 아니라 Asset 구독
def airflow_consumer():
    @task
    def airflow():
        print("Airflow — upstream Asset 이 갱신돼 자동 실행됨")

    airflow()


hello_producer()
airflow_consumer()
