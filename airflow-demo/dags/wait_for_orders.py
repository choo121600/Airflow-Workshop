"""3주차 3 — 기다리기 (현실: 상류 파일 도착 대기).

현실: 상류 팀이 그날의 raw 주문 파일을 아침 6~9시 '아무 때나' 올린다. 크론을 8시에
걸면 아직 없을 때가 있다. 그렇다고 직접 폴링 스크립트를 짜면 2주차 cron-demo 의
고통으로 되돌아간다. → Sensor 로 파일이 도착할 때까지 기다린다.

2주차 Asset 스케줄링과의 대비가 핵심이다:
  * Asset  : 내가 통제하는 producer 가 outlets 로 신호 → 이벤트 기반(폴링 없음)
  * Sensor : producer 를 통제 못 함(남의 S3·서드파티 API) → 폴링으로 확인

또 하나 — mode="reschedule": poke 모드는 기다리는 동안 워커 슬롯을 붙잡는다. 파일을
3시간 기다리는 센서 1000개가 슬롯을 물면 클러스터가 마비된다. reschedule 은 확인
사이에 슬롯을 반납한다 → 오래 기다리는 센서의 정석.

[데모] 이 Dag 를 트리거하면 wait_for_raw 가 대기한다. 그때 파일을 만들어 주면
센서가 다음 확인에서 깨어난다(<ds> 는 실행 논리 날짜):
  docker exec <scheduler> bash -lc \
    'echo "[]" > /usr/local/airflow/include/etl_demo/raw/orders_<ds>.json'
"""

from __future__ import annotations

import json
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task

RAW = Path("/usr/local/airflow/include/etl_demo/raw")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["sensor", "etl"],
    doc_md=__doc__,
)
def wait_for_orders():
    @task.sensor(poke_interval=10, timeout=60 * 30, mode="reschedule")
    def wait_for_raw() -> bool:
        ds = get_current_context()["ds"]
        path = RAW / f"orders_{ds}.json"
        found = path.exists()
        print(f"[sensor] {path} — {'도착!' if found else '아직 없음, 대기'}")
        return found

    @task
    def process_raw() -> None:
        ds = get_current_context()["ds"]
        path = RAW / f"orders_{ds}.json"
        orders = json.loads(path.read_text(encoding="utf-8"))
        print(f"[process] 파일 도착 확인 후 처리 — {len(orders)}건")

    wait_for_raw() >> process_raw()


wait_for_orders()
