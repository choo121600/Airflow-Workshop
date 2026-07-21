"""3주차 3 — 기다리기 (@task.sensor) 최소 예제.

지금까지 태스크는 '조건이 이미 갖춰졌다'고 보고 바로 실행했다. 하지만 외부를
기다려야 할 때가 있다 — 파일 도착, 다른 시스템의 준비 완료 등.

Sensor 는 **조건이 참이 될 때까지 poke_interval 마다 반복 확인**하는 특수 태스크다.
`@task.sensor` 함수가 True 를 반환하면 통과, False 면 poke_interval 뒤 다시 호출된다.

  poke_interval=5, timeout=60  →  5초마다 확인, 60초 안에 안 되면 실패
"""

from __future__ import annotations

import random

from airflow.sdk import dag, task


@dag(schedule=None, catchup=False, tags=["sensor", "101"])
def hello_sensor():
    @task.sensor(poke_interval=5, timeout=60, mode="poke")
    def wait_until_ready() -> bool:
        # 조건 확인. False 면 5초 뒤 이 함수가 다시 호출된다.
        ready = random.random() < 0.3  # 데모: 매 확인마다 30% 확률로 '준비됨'
        print("준비 완료!" if ready else "아직 준비 안 됨 — 5초 뒤 재확인")
        return ready

    @task
    def proceed() -> None:
        print("조건 충족 — 다음 단계 진행")

    wait_until_ready() >> proceed()


hello_sensor()
