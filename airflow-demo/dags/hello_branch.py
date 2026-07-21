"""3주차 2 — 조건 분기 (@task.branch) 최소 예제.

지금까지 모든 태스크는 '항상' 실행됐다. 하지만 현실은 갈림길이다 — 검사 결과에
따라 A 경로 또는 B 경로. `@task.branch` 는 **실행할 downstream 의 task_id 를
문자열로 반환**하고, 선택되지 않은 형제 태스크는 자동으로 skip 된다.

  check() 가 "go_premium" 반환  →  go_premium 실행, go_normal 은 skip

common-ai 데모의 `@task.llm_branch`(LLM 이 분기를 결정)와 짝이다. 여기선 LLM 없이
평범한 파이썬 조건으로 분기한다 — 분기의 '엔진'만 다를 뿐 구조는 같다.
"""

from __future__ import annotations

import random

from airflow.sdk import dag, task


@dag(schedule=None, catchup=False, tags=["branch", "101"])
def hello_branch():
    @task
    def get_amount() -> int:
        amount = random.randint(0, 100000)
        print(f"주문 금액: {amount:,}원")
        return amount

    @task.branch
    def check(amount: int) -> str:
        # 반환한 문자열과 같은 task_id 를 가진 downstream 만 실행된다.
        return "go_premium" if amount >= 50000 else "go_normal"

    @task
    def go_premium() -> None:
        print("프리미엄 처리 경로")

    @task
    def go_normal() -> None:
        print("일반 처리 경로")

    amount = get_amount()
    check(amount) >> [go_premium(), go_normal()]


hello_branch()
