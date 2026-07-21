"""3주차 2 — 분기·실패 제어 (현실: 데이터 품질 게이트).

`hello_branch` 의 `@task.branch` 를 실제 ETL 에 얹는다. 분기가 생기면 따라오는
두 가지 실무 문제를 함께 다룬다:

1. **분기 뒤 합류(join) 문제** — 분기하면 안 고른 경로가 skip 되고, 그 skip 이
   downstream 으로 전파돼 합류 태스크까지 skip 돼 버린다. → `trigger_rule` 로 푼다.
2. **깨졌을 때 어떻게 아나** — `on_failure_callback` 로 담당자에게 알린다.

시나리오: 추출한 주문의 손상률(음수 금액·null 지역)이 임계(5%)를 넘으면 적재를
막고 격리(quarantine)한 뒤 '시끄럽게' 실패시켜 사람을 부른다. 넘지 않으면 정상 적재.

  extract → quality_gate ─┬─(정상)→ load_warehouse ─┐
                          └─(손상)→ quarantine(실패) ─┼→ report   (none_failed_min_one_success)
                                                      └→ cleanup  (all_done: 무슨 일이 있어도 실행)
"""

from __future__ import annotations

import random
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

ANOMALY_THRESHOLD = 0.05  # 손상률이 이 값을 넘으면 적재 중단


def alert_on_failure(context) -> None:
    """태스크가 (재시도까지 소진하고) 최종 실패하면 호출된다.

    실무에선 여기서 Slack/PagerDuty/이메일을 보낸다. 데모라 로그로 대신한다.
    """
    ti = context["task_instance"]
    print(f"🚨 [ALERT] {ti.dag_id}.{ti.task_id} 최종 실패 — 담당자 호출 필요")


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["branch", "trigger-rule", "etl"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
        "on_failure_callback": alert_on_failure,  # 모든 태스크에 알림 콜백 부착
    },
    doc_md=__doc__,
)
def sales_etl_quality_gate():
    @task
    def extract() -> list[dict]:
        # 어떤 날은 소스가 깨끗하고(2%), 어떤 날은 대량 손상(12%)이 섞여 온다.
        # → 실행마다 품질 게이트가 통과/차단을 오간다.
        corruption = random.choice([0.02, 0.12])
        orders = []
        for i in range(100):
            bad = random.random() < corruption
            orders.append(
                {
                    "order_id": i,
                    "region": None if bad else random.choice(["seoul", "busan", "jeju"]),
                    "amount": -random.randint(1, 1000) if bad else random.randint(1000, 50000),
                }
            )
        return orders

    @task.branch
    def quality_gate(orders: list[dict]) -> str:
        bad = sum(1 for o in orders if o["region"] is None or o["amount"] < 0)
        rate = bad / len(orders)
        healthy = rate <= ANOMALY_THRESHOLD
        print(f"[품질검사] 손상 {bad}/{len(orders)} = {rate:.1%} → {'통과' if healthy else '차단'}")
        return "load_warehouse" if healthy else "quarantine"

    @task
    def load_warehouse(orders: list[dict]) -> None:
        print(f"[적재] {len(orders)}건 웨어하우스 적재 (품질 통과)")

    @task
    def quarantine(orders: list[dict]) -> None:
        bad = [o for o in orders if o["region"] is None or o["amount"] < 0]
        print(f"[격리] 손상 {len(bad)}건 격리 테이블로 — 적재 중단")
        # 조용히 넘어가지 않고 '시끄럽게' 실패시킨다 → on_failure_callback 발동, 사람이 조사.
        raise RuntimeError(f"품질 게이트 차단: 손상 {len(bad)}건")

    @task(trigger_rule="none_failed_min_one_success")
    def report() -> None:
        # 분기 뒤 합류. 기본 trigger_rule(all_success)이면 skip 된 형제 때문에 이 태스크도
        # skip 된다. none_failed_min_one_success = "실패한 부모 없고 최소 하나 성공"이면 실행
        # → 정상 경로에서만 리포트가 남는다(quarantine 이 실패한 날엔 실행 안 됨).
        print("[리포트] 정상 처리 요약 기록")

    @task(trigger_rule="all_done")
    def cleanup() -> None:
        # all_done = 부모가 성공/실패/skip '무엇이든 끝나기만' 하면 실행.
        # 파이프라인의 finally 블록 — 임시파일 정리·락 해제는 늘 돌아야 한다.
        print("[정리] 임시 리소스 정리 (성공/실패 무관)")

    orders = extract()
    gate = quality_gate(orders)
    lw = load_warehouse(orders)
    q = quarantine(orders)

    gate >> [lw, q]
    [lw, q] >> report()
    [lw, q] >> cleanup()


sales_etl_quality_gate()
