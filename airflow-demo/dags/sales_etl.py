"""cron-demo 와 '똑같은' ETL(extract → transform → load)을 Airflow Dag 로.

`../cron-demo` 는 순서·재시도·로그·백필·멱등성을 **전부 손으로** 만들어야 했다.
같은 파이프라인을 여기서는 오케스트레이터가 챙겨준다 — 아래가 그 1:1 대응이다.

| 고통 (cron-demo)                    | 해결 (이 Dag)                                  |
|------------------------------------|-----------------------------------------------|
| 순서를 시간(2:10, 2:20)으로 추측     | 코드로 선언: `extract() >> transform() >> load()` |
| 앞 단계 실패를 모른 채 뒷 단계 실행   | upstream 실패 시 downstream 자동 skip           |
| 소스 장애 = 그냥 실패 (재시도 없음)   | `retries=3` → 자동 재시도                        |
| 로그가 파일마다 흩어짐               | 태스크별 로그 + Web UI 에서 한눈에                |
| 백필하려면 날짜를 손으로 바꿔 재실행  | data interval(`ds`) → catchup/backfill 자동      |
| 같은 날짜 재실행 = 중복 적재          | 날짜 파티션 덮어쓰기(delete-then-insert)로 멱등    |

산출물은 cron-demo/data 와 같은 레이아웃으로 include/etl_demo 아래에 쌓인다:
  raw/orders_<ds>.json  →  staging/summary_<ds>.csv  →  warehouse/daily_sales.csv
"""

from __future__ import annotations

import csv
import json
import random
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task

# include/ 는 컨테이너에 마운트되어 호스트(airflow-demo/include)에서도 보인다.
BASE = Path("/usr/local/airflow/include/etl_demo")
RAW, STAGING, WAREHOUSE = BASE / "raw", BASE / "staging", BASE / "warehouse"
REGIONS = ["seoul", "busan", "jeju", "daegu"]


def _ds() -> str:
    """이 실행이 '논리적으로' 처리하는 날짜(data interval).

    cron 의 date.today() 와 달리, 과거 구간을 백필해도 그 구간의 날짜가 들어온다.
    """
    return get_current_context()["ds"]


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Seoul"),
    catchup=False,  # 데모에선 자동 백필을 끄고, 필요할 때 직접 트리거/백필한다
    tags=["etl", "demo", "cron-vs-airflow"],
    default_args={"retries": 3, "retry_delay": timedelta(seconds=15)},
    doc_md=__doc__,
)
def sales_etl():
    @task
    def extract() -> str:
        """1단계 · 주문 원본 추출. cron-demo/jobs/extract.py 와 같은 로직."""
        ds = _ds()
        # 소스가 가끔 죽는다(cron-demo 의 FAIL_RATE 와 동일). 하지만 여기선
        # retries=3 이 알아서 다시 시도한다 → 사람이 새벽에 깨어날 필요가 없다.
        if random.random() < 0.3:
            raise RuntimeError("소스 일시 장애 — Airflow 가 자동 재시도한다")

        n = random.randint(50, 150)
        orders = [
            {
                "order_id": i,
                "region": random.choice(REGIONS),
                "amount": random.randint(1000, 50000),
            }
            for i in range(n)
        ]
        RAW.mkdir(parents=True, exist_ok=True)
        out = RAW / f"orders_{ds}.json"
        out.write_text(json.dumps(orders, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[extract] 주문 {n}건 저장 → {out}")
        return str(out)  # XCom 으로 다음 태스크에 경로를 넘긴다

    @task
    def transform(raw_path: str) -> str:
        """2단계 · 지역별 매출 집계. extract 가 성공해야만 시작된다."""
        ds = _ds()
        # cron-demo 에서는 입력 존재를 손으로 방어해야 했다. 여기선 그럴 필요가 없다 —
        # 의존성이 보장되므로 raw_path 는 반드시 존재한다.
        orders = json.loads(Path(raw_path).read_text(encoding="utf-8"))
        agg: dict[str, dict[str, int]] = {}
        for o in orders:
            r = agg.setdefault(o["region"], {"orders": 0, "revenue": 0})
            r["orders"] += 1
            r["revenue"] += o["amount"]

        STAGING.mkdir(parents=True, exist_ok=True)
        out = STAGING / f"summary_{ds}.csv"
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "region", "orders", "revenue"])
            for region, v in sorted(agg.items()):
                w.writerow([ds, region, v["orders"], v["revenue"]])
        print(f"[transform] {len(agg)}개 지역 집계 → {out}")
        return str(out)

    @task
    def load(summary_path: str) -> None:
        """3단계 · 웨어하우스 적재. 같은 날짜를 몇 번 돌려도 중복되지 않는다."""
        ds = _ds()
        rows = list(csv.reader(Path(summary_path).read_text(encoding="utf-8").splitlines()))
        header, data_rows = rows[0], rows[1:]

        WAREHOUSE.mkdir(parents=True, exist_ok=True)
        warehouse = WAREHOUSE / "daily_sales.csv"

        # 멱등성: 이 ds 파티션을 먼저 걷어낸 뒤 다시 쓴다(delete-then-insert).
        # cron-demo 의 무조건 append 와 달리, 재실행해도 이 날짜 행은 딱 한 벌만 남는다.
        kept: list[list[str]] = []
        if warehouse.exists():
            old = list(csv.reader(warehouse.read_text(encoding="utf-8").splitlines()))
            kept = [r for r in old[1:] if r and r[0] != ds]

        with warehouse.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(kept + data_rows)
        print(f"[load] {len(data_rows)}행 적재(멱등) → {warehouse}")

    # 의존성 = 코드. cron 이 시간으로 추측하던 걸 여기선 한 줄로 선언한다.
    load(transform(extract()))


sales_etl()
