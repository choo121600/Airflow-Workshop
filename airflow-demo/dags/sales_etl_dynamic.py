"""3주차 1 — Dynamic Task Mapping (동적 태스크 매핑).

2주차 `sales_etl` 은 `extract >> transform >> load` 로 **태스크 개수가 고정**이었다.
transform 한 태스크가 모든 지역을 루프로 한 방에 집계했다. 그런데 현실은:

* 지점(지역)이 계속 는다. 오늘 4개, 다음 달 10개. 그때마다 코드를 고칠 순 없다.
* S3 에 매일 들어오는 주문 파일 개수가 그날그날 다르다(어제 3개, 오늘 50개).
* 한 태스크에서 다 처리하면, **부산 데이터 하나가 깨질 때 서울·제주 집계까지
  통째로 실패**한다.

Dynamic Task Mapping 은 "입력 개수를 런타임에 알게 되는" 이 상황의 정답이다.
`.expand()` 로 upstream 이 반환한 리스트의 **원소 수만큼 태스크를 자동 생성**한다.

| 2주차 sales_etl (정적)                     | 이 Dag (동적)                                   |
|-------------------------------------------|------------------------------------------------|
| transform 1개가 모든 지역을 루프로 처리     | 지역마다 process_region 인스턴스 1개 (`.expand`) |
| 지역 추가 = 코드 수정                       | 지역 리스트만 늘면 태스크가 알아서 늘어남          |
| 한 지역이 깨지면 전체 transform 실패        | 그 지역 인스턴스만 실패(격리) — 나머진 진행        |
| 재시도도 전체 단위                          | 매핑 인스턴스마다 독립적으로 retries              |

흐름:  extract_regions() ──fan-out──▶ process_region[지역별] ──fan-in──▶ combine()
       (오늘 처리할 지역 발견)          (지역 하나씩 독립 집계)        (전부 모아 적재)
"""

from __future__ import annotations

import csv
import json
import random
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task

# 2주차 sales_etl 과 같은 레이아웃을 그대로 쓴다.
BASE = Path("/usr/local/airflow/include/etl_demo")
RAW, STAGING, WAREHOUSE = BASE / "raw", BASE / "staging", BASE / "warehouse"

# "코드에 박힌 상수"처럼 보이지만, 실무에선 이 목록이 런타임에 정해진다 —
# S3 버킷의 파일 목록, 설정 테이블의 활성 지점, API 가 알려주는 페이지 수 등.
ALL_REGIONS = ["seoul", "busan", "jeju", "daegu", "incheon", "gwangju"]


def _ds() -> str:
    return get_current_context()["ds"]


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["dynamic-mapping", "etl"],
    default_args={"retries": 3, "retry_delay": timedelta(seconds=15)},
    doc_md=__doc__,
)
def sales_etl_dynamic():
    @task
    def extract_regions() -> list[str]:
        """오늘 '실제로 데이터가 들어온' 지역 목록을 런타임에 발견한다.

        핵심은 개수를 코드에 박지 않는 것. 신규 지점이 열려도(=리스트가 길어져도)
        아래 process_region 태스크가 그 수만큼 자동으로 늘어난다 — 코드 수정 0.
        """
        # 데모: 매일 임의의 부분집합이 들어온다고 가정(어떤 날은 4개, 어떤 날은 6개).
        active = random.sample(ALL_REGIONS, k=random.randint(4, len(ALL_REGIONS)))
        print(f"[extract] 오늘 처리할 지역 {len(active)}개: {active}")
        return active  # 이 리스트의 길이가 곧 매핑될 태스크 개수가 된다

    @task
    def process_region(region: str) -> dict:
        """지역 '하나'를 독립적으로 추출+집계한다.

        이 함수는 지역 1개만 안다. Airflow 가 `.expand(region=...)` 로 지역마다
        이 함수의 인스턴스를 따로 띄운다. 그래서:
          * 부산 인스턴스가 실패해도 서울 인스턴스는 영향 없다(실패 격리).
          * 각 인스턴스가 독립적으로 retries=3 을 적용받는다.
        """
        ds = _ds()
        # 지역별 소스가 가끔 죽는다. 하지만 죽는 건 '이 지역 인스턴스'뿐 —
        # 전체 파이프라인이 아니라. 그리고 그마저도 자동 재시도된다.
        if random.random() < 0.15:
            raise RuntimeError(f"[{region}] 소스 일시 장애 — 이 인스턴스만 재시도된다")

        n = random.randint(20, 80)
        revenue = sum(random.randint(1000, 50000) for _ in range(n))

        RAW.mkdir(parents=True, exist_ok=True)
        (RAW / f"orders_{region}_{ds}.json").write_text(
            json.dumps({"region": region, "orders": n, "revenue": revenue}),
            encoding="utf-8",
        )
        print(f"[{region}] 주문 {n}건 · 매출 {revenue:,}원")
        return {"region": region, "orders": n, "revenue": revenue}

    @task
    def combine(results: list[dict]) -> None:
        """fan-in — 매핑된 모든 지역 결과가 리스트 하나로 모여 들어온다.

        `.expand()` 로 펼쳐진 인스턴스들의 리턴값을 Airflow 가 리스트로 모아
        이 downstream 태스크에 통째로 넘긴다. 여기서 웨어하우스에 적재한다.
        """
        ds = _ds()
        # 매핑 인스턴스 중 일부가 최종 실패(재시도 소진)하면 그 결과는 빠질 수 있다.
        results = [r for r in results if r]

        STAGING.mkdir(parents=True, exist_ok=True)
        out = STAGING / f"summary_{ds}.csv"
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "region", "orders", "revenue"])
            for r in sorted(results, key=lambda x: x["region"]):
                w.writerow([ds, r["region"], r["orders"], r["revenue"]])

        total = sum(r["revenue"] for r in results)
        print(f"[combine] {len(results)}개 지역 집계 → {out} (총매출 {total:,}원)")

    # fan-out(.expand) → fan-in(combine) 을 한 줄로.
    # extract_regions() 가 6개를 반환하면 process_region 인스턴스가 6개 뜨고,
    # combine() 은 그 6개가 모두 끝난 뒤 결과 리스트를 받는다.
    combine(process_region.expand(region=extract_regions()))


sales_etl_dynamic()
