"""3주차 4 — 외부 연결 (현실: 실제 Postgres 웨어하우스에 적재).

2주차 sales_etl 은 CSV 파일에 썼다. 그건 데모였고 실무는 DB 다. 같은 결의 ETL 을
`demo_postgres` Connection + `PostgresHook` 으로 진짜 Postgres 에 적재한다.

배우는 것:
  * Connection/Hook       — 비밀번호를 코드에 두지 않고 conn_id 로 DB 접속
  * 읽기 + 쓰기            — orders/customers 를 지역별로 집계해 요약 테이블에 적재
  * 멱등성(2주차 재등장)  — 같은 스냅샷 날짜를 여러 번 돌려도 중복되지 않게
                           delete-then-insert. 파일 버전과 똑같은 원리, 대상이 DB 일 뿐.
"""

from __future__ import annotations

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task

CONN_ID = "demo_postgres"


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["hook", "etl"],
    doc_md=__doc__,
)
def sales_etl_to_postgres():
    @task
    def ensure_table() -> None:
        # 요약 테이블이 없으면 만든다. (snapshot_date, region) 이 한 벌.
        PostgresHook(postgres_conn_id=CONN_ID).run(
            """
            CREATE TABLE IF NOT EXISTS region_sales_summary (
                snapshot_date DATE           NOT NULL,
                region        TEXT           NOT NULL,
                order_count   INT            NOT NULL,
                revenue       NUMERIC(14, 2) NOT NULL,
                PRIMARY KEY (snapshot_date, region)
            )
            """
        )

    @task
    def load_summary() -> None:
        ds = get_current_context()["ds"]
        hook = PostgresHook(postgres_conn_id=CONN_ID)

        # 읽기: 지역(customers.region)별 정상 주문 수·매출 집계.
        rows = hook.get_records(
            """
            SELECT c.region, count(*), coalesce(sum(o.amount), 0)
            FROM orders o
            JOIN customers c ON c.customer_id = o.customer_id
            WHERE o.status = 'completed' AND o.amount > 0
            GROUP BY c.region
            ORDER BY c.region
            """
        )

        # 멱등: 이 스냅샷 날짜 파티션을 먼저 지우고 다시 넣는다.
        # 같은 ds 를 몇 번 재실행해도 이 날짜 행은 딱 한 벌만 남는다(파일 버전과 동일).
        hook.run("DELETE FROM region_sales_summary WHERE snapshot_date = %s", parameters=(ds,))
        hook.insert_rows(
            table="region_sales_summary",
            rows=[(ds, region, cnt, rev) for region, cnt, rev in rows],
            target_fields=["snapshot_date", "region", "order_count", "revenue"],
        )
        print(f"[적재] {len(rows)}개 지역 요약 → region_sales_summary (snapshot={ds})")

    ensure_table() >> load_summary()


sales_etl_to_postgres()
