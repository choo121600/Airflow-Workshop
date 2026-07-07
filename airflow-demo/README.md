# airflow-demo

Astro CLI 기반 Apache Airflow 3.x 프로젝트. 두 갈래의 예제 Dag가 들어 있다.

| Dag | 내용 | 외부 의존 | 문서 |
|-----|------|-----------|------|
| `sales_etl` | 주문 ETL(extract → transform → load). cron 과 비교하기 위한 예제 | 없음 | [../README.md](../README.md) |
| `ticket_triage` | LLM 으로 지원 티켓 분류·분기 | Anthropic API 키 | [COMMON_AI_DEMO.md](./COMMON_AI_DEMO.md) |
| `data_investigation` | LLM SQL 에이전트로 데이터 이상치 조사 | API 키 + 샘플 Postgres | [COMMON_AI_DEMO.md](./COMMON_AI_DEMO.md) |

`sales_etl` 은 외부 서비스 없이 바로 돌아가고, common-ai 계열 두 개는 API 키 설정이 필요하다.

## 실행

```bash
astro dev start
```

Airflow UI: http://localhost:8080 · 샘플 DB(호스트 쪽): `localhost:5433`.
`docker-compose.override.yml` 의 `demo-postgres` 도 함께 뜬다.

Dag 를 한 번만 동기 실행하려면(스케줄러 컨테이너 안):

```bash
docker exec <scheduler> airflow dags test sales_etl 2026-07-07
```

`<scheduler>` 는 `docker ps | grep scheduler` 로 확인한 컨테이너 이름.

## 구조

```
dags/
  sales_etl.py            주문 ETL (cron-demo 와 짝)
  ticket_triage.py        @task.llm + @task.llm_branch
  data_investigation.py   @task.llm_sql + @task.agent (SQLToolset)
include/
  seed/01_schema.sql      샘플 customers/orders (common-ai 데모용)
  etl_demo/               sales_etl 실행 산출물 (raw / staging / warehouse)
docker-compose.override.yml   demo-postgres 서비스
airflow_settings.yaml         연결·변수 설정
requirements.txt              provider 패키지
```

`include/etl_demo/` 는 `sales_etl` 을 실행하면 생기는 산출물이라 지워도 된다.
common-ai 데모의 설정·확인 방법은 [COMMON_AI_DEMO.md](./COMMON_AI_DEMO.md) 를 참고한다.
