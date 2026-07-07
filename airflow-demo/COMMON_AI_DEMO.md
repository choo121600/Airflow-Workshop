# Common AI provider 데모

[`apache-airflow-providers-common-ai`](https://airflow.apache.org/registry/providers/common-ai/0.5.0/)
(pydantic-ai 기반 LLM task)를 Airflow 3.x 위에서 보여주는 Dag 두 개.

| Dag | 데코레이터 | 필요한 것 |
|-----|-----------|-----------|
| `ticket_triage` | `@task.llm`, `@task.llm_branch` | Anthropic API 키만 |
| `data_investigation` | `@task.llm_sql`, `@task.agent` + `SQLToolset` | API 키 + 샘플 Postgres |

## 설정

1. **Anthropic API 키 입력** — `airflow_settings.yaml`을 열어
   `pydanticai_default` 연결의 `conn_password`에 실제 키를 넣는다.

2. **(선택) 모델 변경** — 두 Dag는 각 파일 상단의 `MODEL_ID` 상수와 연결의
   `extra.model`을 통해 `anthropic:claude-sonnet-5`를 사용한다. 한 곳만 바꾸면
   모델을 교체할 수 있다 (예: `anthropic:claude-opus-4-8`).

3. **시작** — 이미지를 빌드(provider 설치)하고 `docker-compose.override.yml`의
   샘플 Postgres를 함께 띄운다:

   ```bash
   astro dev start
   ```

   Airflow UI: http://localhost:8080 · 샘플 DB(호스트 쪽): `localhost:5433`.

4. **실행** — UI에서 `ticket_triage`(DB 불필요)를 먼저 트리거하고, 그다음
   `data_investigation`을 트리거한다.

## 무엇을 보면 되나

- **`ticket_triage`** → `summarize`가 구조화된 `TicketSummary`를 XCom에 담고,
  `route`는 LLM이 판단하는 분기다. "두 번 청구됐다"는 티켓이면 `handle_billing`이
  켜지고 나머지 두 개는 skip되어야 한다.
- **`data_investigation`** → 에이전트가 `orders`/`customers`를 반복 조회해
  심어 둔 이상치 3개(중복 결제 #1008·#1009, 음수 금액 #1010, 극단 이상치 #1011)를
  `AnomalyReport`에 담아내야 한다. task 로그에서 `enable_tool_logging` 출력으로
  매 SQL 호출을 확인할 수 있다.

## 파일 구성

```
dags/ticket_triage.py         @task.llm + @task.llm_branch
dags/data_investigation.py    @task.llm_sql + @task.agent (SQLToolset)
include/seed/01_schema.sql     샘플 customers/orders (+ 의도적 이상치)
docker-compose.override.yml    demo-postgres 서비스
airflow_settings.yaml          pydanticai_default + demo_postgres 연결
requirements.txt               apache-airflow-providers-common-ai[anthropic,sql] 등
```
