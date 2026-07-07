"""Common AI provider 데모 — 에이전트 기반 데이터 조사.

``apache-airflow-providers-common-ai``의 또 다른 두 데코레이터를 보여준다:

* ``@task.llm_sql`` – 자연어 질문을 대상 테이블에 대한 SQL 쿼리로 변환한다
                      (선택적으로 문법 검증).
* ``@task.agent``   – ``SQLToolset``을 도구로 쥔 다단계 pydantic-ai 에이전트로,
                      필요한 만큼 반복적으로 DB를 조회해 **구조화된** 결론에
                      도달한다.

샘플 ``demo_postgres`` 데이터베이스(docker-compose.override.yml,
include/seed/01_schema.sql 참고)와 ``pydanticai_default`` 연결이 필요하다.
"""

from __future__ import annotations

from typing import Literal

from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.sdk import dag, task
from pydantic import BaseModel, Field

# 모델을 바꾸려면 이 한 줄만 수정하면 된다 (예: "anthropic:claude-opus-4-8").
MODEL_ID = "anthropic:claude-sonnet-5"
DB_CONN_ID = "demo_postgres"


class AnomalyReport(BaseModel):
    """조사 에이전트가 산출하는 구조화된 결론.

    모든 필드에 기본값을 둔다: 도구 사용과 구조화 출력을 함께 쓰는 에이전트는
    가끔 일부 필드를 빠뜨린 채 응답하는데, 기본값이 있으면 그런 부분 응답도
    검증을 통과해 태스크가 실패하지 않는다.
    """

    severity: Literal["low", "medium", "high"] = Field(
        default="medium", description="발견 사항들의 전체 위험도"
    )
    summary: str = Field(default="", description="무엇을 발견했는지에 대한 요약")
    suspect_order_ids: list[int] = Field(
        default_factory=list, description="수상해 보이는 order_id 값 목록"
    )
    recommended_actions: list[str] = Field(
        default_factory=list, description="담당 분석가를 위한 구체적인 후속 조치"
    )


@dag(schedule=None, catchup=False, tags=["common-ai", "demo"])
def data_investigation():
    @task.llm_sql(
        llm_conn_id="pydanticai_default",
        db_conn_id=DB_CONN_ID,
        model_id=MODEL_ID,
        table_names=["orders", "customers"],
        validate_sql=True,
    )
    def top_customers(question: str) -> str:
        # 이 질문이 SQL로 컴파일되어 demo_postgres에서 실행된다.
        return question

    @task.agent(
        llm_conn_id="pydanticai_default",
        model_id=MODEL_ID,
        system_prompt=(
            "당신은 이상거래/데이터 품질 조사관입니다. SQL 도구를 사용해 "
            "`orders`와 `customers` 테이블을 탐색하세요. 특히 다음을 찾으세요: "
            "중복 결제(같은 고객이 짧은 시간 안에 같은 금액 결제), 환불이 아닌 "
            "상태로 남아 있는 음수 금액, 극단적으로 큰 이상치 금액. 필요한 만큼 "
            "여러 번 쿼리하세요.\n\n"
            "조사가 끝나면 반드시 최종 보고서의 모든 필드를 채워서 반환하세요: "
            "severity, summary, suspect_order_ids, recommended_actions. "
            "필드 값 안에는 순수한 내용만 담고, 도구 호출 문법이나 XML 태그를 "
            "절대 포함하지 마세요."
        ),
        output_type=AnomalyReport,
        toolsets=[
            SQLToolset(
                db_conn_id=DB_CONN_ID,
                allowed_tables=["orders", "customers"],
                allow_writes=False,
            )
        ],
        enable_tool_logging=True,
    )
    def investigate(prompt: str) -> str:
        return prompt

    top_customers("총 주문 금액이 가장 큰 상위 5개 고객은 누구인가요?")
    investigate(
        "orders 테이블에서 수상한 거래를 조사하고, 관련된 구체적인 order_id와 "
        "함께 위험도를 요약해 주세요."
    )


data_investigation()
