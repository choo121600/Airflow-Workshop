"""Common AI provider 데모 — 고객 지원 티켓 자동 분류.

``apache-airflow-providers-common-ai``의 두 데코레이터를 보여준다:

* ``@task.llm``        – LLM을 한 번 호출해 **구조화된**(pydantic) 결과를
                         XCom에 반환한다.
* ``@task.llm_branch`` – LLM이 어떤 downstream task를 실행할지 결정한다.
                         즉 분기 "코드"가 프롬프트인 BranchOperator다.

외부 서비스는 필요 없고 ``pydanticai_default`` 연결(Anthropic API 키)만
있으면 된다. 설정은 COMMON_AI_DEMO.md / airflow_settings.yaml 참고.
"""

from __future__ import annotations

from typing import Literal

from airflow.sdk import dag, task
from pydantic import BaseModel, Field

# 모델을 바꾸려면 이 한 줄만 수정하면 된다. pydantic-ai가 이 값을 그대로
# Anthropic API로 전달하므로 유효한 Anthropic 모델 id면 무엇이든 동작한다
# (예: "anthropic:claude-opus-4-8").
MODEL_ID = "anthropic:claude-sonnet-5"

SAMPLE_TICKET = (
    "제목: 요금이 두 번 청구됐어요!!\n\n"
    "안녕하세요 — 이번 달 Pro 구독 요금이 같은 날 29,000원씩 두 번 결제된 걸 "
    "방금 확인했습니다. 계정은 하나뿐인데요. 중복 결제분을 환불해 주시고, "
    "다시는 이런 일이 없도록 조치해 주세요."
)


class TicketSummary(BaseModel):
    """요약 단계가 만들어내는 구조화된 결과."""

    subject: str = Field(description="티켓을 한 줄로 나타내는 짧은 제목")
    sentiment: Literal["분노", "불만", "중립", "만족"] = Field(
        description="고객의 감정 상태"
    )
    summary: str = Field(description="요청 내용을 1~2문장으로 요약")


def _as_text(summary) -> str:
    """XCom이 pydantic 모델 또는 dict로 돌려줄 수 있어 둘 다 처리한다."""
    if isinstance(summary, dict):
        return f"{summary.get('subject')} — {summary.get('summary')}"
    return f"{summary.subject} — {summary.summary}"


@dag(schedule=None, catchup=False, tags=["common-ai", "demo"])
def ticket_triage():
    @task.llm(
        llm_conn_id="pydanticai_default",
        model_id=MODEL_ID,
        system_prompt=(
            "당신은 고객 지원 분석가입니다. 지원 티켓을 읽고 간결한 "
            "구조화 요약을 작성하세요."
        ),
        output_type=TicketSummary,
        require_approval=False,
    )
    def summarize(ticket: str) -> str:
        # 이 함수가 반환하는 문자열이 LLM에 전달되는 사용자 프롬프트가 된다.
        return ticket

    @task.llm_branch(
        llm_conn_id="pydanticai_default",
        model_id=MODEL_ID,
        system_prompt=(
            "지원 티켓을 아래 팀 중 정확히 하나로 라우팅하세요. "
            "해당하는 task_id를 반환하면 됩니다:\n"
            "- handle_billing  : 결제, 환불, 청구서, 구독 관련\n"
            "- handle_technical: 버그, 오류, 장애, 로그인 문제\n"
            "- handle_general  : 그 외 / 일반 문의"
        ),
        allow_multiple_branches=False,
    )
    def route(summary) -> str:
        return _as_text(summary)

    @task
    def handle_billing(summary):
        print(f"[결제팀] 처리: {_as_text(summary)}")

    @task
    def handle_technical(summary):
        print(f"[기술팀] 처리: {_as_text(summary)}")

    @task
    def handle_general(summary):
        print(f"[일반문의팀] 처리: {_as_text(summary)}")

    ticket_summary = summarize(SAMPLE_TICKET)
    chosen = route(ticket_summary)

    # 분기 task는 모든 후보 branch의 바로 위(upstream)에 있어야 한다.
    chosen >> [
        handle_billing(ticket_summary),
        handle_technical(ticket_summary),
        handle_general(ticket_summary),
    ]


ticket_triage()
