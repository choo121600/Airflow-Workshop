"""3주차 1 — Dynamic Task Mapping 최소 예제.

`.expand()` **한 줄만** 본다. 나머지는 전부 걷어냈다(파일 I/O·날짜·재시도 없음).

지금까지 태스크는 코드에 쓴 만큼만 생겼다 — `hello()` 라고 쓰면 1개.
그런데 "처리할 개수를 실행해봐야 아는" 경우가 있다:

* S3 에 오늘 들어온 파일이 몇 개인지 (어제 3개, 오늘 50개)
* API 가 알려주는 페이지 수
* 설정 테이블에 등록된 활성 지점 수

이럴 때 `downstream.expand(인자=리스트)` 를 쓰면, upstream 이 반환한
**리스트의 원소 수만큼 downstream 태스크가 자동으로 생긴다.**

  get_files() 가 3개를 반환  →  process 태스크가 3개 생김
  get_files() 가 50개를 반환 →  process 태스크가 50개 생김   (코드 수정 0)

UI 의 Graph 뷰에서 `process [ ]` 옆에 매핑된 개수가 숫자로 붙는 걸 확인해보자.
개념을 잡았으면, 이걸 실제 ETL 에 적용한 `sales_etl_dynamic.py` 로 넘어간다.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(schedule=None, catchup=False, tags=["dynamic-mapping", "101"])
def hello_dynamic():
    @task
    def get_files() -> list[str]:
        # 오늘 처리할 목록. 개수는 런타임에 정해진다(여기선 3개지만 몇 개든 상관없으ㅁ.
        return ["a.csv", "b.csv", "c.csv"]

    @task
    def process(filename: str) -> int:
        # 이 함수는 '파일 1개'만 안다. Airflow 가 목록 개수만큼 이 함수를 띄운다.
        print(f"처리 중: {filename}")
        return len(filename)

    @task
    def summarize(sizes: list[int]) -> None:
        # fan-in — 매핑된 인스턴스들의 리턴값이 리스트로 모여 들어온다.
        print(f"파일 {len(sizes)}개 처리 완료, 크기 합계 {sum(sizes)}")

    # 이 한 줄이 전부다: get_files 가 준 리스트만큼 process 가 펼쳐지고(fan-out),
    # 그 결과가 summarize 로 모인다(fan-in).
    summarize(process.expand(filename=get_files()))


hello_dynamic()
