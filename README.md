# cron 과 Airflow 비교 데모

같은 ETL 파이프라인(주문 추출 → 지역별 집계 → 웨어하우스 적재)을 cron 과 Airflow 로
각각 구현했다. 데이터 파이프라인이 복잡해질 때 cron 이 부딪히는 한계와,
오케스트레이터가 그 한계를 어떻게 다루는지 비교하려는 예제다.

```
Workshop/
├── cron-demo/      # cron 으로 구현한 파이프라인
└── airflow-demo/   # 같은 파이프라인을 Airflow Dag 로 구현
```

두 데모는 같은 로직과 산출물 레이아웃을 공유한다.

```
extract                   transform                    load
주문 원본 추출        →     지역별 매출 집계        →     웨어하우스 적재
raw/orders_<날짜>.json     staging/summary_<날짜>.csv   warehouse/daily_sales.csv
```

## cron 의 한계

- **작업 등록**: crontab 에 절대경로·PATH·로그 리다이렉션을 직접 써야 하고,
  작업 순서는 실행 시각(2:10, 2:20)으로 추측해야 한다.
- **로그**: 잡별로 로그 파일이 흩어져 실행 이력을 추적하기 번거롭다.
- **파이프라인**: 앞 단계가 실패하거나 늦어져도 뒷 단계는 그대로 실행된다.
  재시도·백필·멱등성은 직접 구현해야 한다.

## 같은 문제를 Airflow 가 다루는 방식

| 항목 | cron-demo | airflow-demo |
|---|---|---|
| 작업 의존성 | 실행 시각으로 추측 | 코드로 선언 (`load(transform(extract()))`) |
| 앞 단계 실패 시 | 뒷 단계가 그대로 실행 | downstream 자동 skip |
| 소스 장애 | 그대로 실패 | `retries` 로 재시도 |
| 로그 | 파일로 흩어짐 | 태스크별 로그 + Web UI |
| 백필 | 날짜를 바꿔 반복 실행 | data interval(`ds`)로 처리 |
| 재실행 | append 되어 중복 | 날짜 파티션 덮어쓰기 |

## 실행

cron-demo:

```bash
cd cron-demo
./run_pipeline.sh
```

airflow-demo (스케줄러 컨테이너 안에서 한 번 실행):

```bash
docker exec <scheduler> airflow dags test sales_etl 2026-07-07
```

Web UI 는 localhost:8080 의 `sales_etl` Dag.

자세한 내용은 각 폴더 문서를 참고한다:
[cron-demo/README.md](./cron-demo/README.md),
[airflow-demo/dags/sales_etl.py](./airflow-demo/dags/sales_etl.py)
