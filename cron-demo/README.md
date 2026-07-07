# cron-demo — cron 으로 데이터 파이프라인을 짜보면 아픈 이유

간단한 **ETL 파이프라인**(추출 → 가공 → 적재)을 오직 cron 으로만 운영해보는 데모입니다.
바로 돌아가고, 일부러 가끔 실패하도록 만들어서 **cron 의 한계를 손으로 만져보게** 하는 게 목적입니다.

> 이 데모의 결론은 "cron 이 나쁘다"가 아니라,
> **데이터가 복잡해질수록 cron 만으로는 감당이 안 되고, 오케스트레이터(예: Airflow)가 필요해진다** 입니다.
> 같은 파이프라인을 도구로 다시 짠 버전은 `../airflow-demo` 에 있습니다.

---

## 시나리오

매일 새벽, 지역별 주문 데이터를 처리하는 3단계 파이프라인:

```
extract          →   transform            →   load
주문 원본 추출        지역별 매출 집계           웨어하우스에 적재
raw/orders_*.json    staging/summary_*.csv    warehouse/daily_sales.csv
```

- `transform` 은 `extract` 결과에 **의존**한다.
- `load` 는 `transform` 결과에 **의존**한다.
- `extract` 는 가끔 실패한다(소스 장애). 그러면 뒤 단계도 다 틀어진다.

```
cron-demo/
├── crontab.txt        # 실제 등록 예시 (등록의 복잡함)
├── run_pipeline.sh    # 의존성을 흉내내는 래퍼 스크립트
├── jobs/
│   ├── common.py      # 로그·경로·실패 시뮬레이션 (전부 손으로)
│   ├── extract.py     # 1단계
│   ├── transform.py   # 2단계 (extract 에 의존)
│   └── load.py        # 3단계 (transform 에 의존)
├── data/              # 실행하면 생김 (raw / staging / warehouse)
└── logs/              # 잡별로 흩어지는 로그
```

---

## 빠른 시작

```bash
cd cron-demo

# 파이프라인 한 번 실행 (오늘 날짜로)
./run_pipeline.sh

# 결과 확인
cat data/warehouse/daily_sales.csv
```

개별 잡만 돌려볼 수도 있습니다:

```bash
python3 jobs/extract.py && python3 jobs/transform.py && python3 jobs/load.py
```

---

## 여기서 아픕니다 — cron 의 3가지 고통

### ① 작업 등록이 복잡하다

cron 에 거는 건 "명령 한 줄"이 아니라, 환경까지 통째로 손으로 세팅하는 일입니다.
[`crontab.txt`](./crontab.txt) 를 보세요.

```cron
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin
CRON_DEMO=/Users/yeonguk/Workshop/cron-demo
PY=/usr/bin/python3

0  2 * * *  cd $CRON_DEMO/jobs && $PY extract.py   >> $CRON_DEMO/logs/extract.log   2>&1
10 2 * * *  cd $CRON_DEMO/jobs && $PY transform.py >> $CRON_DEMO/logs/transform.log 2>&1
20 2 * * *  cd $CRON_DEMO/jobs && $PY load.py      >> $CRON_DEMO/logs/load.log      2>&1
```

- **절대경로**를 다 박아야 한다 (`cd`, python 경로, 로그 경로).
- **PATH/SHELL** 을 직접 선언 안 하면 `command not found` 로 조용히 실패한다.
- 로그를 남기려면 매 줄 `>> ... 2>&1` 를 손으로 붙여야 한다.
- 그리고 결정적으로 — **순서를 표현할 방법이 없어서 시간(2:10, 2:20)으로 추측**한다.

### ② 과거 로그·실행 이력을 파악하기 어렵다

로그가 잡마다 다른 파일로 흩어집니다. "어제 파이프라인이 성공했나?"에 답하려면:

```bash
# 세 파일을 각각 열어서 시간순으로 눈으로 짜맞춰야 한다
tail logs/extract.log
tail logs/transform.log
tail logs/load.log
```

- 한 번의 "파이프라인 실행"을 통째로 보여주는 화면이 없다.
- 성공/실패 여부, 소요 시간, 처리 건수를 구조적으로 조회할 수 없다 → 매번 `grep`.
- 며칠 지난 실행 내역? cron 은 **아무것도 기록해두지 않는다.** 우리가 로그를 안 남겼으면 그냥 사라진다.

### ③ 복잡한 파이프라인(의존성)을 만들기 어렵다

extract 를 **일부러 실패**시키고 뒤 단계가 어떻게 되는지 보세요.

```bash
# 처리한 적 없는 날짜로 extract 를 강제 실패시킴
FORCE_FAIL=1 RUN_DATE=2099-01-01 python3 jobs/extract.py ; echo "exit=$?"

# cron '방법 A'처럼, transform 은 앞 단계 실패를 모른 채 그냥 실행된다
RUN_DATE=2099-01-01 python3 jobs/transform.py ; echo "exit=$?"
#  → "extract 산출물 없음" 에러. cron 은 이 의존을 막아주지 못한다.
```

시간 간격(방법 A)으로 순서를 흉내내면 이런 일이 벌어집니다:
- extract 가 10분보다 오래 걸리면 → transform 이 **빈/이전 데이터로 돌아간다.**
- extract 가 실패하면 → transform·load 는 그것도 모르고 **그냥 돈다.**

> 더 무서운 건 **조용한 오염**: 어제 성공한 파일이 남아있는 날 extract 가 실패하면,
> transform 이 **어제 데이터로 "성공"** 해버리고 아무도 눈치채지 못한다.

그래서 `run_pipeline.sh`(방법 B)처럼 래퍼를 만들지만, 그걸로도:
- **재시도**(실패하면 3번까지 다시) 없음
- **백필**(과거 3일치 다시 처리) 없음 — 날짜를 손으로 바꿔 반복 실행해야 함:
  ```bash
  for d in 2026-07-01 2026-07-02 2026-07-03; do RUN_DATE=$d ./run_pipeline.sh; done
  ```
- **멱등성** 없음 — 같은 날짜를 두 번 돌리면 웨어하우스에 **중복 적재**됨:
  ```bash
  ./run_pipeline.sh && ./run_pipeline.sh
  wc -l data/warehouse/daily_sales.csv   # 행이 두 배로 늘어난다
  ```
- **알림·모니터링·부분 재실행·병렬 실행** — 전부 없음. 필요하면 직접 만들어야 함.

---

## 그래서 — 무엇이 필요한가

데이터가 복잡해질수록(빅데이터 시대) 위 세 가지 고통은 폭발합니다.
결국 이런 것들이 **필요**해집니다:

| 필요한 것 | cron | 오케스트레이터(예: Airflow) |
|---|---|---|
| 복잡한 워크플로우 제어 (의존 그래프) | 시간으로 추측 | 코드로 `extract >> transform >> load` |
| 스케줄링 (배치 + 이벤트) | 시간 배치만 | 시간·데이터·센서 기반 |
| 실행 이력 모니터링 (대시보드) | 없음 (직접 grep) | UI 로 한눈에 |
| 에러 로그 확인 & 재실행 | 로그 흩어짐, 수동 | 태스크 단위 로그 + 클릭 재실행 |
| 재시도 · 백필 · 멱등성 | 직접 구현 | 프레임워크 기본 제공 |

> 다음 단계 → 같은 파이프라인을 오케스트레이터로 다시 짠 [`../airflow-demo`](../airflow-demo) 와 비교해 보세요.

---

## 정리 / 초기화

```bash
rm -rf data logs   # 생성된 데이터와 로그 삭제
crontab -r         # (등록했다면) crontab 해제
```
