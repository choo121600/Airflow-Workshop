"""cron-demo 잡들이 공유하는 최소 유틸.

일부러 Airflow 같은 오케스트레이터의 편의 기능을 하나도 쓰지 않는다.
로그 남기기 · 경로 잡기 · 실패 시뮬레이션을 전부 '손으로' 만들어야 한다는 걸
보여주기 위한 코드다. (airflow-demo 에서는 이게 전부 프레임워크가 해준다.)
"""
from __future__ import annotations

import os
import random
from datetime import date, datetime
from pathlib import Path

# cron 은 실행 시 cwd 가 홈 디렉터리다. 상대경로에 기대면 깨진다.
# 그래서 스크립트 위치 기준으로 절대경로를 직접 계산해 둔다.
BASE_DIR = Path(__file__).resolve().parent.parent  # cron-demo/
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
WAREHOUSE_DIR = DATA_DIR / "warehouse"


def run_date() -> str:
    """처리 대상 날짜.

    cron 은 '지금'밖에 모른다. 과거 날짜를 다시 처리(백필)하려면
    이 값을 손으로 바꿔서 재실행하는 수밖에 없다.
    """
    return os.environ.get("RUN_DATE", date.today().isoformat())


def log(job: str, message: str) -> None:
    """잡별 로그 파일에 한 줄씩 append.

    실행 이력이 잡마다 다른 파일로 흩어진다 → 한 번의 '파이프라인 실행'을
    통째로 보려면 여러 파일을 시간순으로 짜맞춰야 한다.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    line = f"{datetime.now().isoformat(timespec='seconds')} [{job}] {message}"
    print(line)  # stdout → cron 에선 리다이렉션하지 않으면 그대로 사라진다
    with open(LOG_DIR / f"{job}.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")


def maybe_fail(job: str, default_rate: float = 0.0) -> None:
    """소스/네트워크 장애를 흉내낸다.

    - FORCE_FAIL=1  → 무조건 실패 (발표에서 실패 시나리오를 확실히 보여줄 때)
    - FAIL_RATE=0.3 → 30% 확률로 실패 (평소의 '가끔 깨지는' 현실)

    실패했을 때 → 어떻게 알아채지? 어디 로그를 보지? 어떻게 재실행하지?
    이 질문들이 곧 오케스트레이터가 필요한 이유가 된다.
    """
    if os.environ.get("FORCE_FAIL") == "1":
        raise RuntimeError(f"{job}: 강제 실패 (FORCE_FAIL=1)")
    rate = float(os.environ.get("FAIL_RATE", default_rate))
    if rate > 0 and random.random() < rate:
        raise RuntimeError(f"{job}: 일시적 장애 발생 (FAIL_RATE={rate})")
