#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# cron 한 줄로는 '순서'와 '의존'을 표현할 수 없다.
# 그래서 결국 이런 래퍼 스크립트를 직접 만들게 된다.
#
# 이 스크립트가 해주는 것:  순서대로 실행 + 실패하면 중단
# 이 스크립트가 못 해주는 것: 재시도 · 백필 · 알림 · 모니터링 대시보드
#                            · 병렬 실행 · 의존 그래프 · 부분 재실행
# → 결국 그 전부를 직접 만들어야 하고, 이게 오케스트레이터가 필요한 이유다.
# ─────────────────────────────────────────────────────────────────────
set -u

# cron 은 최소한의 환경만 준다. 경로/파이썬을 직접 잡아줘야 한다.
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOBS_DIR="$BASE_DIR/jobs"
LOG_DIR="$BASE_DIR/logs"
PYTHON="${PYTHON:-python3}"
mkdir -p "$LOG_DIR"

RUN_DATE="${RUN_DATE:-$(date +%F)}"
export RUN_DATE

run_step () {
  local name="$1"
  echo ">>> [$name] 시작 (run_date=$RUN_DATE)"
  # cd 를 안 하면 상대경로/모듈 import 가 깨진다 (cron 의 흔한 함정)
  if ! (cd "$JOBS_DIR" && "$PYTHON" "$name.py"); then
    echo "!!! [$name] 실패 → 파이프라인 중단"
    echo "    로그 확인: $LOG_DIR/$name.log"
    exit 1
  fi
  echo "<<< [$name] 성공"
}

# 의존성 = 그냥 '순서대로 돌리고, 실패하면 멈추기'. 우리가 표현할 수 있는 건 이게 전부다.
run_step extract
run_step transform
run_step load

echo "=== 파이프라인 완료 (run_date=$RUN_DATE) ==="
