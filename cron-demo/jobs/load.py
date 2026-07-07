#!/usr/bin/env python3
"""3단계 · load — 집계 결과를 웨어하우스 테이블에 적재.

transform 산출물(data/staging/summary_<date>.csv)에 '의존'한다.
같은 날짜를 두 번 적재하면? 그대로 중복된다.
멱등성(같은 걸 두 번 돌려도 결과가 같음)도 cron 은 안 챙겨준다 → 직접 관리해야 한다.
"""
from __future__ import annotations

import csv
import sys
import time

from common import STAGING_DIR, WAREHOUSE_DIR, log, maybe_fail, run_date

JOB = "load"


def main() -> int:
    d = run_date()
    start = time.time()
    log(JOB, f"시작 (run_date={d})")
    src = STAGING_DIR / f"summary_{d}.csv"
    try:
        if not src.exists():
            raise FileNotFoundError(
                f"transform 산출물 없음: {src.name} "
                f"(transform 이 실패했거나 아직 안 끝났을 수 있음)"
            )
        maybe_fail(JOB, default_rate=0.1)

        rows = list(csv.reader(src.read_text(encoding="utf-8").splitlines()))
        header, data_rows = rows[0], rows[1:]

        WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
        warehouse = WAREHOUSE_DIR / "daily_sales.csv"
        new_file = not warehouse.exists()

        # 멱등성 없음: 같은 날짜를 또 돌리면 검사 없이 그대로 append → 중복 행 발생.
        with open(warehouse, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(header)
            w.writerows(data_rows)

        log(JOB, f"완료: {len(data_rows)}행 적재 → {warehouse.name} ({time.time() - start:.2f}s)")
        return 0
    except Exception as e:  # noqa: BLE001
        log(JOB, f"실패: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
