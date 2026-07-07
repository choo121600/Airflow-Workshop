#!/usr/bin/env python3
"""2단계 · transform — 원본 주문을 지역별 일 매출로 집계.

extract 산출물(data/raw/orders_<date>.json)에 '의존'한다.
그런데 cron 은 이 의존을 모른다:
  - extract 가 아직 안 끝났어도 이 잡은 시간이 되면 그냥 실행된다.
  - extract 가 실패했어도 이 잡은 그것도 모르고 실행된다.
그래서 의존성 방어를 여기서 '손으로' 해야 한다.
"""
from __future__ import annotations

import csv
import json
import sys
import time
from collections import defaultdict

from common import RAW_DIR, STAGING_DIR, log, maybe_fail, run_date

JOB = "transform"


def main() -> int:
    d = run_date()
    start = time.time()
    log(JOB, f"시작 (run_date={d})")
    src = RAW_DIR / f"orders_{d}.json"
    try:
        # cron 이 순서를 보장해주지 않으니, 입력이 있는지 직접 확인해야 한다.
        if not src.exists():
            raise FileNotFoundError(
                f"extract 산출물 없음: {src.name} "
                f"(extract 가 실패했거나 아직 안 끝났을 수 있음)"
            )
        maybe_fail(JOB, default_rate=0.1)

        orders = json.loads(src.read_text(encoding="utf-8"))
        agg: dict[str, dict[str, int]] = defaultdict(lambda: {"orders": 0, "revenue": 0})
        for o in orders:
            agg[o["region"]]["orders"] += 1
            agg[o["region"]]["revenue"] += o["amount"]

        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        out = STAGING_DIR / f"summary_{d}.csv"
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "region", "orders", "revenue"])
            for region, v in sorted(agg.items()):
                w.writerow([d, region, v["orders"], v["revenue"]])

        log(JOB, f"완료: {len(agg)}개 지역 집계 → {out.name} ({time.time() - start:.2f}s)")
        return 0
    except Exception as e:  # noqa: BLE001
        log(JOB, f"실패: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
