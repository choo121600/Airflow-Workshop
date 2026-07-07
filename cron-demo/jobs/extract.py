#!/usr/bin/env python3
"""1단계 · extract — 주문 원본 데이터 추출.

실제라면 외부 API나 운영 DB에서 당일 주문을 끌어온다.
여기선 랜덤 주문을 생성해 data/raw/orders_<date>.json 으로 저장한다.
소스는 가끔 죽는다(FAIL_RATE) — 현실의 데이터 소스가 그렇듯이.
"""
from __future__ import annotations

import json
import random
import sys
import time

from common import RAW_DIR, log, maybe_fail, run_date

JOB = "extract"
REGIONS = ["seoul", "busan", "jeju", "daegu"]


def main() -> int:
    d = run_date()
    start = time.time()
    log(JOB, f"시작 (run_date={d})")
    try:
        maybe_fail(JOB, default_rate=0.2)  # 소스가 가끔 응답을 안 준다

        n = random.randint(50, 150)
        orders = [
            {
                "order_id": i,
                "region": random.choice(REGIONS),
                "amount": random.randint(1000, 50000),
            }
            for i in range(n)
        ]
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        out = RAW_DIR / f"orders_{d}.json"
        out.write_text(json.dumps(orders, ensure_ascii=False, indent=2), encoding="utf-8")

        log(JOB, f"완료: 주문 {n}건 저장 → {out.name} ({time.time() - start:.2f}s)")
        return 0
    except Exception as e:  # noqa: BLE001
        log(JOB, f"실패: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
