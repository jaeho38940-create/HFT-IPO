# -*- coding: utf-8 -*-
"""
신규상장 종목 티커 설정 유틸리티.

전날 밤 실행하여 config/ticker.json 을 갱신합니다.
시장(코스피/코스닥)은 봇이 shcode로 자동 판별합니다.

사용 예:
  python set_ticker.py 012345
  python set_ticker.py 012345 --memo "내일 상장" --start 08:50:00
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime

CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config")
TICKER_PATH = os.path.join(CONFIG_DIR, "ticker.json")


def validate_shcode(shcode: str) -> str:
    code = shcode.strip()
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("shcode는 6자리 숫자여야 합니다.")
    return code


def save_ticker_config(data: dict) -> None:
    os.makedirs(CONFIG_DIR, exist_ok=True)
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(TICKER_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="신규상장 종목 티커 설정 (6자리 1개)")
    parser.add_argument("shcode", help="6자리 종목코드 (예: 012345)")
    parser.add_argument("--start", default="08:50:00", help="매매 시작 시각 HH:MM:SS")
    parser.add_argument("--memo", default="", help="메모")
    parser.add_argument("--isu", default="", help="주문 IsuNo 특수 케이스만 (기본 A+shcode)")
    args = parser.parse_args()

    try:
        shcode = validate_shcode(args.shcode)
        if not re.fullmatch(r"\d{2}:\d{2}:\d{2}", args.start):
            raise ValueError("start_time 형식은 HH:MM:SS 입니다.")

        payload = {
            "shcode": shcode,
            "start_time": args.start,
            "memo": args.memo or f"신규상장 {shcode}",
        }
        if args.isu.strip():
            payload["isu_no"] = args.isu.strip()
        save_ticker_config(payload)

        print(f"[OK] 저장 완료: {TICKER_PATH}")
        print(f"  종목: {shcode} (시장·IsuNo는 봇이 자동 처리)")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
