# -*- coding: utf-8 -*-
"""
LS증권(구 이베스트) xingAPI HFT 스캘핑 봇 — 듀얼 스레드 Producer-Consumer.

[메인 스레드]  COM 이벤트 펌프 + XAReal 수신 → tick_queue / fill_queue 에만 put (즉시 리턴)
[워커 스레드]  CoInitialize + 독립 XAQuery → MA/전략/주문/타임컷 (선주문·후로깅)
[로그 스레드]  log_queue 전용 — print/CSV (워커 핫패스와 완전 분리)

실행: python set_ticker.py 012345  →  python hft_scalping_bot.py
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
import queue
import threading
import collections
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional

import pythoncom
import win32com.client
from dotenv import load_dotenv

# =============================================================================
# 경로 & 설정
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, "config")
TICKER_CONFIG_PATH = os.path.join(CONFIG_DIR, "ticker.json")
ENV_PATH = os.path.join(BASE_DIR, ".env")

# .env 로드 (프로세스 시작 시 1회 — 매매 루프 레이턴시와 무관)
load_dotenv(ENV_PATH)


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def _env_int(key: str, default: int) -> int:
    raw = _env(key, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def _env_pct(key: str, default: float) -> float:
    """비율 파싱 — 1.5(%) 또는 0.015(비율) 모두 허용."""
    raw = _env(key, str(default))
    try:
        v = float(raw)
    except ValueError:
        return default
    if abs(v) > 1.0:
        v /= 100.0
    return v


def normalize_account_no(account: str) -> str:
    """HTS 표기(208607540-01) → xingAPI용(20860754001)."""
    return account.replace("-", "").replace(" ", "").strip()


def shcode_matches(expected: str, received: str) -> bool:
    """K3 수신 shcode ↔ ticker.json 비교 (대소문자·앞 0 차이 허용)."""
    exp = expected.strip().upper()
    recv = received.strip().upper()
    if not exp or not recv:
        return False
    if exp == recv:
        return True
    if exp.lstrip("0") == recv.lstrip("0"):
        return True
    return False


CONFIG = {
    "XING_RES_DIRS": [
        r"C:\LS_SEC\xingAPI\Res",
        r"C:\eBEST\xingAPI\Res",
        r"C:\ls_sec\xingAPI\Res",
    ],
    # 모의: demo.ebestsec.co.kr / 실전: hts.ebestsec.co.kr
    "SERVER_HOST": _env("XING_SERVER", "hts.ebestsec.co.kr"),
    "SERVER_PORT": int(_env("XING_SERVER_PORT", "20001") or "20001"),
    # xingAPI COM 로그인 (REST 앱키/시크릿과 별개)
    "USER_ID": _env("XING_USER_ID"),
    "USER_PW": _env("XING_USER_PW"),
    "CERT_PW": _env("XING_CERT_PW"),  # 실전 공인인증 비번 (모의투자는 빈 값)
    "ACCOUNT_NO": normalize_account_no(_env("XING_ACCOUNT")),
    "ACCOUNT_PW": _env("XING_ACCOUNT_PW", "0000"),
    # 전략 파라미터 (.env — STRATEGY_* 키)
    "MA_WINDOW": _env_int("STRATEGY_MA_WINDOW", 80),
    "ENTRY_GAP": _env_pct("STRATEGY_ENTRY_GAP_PCT", 1.5),
    "TAKE_PROFIT": _env_pct("STRATEGY_TAKE_PROFIT_PCT", 1.5),
    "TIME_STOP_SEC": _env_int("STRATEGY_TIME_STOP_SEC", 4),
    "ORDER_QTY": 1,  # 1주 고정 (STRATEGY_ORDER_QTY는 검증용)
    "ORD_LIMIT": "00",
    "ORD_MARKET": "01",
    "BNS_BUY": "2",
    "BNS_SELL": "1",
    "MGN_TRN_NORMAL": "000",
    "ORD_CND_NONE": "0",
    "TICK_QUEUE_SIZE": 0,  # 0 = 무제한 (고틱 버스트 수용)
    # 정정 TR rate limit (GetTRCountPerSec × 비율, 최소 간격 ms)
    "AMEND_SAFETY_RATIO": float(_env("STRATEGY_AMEND_SAFETY_RATIO", "0.85") or "0.85"),
    "AMEND_MIN_INTERVAL_MS": _env_int("STRATEGY_AMEND_MIN_INTERVAL_MS", 50),
    # REPEG: 매수접수 후 취소 가능까지 대기 (ms) — 너무 빠른 취소 시 02714
    "REPEG_SETTLE_MS": _env_int("STRATEGY_REPEG_SETTLE_MS", 400),
    # 02714 연속 N회 시 HALT
    "QTY_REJECT_HALT_AFTER": _env_int("STRATEGY_QTY_REJECT_HALT_AFTER", 3),
    # 주문당 정정 TR 허용 횟수 — 0이면 항상 취소+재주문 (정정 1회 후 02714 회피)
    "AMEND_MAX_PER_ORDER": _env_int("STRATEGY_AMEND_MAX_PER_ORDER", 0),
}

LOG_DIR = os.path.join(BASE_DIR, "logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
CSV_PATH = os.path.join(LOG_DIR, f"trade_log_{datetime.now().strftime('%Y%m%d')}.csv")

# Producer → Consumer 큐 (메인 스레드 put, 워커 스레드 get)
_tick_qsize = CONFIG["TICK_QUEUE_SIZE"] or 0
tick_queue: queue.Queue = queue.Queue(maxsize=_tick_qsize)
fill_queue: queue.Queue = queue.Queue()
order_event_queue: queue.Queue = queue.Queue()
order_ack_queue: queue.Queue = queue.Queue()
log_queue: queue.Queue = queue.Queue()


@dataclass(frozen=True, slots=True)
class LogMsg:
    action: str
    shcode: str
    price: int
    qty: int
    memo: str


def _write_log_sync(action: str, shcode: str, price: int, qty: int, memo: str = "") -> None:
    """동기 로그 (시작/종료 등 콜드패스 전용)."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = (
        f"[{timestamp}] [{action:^6}] 종목:{shcode} | 가격:{price:,}원 | "
        f"수량:{qty} | 비고:{memo}"
    )
    try:
        print(line)
    except UnicodeEncodeError:
        print(line.encode(sys.stdout.encoding or "utf-8", errors="replace").decode(
            sys.stdout.encoding or "utf-8", errors="replace"
        ))
    try:
        write_header = not os.path.exists(CSV_PATH)
        with open(CSV_PATH, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["Timestamp", "Action", "StockCode", "Price", "Quantity", "Memo"],
            )
            if write_header:
                writer.writeheader()
            writer.writerow({
                "Timestamp": timestamp,
                "Action": action,
                "StockCode": shcode,
                "Price": price,
                "Quantity": qty,
                "Memo": memo,
            })
    except Exception as exc:
        print(f"[LOG_ERROR] CSV 기록 실패: {exc}")


def save_and_print_log(action: str, shcode: str, price: int, qty: int, memo: str = "") -> None:
    """콜드패스: 즉시 출력+CSV."""
    _write_log_sync(action, shcode, price, qty, memo)


def enqueue_log(action: str, shcode: str, price: int, qty: int, memo: str = "") -> None:
    """핫패스: 로그를 큐에만 넣고 즉시 리턴 (주문 발송 후 워커가 flush)."""
    try:
        log_queue.put_nowait(LogMsg(action, shcode, price, qty, memo))
    except Exception:
        pass


_LOG_SENTINEL = object()
_log_thread_running = False
_log_thread: Optional[threading.Thread] = None


def log_worker_loop() -> None:
    """로그 전용 스레드 — 워커/메인 핫패스와 파일IO 완전 분리."""
    global _log_thread_running
    while _log_thread_running:
        try:
            msg = log_queue.get(timeout=0.05)
        except queue.Empty:
            continue
        if msg is _LOG_SENTINEL:
            break
        try:
            _write_log_sync(msg.action, msg.shcode, msg.price, msg.qty, msg.memo)
        except Exception:
            pass
    # 종료 시 잔여 로그 비우기
    while True:
        try:
            msg = log_queue.get_nowait()
            if msg is _LOG_SENTINEL:
                continue
            _write_log_sync(msg.action, msg.shcode, msg.price, msg.qty, msg.memo)
        except queue.Empty:
            break


def start_log_thread() -> None:
    global _log_thread_running, _log_thread
    if _log_thread and _log_thread.is_alive():
        return
    _log_thread_running = True
    _log_thread = threading.Thread(target=log_worker_loop, name="LogWorker", daemon=True)
    _log_thread.start()


def stop_log_thread() -> None:
    global _log_thread_running, _log_thread
    _log_thread_running = False
    try:
        log_queue.put_nowait(_LOG_SENTINEL)
    except Exception:
        pass
    if _log_thread and _log_thread.is_alive():
        _log_thread.join(timeout=3.0)
    _log_thread = None


def flush_log_queue() -> None:
    """종료 시 잔여 로그 동기 처리."""
    while True:
        try:
            msg = log_queue.get_nowait()
            if msg is _LOG_SENTINEL:
                continue
            _write_log_sync(msg.action, msg.shcode, msg.price, msg.qty, msg.memo)
        except queue.Empty:
            break


def load_ticker_config() -> dict:
    """config/ticker.json 에서 당일 매매 종목 1개 로드."""
    if not os.path.isfile(TICKER_CONFIG_PATH):
        raise FileNotFoundError(
            f"티커 설정 없음: {TICKER_CONFIG_PATH}\n"
            f"먼저 실행: python set_ticker.py <6자리종목코드>"
        )
    with open(TICKER_CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    shcode = str(data.get("shcode", "")).strip()
    if not shcode or shcode == "000000" or len(shcode) != 6:
        raise ValueError(
            f"유효한 shcode가 없습니다. python set_ticker.py <종목코드> 로 설정하세요."
        )
    data["shcode"] = shcode
    # 주문 TR용 IsuNo — 기본 A+shcode (종목 1개만 매매)
    data["isu_no"] = str(data.get("isu_no") or f"A{shcode}").strip()
    data["start_time"] = str(data.get("start_time", "08:50:00")).strip()
    return data


def tick_tr_for_market(market: str) -> str:
    return "S3_" if market == "KOSPI" else "K3_"


class _SyncQueryEvents:
    """로그인 후 1회성 TR 동기 대기용."""

    received: bool = False

    def OnReceiveData(self, szTrCode):
        _SyncQueryEvents.received = True


def detect_market(shcode: str) -> str:
    """t8436 종목마스터로 코스피/코스닥 자동 판별 (shcode 하나만으로 충분)."""
    _SyncQueryEvents.received = False
    try:
        query = win32com.client.DispatchWithEvents("XA_DataSet.XAQuery", _SyncQueryEvents)
        query.LoadFromResFile(resolve_res_path("t8436.res"))
        for gubun, market in (("1", "KOSPI"), ("2", "KOSDAQ")):
            _SyncQueryEvents.received = False
            query.SetFieldData("t8436InBlock", "gubun", 0, gubun)
            query.Request(0)
            deadline = datetime.now() + timedelta(seconds=20)
            while not _SyncQueryEvents.received and datetime.now() < deadline:
                pythoncom.PumpWaitingMessages()
            if not _SyncQueryEvents.received:
                continue
            cnt = query.GetBlockCount("t8436OutBlock")
            for i in range(cnt):
                code = str(query.GetFieldData("t8436OutBlock", "shcode", i)).strip()
                if code == shcode:
                    save_and_print_log("DETECT", shcode, 0, 0, f"시장={market}")
                    return market
    except Exception as exc:
        save_and_print_log("ERROR", shcode, 0, 0, f"시장판별: {exc}")
    save_and_print_log("WARN", shcode, 0, 0, "시장 미확인 — KOSDAQ 기본")
    return "KOSDAQ"


class ResCache:
    """XAQuery는 동시에 TR 1개만 활성 — SetFieldData/Request 직전 해당 res 재로드."""

    def __init__(self, query) -> None:
        self._query = query

    def ensure(self, res_filename: str) -> None:
        self._query.LoadFromResFile(resolve_res_path(res_filename))

    def preload_order_trs(self) -> None:
        """시작 시 res 파일 존재 여부만 확인."""
        for name in ("CSPAT00600.res", "CSPAT00700.res", "CSPAT00800.res"):
            resolve_res_path(name)


class SellOrderTemplate:
    """
    익절 매도 지정가 Pre-build.
    BUY_ORDERED 진입 시 고정 필드 SetFieldData 완료 → SC1 시 OrdQty/OrdPrc만 패치 후 Request.
    """

    INBLOCK = "CSPAT00600InBlock1"

    def __init__(self, query, res_cache: ResCache) -> None:
        self._q = query
        self._res_cache = res_cache
        self.ready = False
        self._account_no = ""
        self._account_pw = ""
        self._isu_no = ""

    def prime(
        self,
        account_no: str,
        account_pw: str,
        isu_no: str,
        tp_price: int,
        qty: int,
    ) -> None:
        self._account_no = account_no
        self._account_pw = account_pw
        self._isu_no = isu_no
        self.ready = True

    def fire(self, qty: int, price: int, memo: str = "매도지정가") -> None:
        """ensure() 후 InBlock 전체 재설정 — LoadFromResFile가 필드를 비움."""
        if not self.ready:
            raise RuntimeError("SellOrderTemplate 미초기화")
        self._res_cache.ensure("CSPAT00600.res")
        q = self._q
        q.SetFieldData(self.INBLOCK, "AcntNo", 0, self._account_no)
        q.SetFieldData(self.INBLOCK, "InptPwd", 0, self._account_pw)
        q.SetFieldData(self.INBLOCK, "IsuNo", 0, self._isu_no)
        q.SetFieldData(self.INBLOCK, "BnsTpCode", 0, CONFIG["BNS_SELL"])
        q.SetFieldData(self.INBLOCK, "OrdprcPtnCode", 0, CONFIG["ORD_LIMIT"])
        q.SetFieldData(self.INBLOCK, "MgntrnCode", 0, CONFIG["MGN_TRN_NORMAL"])
        q.SetFieldData(self.INBLOCK, "OrdCndiTpCode", 0, CONFIG["ORD_CND_NONE"])
        q.SetFieldData(self.INBLOCK, "OrdQty", 0, qty)
        q.SetFieldData(self.INBLOCK, "OrdPrc", 0, price)
        q._pending_bns = CONFIG["BNS_SELL"]  # type: ignore[attr-defined]
        q._pending_memo = memo  # type: ignore[attr-defined]
        q._pending_is_cancel = False  # type: ignore[attr-defined]
        q.Request(0)

    def invalidate(self) -> None:
        self.ready = False
        self._account_no = ""
        self._account_pw = ""
        self._isu_no = ""


class BuyAmendTemplate:
    """매수 지정가 정정(CSPAT00700) Pre-build — OrgOrdNo/OrdPrc만 핫패스 패치."""

    INBLOCK = "CSPAT00700InBlock1"

    def __init__(self, query, res_cache: ResCache) -> None:
        self._q = query
        self._res_cache = res_cache
        self.ready = False
        self._account_no = ""
        self._account_pw = ""
        self._isu_no = ""
        self._qty = 0

    def prime(
        self,
        account_no: str,
        account_pw: str,
        isu_no: str,
        org_ord_no: int,
        qty: int,
    ) -> None:
        self._account_no = account_no
        self._account_pw = account_pw
        self._isu_no = isu_no
        self._qty = qty
        self.ready = True

    def fire(self, org_ord_no: int, price: int) -> None:
        if not self.ready:
            raise RuntimeError("BuyAmendTemplate 미초기화")
        self._res_cache.ensure("CSPAT00700.res")
        q = self._q
        q.SetFieldData(self.INBLOCK, "AcntNo", 0, self._account_no)
        q.SetFieldData(self.INBLOCK, "InptPwd", 0, self._account_pw)
        q.SetFieldData(self.INBLOCK, "IsuNo", 0, self._isu_no)
        q.SetFieldData(self.INBLOCK, "OrgOrdNo", 0, org_ord_no)
        q.SetFieldData(self.INBLOCK, "OrdQty", 0, self._qty)
        q.SetFieldData(self.INBLOCK, "OrdprcPtnCode", 0, CONFIG["ORD_LIMIT"])
        q.SetFieldData(self.INBLOCK, "OrdCndiTpCode", 0, CONFIG["ORD_CND_NONE"])
        q.SetFieldData(self.INBLOCK, "OrdPrc", 0, price)
        q._pending_is_amend = True  # type: ignore[attr-defined]
        q._pending_amend_price = price  # type: ignore[attr-defined]
        q._pending_is_cancel = False  # type: ignore[attr-defined]
        q.Request(0)

    def invalidate(self) -> None:
        self.ready = False
        self._account_no = ""
        self._account_pw = ""
        self._isu_no = ""
        self._qty = 0


class TrRateLimiter:
    """xingAPI GetTRCountPerSec 기반 TR 발송 제한 (오류·계정제재 방지)."""

    def __init__(self, query, res_filename: str, safety_ratio: float, min_interval_ms: int) -> None:
        self._timestamps: collections.deque[float] = collections.deque()
        self._per_sec = 5
        self._safety_ratio = max(0.1, min(1.0, safety_ratio))
        self._min_interval = max(0.01, min_interval_ms / 1000.0)
        try:
            query.LoadFromResFile(resolve_res_path(res_filename))
            raw = int(query.GetTRCountPerSec())
            if raw > 0:
                self._per_sec = raw
        except Exception:
            pass
        self._max_per_sec = max(1, int(self._per_sec * self._safety_ratio))
        self._last_send = 0.0

    @property
    def max_per_sec(self) -> int:
        return self._max_per_sec

    def can_send(self, now: Optional[float] = None) -> bool:
        ts = now if now is not None else time.perf_counter()
        if ts - self._last_send < self._min_interval:
            return False
        while self._timestamps and ts - self._timestamps[0] >= 1.0:
            self._timestamps.popleft()
        return len(self._timestamps) < self._max_per_sec

    def record_send(self, now: Optional[float] = None) -> None:
        ts = now if now is not None else time.perf_counter()
        self._timestamps.append(ts)
        self._last_send = ts


def resolve_res_path(filename: str) -> str:
    for base in CONFIG["XING_RES_DIRS"]:
        path = os.path.join(base, filename)
        if os.path.isfile(path):
            return path
    raise FileNotFoundError(f"RES 파일 없음: {filename}")


def get_tick_size(price: int, market: str = "KOSDAQ") -> int:
    """KRX 주식 호가단위 (KOSPI/KOSDAQ)."""
    price = abs(int(price))
    if price < 1_000:
        return 1
    if price < 5_000:
        return 5
    if price < 10_000:
        return 10
    if price < 50_000:
        return 50
    if market.upper() == "KOSPI":
        if price < 100_000:
            return 100
        if price < 500_000:
            return 500
        return 1_000
    if price < 100_000:
        return 100
    return 100


def round_to_tick(price: int, market: str = "KOSDAQ", direction: str = "nearest") -> int:
    """호가단위에 맞게 가격 근사 — 매수는 down, 익절 매도는 up."""
    price = int(price)
    if price <= 0:
        return 0
    tick = get_tick_size(price, market)
    if tick <= 0:
        return price
    if direction == "up":
        result = ((price + tick - 1) // tick) * tick
    elif direction == "down":
        result = (price // tick) * tick
    else:
        result = int(round(price / tick) * tick)
    if result <= 0:
        return tick
    # 구간 경계(예: 9,999→10,000)에서 결과 가격의 호가단위 재검증
    result_tick = get_tick_size(result, market)
    if result_tick != tick:
        if direction == "up" and result % result_tick:
            result = ((result + result_tick - 1) // result_tick) * result_tick
        elif direction == "down" and result % result_tick:
            result = (result // result_tick) * result_tick
    return result


def calc_ma_entry_limit(ma_price: float, market: str, entry_gap: float) -> int:
    """80MA 대비 entry_gap% 아래 지정가 — 호가단위 내림."""
    raw = int(ma_price * (1.0 - entry_gap))
    return round_to_tick(raw, market, "down")


def calc_take_profit_limit(base_price: int, market: str, take_profit: float) -> int:
    """체결가 대비 take_profit% 익절 지정가 — 호가단위 올림."""
    raw = int(base_price * (1.0 + take_profit))
    return round_to_tick(raw, market, "up")


def safe_int(value: Any, default: int = 0) -> int:
    try:
        text = str(value).strip().replace(",", "")
        if not text:
            return default
        return int(float(text))
    except (TypeError, ValueError):
        return default


def safe_abs_price(value: Any) -> int:
    return abs(safe_int(value, 0))


def parse_start_datetime(time_str: str) -> datetime:
    """오늘 날짜 + start_time → datetime."""
    today = datetime.now().date()
    h, m, s = (int(x) for x in time_str.split(":"))
    return datetime(today.year, today.month, today.day, h, m, s)


# =============================================================================
# 큐 메시지 (가벼운 immutable 구조)
# =============================================================================
@dataclass(frozen=True, slots=True)
class TickMsg:
    shcode: str
    price_raw: str
    chetime: str


@dataclass(frozen=True, slots=True)
class FillMsg:
    isuno: str
    ord_no_raw: str
    exec_qty_raw: str
    exec_prc_raw: str
    bns_tp: str
    recv_ts: float  # perf_counter — SC1 수신 시각 (벤치마크)


@dataclass(frozen=True, slots=True)
class OrderEventMsg:
    """SC0/SC2/SC3/SC4 실시간 주문 이벤트 (SC1은 fill_queue)."""
    tr_code: str
    ord_no: int
    org_ord_no: int  # SC2 정정확인 — 원주문번호 (ordno와 다를 수 있음)
    bns_tp: str
    price: int
    unerc_qty: int  # 미체결수량 (0이면 완전체결·취소 등)
    recv_ts: float


@dataclass(frozen=True, slots=True)
class OrderAckMsg:
    tr_code: str
    ord_no: int
    bns_tp: str
    memo: str
    is_cancel: bool = False
    is_amend: bool = False
    is_tr_error: bool = False
    success: bool = True
    amend_price: int = 0
    reject_code: str = ""
    reject_msg: str = ""


def is_account_password_lock(code: str, message: str) -> bool:
    """계좌 주문비밀번호 연속 오류 잠금 (01796 등)."""
    if str(code).strip() == "01796":
        return True
    text = str(message)
    return "비밀번호" in text and "초과" in text


def is_amend_reject_qty_or_funds(code: str, message: str) -> bool:
    """정정 거부 — 매매가능수량/잔고/미체결 수량 문제 (02714 등, 체결과 무관)."""
    c = str(code).strip()
    text = str(message)
    if c in ("02714", "2714"):
        return True
    return "매매가능수량" in text and "초과" in text


def is_tr_success_message(code: str, message: str = "") -> bool:
    """주문 TR 정상 안내 메시지 — OnReceiveMessage에서 에러로 처리하지 않음."""
    c = str(code).strip()
    if c in ("", "0", "00000", "00039", "00040"):
        return True
    text = str(message)
    if "정상 접수" in text or "주문이 완료" in text:
        return True
    return False


class BotState(str, Enum):
    IDLE = "IDLE"
    BUY_PEGGED = "BUY_PEGGED"  # MA×(1-gap) 미체결 매수 유지·정정
    BUY_ORDERED = "BUY_ORDERED"  # 하위호환 alias
    BOUGHT = "BOUGHT"
    SELL_ORDERED = "SELL_ORDERED"
    CANCEL_PENDING = "CANCEL_PENDING"
    MARKET_EXIT = "MARKET_EXIT"


# =============================================================================
# 전략 엔진 (워커 스레드 전용)
# =============================================================================
class StrategyEngine:
    """워커 스레드에서만 접근 — MA/주문/타임컷/상태머신."""

    def __init__(self, ticker_cfg: dict) -> None:
        self.shcode = ticker_cfg["shcode"]
        self.isu_no = ticker_cfg["isu_no"]
        self.market = ticker_cfg.get("market", "KOSDAQ")
        self.account_no = CONFIG["ACCOUNT_NO"]
        self.account_pw = CONFIG["ACCOUNT_PW"]

        self.state = BotState.IDLE
        self.price_deque: collections.deque[int] = collections.deque(
            maxlen=CONFIG["MA_WINDOW"]
        )
        self._ma_sum = 0
        self.last_price = 0
        self.last_ma80 = 0.0

        self.buy_ord_no = 0
        self.sell_ord_no = 0
        self.buy_fill_price = 0
        self.buy_fill_qty = 0
        self.sell_fill_qty = 0
        self.tp_price = 0
        # 전략 C: 매수 지정가 발송 시 TP 사전 계산
        self.pending_buy_limit = 0
        self.pending_tp_price = 0
        # MA 추적 매수(Pegged)
        self.live_buy_limit = 0
        self.queued_buy_limit = 0
        self.buy_order_live = False
        self.amend_in_flight = False
        self.buy_amend_count = 0
        self.buy_cancel_in_flight = False
        self.pending_replace_limit = 0
        self.qty_reject_count = 0
        self.buy_settle_until: Optional[datetime] = None
        self.buy_uncerc_qty = 0
        self._pending_amend_price = 0
        self._tick_count = 0
        self._logged_first_tick = False
        self._logged_ma_ready = False
        self.trading_halted = False
        self.halt_reason = ""

        self.buy_fill_time: Optional[datetime] = None
        self.time_cut_deadline: Optional[datetime] = None
        self.time_cut_triggered = False

        self._worker_query = None
        self._res_cache: Optional[ResCache] = None
        self._sell_template: Optional[SellOrderTemplate] = None
        self._buy_amend_template: Optional[BuyAmendTemplate] = None
        self._amend_limiter: Optional[TrRateLimiter] = None
        self._running = True

    def bind_worker_query(self, query) -> None:
        self._worker_query = query
        self._res_cache = ResCache(query)
        self._res_cache.preload_order_trs()
        self._sell_template = SellOrderTemplate(query, self._res_cache)
        self._buy_amend_template = BuyAmendTemplate(query, self._res_cache)
        self._amend_limiter = TrRateLimiter(
            query,
            "CSPAT00700.res",
            CONFIG["AMEND_SAFETY_RATIO"],
            CONFIG["AMEND_MIN_INTERVAL_MS"],
        )
        save_and_print_log(
            "LIMIT",
            self.shcode,
            0,
            0,
            f"CSPAT00700 max={self._amend_limiter.max_per_sec}/s "
            f"min={CONFIG['AMEND_MIN_INTERVAL_MS']}ms",
        )

    @staticmethod
    def _calc_tp_price(base_price: int, market: str) -> int:
        return calc_take_profit_limit(base_price, market, CONFIG["TAKE_PROFIT"])

    def halt_trading(self, reason: str) -> None:
        """주문 재시도 중단 — 비밀번호 잠금 등 치명 오류."""
        if self.trading_halted:
            return
        self.trading_halted = True
        self.halt_reason = reason
        self.amend_in_flight = False
        self.buy_cancel_in_flight = False
        self.pending_replace_limit = 0
        self.buy_order_live = False
        self.queued_buy_limit = 0
        save_and_print_log("HALT", self.shcode, self.last_price, 0, reason)

    def _can_enter_buy(self) -> bool:
        """1주 사이클 — 포지션·주문 중복 매수 차단."""
        if self.trading_halted:
            return False
        return (
            self.state == BotState.IDLE
            and self.buy_fill_qty == 0
            and self.buy_ord_no == 0
            and not self.buy_order_live
            and not self.amend_in_flight
        )

    def _is_buy_pegged(self) -> bool:
        return self.state == BotState.BUY_PEGGED

    def _buy_already_done(self) -> bool:
        return self.buy_fill_qty >= CONFIG["ORDER_QTY"]

    def _order_event_matches_buy(self, msg: OrderEventMsg) -> bool:
        """SC2는 ordno≠원주문 — orgordno로 매칭."""
        if msg.bns_tp and msg.bns_tp != CONFIG["BNS_BUY"]:
            return False
        if not self.buy_ord_no:
            return True
        return msg.ord_no == self.buy_ord_no or msg.org_ord_no == self.buy_ord_no

    def _mark_buy_settle(self, *, is_repeg: bool = False) -> None:
        """매수접수 직후 — LS 서버 정착 전 취소 시 02714 방지.
        최초 PEG: 100ms (정정이 빠르게 가능하도록)
        REPEG 재배치: REPEG_SETTLE_MS (취소 레이스 방지)
        """
        ms = CONFIG["REPEG_SETTLE_MS"] if is_repeg else 100
        self.buy_settle_until = datetime.now() + timedelta(milliseconds=ms)

    def _on_qty_reject(self, action: str, target_limit: int, code: str, msg: str) -> None:
        """02714 — settle 후 재시도, 연속 실패 시에만 HALT."""
        self.qty_reject_count += 1
        self.amend_in_flight = False
        self.buy_cancel_in_flight = False
        self.pending_replace_limit = 0
        if target_limit > 0:
            self.queued_buy_limit = target_limit
        backoff_ms = CONFIG["REPEG_SETTLE_MS"] * self.qty_reject_count
        self.buy_settle_until = datetime.now() + timedelta(milliseconds=backoff_ms)
        enqueue_log(
            "REPEG" if action == "cancel" else "AMEND",
            self.shcode, target_limit, 0,
            f"{action}거부 ord={self.buy_ord_no} code={code} — "
            f"{backoff_ms}ms 후 재시도 ({self.qty_reject_count}/"
            f"{CONFIG['QTY_REJECT_HALT_AFTER']}) {msg}",
        )
        if self.qty_reject_count >= CONFIG["QTY_REJECT_HALT_AFTER"]:
            self.halt_trading(
                f"02714 {action}거부 {self.qty_reject_count}회 — "
                f"HTS에서 {self.shcode} 미체결 전부 취소·보유 확인 후 재시작"
            )

    def _can_sync_pegged_price(self) -> bool:
        """PEG 가격 동기화(정정 또는 취소+재주문) 가능 여부."""
        if self.trading_halted:
            return False
        if self.buy_settle_until and datetime.now() < self.buy_settle_until:
            return False
        if self._buy_already_done():
            return False
        if not self._is_buy_pegged():
            return False
        if self.amend_in_flight or self.buy_cancel_in_flight:
            return False
        if not self.buy_order_live:
            return False
        if self.buy_ord_no <= 0:
            return False
        return True

    def _can_amend_buy(self) -> bool:
        return self._can_sync_pegged_price()

    def _update_ma(self, price: int) -> None:
        if (
            self.price_deque.maxlen
            and len(self.price_deque) == self.price_deque.maxlen
        ):
            self._ma_sum -= self.price_deque[0]
        self.price_deque.append(price)
        self._ma_sum += price
        n = len(self.price_deque)
        if n > 0:
            self.last_ma80 = self._ma_sum / n

    def _precompute_tp_for_buy(self, limit_price: int, qty: int = 0) -> None:
        """매수 주문 직전 TP 사전 계산 + 매도 주문 Pre-build."""
        order_qty = qty or CONFIG["ORDER_QTY"]
        self.pending_buy_limit = limit_price
        self.pending_tp_price = self._calc_tp_price(limit_price, self.market)
        if self._sell_template is not None:
            self._sell_template.prime(
                self.account_no,
                self.account_pw,
                self.isu_no,
                self.pending_tp_price,
                order_qty,
            )

    def set_account(self, account_no: str) -> None:
        if account_no:
            self.account_no = account_no

    def stop(self) -> None:
        self._running = False

    def set_state(self, new_state: BotState, reason: str = "", *, silent: bool = False) -> None:
        if self.state == new_state:
            return
        if not silent:
            save_and_print_log(
                "STATE",
                self.shcode,
                self.last_price,
                0,
                f"{self.state.value} → {new_state.value} | {reason}",
            )
        self.state = new_state

    def reset_cycle(self, reason: str = "") -> None:
        self.buy_ord_no = 0
        self.sell_ord_no = 0
        self.buy_fill_price = 0
        self.buy_fill_qty = 0
        self.sell_fill_qty = 0
        self.tp_price = 0
        self.pending_buy_limit = 0
        self.pending_tp_price = 0
        self.live_buy_limit = 0
        self.queued_buy_limit = 0
        self.buy_order_live = False
        self.amend_in_flight = False
        self.buy_amend_count = 0
        self.buy_cancel_in_flight = False
        self.pending_replace_limit = 0
        self.qty_reject_count = 0
        self.buy_settle_until = None
        self.buy_uncerc_qty = 0
        self._pending_amend_price = 0
        self.buy_fill_time = None
        self.time_cut_deadline = None
        self.time_cut_triggered = False
        if self._sell_template is not None:
            self._sell_template.invalidate()
        if self._buy_amend_template is not None:
            self._buy_amend_template.invalidate()
        self.set_state(BotState.IDLE, reason or "사이클 초기화")

    # ----- 큐 소비: SC0/SC2/SC3 -----
    def process_order_event(self, msg: OrderEventMsg) -> None:
        try:
            if not self._order_event_matches_buy(msg):
                return

            code = msg.tr_code.upper()
            if code == "SC0":
                if msg.ord_no > 0:
                    self.buy_ord_no = msg.ord_no
                self.buy_order_live = True
                if msg.unerc_qty > 0:
                    self.buy_uncerc_qty = msg.unerc_qty
            elif code == "SC2":
                self.amend_in_flight = False
                if msg.price > 0:
                    self.live_buy_limit = msg.price
                    self.pending_buy_limit = msg.price
                if msg.unerc_qty > 0:
                    self.buy_uncerc_qty = msg.unerc_qty
                enqueue_log(
                    "AMEND", self.shcode, msg.price, CONFIG["ORDER_QTY"],
                    f"SC2 정정확인 ord={msg.ord_no} org={msg.org_ord_no} unerc={msg.unerc_qty}",
                )
                self._flush_queued_amend()
            elif code == "SC3":
                replace_limit = self.pending_replace_limit
                in_replace = self.buy_cancel_in_flight or replace_limit > 0
                self.buy_order_live = False
                self.amend_in_flight = False
                self.buy_cancel_in_flight = False
                self.buy_uncerc_qty = 0
                old_ord = self.buy_ord_no
                self.buy_ord_no = 0
                if in_replace and replace_limit > 0 and not self._buy_already_done():
                    self.pending_replace_limit = 0
                    enqueue_log(
                        "CANCEL", self.shcode, replace_limit, CONFIG["ORDER_QTY"],
                        f"SC3 매수취소 ord={old_ord} → 재배치",
                    )
                    self._repost_pegged_buy(replace_limit)
                elif not self._buy_already_done():
                    enqueue_log(
                        "CANCEL", self.shcode, self.live_buy_limit, CONFIG["ORDER_QTY"],
                        f"SC3 매수취소 ord={msg.ord_no}",
                    )
                    self.live_buy_limit = 0
                    self.pending_replace_limit = 0
                    self.set_state(BotState.IDLE, "매수취소→재배치대기", silent=True)
            elif code == "SC4":
                self.amend_in_flight = False
                enqueue_log(
                    "REJECT", self.shcode, 0, 0,
                    f"SC4 ord={msg.ord_no} org={msg.org_ord_no} unerc={msg.unerc_qty}",
                )
        except Exception as exc:
            save_and_print_log("ERROR", self.shcode, 0, 0, f"주문이벤트: {exc}")

    # ----- 큐 소비: 틱 -----
    def _update_ma_from_tick(self, msg: TickMsg) -> None:
        """워커 루프 틱 버스트 드레인 — MA·카운트·last_price만 갱신, 주문 로직 없음."""
        try:
            if msg.shcode and not shcode_matches(self.shcode, msg.shcode):
                return
            price = safe_abs_price(msg.price_raw)
            if price <= 0:
                return
            self.last_price = price
            self._tick_count += 1
            if not self._logged_first_tick:
                self._logged_first_tick = True
                save_and_print_log(
                    "TICK", self.shcode, price, 1,
                    f"첫체결틱 t={msg.chetime} recv_sh={msg.shcode}",
                )
            self._update_ma(price)
        except Exception:
            pass

    def process_tick(self, msg: TickMsg) -> None:
        try:
            if msg.shcode and not shcode_matches(self.shcode, msg.shcode):
                return
            price = safe_abs_price(msg.price_raw)
            if price <= 0:
                return

            self.last_price = price
            self._tick_count += 1
            if not self._logged_first_tick:
                self._logged_first_tick = True
                save_and_print_log(
                    "TICK", self.shcode, price, 1,
                    f"첫체결틱 t={msg.chetime} recv_sh={msg.shcode}",
                )
            self._update_ma(price)

            if len(self.price_deque) < CONFIG["MA_WINDOW"]:
                if self._tick_count in (1, 10, 40) or self._tick_count % 20 == 0:
                    enqueue_log(
                        "WARMUP", self.shcode, price, self._tick_count,
                        f"MA{CONFIG['MA_WINDOW']} 준비중 {self._tick_count}/{CONFIG['MA_WINDOW']}",
                    )
                return

            if not self._logged_ma_ready:
                self._logged_ma_ready = True
                save_and_print_log(
                    "MAREADY",
                    self.shcode,
                    price,
                    CONFIG["MA_WINDOW"],
                    f"MA{CONFIG['MA_WINDOW']}={self.last_ma80:.1f} t={msg.chetime}",
                )

            self._check_time_cut()

            if self._buy_already_done() or self.state in (
                BotState.SELL_ORDERED,
                BotState.CANCEL_PENDING,
                BotState.MARKET_EXIT,
            ):
                return

            ma_buy_limit = calc_ma_entry_limit(
                self.last_ma80, self.market, CONFIG["ENTRY_GAP"]
            )
            if ma_buy_limit <= 0:
                return

            if self._can_enter_buy():
                self._place_pegged_buy(ma_buy_limit, msg.chetime)
                return

            if self._is_buy_pegged():
                if (
                    not self.buy_order_live
                    and not self.amend_in_flight
                    and self.buy_ord_no <= 0
                    and not self._buy_already_done()
                ):
                    self._place_pegged_buy(ma_buy_limit, msg.chetime)
                    return
                self._sync_pegged_buy(ma_buy_limit)
        except Exception as exc:
            save_and_print_log("ERROR", self.shcode, self.last_price, 0, f"틱 처리: {exc}")

    def _place_pegged_buy(self, limit_price: int, chetime: str = "") -> None:
        """MA×(1-gap) 지정가 최초 배치."""
        self._precompute_tp_for_buy(limit_price, CONFIG["ORDER_QTY"])
        self.live_buy_limit = limit_price
        self.pending_buy_limit = limit_price
        self.buy_order_live = True
        self.amend_in_flight = False
        enqueue_log(
            "SIGNAL",
            self.shcode,
            limit_price,
            CONFIG["ORDER_QTY"],
            f"PEG MA{CONFIG['MA_WINDOW']}={self.last_ma80:.1f} "
            f"limit={limit_price:,} t={chetime}",
        )
        self._request_order(
            CONFIG["BNS_BUY"],
            CONFIG["ORD_LIMIT"],
            CONFIG["ORDER_QTY"],
            limit_price,
            BotState.BUY_PEGGED,
            "매수지정가(PEG)",
        )

    def _sync_pegged_buy(self, target_limit: int) -> None:
        """호가단위 변경 — 1회 정정 후 취소+재주문 (LS 2회째 정정 02714 회피)."""
        if self.buy_cancel_in_flight:
            self.pending_replace_limit = target_limit
            if target_limit != self.live_buy_limit:
                self.queued_buy_limit = target_limit
            return
        if not self._can_sync_pegged_price():
            if target_limit != self.live_buy_limit:
                self.queued_buy_limit = target_limit
            return
        if target_limit == self.live_buy_limit:
            self.queued_buy_limit = 0
            return
        if self._amend_limiter is None or not self._amend_limiter.can_send():
            self.queued_buy_limit = target_limit
            return
        if self.buy_amend_count >= CONFIG["AMEND_MAX_PER_ORDER"]:
            self._send_buy_cancel_replace(target_limit)
        else:
            self._send_buy_amend(target_limit)

    def _send_buy_amend(self, new_limit: int) -> None:
        if not self._can_amend_buy():
            self.queued_buy_limit = new_limit
            return
        if new_limit == self.live_buy_limit:
            return
        try:
            self.amend_in_flight = True
            self._pending_amend_price = new_limit
            amend_qty = self.buy_uncerc_qty or CONFIG["ORDER_QTY"]
            if self._buy_amend_template is not None:
                self._buy_amend_template.prime(
                    self.account_no,
                    self.account_pw,
                    self.isu_no,
                    self.buy_ord_no,
                    amend_qty,
                )
                self._buy_amend_template.fire(self.buy_ord_no, new_limit)
            else:
                self._fire_cspat00700(self.buy_ord_no, new_limit, amend_qty)
            if self._amend_limiter is not None:
                self._amend_limiter.record_send()
            enqueue_log(
                "AMEND", self.shcode, new_limit, CONFIG["ORDER_QTY"],
                f"정정요청 ord={self.buy_ord_no} from={self.live_buy_limit}",
            )
        except Exception as exc:
            self.amend_in_flight = False
            enqueue_log("ERROR", self.shcode, new_limit, 0, f"정정 TR: {exc}")

    def _send_buy_cancel_replace(self, new_limit: int) -> None:
        """가격 변경 — 취소(CSPAT00800) 후 신규 지정가 매수 (정정 대신)."""
        if self.trading_halted:
            return
        if self.buy_cancel_in_flight:
            self.pending_replace_limit = new_limit
            self.queued_buy_limit = new_limit
            return
        if not self._can_sync_pegged_price():
            self.queued_buy_limit = new_limit
            return
        if new_limit == self.live_buy_limit:
            return
        try:
            self.buy_cancel_in_flight = True
            self.pending_replace_limit = new_limit
            cancel_qty = self.buy_uncerc_qty or CONFIG["ORDER_QTY"]
            self._fire_cspat00800(self.buy_ord_no, cancel_qty, for_buy=True)
            if self._amend_limiter is not None:
                self._amend_limiter.record_send()
            enqueue_log(
                "REPEG", self.shcode, new_limit, cancel_qty,
                f"취소요청 ord={self.buy_ord_no} {self.live_buy_limit:,}→{new_limit:,}",
            )
        except Exception as exc:
            self.buy_cancel_in_flight = False
            self.pending_replace_limit = 0
            enqueue_log("ERROR", self.shcode, new_limit, 0, f"취소 TR: {exc}")

    def _repost_pegged_buy(self, limit_price: int) -> None:
        """취소 확인 후 동일 PEG 지정가 재배치."""
        self.buy_amend_count = 0
        self.buy_cancel_in_flight = False
        self._precompute_tp_for_buy(limit_price, CONFIG["ORDER_QTY"])
        self.live_buy_limit = limit_price
        self.pending_buy_limit = limit_price
        self.buy_order_live = True
        self.amend_in_flight = False
        self._request_order(
            CONFIG["BNS_BUY"],
            CONFIG["ORD_LIMIT"],
            CONFIG["ORDER_QTY"],
            limit_price,
            BotState.BUY_PEGGED,
            "매수지정가(REPEG)",
            hot_path=True,
        )

    def _handle_buy_cancel_ack(self, msg: OrderAckMsg) -> None:
        replace_limit = self.pending_replace_limit
        self.buy_cancel_in_flight = False
        if not msg.success and msg.ord_no <= 0:
            if is_amend_reject_qty_or_funds(msg.reject_code, msg.reject_msg):
                self._on_qty_reject(
                    "cancel", replace_limit, msg.reject_code, msg.reject_msg,
                )
                return
            self.pending_replace_limit = 0
            enqueue_log(
                "REPEG", self.shcode, replace_limit, 0,
                f"취소거부 ord={self.buy_ord_no} code={msg.reject_code} {msg.reject_msg}",
            )
            return
        old_ord = self.buy_ord_no
        self.buy_ord_no = 0
        self.buy_order_live = False
        self.buy_uncerc_qty = 0
        self.pending_replace_limit = 0
        self.qty_reject_count = 0
        enqueue_log(
            "CANCEL", self.shcode, self.live_buy_limit, CONFIG["ORDER_QTY"],
            f"매수취소접수 ord={old_ord}",
        )
        if replace_limit > 0 and not self._buy_already_done():
            self._repost_pegged_buy(replace_limit)
        elif self.queued_buy_limit > 0 and not self._buy_already_done():
            target = self.queued_buy_limit
            self.queued_buy_limit = 0
            self._repost_pegged_buy(target)

    def _flush_queued_amend(self) -> None:
        if self._buy_already_done():
            self.queued_buy_limit = 0
            return
        target = self.queued_buy_limit
        if target <= 0 or target == self.live_buy_limit:
            self.queued_buy_limit = 0
            return
        if not self._can_sync_pegged_price():
            return
        self.queued_buy_limit = 0
        self._sync_pegged_buy(target)
        if self.queued_buy_limit == 0 and target != self.live_buy_limit and not self.amend_in_flight:
            self.queued_buy_limit = target

    # ----- 큐 소비: SC1 체결 -----
    def process_fill(self, msg: FillMsg) -> None:
        try:
            isu = msg.isuno.strip()
            if isu and self.isu_no not in isu and self.shcode not in isu:
                return

            ord_no = safe_int(msg.ord_no_raw)
            exec_qty = safe_int(msg.exec_qty_raw)
            exec_prc = safe_abs_price(msg.exec_prc_raw)
            if exec_qty <= 0 or exec_prc <= 0:
                return

            if msg.bns_tp == CONFIG["BNS_BUY"]:
                self._handle_buy_fill(ord_no, exec_qty, exec_prc, msg.recv_ts)
            elif msg.bns_tp == CONFIG["BNS_SELL"]:
                self._handle_sell_fill(ord_no, exec_qty, exec_prc)
        except Exception as exc:
            save_and_print_log("ERROR", self.shcode, self.last_price, 0, f"체결 처리: {exc}")

    def _handle_buy_fill(
        self, ord_no: int, exec_qty: int, exec_prc: int, sc1_recv_ts: float
    ) -> None:
        """매수 체결 핫패스: 사전계산 TP + RES캐시 + Request 선행."""
        if self.state not in (BotState.BUY_PEGGED, BotState.BUY_ORDERED, BotState.BOUGHT):
            if self.buy_ord_no and ord_no != self.buy_ord_no:
                return
            if self.buy_ord_no or self.live_buy_limit > 0:
                enqueue_log(
                    "RECOVER", self.shcode, exec_prc, exec_qty,
                    f"SC1매수체결 state={self.state.value} ord={ord_no} → SELL경로 복구",
                )
                if not self.buy_ord_no:
                    self.buy_ord_no = ord_no
                self.state = BotState.BUY_PEGGED
            else:
                return
        elif self.buy_ord_no and ord_no != self.buy_ord_no:
            return
        if self.buy_fill_qty >= CONFIG["ORDER_QTY"]:
            enqueue_log(
                "WARN", self.shcode, exec_prc, exec_qty,
                f"1주 한도 초과 체결 무시 ord={ord_no}",
            )
            return

        exec_qty = min(exec_qty, CONFIG["ORDER_QTY"] - self.buy_fill_qty)

        t_worker = time.perf_counter()
        prev_state = self.state
        self.buy_order_live = False
        self.amend_in_flight = False
        self.queued_buy_limit = 0
        self.buy_ord_no = ord_no
        self.buy_fill_qty += exec_qty
        self.buy_fill_price = exec_prc
        self.buy_fill_time = datetime.now()
        self.time_cut_deadline = self.buy_fill_time + timedelta(seconds=CONFIG["TIME_STOP_SEC"])

        # 전략 C: 지정가 매수와 체결가가 같으면 사전계산 TP 즉시 사용
        tp_source = "precalc"
        if self.pending_tp_price > 0 and exec_prc == self.pending_buy_limit:
            self.tp_price = self.pending_tp_price
        else:
            self.tp_price = self._calc_tp_price(exec_prc, self.market)
            tp_source = "recalc"
        qty = self.buy_fill_qty or CONFIG["ORDER_QTY"]

        self.state = BotState.SELL_ORDERED
        tpl_used = False
        try:
            if self._sell_template is not None and self._sell_template.ready:
                self._sell_template.fire(qty, self.tp_price, "매도지정가")
                tpl_used = True
            else:
                self._fire_cspat00600(
                    bns_tp=CONFIG["BNS_SELL"],
                    ord_ptn=CONFIG["ORD_LIMIT"],
                    qty=qty,
                    price=self.tp_price,
                    memo="매도지정가",
                )
        except Exception as exc:
            enqueue_log("ERROR", self.shcode, exec_prc, 0, f"익절주문: {exc}")
            self.reset_cycle("익절 주문 실패")
            return

        t_done = time.perf_counter()
        queue_ms = (t_worker - sc1_recv_ts) * 1000.0
        hot_ms = (t_done - t_worker) * 1000.0
        total_ms = (t_done - sc1_recv_ts) * 1000.0
        enqueue_log(
            "BENCH",
            self.shcode,
            self.tp_price,
            qty,
            f"SC1→TP total={total_ms:.2f}ms queue={queue_ms:.2f}ms hot={hot_ms:.2f}ms "
            f"src={tp_source} tpl={tpl_used} fill={exec_prc} pending={self.pending_buy_limit}",
        )

        deadline_str = self.time_cut_deadline.strftime("%H:%M:%S")
        enqueue_log(
            "FILL", self.shcode, exec_prc, exec_qty,
            f"매수체결 ord={ord_no} deadline={deadline_str}",
        )
        enqueue_log(
            "STATE", self.shcode, self.last_price, 0,
            f"{prev_state.value} → SELL_ORDERED | 매수체결→익절",
        )
        enqueue_log("TP", self.shcode, self.tp_price, qty, f"익절지정가 fill={exec_prc} src={tp_source}")
        enqueue_log(
            "ORDER", self.shcode, self.tp_price, qty,
            f"매도지정가 bns={CONFIG['BNS_SELL']} ptn={CONFIG['ORD_LIMIT']}",
        )

    def _handle_sell_fill(self, ord_no: int, exec_qty: int, exec_prc: int) -> None:
        if self.state not in (BotState.SELL_ORDERED, BotState.MARKET_EXIT, BotState.CANCEL_PENDING):
            return

        self.sell_fill_qty += exec_qty
        target = self.buy_fill_qty or CONFIG["ORDER_QTY"]
        save_and_print_log(
            "FILL",
            self.shcode,
            exec_prc,
            exec_qty,
            f"매도체결 ord={ord_no} 누적={self.sell_fill_qty}/{target}",
        )
        if self.sell_fill_qty >= target:
            save_and_print_log("EXIT", self.shcode, exec_prc, self.sell_fill_qty, "청산 완료")
            self.reset_cycle("매도 체결 완료")

    # ----- 논블로킹 타임컷 -----
    def _check_time_cut(self) -> None:
        if self.time_cut_triggered:
            return
        if self.state != BotState.SELL_ORDERED:
            return
        if self.time_cut_deadline is None or datetime.now() < self.time_cut_deadline:
            return

        self.time_cut_triggered = True
        enqueue_log(
            "TIMEOUT",
            self.shcode,
            self.last_price,
            CONFIG["ORDER_QTY"],
            f"{CONFIG['TIME_STOP_SEC']}초 타임컷 — 강제 탈출",
        )
        self._cancel_sell_order()

    def poll_time_cut(self) -> None:
        """틱 없을 때도 타임컷·REPEG settle 검사."""
        try:
            self._check_time_cut()
            self._poll_repeg_settle()
        except Exception as exc:
            save_and_print_log("ERROR", self.shcode, 0, 0, f"타임컷: {exc}")

    def _poll_repeg_settle(self) -> None:
        """settle 만료 후 큐에 쌓인 MA 가격으로 REPEG 재시도."""
        if self.trading_halted or not self._is_buy_pegged():
            return
        if self.buy_settle_until and datetime.now() < self.buy_settle_until:
            return
        target = self.queued_buy_limit
        if target <= 0 or target == self.live_buy_limit:
            return
        if not self._can_sync_pegged_price():
            return
        self.queued_buy_limit = 0
        self._sync_pegged_buy(target)

    # ----- 주문 TR (워커 스레드 독립 XAQuery) -----
    def _send_limit_order(self, side: str, qty: int, price: int, next_state: BotState) -> None:
        bns = CONFIG["BNS_BUY"] if side == "BUY" else CONFIG["BNS_SELL"]
        memo = "매수지정가" if side == "BUY" else "매도지정가"
        if side == "BUY":
            if not self._can_enter_buy():
                save_and_print_log(
                    "WARN", self.shcode, price, qty, "중복 매수 차단 — 포지션/주문 존재",
                )
                return
            qty = CONFIG["ORDER_QTY"]
            self._precompute_tp_for_buy(price, qty)
        self._request_order(bns, CONFIG["ORD_LIMIT"], qty, price, next_state, memo)

    def _send_market_sell(self, qty: int, *, hot_path: bool = False) -> None:
        self._request_order(
            CONFIG["BNS_SELL"], CONFIG["ORD_MARKET"], qty, 0,
            BotState.MARKET_EXIT, "타임컷 시장가", hot_path=hot_path,
        )

    def _fire_cspat00600(
        self,
        bns_tp: str,
        ord_ptn: str,
        qty: int,
        price: int,
        memo: str,
    ) -> None:
        """CSPAT00600 즉시 발송 — 로그·상태 변경 없음 (핫패스 코어)."""
        q = self._worker_query
        if q is None or self._res_cache is None:
            raise RuntimeError("워커 XAQuery 미초기화")
        self._res_cache.ensure("CSPAT00600.res")
        q.SetFieldData("CSPAT00600InBlock1", "AcntNo", 0, self.account_no)
        q.SetFieldData("CSPAT00600InBlock1", "InptPwd", 0, self.account_pw)
        q.SetFieldData("CSPAT00600InBlock1", "IsuNo", 0, self.isu_no)
        q.SetFieldData("CSPAT00600InBlock1", "OrdQty", 0, qty)
        q.SetFieldData("CSPAT00600InBlock1", "OrdPrc", 0, price)
        q.SetFieldData("CSPAT00600InBlock1", "BnsTpCode", 0, bns_tp)
        q.SetFieldData("CSPAT00600InBlock1", "OrdprcPtnCode", 0, ord_ptn)
        q.SetFieldData("CSPAT00600InBlock1", "MgntrnCode", 0, CONFIG["MGN_TRN_NORMAL"])
        q.SetFieldData("CSPAT00600InBlock1", "OrdCndiTpCode", 0, CONFIG["ORD_CND_NONE"])
        q._pending_bns = bns_tp  # type: ignore[attr-defined]
        q._pending_memo = memo  # type: ignore[attr-defined]
        q._pending_is_cancel = False  # type: ignore[attr-defined]
        q.Request(0)

    def _fire_cspat00700(self, org_ord_no: int, price: int, qty: int = 0) -> None:
        """CSPAT00700 매수 지정가 정정 — 로그 없음."""
        q = self._worker_query
        if q is None or self._res_cache is None:
            raise RuntimeError("워커 XAQuery 미초기화")
        self._res_cache.ensure("CSPAT00700.res")
        amend_qty = qty or CONFIG["ORDER_QTY"]
        q.SetFieldData("CSPAT00700InBlock1", "OrgOrdNo", 0, org_ord_no)
        q.SetFieldData("CSPAT00700InBlock1", "AcntNo", 0, self.account_no)
        q.SetFieldData("CSPAT00700InBlock1", "InptPwd", 0, self.account_pw)
        q.SetFieldData("CSPAT00700InBlock1", "IsuNo", 0, self.isu_no)
        q.SetFieldData("CSPAT00700InBlock1", "OrdQty", 0, amend_qty)
        q.SetFieldData("CSPAT00700InBlock1", "OrdprcPtnCode", 0, CONFIG["ORD_LIMIT"])
        q.SetFieldData("CSPAT00700InBlock1", "OrdCndiTpCode", 0, CONFIG["ORD_CND_NONE"])
        q.SetFieldData("CSPAT00700InBlock1", "OrdPrc", 0, price)
        q._pending_is_amend = True  # type: ignore[attr-defined]
        q._pending_amend_price = price  # type: ignore[attr-defined]
        q._pending_is_cancel = False  # type: ignore[attr-defined]
        q.Request(0)

    def _fire_cspat00800(self, org_ord_no: int, qty: int, *, for_buy: bool = False) -> None:
        """CSPAT00800 취소 즉시 발송 — 로그 없음."""
        q = self._worker_query
        if q is None or self._res_cache is None:
            raise RuntimeError("워커 XAQuery 미초기화")
        self._res_cache.ensure("CSPAT00800.res")
        q.SetFieldData("CSPAT00800InBlock1", "OrgOrdNo", 0, org_ord_no)
        q.SetFieldData("CSPAT00800InBlock1", "AcntNo", 0, self.account_no)
        q.SetFieldData("CSPAT00800InBlock1", "InptPwd", 0, self.account_pw)
        q.SetFieldData("CSPAT00800InBlock1", "IsuNo", 0, self.isu_no)
        q.SetFieldData("CSPAT00800InBlock1", "OrdQty", 0, qty)
        q._pending_is_cancel = True  # type: ignore[attr-defined]
        q._pending_cancel_for_buy = for_buy  # type: ignore[attr-defined]
        q.Request(0)

    def _cancel_sell_order(self) -> None:
        if not self.sell_ord_no:
            enqueue_log("WARN", self.shcode, self.last_price, CONFIG["ORDER_QTY"], "매도주문번호 없음 — 시장가")
            self._send_market_sell(self.buy_fill_qty or CONFIG["ORDER_QTY"], hot_path=True)
            return
        try:
            prev = self.state
            self.state = BotState.CANCEL_PENDING
            qty = self.buy_fill_qty or CONFIG["ORDER_QTY"]
            self._fire_cspat00800(self.sell_ord_no, qty)
            enqueue_log("STATE", self.shcode, self.last_price, 0, f"{prev.value} → CANCEL_PENDING | 매도 취소")
            enqueue_log("CANCEL", self.shcode, self.tp_price, qty, f"ord={self.sell_ord_no}")
        except Exception as exc:
            enqueue_log("ERROR", self.shcode, 0, 0, f"취소 TR: {exc}")
            self._send_market_sell(self.buy_fill_qty or CONFIG["ORDER_QTY"], hot_path=True)

    def _request_order(
        self,
        bns_tp: str,
        ord_ptn: str,
        qty: int,
        price: int,
        next_state: BotState,
        memo: str,
        *,
        hot_path: bool = False,
    ) -> None:
        try:
            prev = self.state
            self.state = next_state
            self._fire_cspat00600(bns_tp, ord_ptn, qty, price, memo)
            if hot_path:
                enqueue_log(
                    "STATE", self.shcode, self.last_price, 0,
                    f"{prev.value} → {next_state.value} | {memo}",
                )
                enqueue_log("ORDER", self.shcode, price, qty, f"{memo} bns={bns_tp} ptn={ord_ptn}")
            else:
                save_and_print_log(
                    "STATE", self.shcode, self.last_price, 0,
                    f"{prev.value} → {next_state.value} | {memo}",
                )
                save_and_print_log("ORDER", self.shcode, price, qty, f"{memo} bns={bns_tp} ptn={ord_ptn}")
        except Exception as exc:
            if hot_path:
                enqueue_log("ERROR", self.shcode, price, qty, f"주문 TR: {exc}")
            else:
                save_and_print_log("ERROR", self.shcode, price, qty, f"주문 TR: {exc}")
            self.reset_cycle("주문 실패")

    def _handle_tr_error(self, msg: OrderAckMsg) -> None:
        save_and_print_log(
            "ERROR",
            self.shcode,
            0,
            0,
            f"TR msg code={msg.reject_code} {msg.reject_msg}",
        )
        if is_account_password_lock(msg.reject_code, msg.reject_msg):
            self.halt_trading(
                "계좌 주문비밀번호 잠금(01796) — HTS에서 확인·콜센터 해제 후 .env 수정·재시작"
            )
            return
        if is_tr_success_message(msg.reject_code, msg.reject_msg):
            return
        if self.state in (BotState.BUY_PEGGED, BotState.BUY_ORDERED):
            self.reset_cycle(f"주문거부 code={msg.reject_code}")

    def process_order_ack(self, msg: OrderAckMsg) -> None:
        try:
            if msg.is_tr_error:
                self._handle_tr_error(msg)
                return

            if msg.is_amend:
                self._handle_amend_ack(msg)
                return

            if msg.is_cancel:
                if msg.bns_tp == CONFIG["BNS_BUY"] or self.buy_cancel_in_flight:
                    self._handle_buy_cancel_ack(msg)
                    return
                if self.state in (BotState.SELL_ORDERED, BotState.CANCEL_PENDING, BotState.MARKET_EXIT):
                    enqueue_log(
                        "ACK", self.shcode, self.tp_price, CONFIG["ORDER_QTY"],
                        f"취소접수 ord={msg.ord_no}",
                    )
                    remain = max(0, (self.buy_fill_qty or CONFIG["ORDER_QTY"]) - self.sell_fill_qty)
                    if remain > 0:
                        self._send_market_sell(remain, hot_path=True)
                    else:
                        self.reset_cycle("취소 시점 청산됨")
                return

            if msg.ord_no <= 0:
                enqueue_log("ERROR", self.shcode, 0, 0, f"주문번호 없음 {msg.memo}")
                if self.state in (BotState.BUY_PEGGED, BotState.BUY_ORDERED):
                    self.reset_cycle("매수 거부")
                return

            if msg.bns_tp == CONFIG["BNS_BUY"]:
                self.buy_ord_no = msg.ord_no
                self.buy_order_live = True
                # REPEG 재배치: 전체 settle 대기 / 최초 PEG: 100ms만 대기
                self._mark_buy_settle(is_repeg=self.buy_amend_count > 0 or self.buy_cancel_in_flight)
                if (
                    self.state == BotState.IDLE
                    and not self._buy_already_done()
                    and self.live_buy_limit > 0
                ):
                    self.state = BotState.BUY_PEGGED
                if self._buy_amend_template is not None and self.buy_ord_no > 0:
                    self._buy_amend_template.prime(
                        self.account_no,
                        self.account_pw,
                        self.isu_no,
                        self.buy_ord_no,
                        CONFIG["ORDER_QTY"],
                    )
                enqueue_log(
                    "ACK", self.shcode, self.live_buy_limit or self.last_price,
                    CONFIG["ORDER_QTY"], f"매수접수 ord={msg.ord_no}",
                )
            elif msg.bns_tp == CONFIG["BNS_SELL"]:
                self.sell_ord_no = msg.ord_no
                enqueue_log("ACK", self.shcode, self.tp_price, CONFIG["ORDER_QTY"], f"매도접수 ord={msg.ord_no}")
        except Exception as exc:
            save_and_print_log("ERROR", self.shcode, 0, 0, f"주문응답: {exc}")

    def _handle_amend_ack(self, msg: OrderAckMsg) -> None:
        """정정 ACK — TR 응답 즉시 확정 (SC2는 보조 동기화만)."""
        self.amend_in_flight = False
        if not msg.success and not msg.reject_code and not msg.reject_msg:
            return
        if self._buy_already_done():
            enqueue_log(
                "AMEND", self.shcode, msg.amend_price, 0,
                f"정정스킵(이미체결) code={msg.reject_code}",
            )
            return
        if msg.success:
            if msg.ord_no > 0:
                self.buy_ord_no = msg.ord_no
            if msg.amend_price > 0:
                self.live_buy_limit = msg.amend_price
                self.pending_buy_limit = msg.amend_price
                self._precompute_tp_for_buy(msg.amend_price, CONFIG["ORDER_QTY"])
            self.buy_amend_count += 1
            enqueue_log(
                "AMEND", self.shcode, msg.amend_price, CONFIG["ORDER_QTY"],
                f"정정접수 ord={self.buy_ord_no} ok=1 (#{self.buy_amend_count})",
            )
            self._flush_queued_amend()
            return

        if is_amend_reject_qty_or_funds(msg.reject_code, msg.reject_msg):
            self._on_qty_reject(
                "amend", msg.amend_price, msg.reject_code, msg.reject_msg,
            )
            return
        self.buy_order_live = False
        enqueue_log(
            "AMEND", self.shcode, msg.amend_price, 0,
            f"정정거부 ord={self.buy_ord_no} code={msg.reject_code} {msg.reject_msg}",
        )
        if not self._buy_already_done():
            self.queued_buy_limit = 0


# =============================================================================
# 메인 스레드 COM 핸들러 — 큐 put 만 (연산/로깅 금지)
# =============================================================================
class AppContext:
    """스레드 간 공유 컨텍스트."""

    engine: Optional[StrategyEngine] = None
    ticker_cfg: dict = {}
    session = None
    real_tick = None
    real_order = None
    running = True
    worker_thread: Optional[threading.Thread] = None
    log_thread_started: bool = False
    target_shcode: str = ""
    k3_mismatch_logged: int = 0


def _drain_queue(q: queue.Queue, handler) -> int:
    n = 0
    while True:
        try:
            handler(q.get_nowait())
            n += 1
        except queue.Empty:
            break
    return n


def worker_loop() -> None:
    """
    전략·주문 전용 워커 스레드.
    STA COM: CoInitialize 필수, XAQuery는 이 스레드에서 독립 생성.
    """
    pythoncom.CoInitialize()
    try:
        engine = AppContext.engine
        if engine is None:
            return

        # 워커 전용 XAQuery (메인 스레드 객체와 분리)
        query = win32com.client.DispatchWithEvents("XA_DataSet.XAQuery", WorkerXAQueryEvents)
        engine.bind_worker_query(query)

        save_and_print_log("WORKER", engine.shcode, 0, 0, "워커 스레드 시작 (CoInitialize 완료)")

        while AppContext.running and engine._running:
            # SC1 체결 최우선 → 주문이벤트 → TR응답 → 틱
            _drain_queue(fill_queue, engine.process_fill)
            _drain_queue(order_event_queue, engine.process_order_event)
            _drain_queue(order_ack_queue, engine.process_order_ack)

            # 틱 버스트(IPO 초반 300~500틱/초) 대응:
            # 마지막 틱 앞에 쌓인 틱은 MA·카운트만 갱신 (주문 로직 생략)
            # 마지막 틱 1개만 process_tick 으로 완전 처리
            pending_tick: Optional[TickMsg] = None
            try:
                while True:
                    t = tick_queue.get_nowait()
                    if pending_tick is not None:
                        engine._update_ma_from_tick(pending_tick)  # MA만
                    pending_tick = t
            except queue.Empty:
                pass
            if pending_tick is not None:
                engine.process_tick(pending_tick)  # 최신 틱 완전 처리

            engine.poll_time_cut()
            pythoncom.PumpWaitingMessages()

        save_and_print_log("WORKER", engine.shcode, 0, 0, "워커 스레드 종료")
    except Exception as exc:
        sh = AppContext.target_shcode or "??????"
        save_and_print_log("ERROR", sh, 0, 0, f"워커 스레드: {exc}")
    finally:
        pythoncom.CoUninitialize()


class XASessionEvents:
    login_state: int = 0

    def OnLogin(self, code, msg):
        XASessionEvents.login_state = 1 if str(code) == "0000" else -1

    def OnDisconnect(self):
        pass  # 로깅은 워커/메인에서 처리


class XARealTickEvents:
    """시세 수신 — tick_queue put 후 즉시 리턴."""

    def OnReceiveRealData(self, szTrCode):
        try:
            shcode = str(self.GetFieldData("OutBlock", "shcode")).strip()
            target = AppContext.target_shcode
            if target and not shcode_matches(target, shcode):
                if AppContext.k3_mismatch_logged < 5:
                    AppContext.k3_mismatch_logged += 1
                    save_and_print_log(
                        "WARN", target, 0, 0,
                        f"K3 shcode불일치 무시 recv={shcode} (기대={target})",
                    )
                return
            tick_queue.put_nowait(
                TickMsg(
                    shcode=shcode,
                    price_raw=str(self.GetFieldData("OutBlock", "price")),
                    chetime=str(self.GetFieldData("OutBlock", "chetime")).strip(),
                )
            )
        except Exception:
            pass


class XARealOrderEvents:
    """SC0/SC1/SC2/SC3/SC4 — 큐 put 후 즉시 리턴."""

    def OnReceiveRealData(self, szTrCode):
        try:
            tr = str(szTrCode).strip().upper()
            recv_ts = time.perf_counter()
            if tr == "SC1":
                fill_queue.put_nowait(
                    FillMsg(
                        isuno=str(self.GetFieldData("OutBlock", "isuno")).strip(),
                        ord_no_raw=str(self.GetFieldData("OutBlock", "ordno")),
                        exec_qty_raw=str(self.GetFieldData("OutBlock", "execqty")),
                        exec_prc_raw=str(self.GetFieldData("OutBlock", "execprc")),
                        bns_tp=str(self.GetFieldData("OutBlock", "bnstpcode")).strip(),
                        recv_ts=recv_ts,
                    )
                )
                return
            if tr in ("SC0", "SC2", "SC3", "SC4"):
                order_event_queue.put_nowait(
                    OrderEventMsg(
                        tr_code=tr,
                        ord_no=safe_int(self.GetFieldData("OutBlock", "ordno")),
                        org_ord_no=safe_int(self.GetFieldData("OutBlock", "orgordno")),
                        bns_tp=str(self.GetFieldData("OutBlock", "bnstpcode")).strip(),
                        price=safe_abs_price(self.GetFieldData("OutBlock", "ordprc")),
                        unerc_qty=safe_int(self.GetFieldData("OutBlock", "unercqty")),
                        recv_ts=recv_ts,
                    )
                )
        except Exception:
            pass


class WorkerXAQueryEvents:
    """워커 스레드 XAQuery — 응답을 order_ack_queue 로 전달."""

    def OnReceiveData(self, szTrCode):
        try:
            tr = str(szTrCode)
            is_cancel = bool(getattr(self, "_pending_is_cancel", False))
            if tr == "CSPAT00600":
                ord_no = safe_int(self.GetFieldData("CSPAT00600OutBlock2", "OrdNo", 0))
                bns_tp = str(getattr(self, "_pending_bns", "")).strip()
                memo = str(getattr(self, "_pending_memo", "")).strip()
                order_ack_queue.put_nowait(
                    OrderAckMsg(tr_code=tr, ord_no=ord_no, bns_tp=bns_tp, memo=memo, is_cancel=False)
                )
            elif tr == "CSPAT00700":
                ord_no = safe_int(self.GetFieldData("CSPAT00700OutBlock2", "OrdNo", 0))
                amend_price = int(getattr(self, "_pending_amend_price", 0) or 0)
                if ord_no > 0:
                    order_ack_queue.put_nowait(
                        OrderAckMsg(
                            tr_code=tr,
                            ord_no=ord_no,
                            bns_tp=CONFIG["BNS_BUY"],
                            memo="amend",
                            is_amend=True,
                            success=True,
                            amend_price=amend_price,
                        )
                    )
                setattr(self, "_pending_is_amend", False)
            elif tr == "CSPAT00800":
                ord_no = safe_int(self.GetFieldData("CSPAT00800OutBlock2", "OrdNo", 0))
                for_buy = bool(getattr(self, "_pending_cancel_for_buy", False))
                if ord_no > 0:
                    order_ack_queue.put_nowait(
                        OrderAckMsg(
                            tr_code=tr,
                            ord_no=ord_no,
                            bns_tp=CONFIG["BNS_BUY"] if for_buy else "",
                            memo="cancel",
                            is_cancel=True,
                            success=True,
                        )
                    )
                setattr(self, "_pending_cancel_for_buy", False)
        except Exception:
            pass

    def OnReceiveMessage(self, systemError, messageCode, message):
        try:
            code = str(messageCode).strip()
            msg_text = str(message).strip()
            if not systemError and is_tr_success_message(code, msg_text):
                return
            is_amend = bool(getattr(self, "_pending_is_amend", False))
            is_buy_cancel = bool(getattr(self, "_pending_cancel_for_buy", False))
            amend_price = int(getattr(self, "_pending_amend_price", 0) or 0)
            if is_buy_cancel:
                order_ack_queue.put_nowait(
                    OrderAckMsg(
                        tr_code="CSPAT00800",
                        ord_no=0,
                        bns_tp=CONFIG["BNS_BUY"],
                        memo="cancel_reject",
                        is_cancel=True,
                        success=False,
                        reject_code=code,
                        reject_msg=str(message).strip(),
                    )
                )
                setattr(self, "_pending_cancel_for_buy", False)
                return
            if is_amend:
                order_ack_queue.put_nowait(
                    OrderAckMsg(
                        tr_code="CSPAT00700",
                        ord_no=0,
                        bns_tp=CONFIG["BNS_BUY"],
                        memo="amend_reject",
                        is_amend=True,
                        success=False,
                        amend_price=amend_price,
                        reject_code=code,
                        reject_msg=str(message).strip(),
                    )
                )
                setattr(self, "_pending_is_amend", False)
                return
            order_ack_queue.put_nowait(
                OrderAckMsg(
                    tr_code="TR_MSG",
                    ord_no=0,
                    bns_tp=str(getattr(self, "_pending_bns", "")).strip(),
                    memo=str(getattr(self, "_pending_memo", "")).strip(),
                    is_tr_error=True,
                    success=False,
                    reject_code=code,
                    reject_msg=str(message).strip(),
                )
            )
        except Exception:
            pass


# =============================================================================
# 메인 스레드: 로그인 · 실시간 구독 · COM 펌프
# =============================================================================
def wait_until_start(start_time_str: str) -> None:
    """08:50 등 시작 시각까지 논블로킹 대기 (PumpWaitingMessages 유지)."""
    target = parse_start_datetime(start_time_str)
    now = datetime.now()
    if now >= target:
        save_and_print_log("WAIT", AppContext.target_shcode, 0, 0, f"시작시각 경과 — 즉시 시작 ({start_time_str})")
        return

    save_and_print_log(
        "WAIT",
        AppContext.target_shcode,
        0,
        0,
        f"{start_time_str} 대기 중 (현재 {now.strftime('%H:%M:%S')})",
    )
    while datetime.now() < target and AppContext.running:
        pythoncom.PumpWaitingMessages()


def connect_and_login(session) -> bool:
    host, port = CONFIG["SERVER_HOST"], CONFIG["SERVER_PORT"]
    if not session.ConnectServer(host, port):
        save_and_print_log("ERROR", AppContext.target_shcode, 0, 0, f"서버연결 실패 err={session.GetLastError()}")
        return False

    save_and_print_log("CONN", AppContext.target_shcode, 0, 0, f"{host}:{port}")

    if not session.Login(CONFIG["USER_ID"], CONFIG["USER_PW"], CONFIG["CERT_PW"], 0, False):
        save_and_print_log("ERROR", AppContext.target_shcode, 0, 0, f"로그인요청 실패 err={session.GetLastError()}")
        return False

    while XASessionEvents.login_state == 0:
        pythoncom.PumpWaitingMessages()
    return XASessionEvents.login_state == 1


def register_realtime(real_tick, real_order, ticker_cfg: dict) -> None:
    """단일 종목 실시간 구독 (시장 자동판별 → S3_/K3_ + SC1)."""
    shcode = ticker_cfg["shcode"]
    tick_tr = tick_tr_for_market(ticker_cfg["market"])

    real_tick.LoadFromResFile(resolve_res_path(f"{tick_tr}.res"))
    real_tick.SetFieldData("InBlock", "shcode", shcode)
    real_tick.AdviseRealData()

    for sc_res in ("SC0", "SC1", "SC2", "SC3", "SC4"):
        real_order.LoadFromResFile(resolve_res_path(f"{sc_res}.res"))
        real_order.AdviseRealData()

    save_and_print_log(
        "REAL", shcode, 0, 0, f"{tick_tr} + SC0~SC4 구독 (PEG 정정)",
    )


def resolve_account(session) -> str:
    acct = CONFIG["ACCOUNT_NO"]
    if acct:
        return acct
    try:
        if session.GetAccountListCount() > 0:
            return session.GetAccountList(0)
    except Exception as exc:
        save_and_print_log("ERROR", AppContext.target_shcode, 0, 0, f"계좌조회: {exc}")
    return ""


def shutdown() -> None:
    AppContext.running = False
    if AppContext.engine:
        AppContext.engine.stop()
    try:
        if AppContext.real_tick and AppContext.target_shcode:
            AppContext.real_tick.UnadviseRealDataWithKey(AppContext.target_shcode)
    except Exception:
        pass
    try:
        if AppContext.real_order:
            AppContext.real_order.UnadviseRealData()
    except Exception:
        pass
    if AppContext.worker_thread and AppContext.worker_thread.is_alive():
        AppContext.worker_thread.join(timeout=5.0)
    stop_log_thread()
    flush_log_queue()
    save_and_print_log("STOP", AppContext.target_shcode, 0, 0, "봇 종료")


def is_live_server() -> bool:
    return "demo" not in CONFIG["SERVER_HOST"].lower()


def validate_credentials() -> bool:
    """필수 인증 정보·전략 설정 확인."""
    missing = []
    if not CONFIG["USER_ID"]:
        missing.append("XING_USER_ID")
    if not CONFIG["USER_PW"]:
        missing.append("XING_USER_PW")
    if is_live_server() and not CONFIG["CERT_PW"]:
        missing.append("XING_CERT_PW (실전투자 필수)")
    if missing:
        print(
            f"[ERROR] 필수 설정 누락: {', '.join(missing)}\n"
            f"  → {ENV_PATH} 파일을 만들거나 .env.example 을 참고하세요."
        )
        return False

    order_qty_env = _env_int("STRATEGY_ORDER_QTY", 1)
    if order_qty_env != 1:
        print(
            "[ERROR] STRATEGY_ORDER_QTY는 1만 허용합니다. "
            "겹침 매수 방지를 위해 1주 고정입니다."
        )
        return False
    if CONFIG["MA_WINDOW"] < 1:
        print("[ERROR] STRATEGY_MA_WINDOW는 1 이상이어야 합니다.")
        return False
    if CONFIG["ENTRY_GAP"] <= 0 or CONFIG["TAKE_PROFIT"] <= 0:
        print("[ERROR] STRATEGY_ENTRY_GAP_PCT / STRATEGY_TAKE_PROFIT_PCT는 0보다 커야 합니다.")
        return False
    return True


def main() -> int:
    if not validate_credentials():
        return 1

    try:
        ticker_cfg = load_ticker_config()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return 1

    AppContext.ticker_cfg = ticker_cfg
    AppContext.target_shcode = ticker_cfg["shcode"]
    AppContext.engine = StrategyEngine(ticker_cfg)

    mode = "실전" if is_live_server() else "모의"
    save_and_print_log(
        "START",
        ticker_cfg["shcode"],
        0,
        0,
        f"{mode} | {CONFIG['SERVER_HOST']} | MA{CONFIG['MA_WINDOW']} "
        f"entry=-{CONFIG['ENTRY_GAP']:.2%} tp=+{CONFIG['TAKE_PROFIT']:.2%} "
        f"qty=1 | {ticker_cfg.get('memo', '')}",
    )

    try:
        session = win32com.client.DispatchWithEvents("XA_Session.XASession", XASessionEvents)
        real_tick = win32com.client.DispatchWithEvents("XA_DataSet.XAReal", XARealTickEvents)
        real_order = win32com.client.DispatchWithEvents("XA_DataSet.XAReal", XARealOrderEvents)
        AppContext.session = session
        AppContext.real_tick = real_tick
        AppContext.real_order = real_order
    except Exception as exc:
        save_and_print_log("ERROR", ticker_cfg["shcode"], 0, 0, f"COM 초기화: {exc}")
        return 1

    # 08:50 전 로그인 가능, 시작 시각까지 대기
    wait_until_start(ticker_cfg["start_time"])

    if not connect_and_login(session):
        save_and_print_log("ERROR", ticker_cfg["shcode"], 0, 0, "로그인 실패")
        return 1

    account = resolve_account(session)
    AppContext.engine.set_account(account)
    save_and_print_log("LOGIN", ticker_cfg["shcode"], 0, 0, f"성공 | 계좌={account}")
    peg_mode = (
        "취소+재주문"
        if CONFIG["AMEND_MAX_PER_ORDER"] == 0
        else f"정정≤{CONFIG['AMEND_MAX_PER_ORDER']}회+재주문"
    )
    save_and_print_log(
        "WARN", ticker_cfg["shcode"], 0, 0,
        f"PEG={peg_mode} | 시작 전 HTS에서 {ticker_cfg['shcode']} 미체결·중복주문 없어야 함",
    )

    market = detect_market(ticker_cfg["shcode"])
    ticker_cfg["market"] = market
    AppContext.engine.market = market

    try:
        register_realtime(real_tick, real_order, ticker_cfg)
    except Exception as exc:
        save_and_print_log("ERROR", ticker_cfg["shcode"], 0, 0, f"실시간등록: {exc}")
        return 1

    # 로그 스레드 기동 (print/CSV — 워커와 분리)
    start_log_thread()
    AppContext.log_thread_started = True

    # 워커 스레드 기동 (주문·전략)
    AppContext.worker_thread = threading.Thread(
        target=worker_loop,
        name="StrategyWorker",
        daemon=True,
    )
    AppContext.worker_thread.start()

    save_and_print_log(
        "READY",
        ticker_cfg["shcode"],
        0,
        0,
        "메인=시세 | 워커=전략/주문 | 로그=별도스레드 | Ctrl+C 종료",
    )

    try:
        while AppContext.running:
            pythoncom.PumpWaitingMessages()
    except KeyboardInterrupt:
        shutdown()
    except Exception as exc:
        save_and_print_log("ERROR", ticker_cfg["shcode"], 0, 0, f"메인루프: {exc}")
        shutdown()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
