#!/usr/bin/env python3
"""
AlphaChainBots — Hyperliquid Trading Bot (15M · EMA 8/21)
Entrada : EMA 8/21 crossover + S/R + Fibonacci 0.5/0.618 + Volume Profile
Salida  : dinámica — sin TP fijo. Cierra cuando:
            1) EMA rápida cruza contra la posición
            2) Precio rompe S/R relevante en contra
            3) RSI > 75 (long) o RSI < 25 (short)
            4) Trailing stop 0.5% es tocado
"""

import os
import time
import logging
from typing import Optional
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from eth_account import Account
from hl_client import HLInfo, HLExchange, MAINNET_URL, TESTNET_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

load_dotenv()

TESTNET         = os.getenv("TESTNET", "true").lower() == "true"
PRIVATE_KEY     = os.getenv("PRIVATE_KEY", "")
ACCOUNT_ADDRESS = os.getenv("ACCOUNT_ADDRESS", "")

INTERVAL        = "15m"
EMA_FAST        = 8
EMA_SLOW        = 21
RSI_PERIOD      = 14
RSI_OB          = 75
RSI_OS          = 25
CANDLE_LIMIT    = 300
TOP_N_COINS     = 10
LEVERAGE        = 5
RISK_PER_TRADE  = 0.02
TRAILING_PCT    = 0.005
MIN_VOL_RATIO   = 1.3
FIBO_ZONE_PCT   = 0.010
SR_LOOKBACK     = 60
SR_WINDOW       = 4
SR_BREAK_PCT    = 0.004
LOOP_SLEEP      = 60 * 15

FIBO_LEVELS  = [0.236, 0.382, 0.500, 0.618, 0.786]
FIBO_CONFIRM = {0.500, 0.618}

INTERVAL_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}

FALLBACK_COINS = ["BTC","ETH","SOL","HYPE","TAO","XRP","DOGE","AVAX","BNB","LINK"]


def setup_client() -> tuple:
    if not PRIVATE_KEY:
        raise ValueError("PRIVATE_KEY no configurada en .env")
    key      = PRIVATE_KEY.strip().removeprefix("0x").removeprefix("0X")
    account  = Account.from_key(key)
    address  = ACCOUNT_ADDRESS or account.address
    api_url  = TESTNET_URL if TESTNET else MAINNET_URL
    info     = HLInfo(api_url)
    exchange = HLExchange(account, api_url, account_address=address)
    log.info(f"Conectado a Hyperliquid {'TESTNET' if TESTNET else 'MAINNET'} | Wallet: {address}")
    return info, exchange, address


def get_top_coins(info: HLInfo, n: int = TOP_N_COINS) -> list:
    try:
        meta, ctxs = info.meta_and_asset_ctxs()
        coins_data = []
        for i, asset in enumerate(meta["universe"]):
            if i < len(ctxs):
                try:
                    vol = float(ctxs[i].get("dayNtlVlm", 0))
                except (TypeError, ValueError):
                    vol = 0
                coins_data.append({"coin": asset["name"], "vol": vol})
        coins_data.sort(key=lambda x: x["vol"], reverse=True)
        top = [c["coin"] for c in coins_data[:n]]
        if top:
            return top
    except Exception as e:
        log.warning(f"get_top_coins error: {e}. Usando fallback.")
    return FALLBACK_COINS[:n]


def fetch_candles(info: HLInfo, coin: str, interval: str = INTERVAL,
                  limit: int = CANDLE_LIMIT) -> pd.DataFrame:
    end_ms   = int(time.time() * 1000)
    ms       = INTERVAL_MS.get(interval, INTERVAL_MS["15m"])
    start_ms = end_ms - limit * ms
    raw = info.candles_snapshot(coin, interval, start_ms, end_ms)
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw).rename(columns={
        "t": "time", "o": "open", "h": "high", "l": "low",
        "c": "close", "v": "volume",
    })
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    return df.sort_values("time").reset_index(drop=True)


def add_emas(df: pd.DataFrame) -> pd.DataFrame:
    df["ema_fast"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    return df


def calc_rsi(df: pd.DataFrame, period: int = RSI_PERIOD) -> pd.Series:
    delta    = df["close"].diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, float("inf"))
    return 100 - (100 / (1 + rs))


def detect_crossover(df: pd.DataFrame) -> Optional[str]:
    if len(df) < EMA_SLOW + 3:
        return None
    prev2, prev = df.iloc[-3], df.iloc[-2]
    if prev2["ema_fast"] <= prev2["ema_slow"] and prev["ema_fast"] > prev["ema_slow"]:
        return "long"
    if prev2["ema_fast"] >= prev2["ema_slow"] and prev["ema_fast"] < prev["ema_slow"]:
        return "short"
    return None


def ema_reversal_exit(df: pd.DataFrame, direction: str) -> bool:
    if len(df) < 3:
        return False
    prev2, prev = df.iloc[-3], df.iloc[-2]
    if direction == "long":
        return prev2["ema_fast"] >= prev2["ema_slow"] and prev["ema_fast"] < prev["ema_slow"]
    return prev2["ema_fast"] <= prev2["ema_slow"] and prev["ema_fast"] > prev["ema_slow"]


def sr_breakout_exit(price: float, direction: str,
                     highs: list, lows: list, pct: float = SR_BREAK_PCT) -> bool:
    if direction == "long":
        return any(price < lvl * (1 - pct) for lvl in lows)
    return any(price > lvl * (1 + pct) for lvl in highs)


def rsi_extreme_exit(rsi_val: float, direction: str) -> bool:
    if direction == "long"  and rsi_val > RSI_OB: return True
    if direction == "short" and rsi_val < RSI_OS: return True
    return False


def check_exit_conditions(df, current_price, direction, ts) -> tuple:
    ts.update(current_price)
    if ts.triggered(current_price):
        return True, f"Trailing Stop | precio={current_price:.4f} stop={ts.stop:.4f}"
    if ema_reversal_exit(df, direction):
        return True, "Cruce EMA inverso"
    rsi_val = float(df["rsi"].iloc[-2])
    if rsi_extreme_exit(rsi_val, direction):
        return True, f"RSI extremo {rsi_val:.1f}"
    highs, lows = find_pivots(df)
    if sr_breakout_exit(current_price, direction, highs, lows):
        return True, f"Ruptura S/R | precio={current_price:.4f}"
    return False, ""


def find_pivots(df: pd.DataFrame, window: int = SR_WINDOW) -> tuple:
    data = df.tail(SR_LOOKBACK).reset_index(drop=True)
    highs, lows = [], []
    for i in range(window, len(data) - window):
        if data["high"].iloc[i] == data["high"].iloc[i - window: i + window + 1].max():
            highs.append(float(data["high"].iloc[i]))
        if data["low"].iloc[i] == data["low"].iloc[i - window: i + window + 1].min():
            lows.append(float(data["low"].iloc[i]))
    return highs, lows


def near_sr(price: float, highs: list, lows: list, pct: float = 0.01) -> bool:
    return any(abs(price - lvl) / lvl <= pct for lvl in highs + lows)


def calc_fibonacci(df: pd.DataFrame) -> dict:
    recent = df.tail(SR_LOOKBACK)
    sh, sl = float(recent["high"].max()), float(recent["low"].min())
    return {lvl: sh - lvl * (sh - sl) for lvl in FIBO_LEVELS}


def near_fibo_confirm(price: float, fib: dict) -> bool:
    for lvl, lvl_price in fib.items():
        if lvl in FIBO_CONFIRM and abs(price - lvl_price) / lvl_price <= FIBO_ZONE_PCT:
            return True
    return False


def volume_confirms(df: pd.DataFrame, lookback: int = 20) -> bool:
    if len(df) < lookback + 2:
        return False
    avg_vol  = df["volume"].iloc[-(lookback + 1):-1].mean()
    last_vol = df["volume"].iloc[-2]
    return (last_vol / avg_vol if avg_vol > 0 else 0) >= MIN_VOL_RATIO


def calc_size(equity: float, price: float) -> float:
    return round(equity * RISK_PER_TRADE * LEVERAGE / price, 4)


class TrailingStop:
    def __init__(self, entry: float, direction: str, pct: float = TRAILING_PCT):
        self.direction = direction
        self.pct  = pct
        self.best = entry
        self.stop = entry * (1 - pct) if direction == "long" else entry * (1 + pct)

    def update(self, price: float) -> None:
        if self.direction == "long" and price > self.best:
            self.best = price; self.stop = self.best * (1 - self.pct)
        elif self.direction == "short" and price < self.best:
            self.best = price; self.stop = self.best * (1 + self.pct)

    def triggered(self, price: float) -> bool:
        return price <= self.stop if self.direction == "long" else price >= self.stop

    def __str__(self) -> str:
        return f"TS({self.direction} best={self.best:.4f} stop={self.stop:.4f})"


def set_leverage(exchange: HLExchange, coin: str) -> None:
    try:
        exchange.update_leverage(LEVERAGE, coin, is_cross=False)
    except Exception as e:
        log.warning(f"  No se pudo fijar leverage para {coin}: {e}")


def open_order(exchange: HLExchange, coin: str, direction: str,
               size: float, price: float) -> dict:
    is_buy   = direction == "long"
    slip     = 0.0015
    limit_px = round(price * (1 + slip) if is_buy else price * (1 - slip), 4)
    set_leverage(exchange, coin)
    result = exchange.order(coin, is_buy, size, limit_px,
                            {"limit": {"tif": "Ioc"}}, reduce_only=False)
    log.info(f"  ABRIR {direction.upper()} {coin} | size={size} | px={limit_px} | {result}")
    return result


def close_order(exchange: HLExchange, coin: str, direction: str,
                size: float, price: float) -> dict:
    is_buy   = direction == "short"
    slip     = 0.002
    limit_px = round(price * (1 + slip) if is_buy else price * (1 - slip), 4)
    result = exchange.order(coin, is_buy, abs(size), limit_px,
                            {"limit": {"tif": "Ioc"}}, reduce_only=True)
    log.info(f"  CERRAR {direction.upper()} {coin} | size={abs(size)} | px={limit_px} | {result}")
    return result


def get_equity(info: HLInfo, address: str) -> float:
    state = info.user_state(address)
    return float(state.get("marginSummary", {}).get("accountValue", 0))


def get_open_position(info: HLInfo, address: str, coin: str) -> Optional[dict]:
    state = info.user_state(address)
    for pos in state.get("assetPositions", []):
        p = pos.get("position", {})
        if p.get("coin") == coin and float(p.get("szi", 0)) != 0:
            return p
    return None


class Bot:
    def __init__(self):
        self.info, self.exchange, self.address = setup_client()
        self.coins:     list = []
        self.positions: dict = {}

    def manage_positions(self) -> None:
        for coin in list(self.positions.keys()):
            try:
                df = fetch_candles(self.info, coin)
                if df.empty or len(df) < EMA_SLOW + 3:
                    continue
                df = add_emas(df); df["rsi"] = calc_rsi(df)
                current_price = float(df["close"].iloc[-1])
                pos_data  = self.positions[coin]
                ts        = pos_data["trailing_stop"]
                direction = pos_data["direction"]
                should_close, reason = check_exit_conditions(
                    df, current_price, direction, ts)
                if should_close:
                    pos = get_open_position(self.info, self.address, coin)
                    if pos:
                        close_order(self.exchange, coin, direction,
                                    float(pos["szi"]), current_price)
                    log.info(f"[{coin}] CIERRE — {reason}")
                    del self.positions[coin]
            except Exception as e:
                log.error(f"[{coin}] Error gestionando posición: {e}")

    def scan_entries(self) -> None:
        equity = get_equity(self.info, self.address)
        if equity <= 0:
            log.warning("Equity = 0. Verifica que la testnet está financiada.")
            return
        log.info(f"Equity: ${equity:.2f}")
        for coin in self.coins:
            if coin in self.positions:
                continue
            try:
                df = fetch_candles(self.info, coin)
                if len(df) < EMA_SLOW + 10:
                    continue
                df     = add_emas(df)
                signal = detect_crossover(df)
                if not signal:
                    continue
                price = float(df["close"].iloc[-2])
                highs, lows = find_pivots(df)
                if not near_sr(price, highs, lows):
                    continue
                fib = calc_fibonacci(df)
                if not near_fibo_confirm(price, fib):
                    continue
                if not volume_confirms(df):
                    continue
                size = calc_size(equity, price)
                if size <= 0:
                    continue
                log.info(f"[{coin}] ENTRADA {signal.upper()} size={size} px={price:.4f}")
                open_order(self.exchange, coin, signal, size, price)
                self.positions[coin] = {
                    "direction":     signal,
                    "size":          size,
                    "entry":         price,
                    "trailing_stop": TrailingStop(price, signal),
                }
            except Exception as e:
                log.error(f"[{coin}] Error en escaneo: {e}")

    def run(self) -> None:
        log.info("=" * 60)
        log.info(f" AlphaChainBots 15M·EMA8/21 — {'TESTNET' if TESTNET else 'MAINNET'}")
        log.info(f" EMA {EMA_FAST}/{EMA_SLOW} | Trailing {TRAILING_PCT*100:.1f}%"
                 f" | RSI {RSI_OS}/{RSI_OB} | {LEVERAGE}x")
        log.info("=" * 60)
        self.coins = get_top_coins(self.info)
        while True:
            try:
                log.info(f"\n{'='*50}")
                log.info(f" Ciclo 15M: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                log.info(f"{'='*50}")
                self.manage_positions()
                self.scan_entries()
                log.info(f"\nPosiciones: {list(self.positions.keys()) or 'ninguna'}")
                log.info(f"Próximo ciclo en 15m...\n")
                time.sleep(LOOP_SLEEP)
            except KeyboardInterrupt:
                log.info("Bot detenido."); break
            except Exception as e:
                log.error(f"Error inesperado: {e}"); time.sleep(60)


if __name__ == "__main__":
    Bot().run()
