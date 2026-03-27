#!/usr/bin/env python3
"""
sim_engine.py — Motor de simulación para el dashboard web.
Exporta start() y get_state() para uso desde server.py
"""

import time
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import requests
import pandas as pd

# ─── CONSTANTES ───────────────────────────────────────────────────────────────
INITIAL_EQUITY  = 10_000.0
TOP_N_COINS     = 10
RSI_PERIOD      = 14
SR_LOOKBACK     = 60
SR_WINDOW       = 4
FIBO_LEVELS     = [0.236, 0.382, 0.500, 0.618, 0.786]
FIBO_CONFIRM    = {0.500, 0.618}
SIM_CYCLE_SEC   = 300
MAX_SIGNALS     = 60
MAX_TRADES      = 100

INTERVAL_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000,
    "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}

FALLBACK_COINS = ["BTC", "ETH", "SOL", "HYPE", "TAO", "XRP", "DOGE", "AVAX", "BNB", "LINK"]
_HL_URL = "https://api.hyperliquid.xyz/info"


# ─── CONFIGURACIÓN POR ESTRATEGIA ─────────────────────────────────────────────
@dataclass
class BotConfig:
    name:           str
    label:          str
    interval:       str
    ma_type:        str      # 'ema' | 'sma'
    ma_fast:        int
    ma_slow:        int
    trailing_pct:   float
    min_vol_ratio:  float
    sr_near_pct:    float
    fibo_zone_pct:  float
    candle_limit:   int
    rsi_ob:         float = 75.0
    rsi_os:         float = 25.0
    sr_break_pct:   float = 0.004
    leverage:       float = 3.0
    risk_per_trade: float = 0.02


CONFIGS = [
    BotConfig(
        name="bot_4h",       label="BOT·4H",
        interval="4h",       ma_type="ema",  ma_fast=20,  ma_slow=50,
        trailing_pct=0.015,  min_vol_ratio=1.5, sr_near_pct=0.010,
        fibo_zone_pct=0.015, candle_limit=200,
    ),
    BotConfig(
        name="bot_1h",       label="BOT·1H·EMA",
        interval="1h",       ma_type="ema",  ma_fast=20,  ma_slow=50,
        trailing_pct=0.010,  min_vol_ratio=1.5, sr_near_pct=0.010,
        fibo_zone_pct=0.012, candle_limit=200,
    ),
    BotConfig(
        name="bot_1h_ma",    label="BOT·1H·SMA",
        interval="1h",       ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.010,  min_vol_ratio=1.3, sr_near_pct=0.008,
        fibo_zone_pct=0.012, candle_limit=300,
    ),
]


# ─── API REST (mainnet, sin autenticación) ────────────────────────────────────
def _hl_post(payload: dict):
    r = requests.post(_HL_URL, json=payload, timeout=15)
    r.raise_for_status()
    return r.json()


def get_top_coins(n: int = TOP_N_COINS) -> list:
    try:
        data = _hl_post({"type": "metaAndAssetCtxs"})
        meta, ctxs = data[0], data[1]
        coins_data = []
        for i, asset in enumerate(meta["universe"]):
            if i < len(ctxs):
                try:
                    vol = float(ctxs[i].get("dayNtlVlm", 0))
                except (TypeError, ValueError):
                    vol = 0
                coins_data.append({"coin": asset["name"], "vol": vol})
        coins_data.sort(key=lambda x: x["vol"], reverse=True)
        top = [d["coin"] for d in coins_data[:n]]
        if top:
            return top
    except Exception as e:
        print(f"[sim_engine] get_top_coins error: {e}. Usando fallback.")
    return FALLBACK_COINS[:n]


def fetch_candles(coin: str, interval: str, limit: int) -> pd.DataFrame:
    end_ms   = int(time.time() * 1000)
    ms       = INTERVAL_MS.get(interval, INTERVAL_MS["1h"])
    start_ms = end_ms - limit * ms
    raw = _hl_post({
        "type": "candleSnapshot",
        "req": {"coin": coin, "interval": interval,
                "startTime": start_ms, "endTime": end_ms},
    })
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


# ─── INDICADORES ──────────────────────────────────────────────────────────────
def add_mas(df: pd.DataFrame, cfg: BotConfig) -> pd.DataFrame:
    if cfg.ma_type == "ema":
        df["ma_fast"] = df["close"].ewm(span=cfg.ma_fast, adjust=False).mean()
        df["ma_slow"] = df["close"].ewm(span=cfg.ma_slow, adjust=False).mean()
    else:
        df["ma_fast"] = df["close"].rolling(cfg.ma_fast).mean()
        df["ma_slow"] = df["close"].rolling(cfg.ma_slow).mean()
    return df


def calc_rsi(df: pd.DataFrame) -> pd.Series:
    d    = df["close"].diff()
    gain = d.clip(lower=0)
    loss = -d.clip(upper=0)
    ag   = gain.ewm(com=RSI_PERIOD - 1, adjust=False).mean()
    al   = loss.ewm(com=RSI_PERIOD - 1, adjust=False).mean()
    rs   = ag / al.replace(0, float("inf"))
    return 100 - (100 / (1 + rs))


def detect_crossover(df: pd.DataFrame) -> Optional[str]:
    v = df.dropna(subset=["ma_fast", "ma_slow"])
    if len(v) < 3:
        return None
    p2, p = v.iloc[-3], v.iloc[-2]
    if p2["ma_fast"] <= p2["ma_slow"] and p["ma_fast"] > p["ma_slow"]:
        return "long"
    if p2["ma_fast"] >= p2["ma_slow"] and p["ma_fast"] < p["ma_slow"]:
        return "short"
    return None


def find_pivots(df: pd.DataFrame) -> tuple:
    data = df.tail(SR_LOOKBACK).reset_index(drop=True)
    highs, lows = [], []
    for i in range(SR_WINDOW, len(data) - SR_WINDOW):
        sl = slice(i - SR_WINDOW, i + SR_WINDOW + 1)
        if data["high"].iloc[i] == data["high"].iloc[sl].max():
            highs.append(float(data["high"].iloc[i]))
        if data["low"].iloc[i] == data["low"].iloc[sl].min():
            lows.append(float(data["low"].iloc[i]))
    return highs, lows


def near_sr(price: float, highs: list, lows: list, pct: float) -> bool:
    return any(abs(price - lvl) / lvl <= pct for lvl in highs + lows)


def calc_fibo(df: pd.DataFrame) -> dict:
    r  = df.tail(SR_LOOKBACK)
    sh = float(r["high"].max())
    sl = float(r["low"].min())
    rng = sh - sl
    return {lvl: sh - lvl * rng for lvl in FIBO_LEVELS}


def near_fibo(price: float, fib: dict, zone: float) -> bool:
    return any(
        lvl in FIBO_CONFIRM and abs(price - p) / p <= zone
        for lvl, p in fib.items()
    )


def volume_ok(df: pd.DataFrame, ratio: float) -> bool:
    if len(df) < 22:
        return False
    avg  = df["volume"].iloc[-21:-1].mean()
    last = df["volume"].iloc[-2]
    return (last / avg if avg > 0 else 0) >= ratio


# ─── TRAILING STOP ────────────────────────────────────────────────────────────
class TrailingStop:
    def __init__(self, entry: float, direction: str, pct: float):
        self.direction = direction
        self.pct       = pct
        self.best      = entry
        self.stop      = entry * (1 - pct) if direction == "long" else entry * (1 + pct)

    def update(self, price: float) -> None:
        if self.direction == "long" and price > self.best:
            self.best = price
            self.stop = self.best * (1 - self.pct)
        elif self.direction == "short" and price < self.best:
            self.best = price
            self.stop = self.best * (1 + self.pct)

    def triggered(self, price: float) -> bool:
        return price <= self.stop if self.direction == "long" else price >= self.stop


# ─── POSICIÓN VIRTUAL ─────────────────────────────────────────────────────────
@dataclass
class VirtualPos:
    coin:          str
    direction:     str
    size:          float
    entry_price:   float
    trailing_stop: TrailingStop
    opened_at:     datetime = field(default_factory=datetime.now)
    current_price: float = 0.0

    @property
    def pnl(self) -> float:
        mult = 1 if self.direction == "long" else -1
        return (self.current_price - self.entry_price) * self.size * mult

    @property
    def pnl_pct(self) -> float:
        if self.entry_price == 0:
            return 0.0
        mult = 1 if self.direction == "long" else -1
        return (self.current_price - self.entry_price) / self.entry_price * mult * 100

    @property
    def duration(self) -> str:
        secs = int((datetime.now() - self.opened_at).total_seconds())
        h, m = divmod(secs // 60, 60)
        return f"{h}h{m:02d}m" if h else f"{m}m"

    def to_dict(self) -> dict:
        return {
            "coin":          self.coin,
            "direction":     self.direction,
            "size":          round(self.size, 6),
            "entry_price":   round(self.entry_price, 6),
            "current_price": round(self.current_price, 6),
            "stop":          round(self.trailing_stop.stop, 6),
            "pnl":           round(self.pnl, 2),
            "pnl_pct":       round(self.pnl_pct, 2),
            "duration":      self.duration,
            "opened_at":     self.opened_at.strftime("%H:%M:%S"),
        }


# ─── PORTFOLIO VIRTUAL ────────────────────────────────────────────────────────
class VirtualPortfolio:
    def __init__(self):
        self.cash       = INITIAL_EQUITY
        self.positions: dict[str, VirtualPos] = {}
        self.closed_pnl = 0.0
        self.trades     = 0
        self.wins       = 0
        self.history:   list[dict] = []
        self._lock      = threading.Lock()

    @property
    def equity(self) -> float:
        return self.cash + sum(p.pnl for p in self.positions.values())

    @property
    def total_pnl(self) -> float:
        return self.equity - INITIAL_EQUITY

    @property
    def total_pnl_pct(self) -> float:
        return self.total_pnl / INITIAL_EQUITY * 100

    def open(self, coin: str, direction: str, price: float, cfg: BotConfig) -> bool:
        with self._lock:
            if coin in self.positions or price <= 0:
                return False
            notional = self.cash * cfg.risk_per_trade * cfg.leverage
            size = round(notional / price, 6)
            self.positions[coin] = VirtualPos(
                coin=coin, direction=direction, size=size,
                entry_price=price, current_price=price,
                trailing_stop=TrailingStop(price, direction, cfg.trailing_pct),
            )
            return True

    def update(self, coin: str, price: float) -> None:
        with self._lock:
            if coin in self.positions:
                self.positions[coin].current_price = price
                self.positions[coin].trailing_stop.update(price)

    def close(self, coin: str, reason: str) -> Optional[float]:
        with self._lock:
            pos = self.positions.pop(coin, None)
            if pos is None:
                return None
            pnl = pos.pnl
            self.cash       += pnl
            self.closed_pnl += pnl
            self.trades     += 1
            if pnl > 0:
                self.wins += 1
            if len(self.history) >= MAX_TRADES:
                self.history.pop(0)
            self.history.append({
                "coin":        pos.coin,
                "direction":   pos.direction,
                "entry_price": round(pos.entry_price, 6),
                "exit_price":  round(pos.current_price, 6),
                "pnl":         round(pnl, 2),
                "pnl_pct":     round(pos.pnl_pct, 2),
                "reason":      reason,
                "duration":    pos.duration,
                "closed_at":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ts":          time.time(),
            })
            return pnl

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "equity":       round(self.equity, 2),
                "cash":         round(self.cash, 2),
                "total_pnl":    round(self.total_pnl, 2),
                "total_pnl_pct": round(self.total_pnl_pct, 2),
                "trades":       self.trades,
                "wins":         self.wins,
                "positions":    [p.to_dict() for p in self.positions.values()],
                "history":      list(reversed(self.history)),
            }


# ─── BOT SIMULADO ─────────────────────────────────────────────────────────────
class SimBot:
    def __init__(self, cfg: BotConfig, coins: list):
        self.cfg       = cfg
        self.coins     = coins
        self.portfolio = VirtualPortfolio()
        self.last_scan = "—"
        self.status    = "iniciando"
        self.errors    = 0
        self.signals:  list[dict] = []
        self._sig_lock = threading.Lock()

    def _log_signal(self, coin: str, sig_type: str, action: str, reason: str = "") -> None:
        entry = {
            "time":   datetime.now().strftime("%H:%M:%S"),
            "ts":     time.time(),
            "coin":   coin,
            "type":   sig_type,
            "action": action,
            "reason": reason,
        }
        with self._sig_lock:
            self.signals.insert(0, entry)
            if len(self.signals) > MAX_SIGNALS:
                self.signals.pop()

    def _check_exit(self, coin: str, df: pd.DataFrame, price: float) -> Optional[str]:
        pos = self.portfolio.positions.get(coin)
        if not pos:
            return None
        d   = pos.direction
        cfg = self.cfg

        if pos.trailing_stop.triggered(price):
            return f"Trailing Stop ({pos.trailing_stop.stop:.4f})"

        v = df.dropna(subset=["ma_fast", "ma_slow"])
        if len(v) >= 3:
            p2, p = v.iloc[-3], v.iloc[-2]
            if d == "long"  and p2["ma_fast"] >= p2["ma_slow"] and p["ma_fast"] < p["ma_slow"]:
                return "Cruce MA inverso"
            if d == "short" and p2["ma_fast"] <= p2["ma_slow"] and p["ma_fast"] > p["ma_slow"]:
                return "Cruce MA inverso"

        rsi = float(df["rsi"].iloc[-2])
        if d == "long"  and rsi > cfg.rsi_ob:
            return f"RSI sobrecompra ({rsi:.0f})"
        if d == "short" and rsi < cfg.rsi_os:
            return f"RSI sobreventa ({rsi:.0f})"

        highs, lows = find_pivots(df)
        if d == "long"  and any(price < lvl * (1 - cfg.sr_break_pct) for lvl in lows):
            return "Ruptura soporte"
        if d == "short" and any(price > lvl * (1 + cfg.sr_break_pct) for lvl in highs):
            return "Ruptura resistencia"

        return None

    def run_cycle(self) -> None:
        self.last_scan = datetime.now().strftime("%H:%M:%S")
        self.status    = "escaneando"
        cfg = self.cfg

        for coin in self.coins:
            try:
                df = fetch_candles(coin, cfg.interval, cfg.candle_limit)
                if len(df) < cfg.ma_slow + 10:
                    continue

                df        = add_mas(df, cfg)
                df["rsi"] = calc_rsi(df)
                price     = float(df["close"].iloc[-1])

                # Gestión de posición existente
                if coin in self.portfolio.positions:
                    self.portfolio.update(coin, price)
                    reason = self._check_exit(coin, df, price)
                    if reason:
                        pnl = self.portfolio.close(coin, reason)
                        if pnl is not None:
                            self._log_signal(coin, "CIERRE",
                                             f"PnL {pnl:+.2f}$", reason)
                    continue

                # Búsqueda de entrada
                signal = detect_crossover(df)
                if not signal:
                    continue

                entry = float(df["close"].iloc[-2])
                highs, lows = find_pivots(df)
                ok_sr  = near_sr(entry, highs, lows, cfg.sr_near_pct)
                ok_fib = near_fibo(entry, calc_fibo(df), cfg.fibo_zone_pct)
                ok_vol = volume_ok(df, cfg.min_vol_ratio)

                if not (ok_sr and ok_fib and ok_vol):
                    why = []
                    if not ok_sr:  why.append("sin S/R")
                    if not ok_fib: why.append("sin Fib")
                    if not ok_vol: why.append("vol bajo")
                    self._log_signal(coin, signal.upper(), "DESCARTADO", ", ".join(why))
                    continue

                opened = self.portfolio.open(coin, signal, entry, cfg)
                if opened:
                    self._log_signal(coin, signal.upper(),
                                     f"ENTRADA @ {entry:,.4f}")

            except Exception as e:
                self.errors += 1
                print(f"[{cfg.label}] Error {coin}: {e}")

        self.status = "esperando"

    def run(self) -> None:
        while True:
            self.run_cycle()
            for _ in range(SIM_CYCLE_SEC):
                time.sleep(1)

    def to_dict(self) -> dict:
        with self._sig_lock:
            sigs = list(self.signals)
        return {
            "label":        self.cfg.label,
            "interval":     self.cfg.interval,
            "ma_type":      self.cfg.ma_type,
            "ma_fast":      self.cfg.ma_fast,
            "ma_slow":      self.cfg.ma_slow,
            "trailing_pct": self.cfg.trailing_pct,
            "last_scan":    self.last_scan,
            "status":       self.status,
            "errors":       self.errors,
            "coins":        self.coins,
            "portfolio":    self.portfolio.to_dict(),
            "signals":      sigs,
        }


# ─── ESTADO GLOBAL ────────────────────────────────────────────────────────────
_bots:       list[SimBot] = []
_started     = False
_start_lock  = threading.Lock()
_start_time  = datetime.now()


def get_state() -> dict:
    uptime = int((datetime.now() - _start_time).total_seconds())
    h, rem = divmod(uptime, 3600)
    m, s   = divmod(rem, 60)

    bots_data     = [b.to_dict() for b in _bots]
    total_equity  = sum(b["portfolio"]["equity"] for b in bots_data) if bots_data else 0.0
    initial_total = INITIAL_EQUITY * (len(_bots) if _bots else 3)
    total_pnl     = total_equity - initial_total
    total_pnl_pct = (total_pnl / initial_total * 100) if initial_total > 0 else 0.0

    return {
        "uptime":         f"{h}h {m:02d}m {s:02d}s",
        "total_equity":   round(total_equity, 2),
        "initial_equity": round(initial_total, 2),
        "total_pnl":      round(total_pnl, 2),
        "total_pnl_pct":  round(total_pnl_pct, 2),
        "bots":           bots_data,
        "updated_at":     datetime.now().strftime("%H:%M:%S"),
    }


def start() -> None:
    global _bots, _started, _start_time
    with _start_lock:
        if _started:
            return
        _started    = True
        _start_time = datetime.now()

    print("[sim_engine] Obteniendo top coins...")
    try:
        coins = get_top_coins()
    except Exception as e:
        print(f"[sim_engine] Error obteniendo coins: {e}. Usando fallback.")
        coins = FALLBACK_COINS[:]

    print(f"[sim_engine] Coins: {', '.join(coins)}")
    _bots = [SimBot(cfg, coins) for cfg in CONFIGS]

    for i, bot in enumerate(_bots):
        delay = i * 8

        def _run(b=bot, d=delay):
            if d:
                time.sleep(d)
            b.run()

        t = threading.Thread(target=_run, name=bot.cfg.label, daemon=True)
        t.start()
        print(f"[sim_engine] {bot.cfg.label} arrancado (delay {delay}s)")
