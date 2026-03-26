#!/usr/bin/env python3
"""
AlphaChainBots — run_simulation.py
Simulación completa sin órdenes reales.
• Precios reales via API pública Hyperliquid mainnet (sin autenticación)
• Saldo virtual: $10.000 USDC por bot ($30.000 total)
• Los 3 bots corren en paralelo con sus estrategias reales
• Dashboard en terminal actualizado cada 15 segundos
• Ctrl+C para detener
"""

import os
import sys
import io
import time
import threading
from collections import deque

# ─── ENCODING / ANSI (Windows) ────────────────────────────────────────────────
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

if sys.platform == "win32":
    os.system("")   # activa VT100 / ANSI en la consola de Windows
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd
from hyperliquid.info import Info
from hyperliquid.utils import constants

# ─── ANSI ─────────────────────────────────────────────────────────────────────
RST  = "\033[0m"
BOLD = "\033[1m"
DIM  = "\033[2m"
GRN  = "\033[92m"
RED  = "\033[91m"
YLW  = "\033[93m"
CYN  = "\033[96m"
MGT  = "\033[95m"
WHT  = "\033[97m"
DGR  = "\033[37m"

BOT_COLORS = {
    "BOT·4H":     CYN,
    "BOT·1H·EMA": YLW,
    "BOT·1H·SMA": GRN,
}

def pnl_col(v: float) -> str:
    return GRN if v > 0 else (RED if v < 0 else DGR)


# ─── CONFIGURACIÓN POR ESTRATEGIA ─────────────────────────────────────────────
@dataclass
class BotConfig:
    name:          str
    label:         str
    interval:      str
    ma_type:       str      # 'ema' | 'sma'
    ma_fast:       int
    ma_slow:       int
    trailing_pct:  float
    min_vol_ratio: float
    sr_near_pct:   float
    fibo_zone_pct: float
    candle_limit:  int
    rsi_ob:        float = 75.0
    rsi_os:        float = 25.0
    sr_break_pct:  float = 0.004
    leverage:      float = 3.0
    risk_per_trade:float = 0.02

CONFIGS: list[BotConfig] = [
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

# ─── CONSTANTES GLOBALES ──────────────────────────────────────────────────────
INITIAL_EQUITY  = 10_000.0
TOP_N_COINS     = 10
RSI_PERIOD      = 14
SR_LOOKBACK     = 60
SR_WINDOW       = 4
FIBO_LEVELS     = [0.236, 0.382, 0.500, 0.618, 0.786]
FIBO_CONFIRM    = {0.500, 0.618}
SIM_CYCLE_SEC   = 300       # cada bot re-escanea cada 5 minutos en simulación
DASHBOARD_SEC   = 15        # dashboard se refresca cada 15 segundos

INTERVAL_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000,
    "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}

# Estado compartido entre hilos
signal_log  = deque(maxlen=25)
_log_lock   = threading.Lock()
_dash_lock  = threading.Lock()
stop_event  = threading.Event()


# ─── API PÚBLICA (MAINNET, SIN AUTH) ─────────────────────────────────────────
_info = Info(constants.MAINNET_API_URL, skip_ws=True)

def get_top_coins(n: int = TOP_N_COINS) -> list[str]:
    meta, ctxs = _info.meta_and_asset_ctxs()
    data = []
    for i, asset in enumerate(meta["universe"]):
        if i < len(ctxs):
            data.append({"coin": asset["name"],
                         "vol": float(ctxs[i].get("dayNtlVlm", 0))})
    data.sort(key=lambda x: x["vol"], reverse=True)
    return [d["coin"] for d in data[:n]]

def fetch_candles(coin: str, interval: str, limit: int) -> pd.DataFrame:
    end_ms   = int(time.time() * 1000)
    ms       = INTERVAL_MS.get(interval, INTERVAL_MS["1h"])
    start_ms = end_ms - limit * ms
    raw = _info.candles_snapshot(coin, interval, start_ms, end_ms)
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

def find_pivots(df: pd.DataFrame) -> tuple[list, list]:
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
    r = df.tail(SR_LOOKBACK)
    sh, sl = float(r["high"].max()), float(r["low"].min())
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
    close_reason:  str = ""

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


# ─── PORTFOLIO VIRTUAL ────────────────────────────────────────────────────────
class VirtualPortfolio:
    def __init__(self):
        self.cash        = INITIAL_EQUITY
        self.positions:  dict[str, VirtualPos] = {}
        self.closed_pnl  = 0.0
        self.trades      = 0
        self.wins        = 0
        self._lock       = threading.Lock()

    @property
    def equity(self) -> float:
        return self.cash + sum(p.pnl for p in self.positions.values())

    @property
    def total_pnl(self) -> float:
        return self.equity - INITIAL_EQUITY

    @property
    def total_pnl_pct(self) -> float:
        return self.total_pnl / INITIAL_EQUITY * 100

    @property
    def winrate(self) -> str:
        return f"{self.wins}/{self.trades}" if self.trades else "—"

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
            return pnl


# ─── BOT SIMULADO ─────────────────────────────────────────────────────────────
class SimBot:
    def __init__(self, cfg: BotConfig, coins: list[str]):
        self.cfg          = cfg
        self.coins        = coins
        self.portfolio    = VirtualPortfolio()
        self.last_scan    = "—"
        self.cycle_events: list[str] = []
        self.errors       = 0

    def _log(self, msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        with _log_lock:
            signal_log.appendleft(f"{DIM}{ts}{RST} {BOT_COLORS[self.cfg.label]}{BOLD}[{self.cfg.label}]{RST} {msg}")

    def _check_exit(self, coin: str, df: pd.DataFrame, price: float) -> Optional[str]:
        pos = self.portfolio.positions.get(coin)
        if not pos:
            return None
        d   = pos.direction
        cfg = self.cfg

        if pos.trailing_stop.triggered(price):
            return f"Trailing Stop (stop={pos.trailing_stop.stop:.2f})"

        v = df.dropna(subset=["ma_fast", "ma_slow"])
        if len(v) >= 3:
            p2, p = v.iloc[-3], v.iloc[-2]
            if d == "long"  and p2["ma_fast"] >= p2["ma_slow"] and p["ma_fast"] < p["ma_slow"]:
                return "Cruce MA inverso ↓"
            if d == "short" and p2["ma_fast"] <= p2["ma_slow"] and p["ma_fast"] > p["ma_slow"]:
                return "Cruce MA inverso ↑"

        rsi = float(df["rsi"].iloc[-2])
        if d == "long"  and rsi > cfg.rsi_ob:
            return f"RSI sobrecompra {rsi:.0f}"
        if d == "short" and rsi < cfg.rsi_os:
            return f"RSI sobreventa {rsi:.0f}"

        highs, lows = find_pivots(df)
        if d == "long"  and any(price < lvl * (1 - cfg.sr_break_pct) for lvl in lows):
            return "Ruptura soporte"
        if d == "short" and any(price > lvl * (1 + cfg.sr_break_pct) for lvl in highs):
            return "Ruptura resistencia"

        return None

    def run_cycle(self) -> None:
        self.last_scan    = datetime.now().strftime("%H:%M:%S")
        self.cycle_events = []
        cfg = self.cfg

        for coin in self.coins:
            try:
                df = fetch_candles(coin, cfg.interval, cfg.candle_limit)
                if len(df) < cfg.ma_slow + 10:
                    continue

                df        = add_mas(df, cfg)
                df["rsi"] = calc_rsi(df)
                price     = float(df["close"].iloc[-1])

                # — Gestión de posición existente —
                if coin in self.portfolio.positions:
                    self.portfolio.update(coin, price)
                    reason = self._check_exit(coin, df, price)
                    if reason:
                        pnl = self.portfolio.close(coin, reason)
                        if pnl is not None:
                            ic = GRN + "+" if pnl >= 0 else RED
                            ev = f"{GRN if pnl>=0 else RED}✖ CIERRE {coin}{RST} PnL={ic}{pnl:+.2f}${RST} | {reason}"
                            self.cycle_events.append(ev)
                            self._log(f"✖ CIERRE {coin} PnL={pnl:+.2f}$ | {reason}")
                    continue

                # — Búsqueda de entrada —
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
                    ev = f"{DGR}⚡ {coin} {signal.upper()} → {', '.join(why)}{RST}"
                    self.cycle_events.append(ev)
                    continue

                opened = self.portfolio.open(coin, signal, entry, cfg)
                if opened:
                    ic  = GRN if signal == "long" else RED
                    arr = "▲" if signal == "long" else "▼"
                    ev  = f"{ic}{arr} ENTRADA {signal.upper()} {coin}{RST} @ {entry:,.2f}"
                    self.cycle_events.append(ev)
                    self._log(f"{arr} ENTRADA {signal.upper()} {coin} @ {entry:,.2f}")

            except Exception:
                self.errors += 1

    def run(self) -> None:
        # Escalonar el primer escaneo para no saturar la API
        while not stop_event.is_set():
            self.run_cycle()
            for _ in range(SIM_CYCLE_SEC):
                if stop_event.is_set():
                    break
                time.sleep(1)


# ─── DASHBOARD ────────────────────────────────────────────────────────────────
import re as _re

WIDE = 66
DIV  = "-" * WIDE

def _strip_ansi(s: str) -> str:
    return _re.sub(r'\033\[[0-9;]*m', '', s)

def render(bots: list[SimBot]) -> None:
    now          = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    total_equity = sum(b.portfolio.equity for b in bots)
    total_pnl    = total_equity - INITIAL_EQUITY * len(bots)
    total_pct    = total_pnl / (INITIAL_EQUITY * len(bots)) * 100
    tc           = pnl_col(total_pnl)

    out = []
    out.append(f"\n{'=' * WIDE}")
    out.append(f"  AlphaChainBots -- SIMULACION  |  {now}")
    out.append(f"{'=' * WIDE}")
    out.append(
        f"  Equity total : ${total_equity:>10,.2f}   "
        f"PnL: {tc}{total_pnl:>+9,.2f}$ ({total_pct:>+.2f}%){RST}"
    )
    out.append(
        f"  Saldo inicial: ${INITIAL_EQUITY * len(bots):,.2f} "
        f"({len(bots)} bots x ${INITIAL_EQUITY:,.0f})"
    )
    out.append(DIV)

    for bot in bots:
        p   = bot.portfolio
        col = BOT_COLORS[bot.cfg.label]
        ma  = f"{'EMA' if bot.cfg.ma_type == 'ema' else 'SMA'} {bot.cfg.ma_fast}/{bot.cfg.ma_slow}"
        pc  = pnl_col(p.total_pnl)

        out.append("")
        out.append(
            f"  {col}{bot.cfg.label:<14}{RST}"
            f"  {ma}  {bot.cfg.interval}  trailing={bot.cfg.trailing_pct * 100:.1f}%"
            f"  escaneo: {bot.last_scan}"
        )
        out.append(
            f"  Equity: ${p.equity:>10,.2f}  "
            f"PnL: {pc}{p.total_pnl:>+9,.2f}$ ({p.total_pnl_pct:>+.2f}%){RST}  "
            f"Trades: {p.winrate}"
        )

        if p.positions:
            out.append(
                f"  {'COIN':<6} {'DIR':<5} {'ENTRADA':>10} {'ACTUAL':>10}"
                f" {'PnL $':>9} {'PnL%':>7} {'STOP':>10} {'TIEMPO':>7}"
            )
            out.append(f"  {DIV[:62]}")
            for coin, pos in p.positions.items():
                pc2 = pnl_col(pos.pnl)
                arr = "LONG " if pos.direction == "long" else "SHORT"
                out.append(
                    f"  {coin:<6} {col}{arr}{RST}"
                    f" {pos.entry_price:>10,.2f} {pos.current_price:>10,.2f}"
                    f" {pc2}{pos.pnl:>+9,.2f} {pos.pnl_pct:>+7.2f}%{RST}"
                    f" {pos.trailing_stop.stop:>10,.2f} {pos.duration:>7}"
                )
        else:
            out.append("  Sin posiciones abiertas.")

        for ev in bot.cycle_events[-3:]:
            out.append(f"    > {_strip_ansi(ev)}")

    out.append("")
    out.append(f"  SENALES RECIENTES")
    out.append(DIV)
    with _log_lock:
        recent = list(signal_log)[:8]
    if recent:
        for e in recent:
            out.append(f"  {_strip_ansi(e)}")
    else:
        out.append("  Esperando senales... (primer escaneo en curso)")

    out.append(f"{'=' * WIDE}")
    out.append(
        f"  Actualizacion cada {DASHBOARD_SEC}s | "
        f"Escaneo cada {SIM_CYCLE_SEC // 60}min | Ctrl+C para detener"
    )

    with _dash_lock:
        print("\n".join(out))
        sys.stdout.flush()


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main() -> None:
    print(f"\n{'=' * WIDE}")
    print(f"  AlphaChainBots -- Iniciando simulacion...")
    print(f"{'=' * WIDE}\n")
    print("  Conectando a Hyperliquid mainnet (solo lectura)...")

    try:
        coins = get_top_coins()
    except Exception as e:
        print(f"  {RED}Error obteniendo coins: {e}{RST}")
        sys.exit(1)

    print(f"  Top {len(coins)} coins: {', '.join(coins)}\n")
    print(f"  Iniciando {len(CONFIGS)} bots simulados en paralelo...")
    print(f"  Saldo virtual por bot: ${INITIAL_EQUITY:,.0f} USDC\n")

    bots    = [SimBot(cfg, coins) for cfg in CONFIGS]
    threads = []

    for i, bot in enumerate(bots):
        col = BOT_COLORS[bot.cfg.label]
        t   = threading.Thread(target=bot.run, name=bot.cfg.label, daemon=True)
        # Escalonar arranques: 0s, 8s, 16s para no saturar la API
        t._start_delay = i * 8
        threads.append((t, bot))

    for t, bot in threads:
        def _run(b=bot, delay=t._start_delay):
            time.sleep(delay)
            b.run()
        real_t = threading.Thread(target=_run, name=bot.cfg.label, daemon=True)
        real_t.start()
        delay_s = f"{threads.index((t, bot)) * 8}s"
        print(f"  {BOT_COLORS[bot.cfg.label]}[{bot.cfg.label}]{RST} arrancado (primer escaneo en {delay_s})")

    print(f"\n  Dashboard arranca en 10 segundos...\n")
    time.sleep(10)

    try:
        while True:
            render(bots)
            time.sleep(DASHBOARD_SEC)
    except KeyboardInterrupt:
        stop_event.set()
        print(f"\nSimulacion detenida.\n")

        # Resumen final
        print(f"{'=' * WIDE}")
        print(f"  RESUMEN FINAL")
        print(f"{'=' * WIDE}")
        for bot in bots:
            p   = bot.portfolio
            col = BOT_COLORS[bot.cfg.label]
            pc  = pnl_col(p.total_pnl)
            print(f"  {col}[{bot.cfg.label}]{RST}  "
                  f"Equity: ${p.equity:,.2f}  "
                  f"PnL: {pc}{p.total_pnl:+,.2f}$ ({p.total_pnl_pct:+.2f}%){RST}  "
                  f"Trades: {p.winrate}")
        total_eq  = sum(b.portfolio.equity for b in bots)
        total_pnl = total_eq - INITIAL_EQUITY * len(bots)
        tc = pnl_col(total_pnl)
        print(f"{DIV}")
        print(f"  TOTAL  Equity: ${total_eq:,.2f}  "
              f"PnL: {tc}{total_pnl:+,.2f}${RST}")
        print(f"{'=' * WIDE}\n")


if __name__ == "__main__":
    main()
