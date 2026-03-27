#!/usr/bin/env python3
"""
AlphaChainBots — Hyperliquid Trading Bot (1H · SMA 50/100)
Entrada : SMA 50 cruza SMA 100 + S/R ±0.8% + Fibonacci 0.5/0.618 + Volume Profile 1.3x
Salida  : dinámica — sin TP fijo. Cierra cuando:
            1) SMA rápida cruza contra la posición  (reversión de tendencia)
            2) Precio rompe S/R relevante en contra
            3) RSI > 75 (long) o RSI < 25 (short)
            4) Trailing stop 1.0% es tocado
Diferencias vs bot_1h.py: SMA en vez de EMA | MA50/100 en vez de MA20/50
                           S/R ±0.8% | Volume 1.3x
"""

import os
import time
import logging
from typing import Optional
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from eth_account import Account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants

# ─── LOGGING ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
load_dotenv()

TESTNET         = os.getenv("TESTNET", "true").lower() == "true"
PRIVATE_KEY     = os.getenv("PRIVATE_KEY", "")
ACCOUNT_ADDRESS = os.getenv("ACCOUNT_ADDRESS", "")

# Estrategia
INTERVAL        = "1h"
MA_FAST         = 50          # SMA rápida
MA_SLOW         = 100         # SMA lenta
RSI_PERIOD      = 14
RSI_OB          = 75          # RSI sobrecompra → salida long
RSI_OS          = 25          # RSI sobreventa  → salida short
CANDLE_LIMIT    = 300         # Más velas para cubrir SMA 100 + margen
TOP_N_COINS     = 10
LEVERAGE        = 3
RISK_PER_TRADE  = 0.02        # 2% equity por operación
TRAILING_PCT    = 0.010       # 1.0% trailing stop
MIN_VOL_RATIO   = 1.3         # Umbral de volumen más accesible (1.3x)
FIBO_ZONE_PCT   = 0.012       # ±1.2% zona fib
SR_LOOKBACK     = 60
SR_WINDOW       = 4
SR_NEAR_PCT     = 0.008       # ±0.8% para confirmar proximidad S/R
SR_BREAK_PCT    = 0.004       # 0.4% margen ruptura S/R
LOOP_SLEEP      = 60 * 60     # Ciclo cada 1 hora

FIBO_LEVELS     = [0.236, 0.382, 0.500, 0.618, 0.786]
FIBO_CONFIRM    = {0.500, 0.618}

INTERVAL_MS = {
    "1m":  60_000, "5m": 300_000, "15m": 900_000,
    "1h":  3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}


# ─── CONEXIÓN ─────────────────────────────────────────────────────────────────
def setup_client() -> tuple[Info, Exchange, str]:
    if not PRIVATE_KEY:
        raise ValueError("PRIVATE_KEY no configurada en .env")
    # Normalizar clave: quitar prefijo 0x y espacios
    key = PRIVATE_KEY.strip().removeprefix("0x").removeprefix("0X")
    account = Account.from_key(key)
    address = ACCOUNT_ADDRESS or account.address
    api_url = constants.TESTNET_API_URL if TESTNET else constants.MAINNET_API_URL
    info     = Info(api_url, skip_ws=True)
    exchange = Exchange(account, api_url, account_address=address)
    mode = "TESTNET ⚠️ " if TESTNET else "MAINNET 🔴"
    log.info(f"Conectado a Hyperliquid {mode} | Wallet: {address}")
    return info, exchange, address


# ─── DATOS DE MERCADO ─────────────────────────────────────────────────────────
FALLBACK_COINS = ["BTC","ETH","SOL","HYPE","TAO","XRP","DOGE","AVAX","BNB","LINK"]

def get_top_coins(info: Info, n: int = TOP_N_COINS) -> list[str]:
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
            log.info(f"Top {n} coins por volumen: {top}")
            return top
    except Exception as e:
        log.warning(f"meta_and_asset_ctxs() error: {e}. Usando fallback.")
    return FALLBACK_COINS[:n]


def fetch_candles(info: Info, coin: str, interval: str = INTERVAL, limit: int = CANDLE_LIMIT) -> pd.DataFrame:
    end_ms   = int(time.time() * 1000)
    ms       = INTERVAL_MS.get(interval, INTERVAL_MS["1h"])
    start_ms = end_ms - limit * ms
    raw = info.candles_snapshot(coin, interval, start_ms, end_ms)
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    df = df.rename(columns={"t": "time", "o": "open", "h": "high",
                             "l": "low",  "c": "close", "v": "volume"})
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    return df.sort_values("time").reset_index(drop=True)


# ─── INDICADORES ──────────────────────────────────────────────────────────────
def add_smas(df: pd.DataFrame) -> pd.DataFrame:
    """Medias móviles SIMPLES (SMA), no exponenciales."""
    df["ma_fast"] = df["close"].rolling(window=MA_FAST).mean()
    df["ma_slow"] = df["close"].rolling(window=MA_SLOW).mean()
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
    """
    Cruce de SMA para entradas — compara las dos últimas velas cerradas.
    Devuelve 'long', 'short' o None.
    """
    # Necesitamos al menos MA_SLOW + 3 velas válidas
    valid = df.dropna(subset=["ma_fast", "ma_slow"])
    if len(valid) < 3:
        return None

    prev2 = valid.iloc[-3]
    prev  = valid.iloc[-2]   # última vela cerrada

    bullish = prev2["ma_fast"] <= prev2["ma_slow"] and prev["ma_fast"] > prev["ma_slow"]
    bearish = prev2["ma_fast"] >= prev2["ma_slow"] and prev["ma_fast"] < prev["ma_slow"]

    if bullish:
        return "long"
    if bearish:
        return "short"
    return None


# ─── CONDICIONES DE SALIDA ────────────────────────────────────────────────────
def ma_reversal_exit(df: pd.DataFrame, direction: str) -> bool:
    """
    True si la SMA rápida cruza contra la dirección de la posición.
    LONG → cruce bajista (MA50 cae por debajo de MA100)
    SHORT → cruce alcista (MA50 sube por encima de MA100)
    """
    valid = df.dropna(subset=["ma_fast", "ma_slow"])
    if len(valid) < 3:
        return False
    prev2, prev = valid.iloc[-3], valid.iloc[-2]
    if direction == "long":
        return prev2["ma_fast"] >= prev2["ma_slow"] and prev["ma_fast"] < prev["ma_slow"]
    else:
        return prev2["ma_fast"] <= prev2["ma_slow"] and prev["ma_fast"] > prev["ma_slow"]


def sr_breakout_exit(price: float, direction: str,
                     highs: list[float], lows: list[float],
                     pct: float = SR_BREAK_PCT) -> bool:
    """
    LONG → cierre si precio cae pct% por debajo de un soporte.
    SHORT → cierre si precio sube pct% por encima de una resistencia.
    """
    if direction == "long":
        return any(price < lvl * (1 - pct) for lvl in lows)
    else:
        return any(price > lvl * (1 + pct) for lvl in highs)


def rsi_extreme_exit(rsi_val: float, direction: str) -> bool:
    if direction == "long"  and rsi_val > RSI_OB:
        return True
    if direction == "short" and rsi_val < RSI_OS:
        return True
    return False


def check_exit_conditions(
    df: pd.DataFrame,
    current_price: float,
    direction: str,
    ts: "TrailingStop",
) -> tuple[bool, str]:
    """
    Evalúa las 4 condiciones de salida en orden de prioridad.
    Devuelve (debe_cerrar, razón).
    """
    # 1) Trailing stop
    ts.update(current_price)
    if ts.triggered(current_price):
        return True, f"Trailing Stop tocado | precio={current_price:.2f} stop={ts.stop:.2f}"

    # 2) Cruce MA inverso
    if ma_reversal_exit(df, direction):
        return True, "Cruce SMA inverso — reversión de tendencia"

    # 3) RSI extremo
    rsi_val = float(df["rsi"].iloc[-2])
    if rsi_extreme_exit(rsi_val, direction):
        lbl = f"sobrecompra RSI={rsi_val:.1f} > {RSI_OB}" if direction == "long" \
              else f"sobreventa RSI={rsi_val:.1f} < {RSI_OS}"
        return True, f"RSI extremo — {lbl}"

    # 4) Ruptura de S/R en contra
    highs, lows = find_pivots(df)
    if sr_breakout_exit(current_price, direction, highs, lows):
        return True, f"Ruptura de S/R en contra | precio={current_price:.2f}"

    return False, ""


# ─── SOPORTE / RESISTENCIA ────────────────────────────────────────────────────
def find_pivots(df: pd.DataFrame, window: int = SR_WINDOW) -> tuple[list[float], list[float]]:
    data = df.tail(SR_LOOKBACK).reset_index(drop=True)
    highs, lows = [], []
    for i in range(window, len(data) - window):
        if data["high"].iloc[i] == data["high"].iloc[i - window: i + window + 1].max():
            highs.append(float(data["high"].iloc[i]))
        if data["low"].iloc[i] == data["low"].iloc[i - window: i + window + 1].min():
            lows.append(float(data["low"].iloc[i]))
    return highs, lows


def near_sr(price: float, highs: list[float], lows: list[float],
            pct: float = SR_NEAR_PCT) -> bool:
    """±0.8% para confirmar proximidad a S/R (más estricto que los otros bots)."""
    return any(abs(price - lvl) / lvl <= pct for lvl in highs + lows)


# ─── FIBONACCI ────────────────────────────────────────────────────────────────
def calc_fibonacci(df: pd.DataFrame) -> dict[float, float]:
    recent = df.tail(SR_LOOKBACK)
    sh, sl = float(recent["high"].max()), float(recent["low"].min())
    rng    = sh - sl
    return {lvl: sh - lvl * rng for lvl in FIBO_LEVELS}


def near_fibo_confirm(price: float, fib: dict[float, float]) -> bool:
    for lvl, lvl_price in fib.items():
        if lvl in FIBO_CONFIRM and abs(price - lvl_price) / lvl_price <= FIBO_ZONE_PCT:
            log.info(f"  ✓ Fib {lvl:.3f} = {lvl_price:.2f}")
            return True
    return False


# ─── VOLUME PROFILE ───────────────────────────────────────────────────────────
def volume_confirms(df: pd.DataFrame, lookback: int = 20) -> bool:
    """Volumen de la última vela cerrada ≥ MIN_VOL_RATIO × media (1.3x)."""
    if len(df) < lookback + 2:
        return False
    avg_vol  = df["volume"].iloc[-(lookback + 1):-1].mean()
    last_vol = df["volume"].iloc[-2]
    ratio    = last_vol / avg_vol if avg_vol > 0 else 0
    log.info(f"  Volume ratio: {ratio:.2f}x (mínimo {MIN_VOL_RATIO}x)")
    return ratio >= MIN_VOL_RATIO


# ─── TAMAÑO DE POSICIÓN ───────────────────────────────────────────────────────
def calc_size(equity: float, price: float) -> float:
    notional = equity * RISK_PER_TRADE * LEVERAGE
    return round(notional / price, 4)


# ─── TRAILING STOP ────────────────────────────────────────────────────────────
class TrailingStop:
    def __init__(self, entry: float, direction: str, pct: float = TRAILING_PCT):
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

    def __str__(self) -> str:
        return (f"TrailingStop({self.direction} | best={self.best:.2f} "
                f"stop={self.stop:.2f} dist={self.pct*100:.1f}%)")


# ─── ÓRDENES ──────────────────────────────────────────────────────────────────
def set_leverage(exchange: Exchange, coin: str) -> None:
    try:
        exchange.update_leverage(LEVERAGE, coin, is_cross=False)
    except Exception as e:
        log.warning(f"  No se pudo fijar leverage para {coin}: {e}")


def open_order(exchange: Exchange, coin: str, direction: str, size: float, price: float) -> dict:
    is_buy   = direction == "long"
    slip     = 0.0015
    limit_px = round(price * (1 + slip) if is_buy else price * (1 - slip), 2)
    set_leverage(exchange, coin)
    result = exchange.order(coin, is_buy, size, limit_px,
                            {"limit": {"tif": "Ioc"}}, reduce_only=False)
    log.info(f"  ABRIR {direction.upper()} {coin} | size={size} | px={limit_px} | {result}")
    return result


def close_order(exchange: Exchange, coin: str, direction: str, size: float, price: float) -> dict:
    is_buy   = direction == "short"
    slip     = 0.002
    limit_px = round(price * (1 + slip) if is_buy else price * (1 - slip), 2)
    result = exchange.order(coin, is_buy, abs(size), limit_px,
                            {"limit": {"tif": "Ioc"}}, reduce_only=True)
    log.info(f"  CERRAR {direction.upper()} {coin} | size={abs(size)} | px={limit_px} | {result}")
    return result


# ─── ESTADO DE CUENTA ─────────────────────────────────────────────────────────
def get_equity(info: Info, address: str) -> float:
    state = info.user_state(address)
    return float(state.get("marginSummary", {}).get("accountValue", 0))


def get_open_position(info: Info, address: str, coin: str) -> Optional[dict]:
    state = info.user_state(address)
    for pos in state.get("assetPositions", []):
        p = pos.get("position", {})
        if p.get("coin") == coin and float(p.get("szi", 0)) != 0:
            return p
    return None


# ─── BOT PRINCIPAL ────────────────────────────────────────────────────────────
class Bot:
    def __init__(self):
        self.info, self.exchange, self.address = setup_client()
        self.coins: list[str] = []
        # coin → {direction, size, entry, trailing_stop}
        self.positions: dict[str, dict] = {}

    # ── gestión de posiciones abiertas ───────────────────────────────────────
    def manage_positions(self) -> None:
        for coin in list(self.positions.keys()):
            try:
                df = fetch_candles(self.info, coin)
                if df.empty or len(df) < MA_SLOW + 3:
                    continue

                df              = add_smas(df)
                df["rsi"]       = calc_rsi(df)
                current_price   = float(df["close"].iloc[-1])
                pos_data        = self.positions[coin]
                ts: TrailingStop = pos_data["trailing_stop"]
                entry           = pos_data["entry"]
                direction       = pos_data["direction"]
                pnl_pct         = ((current_price - entry) / entry * 100
                                   if direction == "long"
                                   else (entry - current_price) / entry * 100)

                log.info(
                    f"[{coin}] {direction.upper()} | entrada={entry:.2f} "
                    f"actual={current_price:.2f} | PnL={pnl_pct:+.2f}% | {ts}"
                )

                should_close, reason = check_exit_conditions(
                    df, current_price, direction, ts
                )

                if should_close:
                    pos = get_open_position(self.info, self.address, coin)
                    if pos:
                        close_order(self.exchange, coin, direction,
                                    float(pos["szi"]), current_price)
                    log.info(f"[{coin}] 🔴 CIERRE — {reason}")
                    del self.positions[coin]

            except Exception as e:
                log.error(f"[{coin}] Error gestionando posición: {e}")

    # ── búsqueda de nuevas entradas ──────────────────────────────────────────
    def scan_entries(self) -> None:
        equity = get_equity(self.info, self.address)
        if equity <= 0:
            log.warning("Equity = 0. Verifica que la testnet está financiada.")
            return
        log.info(f"Equity disponible: ${equity:.2f}")

        for coin in self.coins:
            if coin in self.positions:
                continue
            try:
                df = fetch_candles(self.info, coin)
                if len(df) < MA_SLOW + 10:
                    log.warning(f"[{coin}] Pocas velas ({len(df)}) — saltando")
                    continue

                df     = add_smas(df)
                signal = detect_crossover(df)
                if not signal:
                    continue

                price = float(df["close"].iloc[-2])
                log.info(f"\n[{coin}] ─── Señal SMA: {signal.upper()} @ {price:.2f} ───")

                # Confirmación 1: S/R ±0.8%
                highs, lows = find_pivots(df)
                if not near_sr(price, highs, lows):
                    log.info(f"[{coin}] ✗ No cerca de S/R (±{SR_NEAR_PCT*100:.1f}%) → descartado")
                    continue
                log.info(f"[{coin}] ✓ S/R confirmado")

                # Confirmación 2: Fibonacci 0.5 / 0.618
                fib = calc_fibonacci(df)
                if not near_fibo_confirm(price, fib):
                    log.info(f"[{coin}] ✗ Fib 0.5/0.618 no coincide → descartado")
                    continue
                log.info(f"[{coin}] ✓ Fibonacci confirmado")

                # Confirmación 3: Volumen ≥ 1.3x
                if not volume_confirms(df):
                    log.info(f"[{coin}] ✗ Volumen insuficiente → descartado")
                    continue
                log.info(f"[{coin}] ✓ Volumen confirmado")

                # Abrir posición
                size = calc_size(equity, price)
                if size <= 0:
                    log.warning(f"[{coin}] Tamaño = 0 → saltando")
                    continue

                log.info(f"[{coin}] 🟢 ENTRADA {signal.upper()} | size={size} | px={price:.2f}")
                open_order(self.exchange, coin, signal, size, price)
                self.positions[coin] = {
                    "direction":     signal,
                    "size":          size,
                    "entry":         price,
                    "trailing_stop": TrailingStop(price, signal),
                }

            except Exception as e:
                log.error(f"[{coin}] Error en escaneo: {e}")

    # ── loop principal ────────────────────────────────────────────────────────
    def run(self) -> None:
        log.info("=" * 60)
        log.info(f" AlphaChainBots 1H·SMA — {'TESTNET' if TESTNET else 'MAINNET'}")
        log.info(f" SMA {MA_FAST}/{MA_SLOW} | Trailing {TRAILING_PCT*100:.1f}%"
                 f" | RSI {RSI_OS}/{RSI_OB} | Leverage {LEVERAGE}x")
        log.info(f" S/R ±{SR_NEAR_PCT*100:.1f}% | Volume ≥{MIN_VOL_RATIO}x | Fib 0.5/0.618")
        log.info(" Salida: SMA reversal | S/R breakout | RSI extremo | Trailing stop")
        log.info("=" * 60)

        self.coins = get_top_coins(self.info)

        while True:
            try:
                log.info(f"\n{'═'*50}")
                log.info(f" Ciclo 1H·SMA: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                log.info(f"{'═'*50}")
                self.manage_positions()
                self.scan_entries()
                abiertas = list(self.positions.keys())
                log.info(f"\nPosiciones: {abiertas if abiertas else 'ninguna'}")
                log.info(f"Próximo ciclo en 1h...\n")
                time.sleep(LOOP_SLEEP)
            except KeyboardInterrupt:
                log.info("Bot detenido por el usuario.")
                break
            except Exception as e:
                log.error(f"Error inesperado: {e}")
                time.sleep(60)


# ─── ENTRY POINT ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    Bot().run()
