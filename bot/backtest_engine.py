#!/usr/bin/env python3
"""
backtest_engine.py — Motor de backtest histórico con datos reales de Hyperliquid.
Exporta: run_backtest_bg(period), get_result(period), get_progress(period)
"""

import math
import time
import threading
import logging
import requests
from typing import Optional

log = logging.getLogger(__name__)

HL_URL = "https://api.hyperliquid.xyz/info"

INITIAL_EQUITY = 10_000.0
RISK_PCT       = 0.02
BT_LEVERAGE    = 3.0
LIQ_LEVERAGE   = 5.0

PERIODS = {"3m": 90, "6m": 180, "1y": 365, "max": 900}

INTERVAL_MS = {
    "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
}

# 15m/30m → map to 1h (too many candles for long periods)
# 1h/2h/4h → use native interval so MA periods match the optimizer exactly
BT_INTERVAL_MAP = {
    "15m": "1h", "30m": "1h",
    "1h":  "1h", "2h":  "2h", "4h":  "4h",
}

# ── CANDLE FETCH ───────────────────────────────────────────────────────────────

def _fetch_candles(coin: str, interval: str, days: int) -> list:
    """Fetch historical OHLCV candles from Hyperliquid. Returns list of dicts."""
    try:
        end_ms   = int(time.time() * 1000)
        start_ms = end_ms - int(days * 86_400_000)
        r = requests.post(HL_URL, json={
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": interval,
                    "startTime": start_ms, "endTime": end_ms}
        }, timeout=30)
        raw = r.json()
        if not isinstance(raw, list):
            return []
        out = []
        for c in raw:
            try:
                out.append({
                    "t": int(c.get("t", 0)),
                    "o": float(c.get("o", 0) or 0),
                    "h": float(c.get("h", 0) or 0),
                    "l": float(c.get("l", 0) or 0),
                    "c": float(c.get("c", 0) or 0),
                    "v": float(c.get("v", 0) or 0),
                })
            except (TypeError, ValueError):
                pass
        return sorted(out, key=lambda x: x["t"])
    except Exception as e:
        log.warning(f"_fetch_candles {coin}/{interval}/{days}d: {e}")
        return []


# ── INDICATORS ─────────────────────────────────────────────────────────────────

def _ema(closes: list, period: int) -> list:
    if len(closes) < period:
        return []
    k = 2.0 / (period + 1)
    v = sum(closes[:period]) / period
    result = [v]
    for c in closes[period:]:
        v = c * k + v * (1 - k)
        result.append(v)
    return result


def _sma(closes: list, period: int) -> list:
    if len(closes) < period:
        return []
    return [sum(closes[i:i + period]) / period
            for i in range(len(closes) - period + 1)]


def _rsi(closes: list, period: int = 14) -> list:
    if len(closes) < period + 1:
        return []
    diffs  = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in diffs]
    losses = [max(-d, 0.0) for d in diffs]
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    result = []
    for i in range(period, len(diffs)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        rs = ag / al if al > 0 else 100.0
        result.append(100 - 100 / (1 + rs))
    return result


# ── METRICS ────────────────────────────────────────────────────────────────────

def _compute_metrics(equity_snaps: list, trades: list,
                     initial: float, days: int) -> dict:
    """equity_snaps = [[ts_ms, equity], ...]"""
    if not equity_snaps:
        ts = int(time.time() * 1000)
        equity_snaps = [[ts - days * 86_400_000, initial], [ts, initial]]

    equities  = [e for _, e in equity_snaps]
    final     = equities[-1]
    total_pnl = final - initial
    pnl_pct   = (final / initial - 1) * 100 if initial else 0

    # Max drawdown
    peak = initial; max_dd = 0.0
    for eq in equities:
        if eq > peak: peak = eq
        dd = (peak - eq) / peak * 100 if peak > 0 else 0
        if dd > max_dd: max_dd = dd

    pnls = [t["pnl"] for t in trades]
    wins  = [p for p in pnls if p > 0]
    loses = [p for p in pnls if p <= 0]
    n     = len(pnls)
    win_rate      = len(wins) / n * 100 if n else 0
    best_trade    = max(pnls) if pnls else 0
    worst_trade   = min(pnls) if pnls else 0
    profit_factor = (sum(wins) / abs(sum(loses))
                     if loses and sum(wins) > 0 else 0.0)

    # Simplified Sharpe on equity curve returns
    if len(equities) >= 3:
        rets = [(equities[i] - equities[i-1]) / equities[i-1]
                for i in range(1, len(equities)) if equities[i-1] > 0]
        if rets:
            mu  = sum(rets) / len(rets)
            sig = math.sqrt(sum((r - mu)**2 for r in rets) / len(rets)) if len(rets) > 1 else 0
            sharpe = (mu / sig * math.sqrt(len(rets))) if sig > 0 else 0.0
        else:
            sharpe = 0.0
    else:
        sharpe = 0.0

    # Downsample equity curve to ≤120 points for frontend
    curve = _downsample(equity_snaps, 120)

    return {
        "total_pnl":     round(total_pnl, 2),
        "total_pnl_pct": round(pnl_pct, 2),
        "win_rate":      round(win_rate, 1),
        "total_trades":  n,
        "max_drawdown":  round(max_dd, 2),
        "best_trade":    round(best_trade, 2),
        "worst_trade":   round(worst_trade, 2),
        "sharpe":        round(sharpe, 2),
        "profit_factor": round(profit_factor, 2),
        "equity_curve":  curve,
        "final_equity":  round(final, 2),
    }


def _downsample(snaps: list, target: int) -> list:
    if len(snaps) <= target:
        return snaps
    step = max(1, len(snaps) // target)
    result = snaps[::step]
    if snaps[-1] not in result:
        result = result + [snaps[-1]]
    return result


# ── EMA / SMA CROSSOVER BACKTEST ───────────────────────────────────────────────

def _bt_crossover(cfg, candles_cache: dict, days: int,
                  coins_override: list = None) -> dict:
    """
    Backtest for EMA/SMA crossover bots.
    coins_override: if provided, use these coins instead of cfg.coins (from dashboard selection).
    Each coin gets full INITIAL_EQUITY — same model as the optimizer.
    """
    bt_interval = BT_INTERVAL_MAP.get(cfg.interval, "1h")
    leverage    = getattr(cfg, "leverage", BT_LEVERAGE)
    risk_pct    = getattr(cfg, "risk_per_trade", RISK_PCT)
    trailing    = cfg.trailing_pct
    sl_type     = getattr(cfg, "sl_type", "trailing")
    fixed_sl    = getattr(cfg, "fixed_sl_pct", 0.02)
    time_filter = getattr(cfg, "time_filter", "none")
    rsi_filter  = getattr(cfg, "rsi_filter", "none")

    from sim_engine import FALLBACK_COINS

    # Priority: explicit override (from dashboard coin selection) > cfg.coins > all top coins
    if coins_override:
        bt_coins = coins_override
    elif getattr(cfg, "coins", None):
        bt_coins = cfg.coins
    else:
        bt_coins = FALLBACK_COINS[:10]
    coin_equity = INITIAL_EQUITY  # full $10K per coin — matches optimizer model

    all_events: list = []
    all_trades: list = []
    coin_pnls:  dict = {}  # {coin: total_pnl_usd} for debug breakdown

    for coin in bt_coins:
        key = f"{coin}_{bt_interval}"
        df  = candles_cache.get(key, [])
        if len(df) < cfg.ma_slow + 10:
            continue

        closes = [c["c"] for c in df]
        tss    = [c["t"] for c in df]

        if cfg.ma_type == "ema":
            fast_s = _ema(closes, cfg.ma_fast)
            slow_s = _ema(closes, cfg.ma_slow)
        else:
            fast_s = _sma(closes, cfg.ma_fast)
            slow_s = _sma(closes, cfg.ma_slow)

        rsi_s = _rsi(closes, 14) if rsi_filter != "none" else []

        off_f = len(closes) - len(fast_s)
        off_s = len(closes) - len(slow_s)
        off_r = len(closes) - len(rsi_s) if rsi_s else 0
        start = max(off_f, off_s) + 1

        in_pos    = False
        dir_      = ""
        entry     = 0.0
        best      = 0.0
        stop      = 0.0
        eq_c      = coin_equity
        coin_pnl  = 0.0  # accumulate this coin's PnL for the breakdown log

        for i in range(start, len(df)):
            fi = i - off_f
            si = i - off_s
            if fi < 1 or si < 1:
                continue

            price = closes[i]
            ts    = tss[i]
            fc    = fast_s[fi];  fp = fast_s[fi - 1]
            sc    = slow_s[si];  sp = slow_s[si - 1]

            if in_pos:
                exited = False
                pnl    = 0.0

                if sl_type == "fixed":
                    sl_px = entry * (1 - fixed_sl) if dir_ == "long" else entry * (1 + fixed_sl)
                    if dir_ == "long" and price <= sl_px:
                        pnl = eq_c * risk_pct * (price - entry) / entry * leverage
                        exited = True
                    elif dir_ == "short" and price >= sl_px:
                        pnl = eq_c * risk_pct * (entry - price) / entry * leverage
                        exited = True
                else:  # trailing or atr — both use trailing stop logic here
                    if dir_ == "long":
                        if price > best:
                            best = price; stop = best * (1 - trailing)
                        if price <= stop:
                            pnl = eq_c * risk_pct * (price - entry) / entry * leverage
                            exited = True
                    else:
                        if price < best:
                            best = price; stop = best * (1 + trailing)
                        if price >= stop:
                            pnl = eq_c * risk_pct * (entry - price) / entry * leverage
                            exited = True

                # MA reverse crossover exit (applies regardless of sl_type)
                if not exited and fi >= 1 and si >= 1:
                    if dir_ == "long" and fp >= sp and fc < sc:
                        pnl = eq_c * risk_pct * (price - entry) / entry * leverage
                        exited = True
                    elif dir_ == "short" and fp <= sp and fc > sc:
                        pnl = eq_c * risk_pct * (entry - price) / entry * leverage
                        exited = True

                if exited:
                    eq_c     += pnl
                    coin_pnl += pnl
                    all_events.append((ts, pnl))
                    all_trades.append({"pnl": round(pnl, 2)})
                    in_pos = False

            if not in_pos:
                up   = fp <= sp and fc > sc
                down = fp >= sp and fc < sc
                if not (up or down):
                    continue

                # Time filter — skip new entries outside session hours
                if time_filter == "asia":
                    if (ts // 3_600_000) % 24 >= 9:
                        continue
                elif time_filter == "london_ny":
                    if (ts // 3_600_000) % 24 not in range(7, 22):
                        continue

                # RSI filter
                if rsi_filter != "none" and rsi_s:
                    ri = i - off_r
                    if 0 <= ri < len(rsi_s):
                        rv = rsi_s[ri]
                        if rsi_filter == "rsi50":
                            if up and rv <= 50: continue
                            if down and rv >= 50: continue
                        elif rsi_filter == "rsi55":
                            if up and rv <= 55: continue
                            if down and rv >= 45: continue

                in_pos = True
                dir_   = "long" if up else "short"
                entry  = price; best = price
                stop   = entry * (1 - trailing) if dir_ == "long" else entry * (1 + trailing)

        coin_pnls[coin] = round(coin_pnl, 2)

    return _build_result(all_events, all_trades, days, coin_pnls=coin_pnls)


# ── LIQUIDATION BOT BACKTEST (simplified proxies) ─────────────────────────────

def _bt_liq(cfg, candles_cache: dict, days: int) -> dict:
    """
    Simplified backtest for liquidation bots using price/volume proxy signals.
    """
    trailing = cfg.trailing_pct
    leverage = cfg.leverage
    risk_pct = cfg.risk_per_trade
    strategy = cfg.strategy

    from sim_engine import FALLBACK_COINS
    liq_coins = ["BTC", "ETH", "SOL", "XRP"]
    bt_coins  = [c for c in liq_coins if c in FALLBACK_COINS][:4]

    all_events = []
    all_trades = []
    coin_equity = INITIAL_EQUITY / len(bt_coins)

    for coin in bt_coins:
        key = f"{coin}_1h"
        df  = candles_cache.get(key, [])
        if len(df) < 25:
            continue

        closes  = [c["c"] for c in df]
        volumes = [c["v"] for c in df]
        tss     = [c["t"] for c in df]

        in_pos  = False
        dir_    = ""
        entry   = 0.0
        best    = 0.0
        stop    = 0.0
        eq_coin = coin_equity

        rsi_vals = _rsi(closes, 14) if strategy == "funding" else []

        for i in range(20, len(df)):
            price = closes[i]
            ts    = tss[i]

            if in_pos:
                if dir_ == "long":
                    if price > best:
                        best = price; stop = best * (1 - trailing)
                    if price <= stop:
                        pnl = eq_coin * risk_pct * (price - entry) / entry * leverage
                        eq_coin += pnl
                        all_events.append((ts, pnl))
                        all_trades.append({"pnl": round(pnl, 2)})
                        in_pos = False
                else:
                    if price < best:
                        best = price; stop = best * (1 + trailing)
                    if price >= stop:
                        pnl = eq_coin * risk_pct * (entry - price) / entry * leverage
                        eq_coin += pnl
                        all_events.append((ts, pnl))
                        all_trades.append({"pnl": round(pnl, 2)})
                        in_pos = False

            if not in_pos:
                sig = _liq_signal(strategy, closes, volumes, rsi_vals, i)
                if sig:
                    in_pos = True; dir_ = sig; entry = price; best = price
                    stop = entry * (1 - trailing) if sig == "long" else entry * (1 + trailing)

    return _build_result(all_events, all_trades, days)


def _liq_signal(strategy: str, closes: list, volumes: list,
                rsi_vals: list, i: int) -> Optional[str]:
    if i < 20:
        return None
    price = closes[i]

    if strategy in ("agresivo", "moderado"):
        # Fade 1.5% moves
        move = (closes[i] - closes[i - 4]) / closes[i - 4]
        if move < -0.015: return "long"
        if move >  0.015: return "short"

    elif strategy == "conservador":
        move = (closes[i] - closes[i - 8]) / closes[i - 8]
        avg_vol = sum(volumes[i - 10:i]) / 10 if sum(volumes[i - 10:i]) > 0 else 1
        vol_ok  = volumes[i] > avg_vol * 1.5
        if move < -0.025 and vol_ok: return "long"
        if move >  0.025 and vol_ok: return "short"

    elif strategy == "funding":
        # RSI extremes as funding proxy
        rsi_i = i - 20  # rsi_vals starts 20 candles later
        if rsi_i < 0 or rsi_i >= len(rsi_vals): return None
        rsi = rsi_vals[rsi_i]
        if rsi < 25: return "long"
        if rsi > 75: return "short"

    elif strategy == "cascada":
        # 4 consecutive same-direction candles
        if i < 4: return None
        last4 = [closes[j] - closes[j - 1] for j in range(i - 3, i + 1)]
        if all(d < 0 for d in last4): return "long"
        if all(d > 0 for d in last4): return "short"

    elif strategy in ("oi_div", "oi_divergencia"):
        # Price up + volume down → OI divergence proxy
        pmove = (closes[i] - closes[i - 4]) / closes[i - 4]
        avg_v = sum(volumes[i - 4:i]) / 4 if sum(volumes[i - 4:i]) > 0 else 1
        vmove = (volumes[i] - avg_v) / avg_v
        if pmove >  0.005 and vmove < -0.10: return "short"
        if pmove < -0.005 and vmove < -0.10: return "long"

    elif strategy == "whale":
        # Oversized candle body vs average
        body = abs(closes[i] - closes[i - 1]) / closes[i - 1] if closes[i - 1] > 0 else 0
        avg_bodies = [abs(closes[j] - closes[j - 1]) / closes[j - 1]
                      for j in range(i - 10, i) if closes[j - 1] > 0]
        avg_b = sum(avg_bodies) / len(avg_bodies) if avg_bodies else 0
        if avg_b > 0 and body > avg_b * 4:
            return "short" if closes[i] > closes[i - 1] else "long"

    elif strategy == "contra":
        # Fade large 2h move
        move = (closes[i] - closes[i - 2]) / closes[i - 2]
        if move < -0.02: return "long"
        if move >  0.02: return "short"

    return None


def _build_result(events: list, trades: list, days: int,
                  coin_pnls: dict = None) -> dict:
    """
    Build backtest result from accumulated events.
    PnL formula: total_pnl_usd = sum(all coin PnLs), pnl_pct = total_pnl_usd / INITIAL_EQUITY * 100
    Each coin uses full INITIAL_EQUITY as base — results are additive, NOT averaged.
    coin_pnls: optional {coin: pnl_usd} breakdown for logging.
    """
    if not events:
        ts = int(time.time() * 1000)
        snaps = [[ts - days * 86_400_000, INITIAL_EQUITY], [ts, INITIAL_EQUITY]]
        return _compute_metrics(snaps, [], INITIAL_EQUITY, days)

    events.sort(key=lambda x: x[0])

    # Explicit formula: total_pnl_usd = sum of all trades across all coins
    total_pnl_usd = sum(pnl for _, pnl in events)

    # Build equity curve starting from INITIAL_EQUITY
    eq    = INITIAL_EQUITY
    snaps = []
    for ts, pnl in events:
        eq += pnl
        snaps.append([ts, round(eq, 2)])

    if coin_pnls:
        breakdown = ", ".join(f"{c}=${v:+.0f}" for c, v in coin_pnls.items())
        log.debug(f"[BT] PnL breakdown: {breakdown} → total=${total_pnl_usd:+.0f} "
                  f"({total_pnl_usd / INITIAL_EQUITY * 100:+.1f}%)")

    metrics = _compute_metrics(snaps, trades, INITIAL_EQUITY, days)

    # Override with explicit formula to guarantee no hidden division
    metrics["total_pnl"]     = round(total_pnl_usd, 2)
    metrics["total_pnl_pct"] = round(total_pnl_usd / INITIAL_EQUITY * 100, 2)
    metrics["final_equity"]  = round(INITIAL_EQUITY + total_pnl_usd, 2)

    return metrics


# ── CACHE & THREAD MANAGEMENT ─────────────────────────────────────────────────

_cache:    dict          = {}
_progress: dict          = {}
_running:  set           = set()
_bt_lock:  threading.Lock = threading.Lock()


def _cache_key(period: str, coins: str) -> str:
    return f"{period}|{coins}" if coins else period


def get_progress(period: str, coins: str = "") -> int:
    return _progress.get(_cache_key(period, coins), 0)


def get_result(period: str, coins: str = "") -> Optional[dict]:
    return _cache.get(_cache_key(period, coins))


def is_running(period: str, coins: str = "") -> bool:
    return _cache_key(period, coins) in _running


def run_backtest_bg(period: str, coins: str = ""):
    """Start background backtest.
    coins: comma-separated list of coins to override cfg.coins (e.g. 'BTC,ETH').
           Empty string = use each bot's own cfg.coins / FALLBACK_COINS[:10].
    """
    key = _cache_key(period, coins)
    with _bt_lock:
        if key in _running or key in _cache:
            return
        _running.add(key)
        _progress[key] = 1
    t = threading.Thread(target=_run, args=(period, coins),
                         daemon=True, name=f"bt_{key}")
    t.start()


def _run(period: str, coins: str = ""):
    key = _cache_key(period, coins)
    # Parse override coin list
    coins_override = [c.strip() for c in coins.split(",") if c.strip()] if coins else []

    try:
        days = PERIODS.get(period, 90)
        log.info(f"[BT] Starting backtest period={period} coins={coins or 'default'} days={days}")

        from sim_engine import CONFIGS, LIQ_CONFIGS, FALLBACK_COINS

        # Collect required candle keys
        needed = set()
        for cfg in CONFIGS:
            iv = BT_INTERVAL_MAP.get(cfg.interval, "1h")
            # Use override coins if provided, else bot's own coins / fallback
            run_coins = coins_override or (cfg.coins if getattr(cfg, "coins", None) else FALLBACK_COINS[:10])
            for coin in run_coins:
                needed.add((coin, iv))
        for coin in (coins_override or ["BTC", "ETH", "SOL", "XRP"]):
            needed.add((coin, "1h"))

        needed      = list(needed)
        total_steps = len(needed) + len(CONFIGS) + len(LIQ_CONFIGS)
        done        = 0

        def _upd(n: int = 1):
            nonlocal done
            done += n
            _progress[key] = min(99, int(done / total_steps * 100))

        # Fetch candles (with rate-limit courtesy sleep)
        candles_cache = {}
        for coin, iv in needed:
            ck = f"{coin}_{iv}"
            if ck not in candles_cache:
                candles_cache[ck] = _fetch_candles(coin, iv, days)
                time.sleep(0.12)
            _upd()

        # Backtest EMA/SMA bots
        results = []
        for cfg in CONFIGS:
            r = _bt_crossover(cfg, candles_cache, days, coins_override=coins_override or None)
            r["label"]    = cfg.label
            r["strategy"] = f"{cfg.ma_type.upper()} {cfg.ma_fast}/{cfg.ma_slow}"
            r["interval"] = cfg.interval
            r["bot_type"] = "ema"
            r["idx"]      = cfg.idx
            results.append(r)
            _upd()

        # Backtest Liq bots
        for cfg in LIQ_CONFIGS:
            r = _bt_liq(cfg, candles_cache, days)
            r["label"]    = cfg.label
            r["strategy"] = f"LIQ·{cfg.strategy.upper()}"
            r["interval"] = "1h"
            r["bot_type"] = "liq"
            r["idx"]      = cfg.idx
            results.append(r)
            _upd()

        results.sort(key=lambda x: x["total_pnl"], reverse=True)

        with _bt_lock:
            _cache[key] = {
                "period":       period,
                "coins":        coins or "default",
                "days":         days,
                "bots":         results,
                "computed_at":  time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            _progress[key] = 100
            _running.discard(key)

        log.info(f"[BT] Done period={period} coins={coins or 'default'} bots={len(results)}")

    except Exception as exc:
        log.error(f"[BT] Error period={period} coins={coins}: {exc}", exc_info=True)
        with _bt_lock:
            _progress[key] = -1
            _running.discard(key)
