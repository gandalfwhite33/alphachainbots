#!/usr/bin/env python3
"""
backtest_engine.py — Motor de backtest histórico con datos reales de Hyperliquid.
Usa la misma simulación que optimizer_master.py para resultados idénticos.
Exporta: run_backtest_bg(period), get_result(period), get_progress(period)
"""

import math
import time
import threading
import logging
import requests
from typing import Optional

import numpy as np
import optimizer_master as _opt

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

# Usar el intervalo real del bot (no remapear a 1h)
BT_INTERVAL_MAP = {
    "15m": "15m", "30m": "30m",
    "1h": "1h", "2h": "2h", "4h": "4h",
}

# ── EXTERNAL DATA CACHE ──────────────────────────────────────────────────────
_ext_cache = {"fng": None, "funding": None, "ts": 0}


def _fetch_external_data():
    """Fetch Fear & Greed y Funding Rate para evaluación de filtros."""
    now = time.time()
    if now - _ext_cache["ts"] < 300:
        return _ext_cache["fng"], _ext_cache["funding"]
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
        if r.ok:
            d = r.json().get("data", [{}])[0]
            _ext_cache["fng"] = float(d.get("value", 50))
    except Exception:
        pass
    try:
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=1",
            timeout=5)
        if r.ok:
            data = r.json()
            if data:
                _ext_cache["funding"] = float(data[-1].get("fundingRate", 0))
    except Exception:
        pass
    _ext_cache["ts"] = now
    return _ext_cache["fng"], _ext_cache["funding"]


# ── CANDLE FETCH ──────────────────────────────────────────────────────────────

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


# ── LIST-BASED INDICATORS (para _bt_liq backward compat) ─────────────────────

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


# ── METRICS ───────────────────────────────────────────────────────────────────

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


# ── INDICADORES PARA UNA MONEDA (idéntico al optimizer) ──────────────────────

def _compute_indicators(candles_list: list, btc_candles_list: list = None) -> dict:
    """Construye dict de indicadores idéntico a optimizer_master._worker_init."""
    arr = np.array([[c["t"], c["o"], c["h"], c["l"], c["c"], c["v"]]
                    for c in candles_list], dtype=np.float64)
    if len(arr) < 250:
        return None

    ts_col = arr[:, 0]; opens = arr[:, 1]; highs = arr[:, 2]
    lows = arr[:, 3]; closes = arr[:, 4]; volumes = arr[:, 5]

    atr14 = _opt._atr(highs, lows, closes, 14)
    rsi14 = _opt._rsi(closes, 14)
    ema200 = _opt._ema(closes, 200)
    vol_ma = _opt._sma(volumes, 20)
    roll_hi = _opt._roll_max(highs, 60)
    roll_lo = _opt._roll_min(lows, 60)

    macd_l, macd_s, macd_h = _opt._macd(closes)
    lb = 14
    macd_bull_div = np.zeros(len(closes), dtype=bool)
    macd_bear_div = np.zeros(len(closes), dtype=bool)
    for i in range(lb * 2, len(closes)):
        if macd_h[i] != macd_h[i]:
            continue
        sl_c = closes[i - lb:i]; sl_mh = macd_h[i - lb:i]
        lo_c = _opt._safe_nanmin(sl_c); hi_c = _opt._safe_nanmax(sl_c)
        lo_mh = _opt._safe_nanmin(sl_mh); hi_mh = _opt._safe_nanmax(sl_mh)
        if np.isnan(lo_c) or np.isnan(hi_c) or np.isnan(lo_mh) or np.isnan(hi_mh):
            continue
        if closes[i] <= lo_c and macd_h[i] > lo_mh:
            macd_bull_div[i] = True
        if closes[i] >= hi_c and macd_h[i] < hi_mh:
            macd_bear_div[i] = True

    adx = _opt._adx(highs, lows, closes)
    st = _opt._supertrend(highs, lows, closes)
    tenkan, kijun, span_a, span_b = _opt._ichimoku(highs, lows, closes)
    cloud_top = np.maximum(span_a, span_b)
    cloud_bot = np.minimum(span_a, span_b)
    sk, sd = _opt._stoch_rsi(closes)
    cci = _opt._cci(highs, lows, closes)
    wr = _opt._williams_r_arr(highs, lows, closes)
    mom10 = closes - np.roll(closes, 10); mom10[:10] = np.nan
    bb_u, bb_l, bb_w = _opt._bbands(closes)
    kelt_u, kelt_l = _opt._keltner(highs, lows, closes)
    bb_w_sma = _opt._sma(bb_w, 20)
    obv_arr = _opt._obv(closes, volumes)
    obv_ema = _opt._ema(obv_arr, 20)

    obv_bull_div = np.zeros(len(closes), dtype=bool)
    obv_bear_div = np.zeros(len(closes), dtype=bool)
    for i in range(lb * 2, len(closes)):
        sl_c = closes[i - lb:i]; sl_o = obv_arr[i - lb:i]
        lo_c = _opt._safe_nanmin(sl_c); hi_c = _opt._safe_nanmax(sl_c)
        lo_o = _opt._safe_nanmin(sl_o); hi_o = _opt._safe_nanmax(sl_o)
        if np.isnan(lo_c) or np.isnan(hi_c) or np.isnan(lo_o) or np.isnan(hi_o):
            continue
        if closes[i] <= lo_c and obv_arr[i] > lo_o:
            obv_bull_div[i] = True
        if closes[i] >= hi_c and obv_arr[i] < hi_o:
            obv_bear_div[i] = True

    vwap_arr = _opt._vwap_daily(ts_col, highs, lows, closes, volumes)
    cvd_arr = _opt._cvd(opens, closes, volumes)
    cvd_ema = _opt._ema(cvd_arr.astype(float), 14)
    ms_arr = _opt._market_structure(highs, lows)
    ob_bull, ob_bear = _opt._order_blocks(closes, atr14)
    piv_p, piv_r1, piv_r2, piv_s1, piv_s2 = _opt._pivot_levels(
        ts_col, highs, lows, closes, weekly=False)
    wpiv_p, wpiv_r1, wpiv_r2, wpiv_s1, wpiv_s2 = _opt._pivot_levels(
        ts_col, highs, lows, closes, weekly=True)
    rsi_bull_div, rsi_bear_div = _opt._rsi_divergence_arr(closes, rsi14)
    psar_arr = _opt._parabolic_sar(highs, lows)

    price_mean = float(np.nanmean(closes))
    atr_valid = atr14[~np.isnan(atr14)]
    atr_pct_ref = (float(np.nanmean(atr_valid)) / price_mean
                   if len(atr_valid) > 0 and price_mean > 0 else 0.0)
    atr_std = float(np.nanstd(atr_valid)) if len(atr_valid) > 1 else 0.0
    atr_mean = float(np.nanmean(atr_valid)) if len(atr_valid) > 0 else 0.0

    roll_hi10 = _opt._roll_max(highs, 10);  roll_lo10 = _opt._roll_min(lows, 10)
    roll_hi20 = _opt._roll_max(highs, 20);  roll_lo20 = _opt._roll_min(lows, 20)
    roll_hi50 = _opt._roll_max(highs, 50);  roll_lo50 = _opt._roll_min(lows, 50)
    roll_hi100 = _opt._roll_max(highs, 100); roll_lo100 = _opt._roll_min(lows, 100)

    # BTC trend para btc_correlation
    btc_trend = None
    if btc_candles_list is not None and len(btc_candles_list) >= 60:
        btc_arr = np.array([[c["t"], c["o"], c["h"], c["l"], c["c"], c["v"]]
                            for c in btc_candles_list], dtype=np.float64)
        if len(btc_arr) >= 60:
            bc = btc_arr[:, 4]
            e20 = _opt._ema(bc, 20); e50 = _opt._ema(bc, 50)
            btc_trend = np.where(e20 > e50, 1.0, -1.0)

    ind = {
        "ts": ts_col, "opens": opens, "highs": highs, "lows": lows,
        "closes": closes, "volumes": volumes,
        "ema200": ema200, "rsi14": rsi14, "atr14": atr14,
        "vol_ma": vol_ma, "roll_hi": roll_hi, "roll_lo": roll_lo,
        "h_london": _opt._hour_mask(ts_col, _opt._LONDON_NY),
        "h_asia":   _opt._hour_mask(ts_col, _opt._ASIA),
        "h_ny":     _opt._hour_mask(ts_col, _opt._NY),
        "h_lno":    _opt._hour_mask(ts_col, _opt._LDN_NY_OV),
        "atr_pct_ref": atr_pct_ref, "price_mean": price_mean,
        "atr_mean": atr_mean, "atr_std": atr_std,
        "macd_l": macd_l, "macd_s": macd_s, "macd_h": macd_h,
        "macd_bull_div": macd_bull_div, "macd_bear_div": macd_bear_div,
        "adx": adx, "st": st,
        "cloud_top": cloud_top, "cloud_bot": cloud_bot,
        "tenkan": tenkan, "kijun": kijun,
        "sk": sk, "sd": sd, "cci": cci, "wr": wr, "mom10": mom10,
        "bb_u": bb_u, "bb_l": bb_l, "bb_w": bb_w, "bb_w_sma": bb_w_sma,
        "kelt_u": kelt_u, "kelt_l": kelt_l,
        "obv": obv_arr, "obv_ema": obv_ema,
        "obv_bull_div": obv_bull_div, "obv_bear_div": obv_bear_div,
        "vwap": vwap_arr, "cvd": cvd_arr.astype(float), "cvd_ema": cvd_ema,
        "ms": ms_arr, "ob_bull": ob_bull, "ob_bear": ob_bear,
        "piv_p": piv_p, "piv_r1": piv_r1, "piv_r2": piv_r2,
        "piv_s1": piv_s1, "piv_s2": piv_s2,
        "wpiv_p": wpiv_p, "wpiv_r1": wpiv_r1, "wpiv_r2": wpiv_r2,
        "wpiv_s1": wpiv_s1, "wpiv_s2": wpiv_s2,
        "rsi_bull_div": rsi_bull_div, "rsi_bear_div": rsi_bear_div,
        "psar": psar_arr,
        "roll_hi10": roll_hi10, "roll_lo10": roll_lo10,
        "roll_hi20": roll_hi20, "roll_lo20": roll_lo20,
        "roll_hi50": roll_hi50, "roll_lo50": roll_lo50,
        "roll_hi100": roll_hi100, "roll_lo100": roll_lo100,
        "btc_trend": btc_trend,
    }

    # Precalcular MAs para todos los pares
    for (ma_type, fast, slow) in _opt.MA_PAIRS:
        k = f"{ma_type}_{fast}_{slow}"
        fn = _opt._ema if ma_type == "ema" else _opt._sma
        ind[f"maf_{k}"] = fn(closes, fast)
        ind[f"mas_{k}"] = fn(closes, slow)

    return ind


# ── BOTCONFIG → OPTPARAMS ────────────────────────────────────────────────────

def _cfg_to_params(cfg, coin: str) -> _opt.OptParams:
    """Convierte BotConfig + coin a OptParams para _simulate_fast."""
    return _opt.OptParams(
        interval=cfg.interval,
        ma_type=cfg.ma_type,
        ma_fast=cfg.ma_fast,
        ma_slow=cfg.ma_slow,
        leverage=cfg.leverage,
        trailing_pct=cfg.trailing_pct,
        sl_type=getattr(cfg, "sl_type", "trailing"),
        fib_mode=getattr(cfg, "fib_mode", "disabled"),
        rsi_filter=getattr(cfg, "rsi_filter", "none"),
        ema200_filter=getattr(cfg, "ema200_filter", "none"),
        atr_filter=getattr(cfg, "atr_filter", "none"),
        compound=getattr(cfg, "compound", True),
        time_filter=getattr(cfg, "time_filter", "none"),
        vol_profile=getattr(cfg, "vol_profile", "disabled"),
        liq_confirm=getattr(cfg, "liq_confirm", False),
        risk_pct=getattr(cfg, "risk_per_trade", 0.02),
        macd_filter=getattr(cfg, "macd_filter", "none"),
        adx_filter=getattr(cfg, "adx_filter", "none"),
        supertrend_filter=getattr(cfg, "supertrend_filter", "none"),
        ichimoku_filter=getattr(cfg, "ichimoku_filter", "none"),
        stoch_rsi=getattr(cfg, "stoch_rsi", "none"),
        cci_filter=getattr(cfg, "cci_filter", "none"),
        williams_r=getattr(cfg, "williams_r", "none"),
        momentum_filter=getattr(cfg, "momentum_filter", "none"),
        bb_filter=getattr(cfg, "bb_filter", "none"),
        atr_volatility=getattr(cfg, "atr_volatility", "none"),
        keltner_filter=getattr(cfg, "keltner_filter", "none"),
        obv_filter=getattr(cfg, "obv_filter", "none"),
        vwap_filter=getattr(cfg, "vwap_filter", "none"),
        volume_delta=getattr(cfg, "volume_delta", "none"),
        cvd_filter=getattr(cfg, "cvd_filter", "none"),
        market_structure=getattr(cfg, "market_structure", "none"),
        breakout_range=getattr(cfg, "breakout_range", "none"),
        candle_pattern=getattr(cfg, "candle_pattern", "none"),
        order_block=getattr(cfg, "order_block", "none"),
        pivot_filter=getattr(cfg, "pivot_filter", "none"),
        sr_breakout=getattr(cfg, "sr_breakout", "none"),
        fib_retracement=getattr(cfg, "fib_retracement", "none"),
        rsi_divergence=getattr(cfg, "rsi_divergence", "none"),
        btc_correlation=getattr(cfg, "btc_correlation", "none"),
        funding_filter=getattr(cfg, "funding_filter", "none"),
        fear_greed_filter=getattr(cfg, "fear_greed_filter", "none"),
        session_filter=getattr(cfg, "session_filter", "none"),
        position_sizing=getattr(cfg, "position_sizing", "fixed"),
        max_trades_day=getattr(cfg, "max_trades_day", 0),
        trailing_type=getattr(cfg, "trailing_type", "fixed"),
        min_confluences=getattr(cfg, "min_confluences", 0),
        tp_type=getattr(cfg, "tp_type", "none"),
        tp_pct=getattr(cfg, "tp_pct", 10),
        tp_atr=getattr(cfg, "tp_atr", 2.0),
        trailing_activation=getattr(cfg, "trailing_activation", "none"),
        trailing_progressive=getattr(cfg, "trailing_progressive", False),
        atr_tp_adjust=getattr(cfg, "atr_tp_adjust", "none"),
        partial_close=getattr(cfg, "partial_close", "none"),
        partial_trigger=getattr(cfg, "partial_trigger", "1atr"),
        breakeven=getattr(cfg, "breakeven", "none"),
        time_exit=getattr(cfg, "time_exit", "none"),
        session_exit=getattr(cfg, "session_exit", False),
        weekend_exit=getattr(cfg, "weekend_exit", False),
        rr_min=getattr(cfg, "rr_min", "none"),
        coin=coin,
        direction=getattr(cfg, "direction", "both"),
    )


# ── EMA / SMA CROSSOVER BACKTEST (usa simulación del optimizer) ──────────────

def _bt_crossover(cfg, candles_cache: dict, days: int,
                  coins_override: list = None) -> dict:
    """
    Backtest usando _simulate_fast del optimizer — resultados idénticos.
    cfg: BotConfig con todos los filtros del optimizer.
    """
    bt_interval = BT_INTERVAL_MAP.get(cfg.interval, cfg.interval)

    from sim_engine import FALLBACK_COINS
    cfg_coins = cfg.coins if getattr(cfg, "coins", None) else FALLBACK_COINS[:6]
    if coins_override:
        bt_coins = [c for c in cfg_coins if c in coins_override]
        if not bt_coins:
            bt_coins = []
    else:
        bt_coins = cfg_coins

    # Obtener datos externos si hay filtros que los necesitan
    fng_val, fund_val = None, None
    if (getattr(cfg, "fear_greed_filter", "none") != "none" or
            getattr(cfg, "funding_filter", "none") != "none"):
        fng_val, fund_val = _fetch_external_data()

    all_events = []
    all_trades = []

    for coin in bt_coins:
        key = f"{coin}_{bt_interval}"
        df = candles_cache.get(key, [])
        if len(df) < max(cfg.ma_slow + 260, 300):
            continue

        # Candles BTC para filtro btc_correlation
        btc_candles = None
        if getattr(cfg, "btc_correlation", "none") != "none" and coin != "BTC":
            btc_key = f"BTC_{bt_interval}"
            btc_candles = candles_cache.get(btc_key)

        # Calcular indicadores (idéntico al optimizer)
        ind = _compute_indicators(df, btc_candles)
        if ind is None:
            continue

        # Construir OptParams desde BotConfig
        params = _cfg_to_params(cfg, coin)

        # Ejecutar simulación idéntica al optimizer
        result, events, trades = _opt._simulate_fast(
            params, ind,
            return_events=True,
            fng_override=fng_val,
            fund_override=fund_val,
        )

        all_events.extend(events)
        all_trades.extend(trades)

    return _build_result(all_events, all_trades, days)


# ── LIQUIDATION BOT BACKTEST (sin cambios) ───────────────────────────────────

def _bt_liq(cfg, candles_cache: dict, days: int,
            coins_override: list = None) -> dict:
    """
    Simplified backtest for liquidation bots using price/volume proxy signals.
    coins_override: if provided, restrict to these coins.
    """
    trailing = cfg.trailing_pct
    leverage = cfg.leverage
    risk_pct = cfg.risk_per_trade
    strategy = cfg.strategy

    from sim_engine import FALLBACK_COINS
    liq_coins = ["BTC", "ETH", "SOL", "XRP"]
    if coins_override:
        bt_coins = [c for c in liq_coins if c in coins_override and c in FALLBACK_COINS]
    else:
        bt_coins = [c for c in liq_coins if c in FALLBACK_COINS][:4]

    all_events = []
    all_trades = []
    coin_equity = INITIAL_EQUITY

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
        rsi_i = i - 20
        if rsi_i < 0 or rsi_i >= len(rsi_vals): return None
        rsi_v = rsi_vals[rsi_i]
        if rsi_v < 25: return "long"
        if rsi_v > 75: return "short"

    elif strategy == "cascada":
        if i < 4: return None
        last4 = [closes[j] - closes[j - 1] for j in range(i - 3, i + 1)]
        if all(d < 0 for d in last4): return "long"
        if all(d > 0 for d in last4): return "short"

    elif strategy in ("oi_div", "oi_divergencia"):
        pmove = (closes[i] - closes[i - 4]) / closes[i - 4]
        avg_v = sum(volumes[i - 4:i]) / 4 if sum(volumes[i - 4:i]) > 0 else 1
        vmove = (volumes[i] - avg_v) / avg_v
        if pmove >  0.005 and vmove < -0.10: return "short"
        if pmove < -0.005 and vmove < -0.10: return "long"

    elif strategy == "whale":
        body = abs(closes[i] - closes[i - 1]) / closes[i - 1] if closes[i - 1] > 0 else 0
        avg_bodies = [abs(closes[j] - closes[j - 1]) / closes[j - 1]
                      for j in range(i - 10, i) if closes[j - 1] > 0]
        avg_b = sum(avg_bodies) / len(avg_bodies) if avg_bodies else 0
        if avg_b > 0 and body > avg_b * 4:
            return "short" if closes[i] > closes[i - 1] else "long"

    elif strategy == "contra":
        move = (closes[i] - closes[i - 2]) / closes[i - 2]
        if move < -0.02: return "long"
        if move >  0.02: return "short"

    return None


def _build_result(events: list, trades: list, days: int) -> dict:
    if not events:
        ts = int(time.time() * 1000)
        snaps = [[ts - days * 86_400_000, INITIAL_EQUITY], [ts, INITIAL_EQUITY]]
        return _compute_metrics(snaps, [], INITIAL_EQUITY, days)

    events.sort(key=lambda x: x[0])
    eq  = INITIAL_EQUITY
    snaps = []
    for ts, pnl in events:
        eq += pnl
        snaps.append([ts, round(eq, 2)])

    return _compute_metrics(snaps, trades, INITIAL_EQUITY, days)


# ── CACHE & THREAD MANAGEMENT ────────────────────────────────────────────────

_cache:    dict          = {}
_progress: dict          = {}
_running:  set           = set()
_bt_lock:  threading.Lock = threading.Lock()


def _cache_key(period: str, coins_key: str) -> str:
    return f"{period}|{coins_key}"


def get_progress(period: str, coins_key: str = "BTC") -> int:
    return _progress.get(_cache_key(period, coins_key), 0)


def get_result(period: str, coins_key: str = "BTC") -> Optional[dict]:
    return _cache.get(_cache_key(period, coins_key))


def is_running(period: str, coins_key: str = "BTC") -> bool:
    return _cache_key(period, coins_key) in _running


def run_backtest_bg(period: str, coins_key: str = "BTC"):
    """Start background backtest if not already running or cached."""
    key = _cache_key(period, coins_key)
    with _bt_lock:
        if key in _running or key in _cache:
            return
        _running.add(key)
        _progress[key] = 1
    t = threading.Thread(target=_run, args=(period, coins_key),
                         daemon=True, name=f"bt_{key}")
    t.start()


def _run(period: str, coins_key: str = "BTC"):
    ck = _cache_key(period, coins_key)
    try:
        days = PERIODS.get(period, 90)
        coins_override = [c.strip().upper() for c in coins_key.split(",") if c.strip()]
        log.info(f"[BT] Starting backtest period={period} coins={coins_override}")

        from sim_engine import CONFIGS, LIQ_CONFIGS, FALLBACK_COINS

        # Recoger intervalos necesarios (usar intervalo real del bot)
        needed = set()
        for cfg in CONFIGS:
            iv = BT_INTERVAL_MAP.get(cfg.interval, cfg.interval)
            cfg_coins = cfg.coins if getattr(cfg, "coins", None) else FALLBACK_COINS[:6]
            for coin in cfg_coins:
                if coin in coins_override:
                    needed.add((coin, iv))
            # BTC candles para btc_correlation
            if getattr(cfg, "btc_correlation", "none") != "none":
                needed.add(("BTC", iv))
        for coin in coins_override:
            needed.add((coin, "1h"))

        needed      = list(needed)
        total_steps = len(needed) + len(CONFIGS) + len(LIQ_CONFIGS)
        done        = 0

        def _upd(n: int = 1):
            nonlocal done
            done += n
            _progress[ck] = min(99, int(done / total_steps * 100))

        # Fetch candles
        candles_cache = {}
        for coin, iv in needed:
            key = f"{coin}_{iv}"
            if key not in candles_cache:
                candles_cache[key] = _fetch_candles(coin, iv, days)
                time.sleep(0.12)
            _upd()

        # Backtest EMA/SMA bots (usa simulación del optimizer)
        results = []
        for cfg in CONFIGS:
            r = _bt_crossover(cfg, candles_cache, days, coins_override=coins_override)
            r["label"]    = cfg.label
            r["strategy"] = f"{cfg.ma_type.upper()} {cfg.ma_fast}/{cfg.ma_slow}"
            r["interval"] = cfg.interval
            r["bot_type"] = "ema"
            r["idx"]      = cfg.idx
            results.append(r)
            _upd()

        # Backtest Liq bots
        for cfg in LIQ_CONFIGS:
            r = _bt_liq(cfg, candles_cache, days, coins_override=coins_override)
            r["label"]    = cfg.label
            r["strategy"] = f"LIQ·{cfg.strategy.upper()}"
            r["interval"] = "1h"
            r["bot_type"] = "liq"
            r["idx"]      = cfg.idx
            results.append(r)
            _upd()

        results.sort(key=lambda x: x["total_pnl"], reverse=True)

        with _bt_lock:
            _cache[ck] = {
                "period":       period,
                "days":         days,
                "bots":         results,
                "computed_at":  time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            _progress[ck] = 100
            _running.discard(ck)

        log.info(f"[BT] Done period={period} coins={coins_key} bots={len(results)}")

    except Exception as exc:
        log.error(f"[BT] Error period={period} coins={coins_key}: {exc}", exc_info=True)
        with _bt_lock:
            _progress[ck] = -1
            _running.discard(ck)
