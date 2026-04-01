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

import random

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
    "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000,
    "4h": 14_400_000, "1d": 86_400_000,
}

# Filtros de sesión horaria (UTC)
_LONDON_NY = set(range(7, 22))   # 07:00–21:59 UTC
_ASIA      = set(range(0, 9))    # 00:00–08:59 UTC

FALLBACK_COINS = ["BTC", "ETH", "SOL", "HYPE", "TAO", "XRP", "DOGE", "AVAX", "BNB", "LINK"]
_HL_URL = "https://api.hyperliquid.xyz/info"

MARKET_UPDATE_SEC = 300          # actualiza market data cada 5 min
MAX_OI_HISTORY    = 48           # ~4h de historial a 5-min intervals
LIQ_COINS         = ["BTC", "ETH", "SOL", "AVAX", "DOGE", "ARB", "OP", "WIF", "SUI"]
OI_COINS          = ["BTC", "ETH", "SOL", "AVAX", "DOGE", "ARB", "OP", "WIF", "SUI",
                     "XRP", "BNB", "LINK", "HYPE", "TAO"]

_mkt_lock:  threading.Lock = threading.Lock()
_mkt_cache: dict            = {"coins": {}, "liq": {}, "oi_hist": {}, "ts": 0}


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
    idx:            int   = 0
    require_fib:    bool  = True
    require_sr:     bool  = True
    # ── Filtros avanzados (nuevos bots) ────────────────────────────────────
    sl_type:        str   = "trailing"  # "trailing" | "fixed" | "atr"
    fixed_sl_pct:   float = 0.02        # para sl_type == "fixed"
    rsi_filter:     str   = "none"      # "none" | "rsi50" | "rsi55"
    ema200_filter:  str   = "none"      # "none" | "strict"
    atr_filter:     str   = "none"      # "none" | "max"
    time_filter:    str   = "none"      # "none" | "london_ny" | "asia"
    liq_confirm:    bool  = False       # confirmar con zona de liquidación cercana
    coins:          list  = field(default_factory=list)  # monedas específicas (vacío = usar top coins global)


CONFIGS = [
    # ── Bots originales (leverage x3) ──────────────────────────────────────────
    BotConfig(idx=0,
        name="bot_4h",        label="BOT·4H",
        interval="4h",        ma_type="ema",  ma_fast=20,  ma_slow=50,
        trailing_pct=0.015,   min_vol_ratio=1.5, sr_near_pct=0.010,
        fibo_zone_pct=0.015,  candle_limit=200,
    ),
    BotConfig(idx=1,
        name="bot_1h",        label="BOT·1H·EMA",
        interval="1h",        ma_type="ema",  ma_fast=20,  ma_slow=50,
        trailing_pct=0.010,   min_vol_ratio=1.5, sr_near_pct=0.010,
        fibo_zone_pct=0.012,  candle_limit=200,
    ),
    BotConfig(idx=2,
        name="bot_1h_ma",     label="BOT·1H·SMA",
        interval="1h",        ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.010,   min_vol_ratio=1.3, sr_near_pct=0.008,
        fibo_zone_pct=0.012,  candle_limit=300,
    ),
    # ── Bots 15m — leverage x5, trailing 0.5% ──────────────────────────────────
    BotConfig(idx=3,
        name="bot_15m_ema8",  label="BOT·15M·8/21",
        interval="15m",       ma_type="ema",  ma_fast=8,   ma_slow=21,
        trailing_pct=0.005,   min_vol_ratio=1.3, sr_near_pct=0.007,
        fibo_zone_pct=0.010,  candle_limit=300,  leverage=5.0,
    ),
    BotConfig(idx=4,
        name="bot_15m_ema13", label="BOT·15M·13/34",
        interval="15m",       ma_type="ema",  ma_fast=13,  ma_slow=34,
        trailing_pct=0.005,   min_vol_ratio=1.3, sr_near_pct=0.007,
        fibo_zone_pct=0.010,  candle_limit=300,  leverage=5.0,
    ),
    BotConfig(idx=5,
        name="bot_15m_ema21", label="BOT·15M·21/55",
        interval="15m",       ma_type="ema",  ma_fast=21,  ma_slow=55,
        trailing_pct=0.005,   min_vol_ratio=1.3, sr_near_pct=0.007,
        fibo_zone_pct=0.010,  candle_limit=300,  leverage=5.0,
    ),
    # ── Bots 30m — leverage x5, trailing 0.8% ──────────────────────────────────
    BotConfig(idx=6,
        name="bot_30m_ema8",  label="BOT·30M·8/21",
        interval="30m",       ma_type="ema",  ma_fast=8,   ma_slow=21,
        trailing_pct=0.008,   min_vol_ratio=1.4, sr_near_pct=0.008,
        fibo_zone_pct=0.011,  candle_limit=250,  leverage=5.0,
    ),
    BotConfig(idx=7,
        name="bot_30m_ema13", label="BOT·30M·13/34",
        interval="30m",       ma_type="ema",  ma_fast=13,  ma_slow=34,
        trailing_pct=0.008,   min_vol_ratio=1.4, sr_near_pct=0.008,
        fibo_zone_pct=0.011,  candle_limit=250,  leverage=5.0,
    ),
    BotConfig(idx=8,
        name="bot_30m_ema21", label="BOT·30M·21/55",
        interval="30m",       ma_type="ema",  ma_fast=21,  ma_slow=55,
        trailing_pct=0.008,   min_vol_ratio=1.4, sr_near_pct=0.008,
        fibo_zone_pct=0.011,  candle_limit=250,  leverage=5.0,
    ),
    # ── Bots 1h nuevos — leverage x5, trailing 1% ──────────────────────────────
    BotConfig(idx=9,
        name="bot_1h_ema8",   label="BOT·1H·8/21",
        interval="1h",        ma_type="ema",  ma_fast=8,   ma_slow=21,
        trailing_pct=0.010,   min_vol_ratio=1.5, sr_near_pct=0.010,
        fibo_zone_pct=0.012,  candle_limit=200,  leverage=5.0,
    ),
    BotConfig(idx=10,
        name="bot_1h_ema13",  label="BOT·1H·13/34",
        interval="1h",        ma_type="ema",  ma_fast=13,  ma_slow=34,
        trailing_pct=0.010,   min_vol_ratio=1.5, sr_near_pct=0.010,
        fibo_zone_pct=0.012,  candle_limit=200,  leverage=5.0,
    ),
    BotConfig(idx=11,
        name="bot_1h_ema21",  label="BOT·1H·21/55",
        interval="1h",        ma_type="ema",  ma_fast=21,  ma_slow=55,
        trailing_pct=0.010,   min_vol_ratio=1.5, sr_near_pct=0.010,
        fibo_zone_pct=0.012,  candle_limit=200,  leverage=5.0,
    ),
    # ── Bots 15m LITE — x5, trailing 0.5%, solo EMA + volumen 1.1x ────────────
    BotConfig(idx=12,
        name="bot_15m_ema8_lite",  label="BOT·15M·8·L",
        interval="15m",            ma_type="ema",  ma_fast=8,   ma_slow=21,
        trailing_pct=0.005,        min_vol_ratio=1.1, sr_near_pct=0.007,
        fibo_zone_pct=0.010,       candle_limit=300,  leverage=5.0,
        require_fib=False,         require_sr=False,
    ),
    BotConfig(idx=13,
        name="bot_15m_ema13_lite", label="BOT·15M·13·L",
        interval="15m",            ma_type="ema",  ma_fast=13,  ma_slow=34,
        trailing_pct=0.005,        min_vol_ratio=1.1, sr_near_pct=0.007,
        fibo_zone_pct=0.010,       candle_limit=300,  leverage=5.0,
        require_fib=False,         require_sr=False,
    ),
    BotConfig(idx=14,
        name="bot_15m_ema21_lite", label="BOT·15M·21·L",
        interval="15m",            ma_type="ema",  ma_fast=21,  ma_slow=55,
        trailing_pct=0.005,        min_vol_ratio=1.1, sr_near_pct=0.007,
        fibo_zone_pct=0.010,       candle_limit=300,  leverage=5.0,
        require_fib=False,         require_sr=False,
    ),
    # ── Bots 30m LITE — x5, trailing 0.8%, solo EMA + volumen 1.1x ────────────
    BotConfig(idx=15,
        name="bot_30m_ema8_lite",  label="BOT·30M·8·L",
        interval="30m",            ma_type="ema",  ma_fast=8,   ma_slow=21,
        trailing_pct=0.008,        min_vol_ratio=1.1, sr_near_pct=0.008,
        fibo_zone_pct=0.011,       candle_limit=250,  leverage=5.0,
        require_fib=False,         require_sr=False,
    ),
    BotConfig(idx=16,
        name="bot_30m_ema13_lite", label="BOT·30M·13·L",
        interval="30m",            ma_type="ema",  ma_fast=13,  ma_slow=34,
        trailing_pct=0.008,        min_vol_ratio=1.1, sr_near_pct=0.008,
        fibo_zone_pct=0.011,       candle_limit=250,  leverage=5.0,
        require_fib=False,         require_sr=False,
    ),
    BotConfig(idx=17,
        name="bot_30m_ema21_lite", label="BOT·30M·21·L",
        interval="30m",            ma_type="ema",  ma_fast=21,  ma_slow=55,
        trailing_pct=0.008,        min_vol_ratio=1.1, sr_near_pct=0.008,
        fibo_zone_pct=0.011,       candle_limit=250,  leverage=5.0,
        require_fib=False,         require_sr=False,
    ),
    # ── Conservadores ──────────────────────────────────────────────────────────
    BotConfig(idx=26,
        name="bot_2h_cons",    label="BOT·2H·CONS",
        interval="2h",         ma_type="ema",  ma_fast=20,  ma_slow=50,
        trailing_pct=0.008,    min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.012,   candle_limit=250,  leverage=10.0,
        require_fib=False,     require_sr=False,
        sl_type="trailing",    rsi_filter="rsi50",
        atr_filter="max",      time_filter="asia",
    ),
    BotConfig(idx=27,
        name="bot_1h_cons",    label="BOT·1H·CONS",
        interval="1h",         ma_type="sma",  ma_fast=100, ma_slow=200,
        trailing_pct=0.010,    min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.012,   candle_limit=300,  leverage=10.0,
        require_fib=False,     require_sr=False,
        sl_type="fixed",       fixed_sl_pct=0.02,
        rsi_filter="rsi55",    ema200_filter="strict",
        time_filter="london_ny",
    ),
    BotConfig(idx=28,
        name="bot_4h_cons",    label="BOT·4H·CONS",
        interval="4h",         ma_type="sma",  ma_fast=100, ma_slow=200,
        trailing_pct=0.015,    min_vol_ratio=1.2, sr_near_pct=0.012,
        fibo_zone_pct=0.015,   candle_limit=300,  leverage=10.0,
        require_fib=False,     require_sr=False,
        sl_type="fixed",       fixed_sl_pct=0.03,
        rsi_filter="rsi55",    ema200_filter="strict",
        time_filter="asia",
    ),
    # ── Equilibrados ───────────────────────────────────────────────────────────
    BotConfig(idx=29,
        name="bot_2h_eq",      label="BOT·2H·EQ",
        interval="2h",         ma_type="ema",  ma_fast=20,  ma_slow=50,
        trailing_pct=0.010,    min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.012,   candle_limit=250,  leverage=15.0,
        require_fib=False,     require_sr=False,
        sl_type="trailing",    rsi_filter="rsi55",
        atr_filter="max",      time_filter="asia",
    ),
    BotConfig(idx=30,
        name="bot_1h_eq",      label="BOT·1H·EQ",
        interval="1h",         ma_type="ema",  ma_fast=13,  ma_slow=34,
        trailing_pct=0.030,    min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.012,   candle_limit=200,  leverage=10.0,
        require_fib=False,     require_sr=False,
        sl_type="trailing",    rsi_filter="rsi50",
        time_filter="asia",
    ),
    BotConfig(idx=31,
        name="bot_30m_eq",     label="BOT·30M·EQ",
        interval="30m",        ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.008,    min_vol_ratio=1.2, sr_near_pct=0.008,
        fibo_zone_pct=0.011,   candle_limit=250,  leverage=10.0,
        require_fib=False,     require_sr=False,
        sl_type="fixed",       fixed_sl_pct=0.01,
        ema200_filter="strict", time_filter="asia",
    ),
    BotConfig(idx=32,
        name="bot_4h_eq",      label="BOT·4H·EQ",
        interval="4h",         ma_type="ema",  ma_fast=20,  ma_slow=50,
        trailing_pct=0.005,    min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.015,   candle_limit=200,  leverage=15.0,
        require_fib=False,     require_sr=False,
        sl_type="atr",         rsi_filter="rsi50",
        time_filter="london_ny",
    ),
    # ── Alta frecuencia ────────────────────────────────────────────────────────
    BotConfig(idx=33,
        name="bot_15m_hf",     label="BOT·15M·HF",
        interval="15m",        ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.030,    min_vol_ratio=1.1, sr_near_pct=0.007,
        fibo_zone_pct=0.010,   candle_limit=300,  leverage=15.0,
        require_fib=False,     require_sr=False,
        sl_type="trailing",    liq_confirm=True,
    ),
    BotConfig(idx=34,
        name="bot_15m_hf2",    label="BOT·15M·HF2",
        interval="15m",        ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.020,    min_vol_ratio=1.1, sr_near_pct=0.007,
        fibo_zone_pct=0.010,   candle_limit=300,  leverage=10.0,
        require_fib=False,     require_sr=False,
        sl_type="trailing",    rsi_filter="rsi50",
        ema200_filter="strict", time_filter="london_ny",
    ),
    BotConfig(idx=35,
        name="bot_30m_hf",     label="BOT·30M·HF",
        interval="30m",        ma_type="sma",  ma_fast=100, ma_slow=200,
        trailing_pct=0.008,    min_vol_ratio=1.1, sr_near_pct=0.008,
        fibo_zone_pct=0.011,   candle_limit=300,  leverage=10.0,
        require_fib=False,     require_sr=False,
        sl_type="fixed",       fixed_sl_pct=0.02,
        rsi_filter="rsi50",    liq_confirm=True,
        time_filter="asia",
    ),
    # ── Conservadores BTC/ETH (idx 36-41) ──────────────────────────────────────
    BotConfig(idx=36,
        name="bot_4h_cons_btc",   label="BOT\xb74H\xb7CONS\xb7BTC",
        interval="4h",             ma_type="sma",  ma_fast=100, ma_slow=200,
        trailing_pct=0.030,        min_vol_ratio=1.2, sr_near_pct=0.012,
        fibo_zone_pct=0.015,       candle_limit=300,  leverage=15.0,
        require_fib=False,         require_sr=False,
        sl_type="atr",             time_filter="asia",
        coins=["BTC"],
    ),
    BotConfig(idx=37,
        name="bot_4h_cons_eth",   label="BOT\xb74H\xb7CONS\xb7ETH",
        interval="4h",             ma_type="sma",  ma_fast=100, ma_slow=200,
        trailing_pct=0.030,        min_vol_ratio=1.2, sr_near_pct=0.012,
        fibo_zone_pct=0.015,       candle_limit=300,  leverage=15.0,
        require_fib=False,         require_sr=False,
        sl_type="atr",             time_filter="asia",
        coins=["ETH"],
    ),
    BotConfig(idx=38,
        name="bot_1h_cons_btc",   label="BOT\xb71H\xb7CONS\xb7BTC",
        interval="1h",             ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.010,        min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.012,       candle_limit=200,  leverage=15.0,
        require_fib=True,          require_sr=False,
        sl_type="fixed",           fixed_sl_pct=0.02,
        time_filter="asia",        coins=["BTC"],
    ),
    BotConfig(idx=39,
        name="bot_1h_cons_eth",   label="BOT\xb71H\xb7CONS\xb7ETH",
        interval="1h",             ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.010,        min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.012,       candle_limit=200,  leverage=15.0,
        require_fib=True,          require_sr=False,
        sl_type="fixed",           fixed_sl_pct=0.02,
        time_filter="asia",        coins=["ETH"],
    ),
    BotConfig(idx=40,
        name="bot_2h_cons_btc",   label="BOT\xb72H\xb7CONS\xb7BTC",
        interval="2h",             ma_type="sma",  ma_fast=100, ma_slow=200,
        trailing_pct=0.015,        min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.012,       candle_limit=250,  leverage=15.0,
        require_fib=False,         require_sr=False,
        sl_type="trailing",        time_filter="asia",
        coins=["BTC"],
    ),
    BotConfig(idx=41,
        name="bot_2h_cons_eth",   label="BOT\xb72H\xb7CONS\xb7ETH",
        interval="2h",             ma_type="sma",  ma_fast=100, ma_slow=200,
        trailing_pct=0.015,        min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.012,       candle_limit=250,  leverage=15.0,
        require_fib=False,         require_sr=False,
        sl_type="trailing",        time_filter="asia",
        coins=["ETH"],
    ),
    # ── Equilibrados BTC/ETH (idx 42-47) ───────────────────────────────────────
    BotConfig(idx=42,
        name="bot_2h_eq_btc",     label="BOT\xb72H\xb7EQ\xb7BTC",
        interval="2h",             ma_type="ema",  ma_fast=20,  ma_slow=50,
        trailing_pct=0.008,        min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.012,       candle_limit=250,  leverage=15.0,
        require_fib=False,         require_sr=False,
        sl_type="trailing",        time_filter="asia",
        liq_confirm=True,          coins=["BTC"],
    ),
    BotConfig(idx=43,
        name="bot_2h_eq_eth",     label="BOT\xb72H\xb7EQ\xb7ETH",
        interval="2h",             ma_type="ema",  ma_fast=20,  ma_slow=50,
        trailing_pct=0.008,        min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.012,       candle_limit=250,  leverage=15.0,
        require_fib=False,         require_sr=False,
        sl_type="trailing",        time_filter="asia",
        liq_confirm=True,          coins=["ETH"],
    ),
    BotConfig(idx=44,
        name="bot_30m_eq_btc",    label="BOT\xb730M\xb7EQ\xb7BTC",
        interval="30m",            ma_type="ema",  ma_fast=20,  ma_slow=50,
        trailing_pct=0.008,        min_vol_ratio=1.2, sr_near_pct=0.008,
        fibo_zone_pct=0.011,       candle_limit=250,  leverage=15.0,
        require_fib=False,         require_sr=False,
        sl_type="fixed",           fixed_sl_pct=0.01,
        time_filter="asia",        rsi_filter="rsi55",
        coins=["BTC"],
    ),
    BotConfig(idx=45,
        name="bot_30m_eq_eth",    label="BOT\xb730M\xb7EQ\xb7ETH",
        interval="30m",            ma_type="ema",  ma_fast=20,  ma_slow=50,
        trailing_pct=0.008,        min_vol_ratio=1.2, sr_near_pct=0.008,
        fibo_zone_pct=0.011,       candle_limit=250,  leverage=15.0,
        require_fib=False,         require_sr=False,
        sl_type="fixed",           fixed_sl_pct=0.01,
        time_filter="asia",        rsi_filter="rsi55",
        coins=["ETH"],
    ),
    BotConfig(idx=46,
        name="bot_1h_eq_btc",     label="BOT\xb71H\xb7EQ\xb7BTC",
        interval="1h",             ma_type="ema",  ma_fast=13,  ma_slow=34,
        trailing_pct=0.030,        min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.012,       candle_limit=200,  leverage=15.0,
        require_fib=False,         require_sr=False,
        sl_type="trailing",        time_filter="asia",
        rsi_filter="rsi50",        coins=["BTC"],
    ),
    BotConfig(idx=47,
        name="bot_1h_eq_eth",     label="BOT\xb71H\xb7EQ\xb7ETH",
        interval="1h",             ma_type="ema",  ma_fast=13,  ma_slow=34,
        trailing_pct=0.030,        min_vol_ratio=1.2, sr_near_pct=0.010,
        fibo_zone_pct=0.012,       candle_limit=200,  leverage=15.0,
        require_fib=False,         require_sr=False,
        sl_type="trailing",        time_filter="asia",
        rsi_filter="rsi50",        coins=["ETH"],
    ),
    # ── Alta Frecuencia BTC/ETH (idx 48-53) ────────────────────────────────────
    BotConfig(idx=48,
        name="bot_15m_hf_btc",    label="BOT\xb715M\xb7HF\xb7BTC",
        interval="15m",            ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.005,        min_vol_ratio=1.1, sr_near_pct=0.007,
        fibo_zone_pct=0.010,       candle_limit=300,  leverage=15.0,
        require_fib=True,          require_sr=False,
        sl_type="fixed",           fixed_sl_pct=0.01,
        time_filter="none",        coins=["BTC"],
    ),
    BotConfig(idx=49,
        name="bot_15m_hf_eth",    label="BOT\xb715M\xb7HF\xb7ETH",
        interval="15m",            ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.005,        min_vol_ratio=1.1, sr_near_pct=0.007,
        fibo_zone_pct=0.010,       candle_limit=300,  leverage=15.0,
        require_fib=True,          require_sr=False,
        sl_type="fixed",           fixed_sl_pct=0.01,
        time_filter="none",        coins=["ETH"],
    ),
    BotConfig(idx=50,
        name="bot_15m_hf2_btc",   label="BOT\xb715M\xb7HF2\xb7BTC",
        interval="15m",            ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.005,        min_vol_ratio=1.1, sr_near_pct=0.007,
        fibo_zone_pct=0.010,       candle_limit=300,  leverage=15.0,
        require_fib=True,          require_sr=False,
        sl_type="fixed",           fixed_sl_pct=0.01,
        time_filter="asia",        coins=["BTC"],
    ),
    BotConfig(idx=51,
        name="bot_15m_hf2_eth",   label="BOT\xb715M\xb7HF2\xb7ETH",
        interval="15m",            ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.005,        min_vol_ratio=1.1, sr_near_pct=0.007,
        fibo_zone_pct=0.010,       candle_limit=300,  leverage=15.0,
        require_fib=True,          require_sr=False,
        sl_type="fixed",           fixed_sl_pct=0.01,
        time_filter="asia",        coins=["ETH"],
    ),
    BotConfig(idx=52,
        name="bot_30m_hf_btc",    label="BOT\xb730M\xb7HF\xb7BTC",
        interval="30m",            ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.015,        min_vol_ratio=1.1, sr_near_pct=0.008,
        fibo_zone_pct=0.011,       candle_limit=250,  leverage=15.0,
        require_fib=False,         require_sr=False,
        sl_type="trailing",        time_filter="asia",
        rsi_filter="rsi50",        coins=["BTC"],
    ),
    BotConfig(idx=53,
        name="bot_30m_hf_eth",    label="BOT\xb730M\xb7HF\xb7ETH",
        interval="30m",            ma_type="sma",  ma_fast=50,  ma_slow=100,
        trailing_pct=0.015,        min_vol_ratio=1.1, sr_near_pct=0.008,
        fibo_zone_pct=0.011,       candle_limit=250,  leverage=15.0,
        require_fib=False,         require_sr=False,
        sl_type="trailing",        time_filter="asia",
        rsi_filter="rsi50",        coins=["ETH"],
    ),
]


# ─── CONFIGURACIÓN BOTS LIQUIDACIONES ─────────────────────────────────────────
@dataclass
class LiqConfig:
    idx:               int
    name:              str
    label:             str
    strategy:          str
    min_liq_usd:       float
    max_dist_pct:      float
    require_oi_grow:   bool  = False
    require_funding_ex: bool = False
    funding_thresh:    float = 0.001
    min_zones:         int   = 1
    trailing_pct:      float = 0.010
    leverage:          float = 5.0
    risk_per_trade:    float = 0.02
    coins:             list  = field(default_factory=lambda: ["BTC", "ETH", "SOL"])


LIQ_CONFIGS: list = [
    LiqConfig(idx=18, name="bot_liq_agresivo",        label="LIQ·AGRESIVO",
        strategy="agresivo",       min_liq_usd=5e6,   max_dist_pct=0.005),
    LiqConfig(idx=19, name="bot_liq_moderado",        label="LIQ·MODERADO",
        strategy="moderado",       min_liq_usd=10e6,  max_dist_pct=0.010,
        require_oi_grow=True),
    LiqConfig(idx=20, name="bot_liq_conservador",     label="LIQ·CONSERV",
        strategy="conservador",    min_liq_usd=50e6,  max_dist_pct=0.020,
        require_oi_grow=True, require_funding_ex=True, funding_thresh=0.0005),
    LiqConfig(idx=21, name="bot_liq_funding",         label="LIQ·FUNDING",
        strategy="funding",        min_liq_usd=1e6,   max_dist_pct=0.050,
        require_funding_ex=True,   funding_thresh=0.001),
    LiqConfig(idx=22, name="bot_liq_cascada",         label="LIQ·CASCADA",
        strategy="cascada",        min_liq_usd=5e6,   max_dist_pct=0.020,
        min_zones=3),
    LiqConfig(idx=23, name="bot_liq_oi_divergencia",  label="LIQ·OI·DIV",
        strategy="oi_div",         min_liq_usd=1e6,   max_dist_pct=0.050),
    LiqConfig(idx=24, name="bot_liq_whale",           label="LIQ·WHALE",
        strategy="whale",          min_liq_usd=100e6, max_dist_pct=0.030),
    LiqConfig(idx=25, name="bot_liq_contratendencia", label="LIQ·CONTRA",
        strategy="contra",         min_liq_usd=20e6,  max_dist_pct=0.050),
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


# ─── MARKET DATA (OI, Funding, Liquidaciones) ─────────────────────────────────
def _fetch_hl_market() -> dict:
    """Precio, funding y OI para todos los activos de Hyperliquid."""
    data = _hl_post({"type": "metaAndAssetCtxs"})
    meta, ctxs = data[0], data[1]
    result = {}
    for i, asset in enumerate(meta["universe"]):
        if i < len(ctxs):
            ctx   = ctxs[i]
            coin  = asset["name"]
            price = float(ctx.get("markPx", 0) or 0)
            oi    = float(ctx.get("openInterest", 0) or 0)
            result[coin] = {
                "price":   price,
                "funding": float(ctx.get("funding", 0) or 0),
                "oi":      oi,
                "oi_usd":  round(oi * price, 0),
                "vol_24h": float(ctx.get("dayNtlVlm", 0) or 0),
            }
    return result


def _synthetic_liq_zones(coin: str, price: float) -> list:
    """Zonas de liquidación sintéticas realistas cuando Coinglass no responde."""
    if price <= 0:
        price = {"BTC": 85000, "ETH": 2000, "SOL": 140}.get(coin, 500)
    scale = {"BTC": 3e8, "ETH": 3e7, "SOL": 8e6}.get(coin, 2e6)
    rng   = random.Random(int(price / 10) ^ hash(coin) & 0xFFFFFF)
    zones = []
    for lev in [5, 10, 20, 50, 100]:
        d = 1.0 / lev * 0.85
        for sign, ztype in [(-1, "long"), (1, "short")]:
            zp  = price * (1 + sign * d * rng.uniform(0.8, 1.2))
            amt = scale * rng.uniform(0.4, 2.5) / (lev / 10)
            zones.append({"price": round(zp, 4), "liq_usd": round(amt, 0),
                          "type": ztype, "dist_pct": 0.0})
    for _ in range(10):
        d = rng.uniform(-0.06, 0.06)
        zones.append({
            "price":   round(price * (1 + d), 4),
            "liq_usd": round(scale * rng.uniform(0.05, 0.4) / (abs(d) * 8 + 0.5), 0),
            "type":    "long" if d < 0 else "short",
            "dist_pct": 0.0,
        })
    if price > 0:
        for z in zones:
            z["dist_pct"] = round((z["price"] - price) / price * 100, 2)
    return sorted(zones, key=lambda z: z["price"])


def _try_coinglass_liq(coin: str) -> list:
    """Intenta obtener zonas de liquidación desde Coinglass."""
    try:
        url = f"https://open-api.coinglass.com/public/v2/liquidation_ex?symbol={coin}&interval=h8"
        r   = requests.get(url, timeout=8)
        if r.status_code == 200:
            d = r.json()
            if d.get("success") and d.get("data"):
                zones = []
                for side, ztype in [("longLiquidationData", "long"),
                                     ("shortLiquidationData", "short")]:
                    for item in (d["data"].get(side) or []):
                        px  = float(item.get("priceLevel", 0) or 0)
                        amt = float(item.get("cumSum", 0) or 0)
                        if px > 0 and amt > 1e5:
                            zones.append({"price": px, "liq_usd": round(amt, 0),
                                          "type": ztype, "dist_pct": 0.0})
                if zones:
                    return zones
    except Exception:
        pass
    return []


def _fetch_liq_zones(coin: str, price: float) -> list:
    zones = _try_coinglass_liq(coin) or _synthetic_liq_zones(coin, price)
    if price > 0:
        for z in zones:
            z["dist_pct"] = round((z["price"] - price) / price * 100, 2)
    return zones


def _update_market() -> None:
    """Hilo de fondo: actualiza market data cada MARKET_UPDATE_SEC segundos."""
    while True:
        try:
            coins = _fetch_hl_market()
            liq   = {c: _fetch_liq_zones(c, coins.get(c, {}).get("price", 0))
                     for c in LIQ_COINS}
            with _mkt_lock:
                prev_hist = dict(_mkt_cache.get("oi_hist", {}))
                for c, d in coins.items():
                    hist = list(prev_hist.get(c, []))
                    hist.append({"ts": time.time(), "oi": d["oi"], "price": d["price"]})
                    prev_hist[c] = hist[-MAX_OI_HISTORY:]
                _mkt_cache["coins"]   = coins
                _mkt_cache["liq"]     = liq
                _mkt_cache["oi_hist"] = prev_hist
                _mkt_cache["ts"]      = time.time()
        except Exception as e:
            print(f"[market] Error actualizando: {e}")
        time.sleep(MARKET_UPDATE_SEC)


def get_market_state() -> dict:
    """Devuelve datos de mercado para el endpoint /api/market."""
    with _mkt_lock:
        coins   = dict(_mkt_cache.get("coins", {}))
        liq     = dict(_mkt_cache.get("liq", {}))
        oi_hist = dict(_mkt_cache.get("oi_hist", {}))
        ts      = _mkt_cache.get("ts", 0)

    oi_table = []
    for coin in OI_COINS:
        d        = coins.get(coin, {})
        price    = d.get("price", 0)
        funding  = d.get("funding", 0)
        oi_usd   = d.get("oi_usd", 0)
        hist     = oi_hist.get(coin, [])
        prev_oi_usd = hist[-2]["oi"] * hist[-2]["price"] if len(hist) >= 2 else 0
        oi_chg   = round((oi_usd - prev_oi_usd) / prev_oi_usd * 100, 2) if prev_oi_usd > 0 else 0
        ls_ratio = round(max(0.1, 1.0 + funding * 400), 2)
        oi_table.append({
            "coin":     coin,
            "price":    price,
            "funding":  round(funding * 100, 5),
            "oi_usd":   oi_usd,
            "oi_chg":   oi_chg,
            "vol_24h":  d.get("vol_24h", 0),
            "ls_ratio": ls_ratio,
        })

    liq_display = {}
    for coin in LIQ_COINS:
        zones = liq.get(coin, [])
        price = coins.get(coin, {}).get("price", 0)
        liq_display[coin] = {"price": price, "zones": zones[:30]}

    updated = datetime.fromtimestamp(ts).strftime("%H:%M:%S") if ts else "—"
    return {"oi_table": oi_table, "liq": liq_display, "updated_at": updated}


# ─── INDICADORES ──────────────────────────────────────────────────────────────
def add_mas(df: pd.DataFrame, cfg: BotConfig) -> pd.DataFrame:
    if cfg.ma_type == "ema":
        df["ma_fast"] = df["close"].ewm(span=cfg.ma_fast, adjust=False).mean()
        df["ma_slow"] = df["close"].ewm(span=cfg.ma_slow, adjust=False).mean()
    else:
        df["ma_fast"] = df["close"].rolling(cfg.ma_fast).mean()
        df["ma_slow"] = df["close"].rolling(cfg.ma_slow).mean()
    if cfg.ema200_filter != "none":
        df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()
    if cfg.atr_filter != "none" or cfg.sl_type == "atr":
        hl = df["high"] - df["low"]
        hc = (df["high"] - df["close"].shift()).abs()
        lc = (df["low"]  - df["close"].shift()).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        df["atr"]    = tr.ewm(com=13, adjust=False).mean()
        df["atr_ma"] = df["atr"].rolling(20).mean()
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

    def open(self, coin: str, direction: str, price: float, cfg: BotConfig,
             trailing_pct_override: Optional[float] = None) -> bool:
        with self._lock:
            if coin in self.positions or price <= 0:
                return False
            notional = self.equity * 0.95 * cfg.risk_per_trade * cfg.leverage
            size = round(notional / price, 6)
            tp = trailing_pct_override if trailing_pct_override is not None else cfg.trailing_pct
            self.positions[coin] = VirtualPos(
                coin=coin, direction=direction, size=size,
                entry_price=price, current_price=price,
                trailing_stop=TrailingStop(price, direction, tp),
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

        # Stop loss según tipo configurado
        if cfg.sl_type == "fixed":
            sl = pos.entry_price * (1 - cfg.fixed_sl_pct) if d == "long" \
                 else pos.entry_price * (1 + cfg.fixed_sl_pct)
            if (d == "long" and price <= sl) or (d == "short" and price >= sl):
                return f"Fixed SL {cfg.fixed_sl_pct*100:.0f}% ({sl:.4f})"
        else:
            # trailing o atr — ambos usan TrailingStop
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

        # ── Filtro de sesión horaria (aplica a todo el ciclo) ─────────────────
        hour_utc = datetime.utcnow().hour
        if cfg.time_filter == "london_ny" and hour_utc not in _LONDON_NY:
            self.status = "esperando"
            return
        if cfg.time_filter == "asia" and hour_utc not in _ASIA:
            self.status = "esperando"
            return

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

                entry   = float(df["close"].iloc[-2])
                rsi_val = float(df["rsi"].iloc[-2])

                # ── Filtro RSI de entrada ─────────────────────────────────────
                if cfg.rsi_filter == "rsi50":
                    if signal == "long"  and rsi_val <= 50: continue
                    if signal == "short" and rsi_val >= 50: continue
                elif cfg.rsi_filter == "rsi55":
                    if signal == "long"  and rsi_val <= 55: continue
                    if signal == "short" and rsi_val >= 45: continue

                # ── Filtro EMA200 ─────────────────────────────────────────────
                if cfg.ema200_filter == "strict" and "ema200" in df.columns:
                    ema200 = float(df["ema200"].iloc[-2])
                    if signal == "long"  and entry < ema200: continue
                    if signal == "short" and entry > ema200: continue

                # ── Filtro ATR (solo si ATR alto) ─────────────────────────────
                if cfg.atr_filter == "max" and "atr" in df.columns:
                    atr_now = float(df["atr"].iloc[-2])
                    atr_avg = float(df["atr_ma"].iloc[-2]) if not pd.isna(df["atr_ma"].iloc[-2]) else atr_now
                    if atr_now <= atr_avg:
                        continue

                # ── Confirmación de liquidaciones ─────────────────────────────
                if cfg.liq_confirm:
                    with _mkt_lock:
                        liq_zones = list(_mkt_cache.get("liq", {}).get(coin, []))
                    near = any(abs((z["price"] - entry) / entry) <= 0.025
                               for z in liq_zones if z.get("liq_usd", 0) >= 5e6)
                    if not near:
                        self._log_signal(coin, signal.upper(), "DESCARTADO", "sin zona liq cercana")
                        continue

                highs, lows = find_pivots(df)
                ok_sr  = near_sr(entry, highs, lows, cfg.sr_near_pct) if cfg.require_sr  else True
                ok_fib = near_fibo(entry, calc_fibo(df), cfg.fibo_zone_pct) if cfg.require_fib else True
                ok_vol = volume_ok(df, cfg.min_vol_ratio)

                if not (ok_sr and ok_fib and ok_vol):
                    why = []
                    if not ok_sr:  why.append("sin S/R")
                    if not ok_fib: why.append("sin Fib")
                    if not ok_vol: why.append("vol bajo")
                    self._log_signal(coin, signal.upper(), "DESCARTADO", ", ".join(why))
                    continue

                # Para ATR stop: calcular trailing_pct dinámico desde ATR al momento de entrada
                tp_override = None
                if cfg.sl_type == "atr" and "atr" in df.columns:
                    atr_val    = float(df["atr"].iloc[-2])
                    tp_override = min(max(atr_val * 2.0 / entry, 0.005), 0.05)

                opened = self.portfolio.open(coin, signal, entry, cfg,
                                             trailing_pct_override=tp_override)
                if opened:
                    sl_info = f" | SL-ATR {tp_override*100:.1f}%" if tp_override else ""
                    self._log_signal(coin, signal.upper(),
                                     f"ENTRADA @ {entry:,.4f}{sl_info}")

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
            "idx":          self.cfg.idx,
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


# ─── BOT DE LIQUIDACIONES ─────────────────────────────────────────────────────
class LiqBot:
    def __init__(self, cfg: LiqConfig):
        self.cfg       = cfg
        self.portfolio = VirtualPortfolio()
        self.last_scan = "—"
        self.status    = "iniciando"
        self.errors    = 0
        self.signals:  list[dict] = []
        self._sig_lock = threading.Lock()

    def _log_signal(self, coin: str, sig_type: str, action: str, reason: str = "") -> None:
        entry = {
            "time": datetime.now().strftime("%H:%M:%S"), "ts": time.time(),
            "coin": coin, "type": sig_type, "action": action, "reason": reason,
        }
        with self._sig_lock:
            self.signals.insert(0, entry)
            if len(self.signals) > MAX_SIGNALS:
                self.signals.pop()

    # ── Filtros auxiliares ───────────────────────────────────────────────────
    def _oi_growing(self, coin: str, cur_oi: float, oi_hist: dict) -> bool:
        h = oi_hist.get(coin, [])
        return len(h) < 2 or cur_oi >= h[-2]["oi"] * 0.99

    # ── Lógica de señal por estrategia ──────────────────────────────────────
    def _get_signal(self, coin: str, coins: dict, liq: dict, oi_hist: dict) -> Optional[str]:
        cfg     = self.cfg
        price   = coins.get(coin, {}).get("price", 0)
        if price <= 0:
            return None
        zones   = liq.get(coin, [])
        funding = coins.get(coin, {}).get("funding", 0)
        oi      = coins.get(coin, {}).get("oi", 0)
        sig     = None

        if cfg.strategy in ("agresivo", "moderado", "conservador", "whale"):
            # Busca la zona con mayor liquidación dentro de la distancia máxima
            candidates = sorted(
                [z for z in zones if z["liq_usd"] >= cfg.min_liq_usd
                 and abs((z["price"] - price) / price) <= cfg.max_dist_pct],
                key=lambda z: z["liq_usd"], reverse=True,
            )
            if candidates:
                sig = "short" if candidates[0]["price"] < price else "long"

        elif cfg.strategy == "funding":
            if abs(funding) >= cfg.funding_thresh:
                sig = "short" if funding > 0 else "long"

        elif cfg.strategy == "cascada":
            close = [z for z in zones
                     if z["liq_usd"] >= cfg.min_liq_usd
                     and abs((z["price"] - price) / price) <= cfg.max_dist_pct]
            if len(close) >= cfg.min_zones:
                longs_below  = sum(1 for z in close if z["price"] < price)
                shorts_above = len(close) - longs_below
                if longs_below > shorts_above:
                    sig = "short"
                elif shorts_above > longs_below:
                    sig = "long"

        elif cfg.strategy == "oi_div":
            h = oi_hist.get(coin, [])
            if len(h) >= 4:
                prev      = h[-4]
                dprice    = (price - prev["price"]) / prev["price"] if prev["price"] > 0 else 0
                doi       = (oi - prev["oi"]) / prev["oi"] if prev["oi"] > 0 else 0
                if dprice > 0.005 and doi < -0.01:
                    sig = "short"
                elif dprice < -0.005 and doi > 0.01:
                    sig = "long"

        elif cfg.strategy == "contra":
            h = oi_hist.get(coin, [])
            if len(h) >= 2:
                move = (price - h[-2]["price"]) / h[-2]["price"] if h[-2]["price"] > 0 else 0
                has_big = any(z["liq_usd"] >= cfg.min_liq_usd for z in zones)
                if has_big:
                    if move < -0.015:
                        sig = "long"
                    elif move > 0.015:
                        sig = "short"

        if sig is None:
            return None
        if cfg.require_oi_grow and not self._oi_growing(coin, oi, oi_hist):
            return None
        if cfg.require_funding_ex and abs(funding) < cfg.funding_thresh:
            return None
        return sig

    # ── Condición de salida ──────────────────────────────────────────────────
    def _check_exit(self, coin: str, price: float) -> Optional[str]:
        pos = self.portfolio.positions.get(coin)
        if not pos:
            return None
        if pos.trailing_stop.triggered(price):
            return f"Trailing Stop ({pos.trailing_stop.stop:.4f})"
        move = (price - pos.entry_price) / pos.entry_price
        if pos.direction == "long"  and move < -0.04:
            return "Stop adverso -4%"
        if pos.direction == "short" and move > 0.04:
            return "Stop adverso +4%"
        return None

    # ── Ciclo principal ──────────────────────────────────────────────────────
    def run_cycle(self) -> None:
        self.last_scan = datetime.now().strftime("%H:%M:%S")
        self.status    = "escaneando"
        with _mkt_lock:
            coins   = dict(_mkt_cache.get("coins", {}))
            liq     = dict(_mkt_cache.get("liq", {}))
            oi_hist = dict(_mkt_cache.get("oi_hist", {}))
        if not coins:
            self.status = "esperando datos"
            return
        for coin in self.cfg.coins:
            try:
                price = coins.get(coin, {}).get("price", 0)
                if price <= 0:
                    continue
                if coin in self.portfolio.positions:
                    self.portfolio.update(coin, price)
                    reason = self._check_exit(coin, price)
                    if reason:
                        pnl = self.portfolio.close(coin, reason)
                        if pnl is not None:
                            self._log_signal(coin, "CIERRE", f"PnL {pnl:+.2f}$", reason)
                    continue
                signal = self._get_signal(coin, coins, liq, oi_hist)
                if not signal:
                    continue
                opened = self.portfolio.open(coin, signal, price, self.cfg)
                if opened:
                    self._log_signal(coin, signal.upper(), f"ENTRADA @ {price:,.4f}")
            except Exception as e:
                self.errors += 1
                print(f"[{self.cfg.label}] Error {coin}: {e}")
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
            "idx":          self.cfg.idx,
            "label":        self.cfg.label,
            "interval":     "liq",
            "ma_type":      "liq",
            "ma_fast":      0,
            "ma_slow":      0,
            "trailing_pct": self.cfg.trailing_pct,
            "last_scan":    self.last_scan,
            "status":       self.status,
            "errors":       self.errors,
            "coins":        self.cfg.coins,
            "portfolio":    self.portfolio.to_dict(),
            "signals":      sigs,
            "strategy":     self.cfg.strategy,
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

    # ── Hilo de market data (OI, funding, liquidaciones) ────────────────────
    tm = threading.Thread(target=_update_market, name="market_updater", daemon=True)
    tm.start()
    print("[sim_engine] Market updater arrancado")

    # ── Bots EMA ─────────────────────────────────────────────────────────────
    print("[sim_engine] Obteniendo top coins...")
    try:
        coins = get_top_coins()
    except Exception as e:
        print(f"[sim_engine] Error obteniendo coins: {e}. Usando fallback.")
        coins = FALLBACK_COINS[:]

    print(f"[sim_engine] Coins: {', '.join(coins)}")
    ema_bots = [SimBot(cfg, cfg.coins if cfg.coins else coins) for cfg in CONFIGS]
    liq_bots = [LiqBot(cfg) for cfg in LIQ_CONFIGS]
    _bots    = ema_bots + liq_bots

    for i, bot in enumerate(_bots):
        delay = i * 8

        def _run(b=bot, d=delay):
            if d:
                time.sleep(d)
            b.run()

        t = threading.Thread(target=_run, name=bot.cfg.label, daemon=True)
        t.start()
        print(f"[sim_engine] {bot.cfg.label} arrancado (delay {delay}s)")
