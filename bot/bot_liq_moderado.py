#!/usr/bin/env python3
"""
AlphaChainBots — Bot de Liquidaciones MODERADO
Zona >$10M a distancia 1% + confirmación Open Interest creciente.
Trailing stop 1%. x5 leverage.
"""
import os, time, logging, requests
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from eth_account import Account
from hl_client import HLInfo, HLExchange, MAINNET_URL, TESTNET_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
load_dotenv()

TESTNET         = os.getenv("TESTNET", "true").lower() == "true"
PRIVATE_KEY     = os.getenv("PRIVATE_KEY", "")
ACCOUNT_ADDRESS = os.getenv("ACCOUNT_ADDRESS", "")

COINS           = ["BTC", "ETH", "SOL"]
LEVERAGE        = 5
RISK_PER_TRADE  = 0.02
TRAILING_PCT    = 0.010
MIN_LIQ_USD     = 10_000_000
MAX_DIST_PCT    = 0.010
LOOP_SLEEP      = 300
_HL_URL         = "https://api.hyperliquid.xyz/info"
_prev_oi        = {}


def setup_client():
    key     = PRIVATE_KEY.strip().removeprefix("0x").removeprefix("0X")
    account = Account.from_key(key)
    address = ACCOUNT_ADDRESS or account.address
    api_url = TESTNET_URL if TESTNET else MAINNET_URL
    return HLInfo(api_url), HLExchange(account, api_url, account_address=address), address


def get_hl_data(coin: str):
    try:
        r = requests.post(_HL_URL, json={"type":"metaAndAssetCtxs"}, timeout=10)
        meta, ctxs = r.json()[0], r.json()[1]
        for i, a in enumerate(meta["universe"]):
            if a["name"] == coin and i < len(ctxs):
                ctx = ctxs[i]
                return float(ctx.get("markPx",0) or 0), float(ctx.get("openInterest",0) or 0)
    except Exception:
        pass
    return 0.0, 0.0


def get_liq_zones(coin, price):
    import random
    rng = random.Random(int(price/10))
    scale = {"BTC":3e8,"ETH":3e7,"SOL":8e6}.get(coin,2e6)
    zones = []
    for lev in [5,10,20,50]:
        d = 1.0/lev*0.85
        for sign,zt in [(-1,"long"),(1,"short")]:
            zones.append({"price":round(price*(1+sign*d*rng.uniform(0.8,1.2)),4),
                          "liq_usd":round(scale*rng.uniform(0.5,2.5)/(lev/10),0),"type":zt})
    return zones


class TrailingStop:
    def __init__(self, entry, direction):
        self.direction=direction; self.pct=TRAILING_PCT; self.best=entry
        self.stop=entry*(1-TRAILING_PCT) if direction=="long" else entry*(1+TRAILING_PCT)
    def update(self, p):
        if self.direction=="long" and p>self.best: self.best=p; self.stop=self.best*(1-self.pct)
        elif self.direction=="short" and p<self.best: self.best=p; self.stop=self.best*(1+self.pct)
    def triggered(self, p): return p<=self.stop if self.direction=="long" else p>=self.stop


class Bot:
    def __init__(self):
        self.info, self.exchange, self.address = setup_client()
        self.positions = {}

    def get_equity(self):
        return float(self.info.user_state(self.address).get("marginSummary",{}).get("accountValue",0))

    def run(self):
        log.info(" AlphaChainBots LIQ·MODERADO — zona >$10M dist 1% + OI creciente")
        while True:
            try:
                equity = self.get_equity()
                for coin in COINS:
                    price, oi = get_hl_data(coin)
                    if price <= 0: continue
                    if coin in self.positions:
                        pos = self.positions[coin]
                        pos["ts"].update(price)
                        if pos["ts"].triggered(price):
                            log.info(f"[{coin}] CIERRE trailing stop"); del self.positions[coin]
                        continue
                    oi_growing = oi >= _prev_oi.get(coin, oi) * 0.99
                    _prev_oi[coin] = oi
                    if not oi_growing: continue
                    zones = get_liq_zones(coin, price)
                    sig = None
                    for z in sorted(zones, key=lambda z: z["liq_usd"], reverse=True):
                        if z["liq_usd"]<MIN_LIQ_USD: continue
                        if abs((z["price"]-price)/price)<=MAX_DIST_PCT:
                            sig = "short" if z["price"]<price else "long"; break
                    if not sig: continue
                    notional=equity*RISK_PER_TRADE*LEVERAGE; size=round(notional/price,4)
                    if size<=0: continue
                    is_buy=sig=="long"; px=round(price*(1.0015) if is_buy else price*(0.9985),2)
                    try:
                        self.exchange.update_leverage(LEVERAGE,coin,is_cross=False)
                        self.exchange.order(coin,is_buy,size,px,{"limit":{"tif":"Ioc"}},reduce_only=False)
                        log.info(f"[{coin}] ENTRADA {sig.upper()} size={size}")
                        self.positions[coin]={"direction":sig,"ts":TrailingStop(price,sig)}
                    except Exception as e: log.error(f"[{coin}] Error: {e}")
                time.sleep(LOOP_SLEEP)
            except KeyboardInterrupt: break
            except Exception as e: log.error(f"Error: {e}"); time.sleep(60)


if __name__ == "__main__":
    Bot().run()
