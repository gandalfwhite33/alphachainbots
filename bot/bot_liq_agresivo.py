#!/usr/bin/env python3
"""
AlphaChainBots — Bot de Liquidaciones AGRESIVO
Entra cuando hay zona de liquidaciones >$5M a distancia 0.5% del precio actual.
Sin filtros extra. Trailing stop 1%. x5 leverage.
"""
import os, time, logging
from datetime import datetime
from typing import Optional
import requests
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
MIN_LIQ_USD     = 5_000_000
MAX_DIST_PCT    = 0.005
LOOP_SLEEP      = 300
_HL_URL         = "https://api.hyperliquid.xyz/info"


def setup_client():
    key      = PRIVATE_KEY.strip().removeprefix("0x").removeprefix("0X")
    account  = Account.from_key(key)
    address  = ACCOUNT_ADDRESS or account.address
    api_url  = TESTNET_URL if TESTNET else MAINNET_URL
    info     = HLInfo(api_url)
    exchange = HLExchange(account, api_url, account_address=address)
    return info, exchange, address


def get_price(coin: str) -> float:
    try:
        r = requests.post(_HL_URL, json={"type": "metaAndAssetCtxs"}, timeout=10)
        meta, ctxs = r.json()[0], r.json()[1]
        for i, a in enumerate(meta["universe"]):
            if a["name"] == coin and i < len(ctxs):
                return float(ctxs[i].get("markPx", 0) or 0)
    except Exception:
        pass
    return 0.0


def get_liq_zones(coin: str, price: float) -> list:
    try:
        url = f"https://open-api.coinglass.com/public/v2/liquidation_ex?symbol={coin}&interval=h8"
        r   = requests.get(url, timeout=8)
        if r.status_code == 200:
            d = r.json()
            if d.get("success") and d.get("data"):
                zones = []
                for side, zt in [("longLiquidationData","long"),("shortLiquidationData","short")]:
                    for item in (d["data"].get(side) or []):
                        px = float(item.get("priceLevel",0) or 0)
                        amt = float(item.get("cumSum",0) or 0)
                        if px > 0 and amt > 1e5:
                            zones.append({"price":px,"liq_usd":amt,"type":zt})
                if zones:
                    return zones
    except Exception:
        pass
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


def find_entry(zones: list, price: float) -> Optional[str]:
    for z in sorted(zones, key=lambda z: z["liq_usd"], reverse=True):
        if z["liq_usd"] < MIN_LIQ_USD:
            continue
        if abs((z["price"] - price) / price) <= MAX_DIST_PCT:
            return "short" if z["price"] < price else "long"
    return None


class TrailingStop:
    def __init__(self, entry, direction):
        self.direction = direction
        self.pct  = TRAILING_PCT
        self.best = entry
        self.stop = entry*(1-TRAILING_PCT) if direction=="long" else entry*(1+TRAILING_PCT)
    def update(self, price):
        if self.direction=="long" and price>self.best:
            self.best=price; self.stop=self.best*(1-self.pct)
        elif self.direction=="short" and price<self.best:
            self.best=price; self.stop=self.best*(1+self.pct)
    def triggered(self, price):
        return price<=self.stop if self.direction=="long" else price>=self.stop


class Bot:
    def __init__(self):
        self.info, self.exchange, self.address = setup_client()
        self.positions = {}

    def get_equity(self):
        state = self.info.user_state(self.address)
        return float(state.get("marginSummary",{}).get("accountValue",0))

    def run(self):
        log.info("="*60)
        log.info(" AlphaChainBots LIQ·AGRESIVO — zona >$5M dist 0.5%")
        log.info("="*60)
        while True:
            try:
                equity = self.get_equity()
                for coin in COINS:
                    price = get_price(coin)
                    if price <= 0: continue
                    if coin in self.positions:
                        pos = self.positions[coin]
                        pos["ts"].update(price)
                        if pos["ts"].triggered(price):
                            log.info(f"[{coin}] CIERRE trailing stop")
                            del self.positions[coin]
                        continue
                    zones = get_liq_zones(coin, price)
                    signal = find_entry(zones, price)
                    if not signal: continue
                    notional = equity*RISK_PER_TRADE*LEVERAGE
                    size = round(notional/price, 4)
                    if size <= 0: continue
                    is_buy = signal=="long"
                    slip   = 0.0015
                    px     = round(price*(1+slip) if is_buy else price*(1-slip), 2)
                    try:
                        self.exchange.update_leverage(LEVERAGE, coin, is_cross=False)
                        self.exchange.order(coin, is_buy, size, px,
                                            {"limit":{"tif":"Ioc"}}, reduce_only=False)
                        log.info(f"[{coin}] ENTRADA {signal.upper()} size={size} px={px}")
                        self.positions[coin] = {"direction":signal,"ts":TrailingStop(price,signal)}
                    except Exception as e:
                        log.error(f"[{coin}] Error orden: {e}")
                log.info(f"Posiciones: {list(self.positions.keys()) or 'ninguna'}")
                time.sleep(LOOP_SLEEP)
            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"Error: {e}"); time.sleep(60)


if __name__ == "__main__":
    Bot().run()
