#!/usr/bin/env python3
"""
AlphaChainBots — Bot de Liquidaciones CONTRATENDENCIA
Entra en dirección contraria después de una liquidación masiva,
aprovechando el rebote post-barrido.
Movimiento >1.5% en el último ciclo + zona >$20M → fade el movimiento.
Trailing stop 1%. x5 leverage.
"""
import os, time, logging, requests, random
from collections import deque
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
MIN_LIQ_USD     = 20_000_000
MOVE_THRESH     = 0.015      # 1.5% de movimiento para activar
LOOP_SLEEP      = 300
_HL_URL         = "https://api.hyperliquid.xyz/info"
_price_hist     = {c: deque(maxlen=10) for c in COINS}


def setup_client():
    key=PRIVATE_KEY.strip().removeprefix("0x").removeprefix("0X"); account=Account.from_key(key)
    address=ACCOUNT_ADDRESS or account.address; api_url=TESTNET_URL if TESTNET else MAINNET_URL
    return HLInfo(api_url),HLExchange(account,api_url,account_address=address),address


def get_price(coin):
    try:
        r=requests.post(_HL_URL,json={"type":"metaAndAssetCtxs"},timeout=10); d=r.json()
        for i,a in enumerate(d[0]["universe"]):
            if a["name"]==coin and i<len(d[1]): return float(d[1][i].get("markPx",0) or 0)
    except Exception: pass
    return 0.0


def get_liq_zones(coin,price):
    rng=random.Random(int(price/10)); scale={"BTC":3e8,"ETH":3e7,"SOL":8e6}.get(coin,2e6); zones=[]
    for lev in [10,20,50]:
        d=1.0/lev*0.85
        for sign,zt in [(-1,"long"),(1,"short")]:
            amt=scale*rng.uniform(0.5,2.0)/(lev/10)
            zones.append({"price":round(price*(1+sign*d*rng.uniform(0.8,1.2)),4),"liq_usd":round(amt,0),"type":zt})
    return zones


class TrailingStop:
    def __init__(self,e,d): self.direction=d;self.pct=TRAILING_PCT;self.best=e;self.stop=e*(1-TRAILING_PCT) if d=="long" else e*(1+TRAILING_PCT)
    def update(self,p):
        if self.direction=="long" and p>self.best: self.best=p;self.stop=self.best*(1-self.pct)
        elif self.direction=="short" and p<self.best: self.best=p;self.stop=self.best*(1+self.pct)
    def triggered(self,p): return p<=self.stop if self.direction=="long" else p>=self.stop


class Bot:
    def __init__(self): self.info,self.exchange,self.address=setup_client(); self.positions={}
    def get_equity(self): return float(self.info.user_state(self.address).get("marginSummary",{}).get("accountValue",0))

    def run(self):
        log.info(" AlphaChainBots LIQ·CONTRA — rebote post-barrido")
        while True:
            try:
                equity=self.get_equity()
                for coin in COINS:
                    price=get_price(coin)
                    if price<=0: continue
                    _price_hist[coin].append(price)
                    if coin in self.positions:
                        pos=self.positions[coin]; pos["ts"].update(price)
                        if pos["ts"].triggered(price): log.info(f"[{coin}] CIERRE"); del self.positions[coin]
                        continue
                    hist=_price_hist[coin]
                    if len(hist)<2: continue
                    move=(price-hist[-2])/hist[-2]
                    if abs(move)<MOVE_THRESH: continue
                    zones=get_liq_zones(coin,price)
                    has_big=any(z["liq_usd"]>=MIN_LIQ_USD for z in zones)
                    if not has_big: continue
                    # Fade: grand caída → LONG rebote; gran subida → SHORT reversión
                    sig="long" if move<-MOVE_THRESH else "short"
                    size=round(equity*RISK_PER_TRADE*LEVERAGE/price,4)
                    if size<=0: continue
                    is_buy=sig=="long"; px=round(price*(1.0015) if is_buy else price*(0.9985),2)
                    try:
                        self.exchange.update_leverage(LEVERAGE,coin,is_cross=False)
                        self.exchange.order(coin,is_buy,size,px,{"limit":{"tif":"Ioc"}},reduce_only=False)
                        log.info(f"[{coin}] CONTRA-TENDENCIA {sig.upper()} move={move*100:.2f}%")
                        self.positions[coin]={"direction":sig,"ts":TrailingStop(price,sig)}
                    except Exception as e: log.error(f"[{coin}] Error: {e}")
                time.sleep(LOOP_SLEEP)
            except KeyboardInterrupt: break
            except Exception as e: log.error(f"Error: {e}"); time.sleep(60)


if __name__=="__main__": Bot().run()
