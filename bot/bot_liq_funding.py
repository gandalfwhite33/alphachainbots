#!/usr/bin/env python3
"""
AlphaChainBots — Bot de Liquidaciones FUNDING
Opera cuando funding rate en extremos históricos (>0.1% o <-0.1%).
Funding positivo extremo → SHORT (mercado sobrecomprado).
Funding negativo extremo → LONG (mercado sobrevendido).
Trailing stop 1%. x5 leverage.
"""
import os, time, logging, requests
from dotenv import load_dotenv
from eth_account import Account
from hl_client import HLInfo, HLExchange, MAINNET_URL, TESTNET_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
load_dotenv()

TESTNET         = os.getenv("TESTNET", "true").lower() == "true"
PRIVATE_KEY     = os.getenv("PRIVATE_KEY", "")
ACCOUNT_ADDRESS = os.getenv("ACCOUNT_ADDRESS", "")

COINS           = ["BTC", "ETH", "SOL", "XRP", "AVAX", "BNB", "DOGE"]
LEVERAGE        = 5
RISK_PER_TRADE  = 0.02
TRAILING_PCT    = 0.010
FUNDING_THRESH  = 0.001      # 0.1%
LOOP_SLEEP      = 300
_HL_URL         = "https://api.hyperliquid.xyz/info"


def setup_client():
    key=PRIVATE_KEY.strip().removeprefix("0x").removeprefix("0X"); account=Account.from_key(key)
    address=ACCOUNT_ADDRESS or account.address; api_url=TESTNET_URL if TESTNET else MAINNET_URL
    return HLInfo(api_url),HLExchange(account,api_url,account_address=address),address


def get_all_market():
    try:
        r=requests.post(_HL_URL,json={"type":"metaAndAssetCtxs"},timeout=10); d=r.json()
        meta,ctxs=d[0],d[1]; result={}
        for i,a in enumerate(meta["universe"]):
            if i<len(ctxs):
                ctx=ctxs[i]; coin=a["name"]
                result[coin]={"price":float(ctx.get("markPx",0) or 0),"funding":float(ctx.get("funding",0) or 0)}
        return result
    except Exception: return {}


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
        log.info(" AlphaChainBots LIQ·FUNDING — extremos >0.1% o <-0.1%")
        while True:
            try:
                equity=self.get_equity(); mkt=get_all_market()
                for coin in COINS:
                    d=mkt.get(coin,{}); price=d.get("price",0); funding=d.get("funding",0)
                    if price<=0: continue
                    if coin in self.positions:
                        pos=self.positions[coin]; pos["ts"].update(price)
                        if pos["ts"].triggered(price): log.info(f"[{coin}] CIERRE"); del self.positions[coin]
                        continue
                    if abs(funding)<FUNDING_THRESH: continue
                    sig="short" if funding>0 else "long"
                    size=round(equity*RISK_PER_TRADE*LEVERAGE/price,4)
                    if size<=0: continue
                    is_buy=sig=="long"; px=round(price*(1.0015) if is_buy else price*(0.9985),2)
                    try:
                        self.exchange.update_leverage(LEVERAGE,coin,is_cross=False)
                        self.exchange.order(coin,is_buy,size,px,{"limit":{"tif":"Ioc"}},reduce_only=False)
                        log.info(f"[{coin}] ENTRADA {sig.upper()} funding={funding*100:.4f}%")
                        self.positions[coin]={"direction":sig,"ts":TrailingStop(price,sig)}
                    except Exception as e: log.error(f"[{coin}] Error: {e}")
                time.sleep(LOOP_SLEEP)
            except KeyboardInterrupt: break
            except Exception as e: log.error(f"Error: {e}"); time.sleep(60)


if __name__=="__main__": Bot().run()
