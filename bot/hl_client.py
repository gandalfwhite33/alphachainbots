"""
hl_client.py — Hyperliquid REST client completo sin SDK.

Implementa:
  HLInfo     — datos de mercado (velas, equity, posiciones)
  HLExchange — órdenes firmadas con EIP-712 (sin hyperliquid SDK)

Dependencias: requests, eth-account, eth-utils, msgpack
"""
import time
import msgpack
import requests
from eth_account import Account
from eth_account.messages import SignableMessage
from eth_utils import keccak

MAINNET_URL = "https://api.hyperliquid.xyz"
TESTNET_URL = "https://api.hyperliquid-testnet.xyz"

_MAX_DEC = 6   # decimales máximos para precios y tamaños

# ─── EIP-712: hashes constantes pre-calculados ────────────────────────────────
# keccak256 de las type strings (nunca cambian)
_DOMAIN_TYPEHASH = keccak(primitive=(
    b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
))
_AGENT_TYPEHASH = keccak(primitive=b"Agent(string source,bytes32 connectionId)")

# Domain separator de Hyperliquid: chainId=1337, verifyingContract=0x000...0
# keccak256(domainTypeHash + keccak(name) + keccak(version) + chainId + address)
_DOMAIN_SEP = keccak(primitive=(
    _DOMAIN_TYPEHASH
    + keccak(primitive=b"Exchange")   # name  (string → keccak256)
    + keccak(primitive=b"1")          # version (string → keccak256)
    + (1337).to_bytes(32, "big")      # chainId (uint256)
    + bytes(32)                        # verifyingContract = address(0)
))


# ─── HELPERS DE FIRMA ─────────────────────────────────────────────────────────

def _float_to_wire(x: float) -> str:
    """Convierte float al formato wire de Hyperliquid (máximo 6 decimales)."""
    rounded = round(x, _MAX_DEC)
    if rounded == 0:
        return "0"
    s = f"{rounded:.{_MAX_DEC}f}"
    return s.rstrip("0").rstrip(".")


def _sign_action(account, action: dict, nonce: int, is_mainnet: bool) -> dict:
    """
    Firma un L1 action de Hyperliquid con EIP-712 sin usar
    eth_account.structured_data.

    Esquema:  Agent { source: string, connectionId: bytes32 }

    Pasos:
      1. connectionId = keccak256(msgpack(action) + 20_zero_bytes + nonce_be8)
      2. structHash   = keccak256(agentTypeHash + keccak(source) + connectionId)
      3. SignableMessage(version=b'\\x01', header=domainSep, body=structHash)
         → eth_account internamente calcula keccak256(\\x19\\x01 + header + body)
         que es exactamente el hash final de EIP-712.
    """
    # 1. connectionId
    packed = msgpack.packb(action, use_bin_type=True)
    cid    = keccak(primitive=packed + bytes(20) + nonce.to_bytes(8, "big"))

    # 2. struct hash para Agent
    source      = b"a" if is_mainnet else b"b"
    struct_hash = keccak(primitive=(
        _AGENT_TYPEHASH
        + keccak(primitive=source)  # string → keccak256
        + bytes(cid)                # bytes32 → directo (ya son 32 bytes)
    ))

    # 3. Firmar con EIP-191 versión 0x01 (EIP-712)
    #    sign_message llama internamente a keccak(\x19 + version + header + body)
    #    = keccak(\x19\x01 + _DOMAIN_SEP + struct_hash)  ← hash final EIP-712
    signed = account.sign_message(
        SignableMessage(version=b"\x01", header=bytes(_DOMAIN_SEP), body=struct_hash)
    )
    return {"r": hex(signed.r), "s": hex(signed.s), "v": signed.v}


# ─── CLIENTE INFO (datos de mercado públicos) ─────────────────────────────────

class HLInfo:
    """Wrapper REST para la API pública /info de Hyperliquid (sin autenticación)."""

    def __init__(self, base_url: str):
        self.url = base_url.rstrip("/") + "/info"

    def _post(self, payload: dict):
        r = requests.post(self.url, json=payload, timeout=15)
        r.raise_for_status()
        return r.json()

    def meta_and_asset_ctxs(self):
        """Devuelve (meta, ctxs) con universo de activos y contextos."""
        data = self._post({"type": "metaAndAssetCtxs"})
        return data[0], data[1]

    def candles_snapshot(self, coin: str, interval: str,
                         start_ms: int, end_ms: int) -> list:
        """Devuelve lista de velas OHLCV."""
        return self._post({
            "type": "candleSnapshot",
            "req": {
                "coin":      coin,
                "interval":  interval,
                "startTime": start_ms,
                "endTime":   end_ms,
            },
        })

    def user_state(self, address: str) -> dict:
        """Devuelve estado de cuenta (equity, posiciones)."""
        return self._post({"type": "clearinghouseState", "user": address})


# ─── CLIENTE EXCHANGE (órdenes firmadas) ──────────────────────────────────────

class HLExchange:
    """Envía órdenes firmadas a la API /exchange de Hyperliquid."""

    def __init__(self, account, base_url: str, account_address: str = None):
        self.account    = account
        self.address    = account_address or account.address
        self.exch_url   = base_url.rstrip("/") + "/exchange"
        self.info_url   = base_url.rstrip("/") + "/info"
        self.is_mainnet = "testnet" not in base_url.lower()
        self._meta      = None   # caché del índice de assets

    # ── utilidades internas ──────────────────────────────────────────────────
    def _asset_index(self, coin: str) -> int:
        """Obtiene el índice numérico de un coin en el universo de Hyperliquid."""
        if self._meta is None:
            r = requests.post(self.info_url, json={"type": "meta"}, timeout=15)
            r.raise_for_status()
            self._meta = r.json()
        for i, asset in enumerate(self._meta["universe"]):
            if asset["name"] == coin:
                return i
        raise ValueError(f"Coin '{coin}' no encontrada en meta")

    def _send(self, action: dict) -> dict:
        nonce = int(time.time() * 1000)
        sig   = _sign_action(self.account, action, nonce, self.is_mainnet)
        r = requests.post(self.exch_url, json={
            "action":    action,
            "nonce":     nonce,
            "signature": sig,
        }, timeout=15)
        r.raise_for_status()
        return r.json()

    # ── acciones de trading ──────────────────────────────────────────────────
    def update_leverage(self, leverage: int, coin: str,
                        is_cross: bool = False) -> dict:
        return self._send({
            "type":     "updateLeverage",
            "asset":    self._asset_index(coin),
            "isCross":  is_cross,
            "leverage": leverage,
        })

    def order(self, coin: str, is_buy: bool, size: float, limit_px: float,
              order_type: dict, reduce_only: bool = False) -> dict:
        return self._send({
            "type": "order",
            "orders": [{
                "a": self._asset_index(coin),
                "b": is_buy,
                "p": _float_to_wire(limit_px),
                "s": _float_to_wire(size),
                "r": reduce_only,
                "t": order_type,
            }],
            "grouping": "na",
        })
