#!/usr/bin/env python3
"""
telegram_alerts.py — Envía alertas a Telegram.
Lee TELEGRAM_TOKEN y TELEGRAM_CHAT_ID desde el entorno / .env
Si no están configurados, todas las funciones fallan silenciosamente.
"""

import os
import logging
import requests

log = logging.getLogger(__name__)

# Carga .env si existe
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _creds():
    return (
        os.environ.get("TELEGRAM_TOKEN", "").strip(),
        os.environ.get("TELEGRAM_CHAT_ID", "").strip(),
    )


def send_alert(message: str) -> bool:
    """
    Envía un mensaje a Telegram.
    Retorna True si se envió; False/silencioso si no hay credenciales o falla.
    """
    token, chat_id = _creds()
    if not token or not chat_id:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=6,
        )
        if not r.ok:
            log.debug("[Telegram] HTTP %s — %s", r.status_code, r.text[:120])
        return r.ok
    except Exception as exc:
        log.debug("[Telegram] Error: %s", exc)
        return False


def test_connection() -> dict:
    """Prueba la conexión con el bot de Telegram y devuelve {ok, error}."""
    token, chat_id = _creds()
    if not token:
        return {"ok": False, "error": "TELEGRAM_TOKEN no configurado en .env"}
    if not chat_id:
        return {"ok": False, "error": "TELEGRAM_CHAT_ID no configurado en .env"}
    ok = send_alert(
        "🤖 <b>AlphaChainBots</b> — Conexión verificada correctamente ✓"
    )
    return {"ok": ok, "error": None if ok else "Error al enviar mensaje de prueba"}
