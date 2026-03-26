#!/usr/bin/env python3
"""
server.py — Wrapper para Render.com (plan gratuito)
Levanta un servidor HTTP mínimo en el puerto $PORT para que Render
no apague el servicio, y arranca los 3 bots en hilos paralelos.
UptimeRobot hace ping cada 14 minutos al endpoint /health para
mantenerlo activo las 24h.
"""

import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime

# ─── asegurar path ────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import run_all   # importa la lógica de los 3 bots


# ─── HTTP HEALTH SERVER ───────────────────────────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    """Responde /health y / con 200 OK para que Render y UptimeRobot
    detecten el servicio como activo."""

    _start_time = datetime.now()

    def do_GET(self):
        uptime  = datetime.now() - self._start_time
        h, rem  = divmod(int(uptime.total_seconds()), 3600)
        m, s    = divmod(rem, 60)
        body = (
            f"AlphaChainBots OK\n"
            f"Uptime: {h}h {m}m {s}s\n"
            f"Bots: BOT-4H | BOT-1H-EMA | BOT-1H-SMA\n"
            f"Mode: {'TESTNET' if os.getenv('TESTNET','true')=='true' else 'MAINNET'}\n"
        ).encode()

        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass   # silencia los logs HTTP para no ensuciar la consola


def start_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"[server] Health endpoint escuchando en puerto {port}")
    server.serve_forever()


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1) Levantar HTTP en hilo daemon (si muere, no arrastra a los bots)
    http_thread = threading.Thread(target=start_health_server, daemon=True)
    http_thread.start()

    # 2) Arrancar los 3 bots (bloquea aquí hasta Ctrl+C o crash)
    run_all.main()
