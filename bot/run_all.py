#!/usr/bin/env python3
"""
AlphaChainBots — Lanzador paralelo
Arranca bot.py (4H), bot_1h.py (1H·EMA) y bot_1h_ma.py (1H·SMA)
en hilos independientes con logging en tiempo real.
Si un bot falla se reinicia automáticamente sin afectar a los demás.
Detener todos: Ctrl+C
"""

import sys
import os
import time
import logging
import threading
import traceback
from datetime import datetime

# ─── asegurar que el directorio del script está en el path ────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ─── COLORES ANSI ─────────────────────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

COLORS = {
    "BOT·4H":    "\033[96m",   # Cyan
    "BOT·1H·EMA":"\033[93m",   # Amarillo
    "BOT·1H·SMA":"\033[92m",   # Verde
    "RUNNER":    "\033[95m",   # Magenta
}

LEVEL_COLORS = {
    "DEBUG":    "\033[37m",
    "INFO":     "\033[0m",
    "WARNING":  "\033[33m",
    "ERROR":    "\033[31m",
    "CRITICAL": "\033[41m",
}

# ─── FORMATTER PERSONALIZADO ──────────────────────────────────────────────────
class BotFormatter(logging.Formatter):
    def __init__(self, bot_label: str):
        super().__init__()
        self.bot_label = bot_label
        self.color     = COLORS.get(bot_label, RESET)

    def format(self, record: logging.LogRecord) -> str:
        now       = datetime.now().strftime("%H:%M:%S")
        lvl       = record.levelname
        lvl_color = LEVEL_COLORS.get(lvl, RESET)
        label     = f"{self.color}{BOLD}[{self.bot_label}]{RESET}"
        ts        = f"{DIM}{now}{RESET}"
        level_tag = f"{lvl_color}{lvl:<8}{RESET}"
        msg       = record.getMessage()

        # Excepción adjunta si la hay
        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)

        return f"{ts} {label} {level_tag} {msg}"


# ─── HANDLER DE CONSOLA CON LOCK ──────────────────────────────────────────────
_print_lock = threading.Lock()

class LockedStreamHandler(logging.StreamHandler):
    """StreamHandler con mutex para evitar mezcla de líneas entre hilos."""
    def emit(self, record: logging.LogRecord) -> None:
        with _print_lock:
            super().emit(record)


# ─── CONFIGURAR LOGGER POR BOT ────────────────────────────────────────────────
def make_logger(module_name: str, bot_label: str) -> logging.Logger:
    """
    Crea (o reutiliza) el logger del módulo del bot y le añade
    un handler con formato y color propio.
    """
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    # Evitar handlers duplicados si se llama varias veces
    if not any(isinstance(h, LockedStreamHandler) for h in logger.handlers):
        handler = LockedStreamHandler(sys.stdout)
        handler.setFormatter(BotFormatter(bot_label))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


# ─── EVENTO GLOBAL DE PARADA ──────────────────────────────────────────────────
stop_event = threading.Event()


# ─── WRAPPER DE HILO CON REINICIO AUTOMÁTICO ─────────────────────────────────
def bot_thread(module_name: str, bot_label: str, restart_delay: int = 30) -> None:
    """
    Importa el módulo, instancia Bot() y llama a run().
    Si falla, espera restart_delay segundos y reintenta.
    Se detiene limpiamente cuando stop_event está activo.
    """
    thread_log = make_logger(module_name, bot_label)
    thread_log.info(f"Hilo iniciado — módulo '{module_name}'")

    while not stop_event.is_set():
        try:
            # Importar el módulo (la primera vez lo carga; las siguientes usa caché)
            import importlib
            mod = importlib.import_module(module_name)
            # Asegurar que el logger del módulo tenga nuestro handler
            make_logger(module_name, bot_label)

            bot_instance = mod.Bot()
            bot_instance.run()

            # Si run() termina sin excepción (e.g. KeyboardInterrupt interno)
            if not stop_event.is_set():
                thread_log.warning("run() terminó inesperadamente. Reiniciando...")
                time.sleep(restart_delay)

        except KeyboardInterrupt:
            thread_log.info("Señal de parada recibida.")
            break

        except Exception:
            if stop_event.is_set():
                break
            thread_log.error(
                f"El bot falló con excepción:\n{traceback.format_exc().strip()}"
            )
            thread_log.warning(
                f"Reintentando en {restart_delay}s... "
                f"(los otros bots siguen funcionando)"
            )
            # Espera en pequeños intervalos para poder responder al stop_event
            for _ in range(restart_delay):
                if stop_event.is_set():
                    break
                time.sleep(1)

    thread_log.info("Hilo detenido.")


# ─── DEFINICIÓN DE BOTS ───────────────────────────────────────────────────────
BOTS = [
    # (nombre_módulo,  etiqueta_log,    delay_reinicio_seg)
    ("bot",          "BOT·4H",         45),
    ("bot_1h",       "BOT·1H·EMA",     30),
    ("bot_1h_ma",    "BOT·1H·SMA",     30),
]


# ─── LOGGER DEL RUNNER ────────────────────────────────────────────────────────
runner_log = logging.getLogger("runner")
runner_log.setLevel(logging.INFO)
_rh = LockedStreamHandler(sys.stdout)
_rh.setFormatter(BotFormatter("RUNNER"))
runner_log.addHandler(_rh)
runner_log.propagate = False


# ─── BANNER ───────────────────────────────────────────────────────────────────
def print_banner() -> None:
    with _print_lock:
        print(f"\n{BOLD}{'═'*62}{RESET}")
        print(f"{BOLD}   AlphaChainBots — Lanzador paralelo{RESET}")
        print(f"{'═'*62}")
        print(f"  {COLORS['BOT·4H']}{BOLD}BOT·4H    {RESET}  bot.py       — EMA 20/50  · trailing 1.5%")
        print(f"  {COLORS['BOT·1H·EMA']}{BOLD}BOT·1H·EMA{RESET}  bot_1h.py   — EMA 20/50  · trailing 1.0%")
        print(f"  {COLORS['BOT·1H·SMA']}{BOLD}BOT·1H·SMA{RESET}  bot_1h_ma.py — SMA 50/100 · trailing 1.0%")
        print(f"{'═'*62}")
        print(f"  Iniciar: todos los bots arrancando en paralelo...")
        print(f"  Detener: {BOLD}Ctrl+C{RESET}")
        print(f"{'═'*62}\n")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main() -> None:
    print_banner()

    threads: list[threading.Thread] = []

    for module_name, label, delay in BOTS:
        t = threading.Thread(
            target=bot_thread,
            args=(module_name, label, delay),
            name=label,
            daemon=True,   # muere si el proceso principal termina
        )
        t.start()
        threads.append(t)
        runner_log.info(f"Hilo '{label}' arrancado (id={t.ident})")
        time.sleep(2)   # pequeño desfase para que los logs no se pisen al inicio

    runner_log.info(f"{len(threads)} bots corriendo. Esperando Ctrl+C para detener...")

    try:
        # Mantener el hilo principal vivo y mostrar estado cada 10 minutos
        while True:
            time.sleep(600)
            vivos   = [t.name for t in threads if t.is_alive()]
            muertos = [t.name for t in threads if not t.is_alive()]
            runner_log.info(f"Estado — activos: {vivos} | detenidos: {muertos or 'ninguno'}")

    except KeyboardInterrupt:
        runner_log.info("Ctrl+C recibido — deteniendo todos los bots...")
        stop_event.set()

    # Esperar a que todos los hilos terminen (máx 15s cada uno)
    for t in threads:
        t.join(timeout=15)
        estado = "detenido" if not t.is_alive() else "forzado (timeout)"
        runner_log.info(f"Hilo '{t.name}': {estado}")

    with _print_lock:
        print(f"\n{BOLD}Todos los bots detenidos. Hasta pronto.{RESET}\n")


if __name__ == "__main__":
    main()
