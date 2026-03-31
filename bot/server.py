#!/usr/bin/env python3
"""
server.py — Dashboard web de simulación para Render.com
GET /           → Dashboard HTML (auto-refresh 30s)
GET /api/status → JSON con estado de los 3 bots
GET /health     → "OK" para UptimeRobot
"""

import os
import sys
import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sim_engine
import backtest_engine

_SERVER_START = time.time()

try:
    import telegram_alerts as _tg
except Exception:
    _tg = None


# ─── HTML EMBEBIDO ────────────────────────────────────────────────────────────
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>AlphaChainBots \xe2\x80\x94 Dashboard</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>&#x2B21;</text></svg>">
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#07090f;color:#c9d4e0;font-family:'Courier New',monospace;font-size:13px;padding-bottom:40px}
a{color:inherit;text-decoration:none}

/* ── HEADER ── */
.hdr{background:#0b0f1c;border-bottom:1px solid #162030;padding:12px 20px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;position:sticky;top:0;z-index:100}
.hdr-title{color:#4fc3f7;font-size:17px;letter-spacing:3px;font-weight:bold}
.hdr-stats{display:flex;gap:18px;flex-wrap:wrap;align-items:center}
.stat{text-align:center}
.stat-l{color:#37505f;font-size:10px;text-transform:uppercase;letter-spacing:1px;margin-bottom:2px}
.stat-v{font-size:15px;font-weight:bold}

/* ── PROGRESS BAR ── */
.pbar{height:2px;background:#0f1825;position:fixed;bottom:0;left:0;right:0;z-index:200}
.pfill{height:100%;background:#4fc3f7;transition:width 1s linear}

/* ── LAYOUT ── */
.wrap{max-width:1440px;margin:0 auto;padding:14px 16px}
.grid3{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:14px}

/* ── BOT CARDS ── */
.card{background:#0b0f1c;border:1px solid #162030;border-radius:6px;padding:13px;border-left:3px solid #37505f;cursor:pointer;transition:border-color .2s}
.card:hover{border-color:#2a4a6a}
.card.c0{border-left-color:#4fc3f7}.card.c1{border-left-color:#ffd740}
.card.c2{border-left-color:#69f0ae}.card.c3{border-left-color:#ff6b6b}
.card.c4{border-left-color:#ce93d8}.card.c5{border-left-color:#ffab40}
.card.c6{border-left-color:#4dd0e1}.card.c7{border-left-color:#a5d6a7}
.card.c8{border-left-color:#f48fb1}.card.c9{border-left-color:#80cbc4}
.card.c10{border-left-color:#bcaaa4}.card.c11{border-left-color:#b0bec5}
.card.c12{border-left-color:#e6ee9c}.card.c13{border-left-color:#ffcc02}
.card.c14{border-left-color:#80deea}.card.c15{border-left-color:#ef9a9a}
.card.c16{border-left-color:#c5e1a5}.card.c17{border-left-color:#b39ddb}
.card-name{font-size:13px;font-weight:bold;letter-spacing:1px;margin-bottom:6px}
.col0{color:#4fc3f7}.col1{color:#ffd740}.col2{color:#69f0ae}
.col3{color:#ff6b6b}.col4{color:#ce93d8}.col5{color:#ffab40}
.col6{color:#4dd0e1}.col7{color:#a5d6a7}.col8{color:#f48fb1}
.col9{color:#80cbc4}.col10{color:#bcaaa4}.col11{color:#b0bec5}
.col12{color:#e6ee9c}.col13{color:#ffcc02}.col14{color:#80deea}
.col15{color:#ef9a9a}.col16{color:#c5e1a5}.col17{color:#b39ddb}
.card-meta{color:#37505f;font-size:11px;margin-bottom:8px}
.card-eq{font-size:15px;font-weight:bold;margin-bottom:3px;color:#c9d4e0}
.card-pnl{font-size:13px;margin-bottom:8px}
.card-row{display:flex;gap:14px;font-size:11px;color:#546e7a}

/* ── PANELS ── */
.panel{background:#0b0f1c;border:1px solid #162030;border-radius:6px;margin-bottom:12px}
.ph{padding:9px 14px;border-bottom:1px solid #162030;font-size:10px;text-transform:uppercase;letter-spacing:2px;color:#37505f;display:flex;justify-content:space-between;align-items:center}
.ph-cnt{color:#546e7a;font-size:12px;letter-spacing:0}
.tbl-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse}
th{padding:7px 11px;text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:1px;color:#263a4a;border-bottom:1px solid #101820;white-space:nowrap}
td{padding:7px 11px;border-bottom:1px solid #0d1520;white-space:nowrap;font-size:12px}
tr:last-child td{border-bottom:none}
tr:hover td{background:#0d1420}
.empty{text-align:center;padding:22px;color:#263a4a;font-size:12px}

/* ── BADGES ── */
.b{display:inline-block;padding:2px 6px;border-radius:3px;font-size:10px;font-weight:bold;letter-spacing:.5px}
.b-long{background:#002a1a;color:#00e676}
.b-short{background:#300010;color:#ff4466}
.b-entry{background:#001f3a;color:#4fc3f7}
.b-close{background:#1a1500;color:#ffd740}
.b-skip{background:#141018;color:#546e7a}
.b-wait{background:#0f1520;color:#37505f}

/* ── COLORS ── */
.up{color:#00e676}.dn{color:#ff4466}.nu{color:#78909c}

/* ── STATUS BAR ── */
.sbar{position:fixed;bottom:2px;left:0;right:0;display:flex;justify-content:space-between;padding:0 16px;font-size:10px;color:#263a4a;pointer-events:none}
.sbar .live{color:#00e676}

@media(max-width:1200px){.grid3{grid-template-columns:repeat(3,1fr)}}
@media(max-width:900px){.grid3{grid-template-columns:repeat(2,1fr)}}
@media(max-width:580px){.grid3{grid-template-columns:1fr}}
@media(max-width:580px){.hdr{flex-direction:column;align-items:flex-start}.hdr-stats{gap:12px}}

/* ── MARKET DATA ── */
.mkt-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:14px}
@media(max-width:900px){.mkt-grid{grid-template-columns:1fr}}
.mkt-upd{font-size:10px;color:#37505f}
/* Heatmap tabs */
.hmap-tabs{display:flex;gap:6px;margin-bottom:8px}
.hmap-tab{padding:3px 12px;border:1px solid #1e3a4a;border-radius:3px;font-size:11px;font-weight:bold;cursor:pointer;background:#0a0f1c;color:#546e7a;letter-spacing:1px;transition:all .2s}
.hmap-tab.active{background:#0d2030;border-color:#4fc3f7;color:#4fc3f7}
.hmap-pane{display:none}.hmap-pane.active{display:block}
.hmap-canvas-wrap{position:relative;height:280px}
.hmap-danger{font-size:10px;color:#ff6b6b;margin-top:4px;min-height:16px}
/* OI/Funding compact table */
.oi-pos{color:#00e676}.oi-neg{color:#ff4466}.oi-nu{color:#78909c}
.fund-pos{color:#00e676;font-weight:bold}.fund-neg{color:#ff4466;font-weight:bold}.fund-nu{color:#546e7a}
.ls-bull{color:#69f0ae}.ls-bear{color:#ff6b6b}
.oi-icon{font-size:11px;margin-right:2px}

/* ── EXECUTIVE BAR ── */
.exec-bar{background:#08101c;border:1px solid #162030;border-radius:6px;padding:10px 18px;margin-bottom:12px;display:flex;gap:24px;flex-wrap:wrap;align-items:center}
.exec-item{text-align:center;min-width:80px}
.exec-lbl{font-size:9px;text-transform:uppercase;letter-spacing:1.5px;color:#37505f;margin-bottom:3px}
.exec-val{font-size:22px;font-weight:bold;line-height:1}
.exec-val.big{font-size:28px}
.exec-sub{font-size:10px;color:#546e7a;margin-top:2px}
.exec-div{width:1px;background:#162030;align-self:stretch;margin:0 4px}
@media(max-width:700px){.exec-bar{gap:14px}.exec-val.big{font-size:20px}}

/* ── FILTER BAR ── */
.flt-bar{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px;align-items:center}
.flt-lbl{font-size:10px;color:#37505f;text-transform:uppercase;letter-spacing:1px;margin-right:4px}
.flt-btn{padding:4px 12px;border:1px solid #1e3a4a;border-radius:3px;font-size:11px;font-weight:bold;cursor:pointer;background:#0a0f1c;color:#546e7a;transition:all .2s;letter-spacing:.5px;font-family:inherit}
.flt-btn.active{background:#0d2030;border-color:#4fc3f7;color:#4fc3f7}
.flt-btn:hover{border-color:#37505f;color:#c9d4e0}

/* ── TRADINGVIEW MODAL ── */
.tv-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.82);z-index:1000;align-items:center;justify-content:center}
.tv-overlay.open{display:flex}
.tv-modal{background:#0b0f1c;border:1px solid #1e3a4a;border-radius:8px;width:90vw;max-width:1100px;height:82vh;display:flex;flex-direction:column;overflow:hidden}
.tv-modal-hdr{display:flex;justify-content:space-between;align-items:center;padding:9px 14px;border-bottom:1px solid #162030;font-size:11px;text-transform:uppercase;letter-spacing:2px;color:#37505f;flex-shrink:0}
.tv-modal-title{color:#4fc3f7;letter-spacing:2px}
.tv-close{background:none;border:1px solid #1e3a4a;color:#78909c;cursor:pointer;border-radius:3px;padding:3px 10px;font-size:12px;font-family:inherit;transition:all .2s}
.tv-close:hover{border-color:#ff6b6b;color:#ff6b6b}
#tv-container{flex:1;min-height:0}
#tv-container iframe{width:100%;height:100%;border:none}

/* ── FEAR & GREED ── */
.fng-wrap{padding:10px;display:flex;align-items:center;gap:16px;border-top:1px solid #0d1520}
.fng-gauge{flex-shrink:0}
.fng-info{flex:1}
.fng-val{font-size:32px;font-weight:bold;line-height:1}
.fng-lbl{font-size:12px;font-weight:bold;letter-spacing:1px;margin-top:2px}
.fng-ts{font-size:9px;color:#37505f;margin-top:4px}
.fng-ef{color:#ff1744}.fng-f{color:#ff6d00}.fng-n{color:#ffd740}
.fng-g{color:#69f0ae}.fng-eg{color:#00e676}

/* ── STATUS DOT ── */
.sdot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-left:5px;vertical-align:middle;flex-shrink:0}
.sdot-g{background:#00e676;box-shadow:0 0 4px #00e676}
.sdot-y{background:#ffd740;box-shadow:0 0 4px #ffd740}
.sdot-r{background:#ff4466;box-shadow:0 0 4px #ff4466}

/* ── SORTABLE TABLE ── */
.tbl-sort th{cursor:pointer;user-select:none}
.tbl-sort th:hover{color:#4fc3f7}
.sort-asc::after{content:' ▲';font-size:8px}
.sort-desc::after{content:' ▼';font-size:8px}

/* ── OPTIMIZER ── */
.opt-spinner{display:inline-block;width:12px;height:12px;border:2px solid #1e3a4a;border-top-color:#4fc3f7;border-radius:50%;animation:spin .8s linear infinite;margin-right:6px;vertical-align:middle}
@keyframes spin{to{transform:rotate(360deg)}}

/* ── BACKTEST ── */
.bt-ph{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}
.bt-tabs{display:flex;gap:6px}
.bt-tab{padding:3px 12px;border:1px solid #1e3a4a;border-radius:3px;font-size:11px;font-weight:bold;cursor:pointer;background:#0a0f1c;color:#546e7a;transition:all .2s;letter-spacing:.5px}
.bt-tab.active{background:#0d2030;border-color:#4fc3f7;color:#4fc3f7}
.bt-pbar-bg{height:4px;background:#0a0f1c;border-radius:2px;overflow:hidden;margin-top:6px}
.bt-pbar-fill{height:100%;background:linear-gradient(90deg,#4fc3f7,#00e676);border-radius:2px;transition:width .4s}
.bt-status{font-size:11px;color:#546e7a;margin-top:6px}
.bt-eq-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:10px;padding:10px}
.bt-eq-card{background:#060c16;border:1px solid #0d1a28;border-radius:6px;padding:8px}
.bt-eq-title{font-size:10px;font-weight:bold;margin-bottom:4px;letter-spacing:1px}
.bt-eq-canvas{position:relative;height:110px}
.bt-eq-meta{display:flex;gap:8px;font-size:9px;color:#546e7a;margin-top:4px;flex-wrap:wrap}
.bt-sec-title{font-size:11px;color:#4fc3f7;padding:10px 10px 4px;letter-spacing:.5px}
@media(max-width:580px){.bt-eq-grid{grid-template-columns:1fr}}
</style>
</head>
<body>

<div class="hdr">
  <div class="hdr-title">&#x2B21; AlphaChainBots</div>
  <div class="hdr-stats" id="hdr-stats">
    <div class="stat"><div class="stat-l">Estado</div><div class="stat-v nu">Cargando&hellip;</div></div>
  </div>
</div>

<div class="wrap">

  <!-- ── EXECUTIVE SUMMARY BAR ──────────────────────────────────────────────── -->
  <div class="exec-bar" id="exec-bar">
    <div class="exec-item">
      <div class="exec-lbl">Capital Total</div>
      <div class="exec-val big nu">$260K</div>
      <div class="exec-sub">26 bots × $10K</div>
    </div>
    <div class="exec-div"></div>
    <div class="exec-item">
      <div class="exec-lbl">PnL Acumulado</div>
      <div class="exec-val big nu" id="exec-total-pnl">—</div>
      <div class="exec-sub" id="exec-total-pnl-pct"></div>
    </div>
    <div class="exec-div"></div>
    <div class="exec-item">
      <div class="exec-lbl">Bots Rentables</div>
      <div class="exec-val nu" id="exec-profitable">—</div>
      <div class="exec-sub" id="exec-profitable-sub"></div>
    </div>
    <div class="exec-div"></div>
    <div class="exec-item">
      <div class="exec-lbl">Win Rate Global</div>
      <div class="exec-val nu" id="exec-winrate">—</div>
      <div class="exec-sub" id="exec-winrate-sub"></div>
    </div>
    <div class="exec-div"></div>
    <div class="exec-item">
      <div class="exec-lbl">Mejor Bot</div>
      <div class="exec-val up" id="exec-best" style="font-size:13px;padding-top:4px">—</div>
      <div class="exec-sub" id="exec-best-pnl"></div>
    </div>
    <div class="exec-div"></div>
    <div class="exec-item">
      <div class="exec-lbl">Max DD Global</div>
      <div class="exec-val nu" id="exec-maxdd">—</div>
      <div class="exec-sub">peor bot</div>
    </div>
  </div>
  <!-- ── FIN EXECUTIVE BAR ────────────────────────────────────────────────── -->

  <!-- ── MARKET DATA SECTION ─────────────────────────────────────────────── -->
  <div class="mkt-grid">

    <!-- Liquidation Heatmap Chart.js -->
    <div class="panel">
      <div class="ph">
        <span>Liquidation Heatmap</span>
        <span class="mkt-upd">5 min &middot; <span id="mkt-cd">—</span> &middot; <span id="mkt-upd-ts">—</span></span>
      </div>
      <div style="padding:10px">
        <div class="hmap-tabs">
          <button class="hmap-tab active" onclick="switchTab('BTC')">BTC</button>
          <button class="hmap-tab" onclick="switchTab('ETH')">ETH</button>
          <button class="hmap-tab" onclick="switchTab('SOL')">SOL</button>
        </div>
        <div id="hpane-BTC" class="hmap-pane active">
          <div class="hmap-canvas-wrap"><canvas id="hchart-BTC"></canvas></div>
          <div class="hmap-danger" id="hdanger-BTC"></div>
        </div>
        <div id="hpane-ETH" class="hmap-pane">
          <div class="hmap-canvas-wrap"><canvas id="hchart-ETH"></canvas></div>
          <div class="hmap-danger" id="hdanger-ETH"></div>
        </div>
        <div id="hpane-SOL" class="hmap-pane">
          <div class="hmap-canvas-wrap"><canvas id="hchart-SOL"></canvas></div>
          <div class="hmap-danger" id="hdanger-SOL"></div>
        </div>
      </div>
    </div>

    <!-- OI + Funding + L/S Table compact -->
    <div class="panel">
      <div class="ph"><span>Open Interest &middot; Funding &middot; L/S</span></div>
      <div class="tbl-wrap">
        <table>
          <thead><tr>
            <th>Coin</th><th>Precio</th>
            <th>OI</th><th>&#916;OI</th>
            <th>Fund%</th><th>L/S</th><th>Vol24h</th>
          </tr></thead>
          <tbody id="oi-body"><tr><td colspan="7" class="empty">Cargando&hellip;</td></tr></tbody>
        </table>
      </div>
      <!-- Fear & Greed -->
      <div class="fng-wrap" id="fng-wrap">
        <svg class="fng-gauge" id="fng-svg" viewBox="0 0 160 90" width="160" height="90">
          <!-- arcs filled by JS -->
          <g id="fng-arcs"></g>
          <line id="fng-needle" x1="80" y1="80" x2="80" y2="18" stroke="#c9d4e0" stroke-width="2.5" stroke-linecap="round"/>
          <circle cx="80" cy="80" r="5" fill="#c9d4e0"/>
          <text id="fng-num-svg" x="80" y="76" text-anchor="middle" font-size="13" font-weight="bold" fill="#c9d4e0">—</text>
        </svg>
        <div class="fng-info">
          <div class="fng-val nu" id="fng-val">—</div>
          <div class="fng-lbl nu" id="fng-lbl">Cargando&hellip;</div>
          <div class="fng-ts" id="fng-ts"></div>
        </div>
      </div>
    </div>

  </div>
  <!-- ── FIN MARKET DATA ─────────────────────────────────────────────────── -->

  <!-- ── BACKTEST SECTION ────────────────────────────────────────────────── -->
  <div class="panel" id="bt-section">
    <div class="ph bt-ph">
      <span>&#x1F4CA; Backtest Hist&oacute;rico</span>
      <div class="bt-tabs">
        <button class="bt-tab active" onclick="btLoad('3m')">3M</button>
        <button class="bt-tab" onclick="btLoad('6m')">6M</button>
        <button class="bt-tab" onclick="btLoad('1y')">1A</button>
        <button class="bt-tab" onclick="btLoad('max')">M&Aacute;X</button>
      </div>
    </div>
    <div style="padding:10px 10px 4px" id="bt-prog-wrap">
      <div class="bt-pbar-bg"><div class="bt-pbar-fill" id="bt-pbar" style="width:0%"></div></div>
      <div class="bt-status" id="bt-status">Selecciona un periodo para iniciar el backtest &mdash; los datos se calculan en segundo plano.</div>
    </div>
    <div id="bt-results" style="display:none">
      <!-- Equity curves grid -->
      <div class="bt-sec-title">Equity Curves &mdash; <span id="bt-period-lbl"></span></div>
      <div class="bt-eq-grid" id="bt-eq-grid"></div>

      <!-- PnL bar chart -->
      <div class="bt-sec-title">Comparativa PnL por Bot</div>
      <div style="padding:0 10px 10px;position:relative;height:230px">
        <canvas id="bt-bar-canvas"></canvas>
      </div>

      <!-- Comparison table -->
      <div class="bt-sec-title">Tabla Comparativa <span style="color:#37505f;font-size:9px">(ordenado por PnL desc)</span></div>
      <div class="tbl-wrap" style="padding:0 10px 10px">
        <table id="bt-tbl" class="tbl-sort">
          <thead><tr>
            <th>#</th>
            <th onclick="btSortBy('label')">Bot</th>
            <th onclick="btSortBy('strategy')">Estrategia</th>
            <th onclick="btSortBy('total_pnl')" id="btsort-total_pnl" class="sort-desc">PnL $</th>
            <th onclick="btSortBy('total_pnl_pct')">PnL %</th>
            <th onclick="btSortBy('win_rate')">Win%</th>
            <th onclick="btSortBy('total_trades')">Trades</th>
            <th onclick="btSortBy('max_drawdown')">Max DD%</th>
            <th onclick="btSortBy('best_trade')">Mejor $</th>
            <th onclick="btSortBy('worst_trade')">Peor $</th>
            <th onclick="btSortBy('sharpe')">Sharpe</th>
            <th onclick="btSortBy('profit_factor')">PF</th>
          </tr></thead>
          <tbody id="bt-tbody"></tbody>
        </table>
      </div>
      <div style="font-size:9px;color:#263a4a;padding:4px 10px 10px" id="bt-computed-at"></div>
    </div>
  </div>
  <!-- ── FIN BACKTEST ─────────────────────────────────────────────────────── -->

  <!-- ── OPTIMIZER SECTION ─────────────────────────────────────────────────── -->
  <div class="panel" id="opt-section">
    <div class="ph">
      <span>&#x1F52C; Optimizador de Par&aacute;metros</span>
      <span class="mkt-upd" id="opt-upd-ts"></span>
    </div>
    <div id="opt-pending" style="padding:12px 14px;color:#546e7a;font-size:12px">
      <span class="opt-spinner"></span>Buscando resultados de optimizaci&oacute;n&hellip;
    </div>
    <div id="opt-results" style="display:none">
      <div class="bt-sec-title">Top 30 Combinaciones de Par&aacute;metros</div>
      <div class="tbl-wrap" style="padding:0 10px 10px">
        <table>
          <thead><tr>
            <th>#</th><th>PnL $</th><th>PnL %</th><th>Win%</th><th>Trades</th>
            <th>Timeframe</th><th>MA</th><th>Fast</th><th>Slow</th>
            <th>Lev.</th><th>Trail%</th><th>RSI OB</th><th>RSI OS</th>
            <th>Sharpe</th><th>PF</th>
          </tr></thead>
          <tbody id="opt-tbody"></tbody>
        </table>
      </div>
      <div style="font-size:9px;color:#263a4a;padding:4px 10px 10px" id="opt-computed-at"></div>
    </div>
    <div id="opt-empty" style="display:none;padding:12px 14px;color:#37505f;font-size:12px">
      Sin resultados todav&iacute;a. Ejecuta <code style="color:#4fc3f7">python optimizer.py</code> para generar top_params.json.
    </div>
  </div>
  <!-- ── FIN OPTIMIZER ──────────────────────────────────────────────────────── -->

  <!-- ── FILTER BAR ────────────────────────────────────────────────────────── -->
  <div class="flt-bar">
    <span class="flt-lbl">Filtrar:</span>
    <button class="flt-btn active" onclick="applyFilter('all',this)">Todos</button>
    <button class="flt-btn" onclick="applyFilter('win',this)">&#x2B; Ganadores</button>
    <button class="flt-btn" onclick="applyFilter('lose',this)">&#x2212; Perdedores</button>
    <button class="flt-btn" onclick="applyFilter('15m',this)">15m</button>
    <button class="flt-btn" onclick="applyFilter('30m',this)">30m</button>
    <button class="flt-btn" onclick="applyFilter('1h',this)">1h</button>
    <button class="flt-btn" onclick="applyFilter('4h',this)">4h</button>
    <button class="flt-btn" onclick="applyFilter('liq',this)">Liquidaciones</button>
  </div>

  <div class="grid3" id="bot-cards">
    <div style="grid-column:1/-1;padding:22px;text-align:center;color:#37505f;font-size:12px">
      <span class="opt-spinner"></span>Conectando con los 26 bots&hellip;
    </div>
  </div>

  <div class="panel">
    <div class="ph"><span>Posiciones abiertas</span><span class="ph-cnt" id="pos-cnt"></span></div>
    <div class="tbl-wrap"><table>
      <thead><tr><th>Bot</th><th>Coin</th><th>Dir</th><th>Entrada</th><th>Actual</th><th>PnL $</th><th>PnL %</th><th>Stop</th><th>Tiempo</th></tr></thead>
      <tbody id="pos-body"><tr><td colspan="9" class="empty">Sin posiciones abiertas</td></tr></tbody>
    </table></div>
  </div>

  <div class="panel">
    <div class="ph"><span>Historial de operaciones</span><span class="ph-cnt" id="hist-cnt"></span></div>
    <div class="tbl-wrap"><table>
      <thead><tr><th>Bot</th><th>Fecha/Hora</th><th>Coin</th><th>Dir</th><th>Entrada</th><th>Salida</th><th>PnL $</th><th>PnL %</th><th>Dur.</th><th>Motivo</th></tr></thead>
      <tbody id="hist-body"><tr><td colspan="10" class="empty">Sin operaciones cerradas todav&iacute;a</td></tr></tbody>
    </table></div>
  </div>

  <div class="panel">
    <div class="ph"><span>Log de se&ntilde;ales</span><span class="ph-cnt" id="sig-cnt"></span></div>
    <div class="tbl-wrap"><table>
      <thead><tr><th>Bot</th><th>Hora</th><th>Coin</th><th>Se&ntilde;al</th><th>Acci&oacute;n</th><th>Motivo</th></tr></thead>
      <tbody id="sig-body"><tr><td colspan="6" class="empty">Esperando se&ntilde;ales &mdash; primer escaneo en curso</td></tr></tbody>
    </table></div>
  </div>
</div>

<!-- ── TRADINGVIEW MODAL ─────────────────────────────────────────────────── -->
<div class="tv-overlay" id="tv-overlay" onclick="if(event.target===this)closeTVModal()">
  <div class="tv-modal">
    <div class="tv-modal-hdr">
      <span class="tv-modal-title" id="tv-modal-title">TradingView</span>
      <button class="tv-close" onclick="closeTVModal()">&#x2715; Cerrar</button>
    </div>
    <div id="tv-container"></div>
  </div>
</div>

<div class="sbar">
  <span><span class="live">&#9679; LIVE</span> &mdash; refresco cada 30s</span>
  <span id="last-upd"></span>
</div>
<div class="pbar"><div class="pfill" id="pfill" style="width:100%"></div></div>

<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script>
const REFRESH = 30;
let cd = REFRESH, cdTimer;

const COLS  = ["col0","col1","col2","col3","col4","col5","col6","col7","col8","col9","col10","col11","col12","col13","col14","col15","col16","col17"];
const CARDS = ["c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","c10","c11","c12","c13","c14","c15","c16","c17"];

function pc(v){ return v>0?'up':v<0?'dn':'nu'; }
function fp(v){ return (v>=0?'+':'')+v.toFixed(2); }
function fmt(v,d=2){ return Number(v).toLocaleString('en',{minimumFractionDigits:d,maximumFractionDigits:d}); }
function fmtPx(v){ return v>=1000?fmt(v,2):v>=1?fmt(v,4):fmt(v,6); }

function render(d){
  if(!d || !Array.isArray(d.bots)){
    const msg = (d && d.error) ? 'Error: '+d.error : 'Sin respuesta del servidor';
    document.getElementById('hdr-stats').innerHTML =
      `<div class="stat"><div class="stat-l">Estado</div><div class="stat-v dn">${msg}</div></div>`;
    document.getElementById('bot-cards').innerHTML =
      `<div style="grid-column:1/-1;padding:22px;text-align:center;color:#ff4466;font-size:12px">&#9888; ${msg} &mdash; reintentando&hellip;</div>`;
    return;
  }
  d.bots.sort((a,b)=>b.portfolio.total_pnl - a.portfolio.total_pnl);
  // Header
  const tc=pc(d.total_pnl);
  document.getElementById('hdr-stats').innerHTML=`
    <div class="stat"><div class="stat-l">Equity Total</div><div class="stat-v nu">$${fmt(d.total_equity)}</div></div>
    <div class="stat"><div class="stat-l">PnL Total</div><div class="stat-v ${tc}">${fp(d.total_pnl)}$ (${fp(d.total_pnl_pct)}%)</div></div>
    <div class="stat"><div class="stat-l">Capital Inicial</div><div class="stat-v nu">$${fmt(d.initial_equity)}</div></div>
    <div class="stat"><div class="stat-l">Uptime</div><div class="stat-v nu">${d.uptime}</div></div>
    <div class="stat"><div class="stat-l">Modo</div><div class="stat-v nu">SIMULACI&#211;N</div></div>`;

  // ── Executive Summary Bar ─────────────────────────────────────────────────
  const totalPnl = d.total_pnl;
  const totalPct = d.total_pnl_pct;
  const tpCls = pc(totalPnl);
  document.getElementById('exec-total-pnl').className = 'exec-val big '+tpCls;
  document.getElementById('exec-total-pnl').textContent = (totalPnl>=0?'+':'') + totalPnl.toFixed(2) + '$';
  document.getElementById('exec-total-pnl-pct').textContent = (totalPct>=0?'+':'') + totalPct.toFixed(2) + '%';

  const nProfit = d.bots.filter(b=>b.portfolio.total_pnl>0).length;
  const nTot = d.bots.length;
  document.getElementById('exec-profitable').className = 'exec-val '+(nProfit>=nTot/2?'up':'dn');
  document.getElementById('exec-profitable').textContent = nProfit+'/'+nTot;
  document.getElementById('exec-profitable-sub').textContent = nProfit>=nTot/2?'rentables':'en pérdidas';

  let totalWins=0, totalTrades=0;
  d.bots.forEach(b=>{ totalWins+=b.portfolio.wins||0; totalTrades+=b.portfolio.trades||0; });
  const wr = totalTrades>0 ? (totalWins/totalTrades*100).toFixed(1) : '—';
  document.getElementById('exec-winrate').className = 'exec-val '+(totalTrades>0?(totalWins/totalTrades>=0.5?'up':'dn'):'nu');
  document.getElementById('exec-winrate').textContent = wr+(totalTrades>0?'%':'');
  document.getElementById('exec-winrate-sub').textContent = totalTrades>0?totalWins+'/'+totalTrades+' ops':'sin trades';

  const byPnl = [...d.bots].sort((a,b)=>b.portfolio.total_pnl - a.portfolio.total_pnl);
  const best = byPnl[0];
  if(best){
    document.getElementById('exec-best').textContent = best.label;
    document.getElementById('exec-best-pnl').textContent = (best.portfolio.total_pnl>=0?'+':'')+'$'+best.portfolio.total_pnl.toFixed(0);
  }

  let maxDD = 0, maxDDBot = '';
  d.bots.forEach(b=>{ const dd=b.portfolio.max_drawdown||0; if(dd>maxDD){maxDD=dd;maxDDBot=b.label;} });
  document.getElementById('exec-maxdd').className = 'exec-val '+(maxDD>20?'dn':maxDD>10?'':'nu');
  document.getElementById('exec-maxdd').textContent = maxDD.toFixed(1)+'%';

  // ── Bot cards ─────────────────────────────────────────────────────────────
  document.getElementById('bot-cards').innerHTML=d.bots.map((b,i)=>{
    const p=b.portfolio, cc=COLS[b.idx%18], cv=CARDS[b.idx%18];
    const ma=b.ma_type==='liq'?`LIQ·${b.strategy||''}`.toUpperCase()
             :(b.ma_type==='ema'?'EMA':'SMA')+` ${b.ma_fast}/${b.ma_slow}`;
    const tr=(b.trailing_pct*100).toFixed(1);
    const wr=p.trades>0?`${p.wins}/${p.trades} wins`:'0 trades';
    const pc2=pc(p.total_pnl);
    const sBadge=b.status==='escaneando'
      ?'<span class="b b-entry">SCAN</span>'
      :'<span class="b b-wait">'+b.status+'</span>';
    const dotCls = p.positions.length>0?'sdot-g':(p.total_pnl>0?'sdot-y':'sdot-r');
    const bType = b.ma_type==='liq'?'liq':'ema';
    const firstCoin = (b.coins&&b.coins[0])||'BTC';
    return `<div class="card ${cv}" data-interval="${b.interval}" data-pnl-pos="${p.total_pnl>=0?1:0}" data-type="${bType}"
      onclick="openTVModal('${firstCoin}','${b.interval}','${b.label}')">
      <div class="card-name ${cc}">${b.label} ${sBadge}<span class="sdot ${dotCls}"></span></div>
      <div class="card-meta">${ma} &middot; ${b.interval} &middot; trailing ${tr}% &middot; coins: ${b.coins.slice(0,5).join(', ')}&hellip;</div>
      <div class="card-eq">$${fmt(p.equity)}</div>
      <div class="card-pnl ${pc2}">${fp(p.total_pnl)}$ (${fp(p.total_pnl_pct)}%)</div>
      <div class="card-row">
        <span>${wr}</span>
        <span>${p.positions.length} pos.</span>
        <span>scan: ${b.last_scan}</span>
        ${b.errors?`<span class="dn">${b.errors} err</span>`:''}
      </div>
    </div>`;
  }).join('');

  // Positions
  const allPos=[];
  d.bots.forEach((b)=>b.portfolio.positions.forEach(pos=>allPos.push({bidx:b.idx,blabel:b.label,...pos})));
  document.getElementById('pos-cnt').textContent=allPos.length||'';
  document.getElementById('pos-body').innerHTML=allPos.length===0
    ?'<tr><td colspan="9" class="empty">Sin posiciones abiertas</td></tr>'
    :allPos.map(pos=>{
      const cc=COLS[pos.bidx%18], pc2=pc(pos.pnl);
      const dir=pos.direction==='long'
        ?'<span class="b b-long">LONG</span>'
        :'<span class="b b-short">SHORT</span>';
      return `<tr>
        <td class="${cc}">${pos.blabel}</td>
        <td><b>${pos.coin}</b></td><td>${dir}</td>
        <td>$${fmtPx(pos.entry_price)}</td>
        <td>$${fmtPx(pos.current_price)}</td>
        <td class="${pc2}">${fp(pos.pnl)}</td>
        <td class="${pc2}">${fp(pos.pnl_pct)}%</td>
        <td>$${fmtPx(pos.stop)}</td>
        <td>${pos.duration}</td></tr>`;
    }).join('');

  // History
  const allHist=[];
  d.bots.forEach((b)=>b.portfolio.history.forEach(t=>allHist.push({bidx:b.idx,blabel:b.label,...t})));
  allHist.sort((a,b2)=>b2.ts-a.ts);
  document.getElementById('hist-cnt').textContent=allHist.length||'';
  document.getElementById('hist-body').innerHTML=allHist.length===0
    ?'<tr><td colspan="10" class="empty">Sin operaciones cerradas todav&iacute;a</td></tr>'
    :allHist.slice(0,60).map(t=>{
      const cc=COLS[t.bidx%18], pc2=pc(t.pnl);
      const dir=t.direction==='long'
        ?'<span class="b b-long">LONG</span>'
        :'<span class="b b-short">SHORT</span>';
      return `<tr>
        <td class="${cc}">${t.blabel}</td>
        <td>${t.closed_at}</td>
        <td><b>${t.coin}</b></td><td>${dir}</td>
        <td>$${fmtPx(t.entry_price)}</td>
        <td>$${fmtPx(t.exit_price)}</td>
        <td class="${pc2}">${fp(t.pnl)}</td>
        <td class="${pc2}">${fp(t.pnl_pct)}%</td>
        <td>${t.duration}</td>
        <td>${t.reason}</td></tr>`;
    }).join('');

  // Signals
  const allSigs=[];
  d.bots.forEach((b)=>b.signals.forEach(s=>allSigs.push({bidx:b.idx,blabel:b.label,...s})));
  allSigs.sort((a,b2)=>b2.ts-a.ts);
  document.getElementById('sig-cnt').textContent=allSigs.length||'';
  document.getElementById('sig-body').innerHTML=allSigs.length===0
    ?'<tr><td colspan="6" class="empty">Esperando se&ntilde;ales &mdash; primer escaneo en curso</td></tr>'
    :allSigs.slice(0,80).map(s=>{
      const cc=COLS[s.bidx%18];
      let badge;
      if(s.action.startsWith('ENTRADA')) badge='<span class="b b-entry">ENTRADA</span>';
      else if(s.type==='CIERRE')          badge='<span class="b b-close">CIERRE</span>';
      else                                badge='<span class="b b-skip">DESCARTADO</span>';
      return `<tr>
        <td class="${cc}">${s.blabel}</td>
        <td>${s.time}</td>
        <td><b>${s.coin}</b></td>
        <td>${s.type}</td>
        <td>${badge} ${s.action}</td>
        <td>${s.reason||'&mdash;'}</td></tr>`;
    }).join('');

  document.getElementById('last-upd').textContent='Actualizado: '+d.updated_at;
}

function startCD(){
  clearInterval(cdTimer);
  cd=REFRESH;
  const fill=document.getElementById('pfill');
  fill.style.width='100%';
  cdTimer=setInterval(()=>{
    cd--;
    fill.style.width=(cd/REFRESH*100)+'%';
    if(cd<=0){clearInterval(cdTimer);fetchData();}
  },1000);
}

function fetchData(){
  fetch('/api/status')
    .then(r=>r.json())
    .then(data=>{ render(data); startCD(); })
    .catch(()=>startCD());
}
function fetchBots(){ return fetchData(); }

// ── MARKET DATA ──────────────────────────────────────────────────────────────
const MKT_REFRESH = 300;
let mktCd = MKT_REFRESH, mktTimer;
const _liqCharts = {};
let _activeTab = 'BTC';

function fmtM(v){
  if(v>=1e9) return '$'+(v/1e9).toFixed(2)+'B';
  if(v>=1e6) return '$'+(v/1e6).toFixed(1)+'M';
  if(v>=1e3) return '$'+(v/1e3).toFixed(0)+'K';
  return '$'+v.toFixed(0);
}

function switchTab(coin){
  _activeTab = coin;
  document.querySelectorAll('.hmap-tab').forEach(b=>{
    b.classList.toggle('active', b.textContent===coin);
  });
  document.querySelectorAll('.hmap-pane').forEach(p=>{
    p.classList.toggle('active', p.id==='hpane-'+coin);
  });
}

// Custom plugin: draw current-price horizontal line
const priceLinePlugin = {
  id: 'priceLine',
  afterDatasetsDraw(chart){
    const meta = chart.config.options._priceMeta;
    if(!meta) return;
    const {price, labels} = meta;
    // Find bar index closest to price
    let bestIdx = -1, bestDist = Infinity;
    labels.forEach((lbl, i)=>{
      const d = Math.abs(parseFloat(lbl.replace(/[^0-9.]/g,'')) - price);
      if(d < bestDist){ bestDist=d; bestIdx=i; }
    });
    if(bestIdx<0) return;
    const yScale = chart.scales.y;
    const xScale = chart.scales.x;
    const y = yScale.getPixelForValue(bestIdx);
    const ctx = chart.ctx;
    ctx.save();
    ctx.setLineDash([4,3]);
    ctx.strokeStyle = '#4fc3f7';
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.moveTo(xScale.left, y);
    ctx.lineTo(xScale.right, y);
    ctx.stroke();
    // Label
    ctx.fillStyle = '#4fc3f7';
    ctx.font = 'bold 9px monospace';
    ctx.fillText('▶ '+fmtPx(price), xScale.left+2, y-3);
    ctx.restore();
  }
};
if(typeof Chart !== 'undefined'){
  Chart.register(priceLinePlugin);
}

function renderHeatmapChart(coin, data){
  const canvas = document.getElementById('hchart-'+coin);
  if(!canvas) return;
  const zones = data.zones || [];
  const price = data.price || 0;
  if(!zones.length) return;

  // Sort ascending by price (Chart.js indexAxis:y renders bottom→top by default, we reverse)
  const sorted = [...zones].sort((a,b)=>a.price-b.price);
  const maxLiq = Math.max(...sorted.map(z=>z.liq_usd));

  const labels = sorted.map(z=> fmtPx(z.price));
  const longData  = sorted.map(z=> z.type==='long'  ? z.liq_usd : 0);
  const shortData = sorted.map(z=> z.type==='short' ? z.liq_usd : 0);

  // Intensity-based colors
  const longColors  = longData.map(v=>{
    const a = maxLiq>0 ? 0.25 + 0.75*(v/maxLiq) : 0.4;
    return `rgba(0,200,83,${a.toFixed(2)})`;
  });
  const shortColors = shortData.map(v=>{
    const a = maxLiq>0 ? 0.25 + 0.75*(v/maxLiq) : 0.4;
    return `rgba(255,23,68,${a.toFixed(2)})`;
  });

  // Destroy previous chart
  if(_liqCharts[coin]){ _liqCharts[coin].destroy(); }

  _liqCharts[coin] = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label:'Long Liq', data:longData,  backgroundColor:longColors,  borderWidth:0 },
        { label:'Short Liq', data:shortData, backgroundColor:shortColors, borderWidth:0 }
      ]
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 400 },
      _priceMeta: { price, labels },
      plugins: {
        legend: { display:false },
        tooltip: {
          callbacks: {
            label(ctx){
              const v = ctx.raw;
              if(!v) return null;
              return (ctx.dataset.label||'') + ': ' + fmtM(v);
            }
          }
        }
      },
      scales: {
        x: {
          stacked: true,
          grid: { color:'#0d1520' },
          ticks: { color:'#546e7a', font:{size:9}, callback: v=>fmtM(v) }
        },
        y: {
          stacked: true,
          grid: { color:'#0a0f1c' },
          ticks: { color:'#c9d4e0', font:{size:9}, maxTicksLimit:14 }
        }
      }
    }
  });

  // Danger zones — top 3 by liq_usd
  const top3 = [...zones].sort((a,b)=>b.liq_usd-a.liq_usd).slice(0,3);
  const dEl = document.getElementById('hdanger-'+coin);
  if(dEl){
    dEl.innerHTML = top3.map(z=>{
      const dir = z.type==='long'?'&#x25BC;':'&#x25B2;';
      const cls = z.type==='long'?'oi-neg':'oi-pos';
      return `<span class="${cls}">${dir} $${fmtPx(z.price)} &rarr; ${fmtM(z.liq_usd)}</span>`;
    }).join(' &nbsp; ');
  }
}

function renderOI(rows){
  const el = document.getElementById('oi-body');
  if(!el) return;
  if(!rows||!rows.length){ el.innerHTML='<tr><td colspan="7" class="empty">Sin datos</td></tr>'; return; }
  el.innerHTML = rows.map(r=>{
    const fCls = r.funding>0?'fund-pos':r.funding<0?'fund-neg':'fund-nu';
    const fIcon = r.funding>0.0005?'🔴':r.funding<-0.0005?'🟢':'⚪';
    const oCls = r.oi_chg>0?'oi-pos':r.oi_chg<0?'oi-neg':'oi-nu';
    const oIcon = r.oi_chg>0?'↑':r.oi_chg<0?'↓':'→';
    const lsCls = r.ls_ratio>1.05?'ls-bull':r.ls_ratio<0.95?'ls-bear':'';
    return `<tr>
      <td><b>${r.coin}</b></td>
      <td style="font-size:10px">$${fmtPx(r.price)}</td>
      <td style="font-size:10px">${fmtM(r.oi_usd)}</td>
      <td class="${oCls}" style="font-size:10px"><span class="oi-icon">${oIcon}</span>${r.oi_chg>=0?'+':''}${r.oi_chg}%</td>
      <td class="${fCls}" style="font-size:10px">${fIcon} ${r.funding>=0?'+':''}${r.funding.toFixed(3)}%</td>
      <td class="${lsCls}" style="font-size:10px">${r.ls_ratio.toFixed(2)}</td>
      <td style="font-size:10px">${fmtM(r.vol_24h)}</td>
    </tr>`;
  }).join('');
}

function renderMarket(d){
  if(d.oi_table) renderOI(d.oi_table);
  if(d.liq){
    for(const coin of ['BTC','ETH','SOL']){
      if(d.liq[coin]) renderHeatmapChart(coin, d.liq[coin]);
    }
  }
  if(d.updated_at) document.getElementById('mkt-upd-ts').textContent=d.updated_at;
}

function startMktCD(){
  clearInterval(mktTimer);
  mktCd = MKT_REFRESH;
  mktTimer = setInterval(()=>{
    mktCd--;
    const el = document.getElementById('mkt-cd');
    if(el) el.textContent = mktCd+'s';
    if(mktCd<=0){ clearInterval(mktTimer); fetchMarket(); }
  },1000);
}

function fetchMarket(){
  fetch('/api/market')
    .then(r=>r.json())
    .then(data=>{ renderMarket(data); startMktCD(); })
    .catch(()=>startMktCD());
}

// ── FILTER BAR ───────────────────────────────────────────────────────────────
let _curFilter = 'all';
function _filterShow(el, f){
  if(f==='all') return true;
  if(f==='win')  return el.dataset.pnlPos === '1';
  if(f==='lose') return el.dataset.pnlPos === '0';
  if(['15m','30m','1h','4h'].includes(f)) return el.dataset.interval === f;
  if(f==='liq') return el.dataset.type === 'liq';
  return true;
}
function applyFilter(f, btn){
  _curFilter = f;
  document.querySelectorAll('.flt-btn').forEach(b=>b.classList.remove('active'));
  if(btn) btn.classList.add('active');
  document.querySelectorAll('#bot-cards .card').forEach(card=>{
    card.style.display = _filterShow(card, f) ? '' : 'none';
  });
  document.querySelectorAll('#bt-eq-grid .bt-eq-card').forEach(card=>{
    card.style.display = _filterShow(card, f) ? '' : 'none';
  });
  document.querySelectorAll('#bt-tbody tr').forEach(row=>{
    row.style.display = _filterShow(row, f) ? '' : 'none';
  });
}

// ── TRADINGVIEW MODAL ─────────────────────────────────────────────────────────
const TV_IV = {'15m':'15','30m':'30','1h':'60','4h':'240'};
let _tvReady = false;
function _loadTVScript(cb){
  if(_tvReady){cb();return;}
  if(document.querySelector('script[src*="tv.js"]')){
    const wait=setInterval(()=>{if(window.TradingView){_tvReady=true;clearInterval(wait);cb();}},100);
    return;
  }
  const s=document.createElement('script');
  s.src='https://s3.tradingview.com/tv.js';
  s.onload=()=>{_tvReady=true;cb();};
  document.head.appendChild(s);
}
function openTVModal(coin, interval, label){
  const sym  = 'BINANCE:'+coin+'USDT';
  const iv   = TV_IV[interval] || '60';
  document.getElementById('tv-modal-title').textContent = label + ' — ' + coin + 'USDT · ' + interval;
  document.getElementById('tv-overlay').classList.add('open');
  document.body.style.overflow = 'hidden';
  const cont = document.getElementById('tv-container');
  cont.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:#37505f;font-size:12px">Cargando TradingView&hellip;</div>';
  _loadTVScript(()=>{
    cont.innerHTML = '';
    new TradingView.widget({
      autosize: true,
      symbol: sym,
      interval: iv,
      timezone: 'Etc/UTC',
      theme: 'dark',
      style: '1',
      locale: 'en',
      toolbar_bg: '#0b0f1c',
      enable_publishing: false,
      hide_side_toolbar: false,
      allow_symbol_change: true,
      container_id: 'tv-container',
    });
  });
}
function closeTVModal(){
  document.getElementById('tv-overlay').classList.remove('open');
  document.body.style.overflow = '';
  document.getElementById('tv-container').innerHTML = '';
}
document.addEventListener('keydown', e=>{ if(e.key==='Escape') closeTVModal(); });

// ── FEAR & GREED INDEX ────────────────────────────────────────────────────────
const FNG_COLORS = [[0,'#ff1744'],[25,'#ff6d00'],[50,'#ffd740'],[55,'#69f0ae'],[75,'#00e676']];
const FNG_CLASSES = {
  'Extreme Fear':'fng-ef','Fear':'fng-f','Neutral':'fng-n',
  'Greed':'fng-g','Extreme Greed':'fng-eg'
};
const FNG_ES = {
  'Extreme Fear':'Miedo Extremo','Fear':'Miedo','Neutral':'Neutral',
  'Greed':'Codicia','Extreme Greed':'Codicia Extrema'
};
const FNG_BG = {
  'Extreme Fear':'rgba(255,23,68,0.07)','Fear':'rgba(255,109,0,0.07)',
  'Neutral':'rgba(255,215,64,0.06)','Greed':'rgba(105,240,174,0.07)',
  'Extreme Greed':'rgba(0,230,118,0.09)'
};
function _fngArcPath(cx,cy,r,startDeg,endDeg){
  const toR=d=>d*Math.PI/180;
  const x1=cx+r*Math.cos(toR(startDeg));
  const y1=cy+r*Math.sin(toR(startDeg));
  const x2=cx+r*Math.cos(toR(endDeg));
  const y2=cy+r*Math.sin(toR(endDeg));
  const large=Math.abs(endDeg-startDeg)>180?1:0;
  return `M ${x1} ${y1} A ${r} ${r} 0 ${large} 1 ${x2} ${y2}`;
}
function renderFNG(val, label, ts){
  // Draw gauge arcs: 180° semicircle, 0=left, 100=right
  // Zones: 0-25 red, 25-50 orange, 50-55 yellow, 55-75 light green, 75-100 green
  const zones=[
    [0,25,'#ff1744'],[25,50,'#ff6d00'],[50,55,'#ffd740'],
    [55,75,'#69f0ae'],[75,100,'#00e676']
  ];
  const arcsG = document.getElementById('fng-arcs');
  if(!arcsG) return;
  arcsG.innerHTML = '';
  // Map value 0-100 → angle 180-360 (semicircle, left to right)
  const toAngle = v => 180 + v * 1.8;  // 0→180°, 100→360°
  zones.forEach(([s,e,color])=>{
    const p = document.createElementNS('http://www.w3.org/2000/svg','path');
    p.setAttribute('d', _fngArcPath(80,80,62, toAngle(s), toAngle(e)));
    p.setAttribute('stroke', color);
    p.setAttribute('stroke-width', '10');
    p.setAttribute('fill', 'none');
    p.setAttribute('stroke-opacity','0.9');
    arcsG.appendChild(p);
  });
  // Needle angle
  const ang = toAngle(val) * Math.PI / 180;
  const nx  = 80 + 56 * Math.cos(ang);
  const ny  = 80 + 56 * Math.sin(ang);
  const needle = document.getElementById('fng-needle');
  if(needle){ needle.setAttribute('x2', nx.toFixed(1)); needle.setAttribute('y2', ny.toFixed(1)); }
  const numSvg = document.getElementById('fng-num-svg');
  if(numSvg){ numSvg.textContent = val; }

  // Text info
  const cls = FNG_CLASSES[label] || 'nu';
  const labelEs = FNG_ES[label] || label;
  const vEl = document.getElementById('fng-val');
  const lEl = document.getElementById('fng-lbl');
  if(vEl){ vEl.textContent = val; vEl.className = 'fng-val '+cls; }
  if(lEl){ lEl.textContent = labelEs; lEl.className = 'fng-lbl '+cls; }
  const tsEl = document.getElementById('fng-ts');
  if(tsEl) tsEl.textContent = ts ? 'Actualizado: '+new Date(ts*1000).toLocaleString('es') : '';
  const wrap = document.getElementById('fng-wrap');
  if(wrap) wrap.style.background = FNG_BG[label] || '';
}
function fetchFNG(){
  fetch('https://api.alternative.me/fng/?limit=1&format=json')
    .then(r=>r.json())
    .then(d=>{
      const rec   = d.data && d.data[0];
      if(!rec) return;
      const val   = parseInt(rec.value, 10);
      const label = rec.value_classification || '';
      const ts    = parseInt(rec.timestamp, 10);
      renderFNG(val, label, ts);
    })
    .catch(()=>{
      const lEl = document.getElementById('fng-lbl');
      if(lEl) lEl.textContent = 'Sin datos';
    });
}

fetchData();
fetchMarket();
fetchFNG();
setInterval(fetchFNG, 300_000); // actualiza cada 5 min

// ── BACKTEST ──────────────────────────────────────────────────────────────────
let _btPeriod = null, _btPoll = null;
const _eqCharts = {};
let _btBarChart = null;
const BT_PERIOD_LBL = {'3m':'3 Meses','6m':'6 Meses','1y':'1 Año','max':'Máximo'};

function btLoad(period){
  if(_btPeriod === period) return;
  _btPeriod = period;
  document.querySelectorAll('.bt-tab').forEach((b,i)=>{
    b.classList.toggle('active', ['3m','6m','1y','max'][i]===period);
  });
  const resEl = document.getElementById('bt-results');
  const progEl = document.getElementById('bt-prog-wrap');
  resEl.style.display = 'none';
  progEl.style.display = 'block';
  document.getElementById('bt-pbar').style.width = '0%';
  document.getElementById('bt-status').textContent = 'Iniciando backtest en segundo plano…';
  fetch('/api/backtest/start?period='+period).catch(()=>{});
  clearInterval(_btPoll);
  _btPoll = setInterval(btPollOnce, 2500);
}

function btPollOnce(){
  if(!_btPeriod) return;
  fetch('/api/backtest?period='+_btPeriod)
    .then(r=>r.json())
    .then(d=>{
      const pct = d.progress ?? 0;
      document.getElementById('bt-pbar').style.width = Math.max(2,pct)+'%';
      if(pct < 0){
        document.getElementById('bt-status').textContent = '⚠ Error en el backtest.';
        clearInterval(_btPoll);
      } else if(pct >= 100 && d.result){
        clearInterval(_btPoll);
        document.getElementById('bt-status').textContent = 'Completado.';
        document.getElementById('bt-prog-wrap').style.display = 'none';
        btShowResults(d.result);
      } else {
        document.getElementById('bt-status').textContent = `Calculando… ${pct}% — obteniendo velas históricas y simulando estrategias`;
      }
    }).catch(()=>{});
}

function btShowResults(data){
  if(!data||!data.bots) return;
  document.getElementById('bt-results').style.display = 'block';
  const lbl = BT_PERIOD_LBL[data.period] || data.period;
  document.getElementById('bt-period-lbl').textContent = lbl + ' · ' + data.computed_at;
  btRenderEquity(data.bots);
  btRenderBar(data.bots);
  btRenderTable(data.bots);
  document.getElementById('bt-computed-at').textContent = 'Calculado: ' + data.computed_at + ' · Capital inicial $10,000 por bot';
}

function btRenderEquity(bots){
  const grid = document.getElementById('bt-eq-grid');
  grid.innerHTML = '';
  // Destroy old charts
  Object.keys(_eqCharts).forEach(k=>{ if(_eqCharts[k]){_eqCharts[k].destroy();delete _eqCharts[k];} });

  bots.forEach((b,i)=>{
    const card = document.createElement('div');
    card.className = 'bt-eq-card';
    card.dataset.interval = b.interval || 'liq';
    card.dataset.type = b.strategy==='liq'||b.interval==='liq'?'liq':'ema';
    card.dataset.pnlPos = b.total_pnl>=0?'1':'0';
    if(_curFilter !== 'all') card.style.display = _filterShow(card, _curFilter)?'':'none';
    const pCls = b.total_pnl>=0?'up':'dn';
    const sign = b.total_pnl>=0?'+':'';
    card.innerHTML = `
      <div class="bt-eq-title ${COLS[b.idx%18]}">${b.label}</div>
      <div class="bt-eq-canvas"><canvas id="bteq${i}"></canvas></div>
      <div class="bt-eq-meta">
        <span class="${pCls}">${sign}${b.total_pnl.toFixed(0)}$ (${sign}${b.total_pnl_pct.toFixed(1)}%)</span>
        <span>${b.win_rate}% wr</span>
        <span>${b.total_trades} ops</span>
        <span class="${b.max_drawdown>15?'dn':'nu'}">DD ${b.max_drawdown}%</span>
        <span class="${b.sharpe>=1?'up':b.sharpe<0?'dn':'nu'}">S ${b.sharpe}</span>
        <span class="${b.profit_factor>=1?'up':'dn'}">PF ${b.profit_factor}</span>
      </div>`;
    grid.appendChild(card);

    requestAnimationFrame(()=>{
      const cv = document.getElementById('bteq'+i);
      if(!cv||!b.equity_curve||b.equity_curve.length<2) return;
      const color = b.total_pnl>=0?'#00e676':'#ff4466';
      const bg    = b.total_pnl>=0?'rgba(0,230,118,0.07)':'rgba(255,68,102,0.07)';
      _eqCharts[i] = new Chart(cv, {
        type:'line',
        data:{
          labels: b.equity_curve.map(p=>new Date(p[0]).toLocaleDateString('es',{month:'short',day:'numeric'})),
          datasets:[{data:b.equity_curve.map(p=>p[1]),borderColor:color,borderWidth:1.5,
            fill:true,backgroundColor:bg,pointRadius:0,tension:0.25}]
        },
        options:{
          responsive:true,maintainAspectRatio:false,animation:{duration:250},
          plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>'$'+c.raw.toFixed(0)}}},
          scales:{
            x:{display:false},
            y:{grid:{color:'#0a0f1c'},ticks:{color:'#546e7a',font:{size:8},callback:v=>'$'+v.toFixed(0)}}
          }
        }
      });
    });
  });
}

function btRenderBar(bots){
  const cv = document.getElementById('bt-bar-canvas');
  if(!cv) return;
  if(_btBarChart){_btBarChart.destroy();_btBarChart=null;}
  const labels = bots.map(b=>b.label.replace('BOT·','').replace('LIQ·','LIQ·'));
  const vals   = bots.map(b=>b.total_pnl);
  const colors = vals.map(v=>v>=0?'rgba(0,230,118,0.8)':'rgba(255,68,102,0.8)');
  _btBarChart = new Chart(cv,{
    type:'bar',
    data:{labels,datasets:[{data:vals,backgroundColor:colors,borderWidth:0}]},
    options:{
      responsive:true,maintainAspectRatio:false,animation:{duration:350},
      plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>'$'+c.raw.toFixed(2)}}},
      scales:{
        x:{grid:{color:'#0d1520'},ticks:{color:'#c9d4e0',font:{size:9},maxRotation:55,minRotation:35}},
        y:{grid:{color:'#0d1520'},ticks:{color:'#546e7a',font:{size:9},callback:v=>'$'+v}}
      }
    }
  });
}

let _btSortKey = 'total_pnl', _btSortAsc = false, _btSortBots = [];
function btSortBy(key){
  if(_btSortKey === key) _btSortAsc = !_btSortAsc;
  else { _btSortKey = key; _btSortAsc = false; }
  document.querySelectorAll('#bt-tbl th[id^="btsort-"]').forEach(th=>{
    th.classList.remove('sort-asc','sort-desc');
    th.removeAttribute('id');
  });
  document.querySelectorAll('#bt-tbl th').forEach(th=>{
    if(th.getAttribute('onclick')==='btSortBy(\''+key+'\')'){
      th.id = 'btsort-'+key;
      th.classList.add(_btSortAsc?'sort-asc':'sort-desc');
    }
  });
  _btDoRenderTable(_btSortBots);
}
function _btDoRenderTable(bots){
  _btSortBots = bots;
  const sorted = [...bots].sort((a,b)=>{
    const av = a[_btSortKey], bv = b[_btSortKey];
    if(typeof av === 'string') return _btSortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
    return _btSortAsc ? av - bv : bv - av;
  });
  const tb = document.getElementById('bt-tbody');
  if(!tb) return;
  tb.innerHTML = sorted.map((b,i)=>{
    const pCls = b.total_pnl>=0?'up':'dn';
    const ddCls= b.max_drawdown>20?'dn':b.max_drawdown>10?'':'nu';
    const sCls = b.sharpe>=1.0?'up':b.sharpe<0?'dn':'nu';
    const pfCls= b.profit_factor>=1.0?'up':'dn';
    const sign = b.total_pnl>=0?'+':'';
    const bType = b.strategy==='liq'||b.interval==='liq'?'liq':'ema';
    return `<tr data-interval="${b.interval||'liq'}" data-type="${bType}" data-pnl-pos="${b.total_pnl>=0?1:0}"
      style="${_curFilter!=='all'&&!_filterShow({dataset:{interval:b.interval||'liq',type:bType,pnlPos:b.total_pnl>=0?'1':'0'}},_curFilter)?'display:none':''}">
      <td style="color:#37505f">${i+1}</td>
      <td class="${COLS[b.idx%18]}">${b.label}</td>
      <td style="font-size:10px;color:#78909c">${b.strategy}</td>
      <td class="${pCls}">${sign}${b.total_pnl.toFixed(2)}</td>
      <td class="${pCls}">${sign}${b.total_pnl_pct.toFixed(1)}%</td>
      <td>${b.win_rate}%</td>
      <td>${b.total_trades}</td>
      <td class="${ddCls}">${b.max_drawdown}%</td>
      <td class="up">+${b.best_trade.toFixed(2)}</td>
      <td class="dn">${b.worst_trade.toFixed(2)}</td>
      <td class="${sCls}">${b.sharpe}</td>
      <td class="${pfCls}">${b.profit_factor}</td>
    </tr>`;
  }).join('');
}
function btRenderTable(bots){ _btSortBots = bots; _btDoRenderTable(bots); }

// ── OPTIMIZER ─────────────────────────────────────────────────────────────────
let _optPoll = null;
function startOptPoller(){
  fetchOptResults();
  _optPoll = setInterval(fetchOptResults, 60_000);
}
function fetchOptResults(){
  fetch('/api/optimizer/results')
    .then(r=>r.json())
    .then(d=>{
      document.getElementById('opt-pending').style.display='none';
      if(d.ok && d.results && d.results.length){
        renderOptResults(d);
      } else {
        document.getElementById('opt-empty').style.display='block';
        document.getElementById('opt-results').style.display='none';
      }
    })
    .catch(()=>{
      document.getElementById('opt-pending').style.display='none';
      document.getElementById('opt-empty').style.display='block';
    });
}
function renderOptResults(d){
  document.getElementById('opt-results').style.display='block';
  document.getElementById('opt-empty').style.display='none';
  const ts = d.generated_at ? 'Generado: '+d.generated_at : '';
  document.getElementById('opt-computed-at').textContent = ts;
  document.getElementById('opt-upd-ts').textContent = ts ? ts.replace('Generado: ','') : '';
  const tb = document.getElementById('opt-tbody');
  if(!tb) return;
  const top = (d.results||[]).slice(0,30);
  tb.innerHTML = top.map((p,i)=>{
    const pCls = (p.total_pnl||0)>=0?'up':'dn';
    const sign = (p.total_pnl||0)>=0?'+':'';
    const sCls = (p.sharpe||0)>=1?'up':(p.sharpe||0)<0?'dn':'nu';
    const pfCls= (p.profit_factor||0)>=1?'up':'dn';
    return `<tr>
      <td style="color:#37505f">${i+1}</td>
      <td class="${pCls}">${sign}${(p.total_pnl||0).toFixed(2)}</td>
      <td class="${pCls}">${sign}${(p.total_pnl_pct||0).toFixed(1)}%</td>
      <td>${(p.win_rate||0).toFixed(1)}%</td>
      <td>${p.total_trades||0}</td>
      <td>${p.interval||'—'}</td>
      <td>${(p.ma_type||'').toUpperCase()}</td>
      <td>${p.ma_fast||'—'}</td>
      <td>${p.ma_slow||'—'}</td>
      <td>${p.leverage||'—'}x</td>
      <td>${((p.trailing_pct||0)*100).toFixed(1)}%</td>
      <td>${p.rsi_ob||'—'}</td>
      <td>${p.rsi_os||'—'}</td>
      <td class="${sCls}">${(p.sharpe||0).toFixed(2)}</td>
      <td class="${pfCls}">${(p.profit_factor||0).toFixed(2)}</td>
    </tr>`;
  }).join('');
}
startOptPoller();
</script>
</body>
</html>"""


# ─── HTTP HANDLER ─────────────────────────────────────────────────────────────
class DashHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/health":
            try:
                state = sim_engine.get_state()
                bots_data = state.get("bots", [])
                bots_activos = len(bots_data)
                bots_con_pos = sum(1 for b in bots_data if b["portfolio"]["positions"])
                ultima = ""
                for b in bots_data:
                    sigs = b.get("signals", [])
                    if sigs:
                        t = sigs[0].get("time", "")
                        if t > ultima:
                            ultima = t
            except Exception:
                bots_activos, bots_con_pos, ultima = 0, 0, ""
            health = {
                "status": "ok",
                "uptime_seconds": int(time.time() - _SERVER_START),
                "bots_activos": bots_activos,
                "bots_con_posicion": bots_con_pos,
                "ultima_senal": ultima,
                "version": "1.0.0",
            }
            self._respond(200, "application/json", json.dumps(health).encode())

        elif path in ("/api/status", "/api/bots"):
            try:
                data = sim_engine.get_state()
                body = json.dumps(data).encode()
            except Exception as e:
                body = json.dumps({"error": str(e), "bots": []}).encode()
            self._respond(200, "application/json", body)

        elif path == "/api/market":
            try:
                data = sim_engine.get_market_state()
                body = json.dumps(data).encode()
            except Exception as e:
                body = json.dumps({"error": str(e), "oi_table": [], "liq": {}}).encode()
            self._respond(200, "application/json", body)

        elif path == "/api/backtest":
            period = "3m"
            if "period=" in self.path:
                period = self.path.split("period=")[-1].split("&")[0].strip()
            data = {
                "progress": backtest_engine.get_progress(period),
                "result":   backtest_engine.get_result(period),
            }
            self._respond(200, "application/json", json.dumps(data).encode())

        elif path == "/api/backtest/start":
            period = "3m"
            if "period=" in self.path:
                period = self.path.split("period=")[-1].split("&")[0].strip()
            if period not in ("3m", "6m", "1y", "max"):
                period = "3m"
            backtest_engine.run_backtest_bg(period)
            self._respond(200, "application/json", b'{"ok":true}')

        elif path == "/api/optimizer/results":
            search_paths = [
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "top_params.json"),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados", "top_params.json"),
                os.path.join(os.getcwd(), "top_params.json"),
                os.path.join(os.getcwd(), "resultados", "top_params.json"),
            ]
            found = None
            for p2 in search_paths:
                if os.path.isfile(p2):
                    found = p2
                    break
            if found:
                try:
                    with open(found, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    results = data if isinstance(data, list) else data.get("results", [])
                    generated_at = "" if isinstance(data, list) else data.get("generated_at", "")
                    body = json.dumps({"ok": True, "results": results, "generated_at": generated_at}).encode()
                except Exception as e:
                    body = json.dumps({"ok": False, "error": str(e)}).encode()
            else:
                body = json.dumps({"ok": False, "results": []}).encode()
            self._respond(200, "application/json", body)

        elif path == "/api/telegram/test":
            if _tg is None:
                result = {"ok": False, "error": "telegram_alerts no disponible"}
            else:
                try:
                    result = _tg.test_connection()
                except Exception as e:
                    result = {"ok": False, "error": str(e)}
            self._respond(200, "application/json", json.dumps(result).encode())

        elif path in ("/", "/index.html"):
            self._respond(200, "text/html; charset=utf-8", DASHBOARD_HTML.encode("utf-8"))

        else:
            self._respond(404, "text/plain", b"Not Found")

    def _respond(self, code: int, ctype: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass   # silencia logs HTTP


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sim_engine.start()

    port   = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), DashHandler)
    print(f"[server] Dashboard en http://0.0.0.0:{port}")
    server.serve_forever()
