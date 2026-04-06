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
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sim_engine
import backtest_engine


# ─── HTML EMBEBIDO ────────────────────────────────────────────────────────────
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>AlphaChainBots \xe2\x80\x94 Dashboard</title>
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
.hmap-nodata{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#263a4a;font-size:12px;pointer-events:none;background:#07090f}
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
.group-header{grid-column:1/-1;padding:8px 4px 4px;font-size:11px;font-weight:bold;letter-spacing:1.5px;color:#4fc3f7;border-bottom:1px solid #1e3a4a;margin-bottom:4px;margin-top:8px}
.group-header .group-cnt{font-weight:normal;color:#546e7a;margin-left:8px}
#bot-cards{display:flex;flex-direction:column;gap:0}
.group-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:6px}
@media(max-width:1200px){.group-grid{grid-template-columns:repeat(3,1fr)}}
@media(max-width:900px){.group-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:580px){.group-grid{grid-template-columns:1fr}}

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

/* ── CONTROL CENTER ── */
.cc-flt{display:flex;gap:6px;flex-wrap:wrap;padding:10px 14px 0;align-items:center}
.cc-cat{padding:3px 10px;border:1px solid #1e3a4a;border-radius:3px;font-size:11px;font-weight:bold;cursor:pointer;background:#0a0f1c;color:#546e7a;transition:all .2s;font-family:inherit}
.cc-cat.active{background:#0d2030;border-color:#4fc3f7;color:#4fc3f7}
.cc-cat:hover{border-color:#37505f;color:#c9d4e0}
th.srt{cursor:pointer;user-select:none}
th.srt:hover{color:#4fc3f7}
th.srt.asc::after{content:' \25B2'}
th.srt.desc::after{content:' \25BC'}

/* ── CONTROL BAR ── */
.ctrl-bar{background:#08101c;border:1px solid #162030;border-radius:6px;margin-bottom:12px;padding:10px 14px;display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.ctrl-group{display:flex;gap:5px;align-items:center;flex-wrap:wrap}
.ctrl-lbl{font-size:9px;color:#37505f;text-transform:uppercase;letter-spacing:1px;white-space:nowrap;margin-right:2px}
.ctrl-btn{padding:3px 10px;border:1px solid #1e3a4a;border-radius:3px;font-size:11px;font-weight:bold;cursor:pointer;color:#546e7a;background:#0a0f1c;font-family:inherit;transition:all .2s;letter-spacing:.5px}
.ctrl-btn.active{background:#0d2030;border-color:#4fc3f7;color:#4fc3f7}
.ctrl-btn:hover{border-color:#37505f;color:#c9d4e0}
.ctrl-sep{width:1px;background:#162030;align-self:stretch;margin:0 4px}
.ctrl-apply{padding:5px 18px;border:1px solid #1e4a2a;border-radius:3px;font-size:12px;font-weight:bold;cursor:pointer;background:#071207;color:#69f0ae;font-family:inherit;transition:all .2s}
.ctrl-apply:hover{border-color:#69f0ae;background:#0a1a0a}
.ctrl-tag{font-size:10px;color:#37505f;margin-left:6px;letter-spacing:.3px}
/* ── CARD STATS ── */
.card-micro{position:relative;height:52px;margin:4px 0 2px}
.card-filtpnl{font-size:12px;margin-bottom:2px}
.card-tag{font-size:9px;color:#37505f;margin-top:3px;letter-spacing:.2px}
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
      <div class="exec-lbl">PnL HOY</div>
      <div class="exec-val big nu" id="exec-pnl-day">—</div>
      <div class="exec-sub" id="exec-pnl-day-pct"></div>
    </div>
    <div class="exec-div"></div>
    <div class="exec-item">
      <div class="exec-lbl">Mejor Bot</div>
      <div class="exec-val up" id="exec-best" style="font-size:13px;padding-top:4px">—</div>
      <div class="exec-sub" id="exec-best-pnl"></div>
    </div>
    <div class="exec-div"></div>
    <div class="exec-item">
      <div class="exec-lbl">Peor Bot</div>
      <div class="exec-val dn" id="exec-worst" style="font-size:13px;padding-top:4px">—</div>
      <div class="exec-sub" id="exec-worst-pnl"></div>
    </div>
    <div class="exec-div"></div>
    <div class="exec-item">
      <div class="exec-lbl">Posiciones</div>
      <div class="exec-val nu" id="exec-openpos">—</div>
      <div class="exec-sub">abiertas ahora</div>
    </div>
    <div class="exec-div"></div>
    <div class="exec-item">
      <div class="exec-lbl">Bots en &#x2B;</div>
      <div class="exec-val nu" id="exec-pos-pct">—</div>
      <div class="exec-sub" id="exec-pos-sub"></div>
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
          <button class="hmap-tab" onclick="switchTab('AVAX')">AVAX</button>
          <button class="hmap-tab" onclick="switchTab('DOGE')">DOGE</button>
          <button class="hmap-tab" onclick="switchTab('ARB')">ARB</button>
          <button class="hmap-tab" onclick="switchTab('OP')">OP</button>
          <button class="hmap-tab" onclick="switchTab('WIF')">WIF</button>
          <button class="hmap-tab" onclick="switchTab('SUI')">SUI</button>
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
        <div id="hpane-AVAX" class="hmap-pane">
          <div class="hmap-canvas-wrap"><canvas id="hchart-AVAX"></canvas></div>
          <div class="hmap-danger" id="hdanger-AVAX"></div>
        </div>
        <div id="hpane-DOGE" class="hmap-pane">
          <div class="hmap-canvas-wrap"><canvas id="hchart-DOGE"></canvas></div>
          <div class="hmap-danger" id="hdanger-DOGE"></div>
        </div>
        <div id="hpane-ARB" class="hmap-pane">
          <div class="hmap-canvas-wrap"><canvas id="hchart-ARB"></canvas></div>
          <div class="hmap-danger" id="hdanger-ARB"></div>
        </div>
        <div id="hpane-OP" class="hmap-pane">
          <div class="hmap-canvas-wrap"><canvas id="hchart-OP"></canvas></div>
          <div class="hmap-danger" id="hdanger-OP"></div>
        </div>
        <div id="hpane-WIF" class="hmap-pane">
          <div class="hmap-canvas-wrap"><canvas id="hchart-WIF"></canvas></div>
          <div class="hmap-danger" id="hdanger-WIF"></div>
        </div>
        <div id="hpane-SUI" class="hmap-pane">
          <div class="hmap-canvas-wrap"><canvas id="hchart-SUI"></canvas></div>
          <div class="hmap-danger" id="hdanger-SUI"></div>
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

  <!-- ── CONTROL BAR ─────────────────────────────────────────────────────── -->
  <div class="ctrl-bar" id="ctrl-bar">
    <div class="ctrl-group">
      <span class="ctrl-lbl">Periodo:</span>
      <button class="ctrl-btn active" onclick="selectPeriod('3m',this)">3M</button>
      <button class="ctrl-btn" onclick="selectPeriod('6m',this)">6M</button>
      <button class="ctrl-btn" onclick="selectPeriod('1y',this)">1A</button>
      <button class="ctrl-btn" onclick="selectPeriod('max',this)">M&Aacute;X</button>
    </div>
    <div class="ctrl-sep"></div>
    <div class="ctrl-group">
      <span class="ctrl-lbl">Monedas:</span>
      <button class="ctrl-btn active" onclick="toggleCtrlCoin('BTC',this)">BTC</button>
      <button class="ctrl-btn" onclick="toggleCtrlCoin('ETH',this)">ETH</button>
      <button class="ctrl-btn" onclick="toggleCtrlCoin('SOL',this)">SOL</button>
      <button class="ctrl-btn" onclick="toggleCtrlCoin('ARB',this)">ARB</button>
      <button class="ctrl-btn" onclick="toggleCtrlCoin('OP',this)">OP</button>
      <button class="ctrl-btn" onclick="toggleCtrlCoin('AVAX',this)">AVAX</button>
      <button class="ctrl-btn" onclick="toggleCtrlCoin('DOGE',this)">DOGE</button>
      <button class="ctrl-btn" onclick="toggleCtrlCoin('WIF',this)">WIF</button>
      <button class="ctrl-btn" onclick="toggleCtrlCoin('SUI',this)">SUI</button>
    </div>
    <div class="ctrl-sep"></div>
    <button class="ctrl-apply" onclick="applyControl()">&#x25BA; Aplicar</button>
    <span class="ctrl-tag" id="ctrl-tag">&#x1F4CA; BTC &middot; 3M</span>
  </div>
  <!-- ── FIN CONTROL BAR ──────────────────────────────────────────────────── -->

  <!-- ── BACKTEST SECTION ────────────────────────────────────────────────── -->
  <div class="panel" id="bt-section">
    <div class="ph bt-ph">
      <span>&#x1F4CA; Backtest Hist&oacute;rico</span>
      <span class="ph-cnt" id="bt-period-hdr"></span>
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
        <table id="bt-tbl">
          <thead><tr>
            <th>#</th><th>Bot</th><th>Estrategia</th>
            <th>PnL $</th><th>PnL %</th><th>Win%</th>
            <th>Trades</th><th>Max DD%</th>
            <th>Mejor $</th><th>Peor $</th>
            <th>Sharpe</th><th>PF</th>
          </tr></thead>
          <tbody id="bt-tbody"></tbody>
        </table>
      </div>
      <div style="font-size:9px;color:#263a4a;padding:4px 10px 10px" id="bt-computed-at"></div>
    </div>
  </div>
  <!-- ── FIN BACKTEST ─────────────────────────────────────────────────────── -->

  <!-- ── CONTROL CENTER ───────────────────────────────────────────────────── -->
  <div class="panel" id="cc-section">
    <div class="ph">
      <span>&#x1F3DB; Control Center &mdash; Ranking Live</span>
      <span class="ph-cnt" id="cc-cnt"></span>
    </div>
    <div class="cc-flt">
      <span class="flt-lbl">Categor&iacute;a:</span>
      <button class="cc-cat active" onclick="ccFilter('all',this)">Todos</button>
      <button class="cc-cat" onclick="ccFilter('Legacy',this)">Legacy</button>
      <button class="cc-cat" onclick="ccFilter('BTC LONG',this)">BTC LONG</button>
      <button class="cc-cat" onclick="ccFilter('BTC SHORT',this)">BTC SHORT</button>
      <button class="cc-cat" onclick="ccFilter('BTC L+S',this)">BTC L+S</button>
      <button class="cc-cat" onclick="ccFilter('ETH LONG',this)">ETH LONG</button>
      <button class="cc-cat" onclick="ccFilter('ETH SHORT',this)">ETH SHORT</button>
      <button class="cc-cat" onclick="ccFilter('ETH L+S',this)">ETH L+S</button>
      <button class="cc-cat" onclick="ccFilter('Liquidaciones',this)">Liquidaciones</button>
    </div>
    <div class="tbl-wrap" style="padding:0 14px 10px">
      <table id="cc-table">
        <thead><tr>
          <th>#</th>
          <th class="srt" data-col="label" onclick="ccSort('label')">Nombre</th>
          <th class="srt" data-col="cat" onclick="ccSort('cat')">Categor&iacute;a</th>
          <th class="srt" data-col="interval" onclick="ccSort('interval')">TF</th>
          <th class="srt" data-col="pnl" onclick="ccSort('pnl')">PnL $</th>
          <th class="srt" data-col="pnl_pct" onclick="ccSort('pnl_pct')">PnL %</th>
          <th class="srt" data-col="wr" onclick="ccSort('wr')">WinRate%</th>
          <th class="srt" data-col="dd" onclick="ccSort('dd')">MaxDD%</th>
          <th class="srt" data-col="sharpe" onclick="ccSort('sharpe')">Sharpe</th>
          <th class="srt" data-col="score" onclick="ccSort('score')">Score</th>
          <th>Estado</th>
        </tr></thead>
        <tbody id="cc-tbody"><tr><td colspan="11" class="empty">Cargando&hellip;</td></tr></tbody>
      </table>
    </div>
  </div>
  <!-- ── FIN CONTROL CENTER ────────────────────────────────────────────────── -->

  <!-- ── FILTER BAR ────────────────────────────────────────────────────────── -->
  <div class="flt-bar">
    <span class="flt-lbl">Filtrar:</span>
    <button class="flt-btn active" onclick="applyFilter('all',this)">Todos</button>
    <button class="flt-btn" onclick="applyFilter('win',this)">&#x2B; Ganadores</button>
    <button class="flt-btn" onclick="applyFilter('lose',this)">&#x2212; Perdedores</button>
    <button class="flt-btn" onclick="applyFilter('btc',this)">BTC</button>
    <button class="flt-btn" onclick="applyFilter('eth',this)">ETH</button>
    <button class="flt-btn" onclick="applyFilter('long',this)">LONG</button>
    <button class="flt-btn" onclick="applyFilter('short',this)">SHORT</button>
    <button class="flt-btn" onclick="applyFilter('ls',this)">L+S</button>
    <button class="flt-btn" onclick="applyFilter('15m',this)">15m</button>
    <button class="flt-btn" onclick="applyFilter('30m',this)">30m</button>
    <button class="flt-btn" onclick="applyFilter('1h',this)">1h</button>
    <button class="flt-btn" onclick="applyFilter('4h',this)">4h</button>
    <button class="flt-btn" onclick="applyFilter('liq',this)">Liquidaciones</button>
  </div>

  <div id="bot-cards">
    <div class="card c0"><div class="card-name col0">BOT&middot;4H</div><div class="card-meta">Cargando&hellip;</div></div>
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

let _lastD = null;
function render(d){
  _lastD = d;
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
  const today = new Date().toLocaleDateString('es');
  let dayPnl = 0;
  d.bots.forEach(b => b.portfolio.history.forEach(t => {
    if(t.closed_at && t.closed_at.startsWith(today)) dayPnl += (t.pnl||0);
  }));
  const dpCls = pc(dayPnl);
  document.getElementById('exec-pnl-day').className = 'exec-val big '+dpCls;
  document.getElementById('exec-pnl-day').textContent = (dayPnl>=0?'+':'') + dayPnl.toFixed(2) + '$';
  const dayPct = d.initial_equity>0 ? dayPnl/d.initial_equity*100 : 0;
  document.getElementById('exec-pnl-day-pct').textContent = (dayPct>=0?'+':'') + dayPct.toFixed(2) + '%';

  const byPnl = [...d.bots].sort((a,b)=>b.portfolio.total_pnl - a.portfolio.total_pnl);
  const best  = byPnl[0], worst = byPnl[byPnl.length-1];
  if(best){
    document.getElementById('exec-best').textContent = best.label;
    document.getElementById('exec-best-pnl').textContent = '+$'+best.portfolio.total_pnl.toFixed(0);
  }
  if(worst){
    document.getElementById('exec-worst').textContent = worst.label;
    document.getElementById('exec-worst-pnl').textContent = '$'+worst.portfolio.total_pnl.toFixed(0);
  }

  let totalOpen = 0;
  d.bots.forEach(b => totalOpen += b.portfolio.positions.length);
  document.getElementById('exec-openpos').textContent = totalOpen;

  const nPos = d.bots.filter(b=>b.portfolio.total_pnl>0).length;
  const nTot = d.bots.length;
  const pPct = nTot>0 ? Math.round(nPos/nTot*100) : 0;
  document.getElementById('exec-pos-pct').className = 'exec-val '+(pPct>=50?'up':'dn');
  document.getElementById('exec-pos-pct').textContent = pPct+'%';
  document.getElementById('exec-pos-sub').textContent = nPos+'/'+nTot+' bots';

  // ── Bot cards ─────────────────────────────────────────────────────────────
  const coinTag = [..._ctrlCoins].join(' \xB7 ');
  const periodTag = _ctrlPeriod ? _ctrlPeriod.toUpperCase() : '3M';

  // Group definitions: label → idx range (inclusive)
  const BOT_GROUPS = [
    { key:'legacy',   label:'LEGACY',        from:0,  to:35,  icon:'\xF0\x9F\x92\xBC' },
    { key:'btc-long', label:'BTC LONG',       from:36, to:39,  icon:'\xF0\x9F\x9F\xA2' },
    { key:'btc-short',label:'BTC SHORT',      from:40, to:43,  icon:'\xF0\x9F\x94\xB4' },
    { key:'btc-ls',   label:'BTC LONG+SHORT', from:44, to:45,  icon:'\xF0\x9F\x9F\xA1' },
    { key:'eth-long', label:'ETH LONG',       from:46, to:49,  icon:'\xF0\x9F\x9F\xA2' },
    { key:'eth-short',label:'ETH SHORT',      from:50, to:52,  icon:'\xF0\x9F\x94\xB4' },
    { key:'eth-ls',   label:'ETH LONG+SHORT', from:53, to:56,  icon:'\xF0\x9F\x9F\xA1' },
    { key:'liq',      label:'LIQUIDACIONES',  from:18, to:25,  icon:'\u26A1' },
  ];

  function _botGroup(idx){
    for(const g of BOT_GROUPS) if(idx>=g.from && idx<=g.to) return g.key;
    return 'legacy';
  }

  function _cardHtml(b){
    const p=b.portfolio, cc=COLS[b.idx%18], cv=CARDS[b.idx%18];
    const ma=b.ma_type==='liq'?'LIQ\xB7'+(b.strategy||'').toUpperCase()
             :(b.ma_type==='ema'?'EMA':'SMA')+' '+b.ma_fast+'/'+b.ma_slow;
    const tr=(b.trailing_pct*100).toFixed(1);
    const sBadge=b.status==='escaneando'
      ?'<span class="b b-entry">SCAN</span>'
      :'<span class="b b-wait">'+b.status+'</span>';
    const bType = b.ma_type==='liq'?'liq':'ema';
    const firstCoin = (b.coins&&b.coins[0])||'BTC';
    const bDir = b.direction||'both';
    const bCoin = firstCoin.toLowerCase();
    const fStats = _filteredStats(p, _ctrlCoins);
    const fPnlCls = pc(fStats.pnl);
    const hasMini = fStats.hist.length >= 2;
    const grp = _botGroup(b.idx);
    return `<div class="card ${cv}" data-interval="${b.interval}" data-pnl-pos="${p.total_pnl>=0?1:0}" data-type="${bType}" data-dir="${bDir}" data-coin="${bCoin}" data-group="${grp}"
      onclick="openTVModal('${firstCoin}','${b.interval}','${b.label}')">
      <div class="card-name ${cc}">${b.label} ${sBadge}</div>
      <div class="card-meta">${ma} &middot; ${b.interval} &middot; trailing ${tr}%</div>
      <div class="card-eq">$${fmt(p.equity)}</div>
      <div class="card-pnl ${fPnlCls}">${fp(fStats.pnl)}$ (${fStats.wr.toFixed(1)}% wr &middot; ${fStats.trades} ops)</div>
      <div class="card-row">
        <span>${p.positions.length} pos.</span>
        <span>scan: ${b.last_scan}</span>
        ${b.errors?`<span class="dn">${b.errors} err</span>`:''}
      </div>
      ${hasMini?`<div class="card-micro"><canvas id="mini-${b.idx}"></canvas></div>`:''}
      <div class="card-tag">&#x1F4CA; ${coinTag} | ${periodTag}</div>
    </div>`;
  }

  // Build grouped HTML
  let cardsHtml = '';
  for(const g of BOT_GROUPS){
    const groupBots = d.bots.filter(b=>b.idx>=g.from && b.idx<=g.to);
    if(!groupBots.length) continue;
    cardsHtml += `<div class="group-header" data-group="${g.key}">${g.icon} ${g.label} <span class="group-cnt">${groupBots.length} bots</span></div>`;
    cardsHtml += `<div class="grid3 group-grid" data-group="${g.key}">`;
    cardsHtml += groupBots.map(_cardHtml).join('');
    cardsHtml += '</div>';
  }
  document.getElementById('bot-cards').innerHTML = cardsHtml;
  // Render mini charts after DOM update
  requestAnimationFrame(()=>{
    d.bots.forEach(b=>{
      const hist = _filteredStats(b.portfolio, _ctrlCoins).hist;
      if(hist.length >= 2) renderMiniChart(b.idx, hist);
    });
  });
  try{ renderControlCenter(d); }catch(e){ console.error('[CC]',e); }

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

// ── CONTROL CENTER ────────────────────────────────────────────────────────────
function ccCat(label, idx){
  if(label.startsWith('LIQ\xB7')) return 'Liquidaciones';
  if(idx>=36&&idx<=39) return 'BTC LONG';
  if(idx>=40&&idx<=43) return 'BTC SHORT';
  if(idx>=44&&idx<=45) return 'BTC L+S';
  if(idx>=46&&idx<=49) return 'ETH LONG';
  if(idx>=50&&idx<=52) return 'ETH SHORT';
  if(idx>=53&&idx<=56) return 'ETH L+S';
  return 'Legacy';
}
function _ddFromHist(hist, initEq){
  let eq=initEq, peak=initEq, maxDD=0;
  [...hist].sort((a,b2)=>a.ts-b2.ts).forEach(t=>{
    eq+=(t.pnl||0);
    if(eq>peak) peak=eq;
    const dd=peak>0?(peak-eq)/peak*100:0;
    if(dd>maxDD) maxDD=dd;
  });
  return maxDD;
}
function _sharpeFromHist(hist, initEq){
  const days={};
  [...hist].sort((a,b2)=>a.ts-b2.ts).forEach(t=>{
    const d=(t.closed_at||'').slice(0,10);
    if(d) days[d]=(days[d]||0)+(t.pnl||0);
  });
  const rets=Object.values(days).map(v=>v/initEq*100);
  if(rets.length<2) return 0;
  const mean=rets.reduce((s,v)=>s+v,0)/rets.length;
  const std=Math.sqrt(rets.reduce((s,v)=>s+(v-mean)**2,0)/rets.length)||0;
  return std===0?0:+(mean/std*Math.sqrt(365)).toFixed(2);
}
let _ccSortState={col:'score',dir:-1};
let _ccCatFilter='all';
let _ccData=[];
function ccSort(col){
  if(_ccSortState.col===col) _ccSortState.dir*=-1; else {_ccSortState.col=col;_ccSortState.dir=-1;}
  document.querySelectorAll('#cc-table th.srt').forEach(th=>{
    th.classList.remove('asc','desc');
    if(th.dataset.col===col) th.classList.add(_ccSortState.dir>0?'asc':'desc');
  });
  _ccRender();
}
function ccFilter(cat,btn){
  _ccCatFilter=cat;
  document.querySelectorAll('.cc-cat').forEach(b=>b.classList.remove('active'));
  if(btn) btn.classList.add('active');
  _ccRender();
}
function _ccRender(){
  const tb=document.getElementById('cc-tbody');
  if(!tb) return;
  let rows=_ccCatFilter==='all'?[..._ccData]:_ccData.filter(r=>r.cat===_ccCatFilter);
  const {col,dir}=_ccSortState;
  rows.sort((a,b2)=>{
    const va=typeof a[col]==='string'?a[col]:+a[col];
    const vb=typeof b2[col]==='string'?b2[col]:+b2[col];
    return va<vb?dir:va>vb?-dir:0;
  });
  const cnt=document.getElementById('cc-cnt');
  if(cnt) cnt.textContent=rows.length+' bots';
  if(!rows.length){
    tb.innerHTML='<tr><td colspan="11" class="empty">Sin datos \u2014 ejecuta un backtest (3M/6M/1A/M\xC1X) para ver el ranking.</td></tr>';
    return;
  }
  tb.innerHTML=rows.map((r,i)=>{
    const pCls=r.pnl>=0?'up':'dn', sign=r.pnl>=0?'+':'';
    const warn=r.pnl<0?'<span style="color:#ff4466;font-size:10px">\u26A0 Revisar</span>':'<span style="color:#00e676;font-size:10px">OK</span>';
    const wrTxt  = r.wr!==null     ? r.wr.toFixed(1)+'%'    : '<span style="color:#37505f">--</span>';
    const ddTxt  = r.dd!==null     ? r.dd.toFixed(1)+'%'    : '<span style="color:#37505f">--</span>';
    const shTxt  = r.sharpe!==null ? (r.sharpe>5?'\u2B50':'')+r.sharpe : '<span style="color:#37505f">--</span>';
    const ddCls  = r.dd!==null&&r.dd>20?'dn':r.dd!==null&&r.dd>10?'':'nu';
    const sCls   = r.sharpe!==null&&r.sharpe>=1?'up':r.sharpe!==null&&r.sharpe<0?'dn':'nu';
    const srcTxt = r.hasBt?'<span style="font-size:9px;color:#37505f">BT</span>':'<span style="font-size:9px;color:#263a4a">live</span>';
    return `<tr>
      <td style="color:#37505f">${i+1}</td>
      <td class="${COLS[r.idx%18]}">${r.label}</td>
      <td style="font-size:10px;color:#78909c">${r.cat}</td>
      <td style="font-size:10px">${r.interval}</td>
      <td class="${pCls}">${sign}${r.pnl.toFixed(2)}</td>
      <td class="${pCls}">${sign}${r.pnl_pct.toFixed(1)}%</td>
      <td>${wrTxt}</td>
      <td class="${ddCls}">${ddTxt}</td>
      <td class="${sCls}">${shTxt}</td>
      <td style="font-size:10px;color:#78909c">${srcTxt} ${r.score.toFixed(2)}</td>
      <td>${warn}</td>
    </tr>`;
  }).join('');
}
function renderControlCenter(d){
  if(!d||!d.bots) return;
  // Build backtest lookup by bot idx (use latest backtest if available)
  const btMap={};
  if(_btResults&&_btResults.bots){
    _btResults.bots.forEach(b=>{ btMap[b.idx]=b; });
  }
  _ccData=d.bots.map(b=>{
    const p=b.portfolio||{};
    const bt=btMap[b.idx];       // backtest row for this bot (may be undefined)
    const hasBt=!!bt;
    // Prefer backtest stats; fall back to live portfolio
    const pnl    = hasBt ? bt.total_pnl     : (p.total_pnl||0);
    const pnl_pct= hasBt ? bt.total_pnl_pct : (p.total_pnl_pct||0);
    const wr     = hasBt ? bt.win_rate       : ((p.trades||0)>0?(p.wins||0)/(p.trades||1)*100:null);
    const dd     = hasBt ? bt.max_drawdown   : null;
    const sharpe = hasBt ? bt.sharpe         : null;
    const trades = hasBt ? bt.total_trades   : (p.trades||0);
    // Score only meaningful when we have real metrics
    const score  = (wr!==null&&dd!==null) ? (pnl_pct*(wr/100))/Math.max(dd,1) : pnl_pct;
    const status = p.positions&&p.positions.length>0 ? 'En posici\xF3n' : (b.status||'OK');
    return {idx:b.idx,label:b.label||'',cat:ccCat(b.label||'',b.idx),
            interval:b.interval||'',pnl,pnl_pct,wr,dd,sharpe,trades,score,hasBt,status};
  });
  _ccRender();
}

// ── CONTROL BAR STATE ─────────────────────────────────────────────────────────
let _ctrlPeriod = '3m';
const _ctrlCoins = new Set(['BTC']);

function selectPeriod(p, btn){
  _ctrlPeriod = p;
  document.querySelectorAll('#ctrl-bar .ctrl-group:first-child .ctrl-btn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
}

function toggleCtrlCoin(coin, btn){
  if(_ctrlCoins.has(coin)){
    if(_ctrlCoins.size===1) return; // at least 1 required
    _ctrlCoins.delete(coin);
    btn.classList.remove('active');
  } else {
    _ctrlCoins.add(coin);
    btn.classList.add('active');
  }
}

function applyControl(){
  // Update tag in ctrl-bar
  const coinTag = [..._ctrlCoins].join(' \xB7 ');
  const tEl = document.getElementById('ctrl-tag');
  if(tEl) tEl.textContent = '\U0001F4CA '+coinTag+' | '+(_ctrlPeriod||'3m').toUpperCase();
  // Update bt-section header label
  const hEl = document.getElementById('bt-period-hdr');
  if(hEl) hEl.textContent = (_ctrlPeriod||'3m').toUpperCase()+' \xB7 '+coinTag;
  // Run backtest for selected period
  if(_ctrlPeriod) btLoad(_ctrlPeriod);
  // Re-render bot cards with new coin filter
  if(_lastD) render(_lastD);
}

// ── MICRO CHARTS ──────────────────────────────────────────────────────────────
const _miniCharts={};

function _filteredStats(portfolio, coinSet){
  const hist=(portfolio.history||[]).filter(t=>coinSet.has(t.coin));
  const pnl=hist.reduce((s,t)=>s+(t.pnl||0),0);
  const wins=hist.filter(t=>(t.pnl||0)>0).length;
  const wr=hist.length>0?wins/hist.length*100:0;
  return {pnl,wr,trades:hist.length,wins,hist};
}

function renderMiniChart(idx,hist){
  const cv=document.getElementById('mini-'+idx);
  if(!cv) return;
  if(_miniCharts[idx]){_miniCharts[idx].destroy();delete _miniCharts[idx];}
  if(!hist||hist.length<2) return;
  const sorted=[...hist].sort((a,b2)=>a.ts-b2.ts);
  let eq=10000;
  const pts=sorted.map(t=>{eq+=(t.pnl||0);return eq;});
  const color=pts[pts.length-1]>=pts[0]?'#00e676':'#ff4466';
  _miniCharts[idx]=new Chart(cv,{
    type:'line',
    data:{labels:pts.map((_,i)=>i),datasets:[{data:pts,borderColor:color,borderWidth:1.2,
      fill:false,pointRadius:0,tension:0.2}]},
    options:{responsive:true,maintainAspectRatio:false,animation:{duration:0},
      plugins:{legend:{display:false},tooltip:{enabled:false}},
      scales:{x:{display:false},y:{display:false}}}
  });
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
    .then(data=>{render(data);startCD();})
    .catch(()=>startCD());
}

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

function _hmapNodata(coin, msg){
  const wrap = document.getElementById('hchart-'+coin)?.parentElement;
  if(!wrap) return;
  let nd = wrap.querySelector('.hmap-nodata');
  if(!nd){ nd = document.createElement('div'); nd.className='hmap-nodata'; wrap.appendChild(nd); }
  nd.textContent = msg || '';
  nd.style.display = msg ? 'flex' : 'none';
}

function renderHeatmapChart(coin, data){
  const canvas = document.getElementById('hchart-'+coin);
  if(!canvas) return;
  const zones = data.zones || [];
  const price = data.price || 0;
  if(!zones.length){ _hmapNodata(coin, 'Sin datos de liquidaciones'); return; }
  _hmapNodata(coin, '');

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
    for(const coin of ['BTC','ETH','SOL','AVAX','DOGE','ARB','OP','WIF','SUI']){
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
    .then(data=>{renderMarket(data);startMktCD();})
    .catch(()=>startMktCD());
}

// ── FILTER BAR ───────────────────────────────────────────────────────────────
let _curFilter = 'all';
function applyFilter(f, btn){
  _curFilter = f;
  document.querySelectorAll('.flt-btn').forEach(b=>b.classList.remove('active'));
  if(btn) btn.classList.add('active');
  document.querySelectorAll('#bot-cards .card').forEach(card=>{
    let show = true;
    if(f==='win')   show = card.dataset.pnlPos === '1';
    else if(f==='lose')  show = card.dataset.pnlPos === '0';
    else if(f==='btc')   show = card.dataset.coin === 'btc';
    else if(f==='eth')   show = card.dataset.coin === 'eth';
    else if(f==='long')  show = card.dataset.dir === 'long';
    else if(f==='short') show = card.dataset.dir === 'short';
    else if(f==='ls')    show = card.dataset.dir === 'both';
    else if(['15m','30m','1h','2h','4h'].includes(f)) show = card.dataset.interval === f;
    else if(f==='liq')   show = card.dataset.type === 'liq';
    card.style.display = show ? '' : 'none';
  });
  // Show/hide group headers based on visible cards
  document.querySelectorAll('#bot-cards .group-grid').forEach(grid=>{
    const anyVisible = [...grid.querySelectorAll('.card')].some(c=>c.style.display!=='none');
    const hdr = document.querySelector(`.group-header[data-group="${grid.dataset.group}"]`);
    if(hdr) hdr.style.display = anyVisible ? '' : 'none';
    grid.style.display = anyVisible ? '' : 'none';
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
  const vEl = document.getElementById('fng-val');
  const lEl = document.getElementById('fng-lbl');
  if(vEl){ vEl.textContent = val; vEl.className = 'fng-val '+cls; }
  if(lEl){ lEl.textContent = label; lEl.className = 'fng-lbl '+cls; }
  const tsEl = document.getElementById('fng-ts');
  if(tsEl) tsEl.textContent = ts ? 'Actualizado: '+new Date(ts*1000).toLocaleString('es') : '';
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

for(const _c of ['BTC','ETH','SOL','AVAX','DOGE','ARB','OP','WIF','SUI']) _hmapNodata(_c,'Cargando datos\u2026');
fetchData();
fetchMarket();
fetchFNG();
setInterval(fetchFNG, 600_000); // actualiza cada 10 min

// ── BACKTEST ──────────────────────────────────────────────────────────────────
let _btPeriod = null, _btPoll = null;
const _eqCharts = {};
let _btBarChart = null;
let _btResults = null; // latest backtest results (used by Control Center)
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
  _btResults = data; // store for Control Center
  document.getElementById('bt-results').style.display = 'block';
  const lbl = BT_PERIOD_LBL[data.period] || data.period;
  document.getElementById('bt-period-lbl').textContent = lbl + ' · ' + data.computed_at;
  btRenderEquity(data.bots);
  btRenderBar(data.bots);
  btRenderTable(data.bots);
  document.getElementById('bt-computed-at').textContent = 'Calculado: ' + data.computed_at + ' · Capital inicial $10,000 por bot';
  // Refresh Control Center with backtest data
  if(_lastD) try{ renderControlCenter(_lastD); }catch(e){}
}

function btRenderEquity(bots){
  const grid = document.getElementById('bt-eq-grid');
  grid.innerHTML = '';
  // Destroy old charts
  Object.keys(_eqCharts).forEach(k=>{ if(_eqCharts[k]){_eqCharts[k].destroy();delete _eqCharts[k];} });

  bots.forEach((b,i)=>{
    const card = document.createElement('div');
    card.className = 'bt-eq-card';
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

function btRenderTable(bots){
  const tb = document.getElementById('bt-tbody');
  if(!tb) return;
  tb.innerHTML = bots.map((b,i)=>{
    const pCls = b.total_pnl>=0?'up':'dn';
    const ddCls= b.max_drawdown>20?'dn':b.max_drawdown>10?'':'nu';
    const sCls = b.sharpe>=1.0?'up':b.sharpe<0?'dn':'nu';
    const pfCls= b.profit_factor>=1.0?'up':'dn';
    const sign = b.total_pnl>=0?'+':'';
    return `<tr>
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
</script>
</body>
</html>"""


# ─── HTTP HANDLER ─────────────────────────────────────────────────────────────
class DashHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        try:
            self._do_GET_inner()
        except Exception as exc:
            log.exception("do_GET unhandled error: %s", exc)
            try:
                self._respond(500, "text/plain", b"Internal Server Error")
            except Exception:
                pass

    def _do_GET_inner(self):
        path = self.path.split("?")[0]

        if path == "/health":
            self._respond(200, "text/plain", b"OK")

        elif path == "/api/status":
            try:
                data = sim_engine.get_state()
                body = json.dumps(data).encode()
            except Exception as e:
                body = json.dumps({"error": str(e)}).encode()
            self._respond(200, "application/json", body)

        elif path == "/api/market":
            try:
                data = sim_engine.get_market_state()
                body = json.dumps(data).encode()
            except Exception as e:
                body = json.dumps({"error": str(e)}).encode()
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

        elif path in ("/", "/index.html"):
            _html = DASHBOARD_HTML.encode("utf-8", errors="ignore").decode("utf-8")
            self._respond(200, "text/html; charset=utf-8", _html.encode("utf-8", errors="replace"))

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
    # Clear backtest cache on every startup so results always reflect current data
    backtest_engine._cache.clear()
    backtest_engine._progress.clear()
    backtest_engine._running.clear()

    sim_engine.start()

    port   = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), DashHandler)
    print(f"[server] Dashboard en http://0.0.0.0:{port}")
    server.serve_forever()
