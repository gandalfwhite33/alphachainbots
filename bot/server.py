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
.card{background:#0b0f1c;border:1px solid #162030;border-radius:6px;padding:13px;border-left:3px solid #37505f}
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
/* Heatmap */
.hmap-wrap{display:flex;gap:10px;flex-wrap:wrap}
.hmap-coin{flex:1;min-width:180px}
.hmap-title{font-size:11px;font-weight:bold;margin-bottom:6px;letter-spacing:1px}
.hmap-body{position:relative;height:240px;overflow-y:auto;border:1px solid #0d1520;border-radius:4px}
.hmap-row{display:flex;align-items:center;gap:6px;padding:2px 6px;font-size:10px;border-bottom:1px solid #080e18;cursor:default}
.hmap-row:hover{background:#0d1420}
.hmap-price{width:72px;text-align:right;color:#c9d4e0;flex-shrink:0}
.hmap-bar-wrap{flex:1;height:12px;background:#0a0f1c;border-radius:2px;overflow:hidden}
.hmap-bar{height:100%;border-radius:2px;transition:width .4s}
.hmap-bar.long{background:#1a4a2e}
.hmap-bar.short{background:#3a1020}
.hmap-bar.long.big{background:#00c853}
.hmap-bar.short.big{background:#ff1744}
.hmap-amt{width:60px;text-align:right;color:#546e7a;flex-shrink:0}
.hmap-cur{background:#1a2a3a!important;border-left:3px solid #4fc3f7}
/* OI/Funding table */
.oi-pos{color:#00e676}.oi-neg{color:#ff4466}.oi-nu{color:#78909c}
.fund-pos{color:#00e676;font-weight:bold}.fund-neg{color:#ff4466;font-weight:bold}.fund-nu{color:#546e7a}
.ls-bull{color:#69f0ae}.ls-bear{color:#ff6b6b}
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

  <!-- ── MARKET DATA SECTION ─────────────────────────────────────────────── -->
  <div class="mkt-grid">

    <!-- Liquidation Heatmap -->
    <div class="panel">
      <div class="ph">
        <span>Liquidation Heatmap — BTC / ETH / SOL</span>
        <span class="mkt-upd">5 min &middot; <span id="mkt-cd">—</span> &middot; <span id="mkt-upd-ts">—</span></span>
      </div>
      <div style="padding:10px">
        <div class="hmap-wrap" id="hmap-wrap">
          <div class="hmap-coin"><div class="hmap-title col0">BTC</div><div class="hmap-body" id="hmap-BTC"><div class="empty">Cargando&hellip;</div></div></div>
          <div class="hmap-coin"><div class="hmap-title col1">ETH</div><div class="hmap-body" id="hmap-ETH"><div class="empty">Cargando&hellip;</div></div></div>
          <div class="hmap-coin"><div class="hmap-title col2">SOL</div><div class="hmap-body" id="hmap-SOL"><div class="empty">Cargando&hellip;</div></div></div>
        </div>
      </div>
    </div>

    <!-- OI + Funding + L/S Table -->
    <div class="panel">
      <div class="ph"><span>Open Interest &middot; Funding Rate &middot; L/S Ratio</span></div>
      <div class="tbl-wrap">
        <table>
          <thead><tr>
            <th>Coin</th><th>Precio</th>
            <th>OI (USD)</th><th>OI Chg 5m</th>
            <th>Funding %</th><th>L/S</th><th>Vol 24h</th>
          </tr></thead>
          <tbody id="oi-body"><tr><td colspan="7" class="empty">Cargando&hellip;</td></tr></tbody>
        </table>
      </div>
    </div>

  </div>
  <!-- ── FIN MARKET DATA ─────────────────────────────────────────────────── -->

  <div class="grid3" id="bot-cards">
    <div class="card c0"><div class="card-name col0">BOT&middot;4H</div><div class="card-meta">Cargando&hellip;</div></div>
    <div class="card c1"><div class="card-name col1">BOT&middot;1H&middot;EMA</div><div class="card-meta">Cargando&hellip;</div></div>
    <div class="card c2"><div class="card-name col2">BOT&middot;1H&middot;SMA</div><div class="card-meta">Cargando&hellip;</div></div>
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

<div class="sbar">
  <span><span class="live">&#9679; LIVE</span> &mdash; refresco cada 30s</span>
  <span id="last-upd"></span>
</div>
<div class="pbar"><div class="pfill" id="pfill" style="width:100%"></div></div>

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
  d.bots.sort((a,b)=>b.portfolio.total_pnl - a.portfolio.total_pnl);
  // Header
  const tc=pc(d.total_pnl);
  document.getElementById('hdr-stats').innerHTML=`
    <div class="stat"><div class="stat-l">Equity Total</div><div class="stat-v nu">$${fmt(d.total_equity)}</div></div>
    <div class="stat"><div class="stat-l">PnL Total</div><div class="stat-v ${tc}">${fp(d.total_pnl)}$ (${fp(d.total_pnl_pct)}%)</div></div>
    <div class="stat"><div class="stat-l">Capital Inicial</div><div class="stat-v nu">$${fmt(d.initial_equity)}</div></div>
    <div class="stat"><div class="stat-l">Uptime</div><div class="stat-v nu">${d.uptime}</div></div>
    <div class="stat"><div class="stat-l">Modo</div><div class="stat-v nu">SIMULACI&#211;N</div></div>`;

  // Bot cards
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
    return `<div class="card ${cv}">
      <div class="card-name ${cc}">${b.label} ${sBadge}</div>
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
    .then(data=>{render(data);startCD();})
    .catch(()=>startCD());
}

// ── MARKET DATA ──────────────────────────────────────────────────────────────
const MKT_REFRESH = 300;
let mktCd = MKT_REFRESH, mktTimer;

function fmtM(v){
  if(v>=1e9) return '$'+(v/1e9).toFixed(2)+'B';
  if(v>=1e6) return '$'+(v/1e6).toFixed(1)+'M';
  if(v>=1e3) return '$'+(v/1e3).toFixed(0)+'K';
  return '$'+v.toFixed(0);
}

function renderHeatmap(coin, data){
  const el = document.getElementById('hmap-'+coin);
  if(!el) return;
  const zones = data.zones || [];
  const price = data.price || 0;
  if(!zones.length){ el.innerHTML='<div class="empty">Sin datos</div>'; return; }
  const maxLiq = Math.max(...zones.map(z=>z.liq_usd));
  // Sort descending by price to show price scale top→bottom
  const sorted = [...zones].sort((a,b)=>b.price-a.price);
  // Find index closest to current price
  let curIdx = sorted.findIndex(z=>z.price<=price);
  if(curIdx<0) curIdx = sorted.length-1;
  el.innerHTML = sorted.map((z,i)=>{
    const barW  = maxLiq>0 ? Math.round(z.liq_usd/maxLiq*100) : 0;
    const isBig = z.liq_usd >= maxLiq*0.3;
    const isCur = Math.abs(i-curIdx)<=1;
    const cls   = z.type==='long'?(isBig?'long big':'long'):(isBig?'short big':'short');
    const pxStr = price>=1000? fmtPx(z.price) : fmtPx(z.price);
    const amtStr = fmtM(z.liq_usd);
    const distCls = z.type==='long'?'dn':'up';
    return `<div class="hmap-row${isCur?' hmap-cur':''}">
      <span class="hmap-price">${pxStr}</span>
      <div class="hmap-bar-wrap"><div class="hmap-bar ${cls}" style="width:${barW}%"></div></div>
      <span class="hmap-amt ${distCls}">${amtStr}</span>
    </div>`;
  }).join('');
  // Scroll to current price
  if(curIdx>=0){
    const rows = el.querySelectorAll('.hmap-row');
    if(rows[curIdx]) rows[curIdx].scrollIntoView({block:'center',behavior:'smooth'});
  }
}

function renderOI(rows){
  const el = document.getElementById('oi-body');
  if(!el) return;
  if(!rows||!rows.length){ el.innerHTML='<tr><td colspan="7" class="empty">Sin datos</td></tr>'; return; }
  el.innerHTML = rows.map(r=>{
    const fCls = r.funding>0?'fund-pos':r.funding<0?'fund-neg':'fund-nu';
    const oCls = r.oi_chg>0?'oi-pos':r.oi_chg<0?'oi-neg':'oi-nu';
    const lsCls = r.ls_ratio>1.05?'ls-bull':r.ls_ratio<0.95?'ls-bear':'';
    return `<tr>
      <td><b>${r.coin}</b></td>
      <td>$${fmtPx(r.price)}</td>
      <td>${fmtM(r.oi_usd)}</td>
      <td class="${oCls}">${r.oi_chg>=0?'+':''}${r.oi_chg}%</td>
      <td class="${fCls}">${r.funding>=0?'+':''}${r.funding.toFixed(4)}%</td>
      <td class="${lsCls}">${r.ls_ratio.toFixed(2)}</td>
      <td>${fmtM(r.vol_24h)}</td>
    </tr>`;
  }).join('');
}

function renderMarket(d){
  if(d.oi_table) renderOI(d.oi_table);
  if(d.liq){
    for(const coin of ['BTC','ETH','SOL']){
      if(d.liq[coin]) renderHeatmap(coin, d.liq[coin]);
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

fetchData();
fetchMarket();
</script>
</body>
</html>"""


# ─── HTTP HANDLER ─────────────────────────────────────────────────────────────
class DashHandler(BaseHTTPRequestHandler):

    def do_GET(self):
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
