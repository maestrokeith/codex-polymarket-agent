import React, { useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Activity, BrainCircuit, CircleDollarSign, Database, Radar, ShieldCheck, Wifi, WifiOff } from 'lucide-react';
import './styles.css';

type Candidate = { mint:string; symbol?:string; venue:string; stage:string; decision?:'BUY'|'REJECT'; confidence?:number; price_usd?:number; liquidity_usd?:number; volume_5m?:number; buyers_5m?:number; sellers_5m?:number; reasons?:string[]; updated_at?:string };
type Position = { id:string; mint:string; symbol?:string; quantity:number; entry_price:number; mark_price:number; unrealized_pnl:number; stop_price:number; take_profit_price:number; opened_at:string };
type Snapshot = { status:string; candidates:Candidate[]; positions:Position[]; metrics:{equity:number;realized_pnl:number;unrealized_pnl:number;win_rate:number;decisions:number;rejects:number;buys:number}; events:Array<{ts:string;kind:string;message:string}> };

const httpBase=import.meta.env.VITE_STRIX_ENGINE_HTTP||'http://127.0.0.1:8000';
const wsUrl=import.meta.env.VITE_STRIX_ENGINE_WS||httpBase.replace(/^http/,'ws')+'/ws';
const initial:Snapshot={status:'connecting',candidates:[],positions:[],metrics:{equity:1000,realized_pnl:0,unrealized_pnl:0,win_rate:0,decisions:0,rejects:0,buys:0},events:[]};
function money(v:number){return new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',maximumFractionDigits:2}).format(v||0)}
function shortMint(m:string){return m?.length>12?`${m.slice(0,5)}…${m.slice(-5)}`:m}

function App(){
  const [data,setData]=useState<Snapshot>(initial); const [online,setOnline]=useState(false); const retry=useRef<number|undefined>();
  useEffect(()=>{let stopped=false; let socket:WebSocket|undefined;
    const pull=async()=>{try{const r=await fetch(`${httpBase}/v1/snapshot`); if(r.ok&&!stopped)setData(await r.json())}catch{}};
    const connect=()=>{if(stopped)return; socket=new WebSocket(wsUrl); socket.onopen=()=>{setOnline(true);pull()}; socket.onmessage=e=>{try{const msg=JSON.parse(e.data); if(msg.type==='snapshot')setData(msg.data); else if(msg.type==='event')setData(d=>({...d,events:[msg.data,...d.events].slice(0,80)}))}catch{}}; socket.onclose=()=>{setOnline(false);retry.current=window.setTimeout(connect,2000)}; socket.onerror=()=>socket?.close()};
    pull(); connect(); const poll=window.setInterval(pull,10000); return()=>{stopped=true;socket?.close();clearInterval(poll);if(retry.current)clearTimeout(retry.current)};
  },[]);
  const rejectionRate=useMemo(()=>data.metrics.decisions?data.metrics.rejects/data.metrics.decisions:0,[data.metrics]);
  return <div className="shell"><header><div><div className="eyebrow">SOLANA INTELLIGENCE / PAPER EXECUTION</div><h1>STRIX <span>Research Desk</span></h1></div><div className={`status ${online?'on':'off'}`}>{online?<Wifi size={16}/>:<WifiOff size={16}/>} {online?'ENGINE LIVE':'ENGINE OFFLINE'}</div></header>
  <section className="metrics"><Metric icon={<CircleDollarSign/>} label="Paper Equity" value={money(data.metrics.equity)} sub={`Realized ${money(data.metrics.realized_pnl)}`}/><Metric icon={<Activity/>} label="Unrealized P&L" value={money(data.metrics.unrealized_pnl)} sub={`${data.positions.length} open paper positions`}/><Metric icon={<BrainCircuit/>} label="Consensus Buys" value={String(data.metrics.buys)} sub={`${(data.metrics.win_rate*100).toFixed(1)}% closed win rate`}/><Metric icon={<ShieldCheck/>} label="Zero-Trust Reject" value={`${(rejectionRate*100).toFixed(1)}%`} sub={`${data.metrics.rejects}/${data.metrics.decisions} decisions`}/></section>
  <main className="grid"><section className="panel wide"><PanelTitle icon={<Radar/>} title="Live Candidate Pipeline" hint="Every token, every stage, every rejection"/><div className="table-wrap"><table><thead><tr><th>Token</th><th>Venue</th><th>Stage</th><th>Liquidity</th><th>5m Vol</th><th>Flow</th><th>Decision</th></tr></thead><tbody>{data.candidates.length?data.candidates.map(c=><tr key={c.mint}><td><b>{c.symbol||'UNKNOWN'}</b><small>{shortMint(c.mint)}</small></td><td>{c.venue}</td><td><span className="chip">{c.stage}</span></td><td>{money(c.liquidity_usd||0)}</td><td>{money(c.volume_5m||0)}</td><td>{c.buyers_5m||0}/{c.sellers_5m||0}</td><td><span className={`decision ${(c.decision||'').toLowerCase()}`}>{c.decision||'ANALYZE'} {c.confidence!=null?`${Math.round(c.confidence*100)}%`:''}</span>{c.reasons?.[0]&&<small>{c.reasons[0]}</small>}</td></tr>):<tr><td colSpan={7} className="empty">No candidates yet. Start the STRIX engine to stream live paper-research events.</td></tr>}</tbody></table></div></section>
  <section className="panel"><PanelTitle icon={<CircleDollarSign/>} title="Paper Positions" hint="No wallet signing, no live sends"/>{data.positions.length?data.positions.map(p=><div className="position" key={p.id}><div><b>{p.symbol||shortMint(p.mint)}</b><small>{p.quantity.toFixed(4)} units</small></div><div className={p.unrealized_pnl>=0?'positive':'negative'}>{money(p.unrealized_pnl)}<small>{money(p.entry_price)} → {money(p.mark_price)}</small></div></div>):<div className="empty card-empty">No open paper positions.</div>}</section>
  <section className="panel"><PanelTitle icon={<Database/>} title="Audit Stream" hint="Pipeline + agent + ledger telemetry"/><div className="events">{data.events.length?data.events.slice(0,14).map((e,i)=><div className="event" key={`${e.ts}-${i}`}><span>{new Date(e.ts).toLocaleTimeString()}</span><b>{e.kind}</b><p>{e.message}</p></div>):<div className="empty card-empty">Waiting for engine telemetry.</div>}</div></section></main><footer>STRIX v0.2 • automated market research + paper trading only • fail-closed security gates</footer></div>
}
function Metric({icon,label,value,sub}:{icon:React.ReactNode;label:string;value:string;sub:string}){return <div className="metric"><div className="metric-icon">{icon}</div><div><span>{label}</span><strong>{value}</strong><small>{sub}</small></div></div>}
function PanelTitle({icon,title,hint}:{icon:React.ReactNode;title:string;hint:string}){return <div className="panel-title"><div>{icon}<div><h2>{title}</h2><p>{hint}</p></div></div></div>}
createRoot(document.getElementById('root')!).render(<React.StrictMode><App/></React.StrictMode>);
