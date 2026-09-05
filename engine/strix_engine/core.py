from __future__ import annotations

import asyncio, json, random, uuid
from collections import deque
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, AsyncIterator

import httpx
import websockets
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import JSON, DateTime, Float, Integer, String, Text, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

def utcnow()->datetime:return datetime.now(timezone.utc)
class Stage(StrEnum): INGEST='INGEST'; SECURITY='SECURITY'; WALLET='WALLET'; MARKET='MARKET'; CONSENSUS='CONSENSUS'; RISK='RISK'; PAPER='PAPER'; CLOSED='CLOSED'
class Decision(StrEnum): BUY='BUY'; REJECT='REJECT'
class Settings(BaseSettings):
    model_config=SettingsConfigDict(env_prefix='STRIX_',env_file='.env',extra='ignore')
    database_url:str='sqlite+aiosqlite:///./strix.db'; solana_rpc_url:str='https://api.mainnet-beta.solana.com'; solana_ws_url:str='wss://api.mainnet-beta.solana.com'
    pumpfun_program_id:str='6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'; pumpswap_program_id:str='pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'; dexscreener_poll_seconds:float=8
    starting_equity:float=1000; max_open_positions:int=3; risk_per_trade:float=.01; max_position_fraction:float=.10; min_confidence:float=.78
    stop_loss_pct:float=.10; take_profit_pct:float=.28; trailing_stop_pct:float=.10; trailing_activation_pct:float=.60; top10_max_pct:float=.20; lp_lock_min_pct:float=.97; demo_mode:bool=False; llm_enabled:bool=False; llm_model:str='gpt-5-mini'
class Candidate(BaseModel):
    mint:str; symbol:str|None=None; name:str|None=None; venue:str='unknown'; pair_address:str|None=None; deployer:str|None=None; stage:Stage=Stage.INGEST; price_usd:float|None=None; liquidity_usd:float|None=None; volume_1m:float=0; volume_5m:float=0; volume_15m:float=0; buyers_1m:int=0; sellers_1m:int=0; buyers_5m:int=0; sellers_5m:int=0; buyers_15m:int=0; sellers_15m:int=0; mint_authority_active:bool|None=None; freeze_authority_active:bool|None=None; lp_locked_pct:float|None=None; top10_holder_pct:float|None=None; deployer_rug_count:int=0; cluster_risk:float=0; wash_trade_score:float=0; decision:Decision|None=None; confidence:float|None=None; reasons:list[str]=Field(default_factory=list); updated_at:datetime=Field(default_factory=utcnow)
class AgentResult(BaseModel): agent:str; score:float=Field(ge=0,le=1); veto:bool=False; reasons:list[str]=Field(default_factory=list); evidence:dict[str,Any]=Field(default_factory=dict)
class Event(BaseModel): ts:datetime=Field(default_factory=utcnow); kind:str; message:str; payload:dict[str,Any]=Field(default_factory=dict)
class Position(BaseModel): id:str=Field(default_factory=lambda:str(uuid.uuid4())); mint:str; symbol:str|None=None; quantity:float; entry_price:float; mark_price:float; stop_price:float; take_profit_price:float; peak_price:float; opened_at:datetime=Field(default_factory=utcnow); unrealized_pnl:float=0
class Base(DeclarativeBase):pass
class LedgerRow(Base):
    __tablename__='ledger'; id:Mapped[int]=mapped_column(Integer,primary_key=True,autoincrement=True); ts:Mapped[datetime]=mapped_column(DateTime(timezone=True),default=utcnow,index=True); kind:Mapped[str]=mapped_column(String(32),index=True); mint:Mapped[str|None]=mapped_column(String(64),nullable=True,index=True); stage:Mapped[str|None]=mapped_column(String(32),nullable=True); decision:Mapped[str|None]=mapped_column(String(16),nullable=True); confidence:Mapped[float|None]=mapped_column(Float,nullable=True); message:Mapped[str]=mapped_column(Text); payload:Mapped[dict[str,Any]]=mapped_column(JSON,default=dict)
class WalletRep(Base):
    __tablename__='wallet_reputation'; wallet:Mapped[str]=mapped_column(String(64),primary_key=True); rug_count:Mapped[int]=mapped_column(Integer,default=0); wins:Mapped[int]=mapped_column(Integer,default=0); losses:Mapped[int]=mapped_column(Integer,default=0); metadata_json:Mapped[dict[str,Any]]=mapped_column(JSON,default=dict)
class TradeRow(Base):
    __tablename__='paper_trades'; id:Mapped[str]=mapped_column(String(64),primary_key=True); mint:Mapped[str]=mapped_column(String(64),index=True); symbol:Mapped[str|None]=mapped_column(String(32),nullable=True); qty:Mapped[float]=mapped_column(Float); entry:Mapped[float]=mapped_column(Float); exit:Mapped[float|None]=mapped_column(Float,nullable=True); pnl:Mapped[float|None]=mapped_column(Float,nullable=True); status:Mapped[str]=mapped_column(String(16),index=True); opened_at:Mapped[datetime]=mapped_column(DateTime(timezone=True)); closed_at:Mapped[datetime|None]=mapped_column(DateTime(timezone=True),nullable=True); meta:Mapped[dict[str,Any]]=mapped_column(JSON,default=dict)
class Store:
    def __init__(self,url:str):self.engine=create_async_engine(url,pool_pre_ping=True);self.sessions=async_sessionmaker(self.engine,expire_on_commit=False)
    async def init(self):
        async with self.engine.begin() as c:await c.run_sync(Base.metadata.create_all)
    async def log(self,e:Event,mint:str|None=None,stage:Stage|None=None,decision:Decision|None=None,confidence:float|None=None):
        async with self.sessions() as s:s.add(LedgerRow(ts=e.ts,kind=e.kind,mint=mint,stage=stage.value if stage else None,decision=decision.value if decision else None,confidence=confidence,message=e.message,payload=e.payload));await s.commit()
    async def wallet_rugs(self,wallet:str|None)->int:
        if not wallet:return 0
        async with self.sessions() as s:
            row=await s.get(WalletRep,wallet);return row.rug_count if row else 0
    async def save_trade(self,p:Position):
        async with self.sessions() as s:s.add(TradeRow(id=p.id,mint=p.mint,symbol=p.symbol,qty=p.quantity,entry=p.entry_price,status='OPEN',opened_at=p.opened_at,meta={'stop':p.stop_price,'take_profit':p.take_profit_price}));await s.commit()
    async def close_trade(self,p:Position,exit_price:float,pnl:float):
        async with self.sessions() as s:
            row=await s.get(TradeRow,p.id)
            if row:row.exit=exit_price;row.pnl=pnl;row.status='CLOSED';row.closed_at=utcnow();await s.commit()
    async def closed_stats(self)->tuple[int,int,float]:
        async with self.sessions() as s:
            rows=(await s.execute(select(TradeRow).where(TradeRow.status=='CLOSED'))).scalars().all();wins=sum(1 for r in rows if (r.pnl or 0)>0);return len(rows),wins,sum((r.pnl or 0) for r in rows)
class EventBus:
    def __init__(self):self.subs:set[asyncio.Queue[dict[str,Any]]]=set();self.events:deque[Event]=deque(maxlen=100)
    async def publish(self,e:Event):
        self.events.appendleft(e);msg={'type':'event','data':e.model_dump(mode='json')}
        for q in list(self.subs):
            try:q.put_nowait(msg)
            except asyncio.QueueFull:pass
    async def subscribe(self)->AsyncIterator[dict[str,Any]]:
        q:asyncio.Queue[dict[str,Any]]=asyncio.Queue(maxsize=256);self.subs.add(q)
        try:
            while True:yield await q.get()
        finally:self.subs.discard(q)
class SolanaRPC:
    def __init__(self,url:str):self.url=url;self.http=httpx.AsyncClient(timeout=12)
    async def call(self,method:str,params:list[Any])->Any:
        r=await self.http.post(self.url,json={'jsonrpc':'2.0','id':1,'method':method,'params':params});r.raise_for_status();data=r.json()
        if data.get('error'):raise RuntimeError(f"Solana RPC {method}: {data['error']}")
        return data.get('result')
    async def mint_security(self,mint:str)->tuple[bool|None,bool|None,float|None]:
        try:
            info=await self.call('getAccountInfo',[mint,{'encoding':'jsonParsed','commitment':'confirmed'}]);parsed=((info or {}).get('value') or {}).get('data',{}).get('parsed',{}).get('info',{});mint_auth=parsed.get('mintAuthority') is not None;freeze_auth=parsed.get('freezeAuthority') is not None;supply=float(parsed.get('supply') or 0)/(10**int(parsed.get('decimals') or 0));largest=await self.call('getTokenLargestAccounts',[mint,{'commitment':'confirmed'}]);vals=((largest or {}).get('value') or [])[:10];top=sum(float(v.get('uiAmount') or 0) for v in vals);pct=(top/supply) if supply else None;return mint_auth,freeze_auth,pct
        except Exception:return None,None,None
class PumpLogIngestor:
    def __init__(self,ws_url:str,program_id:str):self.ws_url=ws_url;self.program_id=program_id
    async def signatures(self)->AsyncIterator[str]:
        backoff=1.0
        while True:
            try:
                async with websockets.connect(self.ws_url,ping_interval=20,ping_timeout=20,max_size=2_000_000) as ws:
                    await ws.send(json.dumps({'jsonrpc':'2.0','id':1,'method':'logsSubscribe','params':[{'mentions':[self.program_id]},{'commitment':'confirmed'}]}));await ws.recv();backoff=1.0
                    async for raw in ws:
                        msg=json.loads(raw);value=(((msg.get('params') or {}).get('result') or {}).get('value') or {});sig=value.get('signature')
                        if sig:yield sig
            except asyncio.CancelledError:raise
            except Exception:await asyncio.sleep(backoff);backoff=min(30.0,backoff*2)
class DexScreener:
    def __init__(self):self.http=httpx.AsyncClient(timeout=12);self.seen:set[str]=set()
    async def discover(self)->list[Candidate]:
        out=[]
        try:
            r=await self.http.get('https://api.dexscreener.com/token-profiles/latest/v1');r.raise_for_status();profiles=r.json()
            for p in profiles[:80]:
                if p.get('chainId')!='solana':continue
                mint=p.get('tokenAddress')
                if not mint or mint in self.seen:continue
                self.seen.add(mint);c=await self.enrich(mint);out.extend(c[:1])
        except Exception:return []
        return out
    async def enrich(self,mint:str)->list[Candidate]:
        try:
            r=await self.http.get(f'https://api.dexscreener.com/token-pairs/v1/solana/{mint}');r.raise_for_status();pairs=sorted(r.json() or [],key=lambda x:float((x.get('liquidity') or {}).get('usd') or 0),reverse=True);out=[]
            for x in pairs[:2]:
                tx5=(x.get('txns') or {}).get('m5') or {};tx1=(x.get('txns') or {}).get('m1') or {};vol=x.get('volume') or {};base=x.get('baseToken') or {};out.append(Candidate(mint=mint,symbol=base.get('symbol'),name=base.get('name'),venue=x.get('dexId') or 'dex',pair_address=x.get('pairAddress'),price_usd=float(x.get('priceUsd') or 0),liquidity_usd=float((x.get('liquidity') or {}).get('usd') or 0),volume_1m=float(vol.get('m1') or 0),volume_5m=float(vol.get('m5') or 0),volume_15m=float(vol.get('h1') or 0)/4,buyers_1m=int(tx1.get('buys') or 0),sellers_1m=int(tx1.get('sells') or 0),buyers_5m=int(tx5.get('buys') or 0),sellers_5m=int(tx5.get('sells') or 0)))
            return out
        except Exception:return []
class SecurityEngine:
    def __init__(self,cfg:Settings,rpc:SolanaRPC,store:Store):self.cfg=cfg;self.rpc=rpc;self.store=store
    async def run(self,c:Candidate)->AgentResult:
        c.stage=Stage.SECURITY;c.mint_authority_active,c.freeze_authority_active,c.top10_holder_pct=await self.rpc.mint_security(c.mint);c.deployer_rug_count=await self.store.wallet_rugs(c.deployer);reasons=[];veto=False
        if c.mint_authority_active is True:veto=True;reasons.append('mint authority remains active')
        if c.freeze_authority_active is True:veto=True;reasons.append('freeze authority remains active')
        if c.top10_holder_pct is None:veto=True;reasons.append('holder concentration unavailable: fail closed')
        elif c.top10_holder_pct>self.cfg.top10_max_pct:veto=True;reasons.append(f'top-10 concentration {c.top10_holder_pct:.1%} exceeds {self.cfg.top10_max_pct:.0%}')
        if c.deployer_rug_count>0:veto=True;reasons.append(f'deployer linked to {c.deployer_rug_count} prior rug outcome(s)')
        if c.venue not in {'pumpfun','pump.fun','pump'}:
            if c.lp_locked_pct is None:reasons.append('LP lock proof unavailable; soft penalty until provider is configured')
            elif c.lp_locked_pct<self.cfg.lp_lock_min_pct:veto=True;reasons.append('LP lock/burn threshold failed')
        score=max(0,1-(.65 if veto else 0)-(.12 if c.lp_locked_pct is None else 0));return AgentResult(agent='A_SECURITY',score=score,veto=veto,reasons=reasons or ['authority and concentration gates passed'])
class WalletAgent:
    async def run(self,c:Candidate)->AgentResult:
        c.stage=Stage.WALLET;risk=min(1,max(c.cluster_risk,c.wash_trade_score));veto=risk>=.85;return AgentResult(agent='C_WALLET',score=1-risk,veto=veto,reasons=['coordinated/wash activity extreme'] if veto else [f'cluster risk {risk:.2f}'])
class MomentumAgent:
    async def run(self,c:Candidate)->AgentResult:
        c.stage=Stage.MARKET;liq=c.liquidity_usd or 0;total=c.buyers_5m+c.sellers_5m;buy_ratio=c.buyers_5m/total if total else .5;vol_liq=(c.volume_5m or 0)/liq if liq else 0;score=max(0,min(1,.25+buy_ratio*.45+min(vol_liq,2)*.15));return AgentResult(agent='B_MOMENTUM',score=score,reasons=[f'5m buy ratio {buy_ratio:.1%}',f'5m volume/liquidity {vol_liq:.2f}x'],evidence={'buy_ratio':buy_ratio,'vol_liq':vol_liq})
class Consensus:
    def __init__(self,cfg:Settings):self.cfg=cfg
    def decide(self,c:Candidate,results:list[AgentResult])->AgentResult:
        c.stage=Stage.CONSENSUS;weighted=.45*results[0].score+.35*results[1].score+.20*results[2].score;veto=any(r.veto for r in results) or weighted<self.cfg.min_confidence;reasons=[x for r in results for x in r.reasons];reasons.append(f'weighted consensus {weighted:.3f}');return AgentResult(agent='D_RISK_EXECUTION',score=weighted,veto=veto,reasons=reasons)
class PaperExecutor:
    def __init__(self,cfg:Settings,store:Store,bus:EventBus):self.cfg=cfg;self.store=store;self.bus=bus;self.cash=cfg.starting_equity;self.positions:dict[str,Position]={};self.realized=0.0
    async def maybe_enter(self,c:Candidate)->Position|None:
        if c.decision!=Decision.BUY or not c.price_usd or c.price_usd<=0 or len(self.positions)>=self.cfg.max_open_positions:return None
        risk_budget=max(0,self.equity())*self.cfg.risk_per_trade;stop_distance=c.price_usd*self.cfg.stop_loss_pct;qty=min(risk_budget/stop_distance,(self.equity()*self.cfg.max_position_fraction)/c.price_usd);slip=min(.025,max(.002,5000/max(c.liquidity_usd or 1,1)*.001));entry=c.price_usd*(1+slip);cost=qty*entry
        if cost>self.cash:return None
        p=Position(mint=c.mint,symbol=c.symbol,quantity=qty,entry_price=entry,mark_price=entry,stop_price=entry*(1-self.cfg.stop_loss_pct),take_profit_price=entry*(1+self.cfg.take_profit_pct),peak_price=entry);self.cash-=cost;self.positions[p.id]=p;await self.store.save_trade(p);await self.bus.publish(Event(kind='paper_entry',message=f'{c.symbol or c.mint}: paper BUY {qty:.4f} @ {entry:.8f}',payload=p.model_dump(mode='json')));return p
    async def mark(self,mint:str,price:float):
        for p in list(self.positions.values()):
            if p.mint!=mint:continue
            p.mark_price=price;p.peak_price=max(p.peak_price,price);p.unrealized_pnl=(price-p.entry_price)*p.quantity;trailing_active=p.peak_price>=p.entry_price*(1+self.cfg.trailing_activation_pct);trail=p.peak_price*(1-self.cfg.trailing_stop_pct);reason=None
            if price<=p.stop_price:reason='stop_loss'
            elif price>=p.take_profit_price:reason='take_profit'
            elif trailing_active and price<=trail:reason='trailing_stop'
            if reason:await self.close(p,price,reason)
    async def close(self,p:Position,price:float,reason:str):
        pnl=(price-p.entry_price)*p.quantity;self.cash+=price*p.quantity;self.realized+=pnl;self.positions.pop(p.id,None);await self.store.close_trade(p,price,pnl);await self.bus.publish(Event(kind='paper_exit',message=f'{p.symbol or p.mint}: {reason} exit, P&L {pnl:.2f}',payload={'trade_id':p.id,'pnl':pnl,'reason':reason}))
    def equity(self):return self.cash+sum(p.quantity*p.mark_price for p in self.positions.values())
class Pipeline:
    def __init__(self,cfg:Settings,store:Store,bus:EventBus):self.cfg=cfg;self.store=store;self.bus=bus;self.rpc=SolanaRPC(cfg.solana_rpc_url);self.dex=DexScreener();self.pump=PumpLogIngestor(cfg.solana_ws_url,cfg.pumpfun_program_id);self.pump_seen:set[str]=set();self.security=SecurityEngine(cfg,self.rpc,store);self.momentum=MomentumAgent();self.wallet=WalletAgent();self.consensus=Consensus(cfg);self.paper=PaperExecutor(cfg,store,bus);self.candidates:dict[str,Candidate]={};self.decisions=0;self.rejects=0;self.buys=0
    async def process(self,c:Candidate):
        self.candidates[c.mint]=c;await self.emit(c,'candidate',f'{c.symbol or c.mint} discovered on {c.venue}');sec=await self.security.run(c);await self.emit(c,'agent',f'{sec.agent}: {sec.score:.2f} {"VETO" if sec.veto else "PASS"}',sec.model_dump());mom=await self.momentum.run(c);await self.emit(c,'agent',f'{mom.agent}: {mom.score:.2f}',mom.model_dump());wal=await self.wallet.run(c);await self.emit(c,'agent',f'{wal.agent}: {wal.score:.2f} {"VETO" if wal.veto else "PASS"}',wal.model_dump());final=self.consensus.decide(c,[sec,mom,wal]);c.confidence=final.score;c.decision=Decision.REJECT if final.veto else Decision.BUY;c.reasons=final.reasons;c.stage=Stage.RISK;self.decisions+=1
        if c.decision==Decision.REJECT:self.rejects+=1
        else:self.buys+=1
        await self.emit(c,'decision',f'{c.symbol or c.mint}: {c.decision.value} confidence={final.score:.3f}',final.model_dump())
        if c.decision==Decision.BUY:c.stage=Stage.PAPER;await self.paper.maybe_enter(c)
        c.updated_at=utcnow()
    async def emit(self,c:Candidate,kind:str,message:str,payload:dict[str,Any]|None=None):
        e=Event(kind=kind,message=message,payload=payload or {});await self.store.log(e,c.mint,c.stage,c.decision,c.confidence);await self.bus.publish(e)
    async def snapshot(self)->dict[str,Any]:
        closed,wins,realized=await self.store.closed_stats();return {'status':'live','candidates':[x.model_dump(mode='json') for x in sorted(self.candidates.values(),key=lambda x:x.updated_at,reverse=True)[:100]],'positions':[p.model_dump(mode='json') for p in self.paper.positions.values()],'metrics':{'equity':self.paper.equity(),'realized_pnl':realized,'unrealized_pnl':sum(p.unrealized_pnl for p in self.paper.positions.values()),'win_rate':wins/closed if closed else 0,'decisions':self.decisions,'rejects':self.rejects,'buys':self.buys},'events':[e.model_dump(mode='json') for e in list(self.bus.events)[:80]]}
    async def pump_loop(self):
        ignored={'So11111111111111111111111111111111111111112','11111111111111111111111111111111'}
        async for sig in self.pump.signatures():
            try:
                tx=await self.rpc.call('getTransaction',[sig,{'encoding':'jsonParsed','maxSupportedTransactionVersion':0,'commitment':'confirmed'}]);
                if not tx:continue
                meta=tx.get('meta') or {};message=(tx.get('transaction') or {}).get('message') or {};keys=message.get('accountKeys') or [];deployer=None
                for k in keys:
                    if isinstance(k,dict) and k.get('signer'):deployer=k.get('pubkey');break
                mints=[]
                for bal in meta.get('postTokenBalances') or []:
                    mint=bal.get('mint')
                    if mint and mint not in ignored and mint not in self.pump_seen:mints.append(mint)
                for mint in dict.fromkeys(mints):self.pump_seen.add(mint);pairs=await self.dex.enrich(mint);c=(pairs[0] if pairs else Candidate(mint=mint,venue='pump.fun'));c.deployer=deployer;await self.process(c)
            except asyncio.CancelledError:raise
            except Exception as exc:await self.bus.publish(Event(kind='error',message=f'pump log enrichment: {exc}'))
    async def discovery_loop(self):
        while True:
            try:
                for c in await self.dex.discover():await self.process(c)
            except asyncio.CancelledError:raise
            except Exception as exc:await self.bus.publish(Event(kind='error',message=f'discovery loop: {exc}'))
            await asyncio.sleep(self.cfg.dexscreener_poll_seconds)
    async def mark_loop(self):
        while True:
            try:
                for p in list(self.paper.positions.values()):
                    pairs=await self.dex.enrich(p.mint)
                    if pairs and pairs[0].price_usd:await self.paper.mark(p.mint,pairs[0].price_usd)
            except asyncio.CancelledError:raise
            except Exception as exc:await self.bus.publish(Event(kind='error',message=f'mark loop: {exc}'))
            await asyncio.sleep(5)
    async def demo_loop(self):
        while self.cfg.demo_mode:
            mint=str(uuid.uuid4()).replace('-','')[:32];c=Candidate(mint=mint,symbol=random.choice(['OWL','MINT','ARC','NOVA']),venue='demo',price_usd=random.uniform(.00002,.002),liquidity_usd=random.uniform(20000,120000),volume_5m=random.uniform(8000,90000),buyers_5m=random.randint(40,250),sellers_5m=random.randint(10,120),mint_authority_active=False,freeze_authority_active=False,top10_holder_pct=random.uniform(.07,.19),lp_locked_pct=.99);self.candidates[c.mint]=c;mom=await self.momentum.run(c);wal=await self.wallet.run(c);sec=AgentResult(agent='A_SECURITY',score=.95,reasons=['demo security pass']);final=self.consensus.decide(c,[sec,mom,wal]);c.confidence=final.score;c.decision=Decision.BUY if not final.veto else Decision.REJECT;c.reasons=final.reasons;c.stage=Stage.RISK;self.decisions+=1;self.buys+=c.decision==Decision.BUY;self.rejects+=c.decision==Decision.REJECT;await self.emit(c,'decision',f'DEMO {c.symbol}: {c.decision.value} {c.confidence:.2f}');
            if c.decision==Decision.BUY:c.stage=Stage.PAPER;await self.paper.maybe_enter(c)
            await asyncio.sleep(7)
