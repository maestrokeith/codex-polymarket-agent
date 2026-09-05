from __future__ import annotations
import asyncio, json
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from .core import EventBus, Pipeline, Settings, Store

cfg=Settings(); store=Store(cfg.database_url); bus=EventBus(); pipe=Pipeline(cfg,store,bus); tasks:list[asyncio.Task]=[]
@asynccontextmanager
async def lifespan(app:FastAPI):
    await store.init(); tasks.extend([asyncio.create_task(pipe.discovery_loop()),asyncio.create_task(pipe.pump_loop()),asyncio.create_task(pipe.mark_loop())])
    if cfg.demo_mode: tasks.append(asyncio.create_task(pipe.demo_loop()))
    yield
    for t in tasks:t.cancel()
    await asyncio.gather(*tasks,return_exceptions=True)

app=FastAPI(title='STRIX Solana Research Engine',version='0.2.0',lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=['*'],allow_credentials=False,allow_methods=['GET'],allow_headers=['*'])
@app.get('/health')
async def health():return {'ok':True,'mode':'paper','live_execution':False}
@app.get('/v1/snapshot')
async def snapshot():return await pipe.snapshot()
@app.websocket('/ws')
async def ws(websocket:WebSocket):
    await websocket.accept(); await websocket.send_json({'type':'snapshot','data':await pipe.snapshot()})
    try:
        async for msg in bus.subscribe(): await websocket.send_text(json.dumps(msg,default=str))
    except WebSocketDisconnect:return
