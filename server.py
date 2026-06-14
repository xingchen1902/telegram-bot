#!/usr/bin/env python3
"""ARK Dashboard - Unified HTTP + WebSocket on single port"""
import asyncio
import json
import os
import aiohttp
import websockets
from datetime import datetime, timedelta, timezone
from web3 import Web3

BJT = timezone(timedelta(hours=8))
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
os.makedirs(DATA_DIR, exist_ok=True)

RPC_URL = os.environ.get("RPC_URL", "https://bsc-mainnet.nodereal.io/v1/d96a4e697b0541628f61ae6089a97874")
TOKEN = "0xCae117ca6Bc8A341D2E7207F30E180f0e5618B9D"
ADDR_BONUS = "0x8501168656FcaC4628F6910CcABEA8B64Ebe5BD4"
ADDR_STAKE = "0xd1D95292F450b665566df4c4255615eF4Ed9BD0B"
TRANSFER_TOPIC = "0x" + Web3.keccak(text="Transfer(address,address,uint256)").hex()

WS_CLIENTS = set()
SEEN_LOGS = set(tuple)
DATA = {"daily_summary": {}, "current_block": 0, "last_updated": ""}
BONUS_DAILY = {}
STAKE_DAILY = {}

# ====== RPC ======
async def rpc(session, method, params=None):
    if params is None: params = []
    for attempt in range(3):
        try:
            async with session.post(RPC_URL, json={"jsonrpc":"2.0","method":method,"params":params,"id":1}, timeout=30) as r:
                d = await r.json()
                if "result" in d: return d
                if "error" in d: print(f"  RPC err: {d['error']}")
        except: pass
        await asyncio.sleep(1)
    return {}

async def rpc_batch(session, items):
    try:
        async with session.post(RPC_URL, json=items, timeout=60) as r:
            return await r.json()
    except: return []

async def get_block(session):
    d = await rpc(session, "eth_blockNumber")
    return int(d["result"], 16) if d.get("result") else 0

async def get_balance(session, addr):
    d = await rpc(session, "eth_call", [{"to": TOKEN, "data": "0x70a08231"+addr[2:].lower().zfill(64)}, "latest"])
    return int(d["result"], 16)/1e18 if d.get("result") else 0

async def get_ts(session, blocks):
    t = {}
    for i in range(0, len(blocks), 100):
        batch = [{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[hex(bn),False],"id":bn} for bn in blocks[i:i+100]]
        for r in await rpc_batch(session, batch):
            if r.get("result"): t[r["id"]] = int(r["result"]["timestamp"], 16)
    return t

async def fetch_logs(session, addr, f, t, retries=3):
    padded = "0x" + addr[2:].lower().zfill(64)
    all_logs = {}
    chunks = range(f, t+1, 2000)  # Smaller chunks (2000 blocks) to avoid RPC issues
    total = len(list(chunks))
    for idx, start in enumerate(chunks):
        end = min(start+1999, t)
        for tf in [[TRANSFER_TOPIC, padded, None], [TRANSFER_TOPIC, None, padded]]:
            for attempt in range(retries):
                d = await rpc(session, "eth_getLogs", [{"fromBlock":hex(start),"toBlock":hex(end),"address":TOKEN,"topics":tf}])
                if isinstance(d.get("result"), list):
                    for l in d["result"]:
                        k = (l["transactionHash"], l["logIndex"])
                        if k not in SEEN_LOGS:
                            SEEN_LOGS.add(k)
                            all_logs[k] = l
                    break
                await asyncio.sleep(0.5)
        if (idx+1) % 20 == 0:
            print(f"  Logs {addr[:10]}...: {idx+1}/{total} chunks ({len(all_logs)} logs found)")
    return list(all_logs.values())

async def parse_logs(session, logs, addr):
    al = addr.lower()
    bs = set(int(l["blockNumber"],16) for l in logs)
    tm = await get_ts(session, list(bs))
    dly = {}
    for l in logs:
        bn = int(l["blockNumber"],16)
        ts = tm.get(bn)
        if not ts: continue
        d = datetime.fromtimestamp(ts, tz=BJT).strftime("%Y-%m-%d")
        if d not in dly: dly[d] = {"in":0,"out":0}
        v = int(l["data"],16)/1e18
        if "0x"+l["topics"][1][26:]==al: dly[d]["out"]+=v
        if "0x"+l["topics"][2][26:]==al: dly[d]["in"]+=v
    return dly

def build_summary(bd, sd, bb, sb, bn):
    ds = sorted(set(list(bd.keys())+list(sd.keys())))
    bb_, sb_ = {}, {}
    rb, rs = bb, sb
    for d in reversed(ds):
        bb_[d], sb_[d] = round(rb,4), round(rs,4)
        rb -= bd.get(d,{}).get("in",0)-bd.get(d,{}).get("out",0)
        rs -= sd.get(d,{}).get("in",0)-sd.get(d,{}).get("out",0)
    s = {d: {"date":d,"bonus_withdrawal":round(bd.get(d,{}).get("out",0),4),"bonus_balance":bb_.get(d,0),"stake_in":round(sd.get(d,{}).get("in",0),4),"stake_out":round(sd.get(d,{}).get("out",0),4),"stake_balance":sb_.get(d,0),"net_stake":round(sd.get(d,{}).get("in",0)-sd.get(d,{}).get("out",0)-bd.get(d,{}).get("out",0),4)} for d in ds}
    td = s.get(datetime.now(BJT).strftime("%Y-%m-%d"),{})
    now = datetime.now(BJT).isoformat()
    return {"last_updated":now,"current_block":bn,"daily_summary":s,"current_balances":{"bonus_pool":round(bb,4),"stake_pool":round(sb,4)}}, td

def save_files(full, today):
    with open(os.path.join(DATA_DIR,"ark_data.json"),"w") as f: json.dump(full,f,indent=2)
    with open(os.path.join(DATA_DIR,"today_data.json"),"w") as f: json.dump(today,f,indent=2)

async def broadcast(msg):
    if WS_CLIENTS:
        d = json.dumps(msg)
        await asyncio.gather(*(c.send(d) for c in WS_CLIENTS.copy() if c.open), return_exceptions=True)

async def monitor_loop():
    global DATA, BONUS_DAILY, STAKE_DAILY
    async with aiohttp.ClientSession() as sess:
        current = await get_block(sess)
        if not current:
            print("ERROR: Cannot get current block")
            return
        target = int((datetime.now(BJT)-timedelta(days=7)).timestamp())
        lo, hi = max(1,current-1200000), current
        while lo < hi:
            mid = (lo+hi)//2
            d = await rpc(sess, "eth_getBlockByNumber", [hex(mid), False])
            ts = int(d.get("result",{}).get("timestamp",0),16) if d.get("result") else 0
            if ts < target: lo = mid+1
            else: hi = mid
        print(f"History: {lo} -> {current} ({current-lo} blocks)")
        bl = await fetch_logs(sess, ADDR_BONUS, lo, current)
        sl = await fetch_logs(sess, ADDR_STAKE, lo, current)
        print(f"Bonus logs: {len(bl)}, Stake logs: {len(sl)}")
        bd = await parse_logs(sess, bl, ADDR_BONUS)
        sd = await parse_logs(sess, sl, ADDR_STAKE)
        print("Bonus daily:", json.dumps({d:{k:round(v,4) for k,v in bd[d].items()} for d in sorted(bd.keys())}))
        print("Stake daily:", json.dumps({d:{k:round(v,4) for k,v in sd[d].items()} for d in sorted(sd.keys())}))
        bb = await get_balance(sess, ADDR_BONUS)
        sb = await get_balance(sess, ADDR_STAKE)
        print(f"Balances: bonus={bb:.4f}, stake={sb:.4f}")
        BONUS_DAILY, STAKE_DAILY = bd, sd
        full, td = build_summary(bd, sd, bb, sb, current)
        save_files(full, {**td, "last_updated": full["last_updated"]})
        DATA = full
        await broadcast({"type":"full_update","data":full})
        print(f"Done. Block {current}")
        last = current
        while True:
            try:
                current = await get_block(sess)
                if current > last:
                    for addr, daily_store in [(ADDR_BONUS, BONUS_DAILY), (ADDR_STAKE, STAKE_DAILY)]:
                        logs = await fetch_logs(sess, addr, last+1, current)
                        if logs:
                            parsed = await parse_logs(sess, logs, addr)
                            for d,v in parsed.items():
                                if d not in daily_store: daily_store[d] = {"in":0,"out":0}
                                daily_store[d]["in"]+=v["in"]
                                daily_store[d]["out"]+=v["out"]
                    bb = await get_balance(sess, ADDR_BONUS)
                    sb = await get_balance(sess, ADDR_STAKE)
                    full, td = build_summary(BONUS_DAILY, STAKE_DAILY, bb, sb, current)
                    save_files(full, {**td, "last_updated": full["last_updated"]})
                    DATA = full
                    await broadcast({"type":"full_update","data":full})
                    print(f"  [{datetime.now(BJT).strftime('%H:%M:%S')}] Block {current}")
                    last = current
            except Exception as e: print(f"  Poll: {e}")
            await asyncio.sleep(15)

async def ws_handler(ws):
    WS_CLIENTS.add(ws)
    print(f"  WS client ({len(WS_CLIENTS)})")
    try:
        if DATA["daily_summary"]:
            await ws.send(json.dumps({"type":"full_update","data":DATA}))
        async for _ in ws: pass
    except: pass
    finally: WS_CLIENTS.discard(ws)

async def process_request(path, request_headers):
    if path == "/":
        with open(os.path.join(STATIC_DIR,"dashboard.html"),"rb") as f:
            return (200, [("Content-Type","text/html"),("Access-Control-Allow-Origin","*")], f.read())
    elif path == "/api/data":
        if DATA["daily_summary"]:
            body = json.dumps(DATA).encode()
        else:
            try: body = open(os.path.join(DATA_DIR,"ark_data.json"),"rb").read()
            except: body = b'{"daily_summary":{}}'
        return (200, [("Content-Type","application/json"),("Access-Control-Allow-Origin","*")], body)
    elif path == "/api/today":
        try: body = open(os.path.join(DATA_DIR,"today_data.json"),"rb").read()
        except: body = b'{"error":"No data"}'
        return (200, [("Content-Type","application/json"),("Access-Control-Allow-Origin","*")], body)
    elif path.startswith("/static/"):
        fn = path.split("/")[-1]
        fp = os.path.join(STATIC_DIR, fn)
        if os.path.exists(fp):
            with open(fp,"rb") as f:
                ct = "text/html" if fn.endswith(".html") else "text/css"
                return (200, [("Content-Type",ct),("Access-Control-Allow-Origin","*")], f.read())
    return None

async def main():
    port = int(os.environ.get("PORT", 8899))
    asyncio.create_task(monitor_loop())
    async with websockets.serve(ws_handler, "0.0.0.0", port, process_request=process_request):
        print(f"[{datetime.now(BJT).isoformat()}] Server on port {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
