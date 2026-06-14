#!/usr/bin/env python3
"""ARK Dashboard Server - real-time monitoring with REST polling"""
import asyncio
import json
import os
import aiohttp
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

SEEN_LOGS = set()
DATA = {"daily_summary": {}, "current_block": 0, "last_updated": ""}

# ====== RPC ======
async def rpc(session, method, params=None):
    if params is None: params = []
    try:
        async with session.post(RPC_URL, json={"jsonrpc":"2.0","method":method,"params":params,"id":1}, timeout=30) as r:
            return await r.json()
    except: return {}

async def rpc_batch(session, items):
    try:
        async with session.post(RPC_URL, json=items, timeout=45) as r:
            return await r.json()
    except: return []

async def get_block(session):
    d = await rpc(session, "eth_blockNumber")
    return int(d.get("result","0x0"), 16)

async def get_balance(session, addr):
    d = await rpc(session, "eth_call", [{"to": TOKEN, "data": "0x70a08231" + addr[2:].lower().zfill(64)}, "latest"])
    if d and d.get("result"): return int(d["result"], 16) / 1e18
    return 0

async def get_ts(session, blocks):
    t = {}
    for i in range(0, len(blocks), 200):
        batch = [{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[hex(bn),False],"id":bn} for bn in blocks[i:i+200]]
        for r in await rpc_batch(session, batch):
            if r.get("result"): t[r["id"]] = int(r["result"]["timestamp"], 16)
    return t

async def get_logs(session, addr, f, t):
    padded = "0x" + addr[2:].lower().zfill(64)
    logs = {}
    for start in range(f, t + 1, 25000):
        end = min(start + 24999, t)
        for tf in [[TRANSFER_TOPIC, padded, None], [TRANSFER_TOPIC, None, padded]]:
            d = await rpc(session, "eth_getLogs", [{"fromBlock":hex(start),"toBlock":hex(end),"address":TOKEN,"topics":tf}])
            if isinstance(d.get("result"), list):
                for l in d["result"]:
                    k = l["transactionHash"]+l["logIndex"]
                    if k not in SEEN_LOGS: SEEN_LOGS.add(k); logs[k]=l
    return list(logs.values())

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
        if d not in dly: dly[d] = {"in": 0, "out": 0}
        v = int(l["data"], 16) / 1e18
        if "0x"+l["topics"][1][26:] == al: dly[d]["out"] += v
        if "0x"+l["topics"][2][26:] == al: dly[d]["in"] += v
    return dly

def build_summary(bd, sd, bb, sb, bn):
    ds = sorted(set(list(bd.keys())+list(sd.keys())))
    bb_, sb_ = {}, {}
    rb, rs = bb, sb
    for d in reversed(ds):
        bb_[d], sb_[d] = round(rb,4), round(rs,4)
        rb -= bd.get(d,{}).get("in",0)-bd.get(d,{}).get("out",0)
        rs -= sd.get(d,{}).get("in",0)-sd.get(d,{}).get("out",0)
    now = datetime.now(BJT).isoformat()
    s = {}
    for d in ds:
        b, st = bd.get(d,{}), sd.get(d,{})
        bo=round(b.get("out",0),4); si=round(st.get("in",0),4); so=round(st.get("out",0),4); n=round(si-so-bo,4)
        s[d]={"date":d,"bonus_withdrawal":bo,"bonus_balance":bb_.get(d,0),"stake_in":si,"stake_out":so,"stake_balance":sb_.get(d,0),"net_stake":n}
    td = s.get(datetime.now(BJT).strftime("%Y-%m-%d"),{})
    full = {"last_updated":now,"current_block":bn,"daily_summary":s,"current_balances":{"bonus_pool":round(bb,4),"stake_pool":round(sb,4)}}
    return full, td

def save(full, td):
    with open(os.path.join(DATA_DIR,"ark_data.json"),"w") as f: json.dump(full,f,indent=2)
    with open(os.path.join(DATA_DIR,"today_data.json"),"w") as f: json.dump({"bonus_withdrawal":td.get("bonus_withdrawal",0),"stake_in":td.get("stake_in",0),"stake_out":td.get("stake_out",0),"net_stake":td.get("net_stake",0),"bonus_balance":td.get("bonus_balance",0),"stake_balance":td.get("stake_balance",0),"last_updated":full["last_updated"]},f,indent=2)

# ====== Monitor Loop ======
async def monitor():
    global DATA
    async with aiohttp.ClientSession() as sess:
        current = await get_block(sess)
        target = int((datetime.now(BJT)-timedelta(days=7)).timestamp())
        lo, hi = max(1,current-1200000), current
        while lo < hi:
            mid=(lo+hi)//2
            d=await rpc(sess,"eth_getBlockByNumber",[hex(mid),False])
            t=int(d.get("result",{}).get("timestamp",0),16)
            if t<target: lo=mid+1
            else: hi=mid
        print(f"History: blocks {lo}->{current}")
        
        bl = await get_logs(sess, ADDR_BONUS, lo, current)
        sl = await get_logs(sess, ADDR_STAKE, lo, current)
        bd = await parse_logs(sess, bl, ADDR_BONUS)
        sd = await parse_logs(sess, sl, ADDR_STAKE)
        bb = await get_balance(sess, ADDR_BONUS)
        sb = await get_balance(sess, ADDR_STAKE)
        full, td = build_summary(bd, sd, bb, sb, current)
        save(full, td); DATA = full
        print(f"History done. {len(bd)+len(sd)} days, block {current}")
        
        last = current
        while True:
            try:
                current = await get_block(sess)
                if current > last:
                    bl = await get_logs(sess, ADDR_BONUS, last+1, current)
                    sl = await get_logs(sess, ADDR_STAKE, last+1, current)
                    if bl or sl:
                        bn = await parse_logs(sess, bl, ADDR_BONUS)
                        sn = await parse_logs(sess, sl, ADDR_STAKE)
                        for d,v in bn.items():
                            if d not in bd: bd[d]={"in":0,"out":0}
                            bd[d]["in"]+=v["in"]; bd[d]["out"]+=v["out"]
                        for d,v in sn.items():
                            if d not in sd: sd[d]={"in":0,"out":0}
                            sd[d]["in"]+=v["in"]; sd[d]["out"]+=v["out"]
                        bb=await get_balance(sess,ADDR_BONUS)
                        sb=await get_balance(sess,ADDR_STAKE)
                        full,td=build_summary(bd,sd,bb,sb,current)
                        save(full,td); DATA=full
                        print(f"[{datetime.now(BJT).strftime('%H:%M:%S')}] Block {current} updated")
                    last=current
            except Exception as e: print(f"Poll err: {e}")
            await asyncio.sleep(15)

# ====== HTTP Server ======
async def handle(reader, writer):
    try:
        req = (await reader.read(65536)).decode()
        if not req: writer.close(); return
        path = req.split(" ")[1] if " " in req else "/"
        ct = "text/html"; body = b""
        if path == "/":
            with open(os.path.join(STATIC_DIR,"dashboard.html")) as f: body=f.read().encode()
        elif path == "/api/data":
            ct="application/json";
            if DATA["daily_summary"]: body=json.dumps(DATA).encode()
            else:
                try: body=open(os.path.join(DATA_DIR,"ark_data.json"),"rb").read()
                except: body=b'{"daily_summary":{}}'
        elif path == "/api/today":
            ct="application/json";
            try: body=open(os.path.join(DATA_DIR,"today_data.json"),"rb").read()
            except: body=b'{"error":"No data"}'
        elif path.startswith("/static/"):
            fname=path.split("/")[-1]; fp=os.path.join(STATIC_DIR,fname)
            if os.path.exists(fp):
                with open(fp,"rb") as f: body=f.read()
            else: writer.close(); return
        else: writer.close(); return
        h=f"HTTP/1.1 200 OK\r\nContent-Type: {ct}\r\nContent-Length: {len(body)}\r\nAccess-Control-Allow-Origin: *\r\n\r\n"
        writer.write(h.encode()+body); await writer.drain()
    except: pass
    finally: writer.close()

async def main():
    asyncio.create_task(monitor())
    srv = await asyncio.start_server(handle, "0.0.0.0", int(os.environ.get("PORT",8899)))
    print(f"[{datetime.now(BJT).isoformat()}] Server on port {int(os.environ.get('PORT',8899))}")
    async with srv: await srv.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
