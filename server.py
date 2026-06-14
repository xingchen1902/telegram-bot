#!/usr/bin/env python3
"""ARK Dashboard - WebSocket real-time monitoring server"""
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

# WebSocket clients
WS_CLIENTS = set()
SEEN_LOGS = set()

# In-memory data store
DATA = {
    "daily_summary": {},
    "bonus_balance": 0,
    "stake_balance": 0,
    "last_updated": "",
    "current_block": 0
}

# ====== BSC RPC Helpers ======

async def rpc(session, method, params=None):
    if params is None: params = []
    try:
        async with session.post(RPC_URL, json={"jsonrpc": "2.0", "method": method, "params": params, "id": 1}, timeout=30) as r:
            d = await r.json()
            return d
    except: return {}

async def rpc_batch(session, requests_list):
    try:
        async with session.post(RPC_URL, json=requests_list, timeout=45) as r:
            return await r.json()
    except: return []

async def get_block_number(session):
    d = await rpc(session, "eth_blockNumber")
    return int(d["result"], 16)

async def get_balance(session, address):
    data = "0x70a08231" + address[2:].lower().zfill(64)
    d = await rpc(session, "eth_call", [{"to": TOKEN, "data": data}, "latest"])
    if d and "result" in d and d["result"]:
        return int(d["result"], 16) / 1e18
    return 0

async def get_block_timestamps(session, block_nums):
    ts = {}
    for i in range(0, len(block_nums), 200):
        batch = [{"jsonrpc": "2.0", "method": "eth_getBlockByNumber",
                  "params": [hex(bn), False], "id": bn} for bn in block_nums[i:i+200]]
        for res in await rpc_batch(session, batch):
            if "result" in res and res["result"]:
                ts[res["id"]] = int(res["result"]["timestamp"], 16)
    return ts

async def get_logs(session, address, from_block, to_block):
    """Fetch all transfer logs for an address in block range"""
    padded = "0x" + address[2:].lower().zfill(64)
    all_logs = {}
    chunks = list(range(from_block, to_block + 1, 25000))
    for start in chunks:
        end = min(start + 24999, to_block)
        for t_filter in [[TRANSFER_TOPIC, padded, None], [TRANSFER_TOPIC, None, padded]]:
            d = await rpc(session, "eth_getLogs", [{"fromBlock": hex(start), "toBlock": hex(end),
                                                     "address": TOKEN, "topics": t_filter}])
            if isinstance(d.get("result"), list):
                for log in d["result"]:
                    key = log["transactionHash"] + log["logIndex"]
                    if key not in SEEN_LOGS:
                        SEEN_LOGS.add(key)
                        all_logs[key] = log
    return list(all_logs.values())

async def parse_logs(session, logs, address):
    """Parse logs into daily BJT aggregates"""
    addr_lower = address.lower()
    blocks = set(int(l["blockNumber"], 16) for l in logs)
    ts_map = await get_block_timestamps(session, list(blocks))
    daily = {}
    for log in logs:
        bn = int(log["blockNumber"], 16)
        t = ts_map.get(bn)
        if not t: continue
        date = datetime.fromtimestamp(t, tz=BJT).strftime("%Y-%m-%d")
        if date not in daily: daily[date] = {"in": 0, "out": 0}
        value = int(log["data"], 16) / 1e18
        f_addr = "0x" + log["topics"][1][26:]
        t_addr = "0x" + log["topics"][2][26:]
        if f_addr.lower() == addr_lower: daily[date]["out"] += value
        if t_addr.lower() == addr_lower: daily[date]["in"] += value
    return daily

def merge_daily(existing, new_data):
    for date, vals in new_data.items():
        if date not in existing: existing[date] = {"in": 0, "out": 0}
        existing[date]["in"] += vals["in"]
        existing[date]["out"] += vals["out"]

def compute_summary(bonus_daily, stake_daily, bonus_bal, stake_bal, current_block):
    all_dates = sorted(set(list(bonus_daily.keys()) + list(stake_daily.keys())))
    b_bal, s_bal = {}, {}
    rb, rs = bonus_bal, stake_bal
    for d in reversed(all_dates):
        b_bal[d], s_bal[d] = round(rb, 4), round(rs, 4)
        rb -= bonus_daily.get(d, {}).get("in", 0) - bonus_daily.get(d, {}).get("out", 0)
        rs -= stake_daily.get(d, {}).get("in", 0) - stake_daily.get(d, {}).get("out", 0)

    today_bjt = datetime.now(BJT).strftime("%Y-%m-%d")
    summary = {}
    for d in all_dates:
        b, s = bonus_daily.get(d, {}), stake_daily.get(d, {})
        bo = round(b.get("out", 0), 4); si = round(s.get("in", 0), 4)
        so = round(s.get("out", 0), 4); net = round(si - so - bo, 4)
        summary[d] = {"date": d, "bonus_withdrawal": bo, "bonus_balance": b_bal[d],
                      "stake_in": si, "stake_out": so, "stake_balance": s_bal[d], "net_stake": net}
    
    now = datetime.now(BJT).isoformat()
    result = {
        "last_updated": now, "current_block": current_block,
        "daily_summary": summary,
        "current_balances": {"bonus_pool": round(bonus_bal, 4), "stake_pool": round(stake_bal, 4)}
    }
    return result, summary.get(today_bjt, {})

def save_files(full_data, today_data):
    with open(os.path.join(DATA_DIR, "ark_data.json"), "w") as f:
        json.dump(full_data, f, indent=2)
    with open(os.path.join(DATA_DIR, "today_data.json"), "w") as f:
        json.dump(today_data, f, indent=2)

# ====== Real-time Monitor ======

async def broadcast(message):
    if WS_CLIENTS:
        msg = json.dumps(message)
        await asyncio.gather(*[client.send(msg) for client in WS_CLIENTS], return_exceptions=True)

async def monitor_loop():
    """Background loop: download history then poll for new blocks"""
    global DATA
    async with aiohttp.ClientSession() as session:
        # Phase 1: Full history
        print(f"[{datetime.now(BJT).isoformat()}] Downloading 7-day history...")
        current = await get_block_number(session)
        target_ts = int((datetime.now(BJT) - timedelta(days=7)).timestamp())
        lo, hi = max(1, current - 1200000), current
        while lo < hi:
            mid = (lo + hi) // 2
            d = await rpc(session, "eth_getBlockByNumber", [hex(mid), False])
            t = int(d.get("result", {}).get("timestamp", 0), 16)
            if t < target_ts: lo = mid + 1
            else: hi = mid
        
        bonus_logs = await get_logs(session, ADDR_BONUS, lo, current)
        stake_logs = await get_logs(session, ADDR_STAKE, lo, current)
        bonus_daily = await parse_logs(session, bonus_logs, ADDR_BONUS)
        stake_daily = await parse_logs(session, stake_logs, ADDR_STAKE)
        bonus_bal = await get_balance(session, ADDR_BONUS)
        stake_bal = await get_balance(session, ADDR_STAKE)
        
        full, today = compute_summary(bonus_daily, stake_daily, bonus_bal, stake_bal, current)
        save_files(full, {
            "bonus_withdrawal": today.get("bonus_withdrawal", 0),
            "stake_in": today.get("stake_in", 0),
            "stake_out": today.get("stake_out", 0),
            "net_stake": today.get("net_stake", 0),
            "bonus_balance": today.get("bonus_balance", 0),
            "stake_balance": today.get("stake_balance", 0),
            "last_updated": full["last_updated"]
        })
        DATA = full
        await broadcast({"type": "full_update", "data": full})
        print(f"  History done. {len(bonus_daily)+len(stake_daily)} days, block {current}")
        
        # Phase 2: Real-time polling
        print(f"[{datetime.now(BJT).isoformat()}] Real-time monitoring started (poll 15s)...")
        last_block = current
        
        while True:
            try:
                current = await get_block_number(session)
                if current > last_block:
                    bonus_new = await get_logs(session, ADDR_BONUS, last_block + 1, current)
                    stake_new = await get_logs(session, ADDR_STAKE, last_block + 1, current)
                    
                    if bonus_new or stake_new:
                        b_parsed = await parse_logs(session, bonus_new, ADDR_BONUS)
                        s_parsed = await parse_logs(session, stake_new, ADDR_STAKE)
                        merge_daily(bonus_daily, b_parsed)
                        merge_daily(stake_daily, s_parsed)
                        bonus_bal = await get_balance(session, ADDR_BONUS)
                        stake_bal = await get_balance(session, ADDR_STAKE)
                        
                        full, today = compute_summary(bonus_daily, stake_daily, bonus_bal, stake_bal, current)
                        save_files(full, {
                            "bonus_withdrawal": today.get("bonus_withdrawal", 0),
                            "stake_in": today.get("stake_in", 0),
                            "stake_out": today.get("stake_out", 0),
                            "net_stake": today.get("net_stake", 0),
                            "bonus_balance": today.get("bonus_balance", 0),
                            "stake_balance": today.get("stake_balance", 0),
                            "last_updated": full["last_updated"]
                        })
                        DATA = full
                        await broadcast({"type": "full_update", "data": full})
                        print(f"  [{datetime.now(BJT).strftime('%H:%M:%S')}] Updated block {current}")
                    
                    last_block = current
            except Exception as e:
                print(f"  Poll error: {e}")
            
            await asyncio.sleep(15)

# ====== WebSocket Server ======

async def ws_handler(websocket):
    WS_CLIENTS.add(websocket)
    print(f"  WS client connected ({len(WS_CLIENTS)} total)")
    try:
        # Send current data immediately
        if DATA["daily_summary"]:
            await websocket.send(json.dumps({"type": "full_update", "data": DATA}))
        async for _ in websocket:
            pass  # Keep connection open
    except:
        pass
    finally:
        WS_CLIENTS.discard(websocket)
        print(f"  WS client disconnected ({len(WS_CLIENTS)} remaining)")

# ====== HTTP Handler ======

async def http_handler(reader, writer):
    request = (await reader.read(65536)).decode()
    if not request:
        writer.close(); return
    
    path = request.split(" ")[1] if " " in request else "/"
    
    def send(status, content_type, body):
        resp = f"HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {len(body)}\r\nAccess-Control-Allow-Origin: *\r\n\r\n"
        writer.write((resp + body).encode())
        writer.close()
    
    if path == "/":
        try:
            with open(os.path.join(STATIC_DIR, "dashboard.html")) as f:
                send("200 OK", "text/html", f.read())
        except: send("500", "text/plain", "Error")
    
    elif path == "/api/data":
        if DATA["daily_summary"]:
            send("200 OK", "application/json", json.dumps(DATA))
        else:
            try:
                with open(os.path.join(DATA_DIR, "ark_data.json")) as f:
                    send("200 OK", "application/json", f.read())
            except: send("200 OK", "application/json", json.dumps({"daily_summary": {}}))
    
    elif path == "/api/today":
        try:
            with open(os.path.join(DATA_DIR, "today_data.json")) as f:
                send("200 OK", "application/json", f.read())
        except: send("200 OK", "application/json", json.dumps({"error": "No data"}))
    
    elif path.startswith("/static/"):
        fname = path.split("/")[-1]
        fp = os.path.join(STATIC_DIR, fname)
        if os.path.exists(fp):
            with open(fp) as f:
                send("200 OK", "text/html" if fname.endswith(".html") else "text/css", f.read())
        else: send("404", "text/plain", "Not found")
    
    else: send("404", "text/plain", "Not found")

async def main_http():
    server = await asyncio.start_server(http_handler, "0.0.0.0", int(os.environ.get("PORT", 8899)))
    print(f"[{datetime.now(BJT).isoformat()}] HTTP server on port {int(os.environ.get('PORT', 8899))}")
    async with server: await server.serve_forever()

# ====== Start ======

async def main():
    # Start monitor
    asyncio.create_task(monitor_loop())
    # Start WebSocket
    ws_port = int(os.environ.get("WS_PORT", 8898))
    ws_server = await websockets.serve(ws_handler, "0.0.0.0", ws_port)
    print(f"[{datetime.now(BJT).isoformat()}] WebSocket on port {ws_port}")
    # Start HTTP
    await main_http()

if __name__ == "__main__":
    asyncio.run(main())
