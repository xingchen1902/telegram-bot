#!/usr/bin/env python3
"""ARK Dashboard - Bottle HTTP server with data collector"""
import json, os, threading, time, sys
from datetime import datetime, timedelta, timezone
from bottle import route, run, static_file, response, default_app
import requests

BJT = timezone(timedelta(hours=8))
DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(DIR, "data")
STATIC_DIR = os.path.join(DIR, "static")
os.makedirs(DATA_DIR, exist_ok=True)

RPC_URL = os.environ.get("RPC_URL", "https://bsc-mainnet.nodereal.io/v1/d96a4e697b0541628f61ae6089a97874")
TOKEN = "0xCae117ca6Bc8A341D2E7207F30E180f0e5618B9D"
ADDR_BONUS = "0x8501168656FcaC4628F6910CcABEA8B64Ebe5BD4"
ADDR_STAKE = "0xd1D95292F450b665566df4c4255615eF4Ed9BD0B"

DATA = {"daily_summary": {}, "current_block": 0, "last_updated": ""}

# ====== RPC ======
def rpc(method, params=None):
    if params is None: params = []
    for _ in range(3):
        try:
            d = requests.post(RPC_URL, json={"jsonrpc":"2.0","method":method,"params":params,"id":1}, timeout=30).json()
            if "result" in d: return d
        except Exception as e: print(f"  RPC retry: {e}")
        time.sleep(2)
    return {}

def rpc_batch(items):
    try: return requests.post(RPC_URL, json=items, timeout=60).json()
    except: return []

def get_block():
    d = rpc("eth_blockNumber")
    return int(d.get("result","0x0"), 16) if d.get("result") else 0

def get_balance(addr):
    d = rpc("eth_call", [{"to":TOKEN,"data":"0x70a08231"+addr[2:].lower().zfill(64)},"latest"])
    return int(d["result"],16)/1e18 if d.get("result") else 0

def get_ts(blocks):
    t = {}
    for i in range(0, len(blocks), 100):
        batch = [{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[hex(bn),False],"id":bn} for bn in blocks[i:i+100]]
        for r in rpc_batch(batch):
            if r.get("result"): t[r["id"]] = int(r["result"]["timestamp"],16)
    return t

def get_logs(addr, f, t):
    padded = "0x"+addr[2:].lower().zfill(64)
    topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    seen = set()
    logs = []
    for start in range(f, t+1, 2000):
        end = min(start+1999, t)
        for tf in [[topic,padded,None],[topic,None,padded]]:
            d = rpc("eth_getLogs", [{"fromBlock":hex(start),"toBlock":hex(end),"address":TOKEN,"topics":tf}])
            if isinstance(d.get("result"), list):
                for l in d["result"]:
                    k = l["transactionHash"]+l["logIndex"]
                    if k not in seen: seen.add(k); logs.append(l)
    return logs

def parse_logs(logs, addr):
    al = addr.lower()
    bs = set(int(l["blockNumber"],16) for l in logs)
    tm = get_ts(list(bs))
    dly = {}
    for l in logs:
        bn = int(l["blockNumber"],16)
        ts = tm.get(bn)
        if not ts: continue
        d = datetime.fromtimestamp(ts, tz=BJT).strftime("%Y-%m-%d")
        if d not in dly: dly[d]={"in":0,"out":0}
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
    s = {}
    for d in ds:
        b, st = bd.get(d,{}), sd.get(d,{})
        bo=round(b.get("out",0),4); si=round(st.get("in",0),4); so=round(st.get("out",0),4)
        n=round(si-so-bo,4)
        s[d]={"date":d,"bonus_withdrawal":bo,"bonus_balance":bb_.get(d,0),"stake_in":si,"stake_out":so,"stake_balance":sb_.get(d,0),"net_stake":n}
    td = s.get(datetime.now(BJT).strftime("%Y-%m-%d"),{})
    now = datetime.now(BJT).isoformat()
    return {"last_updated":now,"current_block":bn,"daily_summary":s,"current_balances":{"bonus_pool":round(bb,4),"stake_pool":round(sb,4)}}, td

def save_files(full, today):
    try:
        with open(os.path.join(DATA_DIR,"ark_data.json"),"w") as f: json.dump(full,f,indent=2)
        with open(os.path.join(DATA_DIR,"today_data.json"),"w") as f: json.dump(today,f,indent=2)
    except Exception as e: print(f"  Save err: {e}")

# ====== Bottle Routes ======
@route("/")
def index():
    return static_file("dashboard.html", root=STATIC_DIR)

@route("/api/data")
def api_data():
    response.set_header("Access-Control-Allow-Origin","*")
    if DATA["daily_summary"]:
        return DATA
    try:
        with open(os.path.join(DATA_DIR,"ark_data.json")) as f: return json.load(f)
    except: return {"daily_summary":{},"last_updated":"","current_block":0,"current_balances":{}}

@route("/api/today")
def api_today():
    response.set_header("Access-Control-Allow-Origin","*")
    try:
        with open(os.path.join(DATA_DIR,"today_data.json")) as f: return json.load(f)
    except: return {"error":"No data"}

@route("/static/<filename:path>")
def static(filename):
    return static_file(filename, root=STATIC_DIR)

# ====== Collector ======
def collector():
    global DATA
    print("Collector: starting...")
    
    # Load cache first so the dashboard has immediate data
    try:
        with open(os.path.join(DATA_DIR,"ark_data.json")) as f:
            cache = json.load(f)
            if cache.get("daily_summary"):
                DATA = cache
                print(f"Loaded cache: block {cache.get('current_block')}, {len(cache['daily_summary'])} days")
    except Exception as e:
        print(f"No valid cache: {e}")
    
    # Test RPC connection
    current = get_block()
    if not current:
        print("ERROR: Cannot get current block after 3 retries")
        return
    
    print(f"Current block: {current}")
    
    # Find block ~7 days ago
    target = int((datetime.now(BJT)-timedelta(days=7)).timestamp())
    lo, hi = max(1,current-1200000), current
    for _ in range(20):
        if lo >= hi: break
        mid = (lo+hi)//2
        d = rpc("eth_getBlockByNumber", [hex(mid), False])
        ts = int(d.get("result",{}).get("timestamp",0),16) if d.get("result") else 0
        if ts < target: lo = mid+1
        else: hi = mid
    
    print(f"History: blocks {lo} -> {current}")
    
    # Fetch logs
    bl = get_logs(ADDR_BONUS, lo, current)
    sl = get_logs(ADDR_STAKE, lo, current)
    print(f"Bonus: {len(bl)} txns, Stake: {len(sl)} txns")
    
    bd = parse_logs(bl, ADDR_BONUS)
    sd = parse_logs(sl, ADDR_STAKE)
    
    print("Bonus daily:", json.dumps({d:{k:round(v,4) for k,v in bd[d].items()} for d in sorted(bd.keys())}))
    print("Stake daily:", json.dumps({d:{k:round(v,4) for k,v in sd[d].items()} for d in sorted(sd.keys())}))
    
    bb = get_balance(ADDR_BONUS)
    sb = get_balance(ADDR_STAKE)
    print(f"Balances: bonus={bb:.4f}, stake={sb:.4f}")
    
    full, td = build_summary(bd, sd, bb, sb, current)
    save_files(full, {**td, "last_updated": full["last_updated"]})
    DATA = full
    print(f"Done! Block {current}")
    
    # Poll every 15s
    last = current
    while True:
        time.sleep(15)
        try:
            current = get_block()
            if current > last:
                for addr, store in [(ADDR_BONUS, bd), (ADDR_STAKE, sd)]:
                    logs = get_logs(addr, last+1, current)
                    if logs:
                        parsed = parse_logs(logs, addr)
                        for d,v in parsed.items():
                            if d not in store: store[d]={"in":0,"out":0}
                            store[d]["in"]+=v["in"]; store[d]["out"]+=v["out"]
                bb = get_balance(ADDR_BONUS)
                sb = get_balance(ADDR_STAKE)
                full, td = build_summary(bd, sd, bb, sb, current)
                save_files(full, {**td, "last_updated": full["last_updated"]})
                DATA = full
                print(f"[{datetime.now(BJT).strftime('%H:%M:%S')}] Block {current}")
                last = current
        except Exception as e: print(f"  Poll: {e}")

app = default_app()

if __name__ == "__main__":
    t = threading.Thread(target=collector, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 8899))
    print(f"Server on port {port}")
    run(host="0.0.0.0", port=port, server="auto")
