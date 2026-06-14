#!/usr/bin/env python3
"""ARK Dashboard - Bottle HTTP + data collector"""
import json, os, threading, time, traceback, sys
from datetime import datetime, timedelta, timezone
from bottle import route, run, static_file, response, default_app
import requests

BJT = timezone(timedelta(hours=8))
DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(DIR, "data")
STATIC_DIR = os.path.join(DIR, "static")
os.makedirs(DATA_DIR, exist_ok=True)

# RPC endpoints
RPC_URL = "https://bsc-mainnet.nodereal.io/v1/7b7adb4899124647867575e354005c07"
TOKEN = "0xCae117ca6Bc8A341D2E7207F30E180f0e5618B9D"
ADDR_BONUS = "0x8501168656FcaC4628F6910CcABEA8B64Ebe5BD4"
ADDR_STAKE = "0xd1D95292F450b665566df4c4255615eF4Ed9BD0B"

DATA = {"daily_summary": {}, "current_block": 0, "last_updated": ""}

def log(msg):
    print(f"[{datetime.now(BJT).strftime('%H:%M:%S')}] {msg}", flush=True)

def rpc(method, params=None, retries=5):
    if params is None: params=[]
    for i in range(retries):
        try:
            d = requests.post(RPC_URL, json={"jsonrpc":"2.0","method":method,"params":params,"id":1}, timeout=30).json()
            if "result" in d: return d
            log(f"RPC no result: {d}")
        except Exception as e:
            log(f"RPC fail ({i+1}/{retries}): {e}")
            if i == retries-1:
                log(traceback.format_exc())
        time.sleep(2)
    return {}

def rpc_batch(items):
    try: return requests.post(RPC_URL, json=items, timeout=60).json()
    except: return []

def get_block():
    d = rpc("eth_blockNumber")
    return int(d.get("result","0x0"),16)

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
    seen=set(); logs=[]
    chunks = list(range(f, t+1, 5000))
    for idx, start in enumerate(chunks):
        end = min(start+4999, t)
        for tf in [[topic,padded,None],[topic,None,padded]]:
            d = rpc("eth_getLogs", [{"fromBlock":hex(start),"toBlock":hex(end),"address":TOKEN,"topics":tf}])
            if isinstance(d.get("result"), list):
                for l in d["result"]:
                    k = l["transactionHash"]+l["logIndex"]
                    if k not in seen: seen.add(k); logs.append(l)
                break
        time.sleep(0.1)
        if (idx+1)%50==0: log(f"  {addr[:10]}... {idx+1}/{len(chunks)} chunks, {len(logs)} logs")
    return logs

def parse_logs(logs, addr):
    al=addr.lower()
    bs=set(int(l["blockNumber"],16) for l in logs)
    tm=get_ts(list(bs))
    dly={}
    for l in logs:
        bn=int(l["blockNumber"],16); ts=tm.get(bn)
        if not ts: continue
        d=datetime.fromtimestamp(ts,tz=BJT).strftime("%Y-%m-%d")
        if d not in dly: dly[d]={"in":0,"out":0}
        v=int(l["data"],16)/1e18
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
    except Exception as e: log(f"Save err: {e}")

@route("/")
def index():
    return static_file("dashboard.html", root=STATIC_DIR)

@route("/api/data")
def api_data():
    response.set_header("Access-Control-Allow-Origin","*")
    if DATA["daily_summary"]:
        return DATA
    try: return json.load(open(os.path.join(DATA_DIR,"ark_data.json")))
    except: return {"daily_summary":{}}

@route("/api/today")
def api_today():
    response.set_header("Access-Control-Allow-Origin","*")
    try: return json.load(open(os.path.join(DATA_DIR,"today_data.json")))
    except: return {"error":"No data"}

@route("/api/debug")
def api_debug():
    response.set_header("Access-Control-Allow-Origin","*")
    try:
        d = rpc("eth_blockNumber", retries=2)
        bn = int(d.get("result","0x0"),16) if d.get("result") else 0
        return {"rpc_ok": bool(d.get("result")), "block": bn, "data_updated": DATA.get("last_updated","")}
    except Exception as e:
        return {"rpc_ok": False, "error": str(e)}

@route("/static/<filename:path>")
def static(filename):
    return static_file(filename, root=STATIC_DIR)

def collector():
    global DATA
    log("Collector: starting...")
    
    # Load cache
    try:
        with open(os.path.join(DATA_DIR,"ark_data.json")) as f:
            cache = json.load(f)
            if cache.get("daily_summary"):
                DATA = cache
                log(f"Cache loaded: block {cache.get('current_block')}")
    except: pass
    
    # Test RPC
    current = get_block()
    if not current:
        log("ERROR: RPC not responding, collector will retry in 60s")
        time.sleep(60)
        current = get_block()
        if not current:
            log("ERROR: RPC still not responding, giving up")
            return
    
    log(f"RPC OK. Current block: {current}")
    
    # Find 7-day-ago block
    target = int((datetime.now(BJT)-timedelta(days=7)).timestamp())
    lo, hi = max(1,current-1200000), current
    for _ in range(25):
        if lo>=hi: break
        mid=(lo+hi)//2
        d=rpc("eth_getBlockByNumber",[hex(mid),False])
        ts=int(d.get("result",{}).get("timestamp",0),16) if d.get("result") else 0
        if ts<target: lo=mid+1
        else: hi=mid
    
    log(f"History: {lo} -> {current} ({current-lo} blocks)")
    
    bl = get_logs(ADDR_BONUS, lo, current)
    sl = get_logs(ADDR_STAKE, lo, current)
    log(f"Bonus: {len(bl)} txns, Stake: {len(sl)} txns")
    
    log("Parsing bonus...")
    bd = parse_logs(bl, ADDR_BONUS)
    log("Parsing stake...")
    sd = parse_logs(sl, ADDR_STAKE)
    
    log("Bonus raw: " + json.dumps({d:{k:round(v,4) for k,v in bd[d].items()} for d in sorted(bd.keys())}))
    log("Stake raw: " + json.dumps({d:{k:round(v,4) for k,v in sd[d].items()} for d in sorted(sd.keys())}))
    
    bb = get_balance(ADDR_BONUS)
    sb = get_balance(ADDR_STAKE)
    log(f"Balances: bonus={bb:.4f}, stake={sb:.4f}")
    
    full, td = build_summary(bd, sd, bb, sb, current)
    save_files(full, {**td, "last_updated":full["last_updated"]})
    DATA = full
    log(f"DONE! Block {current}")
    
    last = current
    while True:
        time.sleep(15)
        try:
            current = get_block()
            if current > last:
                for addr, store in [(ADDR_BONUS,bd),(ADDR_STAKE,sd)]:
                    logs = get_logs(addr, last+1, current)
                    if logs:
                        parsed = parse_logs(logs, addr)
                        for d,v in parsed.items():
                            if d not in store: store[d]={"in":0,"out":0}
                            store[d]["in"]+=v["in"]; store[d]["out"]+=v["out"]
                bb = get_balance(ADDR_BONUS)
                sb = get_balance(ADDR_STAKE)
                full, td = build_summary(bd, sd, bb, sb, current)
                save_files(full, {**td, "last_updated":full["last_updated"]})
                DATA = full
                log(f"Poll: Block {current}")
                last = current
        except: pass

app = default_app()
if __name__ == "__main__":
    t = threading.Thread(target=collector, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 8899))
    log(f"Server on port {port}")
    run(host="0.0.0.0", port=port, server="auto")
