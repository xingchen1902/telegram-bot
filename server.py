#!/usr/bin/env python3
"""ARK Dashboard - Bottle HTTP + data collector"""
import json, os, threading, time, traceback
from datetime import datetime, timedelta, timezone
from bottle import route, run, static_file, response, default_app
import requests

BJT = timezone(timedelta(hours=8))
DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(DIR, "data")
STATIC_DIR = os.path.join(DIR, "static")
os.makedirs(DATA_DIR, exist_ok=True)

RPC_URL = "https://bsc-mainnet.nodereal.io/v1/7b7adb4899124647867575e354005c07"
TOKEN = "0xCae117ca6Bc8A341D2E7207F30E180f0e5618B9D"
ADDR_BONUS = "0x8501168656FcaC4628F6910CcABEA8B64Ebe5BD4"
ADDR_STAKE = "0xd1D95292F450b665566df4c4255615eF4Ed9BD0B"

DATA_FILE = os.path.join(DATA_DIR, "ark_data.json")
TODAY_FILE = os.path.join(DATA_DIR, "today_data.json")

def log(msg):
    print(f"[{datetime.now(BJT).strftime('%H:%M:%S')}] {msg}", flush=True)

def rpc(method, params=None, retries=3):
    if params is None: params=[]
    for i in range(retries):
        try:
            d = requests.post(RPC_URL, json={"jsonrpc":"2.0","method":method,"params":params,"id":1}, timeout=30).json()
            if "result" in d: return d
        except: pass
        time.sleep(1)
    return {}

def rpc_batch(items):
    try: return requests.post(RPC_URL, json=items, timeout=60).json()
    except: return []

def get_block():
    d = rpc("eth_blockNumber")
    return int(d.get("result","0x0"),16) if d.get("result") else 0

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
    for start in range(f, t+1, 5000):
        end = min(start+4999, t)
        for tf in [[topic,padded,None],[topic,None,padded]]:
            d = rpc("eth_getLogs", [{"fromBlock":hex(start),"toBlock":hex(end),"address":TOKEN,"topics":tf}])
            if isinstance(d.get("result"), list):
                for l in d["result"]:
                    k = l["transactionHash"]+l["logIndex"]
                    if k not in seen: seen.add(k); logs.append(l)
                break
        time.sleep(0.1)
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

def compute(data_dir, block):
    """Recompute summary from stored daily data"""
    bd_file = os.path.join(data_dir, "bonus_daily.json")
    sd_file = os.path.join(data_dir, "stake_daily.json")
    try:
        bd = json.load(open(bd_file))
        sd = json.load(open(sd_file))
    except:
        return None
    bb = get_balance(ADDR_BONUS)
    sb = get_balance(ADDR_STAKE)
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
    full = {"last_updated":datetime.now(BJT).isoformat(),"current_block":block,"daily_summary":s,"current_balances":{"bonus_pool":round(bb,4),"stake_pool":round(sb,4)}}
    today = {"bonus_withdrawal":td.get("bonus_withdrawal",0),"stake_in":td.get("stake_in",0),"stake_out":td.get("stake_out",0),"net_stake":td.get("net_stake",0),"bonus_balance":td.get("bonus_balance",0),"stake_balance":td.get("stake_balance",0),"last_updated":full["last_updated"]}
    json.dump(full, open(DATA_FILE,"w"), indent=2)
    json.dump(today, open(TODAY_FILE,"w"), indent=2)
    return full

# ====== Bottle Routes ======
@route("/")
def index():
    return static_file("dashboard.html", root=STATIC_DIR)

@route("/api/data")
def api_data():
    response.set_header("Access-Control-Allow-Origin","*")
    try: return json.load(open(DATA_FILE))
    except: return {"daily_summary":{}}

@route("/api/today")
def api_today():
    response.set_header("Access-Control-Allow-Origin","*")
    try: return json.load(open(TODAY_FILE))
    except: return {"error":"No data"}

@route("/api/debug")
def api_debug():
    response.set_header("Access-Control-Allow-Origin","*")
    try:
        d = rpc("eth_blockNumber", retries=2)
        bn = int(d.get("result","0x0"),16) if d.get("result") else 0
        try: data = json.load(open(DATA_FILE))
        except: data = {}
        return {"rpc_ok":bool(d.get("result")),"block":bn,"data_updated":data.get("last_updated","")}
    except Exception as e: return {"rpc_ok":False,"error":str(e)}

@route("/api/recollect")
def api_recollect():
    """Manually trigger collection"""
    response.set_header("Access-Control-Allow-Origin","*")
    threading.Thread(target=run_collection, daemon=True).start()
    return {"status":"started"}

@route("/static/<filename:path>")
def static(filename):
    return static_file(filename, root=STATIC_DIR)

# ====== Full Collection ======
def run_collection():
    log("=== Collection START ===")
    try:
        current = get_block()
        if not current:
            log("ERROR: RPC not reachable")
            return
        log(f"Block {current}")
        
        target = int((datetime.now(BJT)-timedelta(days=7)).timestamp())
        lo, hi = max(1,current-1200000), current
        for _ in range(25):
            if lo>=hi: break
            mid=(lo+hi)//2
            d=rpc("eth_getBlockByNumber",[hex(mid),False])
            ts=int(d.get("result",{}).get("timestamp",0),16) if d.get("result") else 0
            if ts<target: lo=mid+1
            else: hi=mid
        log(f"Blocks {lo} -> {current}")
        
        log("Fetching bonus transfers...")
        bl = get_logs(ADDR_BONUS, lo, current)
        log(f"  Bonus: {len(bl)} txns")
        bd = parse_logs(bl, ADDR_BONUS)
        
        log("Fetching stake transfers...")
        sl = get_logs(ADDR_STAKE, lo, current)
        log(f"  Stake: {len(sl)} txns")
        sd = parse_logs(sl, ADDR_STAKE)
        
        log("Bonus: " + json.dumps({d:{k:round(v,4) for k,v in bd[d].items()} for d in sorted(bd.keys())}))
        log("Stake: " + json.dumps({d:{k:round(v,4) for k,v in sd[d].items()} for d in sorted(sd.keys())}))
        
        # Save raw daily data
        json.dump({d:{k:v for k,v in bd[d].items()} for d in bd}, open(os.path.join(DATA_DIR,"bonus_daily.json"),"w"))
        json.dump({d:{k:v for k,v in sd[d].items()} for d in sd}, open(os.path.join(DATA_DIR,"stake_daily.json"),"w"))
        
        # Compute and save
        full = compute(DATA_DIR, current)
        if full:
            log(f"DONE! Block {current}")
        else:
            log("ERROR: compute failed")
    except:
        log("ERROR: " + traceback.format_exc())

if __name__ == "__main__":
    # Run collection before starting server
    run_collection()
    log("Starting server...")
    port = int(os.environ.get("PORT", 8899))
    run(host="0.0.0.0", port=port, server="auto")
