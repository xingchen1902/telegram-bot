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

TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Shared status
STATUS = {"phase": "starting", "progress": 0, "error": ""}

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

def fetch_and_aggregate(addr, direction, from_block, to_block):
    """
    direction: 'from' = addr sends tokens (outgoing)
               'to' = addr receives tokens (incoming)
    Returns: daily dict {date: total}
    """
    padded = "0x" + addr[2:].lower().zfill(64)
    topics = [TOPIC, padded, None] if direction == 'from' else [TOPIC, None, padded]
    
    all_logs = []
    blocks_needed = set()
    chunk = 50000
    total_chunks = (to_block - from_block) // chunk + 1
    chunk_idx = 0
    
    for start in range(from_block, to_block+1, chunk):
        end = min(start+chunk-1, to_block)
        chunk_idx += 1
        d = rpc("eth_getLogs", [{"fromBlock":hex(start),"toBlock":hex(end),"address":TOKEN,"topics":topics}])
        if isinstance(d.get("result"), list):
            for l in d["result"]:
                all_logs.append(l)
                blocks_needed.add(int(l["blockNumber"], 16))
        time.sleep(0.2)
    
    # Get timestamps
    blist = list(blocks_needed)
    bts = {}
    for i in range(0, len(blist), 100):
        batch = [{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[hex(bn),False],"id":bn} for bn in blist[i:i+100]]
        for r in rpc_batch(batch):
            if r.get("result"): bts[r["id"]] = int(r["result"]["timestamp"], 16)
    
    daily = {}
    for l in all_logs:
        bn = int(l["blockNumber"], 16)
        ts = bts.get(bn, get_block_ts(bn))
        if not ts: continue
        date = datetime.fromtimestamp(ts, tz=BJT).strftime("%Y-%m-%d")
        daily[date] = daily.get(date, 0) + int(l["data"], 16) / 1e18
    
    return daily

def get_block_ts(block_num):
    d = rpc("eth_getBlockByNumber", [hex(block_num), False])
    if d.get("result"): return int(d["result"]["timestamp"], 16)
    return 0

def run_collection():
    global STATUS
    try:
        STATUS["phase"] = "connecting"
        current = get_block()
        if not current:
            STATUS["phase"] = "error"
            STATUS["error"] = "RPC not reachable"
            return False
        log(f"Block {current}")
        
        # Find 7-day-ago block
        STATUS["phase"] = "finding_start_block"
        target_ts = int((datetime.now(BJT)-timedelta(days=7)).timestamp())
        lo, hi = max(1, current-1200000), current
        for _ in range(25):
            if lo >= hi: break
            mid = (lo+hi)//2
            ts = get_block_ts(mid)
            if ts == 0: continue
            if ts < target_ts: lo = mid+1
            else: hi = mid
        log(f"Range {lo} -> {current}")
        
        # 1) Bonus outgoing
        STATUS["phase"] = "bonus_out"
        log("Bonus outgoing (动静态提取)...")
        bd = fetch_and_aggregate(ADDR_BONUS, 'from', lo, current)
        log(f"  {json.dumps({d:round(v,4) for d,v in sorted(bd.items())})}")
        STATUS["progress"] = 33
        
        # 2) Stake incoming
        STATUS["phase"] = "stake_in"
        log("Stake incoming (新增质押)...")
        si = fetch_and_aggregate(ADDR_STAKE, 'to', lo, current)
        log(f"  {json.dumps({d:round(v,4) for d,v in sorted(si.items())})}")
        STATUS["progress"] = 66
        
        # 3) Stake outgoing
        STATUS["phase"] = "stake_out"
        log("Stake outgoing (赎回)...")
        so = fetch_and_aggregate(ADDR_STAKE, 'from', lo, current)
        log(f"  {json.dumps({d:round(v,4) for d,v in sorted(so.items())})}")
        STATUS["progress"] = 90
        
        # Balances
        STATUS["phase"] = "balances"
        bb = get_balance(ADDR_BONUS)
        sb = get_balance(ADDR_STAKE)
        log(f"Balances: bonus={bb:.4f}, stake={sb:.4f}")
        
        # Build summary
        all_dates = sorted(set(list(bd.keys()) + list(si.keys()) + list(so.keys())))
        b_bal, s_bal = {}, {}
        rb, rs = bb, sb
        for d in reversed(all_dates):
            b_bal[d], s_bal[d] = round(rb,4), round(rs,4)
            rb -= bd.get(d, 0)
            rs -= si.get(d, 0) - so.get(d, 0)
        
        daily = {}
        for d in all_dates:
            bo, sin, sout = round(bd.get(d,0),4), round(si.get(d,0),4), round(so.get(d,0),4)
            daily[d] = {"date":d,"bonus_withdrawal":bo,"bonus_balance":b_bal.get(d,0),"stake_in":sin,"stake_out":sout,"stake_balance":s_bal.get(d,0),"net_stake":round(sin-sout-bo,4)}
        
        now = datetime.now(BJT).isoformat()
        full = {"last_updated":now,"current_block":current,"daily_summary":daily,"current_balances":{"bonus_pool":round(bb,4),"stake_pool":round(sb,4)}}
        td = daily.get(datetime.now(BJT).strftime("%Y-%m-%d"), {})
        td_data = {"bonus_withdrawal":td.get("bonus_withdrawal",0),"stake_in":td.get("stake_in",0),"stake_out":td.get("stake_out",0),"net_stake":td.get("net_stake",0),"bonus_balance":td.get("bonus_balance",0),"stake_balance":td.get("stake_balance",0),"last_updated":now}
        json.dump(full, open(os.path.join(DATA_DIR,"ark_data.json"),"w"), indent=2)
        json.dump(td_data, open(os.path.join(DATA_DIR,"today_data.json"),"w"), indent=2)
        
        log("=== DONE ===")
        log(json.dumps(full, indent=2))
        STATUS["phase"] = "done"
        STATUS["progress"] = 100
        return True
        
    except Exception as e:
        log("ERROR: " + traceback.format_exc())
        STATUS["phase"] = "error"
        STATUS["error"] = str(e)
        return False

# ====== Routes ======
@route("/")
def index():
    return static_file("dashboard.html", root=STATIC_DIR)

@route("/api/data")
def api_data():
    response.set_header("Access-Control-Allow-Origin","*")
    try: return json.load(open(os.path.join(DATA_DIR, "ark_data.json")))
    except: return {"daily_summary":{}}

@route("/api/today")
def api_today():
    response.set_header("Access-Control-Allow-Origin","*")
    try: return json.load(open(os.path.join(DATA_DIR, "today_data.json")))
    except: return {"error":"No data"}

@route("/api/status")
def api_status():
    response.set_header("Access-Control-Allow-Origin","*")
    try: data = json.load(open(os.path.join(DATA_DIR, "ark_data.json")))
    except: data = {}
    return {"status": STATUS, "data_updated": data.get("last_updated",""), "data_block": data.get("current_block",0)}

@route("/api/debug")
def api_debug():
    response.set_header("Access-Control-Allow-Origin","*")
    try:
        d = rpc("eth_blockNumber", retries=2)
        bn = int(d.get("result","0x0"),16) if d.get("result") else 0
        try: data = json.load(open(os.path.join(DATA_DIR, "ark_data.json")))
        except: data = {}
        return {"rpc_ok":bool(d.get("result")),"block":bn,"data_updated":data.get("last_updated","")}
    except Exception as e: return {"rpc_ok":False,"error":str(e)}

@route("/static/<filename:path>")
def static(filename):
    return static_file(filename, root=STATIC_DIR)

app = default_app()

if __name__ == "__main__":
    # Load cache first so UI works immediately
    try: DATA = json.load(open(os.path.join(DATA_DIR, "ark_data.json")))
    except: pass
    
    # Start collection in background thread
    def bg_collect():
        time.sleep(1)
        run_collection()
    
    threading.Thread(target=bg_collect, daemon=True).start()
    port = int(os.environ.get("PORT", 8899))
    log(f"Server on port {port}")
    run(host="0.0.0.0", port=port, server="auto")
