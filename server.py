#!/usr/bin/env python3
"""ARK Dashboard - Bottle HTTP + data collector + Supabase"""
import json, os, threading, time, traceback, random
from datetime import datetime, timedelta, timezone
from bottle import route, run, static_file, response, default_app
import requests

BJT = timezone(timedelta(hours=8))
DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(DIR, "data")
STATIC_DIR = os.path.join(DIR, "static")
os.makedirs(DATA_DIR, exist_ok=True)

RPC_ENDPOINTS = [
    "https://bsc-dataseed1.binance.org",
    "https://bsc-dataseed2.binance.org",
    "https://bsc-dataseed3.binance.org",
    "https://bsc-dataseed4.binance.org",
]
TOKEN = "0xCae117ca6Bc8A341D2E7207F30E180f0e5618B9D"
ADDR_BONUS = "0x8501168656FcaC4628F6910CcABEA8B64Ebe5BD4"
ADDR_STAKE = "0xd1D95292F450b665566df4c4255615eF4Ed9BD0B"
TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

SUPABASE_URL = "https://jxyztwcoidhufparnpre.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp4eXp0d2NvaWRodWZwYXJucHJlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc3MTk0NzcsImV4cCI6MjA2MzI5NTQ3N30.B8PcmYL2nrYPUhM4K_0IETcRZLGU0TZKDJw0TYvMjts"

STATUS = {"phase": "waiting", "progress": 0, "error": ""}

def log(msg):
    print(f"[{datetime.now(BJT).strftime('%H:%M:%S')}] {msg}", flush=True)

def rpc_call(method, params=None, retries=5):
    """RPC call with multiple endpoints and retries, handles rate limits"""
    if params is None: params=[]
    for i in range(retries):
        url = random.choice(RPC_ENDPOINTS)
        try:
            d = requests.post(url, json={"jsonrpc":"2.0","method":method,"params":params,"id":1}, timeout=30).json()
            if "result" in d: return d
            if "error" in d:
                err = d.get("error",{}).get("message","")
                if "limit" in err.lower():
                    time.sleep(2)  # Rate limited, wait longer
                    continue
        except: pass
        time.sleep(0.5)
    return {}

def rpc_batch(items):
    try: return requests.post(random.choice(RPC_ENDPOINTS), json=items, timeout=60).json()
    except: return []

def get_block():
    d = rpc_call("eth_blockNumber")
    return int(d.get("result","0x0"),16) if d.get("result") else 0

def get_balance(addr):
    d = rpc_call("eth_call", [{"to":TOKEN,"data":"0x70a08231"+addr[2:].lower().zfill(64)},"latest"])
    return int(d["result"],16)/1e18 if d.get("result") else 0

def get_block_ts(block_num):
    d = rpc_call("eth_getBlockByNumber", [hex(block_num), False])
    return int(d["result"]["timestamp"], 16) if d.get("result") else 0

def save_to_supabase(full, current_block):
    today_str = datetime.now(BJT).strftime("%Y-%m-%d")
    td = full["daily_summary"].get(today_str, {})
    headers = {"apikey":SUPABASE_KEY,"Authorization":f"Bearer {SUPABASE_KEY}","Content-Type":"application/json"}
    for date, row in full["daily_summary"].items():
        payload = {"date":date,"bonus_withdrawal":row["bonus_withdrawal"],"bonus_balance":row["bonus_balance"],
                   "stake_in":row["stake_in"],"stake_out":row["stake_out"],"stake_balance":row["stake_balance"],
                   "net_stake":row["net_stake"],"updated_at":datetime.now(BJT).isoformat()}
        try: requests.post(f"{SUPABASE_URL}/rest/v1/ark_daily_summary", headers={**headers,"Prefer":"resolution=merge-duplicates"}, json=payload, timeout=10)
        except: pass
    rt = {"bonus_withdrawal":td.get("bonus_withdrawal",0),"stake_in":td.get("stake_in",0),"stake_out":td.get("stake_out",0),
          "net_stake":td.get("net_stake",0),"bonus_balance":td.get("bonus_balance",0),"stake_balance":td.get("stake_balance",0),
          "current_block":current_block,"recorded_at":datetime.now(BJT).isoformat()}
    try: requests.post(f"{SUPABASE_URL}/rest/v1/ark_realtime", headers=headers, json=rt, timeout=10)
    except: pass
    log("Supabase saved")

def get_logs_small(addr, direction, from_block, to_block):
    """Get logs for a small block range (for polling - fast)"""
    padded = "0x"+addr[2:].lower().zfill(64)
    topics = [TOPIC, padded, None] if direction == 'from' else [TOPIC, None, padded]
    d = rpc_call("eth_getLogs", [{"fromBlock":hex(from_block),"toBlock":hex(to_block),"address":TOKEN,"topics":topics}], retries=3)
    if isinstance(d.get("result"), list): return d["result"]
    return []

def get_logs_history(addr, direction, from_block, to_block):
    """Get logs for historical data - small chunks with delays"""
    padded = "0x"+addr[2:].lower().zfill(64)
    topics = [TOPIC, padded, None] if direction == 'from' else [TOPIC, None, padded]
    all_logs = []
    # Use smaller chunks (2000 blocks) to avoid rate limits
    for start in range(from_block, to_block+1, 2000):
        end = min(start+1999, to_block)
        for attempt in range(10):
            d = rpc_call("eth_getLogs", [{"fromBlock":hex(start),"toBlock":hex(end),"address":TOKEN,"topics":topics}], retries=3)
            if isinstance(d.get("result"), list):
                all_logs.extend(d["result"])
                break
            time.sleep(3)  # Rate limited, wait longer
        time.sleep(0.5)  # Polite delay between chunks
    return all_logs

def parse_history(logs):
    """Parse logs into daily aggregates"""
    blocks_needed = set(int(l["blockNumber"],16) for l in logs)
    blist = list(blocks_needed); bts = {}
    for i in range(0, len(blist), 100):
        batch = [{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[hex(bn),False],"id":bn} for bn in blist[i:i+100]]
        for r in rpc_batch(batch):
            if r.get("result"): bts[r["id"]] = int(r["result"]["timestamp"],16)
        time.sleep(0.3)
    daily = {}
    for l in logs:
        bn = int(l["blockNumber"],16); ts = bts.get(bn)
        if not ts: continue
        date = datetime.fromtimestamp(ts,tz=BJT).strftime("%Y-%m-%d")
        daily[date] = daily.get(date,0) + int(l["data"],16)/1e18
    return daily, bts

def build_output(bd, si, so, bb, sb, current):
    all_dates = sorted(set(list(bd.keys())+list(si.keys())+list(so.keys())))
    b_bal,s_bal = {},{}
    rb,rs = bb,sb
    for d in reversed(all_dates):
        b_bal[d],s_bal[d] = round(rb,4), round(rs,4)
        rb -= bd.get(d,0); rs -= si.get(d,0)-so.get(d,0)
    daily = {}
    for d in all_dates:
        bo,sin_,sout = round(bd.get(d,0),4), round(si.get(d,0),4), round(so.get(d,0),4)
        daily[d] = {"date":d,"bonus_withdrawal":bo,"bonus_balance":b_bal.get(d,0),"stake_in":sin_,"stake_out":sout,"stake_balance":s_bal.get(d,0),"net_stake":round(sin_-sout-bo,4)}
    now = datetime.now(BJT).isoformat()
    full = {"last_updated":now,"current_block":current,"daily_summary":daily,"current_balances":{"bonus_pool":round(bb,4),"stake_pool":round(sb,4)}}
    return full, daily.get(datetime.now(BJT).strftime("%Y-%m-%d"),{})

def run_collection():
    global STATUS
    try:
        STATUS = {"phase":"connecting","progress":5,"error":""}
        log("=== DATA COLLECTION ===")
        current = get_block()
        if not current: STATUS={"phase":"error","progress":0,"error":"RPC dead"}; return
        log(f"Block: {current}")
        
        STATUS = {"phase":"finding_start","progress":10}
        target_ts = int((datetime.now(BJT)-timedelta(days=7)).timestamp())
        lo, hi = max(1,current-1200000), current
        for _ in range(30):
            if lo>=hi: break
            mid=(lo+hi)//2; ts=get_block_ts(mid)
            if ts==0: continue
            if ts<target_ts: lo=mid+1; break
            else: hi=mid
        if lo == max(1,current-1200000):
            for _ in range(30):
                if lo>=hi: break
                mid=(lo+hi)//2; ts=get_block_ts(mid)
                if ts==0: continue
                if ts<target_ts: lo=mid+1
                else: hi=mid
        log(f"Range: {lo} -> {current}")
        
        STATUS = {"phase":"bonus_out","progress":20}
        log("Bonus outgoing...")
        bl = get_logs_history(ADDR_BONUS, 'from', lo, current)
        log(f"  {len(bl)} logs")
        bd, _ = parse_history(bl)
        log(f"  {json.dumps({d:round(v,4) for d,v in sorted(bd.items())})}")
        
        STATUS = {"phase":"stake_in","progress":45}
        log("Stake incoming...")
        si_logs = get_logs_history(ADDR_STAKE, 'to', lo, current)
        log(f"  {len(si_logs)} logs")
        si, _ = parse_history(si_logs)
        log(f"  {json.dumps({d:round(v,4) for d,v in sorted(si.items())})}")
        
        STATUS = {"phase":"stake_out","progress":70}
        log("Stake outgoing...")
        so_logs = get_logs_history(ADDR_STAKE, 'from', lo, current)
        log(f"  {len(so_logs)} logs")
        so, _ = parse_history(so_logs)
        log(f"  {json.dumps({d:round(v,4) for d,v in sorted(so.items())})}")
        
        STATUS = {"phase":"balances","progress":90}
        bb = get_balance(ADDR_BONUS); sb = get_balance(ADDR_STAKE)
        log(f"Balances: bonus={bb:.4f}, stake={sb:.4f}")
        
        if not bd and not si and not so:
            log("NO DATA FOUND - NOT overwriting cache")
            STATUS = {"phase":"error","progress":0,"error":"No data from RPC"}
            return
        
        full, td = build_output(bd, si, so, bb, sb, current)
        td_data = {"bonus_withdrawal":td.get("bonus_withdrawal",0),"stake_in":td.get("stake_in",0),"stake_out":td.get("stake_out",0),
                   "net_stake":td.get("net_stake",0),"bonus_balance":td.get("bonus_balance",0),"stake_balance":td.get("stake_balance",0),"last_updated":full["last_updated"]}
        json.dump(full, open(os.path.join(DATA_DIR,"ark_data.json"),"w"), indent=2)
        json.dump(td_data, open(os.path.join(DATA_DIR,"today_data.json"),"w"), indent=2)
        log("Files saved")
        
        try: save_to_supabase(full, current)
        except Exception as e: log(f"Supabase: {e}")
        
        STATUS = {"phase":"done","progress":100}
        log("=== DONE ===")
        
    except Exception as e:
        log("FATAL: "+traceback.format_exc())
        STATUS = {"phase":"error","progress":0,"error":str(e)}

@route("/")
def index():
    return static_file("dashboard.html", root=STATIC_DIR)

@route("/api/data")
def api_data():
    response.set_header("Access-Control-Allow-Origin","*")
    try: return json.load(open(os.path.join(DATA_DIR,"ark_data.json")))
    except: return {"daily_summary":{}}

@route("/api/today")
def api_today():
    response.set_header("Access-Control-Allow-Origin","*")
    try: return json.load(open(os.path.join(DATA_DIR,"today_data.json")))
    except: return {"error":"No data"}

@route("/api/status")
def api_status():
    response.set_header("Access-Control-Allow-Origin","*")
    try: d = json.load(open(os.path.join(DATA_DIR,"ark_data.json")))
    except: d = {}
    return {"status":STATUS,"data_updated":d.get("last_updated",""),"data_block":d.get("current_block",0),"days":sorted(d.get("daily_summary",{}).keys())}

@route("/api/debug")
def api_debug():
    response.set_header("Access-Control-Allow-Origin","*")
    try:
        d = rpc_call("eth_blockNumber", retries=2)
        bn = int(d.get("result","0x0"),16) if d.get("result") else 0
        return {"rpc_ok":bool(d.get("result")),"block":bn,"status":STATUS}
    except Exception as e: return {"rpc_ok":False,"error":str(e)}

@route("/static/<filename:path>")
def static(filename):
    return static_file(filename, root=STATIC_DIR)

app = default_app()

if __name__ == "__main__":
    def bg():
        time.sleep(1)
        run_collection()
    threading.Thread(target=bg, daemon=True).start()
    port = int(os.environ.get("PORT", 8899))
    log(f"Server on port {port}")
    run(host="0.0.0.0", port=port, server="auto")
