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

# Transfer event topic
TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# Padded addresses for topic filtering
BONUS_PAD = "0x" + ADDR_BONUS[2:].lower().zfill(64)
STAKE_PAD = "0x" + ADDR_STAKE[2:].lower().zfill(64)

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

def get_block_ts(block_num):
    d = rpc("eth_getBlockByNumber", [hex(block_num), False])
    if d.get("result"): return int(d["result"]["timestamp"], 16)
    return 0

def fetch_transfers(addr, from_block, to_block, direction):
    """
    direction: 'from' = addr sends tokens (outgoing)
               'to' = addr receives tokens (incoming)
    """
    padded = "0x" + addr[2:].lower().zfill(64)
    if direction == 'from':
        topics = [TOPIC, padded, None]
    else:
        topics = [TOPIC, None, padded]
    
    logs = []
    chunk = 50000  # 50k blocks per request
    for start in range(from_block, to_block+1, chunk):
        end = min(start+chunk-1, to_block)
        d = rpc("eth_getLogs", [{"fromBlock":hex(start),"toBlock":hex(end),"address":TOKEN,"topics":topics}])
        if isinstance(d.get("result"), list):
            logs.extend(d["result"])
        time.sleep(0.1)
    return logs

def aggregate(logs, addr, direction):
    """
    Group logs by date (BJT) and sum values.
    direction: 'from' = sum as out, 'to' = sum as in
    """
    # Collect unique block numbers
    blocks = set()
    for l in logs:
        blocks.add(int(l["blockNumber"], 16))
    
    # Get timestamps in batch
    bts = {}
    blist = list(blocks)
    for i in range(0, len(blist), 100):
        batch = [{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[hex(bn),False],"id":bn} for bn in blist[i:i+100]]
        for r in rpc_batch(batch):
            if r.get("result"): bts[r["id"]] = int(r["result"]["timestamp"], 16)
    
    daily = {}
    for l in logs:
        bn = int(l["blockNumber"], 16)
        ts = bts.get(bn)
        if not ts: continue
        date = datetime.fromtimestamp(ts, tz=BJT).strftime("%Y-%m-%d")
        if date not in daily:
            daily[date] = 0
        value = int(l["data"], 16) / 1e18
        daily[date] += value
    
    return daily

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

def run_collection():
    log("=== Collection START ===")
    try:
        current = get_block()
        if not current:
            log("ERROR: RPC not reachable")
            return False
        log(f"Current block: {current}")
        
        # Find block ~7 days ago (binary search)
        target_ts = int((datetime.now(BJT)-timedelta(days=7)).timestamp())
        lo, hi = max(1, current-1200000), current
        for _ in range(25):
            if lo >= hi: break
            mid = (lo+hi)//2
            ts = get_block_ts(mid)
            if ts == 0: continue
            if ts < target_ts: lo = mid+1
            else: hi = mid
        log(f"Range: {lo} -> {current}")
        
        # === Collect bonus pool OUTGOING (动静态提取) ===
        log("Fetching bonus pool outgoing (动静态提取)...")
        bl = fetch_transfers(ADDR_BONUS, lo, current, 'from')
        bd = aggregate(bl, ADDR_BONUS, 'from')  # daily: {date: total_out}
        log(f"  Bonus outgoing: {len(bl)} txns, {len(bd)} days")
        log("  " + json.dumps({d: round(v,4) for d,v in sorted(bd.items())}))
        
        # === Collect stake pool INCOMING (新增质押) ===
        log("Fetching stake pool incoming (新增质押)...")
        si = fetch_transfers(ADDR_STAKE, lo, current, 'to')
        sd_in = aggregate(si, ADDR_STAKE, 'to')  # daily: {date: total_in}
        log(f"  Stake incoming: {len(si)} txns, {len(sd_in)} days")
        log("  " + json.dumps({d: round(v,4) for d,v in sorted(sd_in.items())}))
        
        # === Collect stake pool OUTGOING (赎回) ===
        log("Fetching stake pool outgoing (赎回)...")
        so = fetch_transfers(ADDR_STAKE, lo, current, 'from')
        sd_out = aggregate(so, ADDR_STAKE, 'from')  # daily: {date: total_out}
        log(f"  Stake outgoing: {len(so)} txns, {len(sd_out)} days")
        log("  " + json.dumps({d: round(v,4) for d,v in sorted(sd_out.items())}))
        
        # === Get current balances ===
        bb = get_balance(ADDR_BONUS)
        sb = get_balance(ADDR_STAKE)
        log(f"Current balances: bonus_pool={bb:.4f}, stake_pool={sb:.4f}")
        
        # === Build daily summary ===
        all_dates = sorted(set(list(bd.keys()) + list(sd_in.keys()) + list(sd_out.keys())))
        
        # Calculate daily balances (reverse from current)
        bonus_balances = {}
        stake_balances = {}
        rb, rs = bb, sb
        for d in reversed(all_dates):
            bonus_balances[d] = round(rb, 4)
            stake_balances[d] = round(rs, 4)
            rb -= bd.get(d, 0)  # bonus only has outgoing
            rs -= sd_in.get(d, 0) - sd_out.get(d, 0)  # net stake flow
        
        daily = {}
        for d in all_dates:
            bo = round(bd.get(d, 0), 4)
            si = round(sd_in.get(d, 0), 4)
            so = round(sd_out.get(d, 0), 4)
            net = round(si - so - bo, 4)
            daily[d] = {
                "date": d,
                "bonus_withdrawal": bo,
                "bonus_balance": bonus_balances.get(d, 0),
                "stake_in": si,
                "stake_out": so,
                "stake_balance": stake_balances.get(d, 0),
                "net_stake": net
            }
        
        # Save to files
        now = datetime.now(BJT).isoformat()
        full = {
            "last_updated": now,
            "current_block": current,
            "daily_summary": daily,
            "current_balances": {"bonus_pool": round(bb,4), "stake_pool": round(sb,4)}
        }
        today = daily.get(datetime.now(BJT).strftime("%Y-%m-%d"), {})
        today_data = {
            "bonus_withdrawal": today.get("bonus_withdrawal", 0),
            "stake_in": today.get("stake_in", 0),
            "stake_out": today.get("stake_out", 0),
            "net_stake": today.get("net_stake", 0),
            "bonus_balance": today.get("bonus_balance", 0),
            "stake_balance": today.get("stake_balance", 0),
            "last_updated": now
        }
        json.dump(full, open(os.path.join(DATA_DIR,"ark_data.json"),"w"), indent=2)
        json.dump(today_data, open(os.path.join(DATA_DIR,"today_data.json"),"w"), indent=2)
        
        log("=== Collection DONE ===")
        log(json.dumps(full, indent=2))
        return True
        
    except Exception as e:
        log("ERROR: " + traceback.format_exc())
        return False

app = default_app()

if __name__ == "__main__":
    if run_collection():
        log("Collection complete, starting server...")
    else:
        log("Collection failed, starting server with cache...")
    port = int(os.environ.get("PORT", 8899))
    run(host="0.0.0.0", port=port, server="auto")
