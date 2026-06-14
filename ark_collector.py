#!/usr/bin/env python3
"""ARK Token Data Collector - queries BSC via NodeReal RPC (fast)"""
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from web3 import Web3

RPC_URL = "https://bsc-mainnet.nodereal.io/v1/d96a4e697b0541628f61ae6089a97874"
TOKEN = "0xCae117ca6Bc8A341D2E7207F30E180f0e5618B9D"
ADDR_BONUS = "0x8501168656FcaC4628F6910CcABEA8B64Ebe5BD4"
ADDR_STAKE = "0xd1D95292F450b665566df4c4255615eF4Ed9BD0B"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

BJT = timezone(timedelta(hours=8))
TRANSFER_TOPIC = "0x" + Web3.keccak(text="Transfer(address,address,uint256)").hex()
BLOCK_CHUNK = 25000

def rpc(method, params=None):
    if params is None: params = []
    p = json.dumps({"jsonrpc": "2.0", "method": method, "params": params, "id": 1})
    r = subprocess.run(
        ["curl", "-s", "--connect-timeout", "10", "--max-time", "45",
         "-X", "POST", "-H", "Content-Type: application/json", "-d", p, RPC_URL],
        capture_output=True, text=True
    )
    try:
        return json.loads(r.stdout)
    except:
        return {}

def rpc_batch(requests):
    """Execute batch RPC call"""
    p = json.dumps(requests)
    r = subprocess.run(
        ["curl", "-s", "--connect-timeout", "10", "--max-time", "60",
         "-X", "POST", "-H", "Content-Type: application/json", "-d", p, RPC_URL],
        capture_output=True, text=True
    )
    try:
        return json.loads(r.stdout)
    except:
        return []

def get_block_number():
    d = rpc("eth_blockNumber")
    return int(d["result"], 16)

def get_balance(address):
    data = "0x70a08231" + address[2:].lower().zfill(64)
    d = rpc("eth_call", [{"to": TOKEN, "data": data}, "latest"])
    if d and "result" in d and d["result"]:
        return int(d["result"], 16) / 1e18
    return 0

def get_all_logs_batched(address, from_block, to_block):
    """Get all logs for an address using FROM/TO filters in batched parallel chunks"""
    addr_lower = address.lower()
    padded = "0x" + address[2:].lower().zfill(64)
    
    all_logs = {}
    chunks = list(range(from_block, to_block + 1, BLOCK_CHUNK))
    print(f"  {address[:10]}...{address[-6:]}: ", end="", flush=True)
    
    for i, start in enumerate(chunks):
        end = min(start + BLOCK_CHUNK - 1, to_block)
        
        # FROM filter
        d1 = rpc("eth_getLogs", [{"fromBlock": hex(start), "toBlock": hex(end),
                                   "address": TOKEN, "topics": [TRANSFER_TOPIC, padded, None]}])
        if isinstance(d1.get("result"), list):
            for log in d1["result"]:
                all_logs[log["transactionHash"] + log["logIndex"]] = log
        
        # TO filter
        d2 = rpc("eth_getLogs", [{"fromBlock": hex(start), "toBlock": hex(end),
                                   "address": TOKEN, "topics": [TRANSFER_TOPIC, None, padded]}])
        if isinstance(d2.get("result"), list):
            for log in d2["result"]:
                all_logs[log["transactionHash"] + log["logIndex"]] = log
        
        if (i + 1) % 10 == 0 or i == len(chunks) - 1:
            pct = (i + 1) * 100 // len(chunks)
            print(f"{pct}% ", end="", flush=True)
    
    print(f"({len(all_logs)} tx)", flush=True)
    
    # Parse
    transfers = []
    block_set = set()
    for log in all_logs.values():
        topics = log["topics"]
        f_addr = "0x" + topics[1][26:]
        t_addr = "0x" + topics[2][26:]
        value = int(log["data"], 16) / 1e18
        block_num = int(log["blockNumber"], 16)
        transfers.append({"block": block_num, "from": f_addr.lower(), "to": t_addr.lower(), "value": value})
        block_set.add(block_num)
    
    return transfers, block_set

def get_block_timestamps_batched(block_nums):
    """Get timestamps for blocks in batch"""
    timestamps = {}
    for i in range(0, len(block_nums), 200):
        batch = [{"jsonrpc": "2.0", "method": "eth_getBlockByNumber",
                  "params": [hex(bn), False], "id": bn} for bn in block_nums[i:i+200]]
        results = rpc_batch(batch)
        for res in results:
            if "result" in res and res["result"]:
                timestamps[res["id"]] = int(res["result"]["timestamp"], 16)
    return timestamps

def main():
    print(f"[{datetime.now().isoformat()}] Starting ARK data collection...")
    
    current_block = get_block_number()
    bonus_bal = get_balance(ADDR_BONUS)
    stake_bal = get_balance(ADDR_STAKE)
    print(f"  Block: {current_block} | Bonus: {bonus_bal:.2f} | Stake: {stake_bal:.2f}")
    
    # Binary search for start block (7 days BJT ago)
    target_ts = int((datetime.now(BJT) - timedelta(days=7)).timestamp())
    lo, hi = max(1, current_block - 1200000), current_block
    while lo < hi:
        mid = (lo + hi) // 2
        d = rpc("eth_getBlockByNumber", [hex(mid), False])
        t = int(d.get("result", {}).get("timestamp", 0), 16)
        if t < target_ts: lo = mid + 1
        else: hi = mid
    start_block = lo
    print(f"  Start block: {start_block} (7d BJT ago)")
    
    # Collect all transfers
    print("\n=== Bonus pool ===")
    bonus_txs, bonus_blocks = get_all_logs_batched(ADDR_BONUS, start_block, current_block)
    print(f"  {len(bonus_txs)} transfers, {len(bonus_blocks)} unique blocks")
    
    print("\n=== Stake address ===")
    stake_txs, stake_blocks = get_all_logs_batched(ADDR_STAKE, start_block, current_block)
    print(f"  {len(stake_txs)} transfers, {len(stake_blocks)} unique blocks")
    
    # Get timestamps for all blocks
    all_blocks = sorted(bonus_blocks | stake_blocks)
    print(f"\nFetching {len(all_blocks)} block timestamps...", flush=True)
    block_ts = get_block_timestamps_batched(all_blocks)
    
    # Categorize by BJT date
    def get_date(bn):
        ts = block_ts.get(bn)
        if ts: return datetime.fromtimestamp(ts, tz=BJT).strftime("%Y-%m-%d")
        return None
    
    bonus_daily = {}
    for tx in bonus_txs:
        d = get_date(tx["block"])
        if not d: continue
        if d not in bonus_daily: bonus_daily[d] = {"in": 0, "out": 0}
        if tx["to"] == ADDR_BONUS.lower(): bonus_daily[d]["in"] += tx["value"]
        if tx["from"] == ADDR_BONUS.lower(): bonus_daily[d]["out"] += tx["value"]
    
    stake_daily = {}
    for tx in stake_txs:
        d = get_date(tx["block"])
        if not d: continue
        if d not in stake_daily: stake_daily[d] = {"in": 0, "out": 0}
        if tx["to"] == ADDR_STAKE.lower(): stake_daily[d]["in"] += tx["value"]
        if tx["from"] == ADDR_STAKE.lower(): stake_daily[d]["out"] += tx["value"]
    
    # Compute balances
    all_dates = sorted(set(list(bonus_daily.keys()) + list(stake_daily.keys())))
    today_bjt = datetime.now(BJT).strftime("%Y-%m-%d")
    
    b_bal, s_bal = {}, {}
    rb, rs = bonus_bal, stake_bal
    for d in reversed(all_dates):
        b_bal[d], s_bal[d] = round(rb, 4), round(rs, 4)
        rb -= bonus_daily.get(d, {}).get("in", 0) - bonus_daily.get(d, {}).get("out", 0)
        rs -= stake_daily.get(d, {}).get("in", 0) - stake_daily.get(d, {}).get("out", 0)
    
    # Build summary
    summary = {}
    for d in all_dates:
        b = bonus_daily.get(d, {})
        s = stake_daily.get(d, {})
        bo = round(b.get("out", 0), 4)
        si = round(s.get("in", 0), 4)
        so = round(s.get("out", 0), 4)
        net = round(si - so - bo, 4)
        summary[d] = {"date": d, "bonus_withdrawal": bo, "bonus_balance": b_bal.get(d, 0),
                      "stake_in": si, "stake_out": so, "stake_balance": s_bal.get(d, 0), "net_stake": net}
        print(f"  {d}: bonus_out={bo:.2f} bonus_bal={b_bal.get(d,0):.2f} stake_in={si:.2f} stake_out={so:.2f} stake_bal={s_bal.get(d,0):.2f} net={net:.2f}")
    
    result = {"last_updated": datetime.now(timezone.utc).isoformat(), "current_block": current_block,
              "daily_summary": summary,
              "current_balances": {"bonus_pool": round(bonus_bal, 4), "stake_pool": round(stake_bal, 4)}}
    
    with open(os.path.join(DATA_DIR, "ark_data.json"), "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nSaved ark_data.json ({len(summary)} days)")
    
    td = summary.get(today_bjt, {})
    td_data = {"bonus_withdrawal": td.get("bonus_withdrawal", 0), "stake_in": td.get("stake_in", 0),
               "stake_out": td.get("stake_out", 0), "net_stake": td.get("net_stake", 0),
               "bonus_balance": td.get("bonus_balance", 0), "stake_balance": td.get("stake_balance", 0),
               "last_updated": datetime.now(timezone.utc).isoformat()}
    with open(os.path.join(DATA_DIR, "today_data.json"), "w") as f:
        json.dump(td_data, f, indent=2)
    print("Saved today_data.json")
    print(f"[{datetime.now().isoformat()}] Done!")

if __name__ == "__main__":
    main()
