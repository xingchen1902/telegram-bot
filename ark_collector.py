#!/usr/bin/env python3
"""ARK Token Data Collector - queries BSC via NodeReal RPC"""
import json
import os
import requests
from datetime import datetime, timedelta, timezone
from web3 import Web3

RPC_URL = os.environ.get("RPC_URL", "https://bsc-mainnet.nodereal.io/v1/d96a4e697b0541628f61ae6089a97874")
TOKEN = "0xCae117ca6Bc8A341D2E7207F30E180f0e5618B9D"
ADDR_BONUS = "0x8501168656FcaC4628F6910CcABEA8B64Ebe5BD4"
ADDR_STAKE = "0xd1D95292F450b665566df4c4255615eF4Ed9BD0B"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

BJT = timezone(timedelta(hours=8))
TRANSFER_TOPIC = "0x" + Web3.keccak(text="Transfer(address,address,uint256)").hex()
BLOCK_CHUNK = 25000
SESSION = requests.Session()

def rpc(method, params=None):
    if params is None: params = []
    p = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    try:
        r = SESSION.post(RPC_URL, json=p, timeout=45)
        return r.json()
    except: return {}

def rpc_batch(requests_list):
    try:
        r = SESSION.post(RPC_URL, json=requests_list, timeout=60)
        return r.json()
    except: return []

def get_block_number():
    return int(rpc("eth_blockNumber")["result"], 16)

def get_balance(address):
    data = "0x70a08231" + address[2:].lower().zfill(64)
    d = rpc("eth_call", [{"to": TOKEN, "data": data}, "latest"])
    if d and "result" in d and d["result"]:
        return int(d["result"], 16) / 1e18
    return 0

def get_all_logs(address, from_block, to_block):
    padded = "0x" + address[2:].lower().zfill(64)
    all_logs = {}
    chunks = list(range(from_block, to_block + 1, BLOCK_CHUNK))
    for i, start in enumerate(chunks):
        end = min(start + BLOCK_CHUNK - 1, to_block)
        for topic_filter in [[TRANSFER_TOPIC, padded, None], [TRANSFER_TOPIC, None, padded]]:
            d = rpc("eth_getLogs", [{"fromBlock": hex(start), "toBlock": hex(end),
                                      "address": TOKEN, "topics": topic_filter}])
            if isinstance(d.get("result"), list):
                for log in d["result"]:
                    all_logs[log["transactionHash"] + log["logIndex"]] = log
    transfers = []
    block_set = set()
    for log in all_logs.values():
        topics = log["topics"]
        value = int(log["data"], 16) / 1e18
        block_num = int(log["blockNumber"], 16)
        transfers.append({"block": block_num, "from": "0x" + topics[1][26:], "to": "0x" + topics[2][26:], "value": value})
        block_set.add(block_num)
    return transfers, block_set

def get_block_timestamps(block_nums):
    timestamps = {}
    for i in range(0, len(block_nums), 200):
        batch = [{"jsonrpc": "2.0", "method": "eth_getBlockByNumber",
                  "params": [hex(bn), False], "id": bn} for bn in block_nums[i:i+200]]
        for res in rpc_batch(batch):
            if "result" in res and res["result"]:
                timestamps[res["id"]] = int(res["result"]["timestamp"], 16)
    return timestamps

def main():
    current_block = get_block_number()
    bonus_bal = get_balance(ADDR_BONUS)
    stake_bal = get_balance(ADDR_STAKE)
    target_ts = int((datetime.now(BJT) - timedelta(days=7)).timestamp())
    lo, hi = max(1, current_block - 1200000), current_block
    while lo < hi:
        mid = (lo + hi) // 2
        d = rpc("eth_getBlockByNumber", [hex(mid), False])
        t = int(d.get("result", {}).get("timestamp", 0), 16)
        if t < target_ts: lo = mid + 1
        else: hi = mid
    start_block = lo

    bonus_txs, bonus_blocks = get_all_logs(ADDR_BONUS, start_block, current_block)
    stake_txs, stake_blocks = get_all_logs(ADDR_STAKE, start_block, current_block)
    block_ts = get_block_timestamps(sorted(bonus_blocks | stake_blocks))

    def get_date(bn):
        ts = block_ts.get(bn)
        return datetime.fromtimestamp(ts, tz=BJT).strftime("%Y-%m-%d") if ts else None

    bonus_daily, stake_daily = {}, {}
    for tx in bonus_txs:
        d = get_date(tx["block"])
        if not d: continue
        if d not in bonus_daily: bonus_daily[d] = {"in": 0, "out": 0}
        if tx["to"] == ADDR_BONUS.lower(): bonus_daily[d]["in"] += tx["value"]
        if tx["from"] == ADDR_BONUS.lower(): bonus_daily[d]["out"] += tx["value"]
    for tx in stake_txs:
        d = get_date(tx["block"])
        if not d: continue
        if d not in stake_daily: stake_daily[d] = {"in": 0, "out": 0}
        if tx["to"] == ADDR_STAKE.lower(): stake_daily[d]["in"] += tx["value"]
        if tx["from"] == ADDR_STAKE.lower(): stake_daily[d]["out"] += tx["value"]

    all_dates = sorted(set(list(bonus_daily.keys()) + list(stake_daily.keys())))
    today_bjt = datetime.now(BJT).strftime("%Y-%m-%d")
    b_bal, s_bal = {}, {}
    rb, rs = bonus_bal, stake_bal
    for d in reversed(all_dates):
        b_bal[d], s_bal[d] = round(rb, 4), round(rs, 4)
        rb -= bonus_daily.get(d, {}).get("in", 0) - bonus_daily.get(d, {}).get("out", 0)
        rs -= stake_daily.get(d, {}).get("in", 0) - stake_daily.get(d, {}).get("out", 0)

    summary = {}
    for d in all_dates:
        b, s = bonus_daily.get(d, {}), stake_daily.get(d, {})
        bo = round(b.get("out", 0), 4)
        si = round(s.get("in", 0), 4)
        so = round(s.get("out", 0), 4)
        net = round(si - so - bo, 4)
        summary[d] = {"date": d, "bonus_withdrawal": bo, "bonus_balance": b_bal.get(d, 0),
                      "stake_in": si, "stake_out": so, "stake_balance": s_bal.get(d, 0), "net_stake": net}

    with open(os.path.join(DATA_DIR, "ark_data.json"), "w") as f:
        json.dump({"last_updated": datetime.now(BJT).isoformat(), "current_block": current_block,
                   "daily_summary": summary,
                   "current_balances": {"bonus_pool": round(bonus_bal, 4), "stake_pool": round(stake_bal, 4)}}, f, indent=2)
    td = summary.get(today_bjt, {})
    with open(os.path.join(DATA_DIR, "today_data.json"), "w") as f:
        json.dump({"bonus_withdrawal": td.get("bonus_withdrawal", 0), "stake_in": td.get("stake_in", 0),
                   "stake_out": td.get("stake_out", 0), "net_stake": td.get("net_stake", 0),
                   "bonus_balance": td.get("bonus_balance", 0), "stake_balance": td.get("stake_balance", 0),
                   "last_updated": datetime.now(BJT).isoformat()}, f, indent=2)

if __name__ == "__main__":
    main()
