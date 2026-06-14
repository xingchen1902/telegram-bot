#!/usr/bin/env python3
"""ARK Dashboard Web Server"""
import json
import os
import subprocess
import threading
import time
from datetime import datetime, timezone, timedelta

from bottle import Bottle, static_file

BJT = timezone(timedelta(hours=8))
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
COLLECTOR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ark_collector.py")
os.makedirs(DATA_DIR, exist_ok=True)
UPDATE_INTERVAL = 300

app = Bottle()

def run_collector():
    print(f"[{datetime.now(BJT).isoformat()}] Running collector...")
    result = subprocess.run(["python3", COLLECTOR], capture_output=True, text=True, timeout=600)
    for line in (result.stdout or "").strip().split('\n'):
        if line.strip(): print(f"  {line}")
    print(f"[{datetime.now(BJT).isoformat()}] Collector finished")

def collector_loop():
    print(f"[{datetime.now(BJT).isoformat()}] Starting first collection...")
    try:
        run_collector()
    except Exception as e:
        print(f"Initial collector error: {e}")
    while True:
        time.sleep(UPDATE_INTERVAL)
        try:
            run_collector()
        except Exception as e:
            print(f"Collector error: {e}")

t = threading.Thread(target=collector_loop, daemon=True)
t.start()

@app.route('/')
def index():
    return static_file("dashboard.html", root=STATIC_DIR)

@app.route('/api/data')
def api_data():
    p = os.path.join(DATA_DIR, "ark_data.json")
    if not os.path.exists(p): return {"error": "No data yet", "daily_summary": {}}
    with open(p) as f: return json.load(f)

@app.route('/api/today')
def api_today():
    p = os.path.join(DATA_DIR, "today_data.json")
    if not os.path.exists(p): return {"error": "No data yet"}
    with open(p) as f: return json.load(f)

@app.route('/api/refresh')
def api_refresh():
    threading.Thread(target=run_collector, daemon=True).start()
    return {"status": "refreshing"}

@app.route('/static/<filename>')
def static(filename):
    return static_file(filename, root=STATIC_DIR)

if __name__ == "__main__":
    from bottle import run
    port = int(os.environ.get("PORT", 8899))
    print(f"Starting ARK Dashboard on port {port}...")
    run(app=app, host="0.0.0.0", port=port, debug=False)
