#!/usr/bin/env python3
"""ARK Dashboard Web Server"""
import json
import os
import subprocess
import threading
import time
from datetime import datetime

from bottle import route, run, static_file, template, response

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
COLLECTOR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ark_collector.py")
os.makedirs(DATA_DIR, exist_ok=True)
UPDATE_INTERVAL = 300  # 5 minutes

def run_collector():
    """Run the data collector"""
    print(f"[{datetime.now().isoformat()}] Running collector...")
    result = subprocess.run(
        ["python3", COLLECTOR],
        capture_output=True, text=True, timeout=600
    )
    for line in (result.stdout or "").strip().split('\n'):
        if line.strip():
            print(f"  {line}")
    if result.stderr:
        print(f"  ERR: {result.stderr}")
    print(f"[{datetime.now().isoformat()}] Collector finished")

def collector_loop():
    """Background thread - runs collector every 5 minutes"""
    # Wait a bit on first run, then loop
    time.sleep(60)  # Wait 1 minute before first auto-refresh
    while True:
        try:
            run_collector()
        except Exception as e:
            print(f"Collector error: {e}")
        time.sleep(UPDATE_INTERVAL)

@route('/')
def index():
    return static_file("dashboard.html", root=STATIC_DIR)

@route('/api/data')
def api_data():
    path = os.path.join(DATA_DIR, "ark_data.json")
    if not os.path.exists(path):
        return {"error": "No data yet", "daily_summary": {}}
    with open(path) as f:
        return json.load(f)

@route('/api/today')
def api_today():
    path = os.path.join(DATA_DIR, "today_data.json")
    if not os.path.exists(path):
        return {"error": "No data yet"}
    with open(path) as f:
        return json.load(f)

@route('/api/refresh')
def api_refresh():
    threading.Thread(target=run_collector, daemon=True).start()
    return {"status": "refreshing"}

@route('/static/<filename>')
def static(filename):
    return static_file(filename, root=STATIC_DIR)

if __name__ == "__main__":
    # Start background updater
    t = threading.Thread(target=collector_loop, daemon=True)
    t.start()
    print("Starting ARK Dashboard on port 8899...")
    run(host="0.0.0.0", port=8899, debug=False)
