"""Gunicorn config - start collector after worker init"""
import threading, time, sys, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def post_worker_init(worker):
    """Called after worker process is initialized"""
    time.sleep(0.5)
    # Import and start collector in background thread
    import server
    t = threading.Thread(target=server.run_collection, daemon=True)
    t.start()
    import logging
    logging.info("Collector thread started in worker")
